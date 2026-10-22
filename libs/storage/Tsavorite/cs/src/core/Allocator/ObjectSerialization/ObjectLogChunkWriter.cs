// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>Tuning for <see cref="ObjectLogChunkWriter{TStoreFunctions}"/>. The defaults suit production; tests shrink them to reach
    /// page cuts, multi-chunk records, and spine directories with small data.</summary>
    internal sealed class ObjectLogWriterSettings
    {
        /// <summary>Default <see cref="MinPartialBytes"/>: page tails smaller than this are left unused rather than filled.</summary>
        internal const int DefaultMinPartialBytes = 64 * 1024;

        /// <summary>Default <see cref="MaxDirectoryRecordSize"/>.</summary>
        internal const int DefaultMaxDirectoryRecordSize = 1 << 20;

        /// <summary>The settings production uses.</summary>
        internal static readonly ObjectLogWriterSettings Default = new();

        /// <summary>Largest data chunk, and the size of the staging buffer. Must leave room for a length prefix within a page.</summary>
        internal int ChunkSize = IStreamBuffer.BufferSize;

        /// <summary>Smallest piece a chunk is cut into at a page end; a shorter page tail is left unused.</summary>
        internal int MinPartialBytes = DefaultMinPartialBytes;

        /// <summary>Largest directory record before the entry list is written out as a spine record.</summary>
        internal int MaxDirectoryRecordSize = DefaultMaxDirectoryRecordSize;
    }

    /// <summary>
    /// Writes a record's out-of-line components -- an overflow key, and an overflow value or a serialized object value -- to an
    /// <see cref="ObjectLog"/>, and returns the position word and hints to stamp into the record.
    /// </summary>
    /// <remarks>
    /// <para>The components form one byte stream, key first. It is staged in a pooled buffer of <see cref="ObjectLogWriterSettings.ChunkSize"/>
    /// bytes; a source span longer than a chunk is emitted straight from the source instead. Each emitted chunk is one allocation,
    /// cut at a page end when the page tail is worth filling, so a chunk never spans a page.</para>
    /// <para>A record whose stream fits one chunk and has one component is written as a single extent: headerless when it is at most
    /// <see cref="ObjectLogExtent.ExactCutoff"/> bytes, otherwise length-prefixed. Anything else -- more than one chunk, or both an
    /// overflow key and an out-of-line value -- gets a directory: an entry per component portion of each chunk, and a top directory
    /// record that also carries the final data when they fit one allocation together. The position addresses that single extent or
    /// that top record.</para>
    /// <para>No epoch is held while staging or serializing; each chunk's allocation and copy happen inside one <see cref="ObjectLog"/>
    /// scope. The writer is used by one thread at a time.</para>
    /// </remarks>
    internal sealed unsafe class ObjectLogChunkWriter<TStoreFunctions> : IStreamBuffer
        where TStoreFunctions : IStoreFunctions
    {
        readonly ObjectLog log;
        readonly TStoreFunctions storeFunctions;
        readonly int chunkSize;
        readonly int minPartialBytes;
        readonly int maxEntriesPerRecord;

        /// <summary>Staged stream bytes not yet emitted; rented on first use.</summary>
        byte[] staging;
        int stagedLength;

        /// <summary>How many of the staged bytes, from the start, are key bytes. Key bytes always precede value bytes.</summary>
        int stagedKeyLength;

        /// <summary>The current directory record's entries; rented on first use.</summary>
        DirectoryEntry[] entries;
        int entryCount;

        /// <summary>Key entries over the whole chain, excluding spine links.</summary>
        int keyEntryCount;

        /// <summary>Whether a data chunk has been emitted, which means the record needs a directory.</summary>
        bool chunksEmitted;

        bool inRecord;
        bool recordHasKey;
        bool recordHasValue;
        bool recordValueIsObject;
        bool appendingValue;
        ulong valueObjectBytes;
        bool disposed;

        IObjectSerializer<IHeapObject> valueObjectSerializer;
        PinnedMemoryStream<ObjectLogChunkWriter<TStoreFunctions>> pinnedMemoryStream;

        /// <summary>Create a writer over <paramref name="log"/>.</summary>
        /// <param name="log">The log to write to.</param>
        /// <param name="storeFunctions">Supplies the value object serializer.</param>
        /// <param name="settings">Tuning; <see cref="ObjectLogWriterSettings.Default"/> when null.</param>
        internal ObjectLogChunkWriter(ObjectLog log, TStoreFunctions storeFunctions, ObjectLogWriterSettings settings = null)
        {
            this.log = log ?? throw new ArgumentNullException(nameof(log));
            this.storeFunctions = storeFunctions;
            settings ??= ObjectLogWriterSettings.Default;

            // A single extent of a full chunk carries a length prefix, and the whole extent must fit one page.
            chunkSize = Math.Min(settings.ChunkSize, log.PageSize - ObjectLogExtent.LengthPrefixSize);
            if (chunkSize <= ObjectLogExtent.ExactCutoff)
                throw new TsavoriteException($"Object log chunk size {chunkSize} must exceed {ObjectLogExtent.ExactCutoff}; the log page ({log.PageSize}) is too small");
            if (settings.MinPartialBytes <= 0)
                throw new TsavoriteException($"{nameof(ObjectLogWriterSettings.MinPartialBytes)} ({settings.MinPartialBytes}) must be positive");
            minPartialBytes = settings.MinPartialBytes;

            // A spine record is never cut, so it must fit a page, and it must hold at least a link and one more entry.
            var maxDirectoryRecordSize = Math.Min(settings.MaxDirectoryRecordSize, log.PageSize);
            maxEntriesPerRecord = ObjectLogDirectory.MaxEntriesInRecord(maxDirectoryRecordSize);
            if (maxEntriesPerRecord < 2)
                throw new TsavoriteException($"{nameof(ObjectLogWriterSettings.MaxDirectoryRecordSize)} ({maxDirectoryRecordSize}) must hold at least two directory entries");
        }

        /// <inheritdoc/>
        public bool IsForWrite => true;

        /// <summary>
        /// Write a record's out-of-line components and return the position word and hints to stamp into it.
        /// </summary>
        /// <param name="keyOverflow">The overflow key, or empty if the key is inline.</param>
        /// <param name="valueOverflow">The overflow value, or empty if the value is not an overflow value.</param>
        /// <param name="valueIsObject">Whether the value is an object value; <paramref name="valueObject"/> may still be null.</param>
        /// <param name="valueObject">The object value, serialized through the store's value serializer.</param>
        internal ObjectLogWriteResult WriteRecordObjects(in OverflowByteArray keyOverflow, in OverflowByteArray valueOverflow, bool valueIsObject, IHeapObject valueObject)
        {
            var hasKey = !keyOverflow.IsEmpty;
            var valueIsOverflow = !valueOverflow.IsEmpty;
            Debug.Assert(!(valueIsOverflow && valueIsObject), "A value cannot be both an overflow value and an object value");

            BeginRecord(hasKey, hasValue: valueIsOverflow || valueIsObject, valueIsObject);
            if (hasKey)
                AppendKey(keyOverflow.ReadOnlySpan);
            if (valueIsOverflow)
                AppendValue(valueOverflow.ReadOnlySpan);
            else if (valueObject is not null)
                Serialize(valueObject);
            return FinishRecord();
        }

        /// <summary>Begin a record whose components are supplied through <see cref="AppendKey"/> and <see cref="AppendValue"/>.</summary>
        /// <param name="hasKey">Whether the record has an overflow key.</param>
        /// <param name="hasValue">Whether the record has an out-of-line value, even an empty one.</param>
        /// <param name="valueIsObject">Whether the value is an object value.</param>
        internal void BeginRecord(bool hasKey, bool hasValue, bool valueIsObject)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            Debug.Assert(hasKey || hasValue, "A record with no out-of-line components has nothing to write");
            stagedLength = 0;
            stagedKeyLength = 0;
            entryCount = 0;
            keyEntryCount = 0;
            chunksEmitted = false;
            recordHasKey = hasKey;
            recordHasValue = hasValue;
            recordValueIsObject = valueIsObject;
            appendingValue = false;
            valueObjectBytes = 0;
            inRecord = true;
        }

        /// <summary>Append overflow key bytes. All key bytes precede any value bytes.</summary>
        internal void AppendKey(ReadOnlySpan<byte> data)
        {
            Debug.Assert(inRecord && recordHasKey && !appendingValue, "Key bytes must be appended to a record with a key, before its value");
            Append(data, isKey: true);
        }

        /// <summary>Append value bytes: an overflow value's, or a serialized object's.</summary>
        internal void AppendValue(ReadOnlySpan<byte> data)
        {
            Debug.Assert(inRecord && recordHasValue, "Value bytes must be appended to a record with an out-of-line value");
            appendingValue = true;
            if (recordValueIsObject)
                valueObjectBytes += (ulong)data.Length;
            Append(data, isKey: false);
        }

        /// <summary>Finish the record begun by <see cref="BeginRecord"/>: write its single extent or its directory.</summary>
        internal ObjectLogWriteResult FinishRecord()
        {
            Debug.Assert(inRecord, "FinishRecord without BeginRecord");
            inRecord = false;

            var needsDirectory = chunksEmitted || (recordHasKey && recordHasValue);
            long address = 0, payloadLength = 0;
            if (!needsDirectory && !TryWriteSingleExtent(out address, out payloadLength))
                needsDirectory = true;
            if (needsDirectory)
                WriteTopDirectoryRecord(out address, out payloadLength);

            ObjectLogWriteResult result = default;
            result.ValueObjectLength = recordValueIsObject ? valueObjectBytes : 0;
            var word = (ulong)address & ObjectLogPositionInfo.AddressMask;
            if (needsDirectory)
                word |= ObjectLogPositionInfo.kHasDirectoryMask;

            // The key slot sizes the extent whenever the key is overflow: the extent is then either the key alone or a directory.
            bool isExact;
            if (recordHasKey)
            {
                result.KeySizeHint = ObjectLogExtent.EncodeKeyHint(payloadLength, out result.KeyRdhLengthBits, out isExact);
                if (isExact)
                    word |= ObjectLogPositionInfo.kKeyIsExactSizeMask;
            }
            else
            {
                result.ValueSizeHint = ObjectLogExtent.EncodeValueHint(payloadLength, out isExact);
                if (isExact)
                    word |= ObjectLogPositionInfo.kValueIsExactSizeMask;
            }
            result.PositionWord = word;
            return result;
        }

        void Serialize(IHeapObject valueObject)
        {
            if (valueObjectSerializer is null)
            {
                pinnedMemoryStream = new(this);
                valueObjectSerializer = storeFunctions.CreateValueObjectSerializer()
                    ?? throw new TsavoriteException("The store has object values but no value object serializer");
                valueObjectSerializer.BeginSerialize(pinnedMemoryStream);
            }
            appendingValue = true;
            valueObjectSerializer.Serialize(valueObject);
            if (valueObjectBytes >= IHeapObject.MaxSerializedObjectSize)
                throw new TsavoriteException($"Object serialized size {valueObjectBytes} exceeds max serialization limit of {IHeapObject.MaxSerializedObjectSize}");
        }

        /// <inheritdoc/>
        /// <remarks>Called by the value serializer.</remarks>
        public void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AppendValue(data);
        }

        /// <inheritdoc/>
        /// <remarks>A no-op: chunks are emitted as the stream fills them, and the final one by <see cref="FinishRecord"/>.</remarks>
        public void FlushAndReset(CancellationToken cancellationToken = default) { }

        /// <inheritdoc/>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException($"Read is not supported by {nameof(ObjectLogChunkWriter<TStoreFunctions>)}");

        void Append(ReadOnlySpan<byte> data, bool isKey)
        {
            while (!data.IsEmpty)
            {
                // A full staging buffer with more data to come means the record has more than one chunk.
                if (stagedLength == chunkSize)
                {
                    EmitStaged();
                    continue;
                }

                // Nothing staged and more than a chunk to go: emit straight from the source instead of copying it through staging.
                if (stagedLength == 0 && data.Length > chunkSize)
                {
                    EmitDataChunk(data.Slice(0, chunkSize), isKey ? chunkSize : 0);
                    data = data.Slice(chunkSize);
                    continue;
                }

                staging ??= ArrayPool<byte>.Shared.Rent(chunkSize);
                var count = Math.Min(chunkSize - stagedLength, data.Length);
                data.Slice(0, count).CopyTo(staging.AsSpan(stagedLength));
                stagedLength += count;
                if (isKey)
                {
                    Debug.Assert(stagedKeyLength == stagedLength - count, "Key bytes must precede value bytes in the staging buffer");
                    stagedKeyLength += count;
                }
                data = data.Slice(count);
            }
        }

        void EmitStaged()
        {
            EmitDataChunk(staging.AsSpan(0, stagedLength), stagedKeyLength);
            stagedLength = 0;
            stagedKeyLength = 0;
        }

        /// <summary>Write <paramref name="data"/> as data chunks, cutting at a page end when the page tail is worth filling.</summary>
        /// <param name="data">The bytes to write.</param>
        /// <param name="keyBytes">How many of <paramref name="data"/>'s leading bytes are key bytes.</param>
        void EmitDataChunk(ReadOnlySpan<byte> data, int keyBytes)
        {
            chunksEmitted = true;
            var offset = 0;
            while (offset < data.Length)
            {
                var wanted = data.Length - offset;
                long address;
                int allocated;
                var scopeAcquiredEpoch = log.BeginScope();
                try
                {
                    // A chunk's cut point is arbitrary, so fill a page tail exactly when it is large enough to be worth a chunk.
                    var pageRemaining = log.RemainingOnPage(log.TailAddress);
                    if (wanted > pageRemaining && pageRemaining >= minPartialBytes)
                        wanted = pageRemaining;
                    address = log.Allocate(wanted, minPartialBytes, out allocated);
                    data.Slice(offset, allocated).CopyTo(log.AsSpan(address, allocated));
                }
                finally
                {
                    log.EndScope(scopeAcquiredEpoch);
                }
                AddChunkEntries(address, allocated, Math.Clamp(keyBytes - offset, 0, allocated));
                offset += allocated;
            }
        }

        /// <summary>Add the entries for one chunk: one per component portion. A chunk holding the end of the key and the start of the
        /// value gets two entries at the same address; a reader offsets the second by the first's length.</summary>
        void AddChunkEntries(long address, int length, int keyBytes)
        {
            if (keyBytes > 0)
            {
                AddEntry(new DirectoryEntry(address, keyBytes));
                keyEntryCount++;
            }
            if (length > keyBytes)
                AddEntry(new DirectoryEntry(address, length - keyBytes));
        }

        void AddEntry(DirectoryEntry entry)
        {
            entries ??= ArrayPool<DirectoryEntry>.Shared.Rent(maxEntriesPerRecord);
            if (entryCount == maxEntriesPerRecord)
                WriteSpine();
            entries[entryCount++] = entry;
        }

        /// <summary>Write the current entries as a spine record and start a fresh list whose first entry links back to it.</summary>
        void WriteSpine()
        {
            var size = ObjectLogDirectory.RecordSize(entryCount);
            long address;
            var scopeAcquiredEpoch = log.BeginScope();
            try
            {
                address = log.Allocate(size);
                WriteDirectoryRecord(log.GetPhysicalAddress(address), entryCount, keyEntryCountInRecord: 0, DirectoryFlags.None);
            }
            finally
            {
                log.EndScope(scopeAcquiredEpoch);
            }
            entryCount = 0;
            entries[entryCount++] = new DirectoryEntry(address, size, isDirectory: true);
        }

        void WriteDirectoryRecord(byte* destination, int count, int keyEntryCountInRecord, DirectoryFlags flags)
        {
            new DirectoryRecordHeader(count, keyEntryCountInRecord, flags).WriteTo(destination);
            destination += DirectoryRecordHeader.Size;
            for (var i = 0; i < count; i++, destination += DirectoryEntry.Size)
                entries[i].WriteTo(destination);
        }

        /// <summary>Write the staged single component as one extent.</summary>
        /// <returns>False if the allocation was cut at a page end; the piece then holds the record's first data chunk and the rest of
        /// the stream remains staged for a directory record.</returns>
        bool TryWriteSingleExtent(out long address, out long payloadLength)
        {
            payloadLength = stagedLength;
            if (stagedLength == 0)
            {
                // A zero-length component has no bytes on the log; its exact hint of zero says so.
                address = log.TailAddress;
                return true;
            }

            var isExact = ObjectLogExtent.IsExact(stagedLength);
            var extentLength = (int)ObjectLogExtent.ExtentLength(stagedLength);
            int allocated;
            var scopeAcquiredEpoch = log.BeginScope();
            try
            {
                // An exact extent is never cut: it is small, and a cut would turn a one-IO read into a directory.
                address = log.Allocate(extentLength, isExact ? 0 : minPartialBytes, out allocated);
                var destination = log.GetPhysicalAddress(address);
                if (allocated == extentLength)
                {
                    if (!isExact)
                    {
                        *(int*)destination = stagedLength;
                        destination += ObjectLogExtent.LengthPrefixSize;
                    }
                    staging.AsSpan(0, stagedLength).CopyTo(new Span<byte>(destination, stagedLength));
                    return true;
                }

                // Cut at the page end: fill the tail with the start of the stream as a data chunk.
                staging.AsSpan(0, allocated).CopyTo(new Span<byte>(destination, allocated));
            }
            finally
            {
                log.EndScope(scopeAcquiredEpoch);
            }

            chunksEmitted = true;
            AddChunkEntries(address, allocated, Math.Min(stagedKeyLength, allocated));
            ConsumeStaged(allocated);
            return false;
        }

        /// <summary>Drop the first <paramref name="count"/> staged bytes, which have been written.</summary>
        void ConsumeStaged(int count)
        {
            staging.AsSpan(count, stagedLength - count).CopyTo(staging);
            stagedLength -= count;
            stagedKeyLength = Math.Max(0, stagedKeyLength - count);
        }

        /// <summary>Write the top directory record, combined with the staged final data when they fit one allocation together.</summary>
        void WriteTopDirectoryRecord(out long address, out long payloadLength)
        {
            while (true)
            {
                var finalKeyBytes = stagedKeyLength;
                var finalValueBytes = stagedLength - stagedKeyLength;
                var finalEntries = (finalKeyBytes > 0 ? 1 : 0) + (finalValueBytes > 0 ? 1 : 0);
                if (entryCount + finalEntries > maxEntriesPerRecord)
                {
                    entries ??= ArrayPool<DirectoryEntry>.Shared.Rent(maxEntriesPerRecord);
                    WriteSpine();
                    continue;
                }

                var count = entryCount + finalEntries;
                Debug.Assert(count > 0, "A directory record must have at least one entry");
                var recordSize = ObjectLogDirectory.RecordSize(count, stagedLength);
                var isExact = ObjectLogExtent.IsExact(recordSize);
                var extentLength = ObjectLogExtent.ExtentLength(recordSize);
                if (extentLength > log.PageSize)
                {
                    // The final data cannot travel with the directory; write it as its own chunk(s) first.
                    EmitStaged();
                    continue;
                }

                int allocated;
                var scopeAcquiredEpoch = log.BeginScope();
                try
                {
                    // Cutting is worthwhile only when there is final data to put in the page tail.
                    address = log.Allocate((int)extentLength, isExact || stagedLength == 0 ? 0 : minPartialBytes, out allocated);
                    var destination = log.GetPhysicalAddress(address);
                    if (allocated == extentLength)
                    {
                        if (!isExact)
                        {
                            *(int*)destination = recordSize;
                            destination += ObjectLogExtent.LengthPrefixSize;
                        }

                        // The final entries address this record's own data area, which follows the entry vector.
                        entries ??= ArrayPool<DirectoryEntry>.Shared.Rent(maxEntriesPerRecord);
                        var dataAddress = address + (extentLength - recordSize) + ObjectLogDirectory.CombinedDataOffset(count);
                        if (finalKeyBytes > 0)
                        {
                            entries[entryCount++] = new DirectoryEntry(dataAddress, finalKeyBytes);
                            keyEntryCount++;
                        }
                        if (finalValueBytes > 0)
                            entries[entryCount++] = new DirectoryEntry(dataAddress, finalValueBytes);

                        WriteDirectoryRecord(destination, entryCount, keyEntryCount, stagedLength > 0 ? DirectoryFlags.DirectoryIsCombined : DirectoryFlags.None);
                        if (stagedLength > 0)
                            staging.AsSpan(0, stagedLength).CopyTo(new Span<byte>(destination + ObjectLogDirectory.CombinedDataOffset(entryCount), stagedLength));
                        payloadLength = recordSize;
                        stagedLength = 0;
                        stagedKeyLength = 0;
                        return;
                    }

                    // Cut at the page end: fill the tail with the start of the final data, and place the record on the next page.
                    allocated = Math.Min(allocated, stagedLength);
                    staging.AsSpan(0, allocated).CopyTo(new Span<byte>(destination, allocated));
                }
                finally
                {
                    log.EndScope(scopeAcquiredEpoch);
                }

                chunksEmitted = true;
                AddChunkEntries(address, allocated, Math.Min(stagedKeyLength, allocated));
                ConsumeStaged(allocated);
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (disposed)
                return;
            disposed = true;

            var localMemoryStream = Interlocked.Exchange(ref pinnedMemoryStream, null);
            if (localMemoryStream is not null)
            {
                // End serialization before disposing the stream, which may flush final data through this writer.
                valueObjectSerializer?.EndSerialize();
                localMemoryStream.Dispose();
            }

            if (staging is not null)
            {
                ArrayPool<byte>.Shared.Return(staging);
                staging = null;
            }
            if (entries is not null)
            {
                ArrayPool<DirectoryEntry>.Shared.Return(entries);
                entries = null;
            }
        }
    }
}