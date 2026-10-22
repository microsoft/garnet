// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Reads a record's out-of-line components, written by <see cref="ObjectLogChunkWriter{TStoreFunctions}"/>, from an
    /// <see cref="ObjectLog"/>.
    /// </summary>
    /// <remarks>
    /// <para><see cref="BeginRecord"/> reads the extent the record's position addresses, in one IO sized by the record's hint. For a
    /// single extent that is the component itself. For a directory it is the top directory record, including any final data
    /// combined with it; a spine is walked backward from there, and every entry is validated before any data is read. Component
    /// lengths are then exact, so an overflow array is allocated once at its final size and an object's deserializer is bounded by
    /// its own byte count.</para>
    /// <para>Entries sharing an address belong to one chunk: each after the first starts where the previous one ended.</para>
    /// <para>No epoch is held while deserializing; <see cref="ObjectLog.Read"/> takes its own scope per read. The reader is used by one
    /// thread at a time.</para>
    /// </remarks>
    internal sealed unsafe class ObjectLogChunkReader<TStoreFunctions> : IStreamBuffer
        where TStoreFunctions : IStoreFunctions
    {
        readonly ObjectLog log;
        readonly TStoreFunctions storeFunctions;

        ObjectLogRecordRef record;
        bool inRecord;
        bool hasDirectory;

        /// <summary>The top extent as read, prefix included.</summary>
        byte[] topBuffer;
        long topAddress;
        int topPayloadOffset;
        int topPayloadLength;

        /// <summary>The log range of the final data combined with the top directory record; empty when there is none.</summary>
        long combinedStart;
        long combinedEnd;

        /// <summary>The directory's data entries, spine links excluded, in stream order, with each entry's effective address.</summary>
        long[] entryAddresses;
        int[] entryLengths;
        int dataEntryCount;
        int keyEntryCount;

        long keyLength;
        long valueLength;

        /// <summary>Holds a data chunk read for streaming or copying.</summary>
        byte[] chunkBuffer;

        // Object streaming state.
        int nextValueEntry;
        byte[] segmentArray;
        int segmentOffset;
        int segmentRemaining;
        long valueRemaining;

        IObjectSerializer<IHeapObject> valueObjectSerializer;
        PinnedMemoryStream<ObjectLogChunkReader<TStoreFunctions>> pinnedMemoryStream;
        bool disposed;

        /// <summary>Create a reader over <paramref name="log"/>.</summary>
        /// <param name="log">The log to read from.</param>
        /// <param name="storeFunctions">Supplies the value object serializer.</param>
        internal ObjectLogChunkReader(ObjectLog log, TStoreFunctions storeFunctions)
        {
            this.log = log ?? throw new ArgumentNullException(nameof(log));
            this.storeFunctions = storeFunctions;
        }

        /// <inheritdoc/>
        public bool IsForWrite => false;

        /// <summary>The overflow key's exact length; zero when the key is inline.</summary>
        internal long KeyLength => keyLength;

        /// <summary>The out-of-line value's exact length; zero when the value is inline or empty.</summary>
        internal long ValueLength => valueLength;

        /// <summary>
        /// Read the extent <paramref name="record"/>'s position addresses, and for a directory its spine, so its components' lengths
        /// and locations are known.
        /// </summary>
        /// <remarks>The record must be stamped; an unstamped record has no object-log bytes and is materialized without reading.</remarks>
        internal void BeginRecord(in ObjectLogRecordRef record)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (record.IsUnstamped)
                throw new TsavoriteException("An unstamped record has no object-log bytes to read");
            if (!record.KeyIsOverflow && !record.ValueIsOutOfLine)
                throw new TsavoriteException("A record with no out-of-line components has nothing to read from the object log");

            this.record = record;
            inRecord = true;
            hasDirectory = ObjectLogPositionInfo.WordHasDirectory(record.PositionWord);
            topAddress = (long)(record.PositionWord & ObjectLogPositionInfo.AddressMask);
            dataEntryCount = 0;
            keyEntryCount = 0;
            combinedStart = combinedEnd = 0;
            nextValueEntry = 0;
            segmentRemaining = 0;
            valueRemaining = 0;

            if (record.KeyIsOverflow && record.ValueIsOutOfLine && !hasDirectory)
                ThrowCorrupt("a record with both an overflow key and an out-of-line value has no directory");

            // The key slot sizes the extent whenever the key is overflow: it is then the key alone, or a directory.
            var sizedByKey = record.KeyIsOverflow;
            var isExact = sizedByKey
                ? (record.PositionWord & ObjectLogPositionInfo.kKeyIsExactSizeMask) != 0
                : (record.PositionWord & ObjectLogPositionInfo.kValueIsExactSizeMask) != 0;
            ReadTopExtent(sizedByKey, isExact);

            if (!hasDirectory)
            {
                keyLength = sizedByKey ? topPayloadLength : 0;
                valueLength = sizedByKey ? 0 : topPayloadLength;
                return;
            }

            if (topPayloadLength == 0)
                ThrowCorrupt("a directory position addresses an empty extent");
            ParseDirectory();
        }

        void ReadTopExtent(bool sizedByKey, bool isExact)
        {
            if (isExact)
            {
                topPayloadOffset = 0;
                topPayloadLength = sizedByKey ? record.KeySizeHint : record.ValueSizeHint;
                if (topPayloadLength > 0)
                {
                    EnsureCapacity(ref topBuffer, topPayloadLength);
                    log.Read(topAddress, topPayloadLength, topBuffer);
                }
                return;
            }

            // Sized by a page count: read that much, clamped to the extent's page and the log's tail, then take the exact length from
            // the prefix. A prefixed extent never spans a page, so a short first read is completed within the same page.
            var initialRead = sizedByKey
                ? ObjectLogExtent.KeyInitialRead(record.KeyRdhLengthBits, record.KeySizeHint, isExact: false)
                : ObjectLogExtent.ValueInitialRead(record.ValueSizeHint, isExact: false);
            var available = Math.Min(log.RemainingOnPage(topAddress), log.TailAddress - topAddress);
            if (initialRead < ObjectLogExtent.LengthPrefixSize || available < ObjectLogExtent.LengthPrefixSize)
                ThrowCorrupt($"a length-prefixed extent at {topAddress} has room for only {Math.Min(initialRead, available)} bytes");
            var readLength = (int)Math.Min(initialRead, available);
            EnsureCapacity(ref topBuffer, readLength);
            log.Read(topAddress, readLength, topBuffer);

            var payloadLength = Unsafe.ReadUnaligned<int>(ref topBuffer[0]);
            if (payloadLength <= ObjectLogExtent.ExactCutoff || (long)ObjectLogExtent.LengthPrefixSize + payloadLength > available)
                ThrowCorrupt($"the length prefix {payloadLength} at {topAddress} is outside ({ObjectLogExtent.ExactCutoff}, {available - ObjectLogExtent.LengthPrefixSize}]");

            var extentLength = ObjectLogExtent.LengthPrefixSize + payloadLength;
            if (extentLength > readLength)
            {
                EnsureCapacity(ref topBuffer, extentLength, preserve: readLength);
                log.Read(topAddress + readLength, extentLength - readLength, topBuffer.AsSpan(readLength));
            }
            topPayloadOffset = ObjectLogExtent.LengthPrefixSize;
            topPayloadLength = payloadLength;
        }

        void ParseDirectory()
        {
            if (topPayloadLength < DirectoryRecordHeader.Size)
                ThrowCorrupt($"a {topPayloadLength}-byte directory record is shorter than its header");

            DirectoryRecordHeader header;
            DirectoryEntry[] topEntries;
            fixed (byte* payload = &topBuffer[topPayloadOffset])
            {
                header = DirectoryRecordHeader.ReadFrom(payload);
                topEntries = ReadEntries(payload, header, topPayloadLength, isTop: true, out var combinedLength);
                combinedStart = topAddress + topPayloadOffset + ObjectLogDirectory.RecordSize(header.EntryCount);
                combinedEnd = combinedStart + combinedLength;
            }

            // Walk the spine backward from the top record, collecting each record's data entries; then flatten them oldest first.
            var blocks = new List<DirectoryEntry[]> { topEntries };
            var recordAddress = topAddress;
            var link = topEntries[0];
            byte[] spineBuffer = null;
            try
            {
                while (link.IsDirectory)
                {
                    if (link.Address >= recordAddress || link.Address < log.BeginAddress)
                        ThrowCorrupt($"a spine link to {link.Address} does not point backward from {recordAddress}");
                    ValidateExtent(link.Address, link.Length);
                    EnsureCapacity(ref spineBuffer, link.Length);
                    log.Read(link.Address, link.Length, spineBuffer);

                    DirectoryEntry[] spineEntries;
                    fixed (byte* p = spineBuffer)
                    {
                        var spineHeader = DirectoryRecordHeader.ReadFrom(p);
                        if (spineHeader.KeyEntryCount != 0 || spineHeader.IsCombined)
                            ThrowCorrupt($"the spine record at {link.Address} carries top-record fields");
                        spineEntries = ReadEntries(p, spineHeader, link.Length, isTop: false, out _);
                    }
                    blocks.Add(spineEntries);
                    recordAddress = link.Address;
                    link = spineEntries[0];
                }
            }
            finally
            {
                if (spineBuffer is not null)
                    ArrayPool<byte>.Shared.Return(spineBuffer);
            }

            var total = 0L;
            foreach (var block in blocks)
                total += block.Length - (block[0].IsDirectory ? 1 : 0);
            if (total > int.MaxValue)
                ThrowCorrupt($"a directory lists {total} entries");
            EnsureCapacity(ref entryAddresses, (int)total);
            EnsureCapacity(ref entryLengths, (int)total);

            dataEntryCount = 0;
            long previousAddress = -1, sameAddressOffset = 0;
            for (var b = blocks.Count - 1; b >= 0; b--)
            {
                var block = blocks[b];
                for (var i = block[0].IsDirectory ? 1 : 0; i < block.Length; i++)
                {
                    var entry = block[i];
                    long effectiveAddress;
                    if (entry.Address == previousAddress)
                    {
                        effectiveAddress = entry.Address + sameAddressOffset;
                        sameAddressOffset += entry.Length;
                    }
                    else
                    {
                        effectiveAddress = entry.Address;
                        sameAddressOffset = entry.Length;
                        previousAddress = entry.Address;
                    }

                    // Entries sharing an address are one chunk, so together they stay within that address's page.
                    if (log.GetPage(effectiveAddress + entry.Length - 1) != log.GetPage(entry.Address))
                        ThrowCorrupt($"entries at {entry.Address} extend past its page");
                    if (effectiveAddress < combinedStart || effectiveAddress + entry.Length > combinedEnd)
                        ValidateExtent(effectiveAddress, entry.Length);
                    entryAddresses[dataEntryCount] = effectiveAddress;
                    entryLengths[dataEntryCount] = entry.Length;
                    dataEntryCount++;
                }
            }

            keyEntryCount = header.KeyEntryCount;
            if (keyEntryCount > dataEntryCount)
                ThrowCorrupt($"the key has {keyEntryCount} entries but the directory lists only {dataEntryCount}");
            if (record.KeyIsOverflow != (keyEntryCount > 0))
                ThrowCorrupt($"the directory lists {keyEntryCount} key entries for a record whose key is {(record.KeyIsOverflow ? "overflow" : "inline")}");
            if (!record.ValueIsOutOfLine && dataEntryCount > keyEntryCount)
                ThrowCorrupt("the directory lists value entries for a record whose value is inline");

            keyLength = 0;
            for (var i = 0; i < keyEntryCount; i++)
                keyLength += entryLengths[i];
            valueLength = 0;
            for (var i = keyEntryCount; i < dataEntryCount; i++)
                valueLength += entryLengths[i];
        }

        /// <summary>Decode a directory record's entries, checking the header against the record's size.</summary>
        DirectoryEntry[] ReadEntries(byte* record, DirectoryRecordHeader header, int recordLength, bool isTop, out int combinedLength)
        {
            var count = header.EntryCount;
            var entriesSize = (long)ObjectLogDirectory.EntriesOffset + ((long)count * DirectoryEntry.Size);
            if (count <= 0 || entriesSize > recordLength)
                ThrowCorrupt($"a {recordLength}-byte directory record claims {count} entries");
            combinedLength = recordLength - (int)entriesSize;
            if (!isTop && combinedLength != 0)
                ThrowCorrupt($"a spine record of {recordLength} bytes holds {combinedLength} bytes beyond its {count} entries");
            if (header.IsCombined != (combinedLength > 0))
                ThrowCorrupt($"a directory record's combined flag ({header.IsCombined}) disagrees with its {combinedLength} trailing bytes");

            var result = new DirectoryEntry[count];
            var source = record + ObjectLogDirectory.EntriesOffset;
            for (var i = 0; i < count; i++, source += DirectoryEntry.Size)
            {
                result[i] = DirectoryEntry.ReadFrom(source);
                if (i > 0 && result[i].IsDirectory)
                    ThrowCorrupt("a spine link is not the first entry of its record");
            }
            return result;
        }

        void ValidateExtent(long address, int length)
        {
            if (address < log.BeginAddress || address + length > log.TailAddress)
                ThrowCorrupt($"an extent of {length} bytes at {address} is outside the log [{log.BeginAddress}, {log.TailAddress})");
            if (log.GetPage(address) != log.GetPage(address + length - 1))
                ThrowCorrupt($"an extent of {length} bytes at {address} spans a page");
        }

        /// <summary>Read the overflow key.</summary>
        internal OverflowByteArray ReadKey()
        {
            Debug.Assert(inRecord && record.KeyIsOverflow, "ReadKey requires a record with an overflow key");
            var key = OverflowByteArray.AllocateData(checked((int)keyLength));
            ReadComponent(isKey: true, key.Span);
            return key;
        }

        /// <summary>Read the overflow value.</summary>
        internal OverflowByteArray ReadValueOverflow()
        {
            Debug.Assert(inRecord && record.ValueIsOverflow, "ReadValueOverflow requires a record with an overflow value");
            if (valueLength > int.MaxValue)
                ThrowCorrupt($"an overflow value of {valueLength} bytes is too large");
            var value = OverflowByteArray.AllocateData((int)valueLength);
            ReadComponent(isKey: false, value.Span);
            return value;
        }

        void ReadComponent(bool isKey, Span<byte> destination)
        {
            if (!hasDirectory)
            {
                topBuffer.AsSpan(topPayloadOffset, topPayloadLength).CopyTo(destination);
                return;
            }

            var (start, end) = isKey ? (0, keyEntryCount) : (keyEntryCount, dataEntryCount);
            for (var i = start; i < end; i++)
            {
                var length = entryLengths[i];
                CopyEntry(i, destination.Slice(0, length));
                destination = destination.Slice(length);
            }
        }

        void CopyEntry(int index, Span<byte> destination)
        {
            var address = entryAddresses[index];
            if (address >= combinedStart && address + destination.Length <= combinedEnd)
                topBuffer.AsSpan((int)(address - topAddress), destination.Length).CopyTo(destination);
            else
                log.Read(address, destination.Length, destination);
        }

        /// <summary>The bytes of entry <paramref name="index"/>, valid until the next call.</summary>
        ReadOnlySpan<byte> GetEntryBytes(int index)
        {
            LoadEntry(index, out var array, out var offset);
            return array.AsSpan(offset, entryLengths[index]);
        }

        /// <summary>Locate entry <paramref name="index"/>'s bytes: in the top extent when they are its combined data, otherwise read into
        /// <see cref="chunkBuffer"/>.</summary>
        void LoadEntry(int index, out byte[] array, out int offset)
        {
            var address = entryAddresses[index];
            var length = entryLengths[index];
            if (address >= combinedStart && address + length <= combinedEnd)
            {
                array = topBuffer;
                offset = (int)(address - topAddress);
                return;
            }
            EnsureCapacity(ref chunkBuffer, length);
            log.Read(address, length, chunkBuffer);
            array = chunkBuffer;
            offset = 0;
        }

        /// <summary>Deserialize the object value, or return null for an object that serialized to nothing.</summary>
        internal IHeapObject ReadValueObject()
        {
            Debug.Assert(inRecord && record.ValueIsObject, "ReadValueObject requires a record with an object value");
            if (valueLength == 0)
                return null;
            if ((ulong)valueLength >= IHeapObject.MaxSerializedObjectSize)
                ThrowCorrupt($"an object of {valueLength} bytes exceeds the serialization limit of {IHeapObject.MaxSerializedObjectSize}");

            valueRemaining = valueLength;
            if (hasDirectory)
            {
                nextValueEntry = keyEntryCount;
                segmentRemaining = 0;
            }
            else
            {
                segmentArray = topBuffer;
                segmentOffset = topPayloadOffset;
                segmentRemaining = topPayloadLength;
            }

            if (valueObjectSerializer is null)
            {
                pinnedMemoryStream = new(this);
                valueObjectSerializer = storeFunctions.CreateValueObjectSerializer()
                    ?? throw new TsavoriteException("The store has object values but no value object serializer");
                valueObjectSerializer.BeginDeserialize(pinnedMemoryStream);
            }
            valueObjectSerializer.Deserialize(out var valueObject);
            valueRemaining = 0;
            segmentRemaining = 0;
            return valueObject;
        }

        /// <inheritdoc/>
        /// <remarks>Called by the value deserializer; returns zero at the end of the object's bytes.</remarks>
        public int Read(Span<byte> destinationSpan, CancellationToken cancellationToken = default)
        {
            var total = 0;
            while (!destinationSpan.IsEmpty && valueRemaining > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (segmentRemaining == 0)
                {
                    LoadEntry(nextValueEntry, out segmentArray, out segmentOffset);
                    segmentRemaining = entryLengths[nextValueEntry++];
                }

                var count = (int)Math.Min(Math.Min(destinationSpan.Length, segmentRemaining), valueRemaining);
                segmentArray.AsSpan(segmentOffset, count).CopyTo(destinationSpan);
                segmentOffset += count;
                segmentRemaining -= count;
                valueRemaining -= count;
                total += count;
                destinationSpan = destinationSpan.Slice(count);
            }
            return total;
        }

        /// <summary>
        /// Copy this record's components to <paramref name="destination"/>, which writes them as a new record in its own log.
        /// </summary>
        /// <returns>The destination's position word and hints, to stamp into the record in place of its current ones.</returns>
        internal ObjectLogWriteResult CopyRecordTo(ObjectLogChunkWriter<TStoreFunctions> destination)
        {
            Debug.Assert(inRecord, "CopyRecordTo requires BeginRecord");
            destination.BeginRecord(record.KeyIsOverflow, record.ValueIsOutOfLine, record.ValueIsObject);
            if (!hasDirectory)
            {
                var payload = topBuffer.AsSpan(topPayloadOffset, topPayloadLength);
                if (record.KeyIsOverflow)
                    destination.AppendKey(payload);
                else
                    destination.AppendValue(payload);
            }
            else
            {
                for (var i = 0; i < keyEntryCount; i++)
                    destination.AppendKey(GetEntryBytes(i));
                for (var i = keyEntryCount; i < dataEntryCount; i++)
                    destination.AppendValue(GetEntryBytes(i));
            }
            return destination.FinishRecord();
        }

        /// <summary>Finish the record begun by <see cref="BeginRecord"/>.</summary>
        internal void EndRecord()
        {
            inRecord = false;
            valueRemaining = 0;
            segmentRemaining = 0;
            segmentArray = null;
        }

        /// <inheritdoc/>
        public void Write(ReadOnlySpan<byte> data, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException($"Write is not supported by {nameof(ObjectLogChunkReader<TStoreFunctions>)}");

        /// <inheritdoc/>
        public void FlushAndReset(CancellationToken cancellationToken = default)
            => throw new InvalidOperationException($"FlushAndReset is not supported by {nameof(ObjectLogChunkReader<TStoreFunctions>)}");

        static void EnsureCapacity<T>(ref T[] array, int length, int preserve = 0)
        {
            if (array is not null && array.Length >= length)
                return;
            var replacement = ArrayPool<T>.Shared.Rent(length);
            if (array is not null)
            {
                if (preserve > 0)
                    array.AsSpan(0, preserve).CopyTo(replacement);
                ArrayPool<T>.Shared.Return(array);
            }
            array = replacement;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void ThrowCorrupt(string detail)
            => throw new TsavoriteException($"Corrupt object-log record at {topAddress}: {detail}");

        /// <inheritdoc/>
        public void Dispose()
        {
            if (disposed)
                return;
            disposed = true;

            var localMemoryStream = Interlocked.Exchange(ref pinnedMemoryStream, null);
            if (localMemoryStream is not null)
            {
                valueObjectSerializer?.EndDeserialize();
                localMemoryStream.Dispose();
            }
            ReturnArray(ref topBuffer);
            ReturnArray(ref chunkBuffer);
            ReturnArray(ref entryAddresses);
            ReturnArray(ref entryLengths);
        }

        static void ReturnArray<T>(ref T[] array)
        {
            if (array is not null)
            {
                ArrayPool<T>.Shared.Return(array);
                array = null;
            }
        }
    }
}