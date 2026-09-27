// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// A chunked AOF record reassembled from its chunk records: the parsed non-chunked header fields plus the record's key,
    /// value, and input held in their own buffers. It is fed directly to the replay dispatch (see the
    /// <see cref="AofProcessor"/> <c>ProcessAofRecordInternal(int, ChunkedAccumulator, bool, long)</c> overload) so no contiguous
    /// record image is materialized.
    /// </summary>
    internal sealed unsafe class ChunkedAccumulator
    {
        /// <summary>Operation type (from the first chunk's non-chunked header).</summary>
        public AofEntryType opType;
        /// <summary>The non-chunked header type this record reconstitutes to: <see cref="AofHeaderType.BasicHeader"/> or
        /// <see cref="AofHeaderType.ShardedHeader"/>.</summary>
        public AofHeaderType headerType;
        /// <summary>Session id (for transaction grouping).</summary>
        public int sessionID;
        /// <summary>Store version (for <c>SkipRecord</c> version checks).</summary>
        public long storeVersion;
        /// <summary>Sharded sequence number (0 for basic/single-log records); used for read-consistency updates.</summary>
        public long sequenceNumber;
        /// <summary>Key hash (<c>GarnetLog.HASH(key)</c>), carried from the chunk header (set by the writer for every chunk).</summary>
        public long keyHash;

        /// <summary>Whether this op carries a key component (every chunked record does; tracked for symmetry with value/input).</summary>
        public bool hasKey;
        /// <summary>Whether this op carries a value component (Upsert shapes).</summary>
        public bool hasValue;
        /// <summary>Whether this op carries a trailing raw input component (Upsert-with-input / RMW shapes).</summary>
        public bool hasInput;
        /// <summary>Whether the value is a streamed object (accumulated as chunks) vs a pre-sized overflow span.</summary>
        public bool isObjectValue;

        /// <summary>Key buffer, pre-allocated to the header's full key length.</summary>
        public byte[] key;
        /// <summary>Bytes of <see cref="key"/> filled so far (equals the full key length once the record is complete).</summary>
        public int keyOffset;
        /// <summary>Value buffer for span (overflow) values, pre-allocated to the header's full value length.</summary>
        public byte[] value;
        /// <summary>Bytes of <see cref="value"/> filled so far.</summary>
        public int valueOffset;
        /// <summary>Value chunks for streamed object values (length not known up front), accumulated into pooled buffers and
        /// wrapped as a <see cref="ReadOnlySequence{T}"/> by <see cref="GetValueSequence"/> for streaming deserialize with no
        /// contiguous copy. Released by <see cref="ReturnValueChunks"/> once the value has been deserialized.</summary>
        public PooledChunkList valueChunks;
        /// <summary>Input buffer, pre-allocated to the header's full input length.</summary>
        public byte[] input;
        /// <summary>Bytes of <see cref="input"/> filled so far.</summary>
        public int inputOffset;

        /// <summary>The ordered components of a chunked record, packed (and accumulated) in this order.</summary>
        public enum Component { Key, Value, Input }

        /// <summary>The component currently being accumulated; advanced by <see cref="NextComponent"/> as each one completes.
        /// Initialized to the first present component (see <see cref="FirstComponent"/>).</summary>
        public Component currentComponent;
        /// <summary>True once all present components have been fully accumulated (the record is complete).</summary>
        public bool isComplete;

        /// <summary>Full component lengths from the chunk header (for verification).</summary>
        public uint overflowKeyLength, overflowValueLength, inputLength;

        /// <summary>The reassembled key bytes.</summary>
        public ReadOnlySpan<byte> KeySpan => new(key, 0, keyOffset);
        /// <summary>The reassembled span (overflow) value bytes. Only valid when <see cref="hasValue"/> and not <see cref="isObjectValue"/>.</summary>
        public ReadOnlySpan<byte> ValueSpan => new(value, 0, valueOffset);
        /// <summary>The reassembled serialized input bytes. Only valid when <see cref="hasInput"/>.</summary>
        public ReadOnlySpan<byte> InputSpan => new(input, 0, inputOffset);

        /// <summary>Wrap the streamed object value chunks as a <see cref="ReadOnlySequence{T}"/> (no data copy). Only valid
        /// when the value was accumulated rather than streamed (see <see cref="valueIsMaterialized"/>).</summary>
        public ReadOnlySequence<byte> GetValueSequence() => ReadOnlySequenceBuilder.FromChunks(valueChunks);

        /// <summary>Return the streamed object value's pooled buffers. Call once the value has been deserialized and the
        /// sequence from <see cref="GetValueSequence"/> is no longer referenced.</summary>
        public void ReturnValueChunks() => valueChunks?.Reset();

        /// <summary>The object value deserialized while its chunks arrived, when <see cref="valueIsMaterialized"/>. Ownership
        /// passes to the store on a successful upsert; until then this accumulator owns it (see <see cref="DisposeValue"/>).</summary>
        public IGarnetObject valueObject;

        /// <summary>True when <see cref="valueObject"/> holds the deserialized value, so no byte sequence exists. A separate
        /// flag rather than a null check on <see cref="valueObject"/>, because a deserialized value may legitimately be null
        /// (<c>GarnetObjectType.Null</c>).</summary>
        public bool valueIsMaterialized;

        /// <summary>The in-flight streaming deserializer, while the value is still arriving.</summary>
        internal StreamingObjectValueDeserializer valueStream;

        /// <summary>Signal that the value component is complete and take the deserialized object from the streaming worker.
        /// A no-op when the value was accumulated rather than streamed.</summary>
        public void CompleteValueStream()
        {
            var stream = valueStream;
            if (stream is null)
                return;
            valueStream = null;
            try
            {
                valueObject = stream.Complete();
                valueIsMaterialized = true;
            }
            finally
            {
                stream.Dispose();
            }
        }

        /// <summary>Dispose a materialized value that never reached the store, and abort any in-flight stream. Safe to call
        /// more than once; clears the value so a published object is never disposed twice.</summary>
        public void DisposeValue()
        {
            var stream = valueStream;
            valueStream = null;
            stream?.Dispose();

            var obj = valueObject;
            valueObject = null;
            valueIsMaterialized = false;
            (obj as IDisposable)?.Dispose();

            ReturnValueChunks();
        }

        /// <summary>Relinquish ownership of a materialized value once the store has taken it, so later cleanup does not
        /// dispose an object the store now owns.</summary>
        public void ReleaseValueOwnership()
        {
            valueObject = null;
            valueIsMaterialized = false;
        }

        /// <summary>Verify each component's accumulated length matches the chunk header's declared full length.</summary>
        public void Verify()
        {
            if (keyOffset != overflowKeyLength)
                throw new GarnetException($"Chunked key length mismatch: read {keyOffset}, header {overflowKeyLength}");
            if (hasValue && !isObjectValue && valueOffset != overflowValueLength)
                throw new GarnetException($"Chunked value length mismatch: read {valueOffset}, header {overflowValueLength}");
            if (hasInput && inputOffset != inputLength)
                throw new GarnetException($"Chunked input length mismatch: read {inputOffset}, header {inputLength}");
        }

        /// <summary>The first present component to accumulate; the key for every current op (falls through to value/input
        /// should a future op omit the key).</summary>
        public Component FirstComponent()
            => hasKey ? Component.Key : hasValue ? Component.Value : Component.Input;

        /// <summary>Advance <see cref="currentComponent"/> to the next present component after the current one completes;
        /// sets <see cref="isComplete"/> and returns false once the last present component has been consumed.</summary>
        public bool NextComponent()
        {
            if (currentComponent == Component.Key && hasValue)
            {
                currentComponent = Component.Value;
                return true;
            }
            if (currentComponent != Component.Input && hasInput)
            {
                currentComponent = Component.Input;
                return true;
            }
            isComplete = true;
            return false;
        }
    }

    /// <summary>
    /// Accumulates the chunk records of chunked AOF entries (keyed by <c>AofChunkHeader.objectId</c> = the first chunk's
    /// logicalAddress) into an <see cref="ChunkedAccumulator"/>, returned once all of a logical record's components have arrived.
    /// One instance is used per sublog.
    /// </summary>
    /// <remarks>
    /// The full length of each overflow/span component (key, span value, input) is known up front and stored in the chunk
    /// header, so the reader allocates ONE buffer per such component (on the first chunk) and copies the chunks directly into
    /// it. Streamed object values (whose length is not known up front) are either deserialized as their chunks arrive (when
    /// <c>canStream</c> allows it and a slot is free) or accumulated into pooled buffers. The completed
    /// accumulator is dispatched directly (no contiguous record image).
    /// </remarks>
    internal sealed unsafe class AofChunkedRecordReader
    {
        readonly Dictionary<ulong, ChunkedAccumulator> inProgress = [];
        readonly GarnetObjectSerializer objectSerializer;
        readonly StreamingObjectValueDeserializerLimiter streamLimiter;

        internal AofChunkedRecordReader(GarnetObjectSerializer objectSerializer, StreamingObjectValueDeserializerLimiter streamLimiter)
        {
            this.objectSerializer = objectSerializer;
            this.streamLimiter = streamLimiter;
        }

        /// <summary>Abort every partially-accumulated record, disposing any materialized value and aborting any in-flight
        /// stream. Called when replay ends or fails with records still in progress, which means a truncated log.</summary>
        internal void AbortInProgress()
        {
            foreach (var pending in inProgress.Values)
                pending.DisposeValue();
            inProgress.Clear();
        }

        /// <summary>Number of records still awaiting chunks; non-zero at the end of a clean replay means a truncated log.</summary>
        internal int InProgressCount => inProgress.Count;

        /// <summary>
        /// Accumulate a chunk record (<paramref name="ptr"/> points at the chunk header, <paramref name="length"/> is the entry
        /// content length) into its <see cref="ChunkedAccumulator"/>. A record may pack multiple component segments; all are read here
        /// into pre-sized buffers (allocated once, on the first chunk, from the header's full component lengths). Returns true
        /// when the logical record is complete, with <paramref name="acc"/> set to the verified accumulator whose ownership
        /// passes to the caller (it is removed from the in-progress map); otherwise false with <paramref name="acc"/> null.
        /// </summary>
        /// <param name="ptr">Pointer to the chunk header.</param>
        /// <param name="length">Entry content length.</param>
        /// <param name="acc">The completed accumulator, when this chunk completes the record.</param>
        /// <param name="canStream">Whether this call may block to feed a streaming deserializer. False when the caller holds
        /// the log epoch, or is driven by a data source that the blocked thread itself must service (replication replay), in
        /// which case the value is accumulated instead.</param>
        internal bool ReadChunk(byte* ptr, int length, out ChunkedAccumulator acc, bool canStream = false)
        {
            var header = *(AofHeader*)ptr;
            var headerType = header.HeaderType;
            var opType = header.opType;

            ref var chunkHeader = ref AofHeader.GetChunkedHeaderRef(ptr);
            var objectId = chunkHeader.objectId;

            var chunkHeaderSize = headerType == AofHeaderType.ShardedChunkHeader
                ? AofShardedChunkHeader.TotalSize
                : AofBasicChunkHeader.TotalSize;

            // If this objectId is not already being accumulated, create a new accumulator.
            if (!inProgress.TryGetValue(objectId, out acc))
            {
                var hasValue = opType.HasChunkValue();
                var hasInput = opType.HasChunkInput();
                var isObjectValue = opType.HasChunkObjectValue();
                acc = new ChunkedAccumulator
                {
                    opType = opType,
                    keyHash = chunkHeader.keyHash,
                    hasKey = true,
                    hasValue = hasValue,
                    hasInput = hasInput,
                    isObjectValue = isObjectValue,
                    overflowKeyLength = chunkHeader.overflowKeyLength,
                    overflowValueLength = chunkHeader.overflowValueLength,
                    inputLength = chunkHeader.inputLength,
                    key = new byte[chunkHeader.overflowKeyLength],
                };
                acc.currentComponent = acc.FirstComponent();

                // Parse the non-chunked header fields once (session/version, and sequence number for sharded).
                if (headerType == AofHeaderType.ShardedChunkHeader)
                {
                    var sh = ((AofShardedChunkHeader*)ptr)->shardedHeader;
                    Debug.Assert(sh.basicHeader.HeaderType == AofHeaderType.ShardedChunkHeader, "Expected AofHeaderType.ShardedChunkHeader");
                    acc.headerType = AofHeaderType.ShardedHeader;
                    acc.sessionID = sh.basicHeader.sessionID;
                    acc.storeVersion = sh.basicHeader.storeVersion;
                    acc.sequenceNumber = sh.sequenceNumber;
                }
                else
                {
                    var bh = ((AofBasicChunkHeader*)ptr)->basicHeader;
                    Debug.Assert(bh.HeaderType == AofHeaderType.BasicChunkHeader, "Expected AofHeaderType.BasicChunkHeader");
                    acc.headerType = AofHeaderType.BasicHeader;
                    acc.sessionID = bh.sessionID;
                    acc.storeVersion = bh.storeVersion;
                }

                // Pre-size span value / input; an object value's length is not known up front, so it is either deserialized
                // as it arrives (streaming) or accumulated into pooled buffers.
                if (hasValue)
                {
                    if (isObjectValue)
                    {
                        // Streaming is only offered when the caller can afford to block (see canStream) and a slot is free.
                        // Admission is non-blocking by design: waiting here would stall the very thread that feeds the
                        // in-flight values, so a full pool simply falls through to accumulation.
                        if (canStream && objectSerializer is not null && streamLimiter is not null && streamLimiter.TryAcquire())
                            acc.valueStream = new StreamingObjectValueDeserializer(objectSerializer, streamLimiter);
                        else
                        {
                            acc.valueChunks = new PooledChunkList();
                            _ = System.Threading.Interlocked.Increment(ref StreamingObjectValueDeserializer.TotalAccumulated);
                        }
                    }
                    else
                        acc.value = new byte[chunkHeader.overflowValueLength];
                }
                // TODOperf: like a streamed object value (accumulated as a chunk list and exposed via GetValueSequence), a
                // large input could be exposed as a ReadOnlySequence over its chunks and deserialized as a stream, rather than
                // accumulated into one contiguous byte[] here. Rare (only very large inputs, e.g. a multi-database APPEND).
                if (hasInput)
                    acc.input = new byte[chunkHeader.inputLength];
                inProgress[objectId] = acc;
            }

            // A completed record is removed from the in-progress map, so an accumulator we are adding a chunk to must be
            // incomplete; a complete one here means a spurious/duplicate chunk for an already-finished record.
            if (acc.isComplete)
                throw new GarnetException($"Received a chunk for an already-complete record (objectId {objectId})");

            // Read every packed chunk in this record. Each chunk: [4-byte prefix: dataLen | continue-bit][data]. When a
            // chunk's continue-bit is clear, its component is complete and we advance to the next component. The prefix is
            // read whole or not at all: once fewer than sizeof(int) bytes remain, any tail is padding (the writer never
            // splits a prefix across a chunk boundary — see WriteOneRecord), and the deferred prefix opens the next record.
            var payload = ptr + chunkHeaderSize;
            var chunkRegion = length - chunkHeaderSize;
            var off = 0;
            while (off + sizeof(int) <= chunkRegion && !acc.isComplete)
            {
                var prefix = *(int*)(payload + off);
                off += sizeof(int);
                var more = (prefix & ChunkedRecordConstants.ContinuationFlag) != 0;
                var dataLen = prefix & ~ChunkedRecordConstants.ContinuationFlag;
                // Guard against a corrupt/truncated prefix: the segment must fit in this entry's remaining chunk
                // region, otherwise AppendChunk would read past the entry payload (an OOB read of the unmanaged buffer).
                if (dataLen > chunkRegion - off)
                    throw new GarnetException($"Corrupt AOF chunk: segment length {dataLen} exceeds {chunkRegion - off} remaining bytes in entry");
                if (dataLen > 0)
                    AppendChunk(acc, payload + off, dataLen);
                off += dataLen;
                if (!more)
                {
                    // The value's final segment clears the continue-flag, which is the only signal that the (length-unknown)
                    // object value is complete. Drive it here rather than from AppendChunk, because a final segment may be
                    // empty and so never reaches AppendChunk.
                    if (acc.currentComponent == ChunkedAccumulator.Component.Value)
                        acc.CompleteValueStream();
                    _ = acc.NextComponent();
                }
            }

            if (!acc.isComplete)
            {
                // More chunks are still to be accumulated for this record, so report incomplete (return false) to the caller.
                acc = null;
                return false;
            }

            // Complete: hand ownership to the caller (remove from the in-progress map) and verify component lengths.
            _ = inProgress.Remove(objectId);
            acc.Verify();
            return true;
        }

        // Copy a chunk's bytes into the current component's pre-sized buffer (or accumulate for a streamed object value).
        static void AppendChunk(ChunkedAccumulator acc, byte* src, int dataLen)
        {
            switch (acc.currentComponent)
            {
                case ChunkedAccumulator.Component.Key:
                    CopyInto(acc.key, ref acc.keyOffset, src, dataLen);
                    break;
                case ChunkedAccumulator.Component.Value:
                    if (acc.isObjectValue)
                    {
                        if (acc.valueStream is not null)
                            acc.valueStream.Append(new ReadOnlySpan<byte>(src, dataLen));
                        else
                            acc.valueChunks.Append(new ReadOnlySpan<byte>(src, dataLen));
                    }
                    else
                        CopyInto(acc.value, ref acc.valueOffset, src, dataLen);
                    break;
                default:
                    CopyInto(acc.input, ref acc.inputOffset, src, dataLen);
                    break;
            }
        }

        static void CopyInto(byte[] dst, ref int off, byte* src, int len)
        {
            if ((long)off + len > dst.Length)
                throw new GarnetException($"Chunked component overflow: writing {len} bytes at offset {off} exceeds buffer length {dst.Length}");
            fixed (byte* dp = dst)
                Buffer.MemoryCopy(src, dp + off, dst.Length - off, len);
            off += len;
        }
    }
}