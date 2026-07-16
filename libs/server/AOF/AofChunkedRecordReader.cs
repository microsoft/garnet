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
        /// <summary>Value chunks for streamed object values (length not known up front); wrapped as a <see cref="ReadOnlySequence{T}"/>
        /// by <see cref="GetValueSequence"/> for streaming deserialize with no contiguous copy.</summary>
        public List<byte[]> valueChunks;
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

        /// <summary>Wrap the streamed object value chunks as a <see cref="ReadOnlySequence{T}"/> (no data copy).</summary>
        public ReadOnlySequence<byte> GetValueSequence()
        {
            if (valueChunks is null || valueChunks.Count == 0)
                return ReadOnlySequence<byte>.Empty;
            // Common case: a single chunk holds the whole value — wrap that buffer directly, with no ChunkSegment allocation.
            if (valueChunks.Count == 1)
                return new ReadOnlySequence<byte>(valueChunks[0]);
            ChunkSegment first = null, last = null;
            foreach (var chunk in valueChunks)
            {
                last = new ChunkSegment(chunk, last);
                first ??= last;
            }
            return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
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

        sealed class ChunkSegment : ReadOnlySequenceSegment<byte>
        {
            public ChunkSegment(byte[] array, ChunkSegment previous)
            {
                Memory = array;
                if (previous is not null)
                {
                    previous.Next = this;
                    RunningIndex = previous.RunningIndex + previous.Memory.Length;
                }
            }
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
    /// it. Streamed object values (whose length is not known up front) are accumulated as a chunk list. The completed
    /// accumulator is dispatched directly (no contiguous record image).
    /// </remarks>
    internal sealed unsafe class AofChunkedRecordReader
    {
        readonly Dictionary<ulong, ChunkedAccumulator> inProgress = [];

        /// <summary>
        /// Accumulate a chunk record (<paramref name="ptr"/> points at the chunk header, <paramref name="length"/> is the entry
        /// content length) into its <see cref="ChunkedAccumulator"/>. A record may pack multiple component segments; all are read here
        /// into pre-sized buffers (allocated once, on the first chunk, from the header's full component lengths). Returns true
        /// when the logical record is complete, with <paramref name="acc"/> set to the verified accumulator whose ownership
        /// passes to the caller (it is removed from the in-progress map); otherwise false with <paramref name="acc"/> null.
        /// </summary>
        internal bool ReadChunk(byte* ptr, int length, out ChunkedAccumulator acc)
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

                // Pre-size span value / input; accumulate streamed object values (length unknown up front).
                if (hasValue)
                {
                    if (isObjectValue)
                        acc.valueChunks = [];
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
                var more = (prefix & TsavoriteLog.ChunkContinuesFlag) != 0;
                var dataLen = prefix & ~TsavoriteLog.ChunkContinuesFlag;
                if (dataLen > 0)
                    AppendChunk(acc, payload + off, dataLen);
                off += dataLen;
                if (!more)
                    _ = acc.NextComponent();
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
                        acc.valueChunks.Add(new ReadOnlySpan<byte>(src, dataLen).ToArray());
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