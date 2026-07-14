// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Accumulates the chunk records of chunked AOF entries (keyed by <c>AofChunkHeader.objectId</c> = the first chunk's
    /// logicalAddress) and, once all of a logical record's components have arrived, reconstructs the equivalent non-chunked
    /// record so it can be replayed through the normal <see cref="AofProcessor"/> dispatch. One instance is used per sublog.
    /// </summary>
    /// <remarks>
    /// The full length of each overflow/span component (key, span value, input) is known up front and stored in the chunk header,
    /// so the reader allocates ONE buffer per such component (on the first chunk) and copies the chunks directly into it, verifying
    /// the total matches the header length exactly. Streamed object values (whose length is not known up front) are still
    /// accumulated as a chunk list. Reconstructs a contiguous non-chunked record and re-runs it through
    /// <c>ProcessAofRecordInternal</c>, reusing the existing dispatch.
    /// </remarks>
    internal sealed unsafe class AofChunkedRecordReader
    {
        // Must match the high bit of TsavoriteLog's chunked-write segment length prefix (ChunkContinuesFlag).
        const int ChunkContinuesFlag = unchecked((int)0x80000000);

        sealed class Accumulator
        {
            /// <summary>The non-chunked header (AofHeader or AofShardedHeader) with the chunk bit cleared, from the first chunk.</summary>
            public byte[] nonChunkHeader;
            /// <summary>Whether this op carries a length-prefixed value component (Upsert shapes).</summary>
            public bool hasValue;
            /// <summary>Whether this op carries a trailing raw input component (Upsert-with-input / RMW shapes).</summary>
            public bool hasInput;
            /// <summary>Whether the value is a streamed object (accumulated) vs a pre-sized overflow span.</summary>
            public bool isObjectValue;

            /// <summary>Key buffer, pre-allocated to the header's full key length; chunks are copied in at <see cref="keyOff"/>.</summary>
            public byte[] key;
            public int keyOff;
            /// <summary>Value buffer for span (overflow) values, pre-allocated to the header's full value length.</summary>
            public byte[] value;
            public int valueOff;
            /// <summary>Value chunks for streamed object values (length not known up front).</summary>
            public List<byte[]> valueChunks;
            /// <summary>Input buffer, pre-allocated to the header's full input length.</summary>
            public byte[] input;
            public int inputOff;

            /// <summary>Index into the ordered component set (0 = key, then value/input as present).</summary>
            public int component;
            /// <summary>Number of components expected for this op (1 + hasValue + hasInput).</summary>
            public int expectedComponents;

            /// <summary>Full component lengths from the chunk header (for verification).</summary>
            public uint overflowKeyLength, overflowValueLength, inputLength;
        }

        readonly Dictionary<ulong, Accumulator> inProgress = [];

        /// <summary>
        /// Accumulate a chunk record (<paramref name="ptr"/> points at the chunk header, <paramref name="length"/> is the entry
        /// content length). A record may pack multiple component segments; all are read here into pre-sized buffers (allocated
        /// once, on the first chunk, from the header's full component lengths). Returns true when the logical record is complete
        /// (all of its components have arrived), with <paramref name="objectId"/> set for a subsequent
        /// <see cref="Reconstruct"/> / <see cref="Remove"/>.
        /// </summary>
        public bool AddChunk(byte* ptr, int length, out ulong objectId)
        {
            var header = *(AofHeader*)ptr;
            var headerType = header.HeaderType;
            var opType = header.opType;

            ref var chunkHeader = ref AofHeader.GetChunkedHeaderRef(ptr);
            objectId = chunkHeader.objectId;

            int chunkHeaderSize;
            byte[] nonChunkHeader;
            if (headerType == AofHeaderType.ShardedChunkHeader)
            {
                chunkHeaderSize = AofShardedChunkHeader.TotalSize;
                var h = ((AofShardedChunkHeader*)ptr)->shardedHeader;
                h.basicHeader.HeaderType = AofHeaderType.ShardedHeader;
                nonChunkHeader = ToBytes(&h, sizeof(AofShardedHeader));
            }
            else
            {
                chunkHeaderSize = AofBasicChunkHeader.TotalSize;
                var h = ((AofBasicChunkHeader*)ptr)->basicHeader;
                h.HeaderType = AofHeaderType.BasicHeader;
                nonChunkHeader = ToBytes(&h, sizeof(AofHeader));
            }

            if (!inProgress.TryGetValue(objectId, out var acc))
            {
                var hasValue = opType.HasChunkValue();
                var hasInput = opType.HasChunkInput();
                var isObjectValue = opType.HasChunkObjectValue();
                acc = new Accumulator
                {
                    nonChunkHeader = nonChunkHeader,
                    hasValue = hasValue,
                    hasInput = hasInput,
                    isObjectValue = isObjectValue,
                    expectedComponents = 1 + (hasValue ? 1 : 0) + (hasInput ? 1 : 0),
                    overflowKeyLength = chunkHeader.overflowKeyLength,
                    overflowValueLength = chunkHeader.overflowValueLength,
                    inputLength = chunkHeader.inputLength,
                    key = new byte[chunkHeader.overflowKeyLength],
                };
                // Pre-size span value / input; accumulate streamed object values (length unknown up front).
                if (hasValue)
                {
                    if (isObjectValue) acc.valueChunks = [];
                    else acc.value = new byte[chunkHeader.overflowValueLength];
                }
                if (hasInput)
                    acc.input = new byte[chunkHeader.inputLength];
                inProgress[objectId] = acc;
            }

            // Read every packed segment in this record. Each segment: [4-byte prefix: dataLen | continue-bit][data]. When a
            // segment's continue-bit is clear, its component is complete and we advance to the next component.
            var payload = ptr + chunkHeaderSize;
            var segRegion = length - chunkHeaderSize;
            var off = 0;
            while (off + sizeof(int) <= segRegion && acc.component < acc.expectedComponents)
            {
                var prefix = *(int*)(payload + off);
                off += sizeof(int);
                var more = (prefix & ChunkContinuesFlag) != 0;
                var dataLen = prefix & ~ChunkContinuesFlag;
                if (dataLen > 0)
                    AppendSegment(acc, payload + off, dataLen);
                off += dataLen;
                if (!more)
                    acc.component++;
            }

            return acc.component >= acc.expectedComponents;
        }

        enum Component { Key, Value, Input }

        // The component currently being accumulated, in order: key, then (if present) value, then (if present) input.
        static Component CurrentComponent(Accumulator acc)
        {
            if (acc.component == 0)
                return Component.Key;
            if (acc.hasValue && acc.component == 1)
                return Component.Value;
            return Component.Input;
        }

        // Copy a segment's bytes into the current component's pre-sized buffer (or accumulate for a streamed object value).
        static void AppendSegment(Accumulator acc, byte* src, int dataLen)
        {
            switch (CurrentComponent(acc))
            {
                case Component.Key:
                    CopyInto(acc.key, ref acc.keyOff, src, dataLen);
                    break;
                case Component.Value:
                    if (acc.isObjectValue)
                        acc.valueChunks.Add(new ReadOnlySpan<byte>(src, dataLen).ToArray());
                    else
                        CopyInto(acc.value, ref acc.valueOff, src, dataLen);
                    break;
                default:
                    CopyInto(acc.input, ref acc.inputOff, src, dataLen);
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

        /// <summary>
        /// Reconstruct the completed record for <paramref name="objectId"/> into the op's non-chunked layout:
        /// <c>[header][int keyLen + key]</c>, then (for Upsert shapes) <c>[int valueLen + value]</c>, then (for input shapes)
        /// the raw <c>[input]</c> tail. Verifies that each component's accumulated length matches the header's full length.
        /// </summary>
        public byte[] Reconstruct(ulong objectId, out int recordLength)
        {
            var acc = inProgress[objectId];

            var keyLen = acc.keyOff;
            if (keyLen != acc.overflowKeyLength)
                throw new GarnetException($"Chunked key length mismatch: read {keyLen}, header {acc.overflowKeyLength}");

            var valueLen = 0;
            if (acc.hasValue)
            {
                if (acc.isObjectValue)
                {
                    valueLen = TotalLength(acc.valueChunks);
                }
                else
                {
                    valueLen = acc.valueOff;
                    if (valueLen != acc.overflowValueLength)
                        throw new GarnetException($"Chunked value length mismatch: read {valueLen}, header {acc.overflowValueLength}");
                }
            }

            var inputLen = acc.hasInput ? acc.inputOff : 0;
            if (acc.hasInput && inputLen != acc.inputLength)
                throw new GarnetException($"Chunked input length mismatch: read {inputLen}, header {acc.inputLength}");

            var headerLen = acc.nonChunkHeader.Length;
            recordLength = headerLen + sizeof(int) + keyLen
                + (acc.hasValue ? sizeof(int) + valueLen : 0)
                + (acc.hasInput ? inputLen : 0);
            var record = new byte[recordLength];

            Array.Copy(acc.nonChunkHeader, 0, record, 0, headerLen);
            var offset = headerLen;

            offset = WriteInt(record, offset, keyLen);
            Array.Copy(acc.key, 0, record, offset, keyLen);
            offset += keyLen;

            if (acc.hasValue)
            {
                offset = WriteInt(record, offset, valueLen);
                if (acc.isObjectValue)
                    offset = WriteRawChunks(record, offset, acc.valueChunks);
                else
                {
                    Array.Copy(acc.value, 0, record, offset, valueLen);
                    offset += valueLen;
                }
            }

            if (acc.hasInput)
                Array.Copy(acc.input, 0, record, offset, inputLen);

            return record;
        }

        /// <summary>Discard the accumulator for a completed (or abandoned) record.</summary>
        public void Remove(ulong objectId) => inProgress.Remove(objectId);

        static int TotalLength(List<byte[]> chunks)
        {
            var total = 0;
            foreach (var c in chunks)
                total += c.Length;
            return total;
        }

        static int WriteInt(byte[] record, int offset, int value)
        {
            fixed (byte* rp = record)
                *(int*)(rp + offset) = value;
            return offset + sizeof(int);
        }

        static int WriteRawChunks(byte[] record, int offset, List<byte[]> chunks)
        {
            foreach (var c in chunks)
            {
                Array.Copy(c, 0, record, offset, c.Length);
                offset += c.Length;
            }
            return offset;
        }

        static byte[] ToBytes(void* src, int size)
        {
            var bytes = new byte[size];
            fixed (byte* bp = bytes)
                Buffer.MemoryCopy(src, bp, size, size);
            return bytes;
        }
    }
}
