// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Reassembles a <see cref="Garnet.client.MigrationRecordSpanType.ChunkedLogRecord"/> from its chunk records, routing the
    /// incoming byte stream by component so each out-of-line component lands directly in its final buffer (no intermediate copy).
    /// A record too large for one send buffer is sent as a sequence of chunks (each framed <c>[int chunkLength | continuation]
    /// [chunk bytes]</c>); the payloads concatenate into the serialized record stream:
    /// <c>[inline portion][int keyLen][overflow key][int valueLen][overflow value | object bytes]</c> — each overflow key/value is
    /// preceded by its 4-byte length; an object value is streamed with no prefix. One instance is held per connection
    /// because a record's chunks may span multiple commands.
    /// </summary>
    /// <remarks>
    /// As chunk bytes arrive they are routed by a small state machine keyed off the record's data header (read directly from the
    /// accumulated inline buffer once the fixed header is present): the inline portion is accumulated contiguously; each overflow
    /// key/value is allocated up front (from its 4-byte length prefix) as a single <see cref="OverflowByteArray"/> and populated
    /// directly from the network chunks; an object value (whose length is not known up front, and which may exceed 2 GB, the max
    /// length of a single <c>byte[]</c>) is accumulated as a chunk list and later streamed to the deserializer via a
    /// <see cref="ReadOnlySequence{T}"/>. The completed pieces are assembled by
    /// <see cref="DiskLogRecord.CompleteDeserializeChunkedRecord"/> (out-of-line components) or <see cref="DiskLogRecord.Deserialize"/>
    /// (a fully-inline record).
    /// </remarks>
    internal sealed class ChunkedRecordReassembler
    {
        // The component the router is currently consuming bytes for.
        enum Phase
        {
            // Accumulating the inline portion (first the fixed header to learn the layout, then the rest of the inline bytes).
            Inline,
            // Reading a 4-byte overflow key/value length prefix (may be split across chunks).
            KeyLengthPrefix,
            ValueLengthPrefix,
            // Copying overflow key/value bytes directly into the pre-allocated OverflowByteArray.
            KeyData,
            ValueData,
            // Accumulating streamed object-value chunks (runs to the record's last chunk).
            ObjectData,
            // The whole record has been reassembled.
            Complete
        }

        Phase phase = Phase.Inline;

        // Inline portion: accumulated contiguously. inlineSize is -1 until the fixed header is read.
        byte[] inlineBuffer = new byte[256];
        int inlineFilled;
        int inlineSize = -1;

        // The record's data header, read directly from the accumulated inline buffer (RecordInfo followed by RecordDataHeader).
        // Valid once inlineSize >= 0; its public bool properties give the component kinds, so they are not cached as fields.
        RecordDataHeader RecordHeader => MemoryMarshal.Read<RecordDataHeader>(inlineBuffer.AsSpan(RecordInfo.Size));

        // Overflow key/value: single up-front allocation populated directly from the chunks.
        OverflowByteArray keyOverflow;
        int keyLength, keyFilled;
        OverflowByteArray valueOverflow;
        int valueLength, valueFilled;

        // Object value: accumulated as chunks (length not known up front; may exceed 2 GB).
        readonly List<byte[]> objectValueChunks = [];
        long objectValueLength;

        // Staging for a 4-byte length prefix that may arrive split across chunks.
        readonly byte[] prefixBuffer = new byte[sizeof(int)];
        int prefixFilled;

        /// <summary>
        /// Route one chunk's payload into the current component. Returns true when the record is complete
        /// (<paramref name="moreChunksFollow"/> is false), after which the reassembled pieces are exposed via the accessors below
        /// and the caller must <see cref="Reset"/> before the next record.
        /// </summary>
        public bool Append(ReadOnlySpan<byte> chunk, bool moreChunksFollow)
        {
            Process(chunk);

            if (moreChunksFollow)
                return false;

            // The record's last chunk has arrived. An object value has no length prefix, so its bytes run to exactly here.
            if (phase == Phase.ObjectData)
                phase = Phase.Complete;
            if (phase != Phase.Complete)
                throw new GarnetException($"Chunked record ended mid-component (phase {phase})");
            return true;
        }

        // Drive the state machine over one chunk's bytes, routing each byte to the component the current phase is filling.
        // A case returns (rather than advancing) when it needs more bytes than this chunk holds; the phase and partial fill
        // counters persist on this instance so the next Append (possibly a later command) resumes where this one left off.
        void Process(ReadOnlySpan<byte> data)
        {
            while (!data.IsEmpty)
            {
                switch (phase)
                {
                    case Phase.Inline:
                        FillInline(ref data);
                        break;
                    case Phase.KeyLengthPrefix:
                        // Wait for the full 4-byte length before allocating; TryReadLengthPrefix buffers a split prefix.
                        if (!TryReadLengthPrefix(ref data, out keyLength))
                            return;
                        keyOverflow = OverflowByteArray.AllocateData(keyLength);
                        keyFilled = 0;
                        phase = Phase.KeyData;
                        break;
                    case Phase.KeyData:
                        if (FillOverflow(ref data, keyOverflow, keyLength, ref keyFilled))
                            phase = AfterKey();
                        break;
                    case Phase.ValueLengthPrefix:
                        if (!TryReadLengthPrefix(ref data, out valueLength))
                            return;
                        valueOverflow = OverflowByteArray.AllocateData(valueLength);
                        valueFilled = 0;
                        phase = Phase.ValueData;
                        break;
                    case Phase.ValueData:
                        if (FillOverflow(ref data, valueOverflow, valueLength, ref valueFilled))
                            phase = Phase.Complete;
                        break;
                    case Phase.ObjectData:
                        // All remaining bytes are object-value bytes; accumulate and track the total for the RDH length update.
                        objectValueChunks.Add(data.ToArray());
                        objectValueLength += data.Length;
                        data = default;
                        break;
                    default: // Phase.Complete
                        throw new GarnetException("Received extra bytes after a chunked record was already complete");
                }
            }
        }

        // Accumulate the inline portion: first the fixed header (to learn its size), then the remainder.
        void FillInline(ref ReadOnlySpan<byte> data)
        {
            // inlineSize is not yet known: this is the start of a new record, so nothing has been accumulated. The fixed header
            // (RecordInfo + RDH) always arrives whole in a record's first chunk, so we can read it in one shot and learn the
            // total inline size. The component kinds are then read on demand from RecordHeader (in the inline buffer).
            if (inlineSize < 0)
            {
                var headerSize = DiskLogRecord.ChunkedRecordHeaderSize;
                Debug.Assert(inlineFilled == 0, "the fixed header always arrives whole in a record's first chunk");
                var headerTake = Math.Min(headerSize, data.Length);
                EnsureInlineCapacity(headerTake);
                data.Slice(0, headerTake).CopyTo(inlineBuffer.AsSpan(inlineFilled));
                inlineFilled += headerTake;
                data = data.Slice(headerTake);
                if (inlineFilled < headerSize)
                    return; // Header split across chunks (not expected); wait for the rest before reading the inline size.

                inlineSize = DiskLogRecord.GetChunkedRecordInlineSize(inlineBuffer.AsSpan(0, inlineFilled));
                EnsureInlineCapacity(inlineSize);
            }

            // Fill the rest of the inline portion.
            var take = Math.Min(inlineSize - inlineFilled, data.Length);
            data.Slice(0, take).CopyTo(inlineBuffer.AsSpan(inlineFilled));
            inlineFilled += take;
            data = data.Slice(take);
            if (inlineFilled == inlineSize)
                phase = AfterInline();
        }

        // Transition after the inline portion is complete, based on which out-of-line components follow (read from RecordHeader).
        Phase AfterInline()
        {
            var header = RecordHeader;
            if (header.RecordIsInline)
                return Phase.Complete;
            if (header.KeyIsOverflow)
                return Phase.KeyLengthPrefix;
            if (header.ValueIsOverflow)
                return Phase.ValueLengthPrefix;
            if (header.ValueIsObject)
                return Phase.ObjectData;
            return Phase.Complete; // Non-inline with inline key and inline value (nothing out of line); should not occur.
        }

        // Transition after the overflow key is complete, based on what value (if any) follows.
        Phase AfterKey()
        {
            var header = RecordHeader;
            if (header.ValueIsOverflow)
                return Phase.ValueLengthPrefix;
            if (header.ValueIsObject)
                return Phase.ObjectData;
            return Phase.Complete; // Inline value (part of the inline portion).
        }

        // Copy overflow bytes straight into the pre-allocated OverflowByteArray; returns true once the component is full.
        static bool FillOverflow(ref ReadOnlySpan<byte> data, OverflowByteArray overflow, int fullLength, ref int filled)
        {
            var take = Math.Min(fullLength - filled, data.Length);
            data.Slice(0, take).CopyTo(overflow.AsSpan(filled, take));
            filled += take;
            data = data.Slice(take);
            return filled == fullLength;
        }

        // Read a 4-byte little-endian length prefix, buffering across chunks when it arrives split.
        bool TryReadLengthPrefix(ref ReadOnlySpan<byte> data, out int value)
        {
            // Fast path: the whole prefix is present in this chunk (the common case) — read it directly, no staging.
            if (prefixFilled == 0 && data.Length >= sizeof(int))
            {
                value = BinaryPrimitives.ReadInt32LittleEndian(data);
                data = data.Slice(sizeof(int));
                return true;
            }

            // Slow path: the prefix is split across chunks — stage bytes into prefixBuffer until all 4 have arrived.
            var take = Math.Min(sizeof(int) - prefixFilled, data.Length);
            data.Slice(0, take).CopyTo(prefixBuffer.AsSpan(prefixFilled));
            prefixFilled += take;
            data = data.Slice(take);
            if (prefixFilled < sizeof(int))
            {
                value = 0;
                return false;
            }
            value = BinaryPrimitives.ReadInt32LittleEndian(prefixBuffer);
            prefixFilled = 0;
            return true;
        }

        void EnsureInlineCapacity(int size)
        {
            if (inlineBuffer.Length >= size)
                return;
            var newBuffer = new byte[Math.Max(size, inlineBuffer.Length * 2)];
            Array.Copy(inlineBuffer, newBuffer, inlineFilled);
            inlineBuffer = newBuffer;
        }

        /// <summary>True if the reassembled record is fully inline (the whole record is <see cref="InlineBuffer"/>).</summary>
        public bool RecordIsInline => RecordHeader.RecordIsInline;
        /// <summary>True if the reassembled record has a streamed object value (see <see cref="ObjectValueSequence"/>).</summary>
        public bool IsObjectValue => RecordHeader.ValueIsObject;

        /// <summary>The record's inline portion buffer (valid bytes are <c>[0, <see cref="InlineSize"/>)</c>).</summary>
        public byte[] InlineBuffer => inlineBuffer;
        /// <summary>Length of the record's inline portion in <see cref="InlineBuffer"/>.</summary>
        public int InlineSize => inlineSize;

        /// <summary>The pre-populated overflow key (valid only when the record header marks the key overflow).</summary>
        public OverflowByteArray KeyOverflow => keyOverflow;
        /// <summary>The pre-populated overflow value (valid only when the record header marks the value overflow).</summary>
        public OverflowByteArray ValueOverflow => valueOverflow;

        /// <summary>Actual overflow key length (0 if the key is inline); used for the RDH length update on completion.</summary>
        public int KeyLength => keyLength;
        /// <summary>Actual overflow value or serialized object length (0 if the value is inline); used for the RDH length update.</summary>
        public long ValueLength => RecordHeader.ValueIsObject ? objectValueLength : valueLength;

        /// <summary>Wrap the streamed object-value chunks as a <see cref="ReadOnlySequence{T}"/> (no data copy).</summary>
        public ReadOnlySequence<byte> ObjectValueSequence() => ReadOnlySequenceBuilder.FromChunks(objectValueChunks);

        /// <summary>Reset for the next record (keeps the inline-buffer capacity and chunk-list capacity for reuse).</summary>
        public void Reset()
        {
            phase = Phase.Inline;
            inlineFilled = 0;
            inlineSize = -1;
            keyOverflow = default;
            keyLength = keyFilled = 0;
            valueOverflow = default;
            valueLength = valueFilled = 0;
            objectValueChunks.Clear();
            objectValueLength = 0;
            prefixFilled = 0;
        }
    }
}