// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Tsavorite.core.Allocator.ObjectSerialization;

namespace Tsavorite.core
{
    // Chunked (large key/value/object) write support. TsavoriteLog is the IChunkedObjectConsumer: it turns the stream of
    // serialized value bytes (plus the record's key and input) into a sequence of chunk records in the log.
    //
    // Wire format:
    //   Each chunk record is a normal log entry: [entry-length prefix (headerSize)][THeader][packed segments].
    //   THeader is the caller's chunk header (e.g. AofBasicChunkHeader). Its constant fields (keyHash, the full component lengths)
    //   are set by the caller up front; this layer only patches the per-chunk ObjectId (the logicalAddress of the FIRST chunk of
    //   this logical record, identical on every chunk so the reader can group them) at a caller-provided byte offset — it never
    //   needs the concrete header layout.
    //   Components are written in order: Key, then Value, then Input. They are PACKED: a single record holds one [int prefix][data]
    //   segment per component that (partly) fits, in order, so key+value+input can share a record. A component whose data does not
    //   fit is split: its last segment in a record sets the high bit of the prefix (continue-flag), and it resumes in the next
    //   record. The value's continue-flag also stays set across a streamed Consume-buffer boundary (its final segment clears it
    //   only on the isComplete drain). The reader reads every segment in a record (bounded by the entry length), advancing the
    //   component order each time it sees a prefix with the continue-flag clear.
    //
    //   Allocation uses TryAllocateRetryNow(partialSlots: MinPartialAllocSize), so a record can fill a page tail and continue on
    //   the next page (page-tail packing). When the tail is too small to split (< MinPartialAllocSize), the whole record moves to
    //   the next page and the zero tail is skipped by the scan (a dummy end-of-page filler is a deferred optimization; see the TODO
    //   in AllocatorBase.HandlePageOverflow).
    public sealed partial class TsavoriteLog : IChunkedObjectConsumer
    {
        const int ChunkContinuesFlag = unchecked((int)0x80000000);

        [ThreadStatic] static ChunkWriteState chunkWriteState;

        /// <summary>
        /// Writes a chunk record's header: copies the caller's template (which already holds the constant fields — keyHash and the
        /// component lengths) and patches the per-chunk <c>objectId</c> at a caller-provided offset, so this layer does not need
        /// the concrete header type.
        /// </summary>
        abstract class ChunkHeaderWriter
        {
            public int HeaderSize;
            public abstract unsafe void Write(byte* dest, ulong objectId);
        }

        sealed class ChunkHeaderWriter<THeader> : ChunkHeaderWriter
            where THeader : unmanaged
        {
            THeader template;
            readonly int objectIdOffset;

            public unsafe ChunkHeaderWriter(THeader template, int objectIdOffset)
            {
                this.template = template;
                this.objectIdOffset = objectIdOffset;
                HeaderSize = sizeof(THeader);
            }

            public override unsafe void Write(byte* dest, ulong objectId)
            {
                *(THeader*)dest = template;
                *(ulong*)(dest + objectIdOffset) = objectId;
            }
        }

        sealed class ChunkWriteState
        {
            /// <summary>Writes the (opaque) chunk header template, patching the per-chunk objectId.</summary>
            public ChunkHeaderWriter headerWriter;
            /// <summary>Size of the chunk header (sizeof(THeader)).</summary>
            public int headerFieldsSize;
            /// <summary>Log page size in bytes.</summary>
            public int pageSize;
            /// <summary>Maximum entry-content bytes (chunk header + packed segments) in a single record; 4-aligned so
            /// <c>headerSize + Align(maxContent) &lt;= pageSize</c>.</summary>
            public int maxContent;
            /// <summary>LogicalAddress of the first chunk of this logical record; -1 until the first chunk is allocated.</summary>
            public long firstLogicalAddress = -1;
            /// <summary>The objectId written into every chunk (= <see cref="firstLogicalAddress"/>).</summary>
            public ulong objectId;
            /// <summary>True once the key has been fully written (on the first Consume call).</summary>
            public bool keyDone;
            /// <summary>Whether this record has a value component (Upsert shapes). RMW/Delete have none.</summary>
            public bool writeValue;
            /// <summary>Whether this record has a trailing input component (Upsert-with-input / RMW shapes). Object upserts / deletes have none.</summary>
            public bool writeInput;
        }

        // One component (key, value, or input) being packed into records: its bytes, how much is written, and whether it is
        // "complete" (its final segment clears the continue-flag). The value component's completeness is only known on the
        // isComplete Consume call, so its segments before then carry the continue-flag even at a Consume-buffer boundary.
        unsafe struct CompDesc
        {
            public byte* ptr;
            public int len;
            public int off;
            public bool present;
            public bool complete;
            public bool written;

            public readonly bool Remaining => present && (!written || off < len);
        }

        /// <summary>
        /// Enqueue a chunked record by driving the serializer, which streams the value bytes back into <see cref="Consume{TKey, TInput}"/>.
        /// </summary>
        /// <param name="header">The chunk header written into every chunk record; its constant fields (keyHash, component lengths)
        /// are already set by the caller. Only the per-chunk objectId is patched, at <paramref name="objectIdOffset"/>.</param>
        /// <param name="objectIdOffset">Byte offset of the objectId field within <typeparamref name="THeader"/>.</param>
        /// <param name="serializer">The serializer carrying the value object plus the record's key and input.</param>
        /// <param name="writeInput">Whether a trailing input component is written (false for object upserts, which carry no replayed input).</param>
        /// <param name="firstLogicalAddress">The logicalAddress of the first chunk of the record (also its objectId).</param>
        public unsafe void EnqueueChunkedObject<THeader, TKey, TInput>(THeader header, int objectIdOffset, ChunkedObjectSerializer<TKey, TInput> serializer, bool writeInput, out long firstLogicalAddress)
            where THeader : unmanaged
            where TKey : IKey
            where TInput : IStoreInput
        {
            var state = CreateChunkWriteState(header, objectIdOffset);
            state.writeValue = true;   // an object record always has a (streamed) value component
            state.writeInput = writeInput;
            var prev = chunkWriteState;
            chunkWriteState = state;
            epoch.Resume();
            BeginInflightEnqueue();
            try
            {
                serializer.Serialize();
            }
            finally
            {
                EndInflightEnqueue();
                epoch.Suspend();
                chunkWriteState = prev;
            }

            firstLogicalAddress = state.firstLogicalAddress;
            if (autoCommit)
                Commit();
        }

        /// <summary>
        /// Enqueue a chunked record whose components are already fully in memory as spans (the inline/overflow path). Writes the
        /// key, then (if <paramref name="writeValue"/>) the value, then (if <paramref name="writeInput"/>) the input as chunk
        /// records via a single <see cref="Consume{TKey, TInput}"/> call. The component set must match the op's replay layout.
        /// </summary>
        public unsafe void EnqueueChunkedSpan<THeader, TKey, TInput>(THeader header, int objectIdOffset, TKey key, ReadOnlySpan<byte> value, bool writeValue, ref TInput input, bool writeInput, out long firstLogicalAddress)
            where THeader : unmanaged
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TInput : IStoreInput
        {
            var state = CreateChunkWriteState(header, objectIdOffset);
            state.writeValue = writeValue;
            state.writeInput = writeInput;
            var prev = chunkWriteState;
            chunkWriteState = state;
            epoch.Resume();
            BeginInflightEnqueue();
            try
            {
                _ = Consume(value, isComplete: true, key, ref input);
            }
            finally
            {
                EndInflightEnqueue();
                epoch.Suspend();
                chunkWriteState = prev;
            }

            firstLogicalAddress = state.firstLogicalAddress;
            if (autoCommit)
                Commit();
        }

        unsafe ChunkWriteState CreateChunkWriteState<THeader>(THeader header, int objectIdOffset)
            where THeader : unmanaged
        {
            if (commitNum == long.MaxValue)
                throw new TsavoriteException("Attempting to enqueue into a completed log");

            var writer = new ChunkHeaderWriter<THeader>(header, objectIdOffset);

            // A page-tail filler (AllocatorBase.HandlePageOverflow) records the wasted tail in the main-store
            // RecordDataHeader.ValueLength field, and that tail is bounded by MinPartialAllocSize; ensure it fits that field.
            Debug.Assert(MinPartialAllocSize <= (int)RecordDataHeader.kValueLengthLowBitsMask,
                "MinPartialAllocSize must fit the main-store RecordDataHeader.ValueLength field used by page-tail fillers");
            // MinPartialAllocSize must hold a chunk header plus a component's int length prefix so the key length prefix is
            // always written whole in the first chunk (it need not be length-aligned like value/input prefixes).
            Debug.Assert(MinPartialAllocSize >= writer.HeaderSize + sizeof(int),
                "MinPartialAllocSize must hold a chunk header plus a component length prefix");

            var pageSize = (int)allocator.GetPageSize();
            // Max entry content (chunk header + packed segments), 4-aligned so headerSize + Align(maxContent) <= pageSize.
            var maxContent = (pageSize - headerSize) & ~(sizeof(int) - 1);
            if (maxContent <= writer.HeaderSize + sizeof(int))
                throw new TsavoriteException($"Page size {pageSize} is too small for chunked records");

            return new ChunkWriteState
            {
                headerWriter = writer,
                headerFieldsSize = writer.HeaderSize,
                pageSize = pageSize,
                maxContent = maxContent,
            };
        }

        /// <inheritdoc/>
        public unsafe long Consume<TKey, TInput>(ReadOnlySpan<byte> data, bool isComplete, TKey key, ref TInput input)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TInput : IStoreInput
        {
            var state = chunkWriteState ?? throw new TsavoriteException("Chunked Consume called outside of EnqueueChunkedObject");

            // Materialize the input for this call, if any. Input is written only on the final drain (isComplete) for op shapes
            // that carry it. It is materialized to a contiguous buffer so it can be sliced/packed across records (an input may now
            // span pages like any other component; the reader pre-allocates it from the header's inputLength).
            // TODOperf: consider a streaming deserialization in the event of a large parseState, to avoid materializing the whole
            // input here (and pre-allocating it whole on the read side). This would have to be implemented at the *Input level and
            // in ParseState (a streamable serialize/deserialize over the parse-state arguments).
            byte[] inputBytes = null;
            if (isComplete && state.writeInput)
            {
                var inputLen = input.SerializedLength;
                inputBytes = inputLen > 0 ? new byte[inputLen] : [];
                if (inputLen > 0)
                    fixed (byte* ip = inputBytes)
                        _ = input.CopyTo(ip, inputLen);
            }

            var keySpan = state.keyDone ? default : key.KeyBytes;
            var valueSpan = state.writeValue ? data : default;

            fixed (byte* keyPtr = keySpan)
            fixed (byte* valPtr = valueSpan)
            fixed (byte* inpPtr = inputBytes)
            {
                // Components packed in order: Key (first call only), Value (this drain), Input (final drain only). Value's
                // completeness is isComplete (its final segment clears the continue-flag only on the last drain).
                var comp = stackalloc CompDesc[3];
                comp[0] = new CompDesc { ptr = keyPtr, len = keySpan.Length, present = !state.keyDone, complete = true };
                comp[1] = new CompDesc { ptr = valPtr, len = valueSpan.Length, present = state.writeValue, complete = isComplete };
                comp[2] = new CompDesc { ptr = inpPtr, len = inputBytes?.Length ?? 0, present = isComplete && state.writeInput, complete = true };

                while (AnyRemaining(comp))
                    WriteOneRecord(state, comp);
            }

            state.keyDone = true;

            // We consume the entire value buffer this call (the value component is split across records as needed).
            return data.Length;
        }

        static unsafe bool AnyRemaining(CompDesc* comp)
            => comp[0].Remaining || comp[1].Remaining || comp[2].Remaining;

        // Allocate one chunk record (via partialSlots so it can fill a page tail) and pack as many remaining component segments
        // into it as fit, advancing each component's offset. A component that does not fully fit continues in the next record
        // (its last segment in this record carries the continue-flag).
        unsafe void WriteOneRecord(ChunkWriteState state, CompDesc* comp)
        {
            // Desired content = chunk header + a [int prefix][data] segment for each remaining component, capped at one page.
            var desired = state.headerFieldsSize;
            for (var i = 0; i < 3; i++)
            {
                if (!comp[i].Remaining)
                    continue;
                var segTotal = sizeof(int) + (comp[i].len - comp[i].off);
                if (desired + segTotal >= state.maxContent)
                {
                    desired = state.maxContent;
                    break;
                }
                desired += segTotal;
            }

            var numSlots = headerSize + Align(desired);
            ValidateAllocatedLength(numSlots);

            var logicalAddress = AllocateBlockPartial(numSlots, MinPartialAllocSize, out var allocatedLength);
            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);

            if (state.firstLogicalAddress < 0)
            {
                state.firstLogicalAddress = logicalAddress;
                state.objectId = (ulong)logicalAddress;
            }

            // Content capacity actually granted (a partialSlots return may be smaller than numSlots — a page tail).
            var segRegion = allocatedLength - headerSize - state.headerFieldsSize;
            var payload = physicalAddress + headerSize + state.headerFieldsSize;
            var off = 0;
            for (var i = 0; i < 3; i++)
            {
                if (!comp[i].Remaining)
                    continue;
                if (segRegion - off < sizeof(int))
                    break; // no room for another segment prefix; remaining components continue in the next record

                var segCap = segRegion - off - sizeof(int);
                var rem = comp[i].len - comp[i].off;
                var segLen = rem < segCap ? rem : segCap;
                var more = (comp[i].off + segLen < comp[i].len) || !comp[i].complete;

                *(int*)(payload + off) = segLen | (more ? ChunkContinuesFlag : 0);
                off += sizeof(int);
                if (segLen > 0)
                {
                    Buffer.MemoryCopy(comp[i].ptr + comp[i].off, payload + off, segLen, segLen);
                    off += segLen;
                }
                comp[i].off += segLen;
                comp[i].written = true;

                if (off >= segRegion)
                    break; // record full
            }

            // Entry content = chunk header + the segment bytes actually written. The scan's Align() absorbs any <4-byte tail gap;
            // the reader bounds its segment scan by this content length.
            var contentLen = state.headerFieldsSize + off;
            state.headerWriter.Write(physicalAddress + headerSize, state.objectId);
            SetHeader(contentLen, physicalAddress);
        }

        // Allocate a block that may fill a page tail: TryAllocateRetryNow with partialSlots returns the granted length (which may
        // be less than numSlots when the allocation is split across a page boundary). Mirrors AllocateBlock's RETRY_LATER waiting.
        long AllocateBlockPartial(int numSlots, int partialSlots, out int length)
        {
            if (commitNum == long.MaxValue)
                throw new TsavoriteException("Attempting to enqueue into a completed log");

            while (true)
            {
                var flushEvent = allocator.flushEvent;
                if (allocator.TryAllocateRetryNow(numSlots, partialSlots, out var logicalAddress, out var len))
                {
                    length = (int)len;
                    return logicalAddress;
                }

                Debug.Assert(logicalAddress == 0);
                EndInflightEnqueue();
                epoch.Suspend();
                try
                {
                    if (cannedException != null)
                        throw cannedException;
                    flushEvent.Wait();
                }
                finally
                {
                    epoch.Resume();
                    BeginInflightEnqueue();
                }
            }
        }
    }
}
