// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
    public sealed partial class TsavoriteLog : IChunkedObjectSerializerConsumer
    {
        public const int ChunkContinuesFlag = unchecked((int)0x80000000);

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
            /// <summary>Maximum entry-content bytes (chunk header + packed segments) in a single record; 4-aligned so
            /// <c>headerSize + Align(maxContent) &lt;= pageSize</c>.</summary>
            public int maxContent;
            /// <summary>LogicalAddress of the first chunk of this logical record; -1 until the first chunk is allocated.
            /// Returned to the *Enqueue* caller.</summary>
            public long firstLogicalAddress = -1;
            /// <summary>The objectId written into every chunk (= <see cref="firstLogicalAddress"/>).</summary>
            public ulong objectId;
            /// <summary>True once the key has been fully written (on the first Consume call).</summary>
            public bool keyDone;
            /// <summary>Whether this record has a value component (Upsert shapes). RMW/Delete have none.</summary>
            public bool hasValue;
            /// <summary>Whether this record has a trailing input component (Upsert-with-input / RMW shapes). Object upserts / deletes have none.</summary>
            public bool hasInput;
            /// <summary>The caller's (store) epoch, suspended while blocked on an AOF flush during allocation so store epoch
            /// reclamation is not stalled by this thread; null when the caller holds no store epoch (e.g. replication replay).</summary>
            public IEpochAccessor epochAccessor;
            /// <summary>Pinned buffer holding the materialized input, allocated on demand only when the input must be split
            /// across records (see <see cref="WriteOneRecord"/>); kept rooted here for the pointer's lifetime. Null when the
            /// input was written inline (the common case) or the record has no input.</summary>
            public byte[] materializedInput;
        }

        // One component (key, value, or input) being packed into records: its bytes, how much is written, and whether it is
        // "complete" (its final segment clears the continue-flag). The value component's completeness is only known on the
        // isComplete Consume call, so its segments before then carry the continue-flag even at a Consume-buffer boundary.
        unsafe struct ComponentState
        {
            /// <summary>Pointer to this component's first source-span bytes for the current Consume call (the pinned key,
            /// value-first-span, or input-buffer pointer).</summary>
            public byte* ptr;
            /// <summary>Length of the first source span.</summary>
            public int len;
            /// <summary>Pointer to the optional second source span (the value component's wrapped ring span); unused otherwise.</summary>
            public byte* ptr2;
            /// <summary>Length of the second source span; 0 for single-span components (key, input).</summary>
            public int len2;
            /// <summary>Number of bytes of this component already packed into records so far (advances as segments are written).
            /// This may be in <see cref="ptr2"/>; this is handled by the start position in <see cref="ptr2"/> being calculated by
            /// "<see cref="len"/> - <see cref="offset"/>".</summary>
            public int offset;
            /// <summary>Whether this component participates at all: key only on the first call, value when <c>hasValue</c>,
            /// input only on the final (isComplete) drain when <c>hasInput</c>.</summary>
            public bool isPresent;
            /// <summary>Whether all of this component's bytes are known now, so its final segment clears the continue-flag. The
            /// value component is <c>isComplete</c> only on the final drain; earlier drains flag their last segment to continue.</summary>
            public bool isComplete;
            /// <summary>Whether at least one segment of this component has been emitted; with <see cref="HasRemaining"/> this
            /// distinguishes "not started" from "fully written", so a present zero-length component still emits one segment.</summary>
            public bool writeStarted;
            /// <summary>True for the input component while it is still to be serialized directly from <c>IStoreInput.CopyTo</c>
            /// (no materialized buffer yet). It is written inline when the whole input fits the record being packed (the common
            /// case); otherwise it is materialized on demand into a pinned buffer and packed like any other component. Cleared
            /// once materialized.</summary>
            public bool hasPendingInput;

            /// <summary>Total bytes of this component across both source spans.</summary>
            public readonly int TotalLen => len + len2;
            /// <summary>True while this component still has bytes to pack (or has not emitted its first segment yet).</summary>
            public readonly bool HasRemaining => isPresent && (!writeStarted || offset < TotalLen);
        }

        /// <summary>
        /// Enqueue a chunked object record: construct the streaming serializer and drive it, packing the record's key, the
        /// streamed value, and (optionally) its input into chunk records via <see cref="Consume{TContext, TKey, TInput}"/>.
        /// </summary>
        /// <param name="header">The chunk header written into every chunk record; its constant fields (keyHash, component lengths)
        /// are already set by the caller. Only the per-chunk objectId is patched, at <paramref name="objectIdOffset"/>.</param>
        /// <param name="objectIdOffset">Byte offset of the objectId field within <typeparamref name="THeader"/>.</param>
        /// <param name="key">The record's key.</param>
        /// <param name="input">The record's input.</param>
        /// <param name="objectSerializer">Serializes the value object into the streaming buffer.</param>
        /// <param name="value">The value object to serialize.</param>
        /// <param name="bufferSize">Size of the serializer's circular buffer (the max bytes held at once).</param>
        /// <param name="writeInput">Whether a trailing input component is written (false for object upserts, which carry no replayed input).</param>
        /// <param name="epochAccessor">The caller's (store) epoch, suspended while blocked on an AOF flush during allocation; null when the caller holds no store epoch.</param>
        /// <param name="firstLogicalAddress">The logicalAddress of the first chunk of the record (also its objectId).</param>
        public unsafe void EnqueueChunkedObject<THeader, TInput>(THeader header, int objectIdOffset, in ConditionallyHoistedKey key, ref TInput input, IObjectSerializer<IHeapObject> objectSerializer, IHeapObject value, int bufferSize, bool writeInput, IEpochAccessor epochAccessor, out long firstLogicalAddress)
            where THeader : unmanaged
            where TInput : IStoreInput
        {
            var state = CreateChunkWriteState(header, objectIdOffset);
            state.hasValue = true;   // an object record always has a (streamed) value component
            state.hasInput = writeInput;
            state.epochAccessor = epochAccessor;
            var serializer = new ChunkedObjectSerializer<ChunkWriteState, TInput>(in key, ref input, this, objectSerializer, value, bufferSize);
            epoch.Resume();
            BeginInflightEnqueue();
            try
            {
                serializer.Serialize(state);
            }
            finally
            {
                EndInflightEnqueue();
                epoch.Suspend();
            }

            firstLogicalAddress = state.firstLogicalAddress;
            if (autoCommit)
                Commit();
        }

        /// <summary>
        /// Enqueue a chunked record whose components are already fully in memory as spans (the inline/overflow path). Writes the
        /// key, then (if <paramref name="writeValue"/>) the value, then (if <paramref name="writeInput"/>) the input as chunk
        /// records via a single <c>Consume</c> call. The component set must match the op's replay layout.
        /// </summary>
        public unsafe void EnqueueChunkedSpan<THeader, TKey, TInput>(THeader header, int objectIdOffset, TKey key, ReadOnlySpan<byte> value, bool writeValue, ref TInput input, bool writeInput, IEpochAccessor epochAccessor, out long firstLogicalAddress)
            where THeader : unmanaged
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TInput : IStoreInput
        {
            var state = CreateChunkWriteState(header, objectIdOffset);
            state.hasValue = writeValue;
            state.hasInput = writeInput;
            state.epochAccessor = epochAccessor;
            epoch.Resume();
            BeginInflightEnqueue();
            try
            {
                _ = Consume(value, default, isComplete: true, key, ref input, state);
            }
            finally
            {
                EndInflightEnqueue();
                epoch.Suspend();
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
                maxContent = maxContent,
            };
        }

        /// <summary>Value-only chunk consume (the read side). Not yet implemented; wired up for a future deserialize path.</summary>
        public int Consume<TContext>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isComplete, TContext context)
            => throw new NotImplementedException("Value-only chunk consume (read side) is not yet implemented");

        /// <inheritdoc/>
        public unsafe int Consume<TContext, TKey, TInput>(ReadOnlySpan<byte> first, ReadOnlySpan<byte> second, bool isComplete, TKey key, ref TInput input, TContext context)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TInput : IStoreInput
        {
            var state = (ChunkWriteState)(object)context;

            // Input is written only on the final drain (isComplete) for op shapes that carry it. It is NOT materialized up front:
            // WriteOneRecord serializes it inline via IStoreInput.CopyTo directly into the record when the whole input fits the
            // record being packed (the common case), materializing to a pinned buffer only when it must be split across records.
            // TODOperf: for a very large parseState, consider a streaming serialization (at the *Input level and in ParseState)
            // rather than a single CopyTo; the reader has a matching note where it accumulates the input buffer.
            var inputLen = (isComplete && state.hasInput) ? input.SerializedLength : 0;

            var keySpan = state.keyDone ? default : key.KeyBytes;
            var valueFirst = state.hasValue ? first : default;
            var valueSecond = state.hasValue ? second : default;

            fixed (byte* keyPtr = keySpan)
            fixed (byte* valPtr = valueFirst)
            fixed (byte* valPtr2 = valueSecond)
            {
                // Components packed in order: Key (first call only), Value (this drain — possibly wrapped into two ring spans),
                // Input (final drain only). Value's completeness is isComplete (its final segment clears the continue-flag only
                // on the last drain). Input is "pending" (unmaterialized) so WriteOneRecord can inline it; its length is known.
                var components = stackalloc ComponentState[3];
                components[0] = new ComponentState { ptr = keyPtr, len = keySpan.Length, isPresent = !state.keyDone, isComplete = true };
                components[1] = new ComponentState { ptr = valPtr, len = valueFirst.Length, ptr2 = valPtr2, len2 = valueSecond.Length, isPresent = state.hasValue, isComplete = isComplete };
                components[2] = new ComponentState { len = inputLen, isPresent = isComplete && state.hasInput, isComplete = true, hasPendingInput = true };

                while (AnyRemaining(components))
                    WriteOneRecord(state, components, ref input);
            }

            state.keyDone = true;

            // We consume the entire available value (both ring spans) this call.
            return first.Length + second.Length;
        }

        static unsafe bool AnyRemaining(ComponentState* comp)
            => comp[0].HasRemaining || comp[1].HasRemaining || comp[2].HasRemaining;

        // Copy len bytes from a component starting at srcOff, crossing from its first span (ptr, len) into its second span
        // (ptr2, len2) as needed. Only the value component uses a second span (the wrapped ring); key and input have len2 == 0.
        static unsafe void CopyComponent(ref ComponentState comp, int srcOff, byte* dest, int len)
        {
            if (srcOff < comp.len)
            {
                var fromFirst = Math.Min(len, comp.len - srcOff);
                Buffer.MemoryCopy(comp.ptr + srcOff, dest, fromFirst, fromFirst);
                dest += fromFirst;
                len -= fromFirst;
                srcOff += fromFirst;
            }
            if (len > 0)
            {
                // Note the indexing into the second span: srcOff is the offset into the whole component, so the start of the second span is at (srcOff - comp.len).
                Buffer.MemoryCopy(comp.ptr2 + (srcOff - comp.len), dest, len, len);
            }

            // Caller updates comp.offset.
        }

        // Allocate one chunk record (via partialSlots so it can fill a page tail) and pack as many remaining component segments
        // into it as fit, advancing each component's offset. A component that does not fully fit continues in the next record
        // (its last segment in this record carries the continue-flag). The input component is serialized inline via
        // IStoreInput.CopyTo when the whole input fits this record; otherwise it is materialized on demand and packed like the rest.
        unsafe void WriteOneRecord<TInput>(ChunkWriteState state, ComponentState* comp, ref TInput input)
            where TInput : IStoreInput
        {
            // Accumulate the total content bytes needed for this record
            //  chunk header + a [int prefix][data] segment for each remaining component
            // Capped at one page (state.maxContent).
            var desired = state.headerFieldsSize;
            for (var i = 0; i < 3; i++)
            {
                if (!comp[i].HasRemaining)
                    continue;
                var segTotal = sizeof(int) + (comp[i].TotalLen - comp[i].offset);

                // Cap the desired content at the maxContent that fits in a single record (page-tail packing).
                if (desired + segTotal >= state.maxContent)
                {
                    desired = state.maxContent;
                    break;
                }
                desired += segTotal;
            }

            var numSlots = headerSize + Align(desired);
            ValidateAllocatedLength(numSlots);

            var logicalAddress = AllocateBlockPartial(numSlots, MinPartialAllocSize, state.epochAccessor, out var allocatedLength);
            var physicalAddress = (byte*)allocator.GetPhysicalAddress(logicalAddress);

            if (state.firstLogicalAddress < 0)
            {
                // state.firstLogicalAddress is returned to the *Enqueue* caller.
                state.firstLogicalAddress = logicalAddress;
                state.objectId = (ulong)logicalAddress;
            }

            // Terminology:
            //   segment is a part of a component that fits in a record (a [int prefix][data] pair). A component may be split across multiple segments.
            //   chunkRegion is the region in the record that holds the packed segments.
            // Content capacity actually granted (a partialSlots return may be smaller than numSlots — a page tail).
            var chunkRegion = allocatedLength - headerSize - state.headerFieldsSize;
            var payload = physicalAddress + headerSize + state.headerFieldsSize;
            var chunkOffset = 0;
            for (var i = 0; i < 3; i++)
            {
                if (!comp[i].HasRemaining)
                    continue;

                // A length prefix is written whole or not at all: if there are fewer than sizeof(int) bytes left, this
                // component's prefix is deferred to the start of the next chunk record — never split across the
                // boundary. The reader applies the identical bound (off + sizeof(int) <= chunkRegion in ReadChunk).
                if (chunkRegion - chunkOffset < sizeof(int))
                    break; 
                var segCap = chunkRegion - chunkOffset - sizeof(int);

                // For Input not yet materialized: if the whole input fits into this record then serialize it directly (no temp buffer);
                // otherwise materialize once into a pinned buffer and fall through to the normal split-copy path below.
                if (comp[i].hasPendingInput)
                {
                    var inputLen = comp[i].len;
                    if (inputLen <= segCap)
                    {
                        *(int*)(payload + chunkOffset) = inputLen; // whole input fits: one complete segment (continue-flag clear)
                        chunkOffset += sizeof(int);
                        if (inputLen > 0)
                        {
                            _ = input.CopyTo(payload + chunkOffset, inputLen);
                            chunkOffset += inputLen;
                        }
                        comp[i].offset = inputLen;
                        comp[i].writeStarted = true;

                        if (chunkOffset >= chunkRegion)
                            break; // record is full
                        continue;   // continue to next component if any; currently there won't be, as input is last and has been fully written
                    }

                    // Materialize the input into a pinned buffer so it can be split across records via CopyComponent.
                    // The buffer is rooted in the state and comp[i] will get its ptr which will be processed below.
                    MaterializeInput(state, ref comp[i], ref input);
                }

                var chunkRemaining = comp[i].TotalLen - comp[i].offset;
                var segLen = chunkRemaining < segCap ? chunkRemaining : segCap;
                var needSplit = (comp[i].offset + segLen < comp[i].TotalLen) || !comp[i].isComplete;

                *(int*)(payload + chunkOffset) = segLen | (needSplit ? ChunkContinuesFlag : 0);
                chunkOffset += sizeof(int);
                if (segLen > 0)
                {
                    CopyComponent(ref comp[i], comp[i].offset, payload + chunkOffset, segLen);
                    chunkOffset += segLen;
                }
                comp[i].offset += segLen;
                comp[i].writeStarted = true;

                if (chunkOffset >= chunkRegion)
                    break; // record full
            }

            // Entry content = chunk header + the segment bytes actually written. The scan's Align() absorbs any <4-byte tail gap;
            // the reader bounds its segment scan by this content length.
            var contentLen = state.headerFieldsSize + chunkOffset;
            state.headerWriter.Write(physicalAddress + headerSize, state.objectId);
            SetHeader(contentLen, physicalAddress);
        }

        // Materialize the (not-yet-written) input into a pinned buffer so it can be split across records via CopyComponent. Used
        // only when the input does not fit a single record whole; the buffer is rooted in the state for the pointer's lifetime.
        static unsafe void MaterializeInput<TInput>(ChunkWriteState state, ref ComponentState comp, ref TInput input)
            where TInput : IStoreInput
        {
            // Pinned (Pinned Object Heap) so the raw pointer stays valid across the splitting WriteOneRecord calls (no relocation).
            state.materializedInput = GC.AllocateUninitializedArray<byte>(comp.len, pinned: true);
            comp.ptr = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(state.materializedInput));
            _ = input.CopyTo(comp.ptr, comp.len);
            comp.hasPendingInput = false;
        }

        /// <summary>
        /// Test-only entry point that performs the epoch / in-flight ceremony (normally done by the enqueue caller) around
        /// <see cref="AllocateBlockPartial"/>, so a unit test can drive the partial (page-tail split) vs. cross-page behavior
        /// directly. Passes no store epoch accessor (the split/cross paths do not block on a flush).
        /// </summary>
        internal long AllocateBlockPartialForTest(int numSlots, int partialSlots, out int length)
        {
            epoch.Resume();
            BeginInflightEnqueue();
            try
            {
                return AllocateBlockPartial(numSlots, partialSlots, epochAccessor: null, out length);
            }
            finally
            {
                EndInflightEnqueue();
                epoch.Suspend();
            }
        }

        // Allocate a block that may fill a page tail: TryAllocateRetryNow with partialSlots returns the granted length (which may
        // be less than numSlots when the allocation is split across a page boundary). Mirrors AllocateBlock's RETRY_LATER waiting,
        // including suspending the caller's (store) epoch during the flush wait so store epoch reclamation is not stalled here.
        internal long AllocateBlockPartial(int numSlots, int partialSlots, IEpochAccessor epochAccessor, out int length)
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
                var suspended = epochAccessor?.TrySuspend() ?? false;
                try
                {
                    if (cannedException != null)
                        throw cannedException;
                    flushEvent.Wait();
                }
                finally
                {
                    if (suspended)
                        epochAccessor.Resume();
                    epoch.Resume();
                    BeginInflightEnqueue();
                }
            }
        }
    }
}