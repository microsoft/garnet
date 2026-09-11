// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// <see cref="ScratchBufferBuilder"/> is responsible for building a single buffer containing data
    /// supplied by sequential calls to CreateArgSlice.
    /// Whenever the current buffer runs out of space, a new buffer is allocated and the previous buffer's data is the copied over.
    /// The previous allocated buffers are then potentially GCed so any <see cref="PinnedSpanByte"/>s returned prior to any calls to
    /// CreateArgSlice may be pointing to non-allocated space.
    ///
    /// The builder is meant to be called from a single-threaded context (i.e. one builder per session).
    /// Each call to CreateArgSlice will copy the data to the current or new buffer that could contain the data in its entirety,
    /// so rewinding the <see cref="PinnedSpanByte"/> (i.e. releasing the memory) should be called in reverse order to assignment.
    /// 
    /// Note: Use <see cref="ScratchBufferAllocator"/> if you do not need all data to remain in a continuous chunk of memory
    /// and you do not want previously returned <see cref="PinnedSpanByte"/> structs to potentially point to non-allocated memory.
    /// </summary>
    public sealed unsafe class ScratchBufferBuilder
    {
        /// <summary>
        /// Session-local scratch buffer to hold temporary arguments in transactions and GarnetApi
        /// </summary>
        byte[] scratchBuffer;

        /// <summary>
        /// Pointer to head of scratch buffer
        /// </summary>
        byte* scratchBufferHead;

        /// <summary>
        /// Current offset in scratch buffer
        /// </summary>
        int scratchBufferOffset;

#if DEBUG
        /// <summary>
        /// Number of outstanding PinnedSpanByte slices that have been created but not rewound.
        /// Used to detect unsafe multi-alloc patterns where buffer expansion could invalidate earlier pointers.
        /// </summary>
        int outstandingSlices;
#endif

        /// <summary>
        /// Largest number of bytes requested since the last <see cref="Reset"/>, used to decide whether
        /// an over-sized buffer is still earning its keep.
        /// </summary>
        int batchHighWater;

        /// <summary>
        /// Governs release of a buffer that grew to serve one unusually large request.
        /// </summary>
        BufferShrinkPolicy shrinkPolicy;

        /// <summary>Current offset in scratch buffer</summary>
        internal int ScratchBufferOffset => scratchBufferOffset;

        /// <summary>Current capacity of the backing buffer.</summary>
        internal int ScratchBufferCapacity => scratchBuffer?.Length ?? 0;

        /// <summary>
        /// Creates a <see cref="ScratchBufferBuilder"/>.
        /// </summary>
        /// <param name="maxRetainedCapacity">
        /// Capacity retained indefinitely across resets. A buffer that grew beyond this to serve a large
        /// request is released once the session stops needing it, so one large command does not permanently
        /// enlarge the session. Defaults to <see cref="BufferShrinkPolicy.Unbounded"/> (grow forever).
        /// </param>
        /// <param name="shrinkHysteresis">Consecutive resets without needing the extra capacity before releasing.</param>
        public ScratchBufferBuilder(int maxRetainedCapacity = BufferShrinkPolicy.Unbounded,
            int shrinkHysteresis = BufferShrinkPolicy.DefaultHysteresis)
        {
            this.shrinkPolicy = new BufferShrinkPolicy(maxRetainedCapacity, shrinkHysteresis);
        }

        /// <summary>
        /// Reset scratch buffer - loses all ArgSlice instances created on the scratch buffer
        /// </summary>
        public void Reset()
        {
            scratchBufferOffset = 0;
#if DEBUG
            outstandingSlices = 0;
#endif
            if (shrinkPolicy.ShouldShrink(scratchBuffer?.Length ?? 0, batchHighWater))
            {
                // Reallocate at the baseline rather than releasing outright: callers such as
                // WriteArgument dereference scratchBufferHead without a null check, relying on the
                // buffer having been established by an earlier call. Shrinking to a smaller live
                // buffer keeps that invariant exactly as any expansion would.
                scratchBuffer = GC.AllocateArray<byte>(shrinkPolicy.MaxRetainedCapacity, pinned: true);
                scratchBufferHead = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(scratchBuffer));
            }

            batchHighWater = 0;
        }

        /// <summary>
        /// Return the full buffer managed by this <see cref="ScratchBufferBuilder"/>.
        /// </summary>
        public Span<byte> FullBuffer()
        => scratchBuffer;

        /// <summary>
        /// Rewind (pop) the last entry of scratch buffer (rewinding the current scratch buffer offset),
        /// if it contains the given ArgSlice
        /// </summary>
        public bool RewindScratchBuffer(PinnedSpanByte slice)
        {
            if (slice.ptr + slice.Length == scratchBufferHead + scratchBufferOffset)
            {
                scratchBufferOffset -= slice.Length;
#if DEBUG
                outstandingSlices--;
#endif
                slice = default; // invalidate the given ArgSlice
                return true;
            }
            return false;
        }

        /// <summary>
        /// Create an arg slice in scratch buffer, from given ReadOnlySpan, returning the
        /// offset and length instead of a PinnedSpanByte. Safe for multiple calls without
        /// rewind — use <see cref="ViewFullArgSlice"/> to resolve offsets later.
        /// </summary>
        public (int Offset, int Length) CreateArgSliceAsOffset(ReadOnlySpan<byte> bytes)
        {
            ExpandScratchBufferIfNeeded(bytes.Length);

            var offset = scratchBufferOffset;
            var dest = new Span<byte>(scratchBufferHead + scratchBufferOffset, bytes.Length);
            bytes.CopyTo(dest);
            scratchBufferOffset += bytes.Length;
            return (offset, bytes.Length);
        }

        /// <summary>
        /// Create ArgSlice in scratch buffer, from given ReadOnlySpan
        /// </summary>
        public PinnedSpanByte CreateArgSlice(ReadOnlySpan<byte> bytes)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0,
                "ScratchBufferBuilder already has an outstanding slice. " +
                "Rewind or reset before creating a new one, or use CreateArgSliceAsOffset, " +
                "or use ScratchBufferAllocator for slices that must coexist.");
#endif
            ExpandScratchBufferIfNeeded(bytes.Length);

            var retVal = PinnedSpanByte.FromPinnedPointer(scratchBufferHead + scratchBufferOffset, bytes.Length);
            bytes.CopyTo(retVal.Span);
            scratchBufferOffset += bytes.Length;
#if DEBUG
            outstandingSlices++;
#endif
            return retVal;
        }

        /// <summary>
        /// Shift the scratch buffer offset
        /// </summary>
        /// <param name="length"></param>
        public void MoveOffset(int length)
        {
            scratchBufferOffset += length;
        }

        /// <summary>
        /// Create ArgSlice in UTF8 format in scratch buffer, from given string
        /// </summary>
        public PinnedSpanByte CreateArgSlice(string str)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0,
                "ScratchBufferBuilder already has an outstanding slice. " +
                "Rewind or reset before creating a new one, or use ScratchBufferAllocator.");
#endif
            int length = Encoding.UTF8.GetByteCount(str);
            ExpandScratchBufferIfNeeded(length);

            var retVal = PinnedSpanByte.FromPinnedPointer(scratchBufferHead + scratchBufferOffset, length);
            Encoding.UTF8.GetBytes(str, retVal.Span);
            scratchBufferOffset += length;
#if DEBUG
            outstandingSlices++;
#endif
            return retVal;
        }

        public ReadOnlySpan<byte> UTF8EncodeString(string str)
        {
            // We'll always need AT LEAST this many bytes
            ExpandScratchBufferIfNeeded(str.Length);

            var space = FullBuffer()[scratchBufferOffset..];

            // Attempt to fit in the existing buffer first
            if (!Encoding.UTF8.TryGetBytes(str, space, out var written))
            {
                // If that fails, figure out exactly how much space we need
                var neededBytes = Encoding.UTF8.GetByteCount(str);
                ExpandScratchBufferIfNeeded(neededBytes);

                space = FullBuffer()[scratchBufferOffset..];
                written = Encoding.UTF8.GetBytes(str, space);
            }

            return space[..written];
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by the specified ArgSlice (arg)
        /// </summary>
        public PinnedSpanByte FormatScratch(int headerSize, PinnedSpanByte arg)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0,
                "ScratchBufferBuilder already has an outstanding slice. " +
                "Rewind or reset before creating a new one, or use ScratchBufferAllocator.");
#endif
            int length = headerSize + arg.Length;
            ExpandScratchBufferIfNeeded(length);

            var retVal = PinnedSpanByte.FromPinnedPointer(scratchBufferHead + scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            byte* ptr = scratchBufferHead + scratchBufferOffset + headerSize;
            arg.ReadOnlySpan.CopyTo(new Span<byte>(ptr, arg.Length));

            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
#if DEBUG
            outstandingSlices++;
#endif
            return retVal;
        }

        /// <summary>
        /// Create an ArgSlice of specified length, leaves contents as is
        /// </summary>
        public PinnedSpanByte CreateArgSlice(int length)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0,
                "ScratchBufferBuilder already has an outstanding slice. " +
                "Rewind or reset before creating a new one, or use ScratchBufferAllocator.");
#endif
            ExpandScratchBufferIfNeeded(length);

            var retVal = PinnedSpanByte.FromPinnedPointer(scratchBufferHead + scratchBufferOffset, length);
            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
#if DEBUG
            outstandingSlices++;
#endif
            return retVal;
        }

        /// <summary>
        /// View remaining scratch space (of specified minimum length) as a PinnedSpanByte.
        /// Does NOT move the offset forward. The returned value is an immediate-use view
        /// that may be invalidated by any subsequent allocation or expansion — do not store
        /// or return it. Use <see cref="MoveOffset"/> to claim space after writing.
        /// </summary>
        public PinnedSpanByte ViewRemainingArgSlice(int minLength = 0)
        {
            ExpandScratchBufferIfNeeded(minLength);
            return PinnedSpanByte.FromPinnedPointer(scratchBufferHead + scratchBufferOffset, scratchBuffer.Length - scratchBufferOffset);
        }

        /// <summary>
        /// View the full scratch buffer contents (up to current offset) as a PinnedSpanByte.
        /// The returned value is an immediate-use view that may be invalidated by any
        /// subsequent allocation or expansion — do not store or return it.
        /// </summary>
        public PinnedSpanByte ViewFullArgSlice()
        {
            return PinnedSpanByte.FromPinnedPointer(scratchBufferHead, scratchBufferOffset);
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by the specified Memory
        /// </summary>
        public PinnedSpanByte FormatScratch(int headerSize, ReadOnlySpan<byte> arg)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0,
                "ScratchBufferBuilder already has an outstanding slice. " +
                "Rewind or reset before creating a new one, or use ScratchBufferAllocator.");
#endif
            int length = headerSize + arg.Length;
            ExpandScratchBufferIfNeeded(length);

            var retVal = PinnedSpanByte.FromPinnedPointer(scratchBufferHead + scratchBufferOffset, length);
            retVal.Span[..headerSize].Clear(); // Clear the header

            byte* ptr = scratchBufferHead + scratchBufferOffset + headerSize;
            arg.CopyTo(new Span<byte>(ptr, arg.Length));

            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
#if DEBUG
            outstandingSlices++;
#endif
            return retVal;
        }

        /// <summary>
        /// Start a RESP array to hold a command and arguments.
        /// 
        /// Fill it with <paramref name="argCount"/> calls to <see cref="WriteNullArgument"/> and/or <see cref="WriteArgument(ReadOnlySpan{byte})"/>.
        /// </summary>
        public void StartCommand(ReadOnlySpan<byte> cmd, int argCount)
        {
            if (scratchBuffer == null)
                ExpandScratchBuffer(64);

            var ptr = scratchBufferHead + scratchBufferOffset;

            while (!RespWriteUtils.TryWriteArrayLength(argCount + 1, ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }
            AdvanceOffset((int)(ptr - scratchBufferHead));

            while (!RespWriteUtils.TryWriteBulkString(cmd, ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }
            AdvanceOffset((int)(ptr - scratchBufferHead));
        }

        /// <summary>
        /// Use to fill a RESP array with arguments after a call to <see cref="StartCommand(ReadOnlySpan{byte}, int)"/>.
        /// </summary>
        public void WriteNullArgument()
        {
            var ptr = scratchBufferHead + scratchBufferOffset;

            while (!RespWriteUtils.TryWriteNull(ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }

            AdvanceOffset((int)(ptr - scratchBufferHead));
        }

        /// <summary>
        /// Use to fill a RESP array with arguments after a call to <see cref="StartCommand(ReadOnlySpan{byte}, int)"/>.
        /// </summary>
        public void WriteArgument(ReadOnlySpan<byte> arg)
        {
            var ptr = scratchBufferHead + scratchBufferOffset;

            while (!RespWriteUtils.TryWriteBulkString(arg, ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }

            AdvanceOffset((int)(ptr - scratchBufferHead));
        }

        /// <summary>
        /// Publishes an offset reached by writing straight through a pointer, keeping the batch's demand
        /// high-water in step. Demand is otherwise only observed in <see cref="ExpandScratchBuffer"/>, which
        /// runs only when a write does not fit -- so once the buffer is large enough, a batch that keeps
        /// filling it would report no demand at all and the shrink policy would release it every time.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AdvanceOffset(int newOffset)
        {
            scratchBufferOffset = newOffset;
            if (newOffset > batchHighWater) batchHighWater = newOffset;
        }

        void ExpandScratchBufferIfNeeded(int newLength)
        {
            var needed = scratchBufferOffset + newLength;
            if (needed > batchHighWater) batchHighWater = needed;

            if (scratchBuffer == null || newLength > scratchBuffer.Length - scratchBufferOffset)
                ExpandScratchBuffer(needed);
        }

        void ExpandScratchBuffer(int newLength, int? copyLengthOverride = null)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0,
                "ScratchBufferBuilder is expanding with outstanding slices. " +
                "Previously returned PinnedSpanByte values will be invalidated. " +
                "Use ScratchBufferAllocator for slices that must remain valid across allocations, " +
                "or use a single CreateArgSlice and partition the buffer manually.");
#endif
            if (newLength > batchHighWater) batchHighWater = newLength;

            if (newLength < 64) newLength = 64;
            else newLength = (int)BitOperations.RoundUpToPowerOf2((uint)newLength + 1);

            var _scratchBuffer = GC.AllocateArray<byte>(newLength, true);
            var _scratchBufferHead = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(_scratchBuffer));

            var copyLength = copyLengthOverride ?? scratchBufferOffset;
            if (copyLength > 0)
            {
                new ReadOnlySpan<byte>(scratchBufferHead, copyLength).CopyTo(new Span<byte>(_scratchBufferHead, copyLength));
            }
            scratchBuffer = _scratchBuffer;
            scratchBufferHead = _scratchBufferHead;
        }

        /// <summary>
        /// Force backing buffer to grow.
        /// 
        /// <paramref name="copyLengthOverride"/> provides a way to force a chunk at the start of the
        /// previous buffer be copied into the new buffer, even if this <see cref="ScratchBufferBuilder"/>
        /// doesn't consider that chunk in use.
        /// </summary>
        public void GrowBuffer(int? copyLengthOverride = null)
        {
            if (scratchBuffer == null)
            {
                ExpandScratchBuffer(64);
            }
            else
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1, copyLengthOverride);
            }
        }
    }
}