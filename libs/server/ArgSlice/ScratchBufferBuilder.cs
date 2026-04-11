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

        /// <summary>Current offset in scratch buffer</summary>
        internal int ScratchBufferOffset => scratchBufferOffset;

        public ScratchBufferBuilder()
        {
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
            scratchBufferOffset = (int)(ptr - scratchBufferHead);

            while (!RespWriteUtils.TryWriteBulkString(cmd, ref ptr, scratchBufferHead + scratchBuffer.Length))
            {
                ExpandScratchBuffer(scratchBuffer.Length + 1);
                ptr = scratchBufferHead + scratchBufferOffset;
            }
            scratchBufferOffset = (int)(ptr - scratchBufferHead);
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

            scratchBufferOffset = (int)(ptr - scratchBufferHead);
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

            scratchBufferOffset = (int)(ptr - scratchBufferHead);
        }

        void ExpandScratchBufferIfNeeded(int newLength)
        {
            if (scratchBuffer == null || newLength > scratchBuffer.Length - scratchBufferOffset)
                ExpandScratchBuffer(scratchBufferOffset + newLength);
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