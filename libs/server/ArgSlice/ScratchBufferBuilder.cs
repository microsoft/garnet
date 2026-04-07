// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// <see cref="ScratchBufferBuilder"/> is responsible for building a single buffer containing data
    /// supplied by sequential calls to CreateArgSlice.
    /// Whenever the current buffer runs out of space, a new buffer is allocated and the previous buffer's data is the copied over.
    /// The previous allocated buffers are then potentially GCed so any <see cref="ArgSlice"/>s returned prior to any calls to
    /// CreateArgSlice may be pointing to non-allocated space.
    ///
    /// The builder is meant to be called from a single-threaded context (i.e. one builder per session).
    /// Each call to CreateArgSlice will copy the data to the current or new buffer that could contain the data in its entirety,
    /// so rewinding the <see cref="ArgSlice"/> (i.e. releasing the memory) should be called in reverse order to assignment.
    /// 
    /// Note: Use <see cref="ScratchBufferAllocator"/> if you do not need all data to remain in a continuous chunk of memory
    /// and you do not want previously returned <see cref="ArgSlice"/> structs to potentially point to non-allocated memory.
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
        /// Tracks number of outstanding ArgSlice views that have not been rewound.
        /// Used to detect invalid scratch buffer expansion while views are active.
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
        public bool RewindScratchBuffer(ref ArgSlice slice)
        {
            if (slice.ptr + slice.Length == scratchBufferHead + scratchBufferOffset)
            {
                scratchBufferOffset -= slice.Length;
                slice = default; // invalidate the given ArgSlice
#if DEBUG
                outstandingSlices--;
#endif
                return true;
            }
            return false;
        }

        /// <summary>
        /// Resets scratch buffer offset to the specified offset.
        /// </summary>
        /// <param name="offset">Offset to reset to</param>
        /// <returns>True if successful, else false</returns>
        public bool ResetScratchBuffer(int offset)
        {
            if (offset < 0 || offset > scratchBufferOffset)
                return false;

            scratchBufferOffset = offset;
#if DEBUG
            outstandingSlices = 0;
#endif
            return true;
        }

        /// <summary>
        /// Create ArgSlice in scratch buffer, from given ReadOnlySpan
        /// </summary>
        public ArgSlice CreateArgSlice(ReadOnlySpan<byte> bytes)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0, "Cannot create new ArgSlice while there are outstanding slices");
#endif
            ExpandScratchBufferIfNeeded(bytes.Length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, bytes.Length);
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
        public ArgSlice CreateArgSlice(string str)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0, "Cannot create new ArgSlice while there are outstanding slices");
#endif
            int length = Encoding.UTF8.GetByteCount(str);
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
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
        public ArgSlice FormatScratch(int headerSize, ArgSlice arg)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0, "Cannot create new ArgSlice while there are outstanding slices");
#endif
            int length = headerSize + arg.Length;
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
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
        public ArgSlice CreateArgSlice(int length)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0, "Cannot create new ArgSlice while there are outstanding slices");
#endif
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
#if DEBUG
            outstandingSlices++;
#endif
            return retVal;
        }

        /// <summary>
        /// Create an ArgSlice as an offset-length pair, useful when the buffer may be reallocated
        /// and pointer-based ArgSlice would become invalid.
        /// </summary>
        /// <param name="bytes">Input bytes</param>
        /// <returns>Tuple of (Offset, Length) in the scratch buffer</returns>
        public (int Offset, int Length) CreateArgSliceAsOffset(ReadOnlySpan<byte> bytes)
        {
            ExpandScratchBufferIfNeeded(bytes.Length);

            var offset = scratchBufferOffset;
            bytes.CopyTo(new Span<byte>(scratchBufferHead + scratchBufferOffset, bytes.Length));
            scratchBufferOffset += bytes.Length;
            return (offset, bytes.Length);
        }

        /// <summary>
        /// Create an ArgSlice as an offset-length pair of specified length, leaves contents as is.
        /// </summary>
        /// <param name="length">Length of slice</param>
        /// <returns>Tuple of (Offset, Length) in the scratch buffer</returns>
        public (int Offset, int Length) CreateArgSliceAsOffset(int length)
        {
            ExpandScratchBufferIfNeeded(length);

            var offset = scratchBufferOffset;
            scratchBufferOffset += length;
            Debug.Assert(scratchBufferOffset <= scratchBuffer.Length);
            return (offset, length);
        }

        /// <summary>
        /// View remaining scratch space (of specified minimum length) as an ArgSlice.
        /// Does NOT move the offset forward. This is an immediate-use view that
        /// becomes invalid after any subsequent allocation or buffer expansion.
        /// </summary>
        /// <returns>ArgSlice view of remaining space</returns>
        public ArgSlice ViewRemainingArgSlice(int minLength = 0)
        {
            ExpandScratchBufferIfNeeded(minLength);
            return new ArgSlice(scratchBufferHead + scratchBufferOffset, scratchBuffer.Length - scratchBufferOffset);
        }

        /// <summary>
        /// View the full used portion of the scratch buffer as an ArgSlice.
        /// This is an immediate-use view that becomes invalid after any
        /// subsequent allocation or buffer expansion.
        /// </summary>
        public ArgSlice ViewFullArgSlice()
        {
            return new ArgSlice(scratchBufferHead, scratchBufferOffset);
        }

        /// <summary>
        /// Create an ArgSlice that includes a header of specified size, followed by the specified Memory
        /// </summary>
        public ArgSlice FormatScratch(int headerSize, ReadOnlySpan<byte> arg)
        {
#if DEBUG
            Debug.Assert(outstandingSlices == 0, "Cannot create new ArgSlice while there are outstanding slices");
#endif
            int length = headerSize + arg.Length;
            ExpandScratchBufferIfNeeded(length);

            var retVal = new ArgSlice(scratchBufferHead + scratchBufferOffset, length);
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
            Debug.Assert(outstandingSlices == 0, "Cannot expand scratch buffer while there are outstanding slices");
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