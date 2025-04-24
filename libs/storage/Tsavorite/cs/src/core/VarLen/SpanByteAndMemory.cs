// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Output that encapsulates sync stack output (via <see cref="core.PinnedSpanByte"/>) and async heap output (via IMemoryOwner)
    /// </summary>
    public unsafe struct SpanByteAndMemory
    {
        /// <summary>
        /// Stack output as <see cref="core.SpanByte"/>
        /// </summary>
        public PinnedSpanByte SpanByte;

        /// <summary>
        /// Heap output as IMemoryOwner
        /// </summary>
        public IMemoryOwner<byte> Memory;

        /// <summary>
        /// Constructor using given <paramref name="spanByte"/>
        /// </summary>
        public SpanByteAndMemory(PinnedSpanByte spanByte)
        {
            SpanByte = spanByte;
            Memory = default;
        }

        /// <summary>
        /// Get length
        /// </summary>
        public int Length
        {
            readonly get => SpanByte.Length;
            set => SpanByte.Length = value;
        }

        /// <summary>
        /// Is it allocated as <see cref="core.SpanByte"/> (on stack)?
        /// </summary>
        public readonly bool IsSpanByte => SpanByte.IsValid;

        /// <summary>
        /// Constructor using given IMemoryOwner
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SpanByteAndMemory(IMemoryOwner<byte> memory)
        {
            SpanByte.Invalidate();
            Memory = memory;
        }

        /// <summary>
        /// Constructor using given IMemoryOwner and length
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SpanByteAndMemory(IMemoryOwner<byte> memory, int length)
        {
            SpanByte.Invalidate();
            Memory = memory;
            SpanByte.Length = length;
        }

        /// <summary>
        /// As a span of the contained data. Use this when you haven't tested <see cref="IsSpanByte"/>.
        /// </summary>
        public ReadOnlySpan<byte> ReadOnlySpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsSpanByte ? SpanByte.ReadOnlySpan : Memory.Memory.Span.Slice(0, Length); }
        }

        /// <summary>
        /// As a span of the contained data. Use this when you haven't tested <see cref="IsSpanByte"/>.
        /// </summary>
        public ReadOnlySpan<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsSpanByte ? SpanByte.Span : Memory.Memory.Span.Slice(0, Length); }
        }

        /// <summary>
        /// As a ReadOnlySpan of the contained data. Use this when you have already tested <see cref="IsSpanByte"/>.
        /// </summary>
        public readonly ReadOnlySpan<byte> MemoryReadOnlySpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(!IsSpanByte, "Cannot call AsMemoryReadOnlySpan when IsSpanByte");
                return Memory.Memory.Span.Slice(0, Length);
            }
        }

        /// <summary>
        /// As a Span of the contained data. Use this when you have already tested <see cref="IsSpanByte"/>.
        /// </summary>
        public readonly ReadOnlySpan<byte> MemorySpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(!IsSpanByte, "Cannot call AsMemoryReadOnlySpan when IsSpanByte");
                return Memory.Memory.Span.Slice(0, Length);
            }
        }

        /// <summary>
        /// Create a <see cref="SpanByteAndMemory"/> from pinned <paramref name="span"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="span"/> MUST point to pinned memory.
        /// </remarks>
        public static SpanByteAndMemory FromPinnedSpan(ReadOnlySpan<byte> span) => new() { SpanByte = PinnedSpanByte.FromPinnedSpan(span), Memory = default };

        /// <summary>
        /// Create a <see cref="SpanByteAndMemory"/> from a given pinned <paramref name="pointer"/>, of given <paramref name="length"/>
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="pointer"/> MUST point to pinned memory.
        /// </remarks>
        public static SpanByteAndMemory FromPinnedPointer(byte* pointer, int length) => new() { SpanByte = PinnedSpanByte.FromPinnedPointer(pointer, length), Memory = default };

        /// <summary>
        /// Convert to be used on heap (IMemoryOwner)
        /// </summary>
        public void ConvertToHeap() { SpanByte.Invalidate(); }

        /// <summary>
        /// Ensure the required size is available in this structure via the Span or the Memory.
        /// </summary>
        public void EnsureMemorySize(int size, MemoryPool<byte> memoryPool = null)
        {
            if (memoryPool is null)
                memoryPool = MemoryPool<byte>.Shared;

            if (IsSpanByte)
            {
                if (SpanByte.Length >= size)
                    return;
                ConvertToHeap();
            }

            if (Memory is null)
            {
                Memory = memoryPool.Rent(size);
                return;
            }

            if (Memory.Memory.Length >= size)
                return;

            // Reallocate
            Memory.Dispose();
            Memory = null;  // In case the following throws OOM
            Memory = memoryPool.Rent(size);
        }
    }
}