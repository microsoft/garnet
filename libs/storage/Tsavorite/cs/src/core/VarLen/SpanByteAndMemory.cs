// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Output that encapsulates sync stack output (via <see cref="core.SpanByte"/>) and async heap output (via IMemoryOwner)
    /// </summary>
    public unsafe struct SpanByteAndMemory
    {
        /// <summary>
        /// Stack output as <see cref="core.SpanByte"/>
        /// </summary>
        public SpanByte SpanByte;

        /// <summary>
        /// Heap output as IMemoryOwner
        /// </summary>
        public IMemoryOwner<byte> Memory;

        /// <summary>
        /// Constructor using given <paramref name="spanByte"/>
        /// </summary>
        public SpanByteAndMemory(SpanByte spanByte)
        {
            if (spanByte.Serialized) throw new Exception("Cannot create new SpanByteAndMemory using serialized SpanByte");
            SpanByte = spanByte;
            Memory = default;
        }

        /// <summary>
        /// Constructor using <see cref="core.SpanByte"/> at given pinned <paramref name="pointer"/>, of given <paramref name="length"/>
        /// </summary>
        public SpanByteAndMemory(void* pointer, int length)
        {
            SpanByte = new SpanByte(length, (IntPtr)pointer);
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
        public readonly bool IsSpanByte => !SpanByte.Invalid;

        /// <summary>
        /// Constructor using given IMemoryOwner
        /// </summary>
        public SpanByteAndMemory(IMemoryOwner<byte> memory)
        {
            SpanByte = default;
            SpanByte.Invalid = true;
            Memory = memory;
        }

        /// <summary>
        /// Constructor using given IMemoryOwner and length
        /// </summary>
        public SpanByteAndMemory(IMemoryOwner<byte> memory, int length)
        {
            SpanByte = default;
            SpanByte.Invalid = true;
            Memory = memory;
            SpanByte.Length = length;
        }

        /// <summary>
        /// As a span of the contained data. Use this when you haven't tested <see cref="IsSpanByte"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan() => IsSpanByte ? SpanByte.AsReadOnlySpan() : Memory.Memory.Span.Slice(0, Length);

        /// <summary>
        /// As a span of the contained data. Use this when you haven't tested <see cref="IsSpanByte"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan() => IsSpanByte ? SpanByte.AsSpan() : Memory.Memory.Span.Slice(0, Length);

        /// <summary>
        /// As a span of the contained data. Use this when you have already tested <see cref="IsSpanByte"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly ReadOnlySpan<byte> AsMemoryReadOnlySpan()
        {
            Debug.Assert(!IsSpanByte, "Cannot call AsMemoryReadOnlySpan when IsSpanByte");
            return Memory.Memory.Span.Slice(0, Length);
        }

        /// <summary>
        /// Copy from the passed ReadOnlySpan{byte}. Use this when you have not tested <see cref="IsSpanByte"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyFrom(ReadOnlySpan<byte> srcSpan, MemoryPool<byte> memoryPool)
        {
            if (IsSpanByte)
            {
                if (srcSpan.Length < Length)
                {
                    srcSpan.CopyTo(SpanByte.AsSpan());
                    Length = srcSpan.Length;
                    return;
                }
                ConvertToHeap();
            }

            Length = srcSpan.Length;
            Memory = memoryPool.Rent(srcSpan.Length);
            srcSpan.CopyTo(Memory.Memory.Span);
        }

        /// <summary>
        /// Create a <see cref="SpanByteAndMemory"/> from pinned <paramref name="span"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="span"/> MUST point to pinned memory.
        /// </remarks>
        public static SpanByteAndMemory FromPinnedSpan(ReadOnlySpan<byte> span) => new(SpanByte.FromPinnedSpan(span));

        /// <summary>
        /// Convert to be used on heap (IMemoryOwner)
        /// </summary>
        public void ConvertToHeap() { SpanByte.Invalid = true; }
    }
}