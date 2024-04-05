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
    public unsafe struct SpanByteAndMemory : IHeapConvertible
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
        /// <param name="spanByte"></param>
        public SpanByteAndMemory(SpanByte spanByte)
        {
            if (spanByte.Serialized) throw new Exception("Cannot create new SpanByteAndMemory using serialized SpanByte");
            SpanByte = spanByte;
            Memory = default;
        }

        /// <summary>
        /// Constructor using <see cref="core.SpanByte"/> at given pinned pointer, of given length
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
            get => SpanByte.Length;
            set => SpanByte.Length = value;
        }

        /// <summary>
        /// Constructor using given IMemoryOwner
        /// </summary>
        /// <param name="memory"></param>
        public SpanByteAndMemory(IMemoryOwner<byte> memory)
        {
            SpanByte = default;
            SpanByte.Invalid = true;
            Memory = memory;
        }

        /// <summary>
        /// Constructor using given IMemoryOwner and length
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="length"></param>
        public SpanByteAndMemory(IMemoryOwner<byte> memory, int length)
        {
            SpanByte = default;
            SpanByte.Invalid = true;
            Memory = memory;
            SpanByte.Length = length;
        }

        /// <summary>
        /// As a span of the contained data. Use this when you haven't tested IsSpanByte.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan() => IsSpanByte ? SpanByte.AsReadOnlySpan() : Memory.Memory.Span.Slice(0, Length);

        /// <summary>
        /// As a span of the contained data. Use this when you have already tested IsSpanByte.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsMemoryReadOnlySpan()
        {
            Debug.Assert(!IsSpanByte, "Cannot call AsMemoryReadOnlySpan when IsSpanByte");
            return Memory.Memory.Span.Slice(0, Length);
        }

        /// <summary>
        /// Create a <see cref="SpanByteAndMemory"/> from pinned <paramref name="span"/>.
        /// </summary>
        public static SpanByteAndMemory FromPinnedSpan(ReadOnlySpan<byte> span) => new(SpanByte.FromPinnedSpan(span));

        /// <summary>
        /// Convert to be used on heap (IMemoryOwner)
        /// </summary>
        public void ConvertToHeap() { SpanByte.Invalid = true; }

        /// <summary>
        /// Is it allocated as <see cref="SpanByte"/> (on stack)?
        /// </summary>
        public bool IsSpanByte => !SpanByte.Invalid;
    }
}