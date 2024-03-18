// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Output that encapsulates sync stack output (via SpanByte) and async heap output (via IMemoryOwner)
    /// </summary>
    public unsafe struct SpanByteAndMemory : IHeapConvertible
    {
        /// <summary>
        /// Stack output as SpanByte
        /// </summary>
        public SpanByte SpanByte;

        /// <summary>
        /// Heap output as IMemoryOwner
        /// </summary>
        public IMemoryOwner<byte> Memory;

        /// <summary>
        /// Constructor using given SpanByte
        /// </summary>
        /// <param name="spanByte"></param>
        public SpanByteAndMemory(SpanByte spanByte)
        {
            if (spanByte.Serialized) throw new Exception("Cannot create new SpanByteAndMemory using serialized SpanByte");
            SpanByte = spanByte;
            Memory = default;
        }

        /// <summary>
        /// Constructor using SpanByte at given (fixed) pointer, of given length
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
        public ReadOnlySpan<byte> AsReadOnlySpan() => IsSpanByte ? SpanByte.AsReadOnlySpan() : Memory.Memory.Slice(0, Length).Span;

        /// <summary>
        /// As a span of the contained data. Use this when you have already tested IsSpanByte.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsMemoryReadOnlySpan()
        {
            Debug.Assert(!IsSpanByte, "Cannot call AsMemoryReadOnlySpan when IsSpanByte");
            return Memory.Memory.Slice(0, Length).Span;
        }

        /// <summary>
        /// View a fixed Span&lt;byte&gt; as a SpanByteAndMemory
        /// </summary>
        /// <param name="span"></param>
        /// <returns></returns>
        public static SpanByteAndMemory FromFixedSpan(Span<byte> span)
        {
            return new SpanByteAndMemory { SpanByte = SpanByte.FromFixedSpan(span) };
        }

        /// <summary>
        /// Convert to be used on heap (IMemoryOwner)
        /// </summary>
        public void ConvertToHeap() { SpanByte.Invalid = true; }

        /// <summary>
        /// Is it allocated as SpanByte (on stack)?
        /// </summary>
        public bool IsSpanByte => !SpanByte.Invalid;
    }
}