// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Tsavorite.core
{
    /// <summary>
    /// Span{byte} static utility functions for Span{byte} and ReadOnlySpan{byte}.
    /// </summary>
    public static unsafe class SpanByte
    {
        /// <summary>
        /// Create a Span{byte} around a stack variable.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="stackVar"/> MUST be non-movable, such as on the stack.
        /// </remarks>
        public static unsafe Span<byte> FromPinnedVariable<T>(ref T stackVar) where T : unmanaged
            => new(Unsafe.AsPointer(ref stackVar), Unsafe.SizeOf<T>());

        /// <summary>
        /// Create a SpanByte around a pinned memory <paramref name="pointer"/> of given <paramref name="length"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="pointer"/> MUST point to pinned memory.
        /// </remarks>
        public static Span<byte> FromPinnedPointer(byte* pointer, int length) => new(pointer, length);

        /// <summary>
        /// Create a SpanByte around a pinned memory <paramref name="pointer"/> whose first sizeof(int) bytes are the length (i.e. serialized form).
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="pointer"/> MUST point to pinned memory.
        /// </remarks>
        public static Span<byte> FromLengthPrefixedPinnedPointer(byte* pointer) => new(pointer + sizeof(int), *(int*)pointer);

        /// <summary>Total size, including length prefix, of a Span</summary>
        /// <remarks>This must be a methods instead of a property due to extension limitations</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TotalSize(this ReadOnlySpan<byte> span) => sizeof(int) + span.Length;

        /// <summary>Total size, including length prefix, of a Span</summary>
        /// <remarks>This must be a methods instead of a property due to extension limitations</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TotalSize(this Span<byte> span) => sizeof(int) + span.Length;

        /// <summary>Copy to given <see cref="SpanByteAndMemory"/>, using the Span{byte} if possible, else allocating from <paramref name="memoryPool"/></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyTo(this ReadOnlySpan<byte> src, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            if (dst.IsSpanByte)
            {
                if (dst.Length >= src.Length)
                {
                    dst.Length = src.Length;
                    src.CopyTo(dst.SpanByte.Span);
                    return;
                }
                dst.ConvertToHeap();
            }

            dst.Memory = memoryPool.Rent(src.Length);
            dst.Length = src.Length;
            src.CopyTo(dst.MemorySpan);
        }

        /// <summary>Copy to given <see cref="SpanByteAndMemory"/>, using the Span{byte} if possible, else allocating from <paramref name="memoryPool"/></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CopyTo(this Span<byte> src, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
            => ((ReadOnlySpan<byte>)src).CopyTo(ref dst, memoryPool);

        /// <summary>
        /// Unchecked Unsafe cast to a different type; for speed, it does not do the checking for "contains references" etc. that 
        /// <see cref="MemoryMarshal.Cast{TFrom, TTo}(ReadOnlySpan{TFrom})"/> does.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ReadOnlySpan<TTo> UncheckedCast<TTo>(this ReadOnlySpan<byte> src)
            => MemoryMarshal.CreateReadOnlySpan(ref Unsafe.As<byte, TTo>(ref MemoryMarshal.GetReference(src)), src.Length / Unsafe.SizeOf<TTo>());

        /// <summary>
        /// Copy serialized version to specified memory location
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="destination"/> MUST point to pinned memory of at least source.TotalSize().
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SerializeTo(this ReadOnlySpan<byte> source, byte* destination, int destinationSize)
        {
            *(int*)destination = source.Length;
            source.CopyTo(new Span<byte>(destination + sizeof(int), destinationSize - sizeof(int)));
        }

        /// <summary>
        /// Copy serialized version to specified memory location
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="destination"/> MUST point to pinned memory of at least source.TotalSize().
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void SerializeTo(this ReadOnlySpan<byte> source, Span<byte> destination)
        {
            if (destination.Length < source.Length + sizeof(int))
                throw new ArgumentException($"Destination length {destination.Length} is less than source length {source.Length} + sizeof(int)");
            Unsafe.As<byte, int>(ref destination[0]) = source.Length;
            source.CopyTo(destination.Slice(sizeof(int)));
        }

        /// <summary>Length-limited string representation of a Span</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ToShortString(this ReadOnlySpan<byte> span, int maxLen = 20)
        {
            var len = Math.Min(span.Length, maxLen);
            StringBuilder sb = new();
            for (var ii = 0; ii < len; ++ii)
            {
                if (ii > 0 && ii % 4 == 0)
                    _ = sb.Append(' ');
                _ = sb.Append(span[ii].ToString("x2"));
            }
            _ = sb.Append(span.Length > maxLen ? '+' : '~');
            return sb.ToString();
        }

        /// <summary>Length-limited string representation of a Span</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static string ToShortString(this Span<byte> span, int maxLen = 20)
            => ToShortString((ReadOnlySpan<byte>)span, maxLen);
    }
}