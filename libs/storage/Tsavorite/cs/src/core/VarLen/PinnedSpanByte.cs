// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Tsavorite.core
{
    /// <summary>
    /// Represents contiguous region of arbitrary _pinned_ memory.
    /// </summary>
    /// <remarks>
    /// SAFETY: This type is used to represent arguments that are assumed to point to pinned memory.
    /// </remarks>
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public unsafe struct PinnedSpanByte
    {
        public const int Size = 12;

        [FieldOffset(0)]
        public byte* ptr;

        [FieldOffset(8)]
        public int length;

        /// <summary>
        /// Get and set length of ArgSlice. TODO: Replace length and pointer field accesses with properties
        /// </summary>
        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => length;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => length = value;
        }

        /// <summary>
        /// Get pointer to the start of the slice
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly byte* ToPointer() => ptr;

        /// <summary>
        /// Get pointer to the start of the slice
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetPointer(byte *newPtr) => ptr = newPtr;

        /// <summary>
        /// Reset the contained Span
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(byte* newPtr, int newLength)
        {
            ptr = newPtr;
            length = newLength;
        }

        /// <summary>
        /// Total size of the contained span, including the length prefix.
        /// </summary>
        public int TotalSize() => sizeof(int) + length;

        /// <summary>
        /// Set this as invalid; used by <see cref="SpanByteAndMemory"/> to indicate the <see cref="Memory{_byte_}"/> should be used.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Invalidate() => ptr = null;

        /// <summary>
        /// If the pointer is null, this PinnedSpanByte is not valid
        /// </summary>
        public readonly bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ptr != null; }
        }

        /// <summary>
        /// Get slice as ReadOnlySpan
        /// </summary>
        public readonly ReadOnlySpan<byte> ReadOnlySpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new(ptr, length);
        }

        /// <summary>
        /// Get slice as Span
        /// </summary>
        public readonly Span<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return new(ptr, length); }
        }

        /// <summary>
        /// Copies the contents of this slice into a new array.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly byte[] ToArray() => ReadOnlySpan.ToArray();

        /// <summary>
        /// Decodes the contents of this slice as ASCII into a new string.
        /// </summary>
        /// <returns>A string ASCII decoded string from the slice.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override readonly string ToString() => Encoding.ASCII.GetString(ReadOnlySpan);

        /// <summary>
        /// Create a <see cref="PinnedSpanByte"/> from the given <paramref name="span"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="span"/> MUST point to pinned memory.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PinnedSpanByte FromPinnedSpan(ReadOnlySpan<byte> span)
            => FromPinnedPointer((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(span)), span.Length);

        /// <summary>
        /// Create new ArgSlice from given pointer and length
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="ptr"/> MUST point to pinned memory.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PinnedSpanByte FromPinnedPointer(byte* ptr, int length)
            => new() { ptr = ptr, length = length };

        /// <summary>
        /// Check for equality to the provided argSlice
        /// </summary>
        /// <param name="argSlice"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool Equals(PinnedSpanByte argSlice) => argSlice.Span.SequenceEqual(Span);
    }
}