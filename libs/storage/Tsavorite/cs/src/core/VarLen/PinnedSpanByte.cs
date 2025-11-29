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
    /// Represents contiguous region of arbitrary pinned memory.
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
        /// Get and set length of ArgSlice.
        /// </summary>
        public int Length
        {
            readonly get => length;
            set => length = value;
        }

        /// <summary>Correlates to ReadOnlySpan.IsEmpty</summary>
        public readonly bool IsEmpty => Length == 0;

        /// <summary>
        /// Get pointer to the start of the slice
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly byte* ToPointer() => ptr;

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
        public readonly int TotalSize => sizeof(int) + length;

        /// <summary>
        /// Set this as invalid; used by <see cref="SpanByteAndMemory"/> to indicate the <see cref="Memory{_byte_}"/> should be used.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Invalidate() => ptr = null;

        /// <summary>
        /// If the pointer is null, this PinnedSpanByte is not valid
        /// </summary>
        public readonly bool IsValid => ptr != null;

        /// <summary>
        /// Defines an implicit conversion to a <see cref="ReadOnlySpan{T}"/>
        /// </summary>
        public static implicit operator ReadOnlySpan<byte>(PinnedSpanByte psb) => psb.ReadOnlySpan;

        /// <summary>
        /// Get slice as ReadOnlySpan
        /// </summary>
        public readonly ReadOnlySpan<byte> ReadOnlySpan => new(ptr, length);

        /// <summary>
        /// Get slice as ReadOnlySpan
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly ReadOnlySpan<byte> AsReadOnlySpan(int start) => start <= length ? new(ptr + start, length - start) : throw new ArgumentOutOfRangeException(nameof(start));

        /// <summary>
        /// Get slice as ReadOnlySpan
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly ReadOnlySpan<byte> AsReadOnlySpan(int start, int len) => ((ulong)(uint)start + (uint)len <= (uint)length) ? new(ptr + start, len) : throw new ArgumentOutOfRangeException($"start {nameof(start)} + len {len} exceeds length {length}");

        /// <summary>
        /// Get slice as Span
        /// </summary>
        public readonly Span<byte> Span => new(ptr, length);

        /// <summary>
        /// Get slice as Span
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Span<byte> AsSpan(int start) => start <= length ? new(ptr + start, length - start) : throw new ArgumentOutOfRangeException(nameof(start));

        /// <summary>
        /// Get slice as Span
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Span<byte> AsSpan(int start, int len) => ((ulong)(uint)start + (uint)len <= (uint)length) ? new(ptr + start, len) : throw new ArgumentOutOfRangeException($"start {nameof(start)} + len {len} exceeds length {length}");

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
        public override readonly string ToString() => IsValid ? Encoding.ASCII.GetString(ReadOnlySpan) : $"<invalid>, len {Length}";

        /// <summary>
        /// Create a <see cref="PinnedSpanByte"/> from the given <paramref name="span"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="span"/> MUST point to pinned memory.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PinnedSpanByte FromPinnedSpan(ReadOnlySpan<byte> span) => FromPinnedPointer((byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(span)), span.Length);

        /// <summary>
        /// Create new ArgSlice from given pointer and length
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="ptr"/> MUST point to pinned memory.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PinnedSpanByte FromPinnedPointer(byte* ptr, int length) => new() { ptr = ptr, length = length };

        /// <summary>
        /// Create a SpanByte around a pinned memory <paramref name="pointer"/> whose first sizeof(int) bytes are the length (i.e. serialized form).
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="pointer"/> MUST point to pinned memory.
        /// </remarks>
        public static PinnedSpanByte FromLengthPrefixedPinnedPointer(byte* pointer) => new() { ptr = pointer + sizeof(int), length = *(int*)pointer };

        /// <summary>
        /// Check for equality to the provided argSlice
        /// </summary>
        /// <param name="argSlice"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool Equals(PinnedSpanByte argSlice) => argSlice.Span.SequenceEqual(Span);

        /// <summary>
        /// Copy serialized version to specified memory location
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="destination"/> MUST point to pinned memory of at least <see cref="TotalSize"/> length.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void SerializeTo(byte* destination)
        {
            *(int*)destination = length;
            Buffer.MemoryCopy(ptr, destination + sizeof(int), Length, Length);
        }

        /// <summary>
        /// Copy non-serialized version to specified memory location (do not copy the length prefix space)
        /// </summary>
        public readonly void CopyTo(Span<byte> destination) => ReadOnlySpan.CopyTo(destination);

        /// <summary>
        /// Copy non-serialized version to specified <see cref="SpanByteAndMemory"/> (do not copy the length prefix space)
        /// </summary>
        public readonly void CopyTo(ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool) => ReadOnlySpan.CopyTo(ref dst, memoryPool);
    }
}