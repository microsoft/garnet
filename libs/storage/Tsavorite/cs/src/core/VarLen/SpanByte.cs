// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Tsavorite.core
{
    /// <summary>
    /// Represents a pinned variable length byte array that is viewable as a pinned Span&lt;byte&gt;
    /// Important: AOF header version needs to be updated if this struct's disk representation changes
    /// </summary>
    /// <remarks>
    /// Format: [4-byte (int) length of payload][payload bytes...]
    /// First bit of length is used for the serialization indicator bit, so max payload length is 2GB
    /// </remarks>
    [StructLayout(LayoutKind.Explicit, Pack = 4)]
    public unsafe struct SpanByte
    {
        // Byte #31 is used to denote unserialized (1) or serialized (0) data 
        private const int UnserializedBitMask = 1 << 31;
        // Mask for header
        private const int HeaderMask = 0x1 << 31;

        /// <summary>
        /// Length of the payload
        /// </summary>
        [FieldOffset(0)]
        private int length;

        /// <summary>
        /// Start of payload
        /// </summary>
        [FieldOffset(4)]
        private IntPtr payload;

        internal readonly IntPtr Pointer => payload;

        /// <summary>
        /// Pointer to the beginning of payload
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* ToPointer() => Serialized ? (byte*)Unsafe.AsPointer(ref payload) : (byte*)payload;

        /// <summary>
        /// Length of payload
        /// </summary>
        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => length & ~HeaderMask;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { length = (length & HeaderMask) | value; }
        }

        /// <summary>
        /// Format of structure
        /// </summary>
        public readonly bool Serialized => (length & UnserializedBitMask) == 0;

        /// <summary>
        /// Total serialized size in bytes, including header if any
        /// </summary>
        public readonly int TotalSize => sizeof(int) + Length;

        /// <summary>
        /// Create a nonserialized <see cref="SpanByte"/> around a given <paramref name="payload"/> pointer and given <paramref name="length"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SpanByte(int length, IntPtr payload)
        {
            Debug.Assert(length <= ~HeaderMask);
            this.length = length | UnserializedBitMask;
            this.payload = payload;
        }

        /// <summary>
        /// Check or set struct as invalid
        /// </summary>
        public bool Invalid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => ((length & UnserializedBitMask) != 0) && payload == IntPtr.Zero;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert(value, "Cannot restore an Invalid SpanByte to Valid; must reassign the SpanByte as a full value");

                // Set the actual length to 0; a zero length will cause callers' length checks to go
                // through the ConvertToHeap path automatically. Keep the UnserializedBitMask.
                length = UnserializedBitMask;
                payload = IntPtr.Zero;
            }
        }

        /// <summary>
        /// Get Span&lt;byte&gt; for this <see cref="SpanByte"/>'s payload
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan() 
            => Serialized 
                ? new Span<byte>((byte*)Unsafe.AsPointer(ref payload), Length) 
                : new Span<byte>((byte*)payload, Length);

        /// <summary>
        /// Get ReadOnlySpan&lt;byte&gt; for this <see cref="SpanByte"/>'s payload
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan() 
            => Serialized
                ? new ReadOnlySpan<byte>((byte*)Unsafe.AsPointer(ref payload), Length)
                : new ReadOnlySpan<byte>((byte*)payload, Length);

        /// <summary>
        /// Reinterpret a fixed ReadOnlySpan&lt;byte&gt; as a serialized <see cref="SpanByte"/>, without adding length header
        /// </summary>
        public static ref SpanByte ReinterpretWithoutLength(ReadOnlySpan<byte> span)    // TODO: verify correctness at callsite (that span is length-prefixed)
        {
            fixed (byte* ptr = span)
            {
                return ref Reinterpret(ptr);
            }
        }

        /// <summary>
        /// Reinterpret a fixed pointer as a serialized <see cref="SpanByte"/>
        /// </summary>
        public static ref SpanByte Reinterpret(byte* ptr) => ref Unsafe.AsRef<SpanByte>(ptr);

        /// <summary>
        /// Create a SpanByte around a pinned memory <paramref name="pointer"/> of given <paramref name="length"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="pointer"/> MUST point to pinned memory.
        /// </remarks>
        public static SpanByte FromPinnedPointer(byte* pointer, int length) => new(length, (nint)pointer);

        /// <summary>
        /// Create a <see cref="SpanByte"/> from the given <paramref name="span"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="span"/> MUST point to pinned memory.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SpanByte FromPinnedSpan(ReadOnlySpan<byte> span)
        {
            return new SpanByte(span.Length, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(span)));
        }

        /// <summary>
        /// Convert payload to new byte array
        /// </summary>
        public byte[] ToByteArray() => AsReadOnlySpan().ToArray();

        /// <summary>
        /// Convert payload to specified (disposable) memory owner
        /// </summary>
        public (IMemoryOwner<byte> memory, int length) ToMemoryOwner(MemoryPool<byte> pool)
        {
            var dst = pool.Rent(Length);
            AsReadOnlySpan().CopyTo(dst.Memory.Span);
            return (dst, Length);
        }

        /// <summary>
        /// Convert to <see cref="SpanByteAndMemory"/> wrapper
        /// </summary>
        public readonly SpanByteAndMemory ToSpanByteAndMemory() => new(this);

        /// <summary>
        /// Try to copy to given pre-allocated <see cref="SpanByte"/>, checking if space permits at destination <see cref="SpanByte"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryCopyTo(ref SpanByte dst)
        {
            if (dst.Length < Length) 
                return false;
            CopyTo(ref dst);
            return true;
        }

        /// <summary>
        /// Blindly copy to given pre-allocated <see cref="SpanByte"/>, assuming sufficient space.
        /// Does not change length of destination.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(ref SpanByte dst) => AsReadOnlySpan().CopyTo(dst.AsSpan());

        /// <summary>
        /// Shrink the length header of the in-place allocated buffer on
        /// Tsavorite hybrid log, pointed to by the given <see cref="SpanByte"/>.
        /// Zeroes out the extra space to retain log scan correctness.
        /// </summary>
        /// <param name="newLength">New length of payload</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ShrinkSerializedLength(int newLength)
        {
            // Zero-fill extra space - needed so log scan does not see spurious data - *before* setting length to 0.
            if (newLength < Length)
            {
                Unsafe.InitBlockUnaligned(ToPointer() + newLength, 0, (uint)(Length - newLength));
                Length = newLength;
            }
        }

        /// <summary>
        /// Utility to zero out an arbitrary span of bytes. 
        /// One use is to zero extra space after in-place update shrinks a value, to retain log scan correctness.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Clear(byte* pointer, int length) => new Span<byte>(pointer, length).Clear();

        /// <summary>
        /// Copy to given <see cref="SpanByteAndMemory"/> (only payload copied to actual span/memory)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            if (dst.IsSpanByte)
            {
                if (dst.Length >= Length)
                {
                    dst.Length = Length;
                    AsReadOnlySpan().CopyTo(dst.SpanByte.AsSpan());
                    return;
                }
                dst.ConvertToHeap();
            }

            dst.Memory = memoryPool.Rent(Length);
            dst.Length = Length;
            AsReadOnlySpan().CopyTo(dst.Memory.Memory.Span);
        }

        /// <summary>
        /// Copy serialized version to specified memory location
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(Span<byte> buffer)
        {
            fixed (byte* ptr = buffer)
                CopyTo(ptr);
        }

        /// <summary>
        /// Copy serialized version to specified memory location
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(byte* destination)
        {
            if (Serialized)
            {
                *(int*)destination = length;
                Buffer.MemoryCopy(Unsafe.AsPointer(ref payload), destination + sizeof(int), Length, Length);
            }
            else
            {
                *(int*)destination = length & ~UnserializedBitMask;
                Buffer.MemoryCopy((void*)payload, destination + sizeof(int), Length, Length);
            }
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            if (Invalid)
                return "Invalid";
            var bytes = AsSpan();
            var len = Math.Min(Length, bytes.Length);
            StringBuilder sb = new($"len: {Length}, isSer {Serialized}, ");
            for (var ii = 0; ii < len; ++ii)
                _ = sb.Append(bytes[ii].ToString("x2"));
            if (bytes.Length > len)
                _ = sb.Append("...");
            return sb.ToString();
        }

        /// <summary>Return an abbreviated string representation of this <see cref="SpanByte"/></summary>
        public string ToShortString(int maxLen) 
            => Length > maxLen
                ? FromPinnedSpan(AsReadOnlySpan().Slice(0, maxLen)).ToString() + "..."
                : ToString();
    }
}