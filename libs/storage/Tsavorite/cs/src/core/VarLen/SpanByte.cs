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
        public byte* ToPointer()
        {
            Debug.Assert((length == 0 && payload == 0) || !Serialized, "Unexpected Serialized == true in ToPointer()");
            return (byte*)payload;
        }

        [Conditional("DEBUG")]
        readonly void AssertNonSerialized(string methodName) => Debug.Assert((length == 0 && payload == 0) || !Serialized, $"Unexpected Serialized == true in {methodName}");

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
            readonly get
            {
                AssertNonSerialized("Invalid.get");
                return ((length & UnserializedBitMask) != 0) && payload == IntPtr.Zero;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert(value, "Cannot restore an Invalid SpanByte to Valid; must reassign the SpanByte as a full value");
                AssertNonSerialized("Invalid.set");

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
        {
            AssertNonSerialized("AsSpan()");
            return new Span<byte>((byte*)payload, Length);
        }

        /// <summary>
        /// Get ReadOnlySpan&lt;byte&gt; for this <see cref="SpanByte"/>'s payload
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            AssertNonSerialized("AsReadOnlySpan");
            return new ReadOnlySpan<byte>((byte*)payload, Length);
        }

        /// <summary>
        /// Create a SpanByte around a pinned memory <paramref name="pointer"/> of given <paramref name="length"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="pointer"/> MUST point to pinned memory.
        /// </remarks>
        public static SpanByte FromPinnedPointer(byte* pointer, int length) => new(length, (nint)pointer);

        /// <summary>
        /// Create a SpanByte around a pinned memory <paramref name="pointer"/> whose first sizeof(int) bytes are the length (i.e. serialized form).
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="pointer"/> MUST point to pinned memory.
        /// </remarks>
        public static SpanByte FromLengthPrefixedPinnedPointer(byte* pointer) => new(*(int*)pointer, (nint)(pointer + sizeof(int)));

        /// <summary>
        /// Create a <see cref="SpanByte"/> from the given <paramref name="span"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="span"/> MUST point to pinned memory.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SpanByte FromPinnedSpan(ReadOnlySpan<byte> span) => new (span.Length, (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(span)));

        /// <summary>
        /// Convert payload to new byte array
        /// </summary>
        public byte[] ToByteArray() => AsReadOnlySpan().ToArray();

        /// <summary>
        /// Try to copy to given pre-allocated <see cref="SpanByte"/>, checking if space permits at destination <see cref="SpanByte"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryCopyTo(SpanByte dst)
        {
            if (dst.Length < Length)
                return false;
            CopyTo(dst);
            return true;
        }

        /// <summary>
        /// Blindly copy to given pre-allocated <see cref="SpanByte"/>, assuming sufficient space.
        /// Does not change length of destination.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(SpanByte dst) => AsReadOnlySpan().CopyTo(dst.AsSpan());

        /// <summary>
        /// Copy to given <see cref="SpanByteAndMemory"/> (only payload copied to actual span/memory)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            AssertNonSerialized("CopyTo(SBAM)");
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
        /// Copy to specified memory location
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(Span<byte> buffer)
        {
            AssertNonSerialized("CopyTo(Span<byte>)");
            fixed (byte* ptr = buffer)
                CopyTo(ptr);
        }

        /// <summary>
        /// Copy serialized version to specified memory location. TODO: Needs a length arg for verification
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(byte* destination)
        {
            AssertNonSerialized("CopyTo(byte*)");
            *(int*)destination = Length;
            Buffer.MemoryCopy((void*)payload, destination + sizeof(int), Length, Length);
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            AssertNonSerialized("ToString()");
            if (Invalid)
                return "Invalid";
            var bytes = AsSpan();
            var len = Math.Min(Length, bytes.Length);
            StringBuilder sb = new($"len: {Length}, isSer {Serialized}, ");
            if (Length == 0)
                return sb.Append("<empty>").ToString();
            for (var ii = 0; ii < len; ++ii)
                _ = sb.Append(bytes[ii].ToString("x2"));
            if (bytes.Length > len)
                _ = sb.Append("...");
            return sb.ToString();
        }

        /// <summary>Return an abbreviated string representation of this <see cref="SpanByte"/></summary>
        public string ToShortString(int maxLen)
        {
            AssertNonSerialized("ToShortString()");
            return Length > maxLen
                    ? $"len: {Length}, isSer {Serialized}, {FromPinnedSpan(AsReadOnlySpan().Slice(0, maxLen)).ToString()}..."
                    : ToString();
        }
    }
}