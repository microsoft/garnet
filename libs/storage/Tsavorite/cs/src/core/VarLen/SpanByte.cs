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
    /// Format: [4-byte (int) length of payload][[optional 8-byte metadata] payload bytes...]
    /// First 2 bits of length are used as a mask for properties, so max payload length is 1GB
    /// </remarks>
    [StructLayout(LayoutKind.Explicit, Pack = 4)]
    public unsafe struct SpanByte
    {
        // Byte #31 is used to denote unserialized (1) or serialized (0) data 
        private const int UnserializedBitMask = 1 << 31;
        // Byte #30 is used to denote extra metadata present (1) or absent (0) in payload
        private const int ExtraMetadataBitMask = 1 << 30;
        // Bit #29 used to denote if a namespace is present in payload
        private const int NamespaceBitMask = 1 << 29;
        // Mask for header
        private const int HeaderMask = UnserializedBitMask | ExtraMetadataBitMask | NamespaceBitMask;

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
        /// Pointer to the beginning of payload, not including metadata if any
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* ToPointer()
        {
            if (Serialized)
                return MetadataSize + (byte*)Unsafe.AsPointer(ref payload);
            else
                return MetadataSize + (byte*)payload;
        }

        /// <summary>
        /// Pointer to the beginning of payload, including metadata if any
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* ToPointerWithMetadata()
        {
            if (Serialized)
                return (byte*)Unsafe.AsPointer(ref payload);
            else
                return (byte*)payload;
        }

        /// <summary>
        /// Length of payload, including metadata if any
        /// </summary>
        public int Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => length & ~HeaderMask;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { length = (length & HeaderMask) | value; }
        }

        /// <summary>
        /// Length of payload, not including metadata if any
        /// </summary>
        public readonly int LengthWithoutMetadata => (length & ~HeaderMask) - MetadataSize;

        /// <summary>
        /// Format of structure
        /// </summary>
        public readonly bool Serialized => (length & UnserializedBitMask) == 0;

        /// <summary>
        /// Total serialized size in bytes, including header and metadata if any
        /// </summary>
        public readonly int TotalSize => sizeof(int) + Length;

        /// <summary>
        /// Size of metadata header, if any (returns 0, 1, 8, or 9)
        /// </summary>
        public readonly int MetadataSize => ((length & ExtraMetadataBitMask) >> (30 - 3)) + ((length & NamespaceBitMask) >> 29);

        /// <summary>
        /// Create a <see cref="SpanByte"/> around a given <paramref name="payload"/> pointer and given <paramref name="length"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SpanByte(int length, IntPtr payload)
        {
            Debug.Assert(length <= ~HeaderMask);
            this.length = length | UnserializedBitMask;
            this.payload = payload;
        }

        /// <summary>
        /// Extra metadata header
        /// </summary>
        public long ExtraMetadata
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (Serialized)
                    return MetadataSize > 0 ? *(long*)Unsafe.AsPointer(ref payload) : 0;
                else
                    return MetadataSize > 0 ? *(long*)payload : 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                if (value > 0)
                {
                    length |= ExtraMetadataBitMask;
                    Debug.Assert(Length >= MetadataSize);
                    if (Serialized)
                        *(long*)Unsafe.AsPointer(ref payload) = value;
                    else
                        *(long*)payload = value;
                }
            }
        }

        /// <summary>
        /// Mark <see cref="SpanByte"/> as having 8-byte metadata in header of payload
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MarkExtraMetadata()
        {
            Debug.Assert(Length >= 8);
            Debug.Assert((length & NamespaceBitMask) == 0, "Don't use both extension for now");
            length |= ExtraMetadataBitMask;
        }

        /// <summary>
        /// Unmark <see cref="SpanByte"/> as having 8-byte metadata in header of payload
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnmarkExtraMetadata() => length &= ~ExtraMetadataBitMask;

        /// <summary>
        /// Mark <see cref="SpanByte"/> as having 1-byte namespace in header of payload
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void MarkNamespace()
        {
            Debug.Assert(Length >= 1);
            Debug.Assert((length & ExtraMetadataBitMask) == 0, "Don't use both extension for now");
            length |= NamespaceBitMask;
        }

        /// <summary>
        /// Unmark <see cref="SpanByte"/> as having 1-byte namespace in header of payload
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnmarkNamespace() => length &= ~NamespaceBitMask;

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

                // Set the actual length to 0; any metadata is no longer available, and a zero length will cause callers' length checks to go
                // through the ConvertToHeap path automatically. Keep the UnserializedBitMask.
                length = UnserializedBitMask;
                payload = IntPtr.Zero;
            }
        }

        /// <summary>
        /// Get Span&lt;byte&gt; for this <see cref="SpanByte"/>'s payload (excluding metadata if any)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan()
        {
            if (Serialized)
                return new Span<byte>(MetadataSize + (byte*)Unsafe.AsPointer(ref payload), Length - MetadataSize);
            else
                return new Span<byte>(MetadataSize + (byte*)payload, Length - MetadataSize);
        }

        /// <summary>
        /// Get Span&lt;byte&gt; for this <see cref="SpanByte"/>'s payload (excluding metadata if any)
        /// <paramref name="offset">
        /// Parameter to avoid having to call slice when wanting to interact directly with payload skipping ETag at the front of the payload
        /// </paramref>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan(int offset)
        {
            if (Serialized)
                return new Span<byte>(MetadataSize + (byte*)Unsafe.AsPointer(ref payload) + offset, Length - MetadataSize - offset);
            else
                return new Span<byte>(MetadataSize + (byte*)payload + offset, Length - MetadataSize - offset);
        }

        /// <summary>
        /// Get ReadOnlySpan&lt;byte&gt; for this <see cref="SpanByte"/>'s payload (excluding metadata if any)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            if (Serialized)
                return new ReadOnlySpan<byte>(MetadataSize + (byte*)Unsafe.AsPointer(ref payload), Length - MetadataSize);
            else
                return new ReadOnlySpan<byte>(MetadataSize + (byte*)payload, Length - MetadataSize);
        }

        /// <summary>
        /// Get ReadOnlySpan&lt;byte&gt; for this <see cref="SpanByte"/>'s payload (excluding metadata if any)
        /// <paramref name="offset">
        /// Parameter to avoid having to call slice when wanting to interact directly with payload skipping ETag at the front of the payload
        /// </paramref>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan(int offset)
        {
            if (Serialized)
                return new ReadOnlySpan<byte>(MetadataSize + (byte*)Unsafe.AsPointer(ref payload) + offset, Length - MetadataSize - offset);
            else
                return new ReadOnlySpan<byte>(MetadataSize + (byte*)payload + offset, Length - MetadataSize - offset);
        }

        /// <summary>
        /// Get Span&lt;byte&gt; for this <see cref="SpanByte"/>'s payload (including metadata if any)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpanWithMetadata()
        {
            if (Serialized)
                return new Span<byte>((byte*)Unsafe.AsPointer(ref payload), Length);
            else
                return new Span<byte>((byte*)payload, Length);
        }

        /// <summary>
        /// Get ReadOnlySpan&lt;byte&gt; for this <see cref="SpanByte"/>'s payload (including metadata if any)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpanWithMetadata()
        {
            if (Serialized)
                return new ReadOnlySpan<byte>((byte*)Unsafe.AsPointer(ref payload), Length);
            else
                return new ReadOnlySpan<byte>((byte*)payload, Length);
        }

        /// <summary>
        /// If <see cref="SpanByte"/> is in a serialized form, return a non-serialized <see cref="SpanByte"/> wrapper that points to the same payload.
        /// </summary>
        /// <remarks>
        /// SAFETY: The resulting <see cref="SpanByte"/> is safe to heap-copy, as long as the underlying payload remains pinned.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SpanByte Deserialize()
        {
            if (!Serialized) return this;
            return new SpanByte(Length - MetadataSize, (IntPtr)(MetadataSize + (byte*)Unsafe.AsPointer(ref payload)));
        }

        /// <summary>
        /// Reinterpret a fixed Span&lt;byte&gt; as a serialized <see cref="SpanByte"/>. Automatically adds Span length to the first 4 bytes.
        /// </summary>
        public static ref SpanByte Reinterpret(Span<byte> span)
        {
            Debug.Assert(span.Length - sizeof(int) <= ~HeaderMask);

            fixed (byte* ptr = span)
            {
                *(int*)ptr = span.Length - sizeof(int);
                return ref Unsafe.AsRef<SpanByte>(ptr);
            }
        }

        /// <summary>
        /// Reinterpret a fixed ReadOnlySpan&lt;byte&gt; as a serialized <see cref="SpanByte"/>, without adding length header
        /// </summary>
        public static ref SpanByte ReinterpretWithoutLength(ReadOnlySpan<byte> span)
        {
            fixed (byte* ptr = span)
            {
                return ref Unsafe.AsRef<SpanByte>(ptr);
            }
        }

        /// <summary>
        /// Reinterpret a fixed pointer as a serialized <see cref="SpanByte"/>
        /// </summary>
        public static ref SpanByte Reinterpret(byte* ptr)
        {
            return ref Unsafe.AsRef<SpanByte>(ptr);
        }

        /// <summary>
        /// Reinterpret a fixed ref as a serialized <see cref="SpanByte"/> (user needs to write the payload length to the first 4 bytes)
        /// </summary>
        public static ref SpanByte Reinterpret<T>(ref T t)
        {
            return ref Unsafe.As<T, SpanByte>(ref t);
        }

        /// <summary>
        /// Create a SpanByte around a pinned memory <paramref name="pointer"/> of given <paramref name="length"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="pointer"/> MUST point to pinned memory.
        /// </remarks>
        public static SpanByte FromPinnedPointer(byte* pointer, int length) => new(length, (nint)pointer);

        /// <summary>
        /// Create a SpanByte around a pinned unmanaged struct.
        /// </summary>
        /// <remarks>
        /// SAFETY: The provided unmanaged struct MUST be on the stack or point to pinned memory.
        /// </remarks>
        public static SpanByte FromPinnedStruct<T>(T* ptr) where T : unmanaged
            => new(Unsafe.SizeOf<T>(), (nint)ptr);

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
        /// Create SpanByte around a pinned <paramref name="memory"/>.
        /// </summary>
        /// <remarks>
        /// SAFETY: The <paramref name="memory"/> MUST be pinned.
        /// </remarks>
        public static SpanByte FromPinnedMemory(Memory<byte> memory) => FromPinnedSpan(memory.Span);

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
            if (dst.Length < Length) return false;
            CopyTo(ref dst);
            return true;
        }

        /// <summary>
        /// Blindly copy to given pre-allocated <see cref="SpanByte"/>, assuming sufficient space.
        /// Does not change length of destination.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyTo(ref SpanByte dst, long metadata = 0)
        {
            dst.UnmarkExtraMetadata();
            dst.ExtraMetadata = metadata == 0 ? ExtraMetadata : metadata;
            AsReadOnlySpan().CopyTo(dst.AsSpan());
        }

        /// <summary>
        /// Try to copy to given pre-allocated <see cref="SpanByte"/>, checking if space permits at destination <see cref="SpanByte"/>
        /// </summary>
        /// <param name="dst">The target of the copy</param>
        /// <param name="metadata">Optional metadata to add to the destination</param>
        /// <param name="fullDestSize">The size available at the destination (e.g. dst.TotalSize or the log-space Value allocation size)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySafeCopyTo(ref SpanByte dst, int fullDestSize, long metadata = 0)
        {
            // If the incoming caller wants to addMetadata and the destination does not already have metadata, the new length needs to account for it.
            var addMetadata = metadata > 0 && MetadataSize == 0;

            var newTotalSize = addMetadata ? TotalSize + sizeof(long) : TotalSize;
            if (fullDestSize < newTotalSize)
                return false;

            var newLength = addMetadata ? Length + sizeof(long) : Length;
            dst.ShrinkSerializedLength(newLength);
            // Note: If dst is shorter than src we have already verified there is enough extra value space to grow dst to store src.
            dst.Length = newLength;
            CopyTo(ref dst, metadata);

            return true;
        }

        /// <summary>
        /// Shrink the length header of the in-place allocated buffer on
        /// Tsavorite hybrid log, pointed to by the given <see cref="SpanByte"/>.
        /// Zeroes out the extra space to retain log scan correctness.
        /// </summary>
        /// <param name="newLength">New length of payload (including metadata)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ShrinkSerializedLength(int newLength)
        {
            // Zero-fill extra space - needed so log scan does not see spurious data - *before* setting length to 0.
            if (newLength < Length)
            {
                Unsafe.InitBlockUnaligned(ToPointerWithMetadata() + newLength, 0, (uint)(Length - newLength));
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
        /// Copy to given <see cref="SpanByteAndMemory"/> (only payload copied to actual span/memory)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopySliceTo(int sliceLength, ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            if (dst.IsSpanByte)
            {
                if (dst.Length >= Length)
                {
                    dst.Length = Length;
                    AsReadOnlySpan().Slice(0, sliceLength).CopyTo(dst.SpanByte.AsSpan());
                    return;
                }
                dst.ConvertToHeap();
            }

            dst.Memory = memoryPool.Rent(Length);
            dst.Length = Length;
            AsReadOnlySpan().Slice(0, sliceLength).CopyTo(dst.Memory.Memory.Span);
        }

        /// <summary>
        /// Copy to given <see cref="SpanByteAndMemory"/> (header and payload copied to actual span/memory)
        /// </summary>
        public void CopyWithHeaderTo(ref SpanByteAndMemory dst, MemoryPool<byte> memoryPool)
        {
            if (dst.IsSpanByte)
            {
                if (dst.Length >= TotalSize)
                {
                    dst.Length = TotalSize;
                    var span = dst.SpanByte.AsSpan();
                    fixed (byte* ptr = span)
                        *(int*)ptr = Length;
                    dst.SpanByte.ExtraMetadata = ExtraMetadata;

                    AsReadOnlySpan().CopyTo(span.Slice(sizeof(int) + MetadataSize));
                    return;
                }
                dst.ConvertToHeap();
            }

            dst.Memory = memoryPool.Rent(TotalSize);
            dst.Length = TotalSize;
            fixed (byte* ptr = dst.Memory.Memory.Span)
                *(int*)ptr = Length;
            dst.SpanByte.ExtraMetadata = ExtraMetadata;
            AsReadOnlySpan().CopyTo(dst.Memory.Memory.Span.Slice(sizeof(int) + MetadataSize));
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

        /// <summary>
        /// Gets an Etag from the payload of the SpanByte, caller should make sure the SpanByte has an Etag for the record by checking RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetEtagInPayload() => *(long*)this.ToPointer();

        /// <summary>
        /// Gets an Etag from the payload of the SpanByte, caller should make sure the SpanByte has an Etag for the record by checking RecordInfo
        /// </summary>
        /// <param name="etag">The Etag value to set</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetEtagInPayload(long etag) => *(long*)this.ToPointer() = etag;

        /// <summary>
        /// Gets a namespace from the payload of the SpanByte, caller should make sure the SpanByte has a namespace for the record by checking RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte GetNamespaceInPayload() => *(byte*)this.ToPointerWithMetadata();

        /// <summary>
        /// Gets a namespace from the payload of the SpanByte, caller should make sure the SpanByte has a namespace for the record by checking RecordInfo
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetNamespaceInPayload(byte ns) => *(byte*)this.ToPointerWithMetadata() = ns;

        /// <inheritdoc/>
        public override string ToString()
        {
            if (Invalid)
                return "Invalid";
            var bytes = AsSpan();
            var len = Math.Min(Length, bytes.Length);
            StringBuilder sb = new($"len: {Length}, mdLen: {MetadataSize}, isSer {Serialized}, ");
            for (var ii = 0; ii < len; ++ii)
                sb.Append(bytes[ii].ToString("x2"));
            if (bytes.Length > len)
                sb.Append("...");
            return sb.ToString();
        }
    }
}