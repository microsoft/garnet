// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
#if !NET9_0_OR_GREATER
using System.Runtime.InteropServices;
#endif
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// Key type for Vector Set element data - anything that's hidden in a namespace.
    /// 
    /// Has same constraints as <see cref="FixedSpanByteKey"/> - must be pinned and "fixed" for duration of operation.
    /// 
    /// Always has a namespace.
    /// </summary>
    public readonly
#if NET9_0_OR_GREATER
        ref
#endif
        struct VectorElementKey : IKey
    {
#if !NET9_0_OR_GREATER
        private readonly unsafe void* ptr;
        private readonly int len;

        private readonly unsafe void* nsPtr;
        private readonly int nsLen;
#endif


        /// <inheritdoc/>
        public readonly bool IsPinned
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => true;
        }

        /// <inheritdoc/>
        public readonly bool IsEmpty
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => false;
        }

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if NET9_0_OR_GREATER
            get; 
#else
            get
            {
                unsafe
                {
                    return new(ptr, len);
                }
            }
#endif
        }

        /// <inheritdoc/>
        public readonly bool HasNamespace
        {
            get => true;
        }

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> NamespaceBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
#if NET9_0_OR_GREATER
            get; 
#else
            get
            {
                unsafe
                {
                    return new(nsPtr, nsLen);
                }
            }
#endif
        }

        /// <summary>
        /// Construct a new <see cref="VectorElementKey"/>.
        /// 
        /// Note that <paramref name="namespaceBytes"/> cannot be 0.
        /// </summary>
        public VectorElementKey(ReadOnlySpan<byte> namespaceBytes, ReadOnlySpan<byte> key)
        {
            Debug.Assert(namespaceBytes.Length > 0, "Namespace cannot be empty");
            Debug.Assert(namespaceBytes.Length == 1 || (namespaceBytes.Length % 4) == 0, "Namespace must be either 1-byte or a multiple of 4-bytes long");
            Debug.Assert(namespaceBytes.Length != 1 || namespaceBytes[0] is > 0 and < 128, "Namespaces of length 1 must be > 0 and < 128");
            Debug.Assert(namespaceBytes.Length != 4 || BinaryPrimitives.ReadUInt32LittleEndian(namespaceBytes) >= 128, "Namespaces larger than 1 byte must be >= 128");

#if NET9_0_OR_GREATER
            KeyBytes = key;
            NamespaceBytes = namespaceBytes;
#else
            unsafe
            {
                nsPtr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(namespaceBytes));
                ptr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(key));
            }
            len = key.Length;
            nsLen = namespaceBytes.Length;
#endif
        }

        /// <inheritdoc/>
        public override readonly string ToString() => $"ns: {SpanByte.ToShortString(NamespaceBytes)}, {SpanByte.ToShortString(KeyBytes)}";
    }
}