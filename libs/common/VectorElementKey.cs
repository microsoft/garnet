// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
#endif

        // TODO: When variable length namespaces are supported, this will need to change
        private readonly byte namespaceByte;

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
        [UnscopedRef]
        public readonly ReadOnlySpan<byte> NamespaceBytes
        {
            get
            {
                return new(in namespaceByte);
            }
        }

        /// <summary>
        /// Construct a new <see cref="VectorElementKey"/>.
        /// 
        /// Note that <paramref name="namespaceByte"/> cannot be 0.
        /// </summary>
        public VectorElementKey(byte namespaceByte, ReadOnlySpan<byte> key)
        {
            Debug.Assert(namespaceByte != 0, "Namespace must be non-zero");

            this.namespaceByte = namespaceByte;

#if NET9_0_OR_GREATER
            KeyBytes = key;
#else
            unsafe
            {
                ptr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(key));
            }
            len = key.Length;
#endif
        }

        /// <inheritdoc/>
        public override readonly string ToString() => $"ns: {namespaceByte}, {SpanByte.ToShortString(KeyBytes)}";
    }
}