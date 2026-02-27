// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
#if !NET9_0_OR_GREATER
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#endif
using Tsavorite.core;

namespace Garnet.common
{
    /// <summary>
    /// A readonly ref struct that wraps a <see cref="ReadOnlySpan{T}"/> of bytes as an <see cref="IKey"/> implementation.
    /// 
    /// Assumes that the underlying memory is pinned.
    /// </summary>
    public readonly
#if NET9_0_OR_GREATER
        ref
#endif
        struct SpanByteKey : IKey
    {
#if NET9_0_OR_GREATER
        private readonly ReadOnlySpan<byte> _keyBytes;
#else
        private readonly unsafe void* ptr;
        private readonly int len;
#endif

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes
        {
            get
            {
#if NET9_0_OR_GREATER
                return _keyBytes;
#else
                unsafe
                {
                    return new(ptr, len);
                }
#endif
            }
        }

        /// <summary>
        /// Create a new SpanByteKey wrapping the given bytes.
        /// </summary>
        private SpanByteKey(ReadOnlySpan<byte> key)
        {
#if NET9_0_OR_GREATER
            _keyBytes = key;
#else
            unsafe
            {
                ptr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(key));
                len = key.Length;
            }
#endif
        }

        /// <inheritdoc/>
        public readonly bool IsPinned => true;

#if NET9_0_OR_GREATER
        /// <inheritdoc/>
        public readonly bool IsEmpty => false;
#endif

        /// <inheritdoc/>
        public readonly long GetKeyHashCode64() => SpanByteComparer.StaticGetHashCode64(KeyBytes);

        /// <inheritdoc/>
        public readonly bool KeysEqual<TOther>(TOther other) where TOther : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => KeyBytes.SequenceEqual(other.KeyBytes);

        /// <summary>
        /// Explicit conversion from <see cref="ReadOnlySpan{T}"/> of bytes.
        /// 
        /// TODO: We could maybe obviate this with a set of extension methods on Tsavorite context?
        /// </summary>
        public static explicit operator SpanByteKey(ReadOnlySpan<byte> key) => new(key);
    }
}