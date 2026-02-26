// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// A readonly ref struct that wraps a <see cref="ReadOnlySpan{T}"/> of bytes as an <see cref="IKey"/> implementation.
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
        
#endif

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes
        {
            get
            {
#if NET9_0_OR_GREATER
                return _keyBytes;
#else
                return default;
#endif
            }
        }

        /// <summary>
        /// Create a new SpanByteKey wrapping the given bytes.
        /// </summary>
        public SpanByteKey(ReadOnlySpan<byte> key)
        {
#if NET9_0_OR_GREATER
            _keyBytes = key;
#else

#endif
        }

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