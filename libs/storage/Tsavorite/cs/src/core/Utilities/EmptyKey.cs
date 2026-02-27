// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// <see cref="IKey"/> implementation representing a completely empty key.
    /// 
    /// Not a key with no bytes, and key that isn't set.
    /// </summary>
    internal readonly struct EmptyKey : IKey
    {
        private static readonly long HashCode = SpanByteComparer.StaticGetHashCode64([]);

        /// <inheritdoc/>
        public bool IsPinned => true;

        /// <inheritdoc/>
        bool IKey.IsEmpty => true;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> KeyBytes => [];

        /// <inheritdoc/>
        public long GetKeyHashCode64() => HashCode;

        /// <inheritdoc/>
        public bool KeysEqual<TOther>(TOther other)
            where TOther : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        => typeof(TOther) == typeof(EmptyKey);
    }
}