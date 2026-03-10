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
        /// <inheritdoc/>
        public bool IsPinned => true;

        /// <inheritdoc/>
        bool IKey.IsEmpty => true;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> KeyBytes => [];

        /// <inheritdoc/>
        public bool HasNamespace => false;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> NamespaceBytes => [];
    }
}