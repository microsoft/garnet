// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// Defines a key type for Tsavorite operations, providing hashing and equality comparison.
    /// </summary>
    public interface IKey
    {
        /// <summary>
        /// True if the <see cref="KeyBytes"/> and other memory exposed by this <see cref="IKey"/> can be safely assumed to not move.
        /// </summary>
        bool IsPinned { get; }

        /// <summary>
        /// True if the <see cref="IKey"/> is truly empty - not zero bytes, but uninitialized and conceptually bereft of data.
        /// 
        /// This should be false for almost all implementors.
        /// </summary>
        bool IsEmpty => false;

        /// <summary>
        /// Get 64-bit hash code for this key.
        /// </summary>
        long GetKeyHashCode64();

        /// <summary>
        /// The raw bytes of this key.
        /// </summary>
        ReadOnlySpan<byte> KeyBytes { get; }

        /// <summary>
        /// Compare this key for equality with another key.
        /// </summary>
        /// <typeparam name="TOther">The type of the other key.</typeparam>
        /// <param name="other">The other key to compare against.</param>
        bool KeysEqual<TOther>(TOther other) where TOther : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            ;
    }
}