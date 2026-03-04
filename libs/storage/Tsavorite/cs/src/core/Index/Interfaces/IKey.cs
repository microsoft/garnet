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
        /// The raw bytes of this key.
        /// </summary>
        ReadOnlySpan<byte> KeyBytes { get; }
    }
}