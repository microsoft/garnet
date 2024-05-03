// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Tsavorite.core
{
    /// <summary>
    /// Defines methods to support the comparison of Tsavorite keys for equality.
    /// </summary>
    /// <typeparam name="T">The type of keys to compare.</typeparam>
    /// <remarks>This comparer differs from the built-in <see cref="IEqualityComparer{T}"/> in that it implements a 64-bit hash code</remarks>
    public interface ITsavoriteEqualityComparer<T>
    {
        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        long GetHashCode64(ref T key);

        /// <summary>
        /// Equality comparison
        /// </summary>
        /// <param name="k1">Left side</param>
        /// <param name="k2">Right side</param>
        bool Equals(ref T k1, ref T k2);
    }
}