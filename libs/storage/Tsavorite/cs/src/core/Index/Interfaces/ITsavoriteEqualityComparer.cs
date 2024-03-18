// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Key interface
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ITsavoriteEqualityComparer<T>
    {
        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        /// <returns></returns>
        long GetHashCode64(ref T k);

        /// <summary>
        /// Equality comparison
        /// </summary>
        /// <param name="k1">Left side</param>
        /// <param name="k2">Right side</param>
        /// <returns></returns>
        bool Equals(ref T k1, ref T k2);
    }
}