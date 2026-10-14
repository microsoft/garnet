// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Tsavorite.core
{
    /// <summary>
    /// Defines methods to support the comparison of Tsavorite keys for equality.
    /// </summary>
    /// <remarks>This comparer differs from the built-in <see cref="IEqualityComparer{Span}"/> in that it implements a 64-bit hash code</remarks>
    public interface IKeyComparer
    {
        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        long GetHashCode64<TKey>(TKey key)
            where TKey : IKey
                , allows ref struct
            ;

        /// <summary>
        /// Equality comparison
        /// </summary>
        /// <param name="k1">Left side</param>
        /// <param name="k2">Right side</param>
        bool Equals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey
                , allows ref struct
            where TSecondKey : IKey
                , allows ref struct
            ;
    }
}