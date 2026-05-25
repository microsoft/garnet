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
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            ;

        /// <summary>
        /// Equality comparison
        /// </summary>
        /// <param name="k1">Left side</param>
        /// <param name="k2">Right side</param>
        bool Equals<TFirstKey, TSecondKey>(TFirstKey k1, TSecondKey k2)
            where TFirstKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSecondKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            ;
    }
}