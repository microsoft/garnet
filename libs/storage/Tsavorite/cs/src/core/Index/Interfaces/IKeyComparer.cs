// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        long GetHashCode64(ReadOnlySpan<byte> key);

        /// <summary>
        /// Equality comparison
        /// </summary>
        /// <param name="k1">Left side</param>
        /// <param name="k2">Right side</param>
        bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2);
    }
}