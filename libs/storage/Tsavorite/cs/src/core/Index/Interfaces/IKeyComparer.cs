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
        /// Get 64-bit hash code
        /// </summary>
        long GetHashCode64(ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes);

        /// <summary>
        /// Equality comparison
        /// </summary>
        /// <param name="k1">Left side</param>
        /// <param name="k2">Right side</param>
        bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2);

        /// <summary>
        /// Equality comparison
        /// </summary>
        /// <param name="k1">Left side</param>
        /// <param name="ns1">Left side namespace</param>
        /// <param name="k2">Right side</param>
        /// <param name="ns2">Right side namespace</param>
        bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> ns1, ReadOnlySpan<byte> k2, ReadOnlySpan<byte> ns2);
    }
}