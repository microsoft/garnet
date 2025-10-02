// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Abstraction for a set of keys, that supports a single enumeration though.
    /// </summary>
    public interface IKeyEnumerable<TKey>
    {
        /// <summary>
        /// Total number of keys in enumerable.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Get the current key, and advance to the next one.
        /// 
        /// Calling this more than <see cref="Count"/> times is illegal without an intermediate call to Reset.
        /// </summary>
        void GetAndMoveNext(ref TKey into);

        /// <summary>
        /// Move enumerable back to start.
        /// </summary>
        void Reset();
    }
}
