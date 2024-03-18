// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Watch Version Map
    /// An instance per garnet server to store versions of watched keys
    /// </summary>
    public sealed class WatchVersionMap
    {
        private readonly long[] map;
        private readonly long sizeMask;

        /// <summary>
        /// Constructor
        /// </summary>
        public WatchVersionMap(long size)
        {
            Debug.Assert(Utility.IsPowerOfTwo(size));
            sizeMask = size - 1;
            map = new long[size];
        }

        /// <summary>
        /// Read a version of a key
        /// Call before watch
        /// </summary>
        internal long ReadVersion(long keyHash)
            => Interlocked.Read(ref map[keyHash & sizeMask]);

        /// <summary>
        /// Increment version of a watched key
        /// Call while modifying a watched key
        /// </summary>
        internal void IncrementVersion(long keyHash)
            => Interlocked.Increment(ref map[keyHash & sizeMask]);
    }
}