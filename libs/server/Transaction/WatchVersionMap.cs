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
    /// <remarks>
    /// Versions live in a fixed-size array indexed by the key hash; no key bytes are stored and no key is ever
    /// compared, so keys whose hashes land on the same slot share a version. Hash collisions are acceptable by design;
    /// see <see cref="WatchedKeySlice.hash"/> for the rationale.
    /// </remarks>
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
        /// <remarks>Keyed by hash alone; hash collisions are acceptable by design.</remarks>
        internal long ReadVersion(long keyHash)
            => Interlocked.Read(ref map[keyHash & sizeMask]);

        /// <summary>
        /// Increment version of a watched key
        /// Call while modifying a watched key
        /// </summary>
        /// <remarks>
        /// Callers pass the Tsavorite record's key hash, the same <see cref="Utility.HashBytes"/> value the watcher
        /// recorded. Keyed by hash alone; hash collisions are acceptable by design, and bumping a shared slot can only
        /// add a spurious conflict, never mask a real one.
        /// </remarks>
        internal void IncrementVersion(long keyHash)
            => Interlocked.Increment(ref map[keyHash & sizeMask]);
    }
}