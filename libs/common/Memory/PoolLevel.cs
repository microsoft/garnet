// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;

namespace Garnet.common
{
    /// <summary>
    /// Pool level
    /// </summary>
    public class PoolLevel
    {
        /// <summary>
        /// Size
        /// </summary>
        public int size;

        /// <summary>
        /// Items
        /// </summary>
        public readonly ConcurrentQueue<PoolEntry> items;

        /// <summary>
        /// Constructor
        /// </summary>
        public PoolLevel()
        {
            size = 0;
            items = new ConcurrentQueue<PoolEntry>();
        }
    }
}