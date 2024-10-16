// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// The Tsavorite Key/Value store class
    /// </summary>
    public partial class DualTsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1,
                                         TKey2, TValue2, TStoreFunctions2, TAllocator2> : IDisposable
        where TStoreFunctions1 : IStoreFunctions<TKey1, TValue1>
        where TStoreFunctions2 : IStoreFunctions<TKey2, TValue2>
        where TAllocator1 : IAllocator<TKey1, TValue1, TStoreFunctions1>
        where TAllocator2 : IAllocator<TKey2, TValue2, TStoreFunctions2>
    {
        internal readonly TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> store1;
        internal readonly TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> store2;

        internal readonly TStoreFunctions1 storeFunctions1;
        internal readonly TStoreFunctions2 storeFunctions2;

        /// <summary>
        /// Create dual TsavoriteKV instances using shared kernel in a potentially-partitioned implementation.
        /// </summary>
        public DualTsavoriteKV(TsavoriteKernel kernel,
                           KVSettings<TKey1, TValue1> kvSettings1, TStoreFunctions1 storeFunctions1, Func<AllocatorSettings, TStoreFunctions1, TAllocator1> allocatorFactory1,
                           KVSettings<TKey2, TValue2> kvSettings2, TStoreFunctions2 storeFunctions2, Func<AllocatorSettings, TStoreFunctions2, TAllocator2> allocatorFactory2)
        {
            store1 = new(kvSettings1, storeFunctions1, allocatorFactory1);
            store2 = new(kvSettings2, storeFunctions2, allocatorFactory2);
        }

        /// <summary>Get the hashcode for a key; it should be the same in both stores, so use the first one.</summary>
        public long GetKeyHash(TKey1 key) => store1.GetKeyHash(ref key);

        /// <summary>Get the hashcode for a key.</summary>
        public long GetKeyHash(ref TKey1 key) => store1.GetKeyHash(ref key);

        /// <summary>
        /// Dispose DualTsavoriteKV instance
        /// </summary>
        public void Dispose()
        {
            store1.Dispose();
            store2.Dispose();
        }
    }
}