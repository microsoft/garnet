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
        public readonly TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> Store1;
        public readonly TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> Store2;

        internal readonly TStoreFunctions1 storeFunctions1;
        internal readonly TStoreFunctions2 storeFunctions2;

        /// <summary>
        /// Create dual TsavoriteKV instances using shared kernel in a potentially-partitioned implementation.
        /// </summary>
        public DualTsavoriteKV(TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> store1, TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> store2)
        {
            Store1 = store1;
            Store2 = store2;
        }

        /// <summary>Get the hashcode for a key; it should be the same in both stores, so use the first one.</summary>
        public long GetKeyHash(TKey1 key) => Store1.GetKeyHash(ref key);

        /// <summary>Get the hashcode for a key.</summary>
        public long GetKeyHash(ref TKey1 key) => Store1.GetKeyHash(ref key);

        /// <summary>
        /// Dispose DualTsavoriteKV instance
        /// </summary>
        public void Dispose()
        {
            Store1.Dispose();
            Store2.Dispose();
        }
    }
}