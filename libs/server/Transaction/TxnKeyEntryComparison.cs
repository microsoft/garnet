// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    internal sealed class TxnKeyComparison
    {
        public TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> transactionalContext;
        public TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreTransactionalContext;
        public TransactionalContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreTransactionalContext;

        public readonly Comparison<TxnKeyEntry> comparisonDelegate;

        internal TxnKeyComparison(TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> transactionalContext,
                TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreTransactionalContext,
                TransactionalContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreTransactionalContext)
        {
            this.transactionalContext = transactionalContext;
            this.objectStoreTransactionalContext = objectStoreTransactionalContext;
            this.unifiedStoreTransactionalContext = unifiedStoreTransactionalContext;
            comparisonDelegate = Compare;
        }

        /// <inheritdoc />
        public int Compare(TxnKeyEntry key1, TxnKeyEntry key2)
        {
            // This sorts by storeType, then calls Tsavorite to sort by lock code and then by lockType.
            var cmp = key1.storeType.CompareTo(key2.storeType);
            if (cmp != 0)
                return cmp;

            return key1.storeType switch
            {
                StoreType.Main => transactionalContext.CompareKeyHashes(ref key1, ref key2),
                StoreType.Object => objectStoreTransactionalContext.CompareKeyHashes(ref key1, ref key2),
                StoreType.All => unifiedStoreTransactionalContext.CompareKeyHashes(ref key1, ref key2),
                _ => throw new InvalidOperationException($"Unknown store type"),
            };
        }
    }
}