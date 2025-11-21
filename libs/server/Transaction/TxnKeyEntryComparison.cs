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
        public TransactionalContext<UnifiedStoreInput, UnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreTransactionalContext;

        public readonly Comparison<TxnKeyEntry> comparisonDelegate;

        internal TxnKeyComparison(
            TransactionalContext<UnifiedStoreInput, UnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreTransactionalContext)
        {
            this.unifiedStoreTransactionalContext = unifiedStoreTransactionalContext;
            comparisonDelegate = Compare;
        }

        public int Compare(TxnKeyEntry key1, TxnKeyEntry key2)
            => unifiedStoreTransactionalContext.CompareKeyHashes(ref key1, ref key2);
    }
}