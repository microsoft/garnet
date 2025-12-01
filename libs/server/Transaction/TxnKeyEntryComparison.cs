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
        public TransactionalContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> UnifiedTransactionalContext;

        public readonly Comparison<TxnKeyEntry> comparisonDelegate;

        internal TxnKeyComparison(
            TransactionalContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedTransactionalContext)
        {
            this.UnifiedTransactionalContext = unifiedTransactionalContext;
            comparisonDelegate = Compare;
        }

        public int Compare(TxnKeyEntry key1, TxnKeyEntry key2)
            => UnifiedTransactionalContext.CompareKeyHashes(ref key1, ref key2);
    }
}