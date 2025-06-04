// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using ObjectStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    internal sealed class TxnKeyComparison
    {
        public TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> transactionalContext;
        public TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreTransactionalContext;

        public readonly Comparison<TxnKeyEntry> comparisonDelegate;

        internal TxnKeyComparison(TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> transactionalContext,
                TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreTransactionalContext)
        {
            this.transactionalContext = transactionalContext;
            this.objectStoreTransactionalContext = objectStoreTransactionalContext;
            comparisonDelegate = Compare;
        }

        /// <inheritdoc />
        public int Compare(TxnKeyEntry key1, TxnKeyEntry key2)
        {
            // This sorts by isObject, then calls Tsavorite to sort by lock code and then by lockType.
            var cmp = key1.isObject.CompareTo(key2.isObject);
            if (cmp != 0)
                return cmp;
            if (key1.isObject)
                return objectStoreTransactionalContext.CompareKeyHashes(ref key1, ref key2);
            else
                return transactionalContext.CompareKeyHashes(ref key1, ref key2);
        }
    }
}