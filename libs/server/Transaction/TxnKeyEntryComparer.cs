// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    internal class TxnKeyEntryComparer : IComparer<TxnKeyEntry>
    {
        public LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> lockableContext;
        public LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreLockableContext;

        internal TxnKeyEntryComparer(LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> lockableContext, LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreLockableContext)
        {
            this.lockableContext = lockableContext;
            this.objectStoreLockableContext = objectStoreLockableContext;
        }

        /// <inheritdoc />
        public int Compare(TxnKeyEntry key1, TxnKeyEntry key2)
        {
            // This sorts by isObject, then calls Tsavorite to sort by lock code and then by lockType.
            var cmp = key1.isObject.CompareTo(key2.isObject);
            if (cmp != 0)
                return cmp;
            if (key1.isObject)
                return objectStoreLockableContext.CompareKeyHashes(key1, key2);
            else
                return lockableContext.CompareKeyHashes(key1, key2);
        }
    }
}