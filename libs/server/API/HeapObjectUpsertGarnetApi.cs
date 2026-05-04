// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// API wrapper for heap object upsert operations. Provides Upsert using the heap object context.
    /// </summary>
    internal struct HeapObjectUpsertGarnetApi
    {
        readonly StorageSession storageSession;
        HeapObjectBasicContext heapObjectContext;

        internal HeapObjectUpsertGarnetApi(StorageSession storageSession, HeapObjectBasicContext heapObjectContext)
        {
            this.storageSession = storageSession;
            this.heapObjectContext = heapObjectContext;
        }

        /// <summary>
        /// Upsert a heap object into the store.
        /// </summary>
        public Status Upsert(FixedSpanByteKey key, ref HeapObjectInput input)
            => heapObjectContext.Upsert(key, ref input);
    }
}
