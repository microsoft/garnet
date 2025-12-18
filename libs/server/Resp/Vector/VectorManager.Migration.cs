// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Methods related to migrating Vector Sets between different primaries.
    /// 
    /// This is bespoke because normal migration is key based, but Vector Set migration has to move whole namespaces first.
    /// </summary>
    public sealed partial class VectorManager
    {
        // This is a V8 GUID based on 'GARNET MIGRATION' ASCII string
        // It cannot collide with processInstanceIds because it's v8
        // It's unlikely other projects will select the value, so it's unlikely to collide with other v8s
        // If it ends up in logs, it's ASCII equivalent looks suspcious enough to lead back here
        private static readonly Guid MigratedInstanceId = new("4e524147-5445-8d20-8947-524154494f4e");

        /// <summary>
        /// Called to handle a key in a namespace being received during a migration.
        /// 
        /// These keys are what DiskANN stores, that is they are "element" data.
        /// 
        /// The index is handled specially by <see cref="HandleMigratedIndexKey"/>.
        /// </summary>
        public void HandleMigratedElementKey(
            ref BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> basicCtx,
            ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> vectorCtx,
            ref PinnedSpanByte key,
            ref PinnedSpanByte value
        )
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Called to handle a Vector Set key being received during a migration.  These are "index" keys.
        /// 
        /// This is the metadata stuff Garnet creates, DiskANN is not involved.
        /// 
        /// Invoked after all the namespace data is moved via <see cref="HandleMigratedElementKey"/>.
        /// </summary>
        public void HandleMigratedIndexKey(
            GarnetDatabase db,
            StoreWrapper storeWrapper,
            ref PinnedSpanByte key,
            ref PinnedSpanByte value)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Find namespaces used by the given keys, IFF they are Vector Sets.  They may (and often will) not be.
        /// 
        /// Meant for use during migration.
        /// </summary>
        public unsafe HashSet<ulong> GetNamespacesForKeys(StoreWrapper storeWrapper, IEnumerable<byte[]> keys, Dictionary<byte[], byte[]> vectorSetKeys)
        {
            throw new NotImplementedException();
        }
    }
}