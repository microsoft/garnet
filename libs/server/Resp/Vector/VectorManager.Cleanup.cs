// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.networking;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;
    
    /// <summary>
    /// Methods related to cleaning up data after a Vector Set is deleted.
    /// </summary>
    public sealed partial class VectorManager
    {
        private readonly Channel<object> cleanupTaskChannel;
        private readonly Task cleanupTask;
        private readonly Func<IMessageConsumer> getCleanupSession;

        private async Task RunCleanupTaskAsync()
        {
            await Task.Yield();

            throw new NotImplementedException();
        }

        /// <summary>
        /// Called in response to <see cref="TryMarkDeleteInProgress"/> or <see cref="ClearDeleteInProgress"/> to update metadata in Tsavorite.
        /// 
        /// Returns false if there is insufficient size for the value.
        /// </summary>
        internal static bool TryUpdateInProgressDeletes(Span<byte> updateMessage, ref PinnedSpanByte inLogValue, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Before we start smashing a <see cref="Index"/> for deletion, records that we started to delete it so we can recover from crashes.
        /// </summary>
        internal bool TryMarkDeleteInProgress(ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx, ref PinnedSpanByte key, ulong context)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Enumerate any deletes of Vector Sets that are in progress.
        /// 
        /// Used with <see cref="TryMarkDeleteInProgress"/> and <see cref="ClearDeleteInProgress"/> to recover from interrupted deletes.
        /// </summary>
        internal List<(ReadOnlyMemory<byte> Key, ulong Context)> GetDeletesInProgress(StorageSession storageSession)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// After a delete has completed, removes the given key from metadata.
        /// </summary>
        internal void ClearDeleteInProgress(ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx, ref PinnedSpanByte key, ulong context)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// After an index is dropped, called to start the process of removing ancillary data (elements, neighbor lists, attributes, etc.).
        /// </summary>
        internal void CleanupDroppedIndex(ref BasicContext<VectorInput, PinnedSpanByte, long, VectorSessionFunctions, StoreFunctions, StoreAllocator> ctx, ulong context)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Detects if a Vector Set index read out of the main store is in the middle of being deleted.
        /// </summary>
        private static bool PartiallyDeleted(ReadOnlySpan<byte> indexConfig)
        {
            throw new NotImplementedException();
        }
    }
}