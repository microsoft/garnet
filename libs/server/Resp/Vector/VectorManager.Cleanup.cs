// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    /// <summary>
    /// Methods related to cleaning up data after a Vector Set is deleted.
    /// </summary>
    public sealed partial class VectorManager
    {
        /// <summary>
        /// Used as part of scanning post-index-delete to cleanup abandoned data.
        /// </summary>
        private sealed class PostDropCleanupFunctions : IScanIteratorFunctions<SpanByte, SpanByte>
        {
            private readonly StorageSession storageSession;
            private readonly FrozenSet<ulong> contexts;

            public PostDropCleanupFunctions(StorageSession storageSession, HashSet<ulong> contexts)
            {
                this.contexts = contexts.ToFrozenSet();
                this.storageSession = storageSession;
            }

            public bool ConcurrentReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            => SingleReader(ref key, ref value, recordMetadata, numberOfRecords, out cursorRecordResult);

            public void OnException(Exception exception, long numberOfRecords) { }
            public bool OnStart(long beginAddress, long endAddress) => true;
            public void OnStop(bool completed, long numberOfRecords) { }

            public bool SingleReader(ref SpanByte key, ref SpanByte value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
            {
                if (key.MetadataSize != 1)
                {
                    // Not Vector Set, ignore
                    cursorRecordResult = CursorRecordResult.Skip;
                    return true;
                }

                var ns = key.GetNamespaceInPayload();
                var pairedContext = (ulong)ns & ~(ContextStep - 1);
                if (!contexts.Contains(pairedContext))
                {
                    // Vector Set, but not one we're scanning for
                    cursorRecordResult = CursorRecordResult.Skip;
                    return true;
                }

                // Delete it
                var status = storageSession.vectorContext.Delete(ref key, 0);
                if (status.IsPending)
                {
                    SpanByte ignored = default;
                    CompletePending(ref status, ref ignored, ref storageSession.vectorContext);
                }

                cursorRecordResult = CursorRecordResult.Accept;
                return true;
            }
        }

        private readonly Channel<object> cleanupTaskChannel;
        private readonly Task cleanupTask;
        private readonly Func<IMessageConsumer> getCleanupSession;

        private async Task RunCleanupTaskAsync()
        {
            // Each drop index will queue a null object here
            // We'll handle multiple at once if possible, but using a channel simplifies cancellation and dispose
            await foreach (var ignored in cleanupTaskChannel.Reader.ReadAllAsync())
            {
                try
                {
                    HashSet<ulong> needCleanup;
                    lock (this)
                    {
                        needCleanup = contextMetadata.GetNeedCleanup();
                    }

                    if (needCleanup == null)
                    {
                        // Previous run already got here, so bail
                        continue;
                    }

                    // TODO: this doesn't work with non-RESP impls... which maybe we don't care about?
                    using var cleanupSession = (RespServerSession)getCleanupSession();
                    if (cleanupSession.activeDbId != dbId && !cleanupSession.TrySwitchActiveDatabaseSession(dbId))
                    {
                        throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
                    }

                    PostDropCleanupFunctions callbacks = new(cleanupSession.storageSession, needCleanup);

                    ref var ctx = ref cleanupSession.storageSession.vectorContext;

                    // Scan whole keyspace (sigh) and remove any associated data
                    //
                    // We don't really have a choice here, just do it
                    _ = ctx.Session.Iterate(ref callbacks);

                    lock (this)
                    {
                        foreach (var cleanedUp in needCleanup)
                        {
                            contextMetadata.FinishedCleaningUp(cleanedUp);
                        }
                    }

                    UpdateContextMetadata(ref ctx);
                }
                catch (Exception e)
                {
                    logger?.LogError(e, "Failure during background cleanup of deleted vector sets, implies storage leak");
                }
            }
        }

        /// <summary>
        /// After an index is dropped, called to start the process of removing ancillary data (elements, neighbor lists, attributes, etc.).
        /// </summary>
        internal void CleanupDroppedIndex<TContext>(ref TContext ctx, ReadOnlySpan<byte> index)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            ReadIndex(index, out var context, out _, out _, out _, out _, out _, out _, out _);

            CleanupDroppedIndex(ref ctx, context);
        }

        /// <summary>
        /// After an index is dropped, called to start the process of removing ancillary data (elements, neighbor lists, attributes, etc.).
        /// </summary>
        internal void CleanupDroppedIndex<TContext>(ref TContext ctx, ulong context)
            where TContext : ITsavoriteContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions, MainStoreFunctions, MainStoreAllocator>
        {
            lock (this)
            {
                contextMetadata.MarkCleaningUp(context);
            }

            UpdateContextMetadata(ref ctx);

            // Wake up cleanup task
            var writeRes = cleanupTaskChannel.Writer.TryWrite(null);
            Debug.Assert(writeRes, "Request for cleanup failed, this should never happen");
        }

    }
}