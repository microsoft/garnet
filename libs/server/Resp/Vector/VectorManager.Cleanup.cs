// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Methods related to cleaning up data after a Vector Set is deleted.
    /// </summary>
    public sealed partial class VectorManager
    {
        /// <summary>
        /// Used as part of scanning post-index-delete to cleanup abandoned data.
        /// </summary>
        private sealed class PostDropCleanupFunctions : IScanIteratorFunctions
        {
            private readonly StorageSession storageSession;
            private readonly FrozenSet<ulong> contexts;

            public PostDropCleanupFunctions(StorageSession storageSession, HashSet<ulong> contexts)
            {
                this.contexts = contexts.ToFrozenSet();
                this.storageSession = storageSession;
            }

            public void OnException(Exception exception, long numberOfRecords) { }
            public bool OnStart(long beginAddress, long endAddress) => true;
            public void OnStop(bool completed, long numberOfRecords) { }

            /// <inheritdoc/>
            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                if (!logRecord.HasNamespace)
                {
                    // Not Vector Set, ignore
                    cursorRecordResult = CursorRecordResult.Skip;
                    return true;
                }

                var namespaceBytes = logRecord.NamespaceBytes;
                if (namespaceBytes.Length is not (sizeof(byte) or sizeof(uint)))
                {
                    // Not Vector Set, ignore
                    cursorRecordResult = CursorRecordResult.Skip;
                    return true;
                }

                var ns = ExtractContextFromNamespaces(namespaceBytes);

                // We only store the _first_ context in a batch of related contexts to delete
                // so mask it down to just the first context
                var pairedContext = ns & ~(ContextStep - 1);
                if (!contexts.Contains(pairedContext))
                {
                    // Not a target vector set, ignore
                    cursorRecordResult = CursorRecordResult.Skip;
                    return true;
                }

                VectorElementKey toDeleteKey = new(namespaceBytes, logRecord.KeyBytes);

                // Delete it
                var status = storageSession.vectorBasicContext.Delete(toDeleteKey, 0);
                if (status.IsPending)
                {
                    VectorOutput ignored = new();
                    CompletePending(ref status, ref ignored, ref storageSession.vectorBasicContext);
                }

                Debug.Assert(status.IsCompletedSuccessfully, "Nothing else should be deleting namespaced keys");

                cursorRecordResult = CursorRecordResult.Accept;
                return true;
            }
        }

        private readonly VectorSetCleanupWorkChannel<object> cleanupTaskChannel;
        private readonly VectorSetCleanupWorkChannel<ulong> requestCleanupTaskChannel;
        private readonly VectorSetCleanupWorkChannel<object> requestDropTaskChannel;
        private readonly VectorSetCleanupWorkSet<(ulong Context, nint IndexPtr)> requestedDrops;
        private readonly ConcurrentDictionary<ulong, byte[]> potentiallyDeleted;
        private readonly Task cleanupTask;
        private readonly Task requestCleanupTask;
        private readonly Task requestDropTask;
        private readonly Func<IMessageConsumer> getTempSession;

        private bool requestCleanupTaskRunning;
        private int postCheckpointTasksRunning;

        // Pause / resume coordination for the cleanup task vs concurrent Reset.
        //
        // Cluster re-attach paths (ReplicaDisklessSync / ReplicaDiskbasedSync) call
        // storeWrapper.Reset() which tears down and rebuilds the main-store allocator.
        // The cleanup task's iterator path is safe (Tsavorite's Initializing flag causes
        // it to terminate cleanly). However the cleanup task ALSO does post-iterate RMWs
        // on metadata records (ClearDeleteInProgress / UpdateContextMetadata) — those
        // RMWs are NOT Reset-resilient and can dereference freed pagePointers and AVE.
        //
        // The pause/resume API serializes the entire cleanup-iteration (iterate + RMWs)
        // with Reset by holding cleanupGate around the whole loop body, restoring Reset's
        // documented "store is quiesced" contract.
        //
        // SemaphoreSlim used as an async-friendly mutex (initialCount=1, maxCount=1):
        // the cleanup loop takes it around each iteration; PauseCleanupAsync takes it
        // and holds until ResumeCleanup releases. Drops still enqueue items into
        // cleanupTaskChannel during a pause — the cleanup task wakes, awaits the gate
        // until the pause is lifted, then processes the backlog.
        //
        // Contract: PauseCleanupAsync callers MUST balance every successful invocation
        // with ResumeCleanup, ideally in a finally block. A held pause at Dispose time
        // would deadlock shutdown.
        private readonly SemaphoreSlim cleanupGate = new(initialCount: 1, maxCount: 1);

        /// <summary>
        /// Separate task that handles requests to drop the DiskANN side of indexes.
        /// 
        /// This needs to be in the background because we can't drop DiskANN indexes while
        /// they are in use, which means we can't drop them in response to <see cref="GarnetRecordTriggers"/>.
        /// 
        /// An additional subtlety is that indexes which are requested to be dropped cannot be recreated
        /// until that drop is processed.
        /// </summary>
        private async Task RunRequestDropTaskAsync()
        {
            while (await requestDropTaskChannel.WaitToReadAsync().ConfigureAwait(false))
            {
                // Every pass services the whole of requestedDrops, so the backlog collapses into one pass
                requestDropTaskChannel.DrainPending();

                // TODO: this doesn't work with non-RESP impls... which maybe we don't care about?
                using var dropSession = (RespServerSession)getTempSession();
                if (dropSession.activeDbId != dbId && !dropSession.TrySwitchActiveDatabaseSession(dbId))
                {
                    throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
                }

                ActiveThreadSession = dropSession.storageSession;
                try
                {
                    // Process all pending drops
                    foreach (var (k, (context, indexPtr)) in requestedDrops)
                    {
                        long keyHash;
                        unsafe
                        {
                            fixed (byte* keyPtr = k)
                            {
                                keyHash = GarnetKeyComparer.StaticGetHashCode64((FixedSpanByteKey)PinnedSpanByte.FromPinnedPointer(keyPtr, k.Length));
                            }
                        }

                        vectorSetLocks.AcquireExclusiveLock(keyHash, out var lockToken);

                        try
                        {
                            Service.DropIndex(context, indexPtr);
                        }
                        finally
                        {
                            vectorSetLocks.ReleaseLock(lockToken);
                            if (!requestedDrops.TryComplete(k))
                            {
                                logger?.LogCritical("Drop for {key} raced with some other cleanup, this should never happen", SpanByte.ToShortString(k));
                            }
                        }
                    }
                }
                finally
                {
                    ActiveThreadSession = null;
                }
            }
        }

        /// <summary>
        /// Separate task that allows for marking Vector Sets contexts as needing cleanup.
        /// 
        /// Cleanup is actually done by the <see cref="RunCleanupTaskAsync"/>.
        /// 
        /// Separating the two states allows for durable deletion logic, as we can block
        /// deletion of Vector Sets until the context is marked as needing deletion.
        /// </summary>
        private async Task RunRequestCleanupTaskAsync()
        {
            while (await requestCleanupTaskChannel.WaitToReadAsync().ConfigureAwait(false))
            {
                Volatile.Write(ref requestCleanupTaskRunning, true);

                // We do not need to take the cleanupGate here because we block in an OnDispose callback 
                // for this task to make progress.
                //
                // The fact that we're in an OnDispose means Reset() isn't running.

                try
                {
                    // TODO: this doesn't work with non-RESP impls... which maybe we don't care about?
                    using var cleanupSession = (RespServerSession)getTempSession();
                    if (cleanupSession.activeDbId != dbId && !cleanupSession.TrySwitchActiveDatabaseSession(dbId))
                    {
                        throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
                    }

                    ref var delCtx = ref cleanupSession.storageSession.vectorBasicContext;

                    var needsUpdate = false;
                    lock (this)
                    {
                        // Read all pending requests so we can do one update
                        while (requestCleanupTaskChannel.TryRead(out var context))
                        {
                            var (contextIndex, contextValue) = ContextMetadata.DecomposeContext(context);
                            if (!contextMetadatas[contextIndex].IsCleaningUp(contextIndex != 0, contextValue))
                            {
                                contextMetadatas[contextIndex].MarkCleaningUp(contextIndex != 0, contextValue);

                                _ = dirtyContextMetadatas.Add(contextIndex);

                                needsUpdate = true;
                            }
                        }
                    }

                    if (needsUpdate)
                    {
                        UpdateContextMetadata(ref delCtx);
                    }

                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_3);

                    // Pump the cleanup task once we're done
                    _ = cleanupTaskChannel.TryPublish();
                }
                catch (Exception e)
                {
                    logger?.LogError(e, "During request cleanup task");
                }
                finally
                {
                    Volatile.Write(ref requestCleanupTaskRunning, false);
                }
            }
        }

        /// <summary>
        /// Perform cleanup of deleted Vector Set element keys.
        /// 
        /// What needs cleanup is tracked as part of <see cref="ContextMetadata"/>.
        /// </summary>
        private async Task RunCleanupTaskAsync()
        {
            while (await cleanupTaskChannel.WaitToReadAsync().ConfigureAwait(false))
            {
                await cleanupGate.WaitAsync().ConfigureAwait(false);

                // Each item is one outstanding scan
                if (!cleanupTaskChannel.TryRead(out _))
                {
                    continue;
                }

                try
                {
                    // TODO: this doesn't work with non-RESP impls... which maybe we don't care about?
                    using var cleanupSession = (RespServerSession)getTempSession();
                    if (cleanupSession.activeDbId != dbId && !cleanupSession.TrySwitchActiveDatabaseSession(dbId))
                    {
                        throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
                    }

                    // Scan context needs to know how to handle objects and all callbacks, while VectorSessionFunctions is intentionally kept svelte
                    //
                    // So we use to different contexts, one to scan (strings) and one to delete (vectors)
                    ref var scanCtx = ref cleanupSession.storageSession.stringBasicContext;
                    ref var delCtx = ref cleanupSession.storageSession.vectorBasicContext;

                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_1);

                    HashSet<ulong> needCleanup = null;
                    lock (this)
                    {
                        for (var i = 0; i < contextMetadatas.Length; i++)
                        {
                            var subCleanup = contextMetadatas[i].GetNeedCleanup();
                            if (subCleanup != null)
                            {
                                var offset = ContextMetadata.OffsetForContextMetadata(i);

                                needCleanup ??= [];
                                foreach (var item in subCleanup)
                                {
                                    _ = needCleanup.Add(offset + item);
                                }
                            }
                        }
                    }

                    if (needCleanup == null)
                    {
                        // Previous run already got here, so bail
                        continue;
                    }

                    PostDropCleanupFunctions callbacks = new(cleanupSession.storageSession, needCleanup);

                    // Scan whole keyspace and remove any associated data using a snapshot
                    // lookup-based push iterator. This avoids building a parallel tempKv (which
                    // would cost memory proportional to the keyspace) — IterateLookupSnapshot
                    // walks the log and uses hash-chain liveness checks bounded to the snapshot's
                    // TailAddress, so concurrent RCUs don't drop records.
                    _ = scanCtx.Session.IterateLookupSnapshot(ref callbacks);

                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_2);

                    lock (this)
                    {
                        foreach (var cleanedUp in needCleanup)
                        {
                            var (contextIndex, contextValue) = ContextMetadata.DecomposeContext(cleanedUp);
                            contextMetadatas[contextIndex].FinishedCleaningUp(contextIndex != 0, contextValue);

                            _ = dirtyContextMetadatas.Add(contextIndex);
                        }
                    }

                    UpdateContextMetadata(ref delCtx);
                }
                catch (Exception e)
                {
                    logger?.LogError(e, "Failure during background cleanup of deleted vector sets, implies storage leak");
                }
                finally
                {
                    _ = cleanupGate.Release();
                }
            }
        }

        /// <summary>
        /// Block any new cleanup-task iteration from starting and wait for the current one
        /// (if any) to finish. Callers (e.g., cluster re-attach paths) MUST balance every
        /// invocation with <see cref="ResumeCleanup"/>, ideally in a finally block.
        ///
        /// While paused, drops still publish to <see cref="cleanupTaskChannel"/>;
        /// the cleanup task wakes, awaits the gate until the pause is lifted, then
        /// processes the backlog — so no work is lost.
        ///
        /// Use this before invoking <see cref="StoreWrapper.Reset"/> on a running store, to
        /// avoid the cleanup-task scan iterator racing with the allocator teardown.
        ///
        /// The optional <paramref name="cancellationToken"/> aborts the wait if the cleanup
        /// task is mid-iteration over a large keyspace and the caller (e.g., cluster
        /// re-attach) needs to give up. If cancellation throws <see cref="OperationCanceledException"/>,
        /// the gate was NOT acquired and the caller MUST NOT call <see cref="ResumeCleanup"/>.
        /// </summary>
        public Task PauseCleanupAsync(CancellationToken cancellationToken = default)
            => cleanupGate.WaitAsync(cancellationToken);

        /// <summary>
        /// Lift the pause acquired by <see cref="PauseCleanupAsync"/>. Queued cleanup
        /// events resume processing immediately. Must be called exactly once per
        /// successful PauseCleanupAsync — typically from a finally block.
        /// </summary>
        public void ResumeCleanup() => cleanupGate.Release();

        /// <summary>
        /// True if a pending request to drop the DiskANN index behind this _specific_ key exists.
        /// </summary>
        public bool DropRequested(ReadOnlySpan<byte> key) => requestedDrops.Contains(key);

        /// <summary>
        /// Block until <see cref="DropRequested(ReadOnlySpan{byte})"/> would return false.
        /// 
        /// Do not call this while holding any Vector Set related locks, we will deadlock.
        /// </summary>
        public void WaitForDiskANNIndexDrop(ReadOnlySpan<byte> key) => requestedDrops.WaitForCompletion(key);

        /// <summary>
        /// For use during recovery, wait for any background processing (deletion, cleanup, cleanup requests, etc.) to finish.
        /// </summary>
        internal void WaitForQuiescence()
        {
            while (true)
            {
                // We care that all these actions are quiet in a row (indicating that work queued before this call has finished)
                // NOT that they are all quiet at the same time

                var hasPendingDrops = !requestedDrops.IsEmpty;
                var hasPendingReconciliation = !potentiallyDeleted.IsEmpty;
                var hasPendingVaddsAwaitingReplay = !replicationBlockEvent.Wait(0);
                var hasPendingPostCheckpointTask = Interlocked.CompareExchange(ref postCheckpointTasksRunning, 0, 0) != 0;

                // We don't remove from the channel until setting requestCleanupTaskRunning
                var hasPendingCleanupRequests = Volatile.Read(ref requestCleanupTaskRunning) || requestCleanupTaskChannel.HasPending;

                var hasPendingCleanup = cleanupTaskChannel.HasPending;
                if (!hasPendingCleanup)
                {
                    // Acquire and immediately release to ensure cleanup task itself is quiescent
                    if (cleanupGate.Wait(0))
                    {
                        _ = cleanupGate.Release();
                    }
                    else
                    {
                        // Cleanup task is (probably) active since gate is held
                        hasPendingCleanup = true;
                    }
                }

                var quiescent = !hasPendingDrops && !hasPendingReconciliation && !hasPendingVaddsAwaitingReplay && !hasPendingPostCheckpointTask && !hasPendingCleanupRequests && !hasPendingCleanup;
                if (quiescent)
                {
                    return;
                }

                _ = Thread.Yield();
            }
        }

        /// <summary>
        /// Called when a Vector Set is discovered (typically via compaction) to _potentially_ be deleted.
        /// 
        /// Contexts and keys are retained for a final liveliness check when checkpointing completes.
        /// </summary>
        public void VectorSetPotentiallyDeleted(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (value.Length != Index.Size)
            {
                logger?.LogError("Unexpected index size on Vector Set during compaction, {actual} != {expected}", value.Length, Index.Size);
                return;
            }

            ReadIndex(value, out var context, out _, out _, out _, out _, out _, out _, out var flags, out _);

            // Record _may_ be dead, but does not imply anything about the Vector Set if it is
            if (flags.HasFlag(VectorSetFlags.SuppressCleanup))
            {
                return;
            }

            potentiallyDeleted[context] = key.ToArray();
        }

        /// <summary>
        /// Called when a checkpoint completes, signalling that Vector Sets passed to <see cref="VectorSetPotentiallyDeleted(ReadOnlySpan{byte}, ReadOnlySpan{byte})"/> should be processed.
        /// </summary>
        public unsafe void CheckpointCompleted()
        {
            _ = Interlocked.Increment(ref postCheckpointTasksRunning);
            _ = Task.Run(() => QueueCleanups(this));

            static void QueueCleanups(VectorManager self)
            {
                try
                {
                    using var session = (RespServerSession)self.getTempSession();

                    // Just need a Vector Set command, which one doesn't matter
                    StringInput input = new(RespCommand.VINFO);
                    input.parseState.Initialize(1);

                    Span<byte> indexSpan = stackalloc byte[Index.Size];
                    var indexMem = SpanByteAndMemory.FromPinnedSpan(indexSpan);
                    StringOutput output = new(indexMem);

                    while (!self.potentiallyDeleted.IsEmpty)
                    {
                        foreach (var (context, key) in self.potentiallyDeleted)
                        {
                            if (self.potentiallyDeleted.TryRemove(context, out _))
                            {
                                bool needsDelete;

                                fixed (byte* keyPtr = key)
                                {
                                    ReadOnlySpan<byte> keySpan = new(keyPtr, key.Length);
                                    input.parseState.SetArgument(0, PinnedSpanByte.FromPinnedSpan(keySpan));

                                    var status = session.storageSession.Read_MainStore(key, ref input, ref output, ref session.storageSession.stringBasicContext);

                                    if (status != GarnetStatus.OK || !output.SpanByteAndMemory.IsSpanByte || output.SpanByteAndMemory.Length != Index.Size)
                                    {
                                        // WRONGTYPE or missing means the index is not longer live, and a wrong-sized value means we're corrupted somehow
                                        needsDelete = true;
                                    }
                                    else
                                    {
                                        // If the _context_ on this record has changed, that also means the old Vector Set is dead
                                        ReadIndex(output.SpanByteAndMemory.Span, out var liveContext, out _, out _, out _, out _, out _, out _, out _, out _);
                                        needsDelete = liveContext != context;
                                    }
                                }

                                if (needsDelete)
                                {
                                    // No need to wait for marking, since the record is already "deleted"
                                    if (!self.requestCleanupTaskChannel.TryPublish(context))
                                    {
                                        self.logger?.LogWarning("Could not request delete of abandoned Vector Set {key}", SpanByte.ToShortString(key));
                                    }
                                }

                                if (!output.SpanByteAndMemory.IsSpanByte || output.SpanByteAndMemory.Length != Index.Size)
                                {
                                    output.SpanByteAndMemory.Dispose();
                                    output = new(indexMem);
                                }
                            }
                        }
                    }
                }
                finally
                {
                    _ = Interlocked.Decrement(ref self.postCheckpointTasksRunning);
                }
            }
        }
    }
}