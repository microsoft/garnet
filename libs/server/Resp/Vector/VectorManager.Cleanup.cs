// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
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

                // TODO: Implement variable length namespace support
                Debug.Assert(logRecord.Namespace.Length == 1, "Variable length namespaces not supported");

                ulong ns = logRecord.Namespace[0];
                var pairedContext = ns & ~(ContextStep - 1);
                if (!contexts.Contains(pairedContext))
                {
                    // Vector Set, but not one we're scanning for
                    cursorRecordResult = CursorRecordResult.Skip;
                    return true;
                }

                // Delete it
                ReadOnlySpan<byte> nsBytes = [(byte)ns];
                VectorElementKey toDeleteKey = new(nsBytes, logRecord.KeyBytes);

                var status = storageSession.vectorBasicContext.Delete(toDeleteKey, 0);
                if (status.IsPending)
                {
                    VectorOutput ignored = new();
                    CompletePending(ref status, ref ignored, ref storageSession.vectorBasicContext);
                }

                cursorRecordResult = CursorRecordResult.Accept;
                return true;
            }
        }

        private readonly Channel<object> cleanupTaskChannel;
        private readonly Channel<(ulong Context, TaskCompletionSource MarkCompleted)> requestCleanupTaskChannel;
        private readonly Channel<object> requestDropTaskChannel;
        private ConcurrentDictionary<byte[], (ulong Context, nint IndexPtr)> requestedDrops;
#if NET9_0_OR_GREATER
        private ConcurrentDictionary<byte[], (ulong Context, nint IndexPtr)>.AlternateLookup<ReadOnlySpan<byte>> requestedDropsLookup;
#endif
        private readonly Task cleanupTask;
        private readonly Task requestCleanupTask;
        private readonly Task requestDropTask;
        private readonly Func<IMessageConsumer> getCleanupSession;

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
        /// An additional subtlty is that indexes which are requested to be dropped cannot be recreated
        /// until that drop is processed.
        /// </summary>
        private async Task RunRequestDropTaskAsync()
        {
            while (await requestDropTaskChannel.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                // Drain all wake up signals
                while (requestDropTaskChannel.Reader.TryRead(out _))
                {
                }

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
                        if (!requestedDrops.TryRemove(k, out _))
                        {
                            logger?.LogCritical("Drop for {key} raced with some other cleanup, this should never happen", SpanByte.ToShortString(k));
                        }
                    }
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
            while (await requestCleanupTaskChannel.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                // We do not need to take the cleanupGate here because we block in an OnDispose callback 
                // for this task to make progress.
                //
                // The fact that we're in an OnDispose means Reset() isn't running.

                var completions = new List<TaskCompletionSource>();

                try
                {
                    // TODO: this doesn't work with non-RESP impls... which maybe we don't care about?
                    using var cleanupSession = (RespServerSession)getCleanupSession();
                    if (cleanupSession.activeDbId != dbId && !cleanupSession.TrySwitchActiveDatabaseSession(dbId))
                    {
                        throw new GarnetException($"Could not switch VectorManager cleanup session to {dbId}, initialization failed");
                    }

                    ref var delCtx = ref cleanupSession.storageSession.vectorBasicContext;

                    var needsUpdate = false;
                    lock (this)
                    {
                        // Read all pending requests so we can do one update
                        while (requestCleanupTaskChannel.Reader.TryRead(out var t))
                        {
                            if (t.MarkCompleted != null)
                            {
                                completions.Add(t.MarkCompleted);
                            }

                            if (!contextMetadata.IsCleaningUp(t.Context))
                            {
                                contextMetadata.MarkCleaningUp(t.Context);

                                needsUpdate = true;
                            }
                        }
                    }

                    if (needsUpdate)
                    {
                        UpdateContextMetadata(ref delCtx);
                    }

                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_3);

                    foreach (var completion in completions)
                    {
                        try
                        {
                            _ = completion.TrySetResult();
                        }
                        catch (Exception innerE)
                        {
                            logger?.LogError(innerE, "While completing Vector Set cleanup request");
                        }
                    }

                    // Pump the cleanup task once we're done
                    _ = cleanupTaskChannel.Writer.TryWrite(null);
                }
                catch (Exception e)
                {
                    foreach (var completion in completions)
                    {
                        try
                        {
                            _ = completion.TrySetException(e);
                        }
                        catch (Exception innerE)
                        {
                            // Best effort
                            logger?.LogError(innerE, "While cancelling Vector Set cleanup requests");
                        }
                    }
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
            // Each drop index will queue a null object here
            // We'll handle multiple at once if possible, but using a channel simplifies cancellation and dispose
            await foreach (var ignored in cleanupTaskChannel.Reader.ReadAllAsync().ConfigureAwait(false))
            {
                await cleanupGate.WaitAsync().ConfigureAwait(false);

                try
                {
                    // TODO: this doesn't work with non-RESP impls... which maybe we don't care about?
                    using var cleanupSession = (RespServerSession)getCleanupSession();
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
                            contextMetadata.FinishedCleaningUp(cleanedUp);
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
        /// While paused, drops still enqueue items into <see cref="cleanupTaskChannel"/>;
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
        public bool DropRequested(ReadOnlySpan<byte> key)
        {
#if NET9_0_OR_GREATER
            return requestedDropsLookup.ContainsKey(key);
#else
            return requestedDrops.ContainsKey(key.ToArray());
#endif
        }

        /// <summary>
        /// Block until <see cref="DropRequested(ReadOnlySpan{byte})"/> would return false.
        /// 
        /// Do not call this while holding any Vector Set related locks, we will deadlock.
        /// </summary>
        public void WaitForDiskANNIndexDrop(ReadOnlySpan<byte> key)
        {
#if NET9_0_OR_GREATER
            while (requestedDropsLookup.ContainsKey(key))
#else
            var keyBytes = key.ToArray();
            while (requestedDrops.ContainsKey(keyBytes))
#endif
            {
                _ = Thread.Yield();
            }
        }
    }
}