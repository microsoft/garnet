// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
                VectorElementKey toDeleteKey = new((byte)ns, logRecord.KeyBytes);

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

        private readonly Channel<(ulong Context, TaskCompletionSource TCS)> cleanupTaskChannel;
        private readonly Task cleanupTask;
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

        private async Task RunCleanupTaskAsync()
        {
            // Each drop index will queue a null object here
            // We'll handle multiple at once if possible, but using a channel simplifies cancellation and dispose
            await foreach (var (context, tcs) in cleanupTaskChannel.Reader.ReadAllAsync())
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

                    // Sometimes we just want to pump the cleanup task - in that case, we use InvalidContext to indicate we don't need any real work
                    if (context != InvalidContext)
                    {
                        var needsUpdate = false;
                        lock (this)
                        {
                            if (!contextMetadata.IsCleaningUp(context))
                            {
                                contextMetadata.MarkCleaningUp(context);
                                needsUpdate = true;
                            }
                        }

                        if (needsUpdate)
                        {
                            UpdateContextMetadata(ref delCtx);
                        }
                    }

                    ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.VectorSet_Interrupt_Delete_1);

                    HashSet<ulong> needCleanup;
                    lock (this)
                    {
                        needCleanup = contextMetadata.GetNeedCleanup();
                    }

                    if (needCleanup == null)
                    {
                        // Previous run already got here, so bail

                        tcs?.SetResult();
                        continue;
                    }

                    if (vectorSetIndexKeyRecovery != null)
                    {
                        foreach (var toCleanupContext in needCleanup)
                        {
                            if (vectorSetIndexKeyRecovery.TryRemove(toCleanupContext, out var keyBytes))
                            {
                                unsafe
                                {
                                    fixed (byte* keyPtr = keyBytes)
                                    {
                                        var res = scanCtx.Delete((FixedSpanByteKey)SpanByte.FromPinnedPointer(keyPtr, keyBytes.Length));
                                        if (res.IsPending)
                                        {
                                            CompletePending(ref res, ref scanCtx);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Cleanup state has been updated, inform whoever sent the cleanup request if they asked for that notification
                    tcs?.SetResult();

                    PostDropCleanupFunctions callbacks = new(cleanupSession.storageSession, needCleanup);

                    // Scan whole keyspace (sigh) and remove any associated data
                    //
                    // We don't really have a choice here, just do it
                    _ = scanCtx.Session.Iterate(ref callbacks);

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
                    tcs?.TrySetException(e);
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
        /// After an index is dropped, called to start the process of removing ancillary data (elements, neighbor lists, attributes, etc.).
        /// </summary>
        internal void CleanupDroppedIndex(ref VectorBasicContext ctx, ulong context)
        {
            // Wake up cleanup task
            var writeRes = cleanupTaskChannel.Writer.TryWrite((InvalidContext, null));
            Debug.Assert(writeRes, "Request for cleanup failed, this should never happen");
        }

        /// <summary>
        /// Detects if a Vector Set index read out of the main store is in the middle of being deleted.
        /// </summary>
        private static bool PartiallyDeleted(ReadOnlySpan<byte> indexConfig)
        {
            ReadIndex(indexConfig, out var context, out _, out _, out _, out _, out _, out _, out _, out _);
            return context == 0;
        }
    }
}