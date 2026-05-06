// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

        private readonly Channel<object> cleanupTaskChannel;
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
            await foreach (var ignored in cleanupTaskChannel.Reader.ReadAllAsync())
            {
                await cleanupGate.WaitAsync().ConfigureAwait(false);
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

                    // Scan context needs to know how to handle objects and all callbacks, while VectorSessionFunctions is intentionally kept svelte
                    //
                    // So we use to different contexts, one to scan (strings) and one to delete (vectors)
                    ref var scanCtx = ref cleanupSession.storageSession.stringBasicContext;
                    ref var delCtx = ref cleanupSession.storageSession.vectorBasicContext;

                    // Scan whole keyspace (sigh) and remove any associated data
                    //
                    // We don't really have a choice here, just do it
                    _ = scanCtx.Session.Iterate(ref callbacks);

                    // Key is mostly ignored when deleting from InProgressDeletes
                    // So we just need a non-empty one to use with the context
                    Span<byte> basicKeySpan = new byte[1];
                    unsafe
                    {
                        fixed (byte* basicKeyPtr = basicKeySpan)
                        {
                            var basicKey = SpanByte.FromPinnedPointer(basicKeyPtr, basicKeySpan.Length);

                            // Generally there will already be removed, but if deletes fail in odd spots there can
                            // be a little bit to cleanup - so go ahead and do it.
                            //
                            // Not really worth optimizing given that we just scanned the whole key space to remove elements
                            // and that will dominate.
                            foreach (var cleanedUp in needCleanup)
                            {
                                ClearDeleteInProgress(ref delCtx, basicKey, cleanedUp);
                            }
                        }
                    }

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
        /// Called in response to <see cref="TryMarkDeleteInProgress"/> or <see cref="ClearDeleteInProgress"/> to update metadata in Tsavorite.
        /// 
        /// Returns false if there is insufficient size for the value.
        /// </summary>
        internal static bool TryUpdateInProgressDeletes(Span<byte> updateMessage, ref LogRecord recordInfo, in RecordSizeInfo sizeInfo)
        {
            var context = BinaryPrimitives.ReadUInt64LittleEndian(updateMessage);
            var len = BinaryPrimitives.ReadInt32LittleEndian(updateMessage[sizeof(ulong)..]);
            var isAdding = len > 0;
            var key = updateMessage[(sizeof(ulong) + sizeof(int))..];

            Debug.Assert(key.Length == (isAdding ? len : -len), "Key length not expected");
            Debug.Assert(context is >= ContextStep, "Special context not allowed");

            var remaining = recordInfo.ValueSpan;
            while (remaining.Length >= sizeof(ulong) + sizeof(int))
            {
                var curCtx = BinaryPrimitives.ReadUInt64LittleEndian(remaining);

                if (curCtx == 0)
                {
                    // Reached uninitialized data
                    break;
                }

                var curLen = BinaryPrimitives.ReadInt32LittleEndian(remaining[sizeof(ulong)..]);
                if (curCtx == context)
                {
                    if (isAdding)
                    {
                        // Already added, ignore and make no other changes
                        return true;
                    }

                    // Copy later values to cover the one we're removing
                    var afterCur = remaining[(sizeof(ulong) + sizeof(int) + curLen)..];
                    afterCur.CopyTo(remaining);

                    // Clear everything after that so we won't think it's valid
                    remaining[^(sizeof(ulong) + sizeof(int) + curLen)..].Clear();

                    // Shrink record by removed chunk size
                    var newSizeInfo = sizeInfo;
                    newSizeInfo.FieldInfo.ValueSize -= sizeof(ulong) + sizeof(int) + curLen;
                    newSizeInfo.CalculateSizes(newSizeInfo.FieldInfo.KeySize, newSizeInfo.FieldInfo.ValueSize);

                    var shrinkRes = recordInfo.TrySetContentLengths(in newSizeInfo);
                    Debug.Assert(shrinkRes, "Should never fail to shrink");

                    return true;
                }

                remaining = remaining[(sizeof(ulong) + sizeof(int) + curLen)..];
            }

            if (isAdding)
            {
                if (remaining.Length < sizeof(ulong) + sizeof(int) + key.Length)
                {
                    return false;
                }

                // Not already added, so slap it in
                BinaryPrimitives.WriteUInt64LittleEndian(remaining, context);
                BinaryPrimitives.WriteInt32LittleEndian(remaining[sizeof(ulong)..], len);

                key.CopyTo(remaining[(sizeof(ulong) + sizeof(int))..]);

                remaining = remaining[(sizeof(ulong) + sizeof(int) + key.Length)..];

                // Record used length
                var newSize = recordInfo.ValueSpan.Length - remaining.Length;
                var newSizeInfo = sizeInfo;
                newSizeInfo.FieldInfo.ValueSize = newSize;
                newSizeInfo.CalculateSizes(newSizeInfo.FieldInfo.KeySize, newSizeInfo.FieldInfo.ValueSize);

                var growRes = recordInfo.TrySetContentLengths(in newSizeInfo);
                Debug.Assert(growRes, "Should have reserved enough space for this to not fail");
            }

            return true;
        }

        /// <summary>
        /// Before we start smashing a <see cref="Index"/> for deletion, records that we started to delete it so we can recover from crashes.
        /// </summary>
        internal bool TryMarkDeleteInProgress(ref VectorBasicContext ctx, ReadOnlySpan<byte> key, ulong context)
        {
            Span<byte> dataSpan = stackalloc byte[sizeof(ulong) + sizeof(int) + key.Length];
            BinaryPrimitives.WriteUInt64LittleEndian(dataSpan, context);

            // Positive length indicates we're adding this to the list
            BinaryPrimitives.WriteInt32LittleEndian(dataSpan[sizeof(ulong)..], key.Length);
            key.CopyTo(dataSpan[(sizeof(ulong) + sizeof(int))..]);

            // 1 is InProgressDeletes
            VectorElementKey inProgressDeletesKey = new(MetadataNamespace, [1]);

            VectorInput input = default;
            input.Callback = 0;

            // Negative to indicate dynamic-ness
            input.WriteDesiredSize = -(sizeof(ulong) + sizeof(int) + key.Length);
            unsafe
            {
                input.CallbackContext = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dataSpan));
            }

            var status = ctx.RMW(inProgressDeletesKey, ref input);

            if (status.IsPending)
            {
                VectorOutput ignored = new();
                CompletePending(ref status, ref ignored, ref ctx);
            }

            return status.IsCompletedSuccessfully;
        }

        /// <summary>
        /// Enumerate any deletes of Vector Sets that are in progress.
        /// 
        /// Used with <see cref="TryMarkDeleteInProgress"/> and <see cref="ClearDeleteInProgress"/> to recover from interrupted deletes.
        /// </summary>
        internal List<(ReadOnlyMemory<byte> Key, ulong Context)> GetDeletesInProgress(StorageSession storageSession)
        {
            SpanByteAndMemory readValue = default;

            List<(ReadOnlyMemory<byte> Key, ulong Context)> ret = [];
            try
            {
                ActiveThreadSession = storageSession;
                try
                {
                    // 1 is InProgressDeletes
                    // Note that ReadSizeUnknown will attach the namespace for us
                    if (!ReadSizeUnknown(context: MetadataNamespace, forceAlignment: false, [1], ref readValue))
                    {
                        return ret;
                    }
                }
                finally
                {
                    ActiveThreadSession = null;
                }

                var remaining = readValue.ReadOnlySpan;
                while (remaining.Length >= sizeof(ulong) + sizeof(int))
                {
                    var ctx = BinaryPrimitives.ReadUInt64LittleEndian(remaining);
                    if (ctx == 0)
                    {
                        // Encountered uninitialized data
                        break;
                    }

                    var len = BinaryPrimitives.ReadInt32LittleEndian(remaining[sizeof(ulong)..]);

                    var key = remaining.Slice(sizeof(ulong) + sizeof(int), len);

                    ret.Add((key.ToArray(), ctx));

                    remaining = remaining[(sizeof(ulong) + sizeof(int) + len)..];
                }

                return ret;
            }
            finally
            {
                readValue.Memory?.Dispose();
            }
        }

        /// <summary>
        /// After a delete has completed, removes the given key from metadata.
        /// </summary>
        internal void ClearDeleteInProgress(ref VectorBasicContext ctx, ReadOnlySpan<byte> key, ulong context)
        {
            Span<byte> dataSpan = stackalloc byte[sizeof(ulong) + sizeof(int) + key.Length];
            BinaryPrimitives.WriteUInt64LittleEndian(dataSpan, context);

            // Negative length indicates we're removing this from the list
            BinaryPrimitives.WriteInt32LittleEndian(dataSpan[sizeof(ulong)..], -key.Length);
            key.CopyTo(dataSpan[(sizeof(ulong) + sizeof(int))..]);

            // 1 is InProgressDeletes
            VectorElementKey inProgressDeletesKey = new(MetadataNamespace, [1]);

            VectorInput input = default;
            input.Callback = 0;

            // Negative to indicate dynamic-ness
            input.WriteDesiredSize = -(sizeof(ulong) + sizeof(int) + key.Length);
            unsafe
            {
                input.CallbackContext = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(dataSpan));
            }

            var status = ctx.RMW(inProgressDeletesKey, ref input);

            if (status.IsPending)
            {
                VectorOutput ignored = new();
                CompletePending(ref status, ref ignored, ref ctx);
            }
        }

        /// <summary>
        /// After an index is dropped, called to start the process of removing ancillary data (elements, neighbor lists, attributes, etc.).
        /// </summary>
        internal void CleanupDroppedIndex(ref VectorBasicContext ctx, ulong context)
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