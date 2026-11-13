// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession : IDisposable
    {
        internal sealed partial class MigrateOperation
        {
            public readonly Sketch sketch;
            public readonly List<byte[]> keysToDelete;
            public StoreScan storeScan;

            private readonly ConcurrentDictionary<byte[], (byte[] Value, DateTime? Expiration)> vectorSetsIndexKeysToMigrate;
            private readonly ConcurrentDictionary<byte[], byte> rangeIndexKeysToMigrate;

            readonly MigrateSession session;
            readonly GarnetClientSession gcs;
            readonly LocalServerSession localServerSession;
            readonly int collectionBatchCapacity;
            readonly int deleteBatchSize;
            readonly Task<string>[] pendingResponses;
            int responseHead;
            int responseCount;
            CooperativeDisposeGuard disposeGuard;

            public GarnetClientSession Client => gcs;

            public LocalServerSession LocalSession => localServerSession;

            public int KeySendBatchSize => collectionBatchCapacity - 1;

            public IEnumerable<KeyValuePair<byte[], (byte[] Value, DateTime? Expiration)>> VectorSets => vectorSetsIndexKeysToMigrate;

            public IEnumerable<byte[]> RangeIndexKeys => rangeIndexKeysToMigrate.Keys;

            public void ThrowIfCancelled() => session.migrationCancellationToken.ThrowIfCancellationRequested();

            public bool Contains(int slot) => session.ContainsSlot(slot);

            public bool ContainsNamespace(ReadOnlySpan<byte> namespaceBytes)
            {
                // Vector Sets only use these two sizes, can ignore everything else
                if (namespaceBytes.Length is not (sizeof(byte) or sizeof(uint)))
                {
                    return false;
                }

                var ns = VectorManager.ExtractContextFromNamespaces(namespaceBytes);

                return session._namespaces?.Contains(ns) ?? false;
            }
            public void EncounteredVectorSet(byte[] key, byte[] value, DateTime? expiration)
            => vectorSetsIndexKeysToMigrate.TryAdd(key, (value, expiration));

            public void AddRangeIndexKey(byte[] key) => rangeIndexKeysToMigrate.TryAdd(key, 0);

            public MigrateOperation(MigrateSession session, Sketch sketch = null)
            {
                this.session = session;
                var keySendBatchSize = session.clusterProvider.serverOptions.MigrateKeySendBatchSize;
                // ArgSliceVector reserves one slot beyond the usable key limit.
                collectionBatchCapacity = keySendBatchSize + 1;
                deleteBatchSize = session.clusterProvider.serverOptions.MigrationDeleteBatchSize;
                pendingResponses = new Task<string>[session.clusterProvider.serverOptions.MigrationPipelineDepth];
                gcs = session.GetGarnetClient();
                localServerSession = session.GetLocalSession();
                this.sketch = sketch ?? new(keySendBatchSize);
                storeScan = new StoreScan(this);
                keysToDelete = [];
                vectorSetsIndexKeysToMigrate = new(ByteArrayComparer.Instance);
                rangeIndexKeysToMigrate = new(ByteArrayComparer.Instance);
            }

            public async ValueTask<bool> InitializeAsync()
            {
                if (!await session.CheckConnectionAsync(gcs).ConfigureAwait(false))
                    return false;
                gcs.InitializeIterationBuffer(session.clusterProvider.storeWrapper.loggingFrequency);
                return true;
            }

            /// <summary>
            /// Enter the single worker's lifetime before using its client or storage session.
            /// </summary>
            public void EnterWorker()
                => ObjectDisposedException.ThrowIf(!disposeGuard.TryEnter(), this);

            /// <summary>
            /// Exit the worker's lifetime after all child tasks and resource access have completed.
            /// </summary>
            public void ExitWorker()
            {
                if (disposeGuard.ExitAndCheckShouldCleanup())
                    DisposeResources();
            }

            public void Dispose()
            {
                var result = disposeGuard.TryDispose();
                if (result == CooperativeDisposeGuard.DisposeResult.AlreadyDisposed)
                    return;

                try
                {
                    // Closing wakes network waits without recycling a worker's borrowed serialization buffer.
                    gcs.CloseConnection();
                    if (result == CooperativeDisposeGuard.DisposeResult.CleanupNow)
                        DisposeResources();
                }
                finally
                {
                    // The worker alone owns the ring indices; disposal only observes published tasks.
                    for (var i = 0; i < pendingResponses.Length; i++)
                    {
                        var response = Volatile.Read(ref pendingResponses[i]);
                        if (response != null)
                            ObserveResponseFault(response);
                    }
                }
            }

            void DisposeResources()
            {
                try
                {
                    gcs.Dispose();
                }
                finally
                {
                    localServerSession.Dispose();
                }
            }

            static void ObserveResponseFault(Task<string> response)
                => _ = response.ContinueWith(static task => _ = task.Exception, CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

            /// <summary>
            /// Track batch responses in order, waiting when the fixed pipeline window fills.
            /// </summary>
            /// <param name="task">The response for a newly sent batch, or null if no batch was sent.</param>
            /// <param name="drain">Wait for every outstanding response before deleting keys or handing off slots.</param>
            public async Task<bool> HandleMigrateTaskResponseAsync(Task<string> task, bool drain = false)
            {
                if (task != null)
                {
                    if (responseCount == pendingResponses.Length)
                        throw new InvalidOperationException("Migration response window is full.");

                    // A full fence pairs publication with disposal so late faults are always observed.
                    _ = Interlocked.Exchange(ref pendingResponses[(responseHead + responseCount) % pendingResponses.Length], task);
                    responseCount++;
                    if (disposeGuard.IsDisposed)
                    {
                        ObserveResponseFault(task);
                        session.logger?.LogError("Cannot track migration response after operation disposal");
                        session.Status = MigrateState.FAIL;
                        return false;
                    }
                }

                while (responseCount > 0)
                {
                    var response = Volatile.Read(ref pendingResponses[responseHead]);
                    if (!drain && responseCount < pendingResponses.Length && !response.IsCompleted)
                        break;

                    try
                    {
                        var resp = await response.WaitAsync(session._timeout, session.migrationCancellationToken).ConfigureAwait(false);
                        Volatile.Write(ref pendingResponses[responseHead], null);
                        responseHead = (responseHead + 1) % pendingResponses.Length;
                        responseCount--;

                        if (!resp.Equals("OK", StringComparison.Ordinal))
                        {
                            session.logger?.LogError("ClusterMigrate Keys failed with error:{error}.", resp);
                            session.Status = MigrateState.FAIL;
                            return false;
                        }
                    }
                    catch (Exception ex)
                    {
                        session.logger?.LogError(ex, "An error has occurred");
                        session.Status = MigrateState.FAIL;
                        return false;
                    }
                }

                return true;
            }

            /// <summary>
            /// Perform scan to gather keys and build sketch
            /// </summary>
            /// <param name="currentAddress"></param>
            /// <param name="endAddress"></param>
            /// <param name="maxAddress">Snapshot boundary for newer versions across all worker ranges.</param>
            public void Scan(ref long currentAddress, long endAddress, long maxAddress)
                => localServerSession.BasicGarnetApi.IterateStore(ref storeScan, ref currentAddress, endAddress, maxAddress,
                    includeTombstones: true);

            /// <summary>
            /// Transmit gathered keys
            /// </summary>
            /// <returns></returns>
            public async Task<bool> TransmitSlotsAsync()
            {
                var output = new UnifiedOutput();
                var vectorOutput = new VectorOutput();

                try
                {
                    var input = new UnifiedInput(RespCommand.MIGRATE);

                    VectorInput vectorInput = new();
                    vectorInput.MaxMigrationHeapAllocationSize = session.NetworkBufferSettings.MaxSendBufferContentSize;

                    foreach (var (ns, key, hasNs) in sketch.argSliceVector)
                    {
                        if (hasNs)
                        {
                            // Migrating Vector Set element data
                            if (!await session.WriteOrSendRecordAsync(this, ns, key, ref vectorInput, ref vectorOutput, out _).ConfigureAwait(false))
                                return false;
                        }
                        else
                        {
                            // Migrating everything else
                            if (!await session.WriteOrSendRecordAsync(this, key, ref input, ref output, out _).ConfigureAwait(false))
                                return false;
                        }
                    }

                    // Flush final data in client buffer
                    if (!await HandleMigrateTaskResponseAsync(gcs.SendAndResetIterationBuffer(), drain: true).ConfigureAwait(false))
                        return false;
                }
                finally
                {
                    output.Dispose();
                    vectorOutput.SpanByteAndMemory.Dispose();
                }

                return true;
            }

            public async Task<bool> TransmitKeysAsync(Func<PinnedSpanByte, bool> shouldSkipKey)
            {
                // Reused across iterations so the SpanByteAndMemory.Memory can be reused; the serialized record is sent (whole
                // if it fits a send buffer, else chunked) out of epoch after each read.
                var output = new UnifiedOutput();

                try
                {
                    var keys = sketch.Keys;

                    var input = new UnifiedInput(RespCommand.MIGRATE);
                    for (var i = 0; i < keys.Count; i++)
                    {
                        if (keys[i].Item2)
                            continue;

                        // Skip keys that require special handling
                        if (shouldSkipKey(keys[i].Item1))
                            continue;

                        if (!await session.WriteOrSendRecordAsync(this, keys[i].Item1, ref input, ref output, out var status).ConfigureAwait(false))
                            return false;

                        // If key was FOUND, mark it for deletion
                        if (status != GarnetStatus.NOTFOUND)
                            keys[i] = (keys[i].Item1, true);
                    }

                    // Flush final data in client buffer
                    if (!await HandleMigrateTaskResponseAsync(gcs.SendAndResetIterationBuffer(), drain: true).ConfigureAwait(false))
                        return false;
                }
                finally
                {
                    output.Dispose();
                }
                return true;
            }

            /// <summary>
            /// Transmit data in namespaces during a MIGRATE ... KEYS operation.
            /// 
            /// Doesn't delete anything, just scans and transmits.
            /// </summary>
            public async ValueTask<bool> TransmitKeysNamespacesAsync(ILogger logger)
            {
                var migrateOperation = this;

                if (!await migrateOperation.InitializeAsync().ConfigureAwait(false))
                    return false;

                var workerStartAddress = migrateOperation.session.clusterProvider.storeWrapper.store.Log.BeginAddress;
                var workerEndAddress = migrateOperation.session.clusterProvider.storeWrapper.store.Log.TailAddress;

                var cursor = workerStartAddress;
                logger?.LogWarning("<MainStore> migrate keys (namespaces) scan range [{workerStartAddress}, {workerEndAddress}]", workerStartAddress, workerEndAddress);
                while (true)
                {
                    var current = cursor;
                    // Build Sketch
                    migrateOperation.sketch.SetStatus(SketchStatus.INITIALIZING);
                    migrateOperation.Scan(ref current, workerEndAddress, workerEndAddress);

                    // Stop if no keys have been found
                    if (migrateOperation.sketch.argSliceVector.IsEmpty) break;

                    logger?.LogWarning("Scan from {cursor} to {current} and discovered {count} keys", cursor, current, migrateOperation.sketch.argSliceVector.Count);

                    // Transition EPSM to MIGRATING
                    migrateOperation.sketch.SetStatus(SketchStatus.TRANSMITTING);
                    if (!await migrateOperation.session.WaitForConfigPropagationAsync().ConfigureAwait(false))
                    {
                        logger?.LogError("Failed to protect MIGRATE KEYS namespace batch");
                        return false;
                    }
                    migrateOperation.ThrowIfCancelled();

                    // Transmit all keys gathered
                    if (!await migrateOperation.TransmitSlotsAsync().ConfigureAwait(false))
                    {
                        logger?.LogWarning("TransmitSlots failed for {cursor} to {current} (with {count} keys)", cursor, current, migrateOperation.sketch.argSliceVector.Count);
                        return false;
                    }

                    // Transition EPSM to DELETING
                    migrateOperation.sketch.SetStatus(SketchStatus.DELETING);
                    if (!await migrateOperation.session.WaitForConfigPropagationAsync().ConfigureAwait(false))
                    {
                        logger?.LogError("Failed to protect MIGRATE KEYS namespace batch deletion");
                        return false;
                    }
                    migrateOperation.ThrowIfCancelled();

                    // Clear keys from buffer
                    migrateOperation.sketch.Clear();
                    cursor = current;
                }

                return true;
            }

            /// <summary>
            /// Delete keys after migration if copyOption is not set
            /// </summary>
            public void DeleteKeys()
            {
                if (session._copyOption)
                    return;
                var keyCount = session.transferOption == TransferOption.SLOTS ? sketch.argSliceVector.Count : sketch.Keys.Count;
                if (keyCount == 0)
                    return;
                var deleteBatchCapacity = Math.Min(deleteBatchSize, keyCount);
                const int MaxStackDeleteBatchSize = 256;
                PinnedSpanByte[] rented = null;
                Span<PinnedSpanByte> deleteBatch = deleteBatchCapacity <= MaxStackDeleteBatchSize
                    ? stackalloc PinnedSpanByte[deleteBatchCapacity]
                    : (rented = ArrayPool<PinnedSpanByte>.Shared.Rent(deleteBatchCapacity)).AsSpan(0, deleteBatchCapacity);
                try
                {
                    var deleteCount = 0;
                    if (session.transferOption == TransferOption.SLOTS)
                    {
                        var vectorKeys = vectorSetsIndexKeysToMigrate.GetAlternateLookup<ReadOnlySpan<byte>>();
                        var rangeKeys = rangeIndexKeysToMigrate.GetAlternateLookup<ReadOnlySpan<byte>>();
                        foreach (var (ns, key, hasNs) in sketch.argSliceVector)
                        {
                            if (hasNs)
                            {
                                // Namespace'd keys are deleted as part after migration completes
                                continue;
                            }
                            else
                            {
                                if (vectorKeys.ContainsKey(key.ReadOnlySpan) || rangeKeys.ContainsKey(key.ReadOnlySpan))
                                    continue;
                                deleteBatch[deleteCount++] = key;
                                if (deleteCount == deleteBatch.Length)
                                {
                                    localServerSession.DeleteKeys(deleteBatch);
                                    deleteCount = 0;
                                }
                            }
                        }
                    }
                    else
                    {
                        var keys = sketch.Keys;
                        for (var i = 0; i < keys.Count; i++)
                        {
                            // Do not delete the key if it is not marked for deletion because it has not been transmitted to the target node
                            if (!keys[i].Item2)
                                continue;
                            deleteBatch[deleteCount++] = keys[i].Item1;
                            if (deleteCount == deleteBatch.Length)
                            {
                                localServerSession.DeleteKeys(deleteBatch);
                                deleteCount = 0;
                            }
                        }
                    }
                    if (deleteCount > 0)
                        localServerSession.DeleteKeys(deleteBatch[..deleteCount]);
                }
                finally
                {
                    if (rented != null)
                        ArrayPool<PinnedSpanByte>.Shared.Return(rented);
                }
            }

            /// <summary>
            /// Delete a Vector Set after migration if _copyOption is not set.
            /// </summary>
            public void DeleteVectorSet(PinnedSpanByte key)
            {
                if (session._copyOption)
                    return;

                var delRes = localServerSession.BasicGarnetApi.DELETE(key);

                session.logger?.LogDebug("Deleting Vector Set {key} after migration: {delRes}", Encoding.UTF8.GetString(key), delRes);
            }

            /// <summary>
            /// Delete a RangeIndex after migration. COPY option is not yet supported for RangeIndex keys.
            /// </summary>
            public void DeleteRangeIndex(PinnedSpanByte key)
            {
                if (session._copyOption)
                    return;

                var delRes = localServerSession.BasicGarnetApi.DELETE(key);
                session.logger?.LogDebug("Deleted RangeIndex key {key} after migration: {delRes}", Encoding.UTF8.GetString(key), delRes);
            }
        }
    }
}