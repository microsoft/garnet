// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.server;

#if DEBUG
using Garnet.common;
#endif

using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Attempts to reserve contexts on the destination node for migrating vector sets.
        /// 
        /// This maps roughly to "for each <see cref="VectorManager.ContextStep"/> namespaces, reserve one context, record the mapping".
        /// </summary>
        public async Task<bool> ReserveDestinationVectorSetsAsync()
        {
            Debug.Assert((_namespaces.Count % (int)VectorManager.ContextStep) == 0, "Expected to be migrating Vector Sets, and thus to have an even number of namespaces");

            var neededContexts = _namespaces.Count / (int)VectorManager.ContextStep;

            try
            {
                var reservedCtxs = await migrateOperation[0].Client.ExecuteForArrayAsync("CLUSTER", "RESERVE", "VECTOR_SET_CONTEXTS", neededContexts.ToString());

                var rootNamespacesMigrating = _namespaces.Where(static x => (x % VectorManager.ContextStep) == 0);

                var nextReservedIx = 0;

                var namespaceMap = new Dictionary<ulong, ulong>();

                foreach (var migratingContext in rootNamespacesMigrating)
                {
                    var toMapTo = ulong.Parse(reservedCtxs[nextReservedIx]);
                    for (var i = 0U; i < VectorManager.ContextStep; i++)
                    {
                        var fromCtx = migratingContext + i;
                        var toCtx = toMapTo + i;

                        namespaceMap[fromCtx] = toCtx;
                    }

                    nextReservedIx++;
                }

                _namespaceMap = namespaceMap.ToFrozenDictionary();

                return true;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Failed to reserve {count} Vector Set contexts on destination node {node}", neededContexts, _targetNodeId);
                return false;
            }
        }

        /// <summary>
        /// Migrate Slots inline driver
        /// </summary>
        /// <returns></returns>
        public async Task<bool> MigrateSlotsDriverInlineAsync()
        {
            var storeBeginAddress = clusterProvider.storeWrapper.store.Log.BeginAddress;
            var storeTailAddress = clusterProvider.storeWrapper.store.Log.TailAddress;
            var storePageSize = 1 << clusterProvider.serverOptions.PageSizeBits();

#if DEBUG
            // Only on Debug mode
            await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.Migration_Slot_End_Scan_Range_Acquisition).ConfigureAwait(false);
#endif

            // Send store
            logger?.LogWarning("Store migrate scan range [{storeBeginAddress}, {storeTailAddress}]", storeBeginAddress, storeTailAddress);

            var success = await CreateAndRunMigrateTasksAsync(storeBeginAddress, storeTailAddress, storePageSize);
            if (!success) return false;

            return true;

            async Task<bool> CreateAndRunMigrateTasksAsync(long beginAddress, long tailAddress, int pageSize)
            {
                logger?.LogTrace("{method} > Scan in range ({BeginAddress},{TailAddress})", nameof(CreateAndRunMigrateTasksAsync), beginAddress, tailAddress);
                var migrateOperationRunners = new Task<bool>[clusterProvider.serverOptions.ParallelMigrateTaskCount];

                var i = 0;
                while (i < migrateOperationRunners.Length)
                {
                    var idx = i;
                    migrateOperationRunners[idx] = ScanStoreTaskAsync(idx, beginAddress, tailAddress, pageSize);
                    i++;
                }

                var allRunners = Task.WhenAll(migrateOperationRunners);
                try
                {
                    var scanResults = await allRunners.WaitAsync(_timeout, cancellationToken).ConfigureAwait(false);
                    if (!scanResults.All(static x => x))
                    {
                        logger?.LogWarning("Aborting migration due to ScanStoreTask failure");
                        return false;
                    }

                    // Protect each RangeIndex batch before taking its snapshots.
                    var rangeIndexKeys = new HashSet<byte[]>(migrateOperation.SelectMany(static mo => mo.RangeIndexKeys), ByteArrayComparer.Instance);

                    foreach (var batch in rangeIndexKeys.Chunk(migrateOperation[0].KeySendBatchSize))
                    {
                        if (!await MigrateRangeIndexKeysAsync(migrateOperation[0], batch, _cts.Token).ConfigureAwait(false))
                            return false;
                    }

                    // Handle migration of discovered Vector Set keys now that they're namespaces have been moved
                    var vectorSets = migrateOperation.SelectMany(static mo => mo.VectorSets).GroupBy(static g => g.Key, ByteArrayComparer.Instance).ToDictionary(static g => g.Key, g => g.First().Value, ByteArrayComparer.Instance);

                    if (vectorSets.Count > 0)
                    {
                        var gcs = migrateOperation[0].Client;

                        var serializeBufferArr = ArrayPool<byte>.Shared.Rent(128);
                        try
                        {
                            foreach (var (key, (value, expiration)) in vectorSets)
                            {
                                // Update the index context as we move it, so it arrives on the destination node pointed at the appropriate
                                // namespaces for element data
                                VectorManager.ReadIndex(value, out var oldContext, out _, out _, out _, out _, out _, out _, out _, out _);

                                var newContext = _namespaceMap[oldContext];
                                VectorManager.SetContextForMigration(value, newContext);

                                var neededSpace = VectorManager.GetMigratedIndexKeySerializationSize(key, value);

                                if (neededSpace > serializeBufferArr.Length)
                                {
                                    ArrayPool<byte>.Shared.Return(serializeBufferArr);
                                    serializeBufferArr = ArrayPool<byte>.Shared.Rent(neededSpace);
                                }

                                VectorManager.SerializeMigratedIndexKey(serializeBufferArr, key, value, expiration);

                                if (gcs.NeedsInitialization)
                                    SetMigrateHeader(gcs, isVectorSets: true);

                                while (!gcs.TryWriteRecordSpan(serializeBufferArr.AsSpan()[..neededSpace], MigrationRecordSpanType.VectorSetIndex, out var task))
                                {
                                    if (!await migrateOperation[0].HandleMigrateTaskResponseAsync(task).ConfigureAwait(false))
                                    {
                                        logger?.LogCritical("Failed to migrate Vector Set key {key} during migration", SpanByte.ToShortString(key));
                                        return false;
                                    }

                                    SetMigrateHeader(gcs, isVectorSets: true);
                                }

                                // Force a flush before doing the delete, in case that fails
                                if (!await migrateOperation[0].HandleMigrateTaskResponseAsync(gcs.SendAndResetIterationBuffer(), drain: true).ConfigureAwait(false))
                                {
                                    logger?.LogCritical("Flush failed before deletion of Vector Set {key} duration migration", SpanByte.ToShortString(key));
                                    return false;
                                }

                                // Delete the index on this node now that it's moved over to the destination node
                                unsafe
                                {
                                    fixed (byte* keyPtr = key)
                                    {
                                        var pinnedKeySpan = PinnedSpanByte.FromPinnedPointer(keyPtr, key.Length);
                                        migrateOperation[0].DeleteVectorSet(pinnedKeySpan);
                                    }
                                }
                            }
                        }
                        finally
                        {
                            ArrayPool<byte>.Shared.Return(serializeBufferArr);
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{CreateAndRunMigrateTasks}: {beginAddress} {tailAddress} {pageSize}", nameof(CreateAndRunMigrateTasksAsync), beginAddress, tailAddress, pageSize);
                    if (!cancellationToken.IsCancellationRequested)
                        await _cts.CancelAsync().ConfigureAwait(false);
                    return false;
                }
                finally
                {
                    try
                    {
                        await allRunners.ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger?.LogError(ex, "Migration workers failed while joining");
                    }
                }

                return true;
            }

            async Task<bool> ScanStoreTaskAsync(int taskId, long beginAddress, long tailAddress, int pageSize)
            {
                // Force async
                await Task.Yield();

                var migrateOperation = this.migrateOperation[taskId];
                var taskCount = this.migrateOperation.Length;
                var range = (tailAddress - beginAddress) / taskCount;
                var pageMask = ~((long)pageSize - 1);
                // Disjoint page-aligned ranges begin at valid records and avoid concurrent copies of the same record.
                var workerStartAddress = Math.Max(beginAddress, (beginAddress + taskId * range) & pageMask);
                var workerEndAddress = taskId == taskCount - 1
                    ? tailAddress
                    : Math.Max(beginAddress, (beginAddress + (taskId + 1) * range) & pageMask);
                if (workerStartAddress >= workerEndAddress)
                    return true;
                if (!await migrateOperation.InitializeAsync().ConfigureAwait(false))
                    return false;

                logger?.LogWarning("<{taskId}> migrate scan range [{workerStartAddress}, {workerEndAddress}]", taskId, workerStartAddress, workerEndAddress);
                return await migrateOperation.MigrateSlotsAsync(workerStartAddress, workerEndAddress, tailAddress).ConfigureAwait(false);
            }
        }
    }
}