// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
#if DEBUG
using Garnet.common;
#endif
using Garnet.server;
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
        public async Task<bool> MigrateSlotsDriverInline()
        {
            var storeBeginAddress = clusterProvider.storeWrapper.store.Log.BeginAddress;
            var storeTailAddress = clusterProvider.storeWrapper.store.Log.TailAddress;
            var mainStorePageSize = 1 << clusterProvider.serverOptions.PageSizeBits();

#if DEBUG
            // Only on Debug mode
            ExceptionInjectionHelper.WaitOnSet(ExceptionInjectionType.Migration_Slot_End_Scan_Range_Acquisition).GetAwaiter().GetResult();
#endif

            // Send main store
            logger?.LogWarning("Store migrate scan range [{storeBeginAddress}, {storeTailAddress}]", storeBeginAddress, storeTailAddress);
            var success = await CreateAndRunMigrateTasks(StoreType.Main, storeBeginAddress, storeTailAddress, mainStorePageSize);
            if (!success) return false;

            // Send object store
            if (!clusterProvider.serverOptions.DisableObjects)
            {
                var objectStoreBeginAddress = clusterProvider.storeWrapper.objectStore.Log.BeginAddress;
                var objectStoreTailAddress = clusterProvider.storeWrapper.objectStore.Log.TailAddress;
                var objectStorePageSize = 1 << clusterProvider.serverOptions.ObjectStorePageSizeBits();
                logger?.LogWarning("Object Store migrate scan range [{objectStoreBeginAddress}, {objectStoreTailAddress}]", objectStoreBeginAddress, objectStoreTailAddress);
                success = await CreateAndRunMigrateTasks(StoreType.Object, objectStoreBeginAddress, objectStoreTailAddress, objectStorePageSize);
                if (!success) return false;
            }

            return true;

            async Task<bool> CreateAndRunMigrateTasks(StoreType storeType, long beginAddress, long tailAddress, int pageSize)
            {
                logger?.LogTrace("{method} > [{storeType}] Scan in range ({BeginAddress},{TailAddress})", nameof(CreateAndRunMigrateTasks), storeType, beginAddress, tailAddress);
                var migrateOperationRunners = new Task[clusterProvider.serverOptions.ParallelMigrateTaskCount];
                var i = 0;
                while (i < migrateOperationRunners.Length)
                {
                    var idx = i;
                    migrateOperationRunners[idx] = Task.Run(() => ScanStoreTask(idx, storeType, beginAddress, tailAddress, pageSize));
                    i++;
                }

                try
                {
                    await Task.WhenAll(migrateOperationRunners).WaitAsync(_timeout, _cts.Token).ConfigureAwait(false);

                    // Handle migration of discovered Vector Set keys now that they're namespaces have been moved
                    if (storeType == StoreType.Main)
                    {
                        var vectorSets = migrateOperation.SelectMany(static mo => mo.VectorSets).GroupBy(static g => g.Key, ByteArrayComparer.Instance).ToDictionary(static g => g.Key, g => g.First().Value, ByteArrayComparer.Instance);

                        if (vectorSets.Count > 0)
                        {
                            var gcs = migrateOperation[0].Client;

                            foreach (var (key, value) in vectorSets)
                            {
                                // Update the index context as we move it, so it arrives on the destination node pointed at the appropriate
                                // namespaces for element data
                                VectorManager.ReadIndex(value, out var oldContext, out _, out _, out _, out _, out _, out _, out _);

                                var newContext = _namespaceMap[oldContext];
                                VectorManager.SetContextForMigration(value, newContext);

                                unsafe
                                {
                                    fixed (byte* keyPtr = key, valuePtr = value)
                                    {
                                        var keySpan = SpanByte.FromPinnedPointer(keyPtr, key.Length);
                                        var valSpan = SpanByte.FromPinnedPointer(valuePtr, value.Length);

                                        if (gcs.NeedsInitialization)
                                            gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: true, isVectorSets: true);

                                        while (!gcs.TryWriteKeyValueSpanByte(ref keySpan, ref valSpan, out var task))
                                        {
                                            if (!HandleMigrateTaskResponse(task))
                                            {
                                                logger?.LogCritical("Failed to migrate Vector Set key {key} during migration", keySpan);
                                                return false;
                                            }

                                            gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isMainStore: true, isVectorSets: true);
                                        }

                                        // Force a flush before doing the delete, in case that fails
                                        if (!HandleMigrateTaskResponse(gcs.SendAndResetIterationBuffer()))
                                        {
                                            logger?.LogCritical("Flush failed before deletion of Vector Set {key} duration migration", keySpan);
                                            return false;
                                        }

                                        // Delete the index on this node now that it's moved over to the destination node
                                        migrateOperation[0].DeleteVectorSet(ref keySpan);
                                    }
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{CreateAndRunMigrateTasks}: {storeType} {beginAddress} {tailAddress} {pageSize}", nameof(CreateAndRunMigrateTasks), storeType, beginAddress, tailAddress, pageSize);
                    _cts.Cancel();
                    return false;
                }

                return true;
            }

            Task<bool> ScanStoreTask(int taskId, StoreType storeType, long beginAddress, long tailAddress, int pageSize)
            {
                var migrateOperation = this.migrateOperation[taskId];
                var range = (tailAddress - beginAddress) / clusterProvider.storeWrapper.serverOptions.ParallelMigrateTaskCount;
                var workerStartAddress = beginAddress + (taskId * range);
                var workerEndAddress = beginAddress + ((taskId + 1) * range);

                workerStartAddress = workerStartAddress - (2 * pageSize) > 0 ? workerStartAddress - (2 * pageSize) : 0;
                workerEndAddress = workerEndAddress + (2 * pageSize) < storeTailAddress ? workerEndAddress + (2 * pageSize) : storeTailAddress;
                if (!migrateOperation.Initialize())
                    return Task.FromResult(false);

                var cursor = workerStartAddress;
                logger?.LogWarning("<{StoreType}:{taskId}> migrate scan range [{workerStartAddress}, {workerEndAddress}]", storeType, taskId, workerStartAddress, workerEndAddress);
                while (true)
                {
                    var current = cursor;
                    // Build Sketch
                    migrateOperation.sketch.SetStatus(SketchStatus.INITIALIZING);
                    migrateOperation.Scan(storeType, ref current, workerEndAddress);

                    // Stop if no keys have been found
                    if (migrateOperation.sketch.argSliceVector.IsEmpty) break;

                    logger?.LogWarning("[{taskId}> Scan from {cursor} to {current} and discovered {count} keys",
                        taskId, cursor, current, migrateOperation.sketch.argSliceVector.Count);

                    // Transition EPSM to MIGRATING
                    migrateOperation.sketch.SetStatus(SketchStatus.TRANSMITTING);
                    WaitForConfigPropagation();

                    // Transmit all keys gathered
                    migrateOperation.TransmitSlots(storeType);

                    // Transition EPSM to DELETING
                    migrateOperation.sketch.SetStatus(SketchStatus.DELETING);
                    WaitForConfigPropagation();

                    // Deleting keys (Currently gathering keys from push-scan and deleting them outside)
                    migrateOperation.DeleteKeys();

                    // Clear keys from buffer
                    migrateOperation.sketch.Clear();
                    cursor = current;
                }

                return Task.FromResult(true);
            }
        }
    }
}