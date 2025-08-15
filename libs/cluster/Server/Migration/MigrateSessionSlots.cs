// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
#if DEBUG
using Garnet.common;
#endif
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed partial class MigrateSession : IDisposable
    {
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
                    migrateOperation.TrasmitSlots(storeType);

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