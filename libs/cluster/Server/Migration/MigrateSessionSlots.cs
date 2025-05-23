// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Migrate Slots inline driver
        /// </summary>
        /// <returns></returns>
        public bool MigrateSlotsDriverInline()
        {
            var storeBeginAddress = clusterProvider.storeWrapper.store.Log.BeginAddress;
            var storeTailAddress = clusterProvider.storeWrapper.store.Log.TailAddress;
            var mainStorePageSize = 1 << clusterProvider.serverOptions.PageSizeBits();

            // Send main store
            CreateAndRunMigrateTasks(StoreType.Main, storeBeginAddress, storeTailAddress, mainStorePageSize);

            // Send object store
            if (!clusterProvider.serverOptions.DisableObjects)
            {
                var objectStoreBeginAddress = clusterProvider.storeWrapper.objectStore.Log.BeginAddress;
                var objectStoreTailAddress = clusterProvider.storeWrapper.objectStore.Log.TailAddress;
                var objectStorePageSize = 1 << clusterProvider.serverOptions.ObjectStorePageSizeBits();
                CreateAndRunMigrateTasks(StoreType.Object, objectStoreBeginAddress, objectStoreTailAddress, objectStorePageSize);
            }

            return true;

            void CreateAndRunMigrateTasks(StoreType storeType, long beginAddress, long tailAddress, int pageSize)
            {
                logger?.LogTrace("{method} > [{storeType}] Scan in range ({BeginAddress},{TailAddress})", nameof(CreateAndRunMigrateTasks), storeType, beginAddress, tailAddress);
                var migrateTasks = new Task[clusterProvider.serverOptions.ParallelMigrateTasks];
                var i = 0;
                while (i < migrateTasks.Length)
                {
                    var idx = i;
                    migrateTasks[idx] = Task.Run(() => ScanStoreTask(idx, storeType, beginAddress, tailAddress, pageSize));
                    i++;
                }

                Task.WaitAll(migrateTasks, _cts.Token);
            }

            Task<bool> ScanStoreTask(int taskId, StoreType storeType, long beginAddress, long tailAddress, int pageSize)
            {
                var storeScanFunctions = migrateScan[taskId];
                var range = (tailAddress - beginAddress) / clusterProvider.storeWrapper.serverOptions.ParallelMigrateTasks;
                var workerStartAddress = beginAddress + (taskId * range);
                var workerEndAddress = beginAddress + ((taskId + 1) * range);

                workerStartAddress = workerStartAddress - (2 * pageSize) > 0 ? workerStartAddress - (2 * pageSize) : 0;
                workerEndAddress = workerEndAddress + (2 * pageSize) < storeTailAddress ? workerEndAddress + (2 * pageSize) : storeTailAddress;
                storeScanFunctions.Initialize();

                var cursor = workerStartAddress;
                while (true)
                {
                    var current = cursor;
                    // Build Sketch
                    storeScanFunctions.SetKeysStatus(KeyMigrationStatus.QUEUED);
                    storeScanFunctions.SetPhase(MigratePhase.BuildSketch);
                    PerformScan(ref current, workerEndAddress);

                    // Stop if no keys have been found
                    if (storeScanFunctions.Count == 0) break;

                    var currentEnd = current;
                    logger?.LogTrace("[{taskId}> Scan from {cursor} to {current} and discovered {count} keys", taskId, cursor, current, storeScanFunctions.Count);

                    // Transition EPSM to MIGRATING
                    storeScanFunctions.SetKeysStatus(KeyMigrationStatus.MIGRATING);
                    WaitForConfigPropagation();

                    // Iterate main store
                    current = cursor;
                    storeScanFunctions.SetPhase(MigratePhase.TransmitData);
                    PerformScan(ref current, currentEnd);

                    // Transition EPSM to DELETING
                    storeScanFunctions.SetKeysStatus(KeyMigrationStatus.DELETING);
                    WaitForConfigPropagation();

                    // Deleting keys (Currently gathering keys from push-scan and deleting them outside)
                    current = cursor;
                    storeScanFunctions.SetPhase(MigratePhase.DeletingData);
                    PerformScan(ref current, currentEnd);

                    // Delete gathered keys
                    foreach (var key in storeScanFunctions.keysToDelete)
                        _ = localServerSessions[taskId].BasicGarnetApi.DELETE(key);
                    storeScanFunctions.keysToDelete.Clear();
                    storeScanFunctions.SetKeysStatus(KeyMigrationStatus.MIGRATED);
                    storeScanFunctions.sketch.Clear();
                    cursor = current;
                }


                void PerformScan(ref long current, long currentEnd)
                {
                    if (storeType == StoreType.Main)
                        _ = localServerSessions[taskId].BasicGarnetApi.IterateMainStore(ref storeScanFunctions.mss, ref current, currentEnd);
                    else if (storeType == StoreType.Object)
                        _ = localServerSessions[taskId].BasicGarnetApi.IterateObjectStore(ref storeScanFunctions.oss, ref current, currentEnd);
                }

                return Task.FromResult(true);
            }
        }
    }
}