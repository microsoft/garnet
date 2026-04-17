// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// This code implements operations associated with the MIGRATE KEYS transfer option.
    /// </summary>
    internal sealed partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Method used to migrate individual keys from store to target node.
        /// Used with MIGRATE KEYS option
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        private async Task<bool> MigrateKeysFromStoreAsync()
        {
            var migrateTask = migrateOperation[0];

            try
            {
                // Transition keys to MIGRATING status
                migrateTask.sketch.SetStatus(SketchStatus.TRANSMITTING);
                await WaitForConfigPropagationAsync().ConfigureAwait(false);

                // Discover Vector Sets linked namespaces
                var indexesToMigrate = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
                _namespaces = clusterProvider.storeWrapper.DefaultDatabase.VectorManager.GetNamespacesForKeys(clusterProvider.storeWrapper, migrateTask.sketch.Keys.Select(t => t.Item1.ToArray()), indexesToMigrate);

                // If we have any namespaces, that implies Vector Sets, and if we have any of THOSE
                // we need to reserve destination sets on the other side
                if ((_namespaces?.Count ?? 0) > 0 && !await ReserveDestinationVectorSetsAsync().ConfigureAwait(false))
                {
                    logger?.LogError("Failed to reserve destination vector sets, migration failed");
                    return false;
                }

                // Transmit keys from store
                if (!await migrateTask.TransmitKeysAsync(indexesToMigrate).ConfigureAwait(false))
                {
                    logger?.LogError("Failed transmitting keys from store");
                    return false;
                }

                // Move Vector Sets over after individual keys are moved
                if ((_namespaces?.Count ?? 0) > 0)
                {
                    // Actually move element data over
                    if (!await migrateTask.TransmitKeysNamespacesAsync(logger).ConfigureAwait(false))
                    {
                        logger?.LogError("Failed to transmit vector set (namespaced) element data, migration failed");
                        return false;
                    }

                    // Move the indexes over
                    var gcs = migrateTask.Client;

                    var serializeBufferArr = ArrayPool<byte>.Shared.Rent(128);

                    try
                    {

                        foreach (var (key, value) in indexesToMigrate)
                        {
                            // Update the index context as we move it, so it arrives on the destination node pointed at the appropriate
                            // namespaces for element data
                            VectorManager.ReadIndex(value, out var oldContext, out _, out _, out _, out _, out _, out _, out _, out _);

                            var newContext = _namespaceMap[oldContext];
                            VectorManager.SetContextForMigration(value, newContext);

                            var neededSpace = sizeof(int) + key.Length + sizeof(int) + value.Length;

                            if (neededSpace > serializeBufferArr.Length)
                            {
                                ArrayPool<byte>.Shared.Return(serializeBufferArr);
                                serializeBufferArr = ArrayPool<byte>.Shared.Rent(neededSpace);
                            }

                            {
                                Span<byte> serializeBuffer = serializeBufferArr;
                                BinaryPrimitives.WriteInt32LittleEndian(serializeBuffer, key.Length);
                                key.CopyTo(serializeBuffer[sizeof(int)..]);
                                BinaryPrimitives.WriteInt32LittleEndian(serializeBuffer[(sizeof(int) + key.Length)..], value.Length);
                                value.CopyTo(serializeBuffer[(sizeof(int) + key.Length + sizeof(int))..]);
                            }

                            if (gcs.NeedsInitialization)
                                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: true);

                            while (!gcs.TryWriteRecordSpan(serializeBufferArr.AsSpan()[..neededSpace], MigrationRecordSpanType.VectorSetIndex, out var task))
                            {
                                if (!await HandleMigrateTaskResponseAsync(task).ConfigureAwait(false))
                                {
                                    logger?.LogCritical("Failed to migrate Vector Set key {key} during migration", SpanByte.ToShortString(key));
                                    return false;
                                }

                                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: true);
                            }
                        }
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(serializeBufferArr);
                    }

                    if (!await HandleMigrateTaskResponseAsync(gcs.SendAndResetIterationBuffer()).ConfigureAwait(false))
                    {
                        logger?.LogCritical("Final flush after Vector Set migration failed");
                        return false;
                    }
                }
                // Final cleanup, which will also delete Vector Sets
                await DeleteKeysAsync().ConfigureAwait(false);
            }
            finally
            {
                migrateOperation[0].sketch.SetStatus(SketchStatus.INITIALIZING);
            }
            return true;
        }

        /// <summary>
        /// Delete local copy of keys if _copyOption is set to false.
        /// </summary>
        private async Task DeleteKeysAsync()
        {
            var migrateTask = migrateOperation[0];
            // Transition to deleting to block read requests                
            migrateTask.sketch.SetStatus(SketchStatus.DELETING);
            await WaitForConfigPropagationAsync().ConfigureAwait(false);

            // Delete keys
            migrateTask.DeleteKeys();

            // Transition to MIGRATED to release waiting operations
            migrateTask.sketch.SetStatus(SketchStatus.MIGRATED);
            await WaitForConfigPropagationAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Method used to migrate keys from main and object stores.
        /// This method is used to process the MIGRATE KEYS transfer option.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> MigrateKeysAsync()
        {
            try
            {
                var migrateTask = migrateOperation[0];
                if (!await migrateTask.InitializeAsync().ConfigureAwait(false))
                    return false;

                // Migrate main store keys
                if (!await MigrateKeysFromStoreAsync().ConfigureAwait(false))
                    return false;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An error has occurred");
            }
            return true;
        }
    }
}