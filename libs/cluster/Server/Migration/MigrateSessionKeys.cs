// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using Garnet.client;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// This code implements operations associated with the MIGRATE KEYS transfer option.
    /// </summary>
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Method used to migrate individual keys from store to target node.
        /// Used with MIGRATE KEYS option
        /// </summary>
        /// <returns>True on success, false otherwise</returns>
        private bool MigrateKeysFromStore()
        {
            var migrateTask = migrateOperation[0];

            try
            {
                // Transition keys to MIGRATING status
                migrateTask.sketch.SetStatus(SketchStatus.TRANSMITTING);
                WaitForConfigPropagation();

                // Discover Vector Sets linked namespaces
                var indexesToMigrate = new Dictionary<byte[], byte[]>(ByteArrayComparer.Instance);
                _namespaces = clusterProvider.storeWrapper.DefaultDatabase.VectorManager.GetNamespacesForKeys(clusterProvider.storeWrapper, migrateTask.sketch.Keys.Select(t => t.Item1.ToArray()), indexesToMigrate);

                // If we have any namespaces, that implies Vector Sets, and if we have any of THOSE
                // we need to reserve destination sets on the other side
                if ((_namespaces?.Count ?? 0) > 0 && !ReserveDestinationVectorSetsAsync().GetAwaiter().GetResult())
                {
                    logger?.LogError("Failed to reserve destination vector sets, migration failed");
                    return false;
                }

                // Transmit keys from store
                if (!migrateTask.TransmitKeys(indexesToMigrate))
                {
                    logger?.LogError("Failed transmitting keys from store");
                    return false;
                }

                // Move Vector Sets over after individual keys are moved
                if ((_namespaces?.Count ?? 0) > 0)
                {
                    // Actually move element data over
                    if (!migrateTask.TransmitKeysNamespaces(logger))
                    {
                        logger?.LogError("Failed to transmit vector set (namespaced) element data, migration failed");
                        return false;
                    }

                    // Move the indexes over
                    var gcs = migrateTask.Client;

                    Span<byte> serializationBuffer = stackalloc byte[128];
                    byte[] serializeBufferArr = null;
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

                            if (neededSpace > serializationBuffer.Length)
                            {
                                if (serializeBufferArr != null)
                                {
                                    ArrayPool<byte>.Shared.Return(serializeBufferArr);
                                    serializationBuffer = serializeBufferArr = ArrayPool<byte>.Shared.Rent(neededSpace);
                                }
                            }

                            BinaryPrimitives.WriteInt32LittleEndian(serializationBuffer, key.Length);
                            key.CopyTo(serializationBuffer[sizeof(int)..]);
                            BinaryPrimitives.WriteInt32LittleEndian(serializationBuffer[(sizeof(int) + key.Length)..], value.Length);
                            value.CopyTo(serializationBuffer[(sizeof(int) + key.Length + sizeof(int))..]);

                            if (gcs.NeedsInitialization)
                                gcs.SetClusterMigrateHeader(_sourceNodeId, _replaceOption, isVectorSets: true);

                            while (!gcs.TryWriteRecordSpan(serializationBuffer[..neededSpace], MigrationRecordSpanType.VectorSetIndex, out var task))
                            {
                                if (!HandleMigrateTaskResponse(task))
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
                        if (serializeBufferArr != null)
                        {
                            ArrayPool<byte>.Shared.Return(serializeBufferArr);
                        }
                    }

                    if (!HandleMigrateTaskResponse(gcs.SendAndResetIterationBuffer()))
                    {
                        logger?.LogCritical("Final flush after Vector Set migration failed");
                        return false;
                    }
                }

                DeleteKeys();
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
        private void DeleteKeys()
        {
            var migrateTask = migrateOperation[0];
            // Transition to deleting to block read requests                
            migrateTask.sketch.SetStatus(SketchStatus.DELETING);
            WaitForConfigPropagation();

            // Delete keys
            migrateTask.DeleteKeys();

            // Transition to MIGRATED to release waiting operations
            migrateTask.sketch.SetStatus(SketchStatus.MIGRATED);
            WaitForConfigPropagation();
        }

        /// <summary>
        /// Method used to migrate keys from main and object stores.
        /// This method is used to process the MIGRATE KEYS transfer option.
        /// </summary>
        /// <returns></returns>
        public bool MigrateKeys()
        {
            try
            {
                var migrateTask = migrateOperation[0];
                if (!migrateTask.Initialize())
                    return false;

                // Migrate main store keys
                if (!MigrateKeysFromStore())
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