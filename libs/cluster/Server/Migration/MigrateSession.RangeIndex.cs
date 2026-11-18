// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
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
    /// <summary>
    /// RangeIndex migration support: source-side transmit driver.
    /// </summary>
    internal sealed partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Transmit a single RangeIndex key to the destination node.
        /// Uses <see cref="RangeIndexManager.SnapshotRangeIndexAndCreateReader"/> to obtain an async
        /// migration reader that snapshots and streams the BfTree data.
        /// </summary>
        private async Task<bool> TransmitRangeIndexAsync<TFilter>(MigrateOperation<TFilter> migrateOperation, byte[] keyBytes, int chunkSize, CancellationToken cancellationToken)
            where TFilter : struct, IBloomFilter
        {
            if (!clusterProvider.serverOptions.EnableRangeIndexPreview)
            {
                logger?.LogError("TransmitRangeIndexAsync: RangeIndex feature is not enabled, skipping key {key}", Encoding.UTF8.GetString(keyBytes));
                return false;
            }

            var rangeIndexManager = clusterProvider.storeWrapper.DefaultDatabase.RangeIndexManager;

            var sessionClient = migrateOperation.Client;
            var buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
            var sendChunkSize = Math.Min(chunkSize, NetworkBufferSettings.MaxSendBufferContentSize);
            var transmitActivity = RangeIndexMigrationActivities.TransmitActivity.StartActivity();
            try
            {
                using var reader = rangeIndexManager.SnapshotRangeIndexAndCreateReader(migrateOperation.LocalSession, keyBytes);
                transmitActivity.OnSnapshotCompleted(reader.TotalFileBytes);

                while (!reader.IsComplete)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Drain the reader's independent file buffer in send-sized chunks without splitting framing elements.
                    var payloadLen = await reader.ReadNextChunkAsync(buffer.AsMemory(0, sendChunkSize), cancellationToken).ConfigureAwait(false);
                    if (payloadLen == 0)
                    {
                        transmitActivity.OnError("Zero-length payload from reader");
                        logger?.LogError("TransmitRangeIndexAsync: reader returned zero-length payload with a {Size}-byte buffer for key {key}", sendChunkSize, Encoding.UTF8.GetString(keyBytes));
                        return false;
                    }

                    if (!await WriteOrSendRecordSpanAsync(migrateOperation, MigrationRecordSpanType.SerializedRangeIndexStream, buffer.AsSpan(0, payloadLen)).ConfigureAwait(false))
                    {
                        transmitActivity.OnError("Failed to write chunk");
                        logger?.LogError("TransmitRangeIndexAsync: failed to write chunk for key {key}", Encoding.UTF8.GetString(keyBytes));
                        return false;
                    }

                    transmitActivity.OnChunkSent(payloadLen);
                }

                // Force flush and await ACK
                if (!await migrateOperation.HandleMigrateTaskResponseAsync(sessionClient.SendAndResetIterationBuffer(), drain: true).ConfigureAwait(false))
                {
                    transmitActivity.OnError("Flush failed");
                    logger?.LogError("TransmitRangeIndexAsync: flush failed for key {key}", Encoding.UTF8.GetString(keyBytes));
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                transmitActivity.OnError(ex.ToString());
                logger?.LogError(ex, "TransmitRangeIndexAsync: error during snapshot or transmission for key {key}", Encoding.UTF8.GetString(keyBytes));
                return false;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
                transmitActivity.EndAndLogActivity(logger, keyBytes);
            }
        }

        /// <summary>
        /// Migrate a bounded batch of RangeIndex keys with epoch barriers protecting transmission and deletion.
        /// </summary>
        private async Task<bool> MigrateRangeIndexKeysAsync(MigrateOperation<BucketedBloomFilter> migrateOperation, IReadOnlyCollection<byte[]> rangeIndexKeys, CancellationToken cancellationToken)
        {
            var migrateActivity = RangeIndexMigrationActivities.MigrateActivity.StartActivity(rangeIndexKeys.Count);

            try
            {
                logger?.LogWarning("MigrateRangeIndexKeysAsync: migrating {count} RangeIndex keys", rangeIndexKeys.Count);

                // Add all RI keys to sketch during INITIALIZING (no gating yet)
                migrateOperation.sketch.Clear();
                migrateOperation.sketch.SetStatus(SketchStatus.INITIALIZING);
                foreach (var key in rangeIndexKeys)
                {
                    if (!migrateOperation.sketch.TryHashAndStore(key))
                    {
                        migrateActivity.OnError("Failed to protect RangeIndex batch");
                        logger?.LogError("MigrateRangeIndexKeysAsync: failed to protect all {count} RangeIndex keys", rangeIndexKeys.Count);
                        return false;
                    }
                }

                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.RangeIndex_Migration_Before_Transmitting);

                // Block writes during snapshot + transmit
                migrateOperation.sketch.SetStatus(SketchStatus.TRANSMITTING);
                if (!await WaitForConfigPropagationAsync().ConfigureAwait(false))
                {
                    migrateActivity.OnError("Failed to protect RangeIndex batch");
                    logger?.LogError("MigrateRangeIndexKeysAsync: failed to protect RangeIndex batch");
                    return false;
                }
                migrateActivity.OnTransmitting();

                await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.RangeIndex_Migration_After_Transmitting).ConfigureAwait(false);

                foreach (var key in rangeIndexKeys)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (!await TransmitRangeIndexAsync(migrateOperation, key, RangeIndexManager.DefaultMigrationChunkSize, cancellationToken).ConfigureAwait(false))
                    {
                        migrateActivity.OnError($"failed to transmit key {Encoding.UTF8.GetString(key)}");
                        logger?.LogError("MigrateRangeIndexKeysAsync: failed to migrate RangeIndex key {key}", Encoding.UTF8.GetString(key));
                        return false;
                    }
                }

                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.RangeIndex_Migration_Before_Deleting);

                // Block reads + writes during delete
                migrateOperation.sketch.SetStatus(SketchStatus.DELETING);
                if (!await WaitForConfigPropagationAsync().ConfigureAwait(false))
                {
                    migrateActivity.OnError("Failed to protect RangeIndex batch deletion");
                    logger?.LogError("MigrateRangeIndexKeysAsync: failed to protect RangeIndex batch deletion");
                    return false;
                }
                migrateActivity.OnDeleting();

                await ExceptionInjectionHelper.ResetAndWaitAsync(ExceptionInjectionType.RangeIndex_Migration_After_Deleting).ConfigureAwait(false);

                foreach (var key in rangeIndexKeys)
                {
                    try
                    {
                        unsafe
                        {
                            fixed (byte* keyPtr = key)
                                migrateOperation.DeleteRangeIndex(PinnedSpanByte.FromPinnedPointer(keyPtr, key.Length));
                        }
                    }
                    catch (Exception ex)
                    {
                        migrateActivity.OnError($"failed to delete key {Encoding.UTF8.GetString(key)}: {ex}");
                        logger?.LogError(ex, "MigrateRangeIndexKeysAsync: failed to delete RangeIndex key {key} after migration", Encoding.UTF8.GetString(key));
                        throw;
                    }
                }

                return true;
            }
            finally
            {
                // Always clean up the sketch, even on failure, to unblock client operations
                migrateOperation.sketch.RetireBatch();
                migrateActivity.EndAndLogActivity(logger);
            }
        }
    }
}