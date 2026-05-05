// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Threading.Tasks;
using Garnet.client;
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
        /// Forces a flush and awaits ACK before optionally deleting the source key.
        /// </summary>
        private async Task<bool> TransmitRangeIndexAsync(MigrateOperation mo, byte[] keyBytes, byte[] stubBytes, bool deleteAfterTransmit = true, int chunkSize = RangeIndexManager.DefaultMigrationChunkSize)
        {
            var rangeIndexManager = clusterProvider.storeWrapper.DefaultDatabase.RangeIndexManager;
            var gcs = mo.Client;

            RangeIndexMigrationReader reader;
            try
            {
                reader = rangeIndexManager.SnapshotRangeIndexAndCreateReader(mo.LocalSession, keyBytes, stubBytes, chunkSize);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "TransmitRangeIndex: failed to snapshot BfTree for key");
                return false;
            }

            var buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
            try
            {
                using (reader)
                {
                    while (!reader.IsComplete)
                    {
                        var payloadLen = await reader.ReadNextChunkAsync(buffer).ConfigureAwait(false);
                        if (payloadLen == 0)
                        {
                            logger?.LogError("TransmitRangeIndex: reader returned zero-length payload with a {Size}-byte buffer", chunkSize);
                            return false;
                        }

                        if (!WriteOrSendRecordSpan(gcs, MigrationRecordSpanType.SerializedRangeIndexStream, buffer.AsSpan(0, payloadLen)))
                        {
                            logger?.LogError("TransmitRangeIndex: failed to write chunk");
                            return false;
                        }
                    }

                    // Force flush and await ACK before deleting the source key
                    if (!HandleMigrateTaskResponse(gcs.SendAndResetIterationBuffer()))
                    {
                        logger?.LogError("TransmitRangeIndex: flush failed");
                        return false;
                    }

                    // Delete the source key now that destination has confirmed receipt
                    if (deleteAfterTransmit)
                    {
                        var pinnedKey = PinnedSpanByte.FromPinnedSpan(keyBytes);
                        mo.DeleteRangeIndex(pinnedKey);
                    }

                    return true;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }
}