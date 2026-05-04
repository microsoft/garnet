// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using Garnet.client;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// RangeIndex migration support: source-side transmit driver.
    /// </summary>
    internal sealed unsafe partial class MigrateSession : IDisposable
    {
        /// <summary>
        /// Transmit a single RangeIndex key to the destination node.
        /// Uses <see cref="RangeIndexManager.CreateSerializer"/> to obtain a chunked
        /// serializer, wraps each chunk with key framing into a
        /// <see cref="MigrationRecordSpanType.SerializedRangeIndexStream"/> record.
        /// Forces a flush and awaits ACK before deleting the source key.
        /// </summary>
        private bool TransmitRangeIndex(MigrateOperation mo, ReadOnlySpan<byte> keyBytes, ReadOnlySpan<byte> stubBytes, int chunkSize = RangeIndexManager.DefaultMigrationChunkSize)
        {
            var rangeIndexManager = clusterProvider.storeWrapper.DefaultDatabase.RangeIndexManager;
            var gcs = mo.Client;

            RangeIndexChunkedSerializer serializer;
            try
            {
                serializer = rangeIndexManager.CreateSerializer(mo.LocalSession, keyBytes, stubBytes, chunkSize);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "TransmitRangeIndex: failed to snapshot BfTree for key");
                return false;
            }

            var buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
            try
            {
                using (serializer)
                {
                    while (!serializer.IsComplete)
                    {
                        var payloadLen = serializer.MoveNext(buffer);
                        if (payloadLen == 0)
                        {
                            logger?.LogError("TransmitRangeIndex: serializer returned zero-length payload with a {Size}-byte buffer", chunkSize);
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
                    fixed (byte* keyPtr = keyBytes)
                    {
                        var pinnedKey = PinnedSpanByte.FromPinnedPointer(keyPtr, keyBytes.Length);
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