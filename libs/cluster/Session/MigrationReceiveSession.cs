// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Per-<see cref="ClusterSession"/> state for receiving inbound RangeIndex migration data.
    /// Implements a state machine: IDLE → RECEIVING → IDLE.
    /// </summary>
    /// <remarks>
    /// Because the sender uses a single TCP connection, all <c>CLUSTER MIGRATE</c> commands
    /// from one migration arrive on the same <see cref="ClusterSession"/>, guaranteeing
    /// in-order delivery. The dispatcher nulls this object out when the stream completes,
    /// any non-<c>SerializedRangeIndexStream</c> record type as a protocol violation.
    /// </remarks>
    internal sealed class RangeIndexMigrationReceiveState : IDisposable
    {
        private readonly RangeIndexManager rangeIndexManager;
        private RangeIndexMigrationWriter currentWriter;
        private bool isReceiving;

        /// <summary>Whether a stream is currently in progress.</summary>
        internal bool IsReceiving => isReceiving;

        internal RangeIndexMigrationReceiveState(RangeIndexManager rangeIndexManager)
        {
            this.rangeIndexManager = rangeIndexManager;
        }

        /// <summary>
        /// Process a <c>SerializedRangeIndexStream</c> record.
        /// The first record creates the writer; subsequent records feed it.
        /// On completion: the writer extracts the key, validates checksum,
        /// does slot check, and recovers the BfTree.
        /// </summary>
        public bool ProcessRecord(ReadOnlySpan<byte> recordPayload, ClusterConfig currentConfig, ref StringBasicContext stringBasicContext, bool replaceOption)
        {
            if (recordPayload.Length == 0)
                return false;

            if (!isReceiving)
            {
                currentWriter = rangeIndexManager.CreateMigrationWriter();
                isReceiving = true;
            }

            // ProcessChunkAsync with sync wait — the receive path is already synchronous.
            // The async write inside the writer is a ValueTask that completes synchronously
            // for small writes, and the sync .GetAwaiter().GetResult() is acceptable here
            // since this runs on a dedicated IO thread.
            if (!currentWriter.ProcessChunkAsync(recordPayload.ToArray()).GetAwaiter().GetResult())
            {
                Reset();
                return false;
            }

            if (currentWriter.IsComplete)
            {
                var keyBytes = currentWriter.Key;
                var slot = HashSlotUtils.HashSlot(keyBytes);
                if (!currentConfig.IsImportingSlot(slot))
                {
                    Reset();
                    return false;
                }

                if (!currentWriter.Publish(ref stringBasicContext))
                {
                    Reset();
                    return false;
                }

                Reset();
            }

            return true;
        }

        /// <summary>Reset state for the next key stream.</summary>
        private void Reset()
        {
            currentWriter?.Dispose();
            currentWriter = null;
            isReceiving = false;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Reset();
        }
    }
}
