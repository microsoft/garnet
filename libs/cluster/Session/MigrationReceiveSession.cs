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
    /// in-order delivery.
    /// </remarks>
    internal sealed class RangeIndexMigrationReceiveState : IDisposable
    {
        private readonly RangeIndexManager rangeIndexManager;
        private RangeIndexChunkedDeserializer currentDeserializer;
        private bool isReceiving;

        /// <summary>Whether a stream is currently in progress.</summary>
        internal bool IsReceiving => isReceiving;

        internal RangeIndexMigrationReceiveState(RangeIndexManager rangeIndexManager)
        {
            this.rangeIndexManager = rangeIndexManager;
        }

        /// <summary>
        /// Process a <c>SerializedRangeIndexStream</c> record.
        /// The first record creates the deserializer; subsequent records feed it.
        /// On completion: the deserializer extracts the key, validates checksum,
        /// does slot check, and recovers the BfTree.
        /// </summary>
        public bool ProcessRecord(ReadOnlySpan<byte> recordPayload, ClusterConfig currentConfig, ref StringBasicContext stringBasicContext, bool replaceOption)
        {
            if (recordPayload.Length == 0)
                return false;

            if (!isReceiving)
            {
                currentDeserializer = new RangeIndexChunkedDeserializer(rangeIndexManager);
                isReceiving = true;
            }

            if (!currentDeserializer.ProcessChunk(recordPayload))
            {
                Reset();
                return false;
            }

            if (currentDeserializer.IsComplete)
            {
                var keyBytes = currentDeserializer.Key;
                var slot = HashSlotUtils.HashSlot(keyBytes);
                if (!currentConfig.IsImportingSlot(slot))
                {
                    Reset();
                    return false;
                }

                if (!currentDeserializer.Publish(ref stringBasicContext))
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
            currentDeserializer?.Dispose();
            currentDeserializer = null;
            isReceiving = false;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Reset();
        }
    }
}
