// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

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
        private readonly ILogger logger;
        private RangeIndexChunkedDeserializer currentDeserializer;
        private RangeIndexMigrationActivities.ReceiveActivity receiveActivity;
        private CooperativeDisposeGuard disposeGuard;

        /// <summary>Whether a stream is currently in progress.</summary>
        internal bool IsReceiving => currentDeserializer != null;
        internal int CurrentChunkCount => receiveActivity?.ChunkCount ?? 0;

        internal RangeIndexMigrationReceiveState(RangeIndexManager rangeIndexManager, ILogger logger = null)
        {
            this.rangeIndexManager = rangeIndexManager;
            this.logger = logger;
        }

        /// <summary>
        /// Process a <c>SerializedRangeIndexStream</c> record.
        /// The first record creates the deserializer; subsequent records feed it.
        /// On completion: the deserializer extracts the key, validates checksum,
        /// does slot check, and recovers the BfTree.
        /// </summary>
        public bool ProcessRecord(ReadOnlySpan<byte> recordPayload, ClusterConfig currentConfig, ref StringBasicContext stringBasicContext, bool replaceOption)
        {
            if (!disposeGuard.TryEnter())
                throw new ObjectDisposedException(nameof(RangeIndexMigrationReceiveState));

            try
            {
                return ProcessRecordInternal(recordPayload, currentConfig, ref stringBasicContext, replaceOption);
            }
            finally
            {
                if (disposeGuard.ExitAndCheckShouldCleanup())
                {
                    receiveActivity?.OnSessionDisposed();
                    Reset();
                }
            }
        }

        private bool ProcessRecordInternal(ReadOnlySpan<byte> recordPayload, ClusterConfig currentConfig, ref StringBasicContext stringBasicContext, bool replaceOption)
        {
            if (recordPayload.Length == 0)
            {
                receiveActivity?.OnError("Empty payload");
                Reset();
                return false;
            }

            if (currentDeserializer == null)
            {
                currentDeserializer = new RangeIndexChunkedDeserializer(rangeIndexManager.DeriveTempMigrationPath());
                receiveActivity = RangeIndexMigrationActivities.ReceiveActivity.StartActivity();
            }

            receiveActivity.OnChunkReceived(recordPayload.Length);
            if (!currentDeserializer.ProcessChunk(recordPayload))
            {
                receiveActivity.OnError("ProcessChunk failed");
                Reset();
                return false;
            }

            if (currentDeserializer.IsComplete)
            {
                var keyBytes = currentDeserializer.Key;
                var slot = HashSlotUtils.HashSlot(keyBytes);
                if (!currentConfig.IsImportingSlot(slot))
                {
                    receiveActivity.OnError("Slot not in importing state");
                    Reset();
                    return false;
                }

                if (disposeGuard.IsDisposed)
                {
                    receiveActivity.OnError("Disposed before publish");
                    Reset();
                    return false;
                }

                receiveActivity.OnPublishing();
                if (!rangeIndexManager.PublishMigratedIndex(currentDeserializer.Key, currentDeserializer.Stub, currentDeserializer.TempPath, replaceOption, ref stringBasicContext))
                {
                    receiveActivity.OnError("PublishMigratedIndex failed");
                    Reset();
                    return false;
                }

                receiveActivity.LogActivity(logger, keyBytes);
                Reset();
            }

            return true;
        }

        /// <summary>
        /// Reset state for the next key stream.
        /// </summary>
        private void Reset()
        {
            if (receiveActivity != null)
            {
                receiveActivity.EndAndLogActivity(logger, currentDeserializer != null ? currentDeserializer.Key : default);
                receiveActivity = null;
            }

            currentDeserializer?.Dispose();
            currentDeserializer = null;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (disposeGuard.TryDispose() == DisposeResult.CleanupNow)
            {
                receiveActivity?.OnSessionDisposed();
                Reset();
            }
        }
    }
}