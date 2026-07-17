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
        private readonly GarnetAppendOnlyFile appendOnlyFile;
        private readonly ILogger logger;
        private RangeIndexChunkedDeserializer currentDeserializer;
        private RangeIndexMigrationActivities.ReceiveActivity receiveActivity;
        private CooperativeDisposeGuard disposeGuard;

        /// <summary>Whether a stream is currently in progress.</summary>
        internal bool IsReceiving => currentDeserializer != null;
        internal int CurrentChunkCount => receiveActivity?.ChunkCount ?? 0;

        internal RangeIndexMigrationReceiveState(RangeIndexManager rangeIndexManager, GarnetAppendOnlyFile appendOnlyFile = null, ILogger logger = null)
        {
            this.rangeIndexManager = rangeIndexManager;
            this.appendOnlyFile = appendOnlyFile;
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
                    DisposeInternal();
            }
        }

        private bool ProcessRecordInternal(ReadOnlySpan<byte> recordPayload, ClusterConfig currentConfig, ref StringBasicContext stringBasicContext, bool replaceOption)
        {
            if (recordPayload.Length == 0)
                return HandleError("Empty payload");

            if (currentDeserializer == null)
            {
                var tempMigrationPath = rangeIndexManager.DeriveTempMigrationPath();
                currentDeserializer = new RangeIndexChunkedDeserializer(tempMigrationPath);
                receiveActivity = RangeIndexMigrationActivities.ReceiveActivity.StartActivity(tempMigrationPath);
            }

            receiveActivity.OnChunkReceived(recordPayload.Length);
            if (!currentDeserializer.ProcessChunk(recordPayload))
                return HandleError("ProcessChunk failed");

            ExceptionInjectionHelper.WaitOnClear(ExceptionInjectionType.RangeIndex_Migration_Receive_Pause_In_ProcessRecord);

            if (currentDeserializer.IsComplete)
            {
                var keyBytes = currentDeserializer.Key;
                var slot = HashSlotUtils.HashSlot(keyBytes);
                if (!currentConfig.IsImportingSlot(slot))
                    return HandleError("Slot not in importing state");

                if (disposeGuard.IsDisposed)
                    return HandleError("Disposed before publish");

                receiveActivity.OnPublishing();
                var publishResult = rangeIndexManager.PublishMigratedIndex(currentDeserializer.Key, currentDeserializer.Stub, currentDeserializer.TempPath, replaceOption, ref stringBasicContext, appendOnlyFile);
                receiveActivity.OnPublishResult(publishResult);

                if (publishResult == RangeIndexManager.PublishMigratedIndexResult.Failed)
                    return HandleError("PublishMigratedIndex failed");

                // Success, SkippedAlreadyExists, and SkippedReplaceNotSupported are all
                // non-error outcomes: the destination is in a consistent state and the
                // migration protocol can continue. Only Failed is propagated as an error.
                receiveActivity.LogActivity(logger, keyBytes);
                Reset();
            }

            return true;
        }

        /// <summary>
        /// Record an error on the current activity (if any), reset state for the next stream,
        /// and return <c>false</c> so callers can <c>return HandleError(...)</c>.
        /// </summary>
        private bool HandleError(string error)
        {
            receiveActivity?.OnError(error);
            Reset();
            return false;
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
            if (disposeGuard.TryDispose() == CooperativeDisposeGuard.DisposeResult.CleanupNow)
                DisposeInternal();
        }

        /// <summary>
        /// Perform the actual disposal cleanup. Invoked exactly once — either directly from
        /// <see cref="Dispose"/> (when no worker is in-flight) or deferred to an in-flight
        /// <see cref="ProcessRecord"/>'s exit path via the dispose guard.
        /// </summary>
        private void DisposeInternal()
        {
            receiveActivity?.OnSessionDisposed();
            Reset();
        }
    }
}