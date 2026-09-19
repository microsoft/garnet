// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Checkpoint manager for Garnet cluster, inherits from GarnetCheckpointManager.
    /// </summary>
    internal sealed class GarnetClusterCheckpointManager : GarnetCheckpointManager, IDisposable
    {
        readonly bool isMainStore;
        public Action<bool, long, long, bool> checkpointVersionShiftStart;
        public Action<bool, long, long, bool> checkpointVersionShiftEnd;

        readonly bool safelyRemoveOutdated;

        public GarnetClusterCheckpointManager(
            int aofPhysicalSublogCount,
            INamedDeviceFactoryCreator deviceFactoryCreator,
            ICheckpointNamingScheme checkpointNamingScheme,
            bool isMainStore,
            bool safelyRemoveOutdated = false,
            int fastCommitThrottleFreq = 0,
            ILogger logger = null)
            : base(aofPhysicalSublogCount, deviceFactoryCreator, checkpointNamingScheme, removeOutdated: false, fastCommitThrottleFreq, logger)
        {
            this.isMainStore = isMainStore;
            this.safelyRemoveOutdated = safelyRemoveOutdated;
        }

        /// <summary>
        /// Cluster mode manages checkpoint cleanup externally via CheckpointStore with reader-safety checks.
        /// </summary>
        public override bool PerformAutomaticCleanup => false;

        public override void CheckpointVersionShiftStart(long oldVersion, long newVersion, bool isStreaming)
            => checkpointVersionShiftStart?.Invoke(isMainStore, oldVersion, newVersion, isStreaming);

        public override void CheckpointVersionShiftEnd(long oldVersion, long newVersion, bool isStreaming)
            => checkpointVersionShiftEnd?.Invoke(isMainStore, oldVersion, newVersion, isStreaming);

        #region ICheckpointManager

        #endregion
    }
}