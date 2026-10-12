// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        internal long lastVersion;

        private byte[] recoveredCommitCookie;
        /// <summary>
        /// User-specified commit cookie persisted with last recovered commit. Backed by a field
        /// because recovery fills it through an <c>out</c> parameter.
        /// </summary>
        public byte[] RecoveredCommitCookie => recoveredCommitCookie;

        /// <summary>
        /// Storage-slot to logical-database mapping persisted with the last recovered checkpoint, or null if
        /// the checkpoint recorded none. See <see cref="HybridLogRecoveryInfo.databaseMapping"/>.
        /// </summary>
        public int[] RecoveredDatabaseMapping { get; private set; }

        /// <summary>
        /// Swap epoch that <see cref="RecoveredDatabaseMapping"/> belongs to, or zero if the checkpoint
        /// recorded none. See <see cref="HybridLogRecoveryInfo.swapEpoch"/>.
        /// </summary>
        public long RecoveredSwapEpoch { get; private set; }

        /// <summary>
        /// <see cref="HybridLogRecoveryInfo.CheckpointVersion"/> of the last recovered checkpoint, or zero if
        /// no checkpoint has been recovered.
        /// </summary>
        public int RecoveredCheckpointVersion { get; private set; }

        /// <summary>
        /// Get the current state machine state of the system
        /// </summary>
        public SystemState SystemState => stateMachineDriver.SystemState;

        /// <summary>
        /// Version number of the last checkpointed state
        /// </summary>
        public long LastCheckpointedVersion => lastVersion;

        /// <summary>
        /// Current version number of the store
        /// </summary>
        public long CurrentVersion => stateMachineDriver.SystemState.Version;
    }
}