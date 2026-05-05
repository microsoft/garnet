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
        /// User-specified commit cookie persisted with last recovered commit
        /// </summary>
        public byte[] RecoveredCommitCookie => recoveredCommitCookie;

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