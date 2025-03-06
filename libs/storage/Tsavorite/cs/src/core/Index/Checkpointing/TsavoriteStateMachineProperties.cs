﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        internal long lastVersion;

        /// <summary>
        /// Any additional (user specified) metadata to write out with commit
        /// </summary>
        public byte[] CommitCookie { get; set; }

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
        /// Size (tail address) of current incremental snapshot delta log
        /// </summary>
        public long IncrementalSnapshotTailAddress => _lastSnapshotCheckpoint.deltaLog?.TailAddress ?? 0;

        /// <summary>
        /// Current version number of the store
        /// </summary>
        public long CurrentVersion => stateMachineDriver.SystemState.Version;
    }
}