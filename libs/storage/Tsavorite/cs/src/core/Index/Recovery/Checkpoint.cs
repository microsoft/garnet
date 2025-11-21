// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Linked list (chain) of checkpoint info
    /// </summary>
    public struct LinkedCheckpointInfo
    {
        /// <summary>
        /// Next task in checkpoint chain
        /// </summary>
        public Task<LinkedCheckpointInfo> NextTask;
    }

    internal static class EpochPhaseIdx
    {
        public const int Prepare = 0;
        public const int InProgress = 1;
        public const int WaitPending = 2;
        public const int WaitFlush = 3;
        public const int CheckpointCompletionCallback = 4;
    }

    public partial class TsavoriteKV<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {

        internal TaskCompletionSource<LinkedCheckpointInfo> checkpointTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Guid _indexCheckpointToken;
        internal Guid _hybridLogCheckpointToken;
        internal HybridLogCheckpointInfo _hybridLogCheckpoint;
        internal HybridLogCheckpointInfo _lastSnapshotCheckpoint;

        internal Task<LinkedCheckpointInfo> CheckpointTask => checkpointTcs.Task;

        internal void CheckpointVersionShiftStart(long oldVersion, long newVersion, bool isStreaming)
            => checkpointManager.CheckpointVersionShiftStart(oldVersion, newVersion, isStreaming);

        internal void CheckpointVersionShiftEnd(long oldVersion, long newVersion, bool isStreaming)
            => checkpointManager.CheckpointVersionShiftEnd(oldVersion, newVersion, isStreaming);

        internal void WriteHybridLogMetaInfo()
        {
            _hybridLogCheckpoint.info.cookie = checkpointManager.GetCookie();
            checkpointManager.CommitLogCheckpointMetadata(_hybridLogCheckpointToken, _hybridLogCheckpoint.info.ToByteArray());
        }

        internal void CleanupLogCheckpoint()
        {
            checkpointManager.CleanupLogCheckpoint(_hybridLogCheckpointToken);
            Log.ShiftBeginAddress(_hybridLogCheckpoint.info.beginAddress, truncateLog: true);
        }

        internal void WriteHybridLogIncrementalMetaInfo(DeltaLog deltaLog)
        {
            _hybridLogCheckpoint.info.cookie = checkpointManager.GetCookie();
            checkpointManager.CommitLogIncrementalCheckpoint(_hybridLogCheckpointToken, _hybridLogCheckpoint.info.ToByteArray(), deltaLog);
        }

        internal void CleanupLogIncrementalCheckpoint()
        {
            checkpointManager.CleanupLogIncrementalCheckpoint(_hybridLogCheckpointToken);
        }

        internal void WriteIndexMetaInfo()
        {
            checkpointManager.CommitIndexCheckpoint(_indexCheckpointToken, _indexCheckpoint.info.ToByteArray());
        }

        internal void CleanupIndexCheckpoint()
        {
            checkpointManager.CleanupIndexCheckpoint(_indexCheckpointToken);
        }

        internal void InitializeIndexCheckpoint(Guid indexToken)
        {
            _indexCheckpoint.Initialize(indexToken, state[resizeInfo.version].size, checkpointManager);
        }

        internal void InitializeHybridLogCheckpoint(Guid hybridLogToken, long version)
        {
            _hybridLogCheckpoint.Initialize(hybridLogToken, version, checkpointManager);
        }

        // #endregion
    }
}