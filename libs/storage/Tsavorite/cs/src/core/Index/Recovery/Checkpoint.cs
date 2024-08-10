// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Text;
using System.Threading;
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

    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {

        internal TaskCompletionSource<LinkedCheckpointInfo> checkpointTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Guid _indexCheckpointToken;
        internal Guid _hybridLogCheckpointToken;
        internal HybridLogCheckpointInfo _hybridLogCheckpoint;
        internal HybridLogCheckpointInfo _lastSnapshotCheckpoint;

        internal Task<LinkedCheckpointInfo> CheckpointTask => checkpointTcs.Task;

        internal void CheckpointVersionShift(long oldVersion, long newVersion)
            => checkpointManager.CheckpointVersionShift(oldVersion, newVersion);

        internal void WriteHybridLogMetaInfo()
        {
            var metadata = _hybridLogCheckpoint.info.ToByteArray();
            if (CommitCookie != null && CommitCookie.Length != 0)
            {
                var convertedCookie = Convert.ToBase64String(CommitCookie);
                metadata = metadata.Concat(Encoding.Default.GetBytes(convertedCookie)).ToArray();
            }
            checkpointManager.CommitLogCheckpoint(_hybridLogCheckpointToken, metadata);
            Log.ShiftBeginAddress(_hybridLogCheckpoint.info.beginAddress, truncateLog: true);
        }

        internal void WriteHybridLogIncrementalMetaInfo(DeltaLog deltaLog)
        {
            var metadata = _hybridLogCheckpoint.info.ToByteArray();
            if (CommitCookie != null && CommitCookie.Length != 0)
            {
                var convertedCookie = Convert.ToBase64String(CommitCookie);
                metadata = metadata.Concat(Encoding.Default.GetBytes(convertedCookie)).ToArray();
            }
            checkpointManager.CommitLogIncrementalCheckpoint(_hybridLogCheckpointToken, _hybridLogCheckpoint.info.version, metadata, deltaLog);
            Log.ShiftBeginAddress(_hybridLogCheckpoint.info.beginAddress, truncateLog: true);
        }

        internal void WriteIndexMetaInfo()
        {
            checkpointManager.CommitIndexCheckpoint(_indexCheckpointToken, _indexCheckpoint.info.ToByteArray());
        }

        internal bool ObtainCurrentTailAddress(ref long location)
        {
            var tailAddress = hlogBase.GetTailAddress();
            return Interlocked.CompareExchange(ref location, tailAddress, 0) == 0;
        }

        internal void InitializeIndexCheckpoint(Guid indexToken)
        {
            _indexCheckpoint.Initialize(indexToken, kernel.hashTable.spine.state[kernel.hashTable.spine.resizeInfo.version].size, checkpointManager);
        }

        internal void InitializeHybridLogCheckpoint(Guid hybridLogToken, long version)
        {
            _hybridLogCheckpoint.Initialize(hybridLogToken, version, checkpointManager);
            _hybridLogCheckpoint.info.manualLockingActive = hlogBase.NumActiveLockingSessions > 0;
        }

        internal long Compact<T1, T2, T3, T4, CompactionFunctions>(ISessionFunctions<TKey, TValue, object, object, object> functions, CompactionFunctions compactionFunctions, long untilAddress, CompactionType compactionType)
            where CompactionFunctions : ICompactionFunctions<TKey, TValue>
        {
            throw new NotImplementedException();
        }

        // #endregion
    }
}