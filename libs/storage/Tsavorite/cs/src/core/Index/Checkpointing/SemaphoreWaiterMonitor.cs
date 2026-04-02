// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Identifies the type of semaphore added to the state machine driver waiting list,
    /// including the originating state machine task.
    /// </summary>
    public enum StateMachineSemaphoreType
    {
        /// <summary>
        /// Waiting for all transactions in the last version to complete.
        /// </summary>
        LastVersionTransactionsDone,

        /// <summary>
        /// Waiting for the main index checkpoint to complete (IndexCheckpointSMTask).
        /// </summary>
        IndexCheckpointSMTaskMainIndexCheckpoint,

        /// <summary>
        /// Waiting for the overflow buckets checkpoint to complete (IndexCheckpointSMTask).
        /// </summary>
        IndexCheckpointSMTaskOverflowBucketsCheckpoint,

        /// <summary>
        /// Waiting for the hybrid log flush to complete (FoldOverSMTask).
        /// </summary>
        FoldOverSMTaskHybridLogFlushed,

        /// <summary>
        /// Waiting for the hybrid log flush to complete (IncrementalSnapshotCheckpointSMTask).
        /// </summary>
        IncrementalSnapshotCheckpointSMTaskHybridLogFlushed,

        /// <summary>
        /// Waiting for the hybrid log flush to complete (SnapshotCheckpointSMTask).
        /// </summary>
        SnapshotCheckpointSMTaskHybridLogFlushed,
    }

    /// <summary>
    /// Pairs a <see cref="SemaphoreSlim"/> with a <see cref="StateMachineSemaphoreType"/>
    /// so the state machine driver can identify which operation a semaphore belongs to.
    /// </summary>
    internal readonly struct SemaphoreWaiterMonitor
    {
        public SemaphoreSlim Semaphore { get; }
        public StateMachineSemaphoreType Type { get; }

        public SemaphoreWaiterMonitor(SemaphoreSlim semaphore, StateMachineSemaphoreType type)
        {
            Semaphore = semaphore;
            Type = type;
        }

        public override string ToString() => Type.ToString();
    }
}