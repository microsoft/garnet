// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Identifies the type of waiter added to the state machine driver waiting list,
    /// including the originating state machine task.
    /// </summary>
    internal enum StateMachineTaskType
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
}