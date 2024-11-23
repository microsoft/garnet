// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// StreamingSnapshot checkpoint state machine.
    /// </summary>
    class StreamingSnapshotCheckpointStateMachine<TKey, TValue, TStoreFunctions, TAllocator> : VersionChangeStateMachine<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Construct a new StreamingSnapshotCheckpointStateMachine, drawing boundary at targetVersion.
        /// </summary>
        /// <param name="targetVersion">upper limit (inclusive) of the version included</param>
        public StreamingSnapshotCheckpointStateMachine(long targetVersion = -1)
            : base(targetVersion,
                  new VersionChangeTask<TKey, TValue, TStoreFunctions, TAllocator>(),
                  new StreamingSnapshotCheckpointTask<TKey, TValue, TStoreFunctions, TAllocator>())
        { }

        /// <inheritdoc />
        public override SystemState NextState(SystemState start)
        {
            var result = SystemState.Copy(ref start);
            switch (start.Phase)
            {
                case Phase.REST:
                    result.Phase = Phase.PREP_STREAMING_SNAPSHOT_CHECKPOINT;
                    break;
                case Phase.PREP_STREAMING_SNAPSHOT_CHECKPOINT:
                    result.Phase = Phase.PREPARE;
                    break;
                case Phase.IN_PROGRESS:
                    result.Phase = Phase.WAIT_FLUSH;
                    break;
                case Phase.WAIT_FLUSH:
                    result.Phase = Phase.REST;
                    break;
                default:
                    result = base.NextState(start);
                    break;
            }

            return result;
        }
    }
}