// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    static class Checkpoint
    {
        public static IStateMachine Full<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, CheckpointType checkpointType, long targetVersion, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(targetVersion, indexCheckpointTask, backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(targetVersion, indexCheckpointTask, backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine Streaming<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, long targetVersion, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var backend = new StreamingSnapshotCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(targetVersion, store, guid);
            return new StreamingSnapshotCheckpointSM(targetVersion, backend);
        }

        public static IStateMachine IndexOnly<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, long targetVersion, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
            return new IndexCheckpointSM(targetVersion, indexCheckpointTask);
        }

        public static IStateMachine HybridLogOnly<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, CheckpointType checkpointType, long targetVersion, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(targetVersion, backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(targetVersion, backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine IncrementalHybridLogOnly<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, long targetVersion, Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            var backend = new IncrementalSnapshotCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
            return new HybridLogCheckpointSM(targetVersion, backend);
        }
    }
}