// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    static class Checkpoint
    {
        public static IStateMachine Full<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, CheckpointType checkpointType, long targetVersion, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(targetVersion, indexCheckpointTask, backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(targetVersion, indexCheckpointTask, backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine Streaming<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, long targetVersion, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var backend = new StreamingSnapshotCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(targetVersion, store, guid);
            return new StreamingSnapshotCheckpointSM(targetVersion, backend);
        }

        public static IStateMachine IndexOnly<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, long targetVersion, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
            return new IndexCheckpointSM(targetVersion, indexCheckpointTask);
        }

        public static IStateMachine HybridLogOnly<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, CheckpointType checkpointType, long targetVersion, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(targetVersion, backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(targetVersion, backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine IncrementalHybridLogOnly<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, long targetVersion, Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            var backend = new IncrementalSnapshotCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
            return new HybridLogCheckpointSM(targetVersion, backend);
        }
    }
}