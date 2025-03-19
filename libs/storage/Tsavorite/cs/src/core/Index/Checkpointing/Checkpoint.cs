// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    public static class Checkpoint
    {
        #region Single-store APIs
        public static IStateMachine Full<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(indexCheckpointTask, backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(indexCheckpointTask, backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine Streaming<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var backend = new StreamingSnapshotCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
            return new StreamingSnapshotCheckpointSM(backend);
        }

        public static IStateMachine IndexOnly<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
            return new IndexCheckpointSM(indexCheckpointTask);
        }

        public static IStateMachine HybridLogOnly<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine IncrementalHybridLogOnly<TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TValue, TStoreFunctions, TAllocator> store, Guid guid)
            where TStoreFunctions : IStoreFunctions<TValue>
            where TAllocator : IAllocator<TValue, TStoreFunctions>
        {
            var backend = new IncrementalSnapshotCheckpointSMTask<TValue, TStoreFunctions, TAllocator>(store, guid);
            return new HybridLogCheckpointSM(backend);
        }
        #endregion

        #region Two-store APIs
        public static IStateMachine Full<TValue1, TStoreFunctions1, TAllocator1, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TValue2, TStoreFunctions2, TAllocator2> store2,
            CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TValue1>
            where TAllocator1 : IAllocator<TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TValue2>
            where TAllocator2 : IAllocator<TValue2, TStoreFunctions2>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask1 = new IndexCheckpointSMTask<TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
            var indexCheckpointTask2 = new IndexCheckpointSMTask<TValue2, TStoreFunctions2, TAllocator2>(store2, guid);

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend1 = new FoldOverSMTask<TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new FoldOverSMTask<TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
                return new FullCheckpointSM(indexCheckpointTask1, indexCheckpointTask2, backend1, backend2);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend1 = new SnapshotCheckpointSMTask<TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new SnapshotCheckpointSMTask<TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
                return new FullCheckpointSM(indexCheckpointTask1, indexCheckpointTask2, backend1, backend2);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine Streaming<TValue1, TStoreFunctions1, TAllocator1, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TValue2, TStoreFunctions2, TAllocator2> store2,
            out Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TValue1>
            where TAllocator1 : IAllocator<TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TValue2>
            where TAllocator2 : IAllocator<TValue2, TStoreFunctions2>
        {
            guid = Guid.NewGuid();
            var backend1 = new StreamingSnapshotCheckpointSMTask<TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
            var backend2 = new StreamingSnapshotCheckpointSMTask<TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
            return new StreamingSnapshotCheckpointSM(backend1, backend2);
        }

        public static IStateMachine IndexOnly<TValue1, TStoreFunctions1, TAllocator1, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TValue2, TStoreFunctions2, TAllocator2> store2,
            out Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TValue1>
            where TAllocator1 : IAllocator<TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TValue2>
            where TAllocator2 : IAllocator<TValue2, TStoreFunctions2>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask1 = new IndexCheckpointSMTask<TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
            var indexCheckpointTask2 = new IndexCheckpointSMTask<TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
            return new IndexCheckpointSM(indexCheckpointTask1, indexCheckpointTask2);
        }

        public static IStateMachine HybridLogOnly<TValue1, TStoreFunctions1, TAllocator1, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TValue2, TStoreFunctions2, TAllocator2> store2,
            CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TValue1>
            where TAllocator1 : IAllocator<TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TValue2>
            where TAllocator2 : IAllocator<TValue2, TStoreFunctions2>
        {
            guid = Guid.NewGuid();

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend1 = new FoldOverSMTask<TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new FoldOverSMTask<TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
                return new HybridLogCheckpointSM(backend1, backend2);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend1 = new SnapshotCheckpointSMTask<TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new SnapshotCheckpointSMTask<TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
                return new HybridLogCheckpointSM(backend1, backend2);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine IncrementalHybridLogOnly<TValue1, TStoreFunctions1, TAllocator1, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TValue2, TStoreFunctions2, TAllocator2> store2,
            Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TValue1>
            where TAllocator1 : IAllocator<TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TValue2>
            where TAllocator2 : IAllocator<TValue2, TStoreFunctions2>
        {
            var backend1 = new IncrementalSnapshotCheckpointSMTask<TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
            var backend2 = new IncrementalSnapshotCheckpointSMTask<TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
            return new HybridLogCheckpointSM(backend1, backend2);
        }
        #endregion
    }
}