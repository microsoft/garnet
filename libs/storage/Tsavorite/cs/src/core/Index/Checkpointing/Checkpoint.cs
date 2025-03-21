// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    public static class Checkpoint
    {
        #region Single-store APIs
        public static IStateMachine Full<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(indexCheckpointTask, backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(indexCheckpointTask, backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine Streaming<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var backend = new StreamingSnapshotCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
            return new StreamingSnapshotCheckpointSM(backend);
        }

        public static IStateMachine IndexOnly<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
            return new IndexCheckpointSM(indexCheckpointTask);
        }

        public static IStateMachine HybridLogOnly<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            guid = Guid.NewGuid();

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine IncrementalHybridLogOnly<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, Guid guid)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            var backend = new IncrementalSnapshotCheckpointSMTask<TKey, TValue, TStoreFunctions, TAllocator>(store, guid);
            return new HybridLogCheckpointSM(backend);
        }
        #endregion

        #region Two-store APIs
        public static IStateMachine Full<TKey1, TValue1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> store2,
            CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TKey1, TValue1>
            where TAllocator1 : IAllocator<TKey1, TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TKey2, TValue2>
            where TAllocator2 : IAllocator<TKey2, TValue2, TStoreFunctions2>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask1 = new IndexCheckpointSMTask<TKey1, TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
            var indexCheckpointTask2 = new IndexCheckpointSMTask<TKey2, TValue2, TStoreFunctions2, TAllocator2>(store2, guid);

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend1 = new FoldOverSMTask<TKey1, TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new FoldOverSMTask<TKey2, TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
                return new FullCheckpointSM(indexCheckpointTask1, indexCheckpointTask2, backend1, backend2);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend1 = new SnapshotCheckpointSMTask<TKey1, TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new SnapshotCheckpointSMTask<TKey2, TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
                return new FullCheckpointSM(indexCheckpointTask1, indexCheckpointTask2, backend1, backend2);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine Streaming<TKey1, TValue1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> store2,
            out Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TKey1, TValue1>
            where TAllocator1 : IAllocator<TKey1, TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TKey2, TValue2>
            where TAllocator2 : IAllocator<TKey2, TValue2, TStoreFunctions2>
        {
            guid = Guid.NewGuid();
            var backend1 = new StreamingSnapshotCheckpointSMTask<TKey1, TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
            var backend2 = new StreamingSnapshotCheckpointSMTask<TKey2, TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
            return new StreamingSnapshotCheckpointSM(backend1, backend2);
        }

        public static IStateMachine IndexOnly<TKey1, TValue1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> store2,
            out Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TKey1, TValue1>
            where TAllocator1 : IAllocator<TKey1, TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TKey2, TValue2>
            where TAllocator2 : IAllocator<TKey2, TValue2, TStoreFunctions2>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask1 = new IndexCheckpointSMTask<TKey1, TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
            var indexCheckpointTask2 = new IndexCheckpointSMTask<TKey2, TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
            return new IndexCheckpointSM(indexCheckpointTask1, indexCheckpointTask2);
        }

        public static IStateMachine HybridLogOnly<TKey1, TValue1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> store2,
            CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TKey1, TValue1>
            where TAllocator1 : IAllocator<TKey1, TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TKey2, TValue2>
            where TAllocator2 : IAllocator<TKey2, TValue2, TStoreFunctions2>
        {
            guid = Guid.NewGuid();

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend1 = new FoldOverSMTask<TKey1, TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new FoldOverSMTask<TKey2, TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
                return new HybridLogCheckpointSM(backend1, backend2);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend1 = new SnapshotCheckpointSMTask<TKey1, TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new SnapshotCheckpointSMTask<TKey2, TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
                return new HybridLogCheckpointSM(backend1, backend2);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine IncrementalHybridLogOnly<TKey1, TValue1, TStoreFunctions1, TAllocator1, TKey2, TValue2, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TKey1, TValue1, TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TKey2, TValue2, TStoreFunctions2, TAllocator2> store2,
            Guid guid)
            where TStoreFunctions1 : IStoreFunctions<TKey1, TValue1>
            where TAllocator1 : IAllocator<TKey1, TValue1, TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions<TKey2, TValue2>
            where TAllocator2 : IAllocator<TKey2, TValue2, TStoreFunctions2>
        {
            var backend1 = new IncrementalSnapshotCheckpointSMTask<TKey1, TValue1, TStoreFunctions1, TAllocator1>(store1, guid);
            var backend2 = new IncrementalSnapshotCheckpointSMTask<TKey2, TValue2, TStoreFunctions2, TAllocator2>(store2, guid);
            return new HybridLogCheckpointSM(backend1, backend2);
        }
        #endregion
    }
}