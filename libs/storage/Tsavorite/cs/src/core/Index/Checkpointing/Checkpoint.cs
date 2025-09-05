// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    public static class Checkpoint
    {
        #region Single-store APIs
        public static IStateMachine Full<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TStoreFunctions, TAllocator>(store, guid);

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(indexCheckpointTask, backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TStoreFunctions, TAllocator>(store, guid);
                return new FullCheckpointSM(indexCheckpointTask, backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine Streaming<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, out Guid guid)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var backend = new StreamingSnapshotCheckpointSMTask<TStoreFunctions, TAllocator>(store, guid);
            return new StreamingSnapshotCheckpointSM(backend);
        }

        public static IStateMachine IndexOnly<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, out Guid guid)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask = new IndexCheckpointSMTask<TStoreFunctions, TAllocator>(store, guid);
            return new IndexCheckpointSM(indexCheckpointTask);
        }

        public static IStateMachine HybridLogOnly<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            guid = Guid.NewGuid();

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend = new FoldOverSMTask<TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(backend);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend = new SnapshotCheckpointSMTask<TStoreFunctions, TAllocator>(store, guid);
                return new HybridLogCheckpointSM(backend);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine IncrementalHybridLogOnly<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, Guid guid)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var backend = new IncrementalSnapshotCheckpointSMTask<TStoreFunctions, TAllocator>(store, guid);
            return new HybridLogCheckpointSM(backend);
        }
        #endregion

        #region Two-store APIs
        public static IStateMachine Full<TStoreFunctions1, TAllocator1, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TStoreFunctions2, TAllocator2> store2,
            CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions1 : IStoreFunctions
            where TAllocator1 : IAllocator<TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions
            where TAllocator2 : IAllocator<TStoreFunctions2>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask1 = new IndexCheckpointSMTask<TStoreFunctions1, TAllocator1>(store1, guid);
            var indexCheckpointTask2 = new IndexCheckpointSMTask<TStoreFunctions2, TAllocator2>(store2, guid);

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend1 = new FoldOverSMTask<TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new FoldOverSMTask<TStoreFunctions2, TAllocator2>(store2, guid);
                return new FullCheckpointSM(indexCheckpointTask1, indexCheckpointTask2, backend1, backend2);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend1 = new SnapshotCheckpointSMTask<TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new SnapshotCheckpointSMTask<TStoreFunctions2, TAllocator2>(store2, guid);
                return new FullCheckpointSM(indexCheckpointTask1, indexCheckpointTask2, backend1, backend2);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine Streaming<TStoreFunctions1, TAllocator1, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TStoreFunctions2, TAllocator2> store2,
            out Guid guid)
            where TStoreFunctions1 : IStoreFunctions
            where TAllocator1 : IAllocator<TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions
            where TAllocator2 : IAllocator<TStoreFunctions2>
        {
            guid = Guid.NewGuid();
            var backend1 = new StreamingSnapshotCheckpointSMTask<TStoreFunctions1, TAllocator1>(store1, guid);
            var backend2 = new StreamingSnapshotCheckpointSMTask<TStoreFunctions2, TAllocator2>(store2, guid);
            return new StreamingSnapshotCheckpointSM(backend1, backend2);
        }

        public static IStateMachine IndexOnly<TStoreFunctions1, TAllocator1, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TStoreFunctions2, TAllocator2> store2,
            out Guid guid)
            where TStoreFunctions1 : IStoreFunctions
            where TAllocator1 : IAllocator<TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions
            where TAllocator2 : IAllocator<TStoreFunctions2>
        {
            guid = Guid.NewGuid();
            var indexCheckpointTask1 = new IndexCheckpointSMTask<TStoreFunctions1, TAllocator1>(store1, guid);
            var indexCheckpointTask2 = new IndexCheckpointSMTask<TStoreFunctions2, TAllocator2>(store2, guid);
            return new IndexCheckpointSM(indexCheckpointTask1, indexCheckpointTask2);
        }

        public static IStateMachine HybridLogOnly<TStoreFunctions1, TAllocator1, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TStoreFunctions2, TAllocator2> store2,
            CheckpointType checkpointType, out Guid guid)
            where TStoreFunctions1 : IStoreFunctions
            where TAllocator1 : IAllocator<TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions
            where TAllocator2 : IAllocator<TStoreFunctions2>
        {
            guid = Guid.NewGuid();

            if (checkpointType == CheckpointType.FoldOver)
            {
                var backend1 = new FoldOverSMTask<TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new FoldOverSMTask<TStoreFunctions2, TAllocator2>(store2, guid);
                return new HybridLogCheckpointSM(backend1, backend2);
            }
            else if (checkpointType == CheckpointType.Snapshot)
            {
                var backend1 = new SnapshotCheckpointSMTask<TStoreFunctions1, TAllocator1>(store1, guid);
                var backend2 = new SnapshotCheckpointSMTask<TStoreFunctions2, TAllocator2>(store2, guid);
                return new HybridLogCheckpointSM(backend1, backend2);
            }
            else
            {
                throw new TsavoriteException("Invalid checkpoint type");
            }
        }

        public static IStateMachine IncrementalHybridLogOnly<TStoreFunctions1, TAllocator1, TStoreFunctions2, TAllocator2>(
            TsavoriteKV<TStoreFunctions1, TAllocator1> store1,
            TsavoriteKV<TStoreFunctions2, TAllocator2> store2,
            Guid guid)
            where TStoreFunctions1 : IStoreFunctions
            where TAllocator1 : IAllocator<TStoreFunctions1>
            where TStoreFunctions2 : IStoreFunctions
            where TAllocator2 : IAllocator<TStoreFunctions2>
        {
            var backend1 = new IncrementalSnapshotCheckpointSMTask<TStoreFunctions1, TAllocator1>(store1, guid);
            var backend2 = new IncrementalSnapshotCheckpointSMTask<TStoreFunctions2, TAllocator2>(store2, guid);
            return new HybridLogCheckpointSM(backend1, backend2);
        }
        #endregion
    }
}