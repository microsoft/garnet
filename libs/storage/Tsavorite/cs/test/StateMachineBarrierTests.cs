// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using Tsavorite.core;
using Tsavorite.test.recovery.sumstore;

namespace Tsavorite.test.statemachine
{
    using StructStoreFunctions = StoreFunctions<AdId, NumClicks, AdId.Comparer, NoSerializer<AdId>, NoSerializer<NumClicks>, DefaultRecordDisposer<AdId, NumClicks>>;
    using StructAllocator = BlittableAllocator<AdId, NumClicks, StoreFunctions<AdId, NumClicks, AdId.Comparer, NoSerializer<AdId>, NoSerializer<NumClicks>, DefaultRecordDisposer<AdId, NumClicks>>>;

    [TestFixture]
    public class StateMachineBarrierTests
    {
        IDevice log;
        private TsavoriteKV<AdId, NumClicks, StructStoreFunctions, StructAllocator> store;
        const int NumOps = 5000;
        AdId[] inputArray;

        [SetUp]
        public void Setup()
        {
            inputArray = new AdId[NumOps];
            for (int i = 0; i < NumOps; i++)
                inputArray[i].adId = i;

            log = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "StateMachineTest1.log"), deleteOnClose: true);
            string checkpointDir = Path.Join(TestUtils.MethodTestDir, "statemachinetest");
            _ = Directory.CreateDirectory(checkpointDir);

            store = new (new TsavoriteKVSettings<AdId, NumClicks>()
                {
                    IndexSize = 1 << 13, LogDevice = log, MutableFraction = 0.1, PageSize = 1 << 10, MemorySize = 1 << 13,
                    CheckpointDir = checkpointDir, CheckpointVersionSwitchBarrier = true
            }, StoreFunctions<AdId, NumClicks>.Create(new AdId.Comparer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [TestCase]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void StateMachineBarrierTest1()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Invoke Refresh on session s2, it will spin (blocked from working due to CheckpointVersionSwitchBarrier)
            s2.Refresh(waitComplete: false);

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We can complete s2 now, as barrier is done
            s2.CompleteOp();

            // Depending on timing, we should now be in IN_PROGRESS, 2 or WAIT_FLUSH, 2
            Assert.IsTrue(
                SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState) ||
                SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), store.SystemState)
                );

            // Forward the rest of the state machine
            while (!SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState))
            {
                uc1.Refresh();
                s2.Refresh();
            }

            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            uc1.EndUnsafe();
            s1.Dispose();
        }

        void Prepare(out SimpleFunctions f,
            out ClientSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator> s1,
            out UnsafeContext<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator> uc1,
            out ThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator> s2,
            long toVersion = -1)
        {
            f = new SimpleFunctions();

            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            // Take index checkpoint for recovery purposes
            _ = store.TryInitiateIndexCheckpoint(out _);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

            // Index checkpoint does not update version, so
            // we should still be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            NumClicks value;

            s1 = store.NewSession<NumClicks, NumClicks, Empty, SimpleFunctions>(f, "foo");
            var bc1 = s1.BasicContext;

            for (int key = 0; key < NumOps; key++)
            {
                value.numClicks = key;
                _ = bc1.Upsert(ref inputArray[key], ref value, Empty.Default);
            }

            // Ensure state machine needs no I/O wait during WAIT_FLUSH
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, true);

            // Create unsafe context and hold epoch to prepare for manual state machine driver
            uc1 = s1.UnsafeContext;
            uc1.BeginUnsafe();

            // Start session s2 on another thread for testing
            s2 = store.CreateThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator>(f);

            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            _ = store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver, targetVersion: toVersion);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));
        }
    }
}