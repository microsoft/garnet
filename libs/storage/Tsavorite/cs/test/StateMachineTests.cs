// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.test.recovery.sumstore;

namespace Tsavorite.test.statemachine
{
    using StructAllocator = BlittableAllocator<AdId, NumClicks, StoreFunctions<AdId, NumClicks, AdId.Comparer, DefaultRecordDisposer<AdId, NumClicks>>>;
    using StructStoreFunctions = StoreFunctions<AdId, NumClicks, AdId.Comparer, DefaultRecordDisposer<AdId, NumClicks>>;

    [TestFixture]
    public class StateMachineTests
    {
        IDevice log;
        TsavoriteKV<AdId, NumClicks, StructStoreFunctions, StructAllocator> store;
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

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                MutableFraction = 0.1,
                PageSize = 1L << 10,
                MemorySize = 1L << 13,
                CheckpointDir = checkpointDir
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
        public void StateMachineTest1()
        {
            Prepare(out _, out var s1, out var uc1, out var ks1, out var s2);

            // We should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            s2.Refresh();

            // We should be in WAIT_FLUSH, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), store.SystemState));

            uc1.Refresh();

            // We should be in PERSISTENCE_CALLBACK, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), store.SystemState));

            s2.Refresh();

            // We should be in REST, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            ks1.EndUnsafe();
            s1.Dispose();
        }


        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest2()
        {
            Prepare(out _, out var s1, out var uc1, out var ks1, out var s2);

            // We should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            // Dispose session s2; moves state machine forward to WAIT_FLUSH, 2
            s2.Dispose();

            // We should be in WAIT_FLUSH, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), store.SystemState));

            // Since s1 is the only session now, it will fast-foward state machine
            // to completion
            uc1.Refresh();

            // We should be in REST, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            ks1.EndUnsafe();
            s1.Dispose();
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest3()
        {
            Prepare(out _, out var s1, out var uc1, out var ks1, out var s2);

            // We should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s1
            uc1.Refresh();

            // s1 is now in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), SystemState.Make(s1.ctx.phase, s1.ctx.version)));

            // Suspend s1
            ks1.EndUnsafe();

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            ks1.BeginUnsafe();

            s2.Dispose();

            ks1.EndUnsafe();
            s1.Dispose();
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest4()
        {
            Prepare(out _, out var s1, out var uc1, out var ks1, out var s2);

            // We should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            // s1 is now in IN_PROGRESS, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), SystemState.Make(s1.ctx.phase, s1.ctx.version)));

            // Suspend s1
            ks1.EndUnsafe();

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            ks1.BeginUnsafe();

            s2.Dispose();

            ks1.EndUnsafe();
            s1.Dispose();
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest5()
        {
            Prepare(out _, out var s1, out var uc1, out var ks1, out var s2);

            // We should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            uc1.Refresh();
            s2.Refresh();

            // We should now be in IN_PROGRESS, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            uc1.Refresh();

            // We should be in WAIT_FLUSH, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), store.SystemState));


            s2.Refresh();

            // We should be in PERSISTENCE_CALLBACK, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), store.SystemState));

            uc1.Refresh();

            // We should be in REST, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // No callback here since already done
            uc1.Refresh();

            // Suspend s1
            ks1.EndUnsafe();

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            ks1.BeginUnsafe();

            s2.Dispose();

            ks1.EndUnsafe();
            s1.Dispose();
        }


        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest6()
        {
            Prepare(out _, out var s1, out var uc1, out var ks1, out var s2);

            // Suspend s1
            ks1.EndUnsafe();

            // s1 is now in REST, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), SystemState.Make(s1.ctx.phase, s1.ctx.version)));

            // System should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            s2.Dispose();

            _ = store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

            // We should be in REST, 3
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 3), store.SystemState));

            ks1.BeginUnsafe();

            ks1.EndUnsafe();
            s1.Dispose();
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void LUCScenario1()
        {
            CreateSessions(out _, out var s1, out var ts, out var lts);
            // System should be in REST, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            lts.getLUC();
            ClassicAssert.IsTrue(lts.isProtected);

            _ = store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);

            // System should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            ts.Refresh();
            lts.Refresh();

            // System should be in PREPARE, 1 Since there is an active locking session
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            lts.DisposeLUC();

            ts.Refresh();
            // fast-foward state machine to completion
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            lts.Refresh();

            s1.Dispose();
            ts.Dispose();
            lts.Dispose();
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void LUCScenario2()
        {
            CreateSessions(out _, out var s1, out var ts, out var lts);

            // System should be in REST, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            var uc1 = s1.UnsafeContext;
            var ks1 = new TestTransientKernelSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator,
                                          UnsafeContext<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator>>(uc1);
            ks1.BeginUnsafe();

            _ = store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);

            // should not succeed since checkpoint is in progress
            lts.getLUC();
            ClassicAssert.IsFalse(lts.isProtected);

            // We should be in PREPARE phase
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            ts.Refresh();
            // System should be in PREPARE, 1 
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // should not succeed since checkpoint is in progress
            lts.getLUC();
            ClassicAssert.IsFalse(lts.isProtected);

            ks1.EndUnsafe();

            // fast-foward state machine to completion
            ts.Refresh();
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // should be true since checkpoint is done
            lts.getLUC();
            ClassicAssert.IsTrue(lts.isProtected);
            lts.DisposeLUC();

            s1.Dispose();
            ts.Dispose();
            lts.Dispose();
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void LUCScenario3()
        {
            CreateSessions(out _, out var s1, out var ts, out var lts);

            // System should be in REST, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            // Start first LUC before checkpoint
            var luc1 = s1.LockableUnsafeContext;
            var ks1 = new TestTransactionalKernelSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator,
                                          LockableUnsafeContext<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator>>(luc1);
            ks1.BeginUnsafe();
            ks1.BeginTransaction();

            _ = store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);

            // System should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            luc1.Refresh();
            ts.Refresh();
            luc1.Refresh();

            // System should be in PREPARE, 1 Since there is an active locking session
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // should not let new LUC start since checkpoint is in progress
            lts.getLUC();
            ClassicAssert.IsFalse(lts.isProtected);

            // We still should be in PREPARE phase
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // End first LUC 
            ks1.EndTransaction();
            ks1.EndUnsafe();

            s1.BasicContext.Refresh();
            // System should be in IN_PROGRESS, 1 
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            // should be true since checkpoint is in IN_PROGRESS phase
            lts.getLUC();
            ClassicAssert.IsTrue(lts.isProtected);
            lts.DisposeLUC();

            ts.Refresh();
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // Expect checkpoint completion callback
            s1.Dispose();
            ts.Dispose();
            lts.Dispose();
        }

        [TestCase]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void StateMachineCallbackTest1()
        {
            var callback = new TestCallback();
            store.UnsafeRegisterCallback(callback);
            Prepare(out _, out var s1, out var uc1, out var ks1, out var s2);

            // We should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            // Refresh session s2
            s2.Refresh();
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            s2.Refresh();

            // We should be in WAIT_FLUSH, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            uc1.Refresh();

            // We should be in PERSISTENCE_CALLBACK, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            s2.Refresh();

            // We should be in REST, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            ks1.EndUnsafe();
            s1.Dispose();
        }

        [TestCase]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public void VersionChangeTest()
        {
            var toVersion = 1 + (1 << 14);
            Prepare(out _, out var s1, out var uc1, out var ks1, out var s2, toVersion);

            // We should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            s2.Refresh();
            uc1.Refresh();

            // We should now be in IN_PROGRESS, toVersion
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, toVersion), store.SystemState));

            s2.Refresh();

            // We should be in WAIT_FLUSH, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, toVersion), store.SystemState));

            uc1.Refresh();

            // We should be in PERSISTENCE_CALLBACK, 2
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, toVersion), store.SystemState));

            s2.Refresh();

            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, toVersion), store.SystemState));


            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            ks1.EndUnsafe();
            s1.Dispose();
        }

        void Prepare(out SimpleFunctions f,
            out ClientSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator> s1,
            out UnsafeContext<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator> uc1,
            out TestTransientKernelSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator,
                                          UnsafeContext<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator>> ks1,
            out ThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator> s2,
            long toVersion = -1)
        {
            f = new SimpleFunctions();

            // We should be in REST, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            // Take index checkpoint for recovery purposes
            _ = store.TryInitiateIndexCheckpoint(out _);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

            // Index checkpoint does not update version, so
            // we should still be in REST, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

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
            ks1 = new(uc1);
            ks1.BeginUnsafe();

            // Start session s2 on another thread for testing
            s2 = store.CreateThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator>(f);

            // We should be in REST, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            _ = store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver, targetVersion: toVersion);

            // We should be in PREPARE, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));
        }


        void CreateSessions(out SimpleFunctions f,
            out ClientSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator> s1,
            out ThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator> ts,
            out LUCThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator> lts)
        {
            f = new SimpleFunctions();
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

            // Start session s2 on another thread for testing
            ts = store.CreateThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator>(f);
            lts = store.CreateLUCThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions, StructStoreFunctions, StructAllocator>(f);

            // We should be in REST, 1
            ClassicAssert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            // Take index checkpoint for recovery purposes
            _ = store.TryInitiateIndexCheckpoint(out _);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    public class SimpleFunctions : SimpleSessionFunctions<AdId, NumClicks, Empty>
    {
        public override void ReadCompletionCallback(ref AdId key, ref NumClicks input, ref NumClicks output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(key.adId, output.numClicks);
        }
    }

    public class TestCallback : IStateMachineCallback<AdId, NumClicks, StructStoreFunctions, StructAllocator>
    {
        private readonly HashSet<SystemState> invokedStates = [];

        public void BeforeEnteringState(SystemState next, TsavoriteKV<AdId, NumClicks, StructStoreFunctions, StructAllocator> tsavorite)
        {
            ClassicAssert.IsFalse(invokedStates.Contains(next));
            _ = invokedStates.Add(next);
        }

        public void CheckInvoked(SystemState state)
        {
            ClassicAssert.IsTrue(invokedStates.Contains(state));
        }
    }
}