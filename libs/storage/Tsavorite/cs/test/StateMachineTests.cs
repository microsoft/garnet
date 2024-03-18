// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;
using Tsavorite.test.recovery.sumstore;

namespace Tsavorite.test.statemachine
{
    [TestFixture]
    public class StateMachineTests
    {
        IDevice log;
        TsavoriteKV<AdId, NumClicks> store;
        const int numOps = 5000;
        AdId[] inputArray;

        [SetUp]
        public void Setup()
        {
            inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/StateMachineTest1.log", deleteOnClose: true);
            string checkpointDir = TestUtils.MethodTestDir + "/statemachinetest";
            Directory.CreateDirectory(checkpointDir);
            store = new TsavoriteKV<AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 10, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
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
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            s2.Refresh();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), store.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.Refresh();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);


            // We should be in PERSISTENCE_CALLBACK, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), store.SystemState));

            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            uc1.EndUnsafe();
            s1.Dispose();

            RecoverAndTest(log);
        }


        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest2()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            // Dispose session s2; moves state machine forward to WAIT_FLUSH, 2
            s2.Dispose();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), store.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            // Since s1 is the only session now, it will fast-foward state machine
            // to completion
            uc1.Refresh();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            uc1.EndUnsafe();
            s1.Dispose();

            RecoverAndTest(log);
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest3()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s1
            uc1.Refresh();

            // s1 is now in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), SystemState.Make(s1.ctx.phase, s1.ctx.version)));

            // Suspend s1
            uc1.EndUnsafe();

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.BeginUnsafe();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            s2.Dispose();

            uc1.EndUnsafe();
            s1.Dispose();

            RecoverAndTest(log);
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest4()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            // s1 is now in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), SystemState.Make(s1.ctx.phase, s1.ctx.version)));

            // Suspend s1
            uc1.EndUnsafe();

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.BeginUnsafe();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            s2.Dispose();

            uc1.EndUnsafe();
            s1.Dispose();

            RecoverAndTest(log);
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest5()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            uc1.Refresh();
            s2.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            uc1.Refresh();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), store.SystemState));


            s2.Refresh();

            // We should be in PERSISTENCE_CALLBACK, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), store.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.Refresh();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // No callback here since already done
            uc1.Refresh();

            // Suspend s1
            uc1.EndUnsafe();

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // Expect no checkpoint completion callback on resume
            f.checkpointCallbackExpectation = 0;

            uc1.BeginUnsafe();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            s2.Dispose();

            uc1.EndUnsafe();
            s1.Dispose();

            RecoverAndTest(log);
        }


        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void StateMachineTest6()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // Suspend s1
            uc1.EndUnsafe();

            // s1 is now in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), SystemState.Make(s1.ctx.phase, s1.ctx.version)));

            // System should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            s2.Dispose();

            store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

            // We should be in REST, 3
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 3), store.SystemState));

            // Expect checkpoint completion callback on resume
            f.checkpointCallbackExpectation = 1;

            uc1.BeginUnsafe();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            uc1.EndUnsafe();
            s1.Dispose();

            RecoverAndTest(log);
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void LUCScenario1()
        {
            CreateSessions(out var f, out var s1, out var ts, out var lts);
            // System should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            lts.getLUC();
            Assert.IsTrue(lts.isProtected);

            store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);

            // System should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            ts.Refresh();
            lts.Refresh();

            // System should be in PREPARE, 1 Since there is an active locking session
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            lts.DisposeLUC();

            ts.Refresh();
            // fast-foward state machine to completion
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;
            lts.Refresh();

            RecoverAndTest(log);
            s1.Dispose();
            ts.Dispose();
            lts.Dispose();
        }

        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void LUCScenario2()
        {
            CreateSessions(out var f, out var s1, out var ts, out var lts);

            // System should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            var uc1 = s1.UnsafeContext;
            uc1.BeginUnsafe();

            store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);

            // should not succeed since checkpoint is in progress
            lts.getLUC();
            Assert.IsFalse(lts.isProtected);

            // We should be in PREPARE phase
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            ts.Refresh();
            // System should be in PREPARE, 1 
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // should not succeed since checkpoint is in progress
            lts.getLUC();
            Assert.IsFalse(lts.isProtected);

            uc1.EndUnsafe();

            // fast-foward state machine to completion
            ts.Refresh();
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // should be true since checkpoint is done
            lts.getLUC();
            Assert.IsTrue(lts.isProtected);
            lts.DisposeLUC();

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;
            RecoverAndTest(log);
            s1.Dispose();
            ts.Dispose();
            lts.Dispose();
        }
        [TestCase]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void LUCScenario3()
        {

            CreateSessions(out var f, out var s1, out var ts, out var lts);

            // System should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            // Start first LUC before checkpoint
            var luc1 = s1.LockableUnsafeContext;
            luc1.BeginUnsafe();
            luc1.BeginLockable();

            store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);

            // System should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            luc1.Refresh();
            ts.Refresh();
            luc1.Refresh();

            // System should be in PREPARE, 1 Since there is an active locking session
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // should not let new LUC start since checkpoint is in progress
            lts.getLUC();
            Assert.IsFalse(lts.isProtected);

            // We still should be in PREPARE phase
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // End first LUC 
            luc1.EndLockable();
            luc1.EndUnsafe();

            s1.Refresh();
            // System should be in IN_PROGRESS, 1 
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));

            // should be true since checkpoint is in IN_PROGRESS phase
            lts.getLUC();
            Assert.IsTrue(lts.isProtected);
            lts.DisposeLUC();

            ts.Refresh();
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;
            RecoverAndTest(log);
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
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            // Refresh session s2
            s2.Refresh();
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            s2.Refresh();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.Refresh();

            // Completion callback should have been called once
            Assert.IsTrue(f.checkpointCallbackExpectation == 0);

            // We should be in PERSISTENCE_CALLBACK, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), store.SystemState));
            callback.CheckInvoked(store.SystemState);

            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            uc1.EndUnsafe();
            s1.Dispose();

            RecoverAndTest(log);
        }


        [TestCase]
        [Category("TsavoriteKV")]
        [Category("CheckpointRestore")]
        public void VersionChangeTest()
        {
            var toVersion = 1 + (1 << 14);
            Prepare(out var f, out var s1, out var uc1, out var s2, toVersion);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));

            // Refresh session s2
            s2.Refresh();
            uc1.Refresh();

            // We should now be in IN_PROGRESS, toVersion
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, toVersion), store.SystemState));

            s2.Refresh();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, toVersion), store.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.Refresh();

            // Completion callback should have been called once
            Assert.IsTrue(f.checkpointCallbackExpectation == 0);

            // We should be in PERSISTENCE_CALLBACK, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, toVersion), store.SystemState));

            s2.Refresh();

            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, toVersion), store.SystemState));


            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            uc1.EndUnsafe();
            s1.Dispose();

            RecoverAndTest(log);
        }


        void Prepare(out SimpleFunctions f,
            out ClientSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions> s1,
            out UnsafeContext<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions> uc1,
            out ThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions> s2,
            long toVersion = -1)
        {
            f = new SimpleFunctions();

            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            // Take index checkpoint for recovery purposes
            store.TryInitiateIndexCheckpoint(out _);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

            // Index checkpoint does not update version, so
            // we should still be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            NumClicks value;

            s1 = store.NewSession<NumClicks, NumClicks, Empty, SimpleFunctions>(f, "foo");

            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                s1.Upsert(ref inputArray[key], ref value, Empty.Default, key);
            }

            // Ensure state machine needs no I/O wait during WAIT_FLUSH
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, true);

            // Create unsafe context and hold epoch to prepare for manual state machine driver
            uc1 = s1.UnsafeContext;
            uc1.BeginUnsafe();

            // Start session s2 on another thread for testing
            s2 = store.CreateThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions>(f);

            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver, targetVersion: toVersion);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), store.SystemState));
        }


        void CreateSessions(out SimpleFunctions f,
            out ClientSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions> s1,
            out ThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions> ts,
            out LUCThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions> lts)
        {
            f = new SimpleFunctions();
            NumClicks value;

            s1 = store.NewSession<NumClicks, NumClicks, Empty, SimpleFunctions>(f, "foo");

            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                s1.Upsert(ref inputArray[key], ref value, Empty.Default, key);
            }

            // Ensure state machine needs no I/O wait during WAIT_FLUSH
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, true);

            // Start session s2 on another thread for testing
            ts = store.CreateThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions>(f);
            lts = store.CreateLUCThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions>(f);

            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), store.SystemState));

            // Take index checkpoint for recovery purposes
            store.TryInitiateIndexCheckpoint(out _);
            store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
        }

        void RecoverAndTest(IDevice log)
        {
            NumClicks inputArg = default;
            NumClicks output = default;
            var f = new SimpleFunctions();

            var store = new TsavoriteKV<AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 10, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir + "/statemachinetest" }
                );

            store.Recover(); // sync, does not require session

            using (var s3 = store.ResumeSession<NumClicks, NumClicks, Empty, SimpleFunctions>(f, "foo", out CommitPoint lsn))
            {
                Assert.AreEqual(numOps - 1, lsn.UntilSerialNo);

                // Expect checkpoint completion callback
                f.checkpointCallbackExpectation = 1;

                s3.Refresh();

                // Completion callback should have been called once
                Assert.AreEqual(0, f.checkpointCallbackExpectation);

                for (var key = 0; key < numOps; key++)
                {
                    var status = s3.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default, s3.SerialNo);

                    if (status.IsPending)
                        s3.CompletePending(true);
                    else
                    {
                        Assert.AreEqual(key, output.numClicks);
                    }
                }
            }

            store.Dispose();
        }
    }

    public class SimpleFunctions : SimpleFunctions<AdId, NumClicks, Empty>
    {
        public int checkpointCallbackExpectation = 0;

        public override void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
        {
            switch (checkpointCallbackExpectation)
            {
                case 0:
                    Assert.Fail("Unexpected checkpoint callback");
                    break;
                default:
                    Interlocked.Decrement(ref checkpointCallbackExpectation);
                    break;
            }
        }

        public override void ReadCompletionCallback(ref AdId key, ref NumClicks input, ref NumClicks output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key.adId, output.numClicks);
        }
    }

    public class TestCallback : IStateMachineCallback
    {
        private readonly HashSet<SystemState> invokedStates = new();


        public void BeforeEnteringState<Key1, Value>(SystemState next, TsavoriteKV<Key1, Value> tsavorite)
        {
            Assert.IsFalse(invokedStates.Contains(next));
            invokedStates.Add(next);
        }

        public void CheckInvoked(SystemState state)
        {
            Assert.IsTrue(invokedStates.Contains(state));
        }
    }
}