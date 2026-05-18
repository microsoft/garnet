// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests for upgrading AOF from single-log single-replay (SL) to single-log multi-replay (SLMR).
    /// No checkpoint is required — the multi-replay coordinator falls back to all-participate
    /// for old BasicHeader transaction entries, which is conservative but correct.
    /// </summary>
    [TestFixture]
    public class AofUpgradeTests : TestBase
    {
        GarnetServer server;

        const int TestReplayTaskCount = 4;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Tests SL → SLMR upgrade for standalone (non-transactional) operations.
        /// No checkpoint needed — standalone ops have no coordination requirements.
        /// </summary>
        [Test]
        public async Task AofUpgradeSLtoSLMR_StandaloneOpsTestAsync()
        {
            // Phase 1: Start in single-log single-replay mode and write data
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // String SET operations
                for (var i = 0; i < 100; i++)
                    db.StringSet($"slKey{i}", $"slValue{i}");

                // INCR operations
                for (var i = 0; i < 10; i++)
                    db.StringIncrement("slCounter", 1);

                // List operations
                db.ListLeftPush("slList", ["a", "b", "c", "d", "e"]);

                // Hash operations
                db.HashSet("slHash", [
                    new HashEntry("field1", "val1"),
                    new HashEntry("field2", "val2"),
                    new HashEntry("field3", "val3"),
                ]);
            }

            // Commit AOF (no checkpoint) and stop
            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);

            // Phase 2: Restart with multi-replay config — AOF replay uses multiple tasks
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true,
                tryRecover: true, replayTaskCount: TestReplayTaskCount);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Verify all SL-era data survived the replay with multiple tasks
                for (var i = 0; i < 100; i++)
                    ClassicAssert.AreEqual($"slValue{i}", db.StringGet($"slKey{i}").ToString(), $"Key slKey{i} mismatch after upgrade");

                ClassicAssert.AreEqual(10, (long)db.StringGet("slCounter"));
                ClassicAssert.AreEqual(5, db.ListLength("slList"));
                ClassicAssert.AreEqual("val2", db.HashGet("slHash", "field2").ToString());

                // Phase 3: Write new data under SLMR topology
                for (var i = 100; i < 200; i++)
                    db.StringSet($"mlrKey{i}", $"mlrValue{i}");

                db.StringIncrement("slCounter", 5);
                db.ListRightPush("slList", "f");
            }

            // Verify SLMR-era writes also recover correctly with multi-replay
            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true,
                tryRecover: true, replayTaskCount: TestReplayTaskCount);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // SL-era data
                for (var i = 0; i < 100; i++)
                    ClassicAssert.AreEqual($"slValue{i}", db.StringGet($"slKey{i}").ToString());

                // SLMR-era data
                for (var i = 100; i < 200; i++)
                    ClassicAssert.AreEqual($"mlrValue{i}", db.StringGet($"mlrKey{i}").ToString());

                ClassicAssert.AreEqual(15, (long)db.StringGet("slCounter"));
                ClassicAssert.AreEqual(6, db.ListLength("slList"));
            }
        }

        /// <summary>
        /// Tests SL → SLMR upgrade for MULTI/EXEC transactions.
        /// Old BasicHeader transaction entries trigger all-participate fallback in the
        /// multi-replay coordinator — slower but correct.
        /// </summary>
        [Test]
        public async Task AofUpgradeSLtoSLMR_TransactionTestAsync()
        {
            // Phase 1: Start in SL mode and execute transactions
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Transaction 1: Multiple string SETs
                var txn1 = db.CreateTransaction();
                _ = txn1.StringSetAsync("txnKey1", "txnVal1");
                _ = txn1.StringSetAsync("txnKey2", "txnVal2");
                _ = txn1.StringSetAsync("txnKey3", "txnVal3");
                ClassicAssert.IsTrue(txn1.Execute());

                // Transaction 2: Mixed operations
                db.StringSet("txnCounter", "0");
                var txn2 = db.CreateTransaction();
                _ = txn2.StringIncrementAsync("txnCounter", 10);
                _ = txn2.StringSetAsync("txnMarker", "committed");
                ClassicAssert.IsTrue(txn2.Execute());

                // Transaction 3: With condition (optimistic locking)
                var txn3 = db.CreateTransaction();
                txn3.AddCondition(Condition.StringEqual("txnMarker", "committed"));
                _ = txn3.StringSetAsync("txnConditional", "yes");
                ClassicAssert.IsTrue(txn3.Execute());

                // Standalone writes interleaved with transactions
                db.StringSet("standalone1", "val1");

                // Transaction 4: Another set
                var txn4 = db.CreateTransaction();
                _ = txn4.StringSetAsync("txnKey4", "txnVal4");
                _ = txn4.StringSetAsync("txnKey5", "txnVal5");
                ClassicAssert.IsTrue(txn4.Execute());
            }

            // Commit AOF (no checkpoint) and stop
            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);

            // Phase 2: Restart with multi-replay — old txn entries use all-participate fallback
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true,
                tryRecover: true, replayTaskCount: TestReplayTaskCount);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Verify all transaction results from SL era
                ClassicAssert.AreEqual("txnVal1", db.StringGet("txnKey1").ToString());
                ClassicAssert.AreEqual("txnVal2", db.StringGet("txnKey2").ToString());
                ClassicAssert.AreEqual("txnVal3", db.StringGet("txnKey3").ToString());
                ClassicAssert.AreEqual(10, (long)db.StringGet("txnCounter"));
                ClassicAssert.AreEqual("committed", db.StringGet("txnMarker").ToString());
                ClassicAssert.AreEqual("yes", db.StringGet("txnConditional").ToString());
                ClassicAssert.AreEqual("txnVal4", db.StringGet("txnKey4").ToString());
                ClassicAssert.AreEqual("txnVal5", db.StringGet("txnKey5").ToString());
                ClassicAssert.AreEqual("val1", db.StringGet("standalone1").ToString());

                // Phase 3: Execute new transactions under SLMR (these get SingleLogTransactionHeader)
                var txn5 = db.CreateTransaction();
                _ = txn5.StringSetAsync("slmrTxnKey1", "slmrTxnVal1");
                _ = txn5.StringSetAsync("slmrTxnKey2", "slmrTxnVal2");
                _ = txn5.StringIncrementAsync("txnCounter", 5);
                ClassicAssert.IsTrue(txn5.Execute());
            }

            // Verify SLMR-era transactions survive another restart
            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true,
                tryRecover: true, replayTaskCount: TestReplayTaskCount);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // SL-era transactions still intact
                ClassicAssert.AreEqual("txnVal1", db.StringGet("txnKey1").ToString());
                ClassicAssert.AreEqual("txnVal2", db.StringGet("txnKey2").ToString());

                // SLMR-era transactions
                ClassicAssert.AreEqual("slmrTxnVal1", db.StringGet("slmrTxnKey1").ToString());
                ClassicAssert.AreEqual("slmrTxnVal2", db.StringGet("slmrTxnKey2").ToString());
                ClassicAssert.AreEqual(15, (long)db.StringGet("txnCounter"));
            }
        }

        /// <summary>
        /// Tests SL → SLMR upgrade for custom transaction procedures.
        /// Custom procs produce opaque AOF entries — the all-participate fallback ensures
        /// correct replay even without bitmap information in the header.
        /// </summary>
        [Test]
        public async Task AofUpgradeSLtoSLMR_CustomProcTestAsync()
        {
            // Phase 1: Start in SL mode with custom proc registered
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), new RespCommandsInfo { Arity = 4 });
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Setup: write values that the custom proc will read and propagate
                db.StringSet("readSource1", "customValue1");
                var result = db.Execute("READWRITETX", "readSource1", "writeTarget1", "writeTarget2");
                ClassicAssert.AreEqual("SUCCESS", result.ToString());

                // Second custom proc invocation
                db.StringSet("readSource2", "customValue2");
                result = db.Execute("READWRITETX", "readSource2", "writeTarget3", "writeTarget4");
                ClassicAssert.AreEqual("SUCCESS", result.ToString());

                // Verify before upgrade
                ClassicAssert.AreEqual("customValue1", db.StringGet("writeTarget1").ToString());
                ClassicAssert.AreEqual("customValue1", db.StringGet("writeTarget2").ToString());
                ClassicAssert.AreEqual("customValue2", db.StringGet("writeTarget3").ToString());
                ClassicAssert.AreEqual("customValue2", db.StringGet("writeTarget4").ToString());
            }

            // Commit AOF (no checkpoint) and stop
            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);

            // Phase 2: Restart with multi-replay — re-register custom proc
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true,
                tryRecover: true, replayTaskCount: TestReplayTaskCount);
            server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), new RespCommandsInfo { Arity = 4 });
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Verify SL-era custom proc results survived multi-replay
                ClassicAssert.AreEqual("customValue1", db.StringGet("writeTarget1").ToString());
                ClassicAssert.AreEqual("customValue1", db.StringGet("writeTarget2").ToString());
                ClassicAssert.AreEqual("customValue2", db.StringGet("writeTarget3").ToString());
                ClassicAssert.AreEqual("customValue2", db.StringGet("writeTarget4").ToString());

                // Phase 3: Execute custom proc under SLMR topology
                db.StringSet("mlrReadSource", "mlrCustomValue");
                var result = db.Execute("READWRITETX", "mlrReadSource", "mlrWriteTarget1", "mlrWriteTarget2");
                ClassicAssert.AreEqual("SUCCESS", result.ToString());
            }

            // Verify SLMR-era custom proc results survive restart
            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true,
                tryRecover: true, replayTaskCount: TestReplayTaskCount);
            server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), new RespCommandsInfo { Arity = 4 });
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // SL-era
                ClassicAssert.AreEqual("customValue1", db.StringGet("writeTarget1").ToString());
                ClassicAssert.AreEqual("customValue2", db.StringGet("writeTarget3").ToString());

                // SLMR-era
                ClassicAssert.AreEqual("mlrCustomValue", db.StringGet("mlrWriteTarget1").ToString());
                ClassicAssert.AreEqual("mlrCustomValue", db.StringGet("mlrWriteTarget2").ToString());
            }
        }

        /// <summary>
        /// Combined test: SL → SLMR with interleaved standalone ops, MULTI/EXEC, and custom procs
        /// in the same AOF stream. Exercises the multi-replay coordinator handling mixed
        /// BasicHeader entries (standalone + old txn) with the all-participate fallback.
        /// </summary>
        [Test]
        public async Task AofUpgradeSLtoSLMR_MixedOpsTestAsync()
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true);
            server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), new RespCommandsInfo { Arity = 4 });
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Standalone ops
                db.StringSet("standalone1", "val1");
                db.StringSet("standalone2", "val2");

                // MULTI/EXEC transaction
                var txn = db.CreateTransaction();
                _ = txn.StringSetAsync("txn1", "txnVal1");
                _ = txn.StringSetAsync("txn2", "txnVal2");
                ClassicAssert.IsTrue(txn.Execute());

                // Custom proc
                db.StringSet("procInput", "procData");
                var result = db.Execute("READWRITETX", "procInput", "procOut1", "procOut2");
                ClassicAssert.AreEqual("SUCCESS", result.ToString());

                // More standalone ops interleaved
                db.StringSet("standalone3", "val3");
                db.StringIncrement("counter", 42);

                // Another transaction
                var txn2 = db.CreateTransaction();
                _ = txn2.StringSetAsync("txn3", "txnVal3");
                _ = txn2.StringIncrementAsync("counter", 8);
                ClassicAssert.IsTrue(txn2.Execute());
            }

            _ = await server.Store.CommitAOFAsync(default);
            server.Dispose(false);

            // Restart with SLMR config — no checkpoint needed
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true,
                tryRecover: true, replayTaskCount: TestReplayTaskCount);
            server.Register.NewTransactionProc("READWRITETX", () => new ReadWriteTxn(), new RespCommandsInfo { Arity = 4 });
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                // Verify everything from the mixed SL-era workload
                ClassicAssert.AreEqual("val1", db.StringGet("standalone1").ToString());
                ClassicAssert.AreEqual("val2", db.StringGet("standalone2").ToString());
                ClassicAssert.AreEqual("val3", db.StringGet("standalone3").ToString());
                ClassicAssert.AreEqual("txnVal1", db.StringGet("txn1").ToString());
                ClassicAssert.AreEqual("txnVal2", db.StringGet("txn2").ToString());
                ClassicAssert.AreEqual("txnVal3", db.StringGet("txn3").ToString());
                ClassicAssert.AreEqual("procData", db.StringGet("procOut1").ToString());
                ClassicAssert.AreEqual("procData", db.StringGet("procOut2").ToString());
                ClassicAssert.AreEqual(50, (long)db.StringGet("counter"));
            }
        }
    }
}
