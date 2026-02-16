// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Allure.NUnit;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests to investigate data consistency under different shutdown finalization sequences:
    /// 1. Checkpoint first, then AOF commit
    /// 2. AOF commit first, then Checkpoint (current production order)
    /// 3. AOF commit only
    /// 4. Checkpoint only
    ///
    /// Each test writes data to main store (string keys) and object store (sorted sets),
    /// performs the finalization sequence, disposes without cleanup, recovers, and verifies data.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    public class ShutdownDataConsistencyTests : AllureTestBase
    {
        private GarnetServer server;

        private const int KeyCount = 50;
        private const string MainStoreKeyPrefix = "shutdowntest:key:";
        private const string MainStoreValuePrefix = "shutdowntest:value:";
        private const string ObjectStoreKeyPrefix = "shutdowntest:zset:";

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        /// <summary>
        /// Creates a server with both AOF and storage tier enabled (low memory forces spill to disk).
        /// </summary>
        private GarnetServer CreateServerWithAofAndStorage(bool tryRecover = false)
        {
            return TestUtils.CreateGarnetServer(
                TestUtils.MethodTestDir,
                enableAOF: true,
                lowMemory: true,
                tryRecover: tryRecover);
        }

        /// <summary>
        /// Populates main store with string key-value pairs and object store with sorted sets.
        /// </summary>
        private void PopulateData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig(allowAdmin: true));
            var db = redis.GetDatabase(0);

            // Main store: string keys
            for (var i = 0; i < KeyCount; i++)
            {
                db.StringSet($"{MainStoreKeyPrefix}{i}", $"{MainStoreValuePrefix}{i}");
            }

            // Object store: sorted sets
            for (var i = 0; i < KeyCount; i++)
            {
                var entries = new SortedSetEntry[]
                {
                    new($"member_a_{i}", i * 10),
                    new($"member_b_{i}", i * 10 + 1),
                    new($"member_c_{i}", i * 10 + 2),
                };
                db.SortedSetAdd($"{ObjectStoreKeyPrefix}{i}", entries);
            }
        }

        /// <summary>
        /// Verifies all main store and object store data is recovered correctly.
        /// Returns (mainStoreRecovered, objectStoreRecovered) counts.
        /// </summary>
        private (int mainStoreRecovered, int objectStoreRecovered) VerifyRecoveredData()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var mainStoreRecovered = 0;
            for (var i = 0; i < KeyCount; i++)
            {
                var value = db.StringGet($"{MainStoreKeyPrefix}{i}");
                if (value.HasValue)
                {
                    ClassicAssert.AreEqual($"{MainStoreValuePrefix}{i}", value.ToString(),
                        $"Main store key {MainStoreKeyPrefix}{i} has wrong value after recovery");
                    mainStoreRecovered++;
                }
            }

            var objectStoreRecovered = 0;
            for (var i = 0; i < KeyCount; i++)
            {
                var members = db.SortedSetRangeByScoreWithScores($"{ObjectStoreKeyPrefix}{i}");
                if (members.Length > 0)
                {
                    ClassicAssert.AreEqual(3, members.Length,
                        $"Object store key {ObjectStoreKeyPrefix}{i} should have 3 members");
                    ClassicAssert.AreEqual($"member_a_{i}", members[0].Element.ToString());
                    ClassicAssert.AreEqual(i * 10, members[0].Score);
                    ClassicAssert.AreEqual($"member_b_{i}", members[1].Element.ToString());
                    ClassicAssert.AreEqual(i * 10 + 1, members[1].Score);
                    ClassicAssert.AreEqual($"member_c_{i}", members[2].Element.ToString());
                    ClassicAssert.AreEqual(i * 10 + 2, members[2].Score);
                    objectStoreRecovered++;
                }
            }

            return (mainStoreRecovered, objectStoreRecovered);
        }

        /// <summary>
        /// Scenario 1: Checkpoint → AOF commit sequence.
        /// Takes checkpoint first, then commits AOF.
        /// </summary>
        [Test]
        public void CheckpointThenAofCommit_DataConsistencyTest()
        {
            server = CreateServerWithAofAndStorage();
            server.Start();

            PopulateData();

            // Sequence: Checkpoint first, then AOF commit
            server.Store.TakeCheckpoint(background: false);
            server.Store.CommitAOF(spinWait: true);

            server.Dispose(false);

            // Recover and verify
            server = CreateServerWithAofAndStorage(tryRecover: true);
            server.Start();

            var (mainRecovered, objRecovered) = VerifyRecoveredData();

            TestContext.Progress.WriteLine(
                $"[Checkpoint→AOF] Main store: {mainRecovered}/{KeyCount}, Object store: {objRecovered}/{KeyCount}");

            ClassicAssert.AreEqual(KeyCount, mainRecovered,
                "Checkpoint→AOF: Not all main store keys recovered");
            ClassicAssert.AreEqual(KeyCount, objRecovered,
                "Checkpoint→AOF: Not all object store keys recovered");
        }

        /// <summary>
        /// Scenario 2: AOF commit → Checkpoint sequence (current production order).
        /// Commits AOF first, then takes checkpoint.
        /// </summary>
        [Test]
        public void AofCommitThenCheckpoint_DataConsistencyTest()
        {
            server = CreateServerWithAofAndStorage();
            server.Start();

            PopulateData();

            // Sequence: AOF commit first, then Checkpoint (matches current FinalizeDataAsync order)
            server.Store.CommitAOF(spinWait: true);
            server.Store.TakeCheckpoint(background: false);

            server.Dispose(false);

            // Recover and verify
            server = CreateServerWithAofAndStorage(tryRecover: true);
            server.Start();

            var (mainRecovered, objRecovered) = VerifyRecoveredData();

            TestContext.Progress.WriteLine(
                $"[AOF→Checkpoint] Main store: {mainRecovered}/{KeyCount}, Object store: {objRecovered}/{KeyCount}");

            ClassicAssert.AreEqual(KeyCount, mainRecovered,
                "AOF→Checkpoint: Not all main store keys recovered");
            ClassicAssert.AreEqual(KeyCount, objRecovered,
                "AOF→Checkpoint: Not all object store keys recovered");
        }

        /// <summary>
        /// Scenario 3: AOF commit only (no checkpoint).
        /// Only commits AOF before shutdown.
        /// </summary>
        [Test]
        public void AofCommitOnly_DataConsistencyTest()
        {
            server = CreateServerWithAofAndStorage();
            server.Start();

            PopulateData();

            // Sequence: AOF commit only
            server.Store.CommitAOF(spinWait: true);

            server.Dispose(false);

            // Recover and verify
            server = CreateServerWithAofAndStorage(tryRecover: true);
            server.Start();

            var (mainRecovered, objRecovered) = VerifyRecoveredData();

            TestContext.Progress.WriteLine(
                $"[AOF Only] Main store: {mainRecovered}/{KeyCount}, Object store: {objRecovered}/{KeyCount}");

            ClassicAssert.AreEqual(KeyCount, mainRecovered,
                "AOF Only: Not all main store keys recovered");
            ClassicAssert.AreEqual(KeyCount, objRecovered,
                "AOF Only: Not all object store keys recovered");
        }

        /// <summary>
        /// Scenario 4: Checkpoint only (no AOF commit).
        /// Only takes checkpoint before shutdown.
        /// </summary>
        [Test]
        public void CheckpointOnly_DataConsistencyTest()
        {
            server = CreateServerWithAofAndStorage();
            server.Start();

            PopulateData();

            // Sequence: Checkpoint only (no AOF commit)
            server.Store.TakeCheckpoint(background: false);

            server.Dispose(false);

            // Recover and verify
            server = CreateServerWithAofAndStorage(tryRecover: true);
            server.Start();

            var (mainRecovered, objRecovered) = VerifyRecoveredData();

            TestContext.Progress.WriteLine(
                $"[Checkpoint Only] Main store: {mainRecovered}/{KeyCount}, Object store: {objRecovered}/{KeyCount}");

            // Note: With checkpoint only (no AOF commit), data written after the last
            // checkpoint but before the AOF commit may be lost. This test documents
            // the actual behavior for investigation purposes.
            ClassicAssert.AreEqual(KeyCount, mainRecovered,
                "Checkpoint Only: Not all main store keys recovered");
            ClassicAssert.AreEqual(KeyCount, objRecovered,
                "Checkpoint Only: Not all object store keys recovered");
        }

        /// <summary>
        /// Scenario 5: No finalization at all (baseline - expect potential data loss).
        /// Neither AOF commit nor checkpoint before shutdown.
        /// This serves as a negative baseline to confirm that finalization is actually needed.
        /// </summary>
        [Test]
        public void NoFinalization_DataConsistencyTest()
        {
            server = CreateServerWithAofAndStorage();
            server.Start();

            PopulateData();

            // No finalization at all - just dispose
            server.Dispose(false);

            // Recover and verify
            server = CreateServerWithAofAndStorage(tryRecover: true);
            server.Start();

            var (mainRecovered, objRecovered) = VerifyRecoveredData();

            TestContext.Progress.WriteLine(
                $"[No Finalization] Main store: {mainRecovered}/{KeyCount}, Object store: {objRecovered}/{KeyCount}");

            // This is a baseline test - data loss is expected here.
            // The purpose is to show that finalization (AOF commit and/or checkpoint) is required.
            TestContext.Progress.WriteLine(
                $"[No Finalization] Data loss: main store={KeyCount - mainRecovered}, object store={KeyCount - objRecovered}");
        }

        /// <summary>
        /// Scenario 6: Interleaved writes with checkpoint then additional writes with AOF commit.
        /// Simulates the case where new writes happen between checkpoint and AOF commit.
        /// </summary>
        [Test]
        public void CheckpointThenMoreWritesThenAofCommit_DataConsistencyTest()
        {
            server = CreateServerWithAofAndStorage();
            server.Start();

            // Phase 1: Initial data
            PopulateData();

            // Take checkpoint
            server.Store.TakeCheckpoint(background: false);

            // Phase 2: Write additional data AFTER checkpoint
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = KeyCount; i < KeyCount * 2; i++)
                {
                    db.StringSet($"{MainStoreKeyPrefix}{i}", $"{MainStoreValuePrefix}{i}");
                }
            }

            // Now commit AOF (should capture phase 2 writes)
            server.Store.CommitAOF(spinWait: true);

            server.Dispose(false);

            // Recover and verify
            server = CreateServerWithAofAndStorage(tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var phase1Recovered = 0;
                for (var i = 0; i < KeyCount; i++)
                {
                    var value = db.StringGet($"{MainStoreKeyPrefix}{i}");
                    if (value.HasValue && value.ToString() == $"{MainStoreValuePrefix}{i}")
                        phase1Recovered++;
                }

                var phase2Recovered = 0;
                for (var i = KeyCount; i < KeyCount * 2; i++)
                {
                    var value = db.StringGet($"{MainStoreKeyPrefix}{i}");
                    if (value.HasValue && value.ToString() == $"{MainStoreValuePrefix}{i}")
                        phase2Recovered++;
                }

                TestContext.Progress.WriteLine(
                    $"[Checkpoint→Writes→AOF] Phase1: {phase1Recovered}/{KeyCount}, Phase2 (post-checkpoint): {phase2Recovered}/{KeyCount}");

                ClassicAssert.AreEqual(KeyCount, phase1Recovered,
                    "Checkpoint→Writes→AOF: Not all phase 1 keys recovered");
                ClassicAssert.AreEqual(KeyCount, phase2Recovered,
                    "Checkpoint→Writes→AOF: Not all phase 2 (post-checkpoint) keys recovered");
            }
        }

        /// <summary>
        /// Scenario 7: AOF commit then additional writes then checkpoint.
        /// Simulates the case where new writes happen between AOF commit and checkpoint.
        /// </summary>
        [Test]
        public void AofCommitThenMoreWritesThenCheckpoint_DataConsistencyTest()
        {
            server = CreateServerWithAofAndStorage();
            server.Start();

            // Phase 1: Initial data
            PopulateData();

            // Commit AOF
            server.Store.CommitAOF(spinWait: true);

            // Phase 2: Write additional data AFTER AOF commit
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                for (var i = KeyCount; i < KeyCount * 2; i++)
                {
                    db.StringSet($"{MainStoreKeyPrefix}{i}", $"{MainStoreValuePrefix}{i}");
                }
            }

            // Now take checkpoint (should capture phase 2 writes)
            server.Store.TakeCheckpoint(background: false);

            server.Dispose(false);

            // Recover and verify
            server = CreateServerWithAofAndStorage(tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);

                var phase1Recovered = 0;
                for (var i = 0; i < KeyCount; i++)
                {
                    var value = db.StringGet($"{MainStoreKeyPrefix}{i}");
                    if (value.HasValue && value.ToString() == $"{MainStoreValuePrefix}{i}")
                        phase1Recovered++;
                }

                var phase2Recovered = 0;
                for (var i = KeyCount; i < KeyCount * 2; i++)
                {
                    var value = db.StringGet($"{MainStoreKeyPrefix}{i}");
                    if (value.HasValue && value.ToString() == $"{MainStoreValuePrefix}{i}")
                        phase2Recovered++;
                }

                TestContext.Progress.WriteLine(
                    $"[AOF→Writes→Checkpoint] Phase1: {phase1Recovered}/{KeyCount}, Phase2 (post-AOF): {phase2Recovered}/{KeyCount}");

                ClassicAssert.AreEqual(KeyCount, phase1Recovered,
                    "AOF→Writes→Checkpoint: Not all phase 1 keys recovered");
                ClassicAssert.AreEqual(KeyCount, phase2Recovered,
                    "AOF→Writes→Checkpoint: Not all phase 2 (post-AOF) keys recovered");
            }
        }
    }
}