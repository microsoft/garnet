// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Verifies that an internal (implicit) transaction survives an AOF recovery restart on a standalone server with
    /// multiple physical AOF sublogs (AofPhysicalSublogCount &gt; 1). RENAME is used as the transaction: it must route its
    /// TxnStart marker to the physical sublog(s) its keys hash to, which ComputeSublogAccessVector derives from the
    /// transaction's locked keys. Parameterized over subLogCount: 1 (single physical log) and 2 (sharded).
    /// </summary>
    [TestFixture]
    public class AofShardedTxnRecoveryTests : TestBase
    {
        GarnetServer server;

        GarnetServer CreateShardedAofServer(int subLogCount, bool tryRecover)
        {
            // Standalone (enableCluster: false) server with subLogCount physical AOF sublogs, built via
            // GetGarnetServerOptions (the standalone CreateGarnetServer overload does not expose sublogCount).
            var opts = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                enableAOF: true,
                tryRecover: tryRecover,
                sublogCount: subLogCount);
            return new GarnetServer(opts);
        }

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // The test method carries a [Values(1, 2)] int subLogCount, but [SetUp] cannot receive the test case's
            // arguments directly, so read subLogCount from the current test's arguments (see Tsavorite's
            // OverflowBucketLockTableTests.Setup).
            var subLogCount = 1;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is int sc)
                {
                    subLogCount = sc;
                    break;
                }
            }

            server = CreateShardedAofServer(subLogCount, tryRecover: false);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.OnTearDown();
        }

        [Test]
        public void ShardedStandaloneRenameTransactionSurvivesRecovery([Values(1, 2)] int subLogCount)
        {
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                db.StringSet("k1", "v1");

                // RENAME runs as an internal (implicit) transaction: its TxnStart marker must reach the physical
                // sublog(s) its keys hash to. This exercises the standalone sharded transaction path.
                ClassicAssert.IsTrue(db.KeyRename("k1", "k2"));
                ClassicAssert.AreEqual("v1", (string)db.StringGet("k2"));

                // Commit the AOF to disk so the restart below must replay the RENAME transaction.
                db.Execute("COMMITAOF");
            }

            // Restart with recovery (keep the on-disk AOF/checkpoint).
            server.Dispose(false);
            server = CreateShardedAofServer(subLogCount, tryRecover: true);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual("v1", (string)db.StringGet("k2"), "renamed key should survive recovery");
                ClassicAssert.IsFalse(db.KeyExists("k1"), "old key should not exist after rename + recovery");
            }
        }
    }
}