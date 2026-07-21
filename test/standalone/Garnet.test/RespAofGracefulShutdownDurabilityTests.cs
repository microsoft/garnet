// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Proves that a write which was acknowledged to the client but whose AOF commit has not become
    /// durable is LOST on a graceful shutdown. Graceful <c>Dispose</c> cancels the commit task and tears
    /// the log down without flushing, so recovery only sees data up to <c>CommittedUntilAddress</c>.
    /// </summary>
    [TestFixture]
    public class RespAofGracefulShutdownDurabilityTests : TestBase
    {
        GarnetServer server;

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
        /// With a high commit frequency (auto-commit disabled and the periodic commit far from firing), a write
        /// that is not explicitly committed is acked but never made durable, so a graceful shutdown loses it while
        /// an explicitly committed control write survives. This characterizes the shutdown contract: graceful
        /// <c>Dispose</c> does not flush the AOF, so durability requires an explicit commit (COMMITAOF/WaitForCommit).
        /// </summary>
        [Test]
        public void UncommittedWriteLostButCommittedWriteSurvivesOnGracefulShutdown()
        {
            const int farOffCommitMs = 60 * 60 * 1000;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, commitFrequencyMs: farOffCommitMs);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.IsTrue(db.StringSet("control", "cv"), "control SET should be acknowledged");
                db.Execute("COMMITAOF");
                ClassicAssert.IsTrue(db.StringSet("k", "v"), "target SET should be acknowledged to the client");
            }

            server.Dispose(false);
            server = null;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, commitFrequencyMs: farOffCommitMs);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual("cv", db.StringGet("control").ToString(),
                    "explicitly committed control write must survive recovery (proves the AOF/recovery pipeline works)");
                ClassicAssert.IsFalse(db.KeyExists("k"),
                    "target write must be LOST: it was never committed and graceful shutdown does not flush the AOF");
            }
        }
    }
}
