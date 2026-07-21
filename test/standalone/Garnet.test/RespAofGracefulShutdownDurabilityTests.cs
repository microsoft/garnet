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
        /// With a high commit frequency (auto-commit disabled and the periodic commit far from firing),
        /// a SET is acked but never committed. A graceful shutdown before the periodic commit loses it.
        /// </summary>
        [Test]
        public void HighCommitFrequencyLosesAckedWriteOnGracefulShutdown()
        {
            const int farOffCommitMs = 60 * 60 * 1000;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, commitFrequencyMs: farOffCommitMs);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.IsTrue(db.StringSet("k", "v"), "SET should be acknowledged to the client");
            }

            server.Dispose(false);
            server = null;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, tryRecover: true, enableAOF: true, commitFrequencyMs: farOffCommitMs);
            server.Start();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.IsFalse(db.KeyExists("k"),
                    "acked write must be LOST: it was never committed and graceful shutdown does not flush the AOF");
            }
        }
    }
}
