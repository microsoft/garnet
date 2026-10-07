// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Behavior of a <c>WATCH</c> that spans an internal transaction.
    /// </summary>
    /// <remarks>
    /// Commands such as <c>SMOVE</c>, <c>LMOVE</c> and <c>RENAME</c> wrap themselves in an internal transaction, whose
    /// commit resets the transaction manager's scratch allocator in cluster mode. A watch taken before one of those
    /// must still be in force afterwards. This covers that sequence, which was otherwise untested.
    /// <para>
    /// Note this does <b>not</b> detect the allocator-lifetime defect that prompted it — it passes either way. While
    /// the watched keys were copied into the shared transaction allocator, that reset let the next transaction's keys
    /// overwrite them (instrumentation showed a watched <c>{aaa}watched</c> reading back as <c>{aaa}queuedd</c>), but
    /// the damage is not reachable from here: <c>ValidateWatchVersion</c> compares the key hash captured at WATCH
    /// time rather than re-reading the bytes, and the keys share a hash tag so slot verification is unaffected. What
    /// the corrupted bytes do reach is the lock taken at EXEC, which is not observable from a single session.
    /// </para>
    /// </remarks>
    [TestFixture]
    [NonParallelizable]
    public class ClusterWatchKeyLifetimeTests : TestBase
    {
        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context?.TearDown();

        /// <summary>
        /// A transaction queued after an internal transaction has run must still hold the key the client watched, and
        /// the watch must still abort a later transaction when that key is modified.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void WatchedKeySurvivesAnInternalTransaction([Values(false, true)] bool runInternalTransaction)
        {
            context.CreateInstances(1, disableObjects: false, enableAOF: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            var endpoint = context.clusterTestUtils.GetEndPoint(0);
            var config = new ConfigurationOptions { AbortOnConnectFail = false, ConnectRetry = 5, ConnectTimeout = 5000 };
            config.EndPoints.Add(endpoint);
            using var redis = ConnectionMultiplexer.Connect(config);
            var db = redis.GetDatabase(0);

            _ = db.SetAdd("{aaa}src", "m");

            // Same hash tag, so the transaction is single-slot either way; the watched key is the longer of the two.
            const string Watched = "{aaa}watched-key-that-is-long";
            const string Queued = "{aaa}q";

            ClassicAssert.AreEqual("OK", db.Execute("WATCH", Watched).ToString());

            if (runInternalTransaction)
            {
                // SMOVE wraps itself in an internal transaction, whose commit resets the transaction scratch allocator.
                _ = db.Execute("SMOVE", "{aaa}src", "{aaa}dst", "m");
            }

            ClassicAssert.AreEqual("OK", db.Execute("MULTI").ToString());
            ClassicAssert.AreEqual("QUEUED", db.Execute("SET", Queued, "v").ToString());

            var exec = db.Execute("EXEC");
            ClassicAssert.AreEqual("v", db.StringGet(Queued).ToString(),
                $"transaction did not apply (EXEC returned '{exec}')");

            // The watch is still in force: modifying the watched key must abort the next transaction. A watched slice
            // overwritten by the queued key would leave the client watching the wrong key.
            ClassicAssert.AreEqual("OK", db.Execute("WATCH", Watched).ToString());
            _ = db.StringSet(Watched, "changed-by-someone-else");

            ClassicAssert.AreEqual("OK", db.Execute("MULTI").ToString());
            ClassicAssert.AreEqual("QUEUED", db.Execute("SET", Queued, "second").ToString());

            var abortedExec = db.Execute("EXEC");
            ClassicAssert.IsTrue(abortedExec.IsNull, "EXEC should have aborted: the watched key was modified");
            ClassicAssert.AreEqual("v", db.StringGet(Queued).ToString(),
                "aborted transaction must not have applied");
        }
    }
}