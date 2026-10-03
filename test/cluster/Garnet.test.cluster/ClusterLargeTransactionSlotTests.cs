// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Cluster slot verification of a queued MULTI/EXEC transaction.
    /// </summary>
    /// <remarks>
    /// In cluster mode the keys of each queued command are recorded as the transaction is built, and verified together
    /// at EXEC. A transaction large enough to span several network reads is the interesting case: the receive buffer is
    /// compacted between reads, so anything recorded as a reference into it would be reading the wrong bytes by the time
    /// EXEC verifies.
    /// </remarks>
    [TestFixture]
    [NonParallelizable]
    public class ClusterLargeTransactionSlotTests : TestBase
    {
        ClusterTestContext context;

        /// <summary>
        /// The direct connection used by the current test, disposed in <see cref="TearDown"/>.
        /// </summary>
        /// <remarks>
        /// Disposal is not merely tidiness. Every cluster fixture in this assembly binds the same reserved ports, so
        /// node 0 of the next fixture listens where this one did. A multiplexer left running has
        /// <see cref="ConfigurationOptions.AbortOnConnectFail"/> false, so it keeps retrying that endpoint and
        /// reconnects to whichever server binds it next, showing up there as an unexplained client connection.
        /// </remarks>
        ConnectionMultiplexer redis;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            // Before the servers, so the client stops reconnecting while they are being torn down.
            redis?.Dispose();
            redis = null;
            context?.TearDown();
        }

        IDatabase SetUpSingleNodeCluster()
        {
            context.CreateInstances(1, disableObjects: false, enableAOF: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            var config = new ConfigurationOptions { AbortOnConnectFail = false, ConnectRetry = 5, ConnectTimeout = 5000 };
            config.EndPoints.Add(context.clusterTestUtils.GetEndPoint(0));
            redis = ConnectionMultiplexer.Connect(config);
            return redis.GetDatabase(0);
        }

        /// <summary>
        /// A transaction whose keys are all in one slot must commit however many commands it holds. The counts here
        /// straddle the receive buffer: the largest spans several network reads, so the buffer is compacted underneath
        /// the recorded keys while the transaction is still being queued.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void LargeSameSlotTransactionCommits([Values(1, 100, 1000, 2000)] int opsPerKey)
        {
            var db = SetUpSingleNodeCluster();
            string[] keys = ["{lt}a", "{lt}b", "{lt}c"];

            var txn = db.CreateTransaction();
            for (var i = 0; i < opsPerKey; i++)
            {
                foreach (var key in keys)
                    _ = txn.StringIncrementAsync(key, 1);
            }

            ClassicAssert.IsTrue(txn.Execute(), "transaction was not committed");

            var expected = opsPerKey.ToString();
            foreach (var key in keys)
                ClassicAssert.AreEqual(expected, db.StringGet(key).ToString(), $"wrong value for {key}");
        }

        /// <summary>
        /// The same, on a single key. One key cannot span two slots, so a CROSSSLOT here can only mean the verified
        /// keys were not the ones the transaction queued.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void LargeSingleKeyTransactionCommits([Values(1000, 6000)] int ops)
        {
            var db = SetUpSingleNodeCluster();
            const string Key = "{lt}solo";

            var txn = db.CreateTransaction();
            for (var i = 0; i < ops; i++)
                _ = txn.StringIncrementAsync(Key, 1);

            ClassicAssert.IsTrue(txn.Execute(), "transaction was not committed");
            ClassicAssert.AreEqual(ops.ToString(), db.StringGet(Key).ToString());
        }

        /// <summary>
        /// A genuinely cross-slot transaction must still be rejected, including when it is large enough to span the
        /// receive buffer — the fix must not have turned the check off.
        /// </summary>
        /// <remarks>
        /// Driven through the raw RESP client rather than <c>StackExchange.Redis</c>, whose transaction API rejects a
        /// cross-slot batch on the client and so never reaches the server's check.
        /// </remarks>
        [Test]
        [Category("CLUSTER")]
        public void LargeCrossSlotTransactionIsRejected()
        {
            _ = SetUpSingleNodeCluster();
            var client = context.clusterTestUtils.GetGarnetClientSession(0);

            ClassicAssert.AreEqual("OK", client.ExecuteAsync(["MULTI"]).GetAwaiter().GetResult());

            // Two distinct hash tags, so the transaction spans two slots, and enough commands to span the buffer.
            for (var i = 0; i < 2000; i++)
            {
                ClassicAssert.AreEqual("QUEUED", client.ExecuteAsync(["INCR", "{lt1}a"]).GetAwaiter().GetResult());
                ClassicAssert.AreEqual("QUEUED", client.ExecuteAsync(["INCR", "{lt2}b"]).GetAwaiter().GetResult());
            }

            var ex = Assert.Throws<System.Exception>(() => client.ExecuteAsync(["EXEC"]).GetAwaiter().GetResult());
            ClassicAssert.AreEqual("CROSSSLOT Keys in request do not hash to the same slot", ex.Message);
        }
    }
}