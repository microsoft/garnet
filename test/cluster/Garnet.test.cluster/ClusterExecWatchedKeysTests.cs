// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Reflection;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// How an <c>EXEC</c> lists the keys it watches for slot verification.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterExecWatchedKeysTests : TestBase
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
        /// An EXEC that finds a key being migrated parks, and is verified again when the migration moves on. The keys
        /// it watches are listed once however many times that happens, so a transaction parked across a long migration
        /// does not grow its key list, and the key bytes behind it, with every wake-up. The next transaction on the
        /// session lists its own watched keys.
        /// </summary>
        [Test]
        [Category("CLUSTER")]
        public void WatchedKeysAreListedOnceHoweverOftenAnExecIsVerified()
        {
            context.CreateInstances(1, disableObjects: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 0, logger: context.logger);

            using var client = new RawRespSession(endpoint: context.endpoints[0]);
            var clientId = long.Parse(client.Execute("CLIENT", "ID").Trim().TrimStart(':'));
            var txn = SessionOf(context.nodes[0], clientId).txnManager;

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("WATCH", "{a}one", "{a}two"));
            ClassicAssert.AreEqual("+OK\r\n", client.Execute("MULTI"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("SET", "{a}queued", "v"));

            var queued = txn.txnKeysParseState.Count;
            ClassicAssert.AreEqual(1, queued, "The queued SET lists its key");

            txn.GetSlotVerificationInput(0, out _);
            ClassicAssert.AreEqual(queued + 2, txn.txnKeysParseState.Count, "The two watched keys join the queued key");

            txn.GetSlotVerificationInput(0, out _);
            ClassicAssert.AreEqual(queued + 2, txn.txnKeysParseState.Count, "Verifying the same EXEC again lists nothing more");

            ClassicAssert.AreEqual("*1\r\n+OK\r\n", client.Execute("EXEC"));

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("WATCH", "{a}three"));
            ClassicAssert.AreEqual("+OK\r\n", client.Execute("MULTI"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("SET", "{a}queued", "w"));

            queued = txn.txnKeysParseState.Count;
            txn.GetSlotVerificationInput(0, out _);
            ClassicAssert.AreEqual(queued + 1, txn.txnKeysParseState.Count, "The next transaction lists its own watched key");
        }

        static RespServerSession SessionOf(GarnetServer server, long clientId)
        {
            var field = typeof(GarnetServer).GetField("servers", BindingFlags.Instance | BindingFlags.NonPublic);
            ClassicAssert.IsNotNull(field, "GarnetServer should still hold its listeners in a 'servers' field");

            foreach (var listener in (IGarnetServer[])field.GetValue(server))
            {
                foreach (var consumer in ((GarnetServerBase)listener).ActiveConsumers())
                {
                    if (consumer is RespServerSession session && session.Id == clientId)
                        return session;
                }
            }

            Assert.Fail($"No live session has id {clientId}");
            return null;
        }
    }
}