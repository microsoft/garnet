// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class PrimaryFailoverTests : TestBase
    {
        private ClusterTestContext context;
        private const string Key = "{failover}key";

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            context.TearDown();
        }

        [TestCase(FailoverOption.FORCE)]
        [TestCase(FailoverOption.TAKEOVER)]
        public async Task ClientFailoverOptionTest(FailoverOption option)
        {
            CreatePrimaryAndReplica(1);
            using GarnetClient client = new(context.clusterTestUtils.GetEndPoint(1));
            await client.ConnectAsync(context.cts.Token);

            Assert.That(await client.Failover(option, context.cts.Token), Is.True);
            WaitForPrimary(1);
            Assert.That(ExecuteNode(1, "GET", Key).ToString(), Is.EqualTo("value"));
        }

        [TestCase(1)]
        [TestCase(2)]
        public void PrimaryFailoverPromotesWritableReplicaWithExistingDataTest(int sublogCount)
        {
            CreatePrimaryAndReplica(sublogCount);
            string primaryId = ExecuteNode(0, "CLUSTER", "MYID").ToString();
            string replicaId = ExecuteNode(1, "CLUSTER", "MYID").ToString();
            IPEndPoint replicaEndpoint = context.clusterTestUtils.GetEndPoint(1);

            Assert.That(ExecuteNode(0, "FAILOVER", "TO", replicaEndpoint.Address.ToString(), replicaEndpoint.Port, "TIMEOUT", 30000).ToString(), Is.EqualTo("OK"));
            WaitForPrimary(1);
            Assert.That(ExecuteNode(1, "GET", Key).ToString(), Is.EqualTo("value"));
            Assert.That(ExecuteNode(1, "CLUSTER", "MYID").ToString(), Is.EqualTo(replicaId));
            Assert.That(ExecuteNode(1, "CLUSTER", "MYID").ToString(), Is.Not.EqualTo(primaryId));
            Assert.That(ExecuteNode(1, "SET", Key, "updated").ToString(), Is.EqualTo("OK"));
            Assert.That(ExecuteNode(1, "GET", Key).ToString(), Is.EqualTo("updated"));
        }

        private void CreatePrimaryAndReplica(int sublogCount)
        {
            context.CreateInstances(2, enableAOF: true, sublogCount: sublogCount);
            context.CreateConnection();
            context.SimplePrimaryReplicaSetup();
            Assert.That(ExecuteNode(0, "SET", Key, "value").ToString(), Is.EqualTo("OK"));
            Assert.That(context.clusterTestUtils.ClusterReplicate(1, 0), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, cancellation: context.cts.Token);
            string replicaId = ExecuteNode(1, "CLUSTER", "MYID").ToString();
            string primaryId = ExecuteNode(0, "CLUSTER", "MYID").ToString();
            context.clusterTestUtils.WaitForSlotOwnership(1, primaryId, [0, 16383]);
            while (!((RedisResult[])ExecuteNode(0, "CLUSTER", "REPLICAS", primaryId)).Any(replica => replica.ToString().StartsWith(replicaId, StringComparison.Ordinal)))
            {
                context.cts.Token.ThrowIfCancellationRequested();
                Thread.Sleep(20);
            }
        }

        private void WaitForPrimary(int index)
        {
            while (((RedisResult[])ExecuteNode(index, "ROLE"))[0].ToString() != "master")
            {
                context.cts.Token.ThrowIfCancellationRequested();
                Thread.Sleep(20);
            }
        }

        private RedisResult ExecuteNode(int index, string command, params object[] args)
            => context.clusterTestUtils.GetServer(index).Execute(0, command, args, CommandFlags.NoRedirect);
    }
}