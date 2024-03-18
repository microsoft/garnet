// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ClusterManagementTests
    {
        ClusterTestContext context;
        readonly int defaultShards = 3;

        readonly HashSet<string> monitorTests = new()
        {
            //Add test names here to change logger verbosity
        };

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests);
        }

        [TearDown]
        public void TearDown()
        {
            context.TearDown();
        }

        [Test, Order(1)]
        [TestCase(0, 16383)]
        [TestCase(1234, 5678)]
        public void ClusterSlotsTest(int startSlot, int endSlot)
        {
            List<(int, int)>[] slotRanges = new List<(int, int)>[1];
            slotRanges[0] = new List<(int, int)> { (startSlot, endSlot) };
            context.CreateInstances(defaultShards);
            context.CreateConnection();
            context.clusterTestUtils.SimpleSetupCluster(customSlotRanges: slotRanges, logger: context.logger);

            var slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
            Assert.IsTrue(slotsResult.Count == 1);
            Assert.AreEqual(startSlot, slotsResult[0].startSlot);
            Assert.AreEqual(endSlot, slotsResult[0].endSlot);
            Assert.IsTrue(slotsResult[0].nnInfo.Length == 1);
            Assert.IsTrue(slotsResult[0].nnInfo[0].isPrimary);
            Assert.AreEqual(slotsResult[0].nnInfo[0].address, context.clusterTestUtils.GetEndPoint(0).Address.ToString());
            Assert.AreEqual(slotsResult[0].nnInfo[0].port, context.clusterTestUtils.GetEndPoint(0).Port);
            Assert.AreEqual(slotsResult[0].nnInfo[0].nodeid, context.clusterTestUtils.GetNodeIdFromNode(0, context.logger));
        }

        [Test, Order(2)]
        public void ClusterSlotRangesTest()
        {
            context.CreateInstances(defaultShards);
            context.CreateConnection();
            List<(int, int)>[] slotRanges = new List<(int, int)>[3];
            slotRanges[0] = new List<(int, int)>() { (5680, 6150), (12345, 14567) };
            slotRanges[1] = new List<(int, int)>() { (1021, 2371), (3376, 5678) };
            slotRanges[2] = new List<(int, int)>() { (782, 978), (7345, 11819) };
            context.clusterTestUtils.SimpleSetupCluster(customSlotRanges: slotRanges, logger: context.logger);

            var slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
            while (slotsResult.Count < 6)
                slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
            Assert.AreEqual(6, slotsResult.Count);

            List<(int, (int, int))>[] origSlotRanges = new List<(int, (int, int))>[3];
            for (int i = 0; i < slotRanges.Length; i++)
            {
                origSlotRanges[i] = new List<(int, (int, int))>();
                for (int j = 0; j < slotRanges[i].Count; j++)
                    origSlotRanges[i].Add((i, slotRanges[i][j]));
            }
            var ranges = origSlotRanges.SelectMany(x => x).OrderBy(x => x.Item2.Item1).ToList();
            Assert.IsTrue(slotsResult.Count == ranges.Count);
            for (int i = 0; i < slotsResult.Count; i++)
            {
                var origRange = ranges[i];
                var retRange = slotsResult[i];
                Assert.AreEqual(origRange.Item2.Item1, retRange.startSlot);
                Assert.AreEqual(origRange.Item2.Item2, retRange.endSlot);
                Assert.IsTrue(retRange.nnInfo.Length == 1);
                Assert.IsTrue(retRange.nnInfo[0].isPrimary);
                Assert.AreEqual(context.clusterTestUtils.GetEndPoint(origRange.Item1).Address.ToString(), retRange.nnInfo[0].address);
                Assert.AreEqual(context.clusterTestUtils.GetEndPoint(origRange.Item1).Port, retRange.nnInfo[0].port);
                Assert.AreEqual(context.clusterTestUtils.GetNodeIdFromNode(origRange.Item1, context.logger), retRange.nnInfo[0].nodeid);
            }
        }

        [Test, Order(3)]
        public void ClusterForgetTest()
        {
            int node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

            var nodeIds = context.clusterTestUtils.GetNodeIds(logger: context.logger);

            //forget node0
            for (int i = 1; i < node_count; i++)
            {
                //issue forget node i to node 0 for 30 seconds
                context.clusterTestUtils.ClusterForget(0, nodeIds[i], 30, context.logger);
                //issue forget node 0 to node i
                context.clusterTestUtils.ClusterForget(i, nodeIds[0], 30, context.logger);
            }

            //Retrieve config for nodes 1 to i-1
            List<ClusterConfiguration> configs = new();
            for (int i = 1; i < node_count; i++)
                configs.Add(context.clusterTestUtils.ClusterNodes(i, context.logger));

            //Check if indeed nodes 1 to i-1 have forgotten node 0
            foreach (var config in configs)
                foreach (var node in config.Nodes)
                    Assert.AreNotEqual(nodeIds[0], node.NodeId, "node 0 node forgotten");
        }

        [Test, Order(4)]
        public void ClusterResetTest()
        {
            int node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

            //Get slot ranges for node 0
            var config = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var slots = config.Nodes.First().Slots;
            List<(int, int)> slotRanges = new();
            foreach (var slot in slots)
                slotRanges.Add((slot.From, slot.To));

            var nodeIds = context.clusterTestUtils.GetNodeIds(logger: context.logger);
            //Issue forget of node 0 to nodes 1 to i-1
            for (int i = 1; i < node_count; i++)
                context.clusterTestUtils.ClusterForget(i, nodeIds[0], 10, context.logger);

            try
            {
                //Add data to server
                var resp = context.clusterTestUtils.GetServer(0).Execute("SET", "wxz", "1234");
                Assert.AreEqual("OK", (string)resp);

                resp = context.clusterTestUtils.GetServer(0).Execute("GET", "wxz");
                Assert.AreEqual("1234", (string)resp);
            }
            catch (Exception ex)
            {
                context.logger?.LogError(ex, "An error occurred at ClusterResetTest");
            }

            //Hard reset node state. clean db data and cluster config
            context.clusterTestUtils.ClusterReset(0, soft: false, 10, context.logger);
            config = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var node = config.Nodes.First();

            //Assert node 0 does not know anything about the cluster
            Assert.AreEqual(1, config.Nodes.Count);
            Assert.AreNotEqual(nodeIds[0], node.NodeId);
            Assert.AreEqual(0, node.Slots.Count);
            Assert.IsFalse(node.IsReplica);

            //Add slotRange for clean node
            context.clusterTestUtils.AddSlotsRange(0, slotRanges, context.logger);
            try
            {
                //Check DB was flushed due to hard reset
                var resp = context.clusterTestUtils.GetServer(0).Execute("GET", "wxz");
                Assert.IsTrue(resp.IsNull, "DB not flushed after HARD reset");

                //Add data to server
                resp = context.clusterTestUtils.GetServer(0).Execute("SET", "wxz", "1234");
                Assert.AreEqual("OK", (string)resp);

                resp = context.clusterTestUtils.GetServer(0).Execute("GET", "wxz");
                Assert.AreEqual("1234", (string)resp);
            }
            catch (Exception ex)
            {
                context.logger?.LogError(ex, "An error occurred at ClusterResetTest");
            }

            //Add node back to the cluster
            context.clusterTestUtils.SetConfigEpoch(0, 1, context.logger);
            context.clusterTestUtils.Meet(0, 1, context.logger);

            context.clusterTestUtils.WaitUntilNodeIsKnownByAllNodes(0, context.logger);
        }

        // Disabling test until it stabilizes on CI
        // [Test, Order(5)]
        // [Category("CLUSTER")]
        public void ClusterRestartNodeDropGossip()
        {
            var logger = context.loggerFactory.CreateLogger("ClusterRestartNodeDropGossip");
            context.CreateInstances(defaultShards);
            context.CreateConnection();
            var (_, slots) = context.clusterTestUtils.SimpleSetupCluster(logger: logger);

            int restartingNode = 2;
            // Dispose node and delete data
            context.nodes[restartingNode].Dispose(deleteDir: true);

            context.nodes[restartingNode] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(restartingNode).Port,
                disableObjects: true,
                tryRecover: false,
                enableAOF: true,
                timeout: 60,
                gossipDelay: 1,
                cleanClusterConfig: false);
            context.nodes[restartingNode].Start();
            context.CreateConnection();

            Thread.Sleep(5000);
            for (int i = 0; i < 2; i++)
            {
                var config = context.clusterTestUtils.ClusterNodes(restartingNode, logger: logger);
                var knownNodes = config.Nodes.ToArray();
                Assert.AreEqual(knownNodes.Length, 1);
                Thread.Sleep(1000);
            }
        }
    }
}