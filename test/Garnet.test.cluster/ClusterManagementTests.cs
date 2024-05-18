﻿// Copyright (c) Microsoft Corporation.
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

        readonly HashSet<string> monitorTests = [];

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
            var slotRanges = new List<(int, int)>[1];
            slotRanges[0] = [(startSlot, endSlot)];
            context.CreateInstances(defaultShards);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(customSlotRanges: slotRanges, logger: context.logger);

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
            var slotRanges = new List<(int, int)>[3];
            slotRanges[0] = [(5680, 6150), (12345, 14567)];
            slotRanges[1] = [(1021, 2371), (3376, 5678)];
            slotRanges[2] = [(782, 978), (7345, 11819)];
            _ = context.clusterTestUtils.SimpleSetupCluster(customSlotRanges: slotRanges, logger: context.logger);

            var slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
            while (slotsResult.Count < 6)
                slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
            Assert.AreEqual(6, slotsResult.Count);

            List<(int, (int, int))>[] origSlotRanges = new List<(int, (int, int))>[3];
            for (var i = 0; i < slotRanges.Length; i++)
            {
                origSlotRanges[i] = new List<(int, (int, int))>();
                for (var j = 0; j < slotRanges[i].Count; j++)
                    origSlotRanges[i].Add((i, slotRanges[i][j]));
            }
            var ranges = origSlotRanges.SelectMany(x => x).OrderBy(x => x.Item2.Item1).ToList();
            Assert.IsTrue(slotsResult.Count == ranges.Count);
            for (var i = 0; i < slotsResult.Count; i++)
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
            var node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

            var nodeIds = context.clusterTestUtils.GetNodeIds(logger: context.logger);

            // Forget node0
            for (var i = 1; i < node_count; i++)
            {
                // Issue forget node i to node 0 for 30 seconds
                _ = context.clusterTestUtils.ClusterForget(0, nodeIds[i], 30, context.logger);
                // Issue forget node 0 to node i
                _ = context.clusterTestUtils.ClusterForget(i, nodeIds[0], 30, context.logger);
            }

            // Retrieve config for nodes 1 to i-1
            List<ClusterConfiguration> configs = new();
            for (var i = 1; i < node_count; i++)
                configs.Add(context.clusterTestUtils.ClusterNodes(i, context.logger));

            // Check if indeed nodes 1 to i-1 have forgotten node 0
            foreach (var config in configs)
                foreach (var node in config.Nodes)
                    Assert.AreNotEqual(nodeIds[0], node.NodeId, "node 0 node forgotten");
        }

        [Test, Order(4)]
        public void ClusterResetTest()
        {
            var node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

            // Get slot ranges for node 0
            var config = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var slots = config.Nodes.First().Slots;
            List<(int, int)> slotRanges = new();
            foreach (var slot in slots)
                slotRanges.Add((slot.From, slot.To));

            var nodeIds = context.clusterTestUtils.GetNodeIds(logger: context.logger);
            // Issue forget of node 0 to nodes 1 to i-1
            for (var i = 1; i < node_count; i++)
                _ = context.clusterTestUtils.ClusterForget(i, nodeIds[0], 10, context.logger);

            try
            {
                // Add data to server
                var resp = context.clusterTestUtils.GetServer(0).Execute("SET", "wxz", "1234");
                Assert.AreEqual("OK", (string)resp);

                resp = context.clusterTestUtils.GetServer(0).Execute("GET", "wxz");
                Assert.AreEqual("1234", (string)resp);
            }
            catch (Exception ex)
            {
                context.logger?.LogError(ex, "An error occured at ClusterResetTest");
            }

            // Hard reset node state. clean db data and cluster config
            _ = context.clusterTestUtils.ClusterReset(0, soft: false, 10, context.logger);
            config = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var node = config.Nodes.First();

            // Assert node 0 does not know anything about the cluster
            Assert.AreEqual(1, config.Nodes.Count);
            Assert.AreNotEqual(nodeIds[0], node.NodeId);
            Assert.AreEqual(0, node.Slots.Count);
            Assert.IsFalse(node.IsReplica);

            //Add slotRange for clean node
            context.clusterTestUtils.AddSlotsRange(0, slotRanges, context.logger);
            try
            {
                // Check DB was flushed due to hard reset
                var resp = context.clusterTestUtils.GetServer(0).Execute("GET", "wxz");
                Assert.IsTrue(resp.IsNull, "DB not flushed after HARD reset");

                // Add data to server
                resp = context.clusterTestUtils.GetServer(0).Execute("SET", "wxz", "1234");
                Assert.AreEqual("OK", (string)resp);

                resp = context.clusterTestUtils.GetServer(0).Execute("GET", "wxz");
                Assert.AreEqual("1234", (string)resp);
            }
            catch (Exception ex)
            {
                context.logger?.LogError(ex, "An error occured at ClusterResetTest");
            }

            // Add node back to the cluster
            context.clusterTestUtils.SetConfigEpoch(0, 1, context.logger);
            context.clusterTestUtils.Meet(0, 1, context.logger);

            context.clusterTestUtils.WaitUntilNodeIsKnownByAllNodes(0, context.logger);
        }

        [Test, Order(5)]
        public void ClusterKeySlotTest()
        {
            var node_count = 1;
            context.CreateInstances(node_count);
            context.CreateConnection();

            (string, int)[] testCases = [("6e6bzswz8}", 7038),
                ("8}jb94e7tf", 4828),
                ("{}2xc5pbb7", 11672),
                ("vr{a07}pdt", 12154),
                ("cx{ldv}wdl", 14261),
                ("erv805by}u", 15389),
                ("{ey1pqbij}", 8341),
                ("2tbjjyn}n8", 5152),
                ("t}jehlyo06", 1232),
                ("{u08t}xjal", 2490),
                ("5g{mkb95a}", 3345),
                ("x{v}x70nka", 7761),
                ("g67ikt}q8q", 7694),
                ("ovi8}mn7t7", 14473),
                ("p5ljmg{}8s", 11196),
                ("3wov{fd}8m", 3502),
                ("bxmcjzi3{}", 10246),
                ("{b1rrm7rn}", 14105),
                ("e0{4ylm}78", 5069),
                ("rkptge5}sx", 3468),
                ("o6{uyxsy}j", 3278),
                ("ykd6q{ma8}", 5754),
                ("w{j5pz3iy}", 6520),
                ("mhsr{dm}x0", 15077),
                ("0}dtokfryr", 5134),
                ("h7}0cj9mwm", 8187),
                ("w{jhqd}frk", 5369),
                ("5yzd{6}hzw", 5781),
                ("w6b4vgtzr}", 6045),
                ("4{b17h85}l", 5923),
                ("Hm{W\x13\x1c", 7517),
                ("zyy8yt1chw", 3081),
                ("7858tqv03y", 773),
                ("fdhhuk8yqv", 5763),
                ("8bfgeino4s", 6257)];

            foreach (var testCase in testCases)
            {
                var key = testCase.Item1;
                var expectedSlot = testCase.Item2;
                var slot = context.clusterTestUtils.ClusterKeySlot(0, key: key, logger: context.logger);
                Assert.AreEqual(expectedSlot, slot, $"{key}");
            }
        }

        //[Test, Order(5)]
        //[Category("CLUSTER")]
        public void ClusterRestartNodeDropGossip()
        {
            var logger = context.loggerFactory.CreateLogger("ClusterRestartNodeDropGossip");
            context.CreateInstances(defaultShards);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(logger: logger);

            var restartingNode = 2;
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
            for (var i = 0; i < 2; i++)
            {
                var config = context.clusterTestUtils.ClusterNodes(restartingNode, logger: logger);
                var knownNodes = config.Nodes.ToArray();
                Assert.AreEqual(knownNodes.Length, 1);
                Thread.Sleep(1000);
            }
        }
    }
}