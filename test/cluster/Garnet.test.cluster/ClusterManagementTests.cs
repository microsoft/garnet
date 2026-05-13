// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    public enum InstanceType
    {
        Standalone,
        Cluster
    }
    [TestFixture, NonParallelizable]
    public class ClusterManagementTests : TestBase
    {
        ClusterTestContext context;
        readonly int defaultShards = 3;

        readonly Dictionary<string, LogLevel> monitorTests = [];

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
        [TestCase(0, 16383, ClusterPreferredEndpointType.Ip, true)]
        [TestCase(0, 16383, ClusterPreferredEndpointType.Ip, false)]
        [TestCase(1234, 5678, ClusterPreferredEndpointType.Ip, false)]
        [TestCase(1234, 5678, ClusterPreferredEndpointType.Ip, true)]
        [TestCase(0, 16383, ClusterPreferredEndpointType.Hostname, true)]
        [TestCase(0, 16383, ClusterPreferredEndpointType.Hostname, false)]
        [TestCase(1234, 5678, ClusterPreferredEndpointType.Hostname, true)]
        [TestCase(1234, 5678, ClusterPreferredEndpointType.Hostname, false)]
        [TestCase(0, 16383, ClusterPreferredEndpointType.Unknown, true)]
        [TestCase(0, 16383, ClusterPreferredEndpointType.Unknown, false)]
        [TestCase(1234, 5678, ClusterPreferredEndpointType.Unknown, true)]
        [TestCase(1234, 5678, ClusterPreferredEndpointType.Unknown, false)]
        public void ClusterSlotsTest(int startSlot, int endSlot, ClusterPreferredEndpointType preferredType, bool useClusterAnnounceHostname)
        {
            var slotRanges = new List<(int, int)>[1];
            slotRanges[0] = [(startSlot, endSlot)];
            context.CreateInstances(defaultShards, clusterPreferredEndpointType: preferredType, useClusterAnnounceHostname: useClusterAnnounceHostname);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(customSlotRanges: slotRanges, logger: context.logger);

            var slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
            ClassicAssert.IsTrue(slotsResult.Count == 1);
            ClassicAssert.AreEqual(startSlot, slotsResult[0].startSlot);
            ClassicAssert.AreEqual(endSlot, slotsResult[0].endSlot);
            ClassicAssert.IsTrue(slotsResult[0].nnInfo.Length == 1);
            ClassicAssert.IsTrue(slotsResult[0].nnInfo[0].isPrimary);
            ClassicAssert.AreEqual(slotsResult[0].nnInfo[0].port, context.clusterTestUtils.GetEndPoint(0).Port);
            ClassicAssert.AreEqual(slotsResult[0].nnInfo[0].nodeid, context.clusterTestUtils.GetNodeIdFromNode(0, context.logger));
            // CheckMetadata(preferredType, useCl ,slotsResult[0].nnInfo[0]);
        }

        [Test, Order(2)]
        [TestCase(ClusterPreferredEndpointType.Ip, true)]
        [TestCase(ClusterPreferredEndpointType.Ip, false)]
        [TestCase(ClusterPreferredEndpointType.Hostname, true)]
        [TestCase(ClusterPreferredEndpointType.Hostname, false)]
        [TestCase(ClusterPreferredEndpointType.Unknown, true)]
        [TestCase(ClusterPreferredEndpointType.Unknown, false)]
        public void ClusterSlotRangesTest(
            ClusterPreferredEndpointType preferredType,
            bool useClusterAnnounceHostname
        )
        {
            context.CreateInstances(defaultShards, clusterPreferredEndpointType: preferredType, useClusterAnnounceHostname: useClusterAnnounceHostname);
            context.CreateConnection();
            var slotRanges = new List<(int, int)>[3];
            slotRanges[0] = [(5680, 6150), (12345, 14567)];
            slotRanges[1] = [(1021, 2371), (3376, 5678)];
            slotRanges[2] = [(782, 978), (7345, 11819)];
            _ = context.clusterTestUtils.SimpleSetupCluster(customSlotRanges: slotRanges, logger: context.logger);

            var slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
            while (slotsResult.Count < 6)
                slotsResult = context.clusterTestUtils.ClusterSlots(0, context.logger);
            ClassicAssert.AreEqual(6, slotsResult.Count);

            List<(int, (int, int))>[] origSlotRanges = new List<(int, (int, int))>[3];
            for (var i = 0; i < slotRanges.Length; i++)
            {
                origSlotRanges[i] = new List<(int, (int, int))>();
                for (var j = 0; j < slotRanges[i].Count; j++)
                    origSlotRanges[i].Add((i, slotRanges[i][j]));
            }
            var ranges = origSlotRanges.SelectMany(x => x).OrderBy(x => x.Item2.Item1).ToList();
            ClassicAssert.IsTrue(slotsResult.Count == ranges.Count);
            for (var i = 0; i < slotsResult.Count; i++)
            {
                var origRange = ranges[i];
                var retRange = slotsResult[i];
                ClassicAssert.AreEqual(origRange.Item2.Item1, retRange.startSlot);
                ClassicAssert.AreEqual(origRange.Item2.Item2, retRange.endSlot);
                ClassicAssert.IsTrue(retRange.nnInfo.Length == 1);
                ClassicAssert.IsTrue(retRange.nnInfo[0].isPrimary);
                ClassicAssert.AreEqual(context.clusterTestUtils.GetEndPoint(origRange.Item1).Port, retRange.nnInfo[0].port);
                ClassicAssert.AreEqual(context.clusterTestUtils.GetNodeIdFromNode(origRange.Item1, context.logger), retRange.nnInfo[0].nodeid);

                CheckMetadata(preferredType, useClusterAnnounceHostname, retRange.nnInfo[0]);
            }
        }

        private void CheckMetadata(ClusterPreferredEndpointType preferredType, bool useClusterAnnounceHostname, NodeNetInfo nodeNetInfo)
        {
            if (preferredType == ClusterPreferredEndpointType.Ip)
            {
                if (useClusterAnnounceHostname)
                {
                    ClassicAssert.AreEqual(context.nodeOptions[0].ClusterAnnounceHostname, nodeNetInfo.hostname);
                }

                ClassicAssert.Null(nodeNetInfo.ip);
            }

            if (preferredType == ClusterPreferredEndpointType.Hostname)
            {
                ClassicAssert.AreEqual(context.clusterTestUtils.GetEndPoint(0).Address.ToString(), nodeNetInfo.ip);
            }

            if (preferredType == ClusterPreferredEndpointType.Unknown)
            {
                ClassicAssert.AreEqual(context.clusterTestUtils.GetEndPoint(0).Address.ToString(), nodeNetInfo.ip);
            }
        }

        [Test, Order(3)]
        [TestCase(ClusterPreferredEndpointType.Ip, false)]
        [TestCase(ClusterPreferredEndpointType.Ip, true)]
        [TestCase(ClusterPreferredEndpointType.Hostname, true)]
        [TestCase(ClusterPreferredEndpointType.Unknown, false)]
        [TestCase(ClusterPreferredEndpointType.Unknown, true)]
        public void ClusterShardsTest(
            ClusterPreferredEndpointType preferredType,
            bool useClusterAnnounceHostname)
        {
            context.CreateInstances(defaultShards, clusterPreferredEndpointType: preferredType, useClusterAnnounceHostname: useClusterAnnounceHostname);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.IsNotNull(shards);
            ClassicAssert.AreEqual(defaultShards, shards.Count);

            var expectedAddress = context.clusterTestUtils.GetEndPoint(0).Address.ToString();
            var expectedHostname = useClusterAnnounceHostname ? context.nodeOptions[0].ClusterAnnounceHostname : null;

            foreach (var shard in shards)
            {
                foreach (var node in shard.nodes)
                {
                    // ip is always the address
                    ClassicAssert.AreEqual(expectedAddress, node.ip);

                    // endpoint depends on preferred type
                    switch (preferredType)
                    {
                        case ClusterPreferredEndpointType.Hostname:
                            ClassicAssert.AreEqual(useClusterAnnounceHostname ? expectedHostname : node.hostname, node.endpoint);
                            break;
                        case ClusterPreferredEndpointType.Unknown:
                            ClassicAssert.AreEqual("?", node.endpoint);
                            break;
                        default:
                            ClassicAssert.AreEqual(expectedAddress, node.endpoint);
                            break;
                    }

                    if (useClusterAnnounceHostname)
                        ClassicAssert.AreEqual(expectedHostname, node.hostname);
                    else
                        ClassicAssert.IsNotNull(node.hostname);
                }
            }
        }

        [Test, Order(4)]
        [TestCase(false)]
        [TestCase(true)]
        public void ClusterNodesHostnameTest(bool useClusterAnnounceHostname)
        {
            context.CreateInstances(defaultShards, useClusterAnnounceHostname: useClusterAnnounceHostname);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(logger: context.logger);

            var result = (string)context.clusterTestUtils.Execute(
                context.clusterTestUtils.GetEndPoint(0), "cluster", ["nodes"]);

            var lines = result.Split('\n', StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in lines)
            {
                var parts = line.Split(' ');
                var addressPart = parts[1];

                // Address format: ip:port@cport[,hostname]
                var atIdx = addressPart.IndexOf('@');
                var afterAt = addressPart[(atIdx + 1)..];

                if (useClusterAnnounceHostname)
                {
                    // Should contain comma + configured hostname
                    var commaIdx = afterAt.IndexOf(',');
                    ClassicAssert.IsTrue(commaIdx > 0, $"Expected hostname in address part: {addressPart}");
                    var hostname = afterAt[(commaIdx + 1)..];
                    ClassicAssert.AreEqual(context.nodeOptions[0].ClusterAnnounceHostname, hostname);
                }
                else
                {
                    // Garnet defaults to system hostname; verify format is valid
                    var commaIdx = afterAt.IndexOf(',');
                    if (commaIdx > 0)
                    {
                        // System hostname is present: comma + hostname
                        var hostname = afterAt[(commaIdx + 1)..];
                        ClassicAssert.IsNotEmpty(hostname);
                    }
                    else
                    {
                        // No hostname: address is exactly ip:port@cport with no trailing comma
                        ClassicAssert.IsTrue(afterAt.All(char.IsDigit),
                            $"Expected numeric cport with no hostname in address part, but got: {addressPart}");
                    }
                }
            }
        }

        [Test, Order(5)]
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
                    ClassicAssert.AreNotEqual(nodeIds[0], node.NodeId, "node 0 node forgotten");
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

            // Hard reset node state. clean db data and cluster config
            _ = context.clusterTestUtils.ClusterReset(0, soft: false, 10, context.logger);
            config = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var node = config.Nodes.First();

            // Assert node 0 does not know anything about the cluster
            ClassicAssert.AreEqual(1, config.Nodes.Count);
            ClassicAssert.AreNotEqual(nodeIds[0], node.NodeId);
            ClassicAssert.AreEqual(0, node.Slots.Count);
            ClassicAssert.IsFalse(node.IsReplica);

            //Add slotRange for clean node
            context.clusterTestUtils.AddSlotsRange(0, slotRanges, context.logger);

            // Add node back to the cluster
            context.clusterTestUtils.SetConfigEpoch(0, 1, context.logger);
            context.clusterTestUtils.Meet(0, 1, context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnownByAllNodes(0, context.logger);
            for (int i = 0; i < node_count; i++)
            {
                Console.WriteLine(i);
                context.clusterTestUtils.WaitUntilNodeIsKnownByAllNodes(i, context.logger);
            }
        }

        [Test, Order(4)]
        public void ClusterResetFailsForMasterWithKeysInSlotsTest()
        {
            var node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

            var expectedSlotRange = new SlotRange(0, 4095);
            var config = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var node = config.Nodes.First();
            ClassicAssert.AreEqual(expectedSlotRange, node.Slots[0]);
            byte[] key = new byte[16];
            context.clusterTestUtils.RandomBytesRestrictedToSlot(ref key, node.Slots.First().From);

            context.clusterTestUtils.GetServer(0).Execute("SET", key, "1234");
            string res = context.clusterTestUtils.GetServer(0).Execute("GET", key).ToString();
            ClassicAssert.AreEqual("1234", res);

            VerifyClusterResetFails(true);
            VerifyClusterResetFails(false);

            // soft reset node state. clean db data and cluster config
            var nodeIds = context.clusterTestUtils.GetNodeIds(logger: context.logger);
            ClassicAssert.AreEqual(4, config.Nodes.Count);
            ClassicAssert.AreEqual(nodeIds[0], node.NodeId);
            ClassicAssert.AreEqual(expectedSlotRange, node.Slots[0]);
            ClassicAssert.IsFalse(node.IsReplica);
        }

        [Test, Order(4)]
        public void ClusterResetFailsForMasterWithKeysInSlotsObjectStoreTest()
        {
            var node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);
            context.kvPairsObj = new Dictionary<string, List<int>>();
            context.PopulatePrimaryWithObjects(ref context.kvPairsObj, 16, 10, 0);

            var expectedSlotRange = new SlotRange(0, 4095);
            var config = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var node = config.Nodes.First();
            ClassicAssert.AreEqual(expectedSlotRange, node.Slots[0]);
            byte[] key = new byte[16];
            context.clusterTestUtils.RandomBytesRestrictedToSlot(ref key, node.Slots.First().From);

            VerifyClusterResetFails(true);
            VerifyClusterResetFails(false);

            var nodeIds = context.clusterTestUtils.GetNodeIds(logger: context.logger);
            ClassicAssert.AreEqual(4, config.Nodes.Count);
            ClassicAssert.AreEqual(nodeIds[0], node.NodeId);
            ClassicAssert.AreEqual(expectedSlotRange, node.Slots[0]);
            ClassicAssert.IsFalse(node.IsReplica);
        }

        [Test, Order(4)]
        public void ClusterResetAfterFLushAllTest()
        {
            var node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);
            context.kvPairsObj = new Dictionary<string, List<int>>();
            context.PopulatePrimaryWithObjects(ref context.kvPairsObj, 16, 10, 0);

            var expectedSlotRange = new SlotRange(0, 4095);
            var config = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var node = config.Nodes.First();
            ClassicAssert.AreEqual(expectedSlotRange, node.Slots[0]);
            byte[] key = new byte[16];
            context.clusterTestUtils.RandomBytesRestrictedToSlot(ref key, node.Slots.First().From);

            VerifyClusterResetFails(true);
            VerifyClusterResetFails(false);

            var nodeIds = context.clusterTestUtils.GetNodeIds(logger: context.logger);
            ClassicAssert.AreEqual(4, config.Nodes.Count);
            ClassicAssert.AreEqual(nodeIds[0], node.NodeId);
            ClassicAssert.AreEqual(expectedSlotRange, node.Slots[0]);
            ClassicAssert.IsFalse(node.IsReplica);

            context.clusterTestUtils.FlushAll(0, context.logger);
            _ = context.clusterTestUtils.ClusterReset(0, soft: false, 10, context.logger);

            config = context.clusterTestUtils.ClusterNodes(0, context.logger);
            node = config.Nodes.First();
            // Assert node 0 does not know anything about the cluster
            ClassicAssert.AreEqual(1, config.Nodes.Count);
            ClassicAssert.AreNotEqual(nodeIds[0], node.NodeId);
            ClassicAssert.AreEqual(0, node.Slots.Count);
            ClassicAssert.IsFalse(node.IsReplica);
        }

        private void VerifyClusterResetFails(bool softReset = true)
        {
            var server = context.clusterTestUtils.GetServer(0);
            var args = new List<object>() {
                "reset",
                softReset ? "soft" : "hard",
                "60"
            };

            try
            {
                _ = (string)server.Execute("cluster", args);
            }
            catch (RedisServerException ex)
            {
                ClassicAssert.AreEqual("ERR CLUSTER RESET can't be called with master nodes containing keys", ex.Message);
            }
        }

        [Test, Order(4)]
        public async Task ClusterResetDisposesGossipConnections()
        {
            var node_count = 3;
            context.CreateInstances(node_count, metricsSamplingFrequency: 1);
            context.CreateConnection();
            var endpoints = context.clusterTestUtils.GetEndpoints();
            for (int i = 0; i < endpoints.Length - 1; i++)
            {
                context.clusterTestUtils.Meet(i, i + 1, context.logger);
            }

            for (int i = 0; i < node_count; i++)
            {
                context.clusterTestUtils.WaitUntilNodeIsKnownByAllNodes(i);
            }

            await Task.Delay(1000).ConfigureAwait(false);

            var server = context.clusterTestUtils.GetServer(0);
            var gossipConnections = GetStat(server, "Stats", "gossip_open_connections");
            ClassicAssert.AreEqual(node_count - 1, int.Parse(gossipConnections), "Expected one gossip connection per node.");

            context.clusterTestUtils.ClusterReset(0, soft: true);

            await Task.Delay(1000).ConfigureAwait(false);

            gossipConnections = GetStat(server, "Stats", "gossip_open_connections");
            ClassicAssert.AreEqual("0", gossipConnections, "All gossip connections should be closed after a reset.");
            ClassicAssert.AreEqual(1, context.clusterTestUtils.ClusterNodes(0).Nodes.Count(), "Expected the node to only know about itself after a reset.");
        }

        private string GetStat(IServer server, string section, string statName)
        {
            return server.Info(section).FirstOrDefault(x => x.Key == section)?.FirstOrDefault(x => x.Key == statName).Value;
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
                ClassicAssert.AreEqual(expectedSlot, slot, $"{key}");
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
                context.clusterTestUtils.GetEndPoint(restartingNode),
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
                ClassicAssert.AreEqual(knownNodes.Length, 1);
                Thread.Sleep(1000);
            }
        }

        [Test, Order(7)]
        public void ClusterClientList()
        {
            const int NodeCount = 4;
            context.CreateInstances(NodeCount, enableAOF: true, FastAofTruncate: true, CommitFrequencyMs: -1);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(NodeCount / 2, 1, logger: context.logger);

            // Wait for all nodes to be fully connected
            for (var nodeIx = 0; nodeIx < NodeCount; nodeIx++)
            {
                for (var otherNodeIx = 0; otherNodeIx < NodeCount; otherNodeIx++)
                {
                    if (nodeIx == otherNodeIx)
                    {
                        continue;
                    }

                    context.clusterTestUtils.WaitUntilNodeIsKnown(nodeIx, otherNodeIx);
                }
            }

            // Wait for at least one Gossip messages to be exchanged between all nodes
            //
            // This is necessary because that Gossip message is how a connection is classified
            // as from a replica/master.
            var waitingForGossip = true;
            while (waitingForGossip)
            {
                waitingForGossip = false;

                for (var nodeIx = 0; nodeIx < NodeCount; nodeIx++)
                {
                    var node = context.nodes[nodeIx];

                    var clusterNodes = (string)context.clusterTestUtils.GetServer(nodeIx).Execute("CLUSTER", "NODES");
                    var connectionState = new List<(string NodeId, long LastPingSent, long LastPongRecv)>();
                    foreach (var entry in clusterNodes.Split("\n", StringSplitOptions.RemoveEmptyEntries))
                    {
                        var parts = Regex.Match(entry, @"^(?<id>[^\s]+) \s+ (?<ip>[^\s]+) \s+ (?<flags>[^\s]+) \s+ (?<primary>[^\s]+) \s+ (?<pingSent>[^\s]+) \s+ (?<pongRecv>[^\s]+) \s+ (?<configEpoch>[^\s]+) \s+ (?<linkStart>[^\s]+)", RegexOptions.IgnorePatternWhitespace);
                        var id = parts.Groups["id"].Value;
                        var pingSent = long.Parse(parts.Groups["pingSent"].Value);
                        var pongRecv = long.Parse(parts.Groups["pongRecv"].Value);

                        connectionState.Add((id, pingSent, pongRecv));
                    }

                    for (var otherNodeIx = 0; otherNodeIx < NodeCount; otherNodeIx++)
                    {
                        if (nodeIx == otherNodeIx)
                        {
                            continue;
                        }

                        var otherNodeId = context.clusterTestUtils.GetNodeIdFromNode(otherNodeIx, context.logger);

                        var matching = connectionState.Single(x => x.NodeId == otherNodeId);

                        if (matching.LastPingSent == 0 || matching.LastPongRecv == 0)
                        {
                            waitingForGossip = true;
                            goto waitAndTryAgain;
                        }
                    }
                }

            waitAndTryAgain:
                ClusterTestUtils.BackOff(context.cts.Token);
            }

            // At this point, the cluster should look like
            // Node 1 (master)
            //  * connected to node 2 (master <-> master)
            //  * connected to node 3 (master <-> replica)
            //  * connected to node 4 (master <-> replica)
            // Node 2 (master)
            //  * connected to node 1 (master <-> master)
            //  * connected to node 3 (master <-> replica)
            //  * connected to node 4 (master <-> replica)
            // Node 3 (replica of Node 1)
            //  * connected to node 1 (replica <-> master)
            //  * connected to node 2 (replica <-> master)
            //  * connected to node 4 (replica <-> replica)
            // Node 4 (replica of Node 2)
            //  * connected to node 1 (replica <-> master)
            //  * connected to node 2 (replica <-> master)
            //  * connected to node 3 (replica <-> replica)
            //
            // Then we establish a connection to every node, so every node has 1 normal connection
            //
            // So each master has 1 normal connection, 1 connection from a master, and 2 connections from replicas
            // and each replica has 1 normal connection, 2 connections from masters, and 1 connection from a replica

            // Check that all nodes have 4 connections
            var numWithTwoMasterConnections = 0;
            var numWithTwoReplicaConnections = 0;

            // Every node should have 1 normal connection and either 2 master + 1 replica, or 2 replica + 1 master
            for (var nodeIx = 0; nodeIx < NodeCount; nodeIx++)
            {
                var fullList = (string)context.clusterTestUtils.Execute((IPEndPoint)context.endpoints[nodeIx], "CLIENT", ["LIST"]);
                var numNormal = fullList.Split("\n").Count(static x => x.Contains(" flags=N "));
                var numReplica = fullList.Split("\n").Count(static x => x.Contains(" flags=S "));
                var numMaster = fullList.Split("\n").Count(static x => x.Contains(" flags=M "));

                ClassicAssert.IsTrue(2 >= numNormal, $"normalCheck: nodeIx={nodeIx}, normal={numNormal}, replica={numReplica}, master={numMaster}");
                ClassicAssert.IsTrue(numReplica >= 1 && numReplica <= 2, $"replicaCheck: nodeIx={nodeIx}, normal={numNormal}, replica={numReplica}, master={numMaster}");
                ClassicAssert.IsTrue(numMaster >= 1 && numMaster <= 2, $"masterCheck: nodeIx={nodeIx}, normal={numNormal}, replica={numReplica}, master={numMaster}");

                if (numMaster == 1)
                {
                    ClassicAssert.AreEqual(2, numReplica, $"unexpected replica count for master: nodeIx={nodeIx}, normal={numNormal}, replica={numReplica}, master={numMaster}");
                    numWithTwoReplicaConnections++;
                }
                else
                {
                    ClassicAssert.AreEqual(1, numReplica, $"unexpected replica count for replica: nodeIx={nodeIx}, normal={numNormal}, replica={numReplica}, master={numMaster}");
                    numWithTwoMasterConnections++;
                }

                var replicaList = (string)context.clusterTestUtils.Execute((IPEndPoint)context.endpoints[nodeIx], "CLIENT", ["LIST", "TYPE", "REPLICA"]);
                var masterList = (string)context.clusterTestUtils.Execute((IPEndPoint)context.endpoints[nodeIx], "CLIENT", ["LIST", "TYPE", "MASTER"]);

                ClassicAssert.AreEqual(numReplica, replicaList.Split("\n", StringSplitOptions.RemoveEmptyEntries).Length, $"nodeIx={nodeIx}, normal={numNormal}, replica={numReplica}, master={numMaster}");
                ClassicAssert.AreEqual(numMaster, masterList.Split("\n", StringSplitOptions.RemoveEmptyEntries).Length, $"nodeIx={nodeIx}, normal={numNormal}, replica={numReplica}, master={numMaster}");
            }

            ClassicAssert.AreEqual(2, numWithTwoMasterConnections);
            ClassicAssert.AreEqual(2, numWithTwoReplicaConnections);
        }

        [Test, Order(7)]
        public void ClusterClientKill()
        {
            const int NodeCount = 4;
            context.CreateInstances(NodeCount, enableAOF: true, FastAofTruncate: true, CommitFrequencyMs: -1);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(NodeCount / 2, 1, logger: context.logger);

            var killedMaster = (int)context.clusterTestUtils.Execute((IPEndPoint)context.endpoints[0], "CLIENT", ["KILL", "TYPE", "MASTER"]);
            var killedReplica = (int)context.clusterTestUtils.Execute((IPEndPoint)context.endpoints[0], "CLIENT", ["KILL", "TYPE", "REPLICA"]);

            ClassicAssert.IsTrue(killedMaster >= 1);
            ClassicAssert.IsTrue(killedReplica >= 1);
        }

        [Test, Order(7)]
        public void ClusterClientKillSlave()
        {
            // Test SLAVE separately - it's equivalent to REPLICA, but needed for compatibility

            const int NodeCount = 4;
            context.CreateInstances(NodeCount, enableAOF: true, FastAofTruncate: true, CommitFrequencyMs: -1);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(NodeCount / 2, 1, logger: context.logger);

            var killed = (int)context.clusterTestUtils.Execute((IPEndPoint)context.endpoints[0], "CLIENT", ["KILL", "TYPE", "SLAVE"]);

            ClassicAssert.IsTrue(killed >= 1);
        }

        [Test, Order(8)]
        public void FailoverBadOptions()
        {
            var node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

            var endpoint = (IPEndPoint)context.endpoints[0];

            // Default rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "FAILOVER", ["DEFAULT", "localhost", "6379"]);
                ClassicAssert.AreEqual("ERR syntax error", errorMsg);
            }

            // Invalid rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "FAILOVER", ["INVALID", "localhost", "6379"]);
                ClassicAssert.AreEqual("ERR syntax error", errorMsg);
            }

            // Numeric equivalent rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "FAILOVER", ["2", "localhost", "6379"]);
                ClassicAssert.AreEqual("ERR syntax error", errorMsg);
            }

            // Numeric out of range rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "FAILOVER", ["128", "localhost", "6379"]);
                ClassicAssert.AreEqual("ERR syntax error", errorMsg);
            }
        }

        [Test, Order(9)]
        public void ClusterFailoverBadOptions()
        {
            var node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

            var endpoint = (IPEndPoint)context.endpoints[0];

            // Default rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["FAILOVER", "DEFAULT"]);
                ClassicAssert.AreEqual("ERR Failover option (DEFAULT) not supported", errorMsg);
            }

            // Invalid rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["FAILOVER", "Invalid"]);
                ClassicAssert.AreEqual("ERR Failover option (Invalid) not supported", errorMsg);
            }

            // Numeric equivalent rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["FAILOVER", "2"]);
                ClassicAssert.AreEqual("ERR Failover option (2) not supported", errorMsg);
            }

            // Numeric out of range rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["FAILOVER", "128"]);
                ClassicAssert.AreEqual("ERR Failover option (128) not supported", errorMsg);
            }
        }

        [Test, Order(10)]
        public void ClusterSetSlotBadOptions()
        {
            var node_count = 4;
            context.CreateInstances(node_count);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(node_count, 0, logger: context.logger);

            var endpoint = (IPEndPoint)context.endpoints[0];

            // Non-numeric slot
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["SETSLOT", "abc", "STABLE"]);
                ClassicAssert.AreEqual("ERR Invalid or out of range slot", errorMsg);
            }

            // Invalid rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["SETSLOT", "123", "Invalid"]);
                ClassicAssert.AreEqual("ERR Slot state Invalid not supported.", errorMsg);
            }

            // Offline rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["SETSLOT", "123", "Offline"]);
                ClassicAssert.AreEqual("ERR Slot state Offline not supported.", errorMsg);
            }

            // Numeric rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["SETSLOT", "123", "1"]);
                ClassicAssert.AreEqual("ERR Slot state 1 not supported.", errorMsg);
            }

            // Numeric out of range rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["SETSLOT", "123", "128"]);
                ClassicAssert.AreEqual("ERR Slot state 128 not supported.", errorMsg);
            }

            // Stable with node id rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["SETSLOT", "123", "STABLE", "foo"]);
                ClassicAssert.AreEqual("ERR syntax error", errorMsg);
            }

            // Non-stable without node id rejected
            {
                var errorMsg = (string)context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["SETSLOT", "123", "IMPORTING"]);
                ClassicAssert.AreEqual("ERR syntax error", errorMsg);
            }
        }

        [Test, Order(11)]
        public void ClusterRoleCommand([Values] bool useMultiLog)
        {
            var primaryIndex = 0;
            var sublogCount = useMultiLog ? 4 : 1;
            var node_count = 3;
            var replica_count = node_count - 1;
            context.CreateInstances(node_count, enableAOF: true, sublogCount: sublogCount);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(1, replica_count, logger: context.logger);

            var primaryFormatLength = Enum.GetValues<RoleCommandPrimaryFormat>().Length - (useMultiLog ? 0 : 1);
            var result = context.clusterTestUtils.GetServer(primaryIndex).Execute("ROLE");
            ClassicAssert.AreEqual(primaryFormatLength, result.Length);
            // RoleType
            ClassicAssert.AreEqual("master", result[(int)RoleCommandPrimaryFormat.RoleType].ToString());
            // Replication offset
            ClassicAssert.True(int.TryParse(result[(int)RoleCommandPrimaryFormat.RoleReplicationOffset].ToString(), out var parsed));
            ClassicAssert.AreEqual(64, parsed);

            ClassicAssert.AreEqual(2, result[(int)RoleCommandPrimaryFormat.RoleReplicaInfo].Length);
            if (useMultiLog)
            {
                // ReplicationOffsetVector (MultiLog support)
                var aofAddress = AofAddress.FromString(result[(int)RoleCommandPrimaryFormat.RoleReplicationOffsetString].ToString());
                ClassicAssert.AreEqual(sublogCount, aofAddress.Length);
            }

            for (var i = 0; i < replica_count; i++)
            {
                var primaryResultForReplica = result[(int)RoleCommandPrimaryFormat.RoleReplicaInfo][i];
                var replicaIndex = i + 1;
                var roleReplicaFormatLength = Enum.GetValues<RoleCommandReplicaFormat>().Length - (useMultiLog ? 0 : 1);

                // NOTE: Role command from primary perspective does not include role type or connection status
                ClassicAssert.AreEqual(roleReplicaFormatLength - 2, primaryResultForReplica.Length);
                // Address
                ClassicAssert.AreEqual("127.0.0.1", primaryResultForReplica[(int)RoleCommandReplicaFormat.RoleAddress - 1].ToString(), "Failed to match replica address");
                // Port
                ClassicAssert.True(int.TryParse(primaryResultForReplica[(int)RoleCommandReplicaFormat.RolePort - 1].ToString(), out parsed), "Failed to match replica port");
                ClassicAssert.AreEqual(7000 + replicaIndex, parsed);
                // ReplicationOffset
                ClassicAssert.True(int.TryParse(primaryResultForReplica[(int)RoleCommandReplicaFormat.RoleReplicationOffset - 2].ToString(), out parsed), "Failed to match replica replication offset");
                ClassicAssert.AreEqual(64, parsed);
                if (useMultiLog)
                {
                    // ReplicationOffsetVector (MultiLog support)
                    var replicaAofAddress = AofAddress.FromString(primaryResultForReplica[(int)RoleCommandReplicaFormat.RoleReplicationOffsetString - 2].ToString());
                    ClassicAssert.AreEqual(sublogCount, replicaAofAddress.Length);
                }

                var replicaResult = context.clusterTestUtils.GetServer(i + 1).Execute("ROLE");
                ClassicAssert.AreEqual(roleReplicaFormatLength, replicaResult.Length);
                // RoleType
                ClassicAssert.AreEqual("slave", replicaResult[(int)RoleCommandReplicaFormat.RoleType].ToString());
                // Replica Address
                ClassicAssert.AreEqual("127.0.0.1", replicaResult[(int)RoleCommandReplicaFormat.RoleAddress].ToString());
                // Replica Port
                ClassicAssert.True(int.TryParse(replicaResult[(int)RoleCommandReplicaFormat.RolePort].ToString(), out parsed));
                ClassicAssert.AreEqual(7000, parsed);
                // Connection State
                ClassicAssert.AreEqual("connected", replicaResult[(int)RoleCommandReplicaFormat.RoleState].ToString());
                // ReplicationOffset
                ClassicAssert.True(int.TryParse(replicaResult[(int)RoleCommandReplicaFormat.RoleReplicationOffset].ToString(), out parsed));
                ClassicAssert.AreEqual(64, parsed);
                if (useMultiLog)
                {
                    // ReplicationOffsetVector (MultiLog support)
                    var replicaAofAddress = AofAddress.FromString(replicaResult[(int)RoleCommandReplicaFormat.RoleReplicationOffsetString].ToString());
                    ClassicAssert.AreEqual(sublogCount, replicaAofAddress.Length);
                }
            }
        }

        [Test, Order(12)]
        public void ClusterNodeCommand()
        {
            var primary_count = 1;
            var node_count = 3;
            var replica_count = node_count - primary_count;
            context.CreateInstances(node_count, enableAOF: true);
            context.CreateConnection();
            var (shardInfo, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);
            var endpoint = (IPEndPoint)context.endpoints[0];
            var result = context.clusterTestUtils.Execute(endpoint, "CLUSTER", ["NODES"]);
            ClassicAssert.IsNotNull(result);
            var lines = result.ToString().Split("\n", StringSplitOptions.RemoveEmptyEntries);
            ClassicAssert.AreEqual(node_count, lines.Length);

            var primaries = shardInfo[0].nodes.Where(x => x.role == NodeRole.PRIMARY).Select(w => w.nodeid).ToArray();
            foreach (var line in lines)
            {
                var fields = line.Split(' ');
                ClassicAssert.IsTrue(shardInfo[0].nodes.Any(e => e.nodeid == fields[0]));

                var node = shardInfo[0].nodes.Single(e => e.nodeid == fields[0]);
                if (node.role == NodeRole.PRIMARY)
                {
                    ClassicAssert.GreaterOrEqual(fields.Length, 8);
                    ClassicAssert.IsTrue(fields[1].StartsWith("127.0.0.1"));
                    ClassicAssert.IsTrue(fields[2].Contains("master"));
                    ClassicAssert.AreEqual("-", fields[3]); // primary node id
                    ClassicAssert.AreEqual("1", fields[6]); // default config-epoch
                    ClassicAssert.AreEqual("connected", fields[7]);
                }
                else
                {
                    ClassicAssert.GreaterOrEqual(fields.Length, 8);
                    ClassicAssert.IsTrue(fields[1].StartsWith("127.0.0.1"));
                    ClassicAssert.AreEqual("slave", fields[2]);
                    ClassicAssert.Contains(fields[3], primaries);
                    ClassicAssert.AreEqual("connected", fields[7]);
                }
            }
        }

        [Test, Order(13)]
        public void ClusterMeetHostname([Values] bool useLoopback)
        {
            var hostname = useLoopback ? "localhost" : TestUtils.GetHostName(context.logger);
            ClassicAssert.IsNotNull(hostname);
            ClassicAssert.IsNotEmpty(hostname);

            var node_count = 3;
            context.CreateInstances(node_count, enableAOF: true, useHostname: !useLoopback);
            context.CreateConnection();

            for (var i = 0; i < node_count; i++)
                context.clusterTestUtils.SetConfigEpoch(i, i + 1, context.logger);


            for (var i = 1; i < node_count; i++)
                context.clusterTestUtils.Meet(0, i, hostname, context.logger);

            for (var i = 0; i < node_count; i++)
                for (var j = 0; j < node_count; j++)
                    if (i != j)
                        context.clusterTestUtils.WaitUntilNodeIsKnown(i, j, context.logger);
        }

        [Test, Order(14)]
        public void ClusterFlushAll()
        {
            var node_count = 2;
            var primaryIndex = 0;
            var replicaIndex = 1;
            context.CreateInstances(node_count, enableAOF: true);
            context.CreateConnection();
            var (_, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count: 1, replica_count: 1, logger: context.logger);

            var key = "mykey";
            var value = "myvalue";

            // Set value
            var result = (string)context.clusterTestUtils.GetServer(primaryIndex).Execute("SET", key, value);
            ClassicAssert.AreEqual("OK", result);

            // Get value
            result = (string)context.clusterTestUtils.GetServer(primaryIndex).Execute("GET", key);
            ClassicAssert.AreEqual(value, result);

            // Get value from replica
            while (true)
            {
                result = (string)context.clusterTestUtils.GetServer(replicaIndex).Execute("GET", key);
                if (result == value)
                    break;
            }
            ClassicAssert.AreEqual(value, result);

            // Try to flushall on replica
            var exception = Assert.Throws<RedisServerException>(() => context.clusterTestUtils.GetServer(replicaIndex).FlushAllDatabases());
            ClassicAssert.AreEqual("ERR You can't write against a read only replica.", exception.Message);

            // Flushall on primary
            Assert.DoesNotThrow(() => context.clusterTestUtils.GetServer(primaryIndex).FlushAllDatabases());

            // Get value
            result = (string)context.clusterTestUtils.GetServer(primaryIndex).Execute("GET", key);
            ClassicAssert.IsNull(result);

            // Get value from replica
            while (true)
            {
                result = (string)context.clusterTestUtils.GetServer(replicaIndex).Execute("GET", key);
                if (result == null)
                    break;
            }
            ClassicAssert.IsNull(result);
        }

        [Test, Order(15)]
        public void ClusterCheckpointUpgradeFrom([Values] InstanceType instanceType)
        {
            // Startup in cluster or standalone mode
            var isCluster = instanceType == InstanceType.Cluster;
            context.CreateInstances(1, enableCluster: isCluster);
            context.CreateConnection(enabledCluster: isCluster);

            // If cluster mode assign slots
            if (isCluster)
                ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, context.logger));

            var keyLength = 32;
            var kvpairCount = 128;
            Dictionary<string, int> kvPairs = [];
            context.PopulatePrimary(ref kvPairs, keyLength, kvpairCount, 0);

            var primaryLastSaveTime = context.clusterTestUtils.LastSave(0, logger: context.logger);
            context.clusterTestUtils.Checkpoint(0, context.logger);
            context.clusterTestUtils.WaitCheckpoint(0, primaryLastSaveTime, logger: context.logger);

            context.nodes[0].Dispose(false);
            // Restart in standalone or cluster mode
            context.nodes[0] = context.CreateInstance(context.clusterTestUtils.GetEndPoint(0), enableCluster: !isCluster, tryRecover: true);
            context.nodes[0].Start();
            context.CreateConnection(enabledCluster: !isCluster);

            // Assign slot if started initially in standalone
            if (!isCluster)
                ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, context.logger));

            context.clusterTestUtils.PingAll(logger: context.logger);

            var db = context.clusterTestUtils.GetDatabase();
            foreach (var kv in kvPairs)
            {
                var key = kv.Key;
                var value = kv.Value;
                var dbValue = (int)db.StringGet(key);
                ClassicAssert.AreEqual(value, dbValue);
            }
        }

        [Test, Order(16)]
        [CancelAfter(30_000)]
        [TestCase(false)]
        [TestCase(true)]
        public async Task ClusterReplicationObjectCollectTest(bool useManualCollect, CancellationToken cancellationToken)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;

            context.CreateInstances(nodes_count, disableObjects: false, enableAOF: true, useTLS: false, asyncReplay: false, expiredObjectCollectionFrequencySecs: !useManualCollect ? 100 : 0);
            context.CreateConnection(useTLS: false);

            var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            var replicaServer = context.clusterTestUtils.GetServer(replicaNodeIndex);

            // Setup cluster
            context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaNodeIndex, replicaNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaNodeIndex, primaryNodeIndex, logger: context.logger);

            var db = context.clusterTestUtils.GetDatabase();
            HashEntry[] elements = [new HashEntry("field1", "hello"), new HashEntry("field2", "world"), new HashEntry("field3", "value3"), new HashEntry("field4", "value4"), new HashEntry("field5", "value5"), new HashEntry("field6", "value6")];
            // Attach replica
            var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryNodeIndex, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            // Execute first hash set workload
            var hashSetKey = "myhash";
            ExecuteHashSet(hashSetKey, elements);
            await Task.Delay(3000).ConfigureAwait(false);
            ManualCollect();
            ValidateHashSet(hashSetKey);

            // Execute second hash set workload
            hashSetKey = "myhash2";
            ExecuteHashSet(hashSetKey, elements);
            await Task.Delay(3000).ConfigureAwait(false);
            ManualCollect();
            ValidateHashSet(hashSetKey);

            void ExecuteHashSet(string key, HashEntry[] elements)
            {
                db.HashSet(key, elements);

                var result = primaryServer.Execute("HPEXPIRE", key, "500", "FIELDS", "2", "field1", "field2");
                var results = (RedisResult[])result;
                ClassicAssert.AreEqual(2, results.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
                ClassicAssert.AreEqual(1, (long)results[1]);

                result = primaryServer.Execute("HPEXPIRE", key, "500", "FIELDS", "2", "field3", "field4");
                results = (RedisResult[])result;
                ClassicAssert.AreEqual(2, results.Length);
                ClassicAssert.AreEqual(1, (long)results[0]);
                ClassicAssert.AreEqual(1, (long)results[1]);
            }

            void ValidateHashSet(string key)
            {
                // Check expected result at primary
                var expectedFieldsAndValues = elements.AsSpan().Slice(4).ToArray()
                    .SelectMany(e => new[] { e.Name.ToString(), e.Value.ToString() })
                    .ToArray();
                var fields = (string[])primaryServer.Execute("HGETALL", [key]);
                ClassicAssert.AreEqual(4, fields.Length);
                ClassicAssert.AreEqual(expectedFieldsAndValues, fields);

                // Wait to ensure sync
                context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger, cancellationToken);

                // Check if replica is caught up
                fields = (string[])replicaServer.Execute("HGETALL", [key]);
                ClassicAssert.AreEqual(4, fields.Length);
                ClassicAssert.AreEqual(expectedFieldsAndValues, fields);
            }

            void ManualCollect()
            {
                if (useManualCollect)
                {
                    Assert.Throws<RedisServerException>(() => replicaServer.Execute("HCOLLECT", ["*"], CommandFlags.NoRedirect),
                        $"Expected exception was not thrown");

                    resp = (string)primaryServer.Execute("HCOLLECT", ["*"]);
                    ClassicAssert.AreEqual("OK", resp);
                }
            }
        }

        [Test, Order(17)]
        [Category("CLUSTER")]
        [CancelAfter(30_000)]
        public async Task ReplicasRestartAsReplicasAsync(CancellationToken cancellation)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + primary_count * replica_count;
            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(nodes_count, disableObjects: false, enableAOF: true, useTLS: true, tryRecover: false, FastAofTruncate: true, CommitFrequencyMs: -1, clusterReplicationReestablishmentTimeout: 1);
            context.CreateConnection(useTLS: true);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var primary = (IPEndPoint)context.endpoints[0];
            var replica = (IPEndPoint)context.endpoints[1];

            // Make sure role assignment is as expected
            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(replica).Value);

            context.ShutdownNode(primary);
            context.ShutdownNode(replica);

            // Intentionally leaving primary offline
            context.RestartNode(replica);

            // Delay a bit for replication init tasks to fire off
            await Task.Delay(100, cancellation).ConfigureAwait(false);

            // Make sure replica did not promote to Primary
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(replica).Value);
        }

        [Test, Order(18)]
        [Category("CLUSTER")]
        [CancelAfter(30_000)]
        [TestCase(ExceptionInjectionType.None, true)]
        [TestCase(ExceptionInjectionType.None, false)]
        [TestCase(ExceptionInjectionType.Divergent_AOF_Stream, true)]
        [TestCase(ExceptionInjectionType.Divergent_AOF_Stream, false)]
        [TestCase(ExceptionInjectionType.Aof_Sync_Task_Consume, true)]
        [TestCase(ExceptionInjectionType.Aof_Sync_Task_Consume, false)]
        public async Task PrimaryUnavailableRecoveryAsync(ExceptionInjectionType faultType, bool replicaFailoverBeforeShutdown, CancellationToken cancellation)
        {
            // Case we're testing is where a Primary _and_ it's Replica die, but only the Replica comes back.
            //
            // If configured correctly (for example, as a cache), we still want the Replica to come back up with some data - even if it's
            // acknowledges writes to the Primary are dropped.
            //
            // We also sometimes inject faults into the Primaries and Replicas before the proper fault, to simulate a cluster
            // in an unstable environment.
            //
            // Additionally we simulate both when we detect failures and intervene in time to promote Replicas to Primaries,
            // and when we don't intervene until after everything dies and the Replicas come back.

#if !DEBUG
            Assert.Ignore($"Depends on {nameof(ExceptionInjectionHelper)}, which is disabled in non-Debug builds");
#endif

            var replica_count = 1;// Per primary
            var primary_count = 2;
            var nodes_count = primary_count + primary_count * replica_count;
            ClassicAssert.IsTrue(primary_count > 0);

            // Config lifted from a deployed product, be wary of changing these without discussion
            context.CreateInstances(
                nodes_count,
                tryRecover: true,
                disablePubSub: false,
                disableObjects: false,
                enableAOF: true,
                AofMemorySize: "128m",
                CommitFrequencyMs: -1,
                aofSizeLimit: "256m",
                compactionFrequencySecs: 30,
                compactionType: LogCompactionType.Scan,
                latencyMonitory: true,
                metricsSamplingFrequency: 1,
                loggingFrequencySecs: 10,
                checkpointThrottleFlushDelayMs: 0,
                FastCommit: true,
                FastAofTruncate: true,
                OnDemandCheckpoint: true,
                useTLS: true,
                enableLua: true,
                luaMemoryMode: LuaMemoryManagementMode.Tracked,
                luaTransactionMode: true,
                luaMemoryLimit: "2M",
                clusterReplicationReestablishmentTimeout: 1,
                clusterReplicaResumeWithData: true
            );
            context.CreateConnection(useTLS: true);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(2, shards.Count);

            IPEndPoint primary1 = (IPEndPoint)context.endpoints[0];
            IPEndPoint primary2 = (IPEndPoint)context.endpoints[1];
            IPEndPoint replica1 = (IPEndPoint)context.endpoints[2];
            IPEndPoint replica2 = (IPEndPoint)context.endpoints[3];

            // Make sure role assignment is as expected
            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary1).Value);
            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary2).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(replica1).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(replica2).Value);

            // Populate both shards
            var writtenToPrimary1 = new ConcurrentDictionary<string, string>();
            var writtenToPrimary2 = new ConcurrentDictionary<string, string>();
            using (var writeTaskCancel = CancellationTokenSource.CreateLinkedTokenSource(cancellation))
            {
                var uniquePrefix = Guid.NewGuid();

                var writeToPrimary1Task =
                    Task.Run(
                        async () =>
                        {
                            var ix = -1;

                            var p1 = shards.Single(s => s.nodes.Any(n => n.nodeIndex == context.endpoints.IndexOf(primary1)));

                            while (!writeTaskCancel.IsCancellationRequested)
                            {
                                ix++;

                                var key = $"pura-1-{uniquePrefix}-{ix}";

                                var slot = context.clusterTestUtils.HashSlot(key);
                                if (!p1.slotRanges.Any(x => slot >= x.Item1 && slot <= x.Item2))
                                {
                                    continue;
                                }

                                var value = Guid.NewGuid().ToString();
                                try
                                {
                                    var res = (string)context.clusterTestUtils.Execute(primary1, "SET", [key, value]);
                                    if (res == "OK")
                                    {
                                        writtenToPrimary1[key] = value;
                                    }

                                    await Task.Delay(10, writeTaskCancel.Token).ConfigureAwait(false);
                                }
                                catch
                                {
                                    // Ignore, cancellation or throwing should both just be powered through
                                }
                            }
                        },
                        cancellation
                    );

                var writeToPrimary2Task =
                    Task.Run(
                        async () =>
                        {
                            var ix = -1;

                            var p2 = shards.Single(s => s.nodes.Any(n => n.nodeIndex == context.endpoints.IndexOf(primary2)));

                            while (!writeTaskCancel.IsCancellationRequested)
                            {
                                ix++;

                                var key = $"pura-2-{uniquePrefix}-{ix}";

                                var slot = context.clusterTestUtils.HashSlot(key);
                                if (!p2.slotRanges.Any(x => slot >= x.Item1 && slot <= x.Item2))
                                {
                                    continue;
                                }

                                var value = Guid.NewGuid().ToString();

                                try
                                {
                                    var res = (string)context.clusterTestUtils.Execute(primary1, "SET", [key, value]);
                                    if (res == "OK")
                                    {
                                        writtenToPrimary2[key] = value;
                                    }

                                    await Task.Delay(10, writeTaskCancel.Token).ConfigureAwait(false);
                                }
                                catch
                                {
                                    // Ignore, cancellation or throwing should both just be powered through
                                }
                            }
                        },
                        cancellation
                    );

                // Simulate out of band checkpointing
                var checkpointTask =
                    Task.Run(
                        async () =>
                        {
                            while (!writeTaskCancel.IsCancellationRequested)
                            {
                                foreach (var node in context.nodes)
                                {
                                    try
                                    {
                                        _ = await node.Store.CommitAOFAsync(writeTaskCancel.Token).ConfigureAwait(false);
                                    }
                                    catch (TaskCanceledException)
                                    {
                                        // Cancel is fine
                                        break;
                                    }
                                    catch
                                    {
                                        // Ignore everything else
                                    }
                                }

                                try
                                {
                                    await Task.Delay(100, writeTaskCancel.Token).ConfigureAwait(false);
                                }
                                catch
                                {
                                    continue;
                                }
                            }
                        },
                        cancellation
                    );

                // Wait for a bit, optionally injecting a fault
                if (faultType == ExceptionInjectionType.None)
                {
                    await Task.Delay(10_000, cancellation).ConfigureAwait(false);
                }
                else
                {
                    var timer = Stopwatch.StartNew();

                    // Things start fine
                    await Task.Delay(1_000, cancellation).ConfigureAwait(false);

                    // Wait for something to get replicated
                    var replica1Happened = false;
                    var replica2Happened = false;
                    do
                    {
                        if (!replica1Happened)
                        {
                            var readonlyRes = (string)context.clusterTestUtils.Execute(replica1, "READONLY", []);
                            ClassicAssert.AreEqual("OK", readonlyRes);

                            foreach (var kv in writtenToPrimary1)
                            {
                                var val = (string)context.clusterTestUtils.Execute(replica1, "GET", [kv.Key]);
                                if (val == kv.Value)
                                {
                                    replica1Happened = true;
                                    break;
                                }
                            }
                        }

                        if (!replica2Happened)
                        {
                            var readonlyRes = (string)context.clusterTestUtils.Execute(replica2, "READONLY", []);
                            ClassicAssert.AreEqual("OK", readonlyRes);

                            foreach (var kv in writtenToPrimary2)
                            {
                                var val = (string)context.clusterTestUtils.Execute(replica2, "GET", [kv.Key]);
                                if (val == kv.Value)
                                {
                                    replica2Happened = true;
                                    break;
                                }
                            }
                        }

                        await Task.Delay(100, cancellation).ConfigureAwait(false);
                    } while (!replica1Happened || !replica2Happened);

                    // Things fail for a bit
                    ExceptionInjectionHelper.EnableException(faultType);
                    await Task.Delay(2_000, cancellation).ConfigureAwait(false);

                    // Things recover
                    ExceptionInjectionHelper.DisableException(faultType);

                    timer.Stop();

                    // Wait out the rest of the duration
                    if (timer.ElapsedMilliseconds < 10_000)
                    {
                        await Task.Delay((int)(10_000 - timer.ElapsedMilliseconds), cancellation).ConfigureAwait(false);
                    }
                }

                // Stop writing
                writeTaskCancel.Cancel();

                // Wait for all our writes and checkpoints to spin down
                await Task.WhenAll(writeToPrimary1Task, writeToPrimary2Task, checkpointTask).ConfigureAwait(false);
            }

            // Shutdown all primaries
            context.ShutdownNode(primary1);
            context.ShutdownNode(primary2);

            // Sometimes we can intervene post-Primary crash but pre-Replica crash, simulate that
            if (replicaFailoverBeforeShutdown)
            {
                await UpgradeReplicasAsync(context, replica1, replica2, cancellation).ConfigureAwait(false);
            }

            // Shutdown the (old) replicas
            context.ShutdownNode(replica1);
            context.ShutdownNode(replica2);

            // Restart just the replicas
            context.RestartNode(replica1);
            context.RestartNode(replica2);

            // If we didn't promte pre-crash, promote now that Replicas came back
            if (!replicaFailoverBeforeShutdown)
            {
                await UpgradeReplicasAsync(context, replica1, replica2, cancellation).ConfigureAwait(false);
            }

            // Confirm that at least some of the data is available on each Replica
            var onReplica1 = 0;
            foreach (var (k, v) in writtenToPrimary1)
            {
                var res = (string)context.clusterTestUtils.Execute(replica1, "GET", [k]);
                if (res is not null)
                {
                    ClassicAssert.AreEqual(v, res);
                    onReplica1++;
                }
            }

            var onReplica2 = 0;
            foreach (var (k, v) in writtenToPrimary2)
            {
                var res = (string)context.clusterTestUtils.Execute(replica2, "GET", [k]);
                if (res is not null)
                {
                    ClassicAssert.AreEqual(v, res);
                    onReplica2++;
                }
            }

            // Something, ANYTHING, made it
            ClassicAssert.IsTrue(onReplica1 > 0, $"Nothing made it to replica 1, should have been up to {writtenToPrimary1.Count} values");
            ClassicAssert.IsTrue(onReplica2 > 0, $"Nothing made it to replica 2, should have been up to {writtenToPrimary2.Count} values");

            static async Task UpgradeReplicasAsync(ClusterTestContext context, IPEndPoint replica1, IPEndPoint replica2, CancellationToken cancellation)
            {
                // Promote the replicas, if no primary is coming back
                var takeOverRes1 = (string)context.clusterTestUtils.Execute(replica1, "CLUSTER", ["FAILOVER", "FORCE"]);
                var takeOverRes2 = (string)context.clusterTestUtils.Execute(replica2, "CLUSTER", ["FAILOVER", "FORCE"]);
                ClassicAssert.AreEqual("OK", takeOverRes1);
                ClassicAssert.AreEqual("OK", takeOverRes2);

                // Wait for roles to update
                while (true)
                {
                    await Task.Delay(10, cancellation).ConfigureAwait(false);

                    if (context.clusterTestUtils.RoleCommand(replica1).Value != "master")
                    {
                        continue;
                    }

                    if (context.clusterTestUtils.RoleCommand(replica2).Value != "master")
                    {
                        continue;
                    }

                    break;
                }
            }
        }
    }
}