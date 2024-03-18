// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture(false), NonParallelizable]
    public class ClusterReplicationTests
    {
        public (Action, string)[] GetUnitTests()
        {
            (Action, string)[] x = new (Action, string)[39];
            //1
            x[0] = new(() => ClusterSRTest(true), "ClusterSRTest(true)");
            x[1] = new(() => ClusterSRTest(false), "ClusterSRTest(false)");

            //2
            x[2] = new(() => ClusterSRNoCheckpointRestartSecondary(false, false), "ClusterSRNoCheckpointRestartSecondary(false, false)");
            x[3] = new(() => ClusterSRNoCheckpointRestartSecondary(false, true), "ClusterSRNoCheckpointRestartSecondary(false, true)");
            x[4] = new(() => ClusterSRNoCheckpointRestartSecondary(true, false), "ClusterSRNoCheckpointRestartSecondary(true, false)");
            x[5] = new(() => ClusterSRNoCheckpointRestartSecondary(true, true), "ClusterSRNoCheckpointRestartSecondary(true, true)");

            //3
            x[6] = new(() => ClusterSRPrimaryCheckpoint(false, false), "ClusterSRPrimaryCheckpoint(false, false)");
            x[7] = new(() => ClusterSRPrimaryCheckpoint(false, true), "ClusterSRPrimaryCheckpoint(false, true)");
            x[8] = new(() => ClusterSRPrimaryCheckpoint(true, false), "ClusterSRPrimaryCheckpoint(true, false)");
            x[9] = new(() => ClusterSRPrimaryCheckpoint(true, true), "ClusterSRPrimaryCheckpoint(true, true)");

            //4
            x[10] = new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, false, false), "ClusterSRPrimaryCheckpointRetrieve(false, false, false, false)");
            x[11] = new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, false, true), "ClusterSRPrimaryCheckpointRetrieve(false, false, false, true)");
            x[12] = new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, false, false), "ClusterSRPrimaryCheckpointRetrieve(false, true, false, false)");
            x[13] = new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, false, true), "ClusterSRPrimaryCheckpointRetrieve(false, true, false, true)");
            x[14] = new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, false, false), "ClusterSRPrimaryCheckpointRetrieve(true, false, false, false)");
            x[15] = new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, false, true), "ClusterSRPrimaryCheckpointRetrieve(true, false, false, true)");
            x[16] = new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, false, false), "ClusterSRPrimaryCheckpointRetrieve(true, true, false, false)");
            x[17] = new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, false, true), "ClusterSRPrimaryCheckpointRetrieve(true, true, false, true)");
            x[18] = new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, true, false), "ClusterSRPrimaryCheckpointRetrieve(false, false, false)");
            x[19] = new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, true, true), "ClusterSRPrimaryCheckpointRetrieve(false, false, true)");
            x[20] = new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, true, false), "ClusterSRPrimaryCheckpointRetrieve(false, true, true, false)");
            x[21] = new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, true, true), "ClusterSRPrimaryCheckpointRetrieve(false, true, true, true)");
            x[22] = new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, true, false), "ClusterSRPrimaryCheckpointRetrieve(true, false, true, false)");
            x[23] = new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, true, true), "ClusterSRPrimaryCheckpointRetrieve(true, false, true, true)");
            x[24] = new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, true, false), "ClusterSRPrimaryCheckpointRetrieve(true, true, true, false)");
            x[25] = new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, true, true), "ClusterSRPrimaryCheckpointRetrieve(true, true, true, true)");

            //5
            x[26] = new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(false, false, false), "ClusterSRAddReplicaAfterPrimaryCheckpoint(false, false, false)");
            x[27] = new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(false, false, true), "ClusterSRAddReplicaAfterPrimaryCheckpoint(false, false, true)");
            x[28] = new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(false, true, false), "ClusterSRAddReplicaAfterPrimaryCheckpoint(false, true, false)");
            x[29] = new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(false, true, true), "ClusterSRAddReplicaAfterPrimaryCheckpoint(false, true, true)");
            x[30] = new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(true, false, false), "ClusterSRAddReplicaAfterPrimaryCheckpoint(true, false, false)");
            x[31] = new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(true, false, true), "ClusterSRAddReplicaAfterPrimaryCheckpoint(true, false, true)");
            x[32] = new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(true, true, false), "ClusterSRAddReplicaAfterPrimaryCheckpoint(true, true, false)");
            x[33] = new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(true, true, true), "ClusterSRAddReplicaAfterPrimaryCheckpoint(true, true, true)");

            //6
            x[34] = new(() => ClusterSRPrimaryRestart(false, false), "ClusterSRPrimaryRestart(false, false)");
            x[35] = new(() => ClusterSRPrimaryRestart(false, true), "ClusterSRPrimaryRestart(false, true)");
            x[36] = new(() => ClusterSRPrimaryRestart(true, false), "ClusterSRPrimaryRestart(true, false)");
            x[37] = new(() => ClusterSRPrimaryRestart(true, true), "ClusterSRPrimaryRestart(true, true)");

            //7
            x[38] = new(ClusterSRRedirectWrites, "ClusterSRRedirectWrites()");

            return x;
        }

        ClusterTestContext context;

        public void SetLogTextWriter(TextWriter logTextWriter) => context.logTextWriter = logTextWriter;
        bool useTLS = false;

        public ClusterReplicationTests(bool UseTLS = false)
        {
            this.useTLS = UseTLS;
        }

        int timeout = 60;
        readonly int keyCount = 256;

        public HashSet<string> monitorTests = new()
        {
            //Add test names here to change logger verbosity
            //"ClusterFailoverAttachReplicas"
        };

        readonly HashSet<string> skipCmd = new()
        {
            "CLUSTER",
            "GET",
            "INCRBY",
            "SET",
            "GET",
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
        [Category("REPLICATION")]
        public void ClusterSRTest([Values] bool disableObjects)
        {
            int replica_count = 1;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            int keyLength = 16;
            int kvpairCount = keyCount;
            context.kvPairs = new();

            //Populate Primary
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);

            for (int i = 1; i < replica_count; i++)
                context.clusterTestUtils.WaitForReplicaAofSync(0, i);

            for (int i = 1; i < replica_count; i++)
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, i);
        }

        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterSRNoCheckpointRestartSecondary([Values] bool performRMW, [Values] bool disableObjects)
        {
            int replica_count = 1;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            int keyLength = 32;
            int kvpairCount = keyCount;
            int addCount = 5;
            context.kvPairs = new();

            //Populate Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            //Wait for replication offsets to synchronize
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);

            //clusterTestUtils.GetServer(1).Execute("QUIT");
            //Shutdown secondary
            context.nodes[1].Store.CommitAOF(true);
            context.nodes[1].Dispose(false);

            Thread.Sleep(TimeSpan.FromSeconds(2));

            ////New insert
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            //Restart secondary
            context.nodes[1] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(1).Port,
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false);
            context.nodes[1].Start();
            context.CreateConnection(useTLS: useTLS);

            //Validate synchronization was success
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);
        }

        [Test, Order(3)]
        [Category("REPLICATION")]
        public void ClusterSRPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects)
        {
            int replica_count = 1;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            int keyLength = 32;
            int kvpairCount = keyCount;
            int addCount = 5;
            context.kvPairs = new();

            //Populate Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);
            context.clusterTestUtils.Checkpoint(0, logger: context.logger);

            //Populate Primary
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);

            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.clusterTestUtils.WaitCheckpoint(0, context.logger);
            context.clusterTestUtils.WaitCheckpoint(1, context.logger);

            //Shutdown secondary
            context.nodes[1].Store.CommitAOF(true);
            context.nodes[1].Dispose(false);
            Thread.Sleep(TimeSpan.FromSeconds(2));

            ////New insert
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            //Restart secondary
            context.nodes[1] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(1).Port,
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false);
            context.nodes[1].Start();
            context.CreateConnection(useTLS: useTLS);

            for (int i = 1; i < replica_count; i++) context.clusterTestUtils.WaitForReplicaRecovery(i, context.logger);
            context.clusterTestUtils.WaitForConnectedReplicaCount(0, replica_count, context.logger);

            //Validate synchronization was success
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);
        }

        [Test, Order(4)]
        [Category("REPLICATION")]
        public void ClusterCheckpointRetrieveDisableStorageTier([Values] bool performRMW, [Values] bool disableObjects)
        {
            ClusterSRPrimaryCheckpointRetrieve(performRMW, disableObjects, false, false, true, false);
        }

        [Test, Order(5)]
        [Category("REPLICATION")]
        public void ClusterCheckpointRetrieveDelta([Values] bool performRMW)
        {
            ClusterSRPrimaryCheckpointRetrieve(performRMW, true, false, false, false, true);
        }

        [Test, Order(6)]
        [Category("REPLICATION")]
        public void ClusterSRPrimaryCheckpointRetrieve([Values] bool performRMW, [Values] bool disableObjects, [Values] bool lowMemory, [Values] bool manySegments)
            => ClusterSRPrimaryCheckpointRetrieve(performRMW, disableObjects, lowMemory, manySegments, false, false);

        void ClusterSRPrimaryCheckpointRetrieve(bool performRMW, bool disableObjects, bool lowMemory, bool manySegments, bool disableStorageTier, bool incrementalSnapshots)
        {
            //test many segments on or off with lowMemory
            manySegments = !lowMemory ? false : manySegments;

            int replica_count = 1;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, lowMemory: lowMemory, SegmentSize: manySegments ? "4k" : "1g", DisableStorageTier: disableStorageTier, EnableIncrementalSnapshots: incrementalSnapshots, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            int keyLength = 32;
            int kvpairCount = disableStorageTier ? 16 : keyCount;
            int addCount = 5;
            context.kvPairs = new();
            context.kvPairsObj = new Dictionary<string, List<int>>();

            context.logger?.LogTrace("Test disposing node 1");
            context.nodes[1].Dispose(false);
            Thread.Sleep(TimeSpan.FromSeconds(1));

            //Populate Primary
            if (disableObjects)
            {
                if (!performRMW)
                    context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0, null, incrementalSnapshots, 0);
                else
                    context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount, null, incrementalSnapshots, 0);
            }
            else
            {
                if (disableStorageTier)
                    context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, 0, 1, 8);
                else
                    context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, 0);
            }
            context.clusterTestUtils.Checkpoint(0, logger: context.logger);

            //Restart secondary
            context.nodes[1] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(1).Port,
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false,
                lowMemory: lowMemory,
                SegmentSize: manySegments ? "4k" : "1g",
                DisableStorageTier: disableStorageTier);
            context.nodes[1].Start();
            context.CreateConnection(useTLS: useTLS);

            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            if (disableObjects)
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);
            else
                context.ValidateNodeObjects(ref context.kvPairsObj, 1);
        }

        [Test, Order(7)]
        [Category("REPLICATION")]
        public void ClusterSRAddReplicaAfterPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects, [Values] bool lowMemory)
        {
            int replica_count = 1;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: disableObjects, lowMemory: lowMemory, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);

            Assert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true, context.logger));
            context.clusterTestUtils.BumpEpoch(0, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            int keyLength = 32;
            int kvpairCount = keyCount;
            int addCount = 5;
            context.kvPairs = new();
            context.kvPairsObj = new Dictionary<string, List<int>>();

            //Populate Primary
            if (disableObjects)
            {
                if (!performRMW)
                    context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
                else
                    context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);
            }
            else
            {
                context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, 0);
            }
            context.clusterTestUtils.Checkpoint(0, logger: context.logger);

            //Add new replica node
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            context.clusterTestUtils.Meet(1, 0, logger: context.logger);
            context.clusterTestUtils.WaitAll(context.logger);
            context.clusterTestUtils.ClusterReplicate(1, primaryId, async: false, logger: context.logger);
            context.clusterTestUtils.BumpEpoch(1, logger: context.logger);

            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            if (disableObjects)
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);
            else
                context.ValidateNodeObjects(ref context.kvPairsObj, 1);
        }

        [Test, Order(8)]
        [Category("REPLICATION")]
        public void ClusterSRPrimaryRestart([Values] bool performRMW, [Values] bool disableObjects)
        {
            int replica_count = 1;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);

            Assert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true, context.logger));
            context.clusterTestUtils.BumpEpoch(0, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            int keyLength = 32;
            int kvpairCount = keyCount;
            int addCount = 5;
            Dictionary<string, int> kvPairs = new();

            if (!performRMW)
                context.PopulatePrimary(ref kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref kvPairs, keyLength, kvpairCount, 0, addCount);
            context.clusterTestUtils.Checkpoint(0, logger: context.logger);

            var storeCurrentAofAddress = context.clusterTestUtils.GetStoreCurrentAofAddress(0, logger: context.logger);
            long objectStoreCurrentAofAddress = -1;
            if (!disableObjects)
                objectStoreCurrentAofAddress = context.clusterTestUtils.GetObjectStoreCurrentAofAddress(0, context.logger);

            context.nodes[0].Dispose(false);
            Thread.Sleep(TimeSpan.FromSeconds(1));

            //Restart Primary
            context.nodes[0] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(0).Port,
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false);
            context.nodes[0].Start();
            context.CreateConnection(useTLS: useTLS);

            var storeRecoveredAofAddress = context.clusterTestUtils.GetStoreRecoveredAofAddress(0, context.logger);
            long objectStoreRecoveredAofAddress = -1;
            if (!disableObjects)
                objectStoreRecoveredAofAddress = context.clusterTestUtils.GetObjectStoreRecoveredAofAddress(0, logger: context.logger);

            Assert.AreEqual(storeCurrentAofAddress, storeRecoveredAofAddress);
            if (!disableObjects)
                Assert.AreEqual(objectStoreCurrentAofAddress, objectStoreRecoveredAofAddress);
        }

        [Test, Order(9)]
        [Category("REPLICATION")]
        public void ClusterSRRedirectWrites()
        {
            int replica_count = 1;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);

            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var resp = context.clusterTestUtils.SetKey(1, Encoding.ASCII.GetBytes("testKey"), Encoding.ASCII.GetBytes("testValue"), out _, out _, out _, logger: context.logger);
            Assert.AreEqual(ResponseState.MOVED, resp);
        }

        [Test, Order(10)]
        [Category("REPLICATION")]
        public void ClusterSRReplicaOfTest([Values] bool performRMW)
        {
            int nodes_count = 2;
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: true, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);

            Assert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true, context.logger));

            context.clusterTestUtils.SetConfigEpoch(0, 1, context.logger);
            context.clusterTestUtils.SetConfigEpoch(1, 2, context.logger);

            var configEpoch = context.clusterTestUtils.GetConfigEpoch(0, context.logger);
            Assert.AreEqual(1, configEpoch);

            configEpoch = context.clusterTestUtils.GetConfigEpoch(1, context.logger);
            Assert.AreEqual(2, configEpoch);

            context.clusterTestUtils.Meet(0, 1, logger: context.logger);
            context.clusterTestUtils.WaitClusterNodesSync(syncOnNodeIndex: 0, count: 2, context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(1, 0, logger: context.logger);

            int keyLength = 32;
            int kvpairCount = keyCount;
            int addCount = 5;
            context.kvPairs = new();
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            context.clusterTestUtils.ReplicaOf(replicaNodeIndex: 1, primaryNodeIndex: 0, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1);
        }

        [Test, Order(11)]
        [Category("REPLICATION")]
        public void ClusterReplicationSimpleFailover([Values] bool performRMW, [Values] bool checkpoint)
        {
            int replica_count = 1;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            int keyLength = 32;
            int kvpairCount = 16;
            int addCount = 10;
            context.kvPairs = new();

            //Populate Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            //Wait for replication offsets to synchronize
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);

            if (checkpoint) context.clusterTestUtils.Checkpoint(0);

            #region InitiateFailover
            int[] slotMap = new int[16384];
            //Failover primary
            context.ClusterFailoverRetry(1);
            //Reconfigure slotMap to reflect new primary
            for (int i = 0; i < 16384; i++)
                slotMap[i] = 1;
            #endregion

            //Check if allowed to write to new Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0, slotMap: slotMap);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount, slotMap: slotMap);
        }

        [Test, Order(12)]
        [Category("REPLICATION")]
        public void ClusterFailoverAttachReplicas([Values] bool performRMW, [Values] bool takePrimaryCheckpoint, [Values] bool takeNewPrimaryCheckpoint, [Values] bool enableIncrementalSnapshots)
        {
            int replica_count = 2; // Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, EnableIncrementalSnapshots: enableIncrementalSnapshots, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var primary = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", primary.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, primary.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var rconfig1 = context.clusterTestUtils.ClusterNodes(1, context.logger);
            var rconfig2 = context.clusterTestUtils.ClusterNodes(2, context.logger);
            Assert.AreEqual(primary.NodeId, rconfig1.Nodes.First().ParentNodeId);
            Assert.AreEqual(primary.NodeId, rconfig2.Nodes.First().ParentNodeId);

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            int keyLength = 32;
            int kvpairCount = keyCount;
            int addCount = 5;
            context.kvPairs = new();

            // Populate Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            if (takePrimaryCheckpoint)
            {
                context.clusterTestUtils.Checkpoint(0, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(0, logger: context.logger);
            }

            // Wait for replication offsets to synchronize
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 2, context.logger);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);

            // Simulate primary crash
            context.nodes[0].Dispose();
            context.nodes[0] = null;

            // Takeover as new primary
            context.clusterTestUtils.ClusterFailover(1, "TAKEOVER", logger: context.logger);

            //Wait for both nodes to enter no failover
            context.clusterTestUtils.WaitForNoFailover(1, context.logger);
            context.clusterTestUtils.WaitForNoFailover(2, context.logger);

            //Wait for replica to recover
            context.clusterTestUtils.WaitForReplicaRecovery(2, context.logger);

            if (takeNewPrimaryCheckpoint)
            {
                context.clusterTestUtils.Checkpoint(1, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(1, logger: context.logger);
            }
            context.clusterTestUtils.WaitForReplicaAofSync(1, 2, context.logger);

            // Check if replica 2 after failover contains keys
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 2, primaryIndex: 1);

            // Send extra keys to primary, verify replica gets the keys
            // This also ensures that the test does not end while failover AOF sync connection is in progress

            context.SendAndValidateKeys(1, 2, keyLength, 5);
        }

        [Test, Order(13)]
        public void ClusterReplicationCheckpointCleanupTest([Values] bool performRMW, [Values] bool disableObjects, [Values] bool enableIncrementalSnapshots)
        {
            int replica_count = 2;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: disableObjects, lowMemory: true, SegmentSize: "4k", EnableIncrementalSnapshots: enableIncrementalSnapshots, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);
            Assert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true, context.logger));
            context.clusterTestUtils.BumpEpoch(0, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = String.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            Assert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            Assert.AreEqual(1, shards.Count);
            Assert.AreEqual(1, shards[0].slotRanges.Count);
            Assert.AreEqual(0, shards[0].slotRanges[0].Item1);
            Assert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            context.kvPairs = new();
            context.kvPairsObj = new Dictionary<string, List<int>>();
            context.checkpointTask = Task.Run(() => context.PopulatePrimaryAndTakeCheckpointTask(performRMW, disableObjects, takeCheckpoint: true));
            var attachReplicaTask = Task.Run(() => context.AttachAndWaitForSync(primary_count, replica_count, disableObjects));

            if (!attachReplicaTask.Wait(TimeSpan.FromSeconds(100)))
                Assert.Fail("attachReplicaTask timeout");
        }

        [Test, Order(14)]
        [Category("REPLICATION")]
        public void ClusterMainMemoryReplicationAttachReplicas()
        {
            int replica_count = 2; // Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, MainMemoryReplication: true, OnDemandCheckpoint: true, CommitFrequencyMs: -1, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);

            Assert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true));
            context.clusterTestUtils.SetConfigEpoch(0, 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(1, 2, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(2, 3, logger: context.logger);

            for (int i = 1; i < nodes_count; i++) context.clusterTestUtils.Meet(0, i);

            for (int i = 0; i < nodes_count; i++)
                for (int j = 0; j < nodes_count; j++)
                    if (i != j) context.clusterTestUtils.WaitUntilNodeIsKnown(i, j, context.logger);

            context.kvPairs = new();
            context.kvPairsObj = new Dictionary<string, List<int>>();
            var task = Task.Run(()
                => context.PopulatePrimaryAndTakeCheckpointTask(performRMW: false, disableObjects: true, takeCheckpoint: false, iter: 10));

            var primaryId = context.clusterTestUtils.ClusterMyId(0, context.logger);
            context.clusterTestUtils.ClusterReplicate(1, primaryId, async: true, logger: context.logger);
            context.clusterTestUtils.ClusterReplicate(2, primaryId, async: true, logger: context.logger);

            if (!task.Wait(TimeSpan.FromSeconds(100)))
                Assert.Fail("Checkpoint task timeout");

            context.clusterTestUtils.WaitForReplicaRecovery(1, context.logger);
            context.clusterTestUtils.WaitForReplicaRecovery(2, context.logger);

            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 2, context.logger);

            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 2);
        }

        [Test, Order(15)]
        [Category("REPLICATION")]
        public void ClusterDontKnowReplicaFailTest([Values] bool performRMW, [Values] bool MainMemoryReplication, [Values] bool onDemandCheckpoint, [Values] bool useReplicaOf)
        {
            int replica_count = 1;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, MainMemoryReplication: MainMemoryReplication, OnDemandCheckpoint: onDemandCheckpoint, CommitFrequencyMs: -1, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);

            Assert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true, context.logger));
            context.clusterTestUtils.SetConfigEpoch(0, 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(1, 2, logger: context.logger);
            context.clusterTestUtils.Meet(0, 1, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(0, 1, logger: context.logger);

            var replicaId = context.clusterTestUtils.ClusterMyId(1, logger: context.logger);
            context.clusterTestUtils.ClusterForget(0, replicaId, 5, logger: context.logger);

            var primaryId = context.clusterTestUtils.ClusterMyId(0, logger: context.logger);
            string resp;
            if (!useReplicaOf)
                resp = context.clusterTestUtils.ClusterReplicate(1, primaryId, failEx: false, logger: context.logger);
            else
                resp = context.clusterTestUtils.ReplicaOf(1, 0, failEx: false, logger: context.logger);
            Assert.IsTrue(resp.StartsWith("PRIMARY-ERR"));

            while (true)
            {
                context.clusterTestUtils.Meet(0, 1, logger: context.logger);
                context.clusterTestUtils.BumpEpoch(1, logger: context.logger);
                var config = context.clusterTestUtils.ClusterNodes(0, logger: context.logger);
                if (config.Nodes.Count == 2) break;
                Thread.Sleep(1000);
            }

            context.clusterTestUtils.ClusterReplicate(1, primaryId, logger: context.logger);

            context.kvPairs = new();
            int keyLength = 32;
            int kvpairCount = keyCount;
            int addCount = 5;
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);
        }

        [Test, Order(16)]
        [Category("REPLICATION")]
        public void ClusterDivergentReplicasTest([Values] bool performRMW, [Values] bool disableObjects, [Values] bool ckptBeforeDivergence)
            => ClusterDivergentReplicasTest(performRMW, disableObjects, ckptBeforeDivergence, false, false, fastCommit: false);

        [Test, Order(17)]
        [Category("REPLICATION")]
        public void ClusterDivergentCheckpointTest([Values] bool performRMW, [Values] bool disableObjects)
            => ClusterDivergentReplicasTest(
                performRMW,
                disableObjects,
                ckptBeforeDivergence: true,
                multiCheckpointAfterDivergence: true,
                mainMemoryReplication: false,
                fastCommit: false);

        [Test, Order(18)]
        [Category("REPLICATION")]
        public void ClusterDivergentReplicasMMTest([Values] bool performRMW, [Values] bool disableObjects, [Values] bool ckptBeforeDivergence)
            => ClusterDivergentReplicasTest(
                performRMW,
                disableObjects,
                ckptBeforeDivergence,
                multiCheckpointAfterDivergence: false,
                mainMemoryReplication: true,
                fastCommit: false);

        [Test, Order(19)]
        [Category("REPLICATION")]
        public void ClusterDivergentCheckpointMMTest([Values] bool performRMW, [Values] bool disableObjects)
            => ClusterDivergentReplicasTest(
                performRMW,
                disableObjects,
                ckptBeforeDivergence: true,
                multiCheckpointAfterDivergence: true,
                mainMemoryReplication: true,
                fastCommit: false);

        [Test, Order(20)]
        [Category("REPLICATION")]
        public void ClusterDivergentCheckpointMMFastCommitTest([Values] bool disableObjects, [Values] bool mainMemoryReplication)
            => ClusterDivergentReplicasTest(
                performRMW: false,
                disableObjects: disableObjects,
                ckptBeforeDivergence: true,
                multiCheckpointAfterDivergence: true,
                mainMemoryReplication: mainMemoryReplication,
                fastCommit: true);

        void ClusterDivergentReplicasTest(bool performRMW, bool disableObjects, bool ckptBeforeDivergence, bool multiCheckpointAfterDivergence, bool mainMemoryReplication, bool fastCommit)
        {
            bool set = false;
            int replica_count = 2;//Per primary
            int primary_count = 1;
            int nodes_count = primary_count + primary_count * replica_count;
            Assert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, MainMemoryReplication: mainMemoryReplication, CommitFrequencyMs: mainMemoryReplication ? -1 : 0, OnDemandCheckpoint: mainMemoryReplication, FastCommit: fastCommit, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            int oldPrimaryIndex = 0;
            int newPrimaryIndex = 1;
            int replicaIndex = 2;

            int keyLength = 8;
            int kvpairCount = 16;
            int addCount = 5;
            context.kvPairs = new();
            context.kvPairsObj = new Dictionary<string, List<int>>();

            var oldPrimaryId = context.clusterTestUtils.ClusterMyId(oldPrimaryIndex, context.logger);

            //Populate Primary
            if (disableObjects)
            {
                if (!performRMW) context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex);
                else context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, addCount);
            }
            else context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, set: set);

            if (ckptBeforeDivergence) context.clusterTestUtils.Checkpoint(oldPrimaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(oldPrimaryIndex, newPrimaryIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(oldPrimaryIndex, replicaIndex, context.logger);
            context.clusterTestUtils.WaitCheckpoint(newPrimaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(replicaIndex, logger: context.logger);

            //Make this replica of no-one
            context.clusterTestUtils.ReplicaOf(1, logger: context.logger);

            //Populate primary to diverge from replica 1 history
            //Use temporary dictionary to populate values lost to replica 1
            Dictionary<string, int> kvPairs2 = new();
            Dictionary<string, List<int>> kvPairsObj2 = new Dictionary<string, List<int>>();
            if (disableObjects)
            {
                if (!performRMW) context.PopulatePrimary(ref kvPairs2, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex);
                else context.PopulatePrimaryRMW(ref kvPairs2, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, addCount);
            }
            else context.PopulatePrimaryWithObjects(ref kvPairsObj2, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, set: set);

            //Take multiple checkpoints after divergence
            if (multiCheckpointAfterDivergence)
            {
                int count = 2;
                while (--count > 0)
                {
                    context.clusterTestUtils.Checkpoint(oldPrimaryIndex, logger: context.logger);

                    if (disableObjects)
                    {
                        if (!performRMW) context.PopulatePrimary(ref kvPairs2, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex);
                        else context.PopulatePrimaryRMW(ref kvPairs2, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, addCount);
                    }
                    else context.PopulatePrimaryWithObjects(ref kvPairsObj2, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, set: set);
                }
            }
            context.clusterTestUtils.WaitForReplicaAofSync(oldPrimaryIndex, replicaIndex, context.logger);

            //Dispose primary
            context.nodes[0].Dispose(false);
            context.nodes[0] = null;
            //clusterTestUtils.Reconnect(nodes: new List<int> { newPrimaryIndex, replicaIndex}, textWriter: logTextWriter, logger: logger);

            //Re-assign slots to replica manually since failover option was not            
            context.clusterTestUtils.AddDelSlotsRange(newPrimaryIndex, new List<(int, int)>() { (0, 16383) }, addslot: false, context.logger);
            context.clusterTestUtils.AddDelSlotsRange(replicaIndex, new List<(int, int)>() { (0, 16383) }, addslot: false, context.logger);

            context.clusterTestUtils.AddDelSlotsRange(newPrimaryIndex, new List<(int, int)>() { (0, 16383) }, addslot: true, context.logger);
            context.clusterTestUtils.BumpEpoch(newPrimaryIndex, logger: context.logger);

            //New primary diverges to its own history by new random seed
            kvpairCount <<= 1;
            if (disableObjects)
            {
                if (!performRMW) context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: newPrimaryIndex, randomSeed: 1234);
                else context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: newPrimaryIndex, addCount, randomSeed: 1234);
            }
            else context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, primaryIndex: newPrimaryIndex, randomSeed: 1234, set: set);

            if (!ckptBeforeDivergence || multiCheckpointAfterDivergence) context.clusterTestUtils.Checkpoint(newPrimaryIndex, logger: context.logger);

            var newPrimaryId = context.clusterTestUtils.ClusterMyId(newPrimaryIndex, context.logger);
            while (true)
            {
                var replicaConfig = context.clusterTestUtils.ClusterNodes(replicaIndex, context.logger);
                var clusterNode = replicaConfig.GetBySlot(0);
                if (clusterNode != null && clusterNode.NodeId.Equals(newPrimaryId))
                    break;
                Thread.Yield();
            }

            context.clusterTestUtils.ReplicaOf(replicaIndex, newPrimaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaRecovery(replicaIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(newPrimaryIndex, replicaIndex, context.logger);

            // Check if replica 2 after failover contains keys
            if (disableObjects)
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex: replicaIndex, primaryIndex: newPrimaryIndex);
            else
                context.ValidateNodeObjects(ref context.kvPairsObj, nodeIndex: newPrimaryIndex, set: set);
        }
    }
}