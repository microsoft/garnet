// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [NonParallelizable]
    public class ClusterReplicationBaseTests
    {
        public (Action, string)[] GetUnitTests()
        {
            var x = new (Action, string)[39];
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
        protected bool useTLS = false;
        protected bool asyncReplay = false;
        readonly int timeout = 60;
        readonly int keyCount = 256;

        public Dictionary<string, LogLevel> monitorTests = new()
        {
            {"ClusterReplicationSimpleFailover", LogLevel.Warning},
        };

        [SetUp]
        public virtual void Setup()
        {
            context = new ClusterTestContext();
            context.Setup(monitorTests);
        }

        [TearDown]
        public virtual void TearDown()
        {
            context?.TearDown();
        }

        [Test, Order(1)]
        [Category("REPLICATION")]
        public void ClusterSRTest([Values] bool disableObjects)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + primary_count * replica_count;
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var keyLength = 16;
            var kvpairCount = keyCount;
            context.kvPairs = [];

            //Populate Primary
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);

            for (var i = 1; i < replica_count; i++)
                context.clusterTestUtils.WaitForReplicaAofSync(0, i);

            for (var i = 1; i < replica_count; i++)
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, i);
        }

        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterSRNoCheckpointRestartSecondary([Values] bool performRMW, [Values] bool disableObjects)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var keyLength = 32;
            var kvpairCount = keyCount;
            var addCount = 5;
            context.kvPairs = [];

            // Populate Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            // Wait for replication offsets to synchronize
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);

            // Shutdown secondary
            context.nodes[1].Dispose(false);

            Thread.Sleep(TimeSpan.FromSeconds(2));

            // New insert
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            // Restart secondary
            context.nodes[1] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(1),
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false);
            context.nodes[1].Start();
            context.CreateConnection(useTLS: useTLS);

            // Validate synchronization was success
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);
        }

        [Test, Order(3)]
        [Category("REPLICATION")]
        public void ClusterSRPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var keyLength = 32;
            var kvpairCount = keyCount;
            var addCount = 5;
            context.kvPairs = [];

            // Populate Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            var primaryLastSaveTime = context.clusterTestUtils.LastSave(0, logger: context.logger);
            var replicaLastSaveTime = context.clusterTestUtils.LastSave(1, logger: context.logger);
            context.clusterTestUtils.Checkpoint(0, logger: context.logger);

            // Populate Primary
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);

            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.clusterTestUtils.WaitCheckpoint(0, primaryLastSaveTime, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(1, replicaLastSaveTime, logger: context.logger);

            // Shutdown secondary
            context.nodes[1].Dispose(false);
            Thread.Sleep(TimeSpan.FromSeconds(2));

            // New insert
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            // Restart secondary
            context.nodes[1] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(1),
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false,
                asyncReplay: asyncReplay);
            context.nodes[1].Start();
            context.CreateConnection(useTLS: useTLS);

            for (int i = 1; i < replica_count; i++) context.clusterTestUtils.WaitForReplicaRecovery(i, context.logger);
            context.clusterTestUtils.WaitForConnectedReplicaCount(0, replica_count, context.logger);

            // Validate synchronization was success
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
            => ClusterSRPrimaryCheckpointRetrieve(performRMW: performRMW, disableObjects: disableObjects, lowMemory: lowMemory, manySegments: manySegments, false, false);

        void ClusterSRPrimaryCheckpointRetrieve(bool performRMW, bool disableObjects, bool lowMemory, bool manySegments, bool disableStorageTier, bool incrementalSnapshots)
        {
            // Test many segments on or off with lowMemory
            manySegments = lowMemory && manySegments;

            var primaryIndex = 0;
            var replicaIndex = 1;
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + primary_count * replica_count;
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, lowMemory: lowMemory, segmentSize: manySegments ? "4k" : "1g", DisableStorageTier: disableStorageTier, EnableIncrementalSnapshots: incrementalSnapshots, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var keyLength = 32;
            var kvpairCount = disableStorageTier ? 16 : keyCount;
            var addCount = 5;
            context.kvPairs = [];
            context.kvPairsObj = [];

            context.logger?.LogTrace("Test disposing node 1");
            context.nodes[1].Dispose(false);
            Thread.Sleep(TimeSpan.FromSeconds(1));

            // Populate Primary
            if (disableObjects)
            {
                if (!performRMW)
                    context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex, null, incrementalSnapshots, primaryIndex);
                else
                    context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryIndex, addCount, null, incrementalSnapshots, primaryIndex);
            }
            else
            {
                if (disableStorageTier)
                    context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, primaryIndex, 1, 8);
                else
                    context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, primaryIndex);
            }

            context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);

            // Restart secondary
            context.nodes[replicaIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(replicaIndex),
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false,
                lowMemory: lowMemory,
                SegmentSize: manySegments ? "4k" : "1g",
                DisableStorageTier: disableStorageTier,
                asyncReplay: asyncReplay);
            context.nodes[replicaIndex].Start();
            context.CreateConnection(useTLS: useTLS);

            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            if (disableObjects)
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex);
            else
                context.ValidateNodeObjects(ref context.kvPairsObj, replicaIndex);
        }

        [Test, Order(7)]
        [Category("REPLICATION")]
        public void ClusterSRAddReplicaAfterPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects, [Values] bool lowMemory)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: disableObjects, lowMemory: lowMemory, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);

            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true, context.logger));
            context.clusterTestUtils.BumpEpoch(0, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var keyLength = 32;
            var kvpairCount = keyCount;
            var addCount = 5;
            context.kvPairs = [];
            context.kvPairsObj = [];

            // Populate Primary
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

            // Add new replica node
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            context.clusterTestUtils.Meet(1, 0, logger: context.logger);
            context.clusterTestUtils.WaitAll(context.logger);
            _ = context.clusterTestUtils.ClusterReplicate(1, primaryId, async: false, logger: context.logger);
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
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);

            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true, context.logger));
            context.clusterTestUtils.BumpEpoch(0, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var keyLength = 32;
            var kvpairCount = keyCount;
            var addCount = 5;
            Dictionary<string, int> kvPairs = [];

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

            // Restart Primary
            context.nodes[0] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(0),
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false,
                asyncReplay: asyncReplay);
            context.nodes[0].Start();
            context.CreateConnection(useTLS: useTLS);

            var storeRecoveredAofAddress = context.clusterTestUtils.GetStoreRecoveredAofAddress(0, context.logger);
            long objectStoreRecoveredAofAddress = -1;
            if (!disableObjects)
                objectStoreRecoveredAofAddress = context.clusterTestUtils.GetObjectStoreRecoveredAofAddress(0, logger: context.logger);

            ClassicAssert.AreEqual(storeCurrentAofAddress, storeRecoveredAofAddress);
            if (!disableObjects)
                ClassicAssert.AreEqual(objectStoreCurrentAofAddress, objectStoreRecoveredAofAddress);
        }

        [Test, Order(9)]
        [Category("REPLICATION")]
        public void ClusterSRRedirectWrites()
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);

            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var resp = context.clusterTestUtils.SetKey(1, Encoding.ASCII.GetBytes("testKey"), Encoding.ASCII.GetBytes("testValue"), out _, out _, logger: context.logger);
            ClassicAssert.AreEqual(ResponseState.MOVED, resp);
        }

        [Test, Order(10)]
        [Category("REPLICATION")]
        public void ClusterSRReplicaOfTest([Values] bool performRMW)
        {
            var nodes_count = 2;
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: true, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);

            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], true, context.logger));

            context.clusterTestUtils.SetConfigEpoch(0, 1, context.logger);
            context.clusterTestUtils.SetConfigEpoch(1, 2, context.logger);

            var configEpoch = context.clusterTestUtils.GetConfigEpoch(0, context.logger);
            ClassicAssert.AreEqual(1, configEpoch);

            configEpoch = context.clusterTestUtils.GetConfigEpoch(1, context.logger);
            ClassicAssert.AreEqual(2, configEpoch);

            context.clusterTestUtils.Meet(0, 1, logger: context.logger);
            context.clusterTestUtils.WaitClusterNodesSync(syncOnNodeIndex: 0, count: 2, context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(1, 0, logger: context.logger);

            var keyLength = 32;
            var kvpairCount = keyCount;
            var addCount = 5;
            context.kvPairs = [];
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
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var primaryIndex = 0;
            var replicaIndex = 1;
            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var keyLength = 32;
            var kvpairCount = 16;
            var addCount = 10;
            context.kvPairs = [];

            // Populate Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryIndex, addCount);

            // Wait for replication offsets to synchronize
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex);

            if (checkpoint)
            {
                var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
                var replicaLastSaveTime = context.clusterTestUtils.LastSave(replicaIndex, logger: context.logger);
                context.clusterTestUtils.Checkpoint(primaryIndex);
                context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(replicaIndex, replicaLastSaveTime, logger: context.logger);
                context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            }

            #region InitiateFailover
            var slotMap = new int[16384];
            // Failover primary
            _ = context.clusterTestUtils.ClusterFailover(1, logger: context.logger);

            // Reconfigure slotMap to reflect new primary
            for (var i = 0; i < 16384; i++)
                slotMap[i] = 1;
            #endregion

            // Wait for attaching primary to finish
            context.clusterTestUtils.WaitForNoFailover(replicaIndex, logger: context.logger);
            context.clusterTestUtils.WaitForFailoverCompleted(replicaIndex, logger: context.logger);
            // Enable when old primary becomes replica
            context.clusterTestUtils.WaitForReplicaRecovery(primaryIndex, logger: context.logger);

            // Check if allowed to write to new Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, replicaIndex, slotMap: slotMap);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, replicaIndex, addCount, slotMap: slotMap);
        }

        [Test, Order(12)]
        [Category("REPLICATION")]
        public void ClusterFailoverAttachReplicas([Values] bool performRMW, [Values] bool takePrimaryCheckpoint, [Values] bool takeNewPrimaryCheckpoint, [Values] bool enableIncrementalSnapshots)
        {
            var replica_count = 2; // Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, EnableIncrementalSnapshots: enableIncrementalSnapshots, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var primary = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", primary.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, primary.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var rconfig1 = context.clusterTestUtils.ClusterNodes(1, context.logger);
            var rconfig2 = context.clusterTestUtils.ClusterNodes(2, context.logger);
            ClassicAssert.AreEqual(primary.NodeId, rconfig1.Nodes.First().ParentNodeId);
            ClassicAssert.AreEqual(primary.NodeId, rconfig2.Nodes.First().ParentNodeId);

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var keyLength = 32;
            var kvpairCount = keyCount;
            var addCount = 5;
            context.kvPairs = [];

            // Populate Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, 0, addCount);

            if (takePrimaryCheckpoint)
            {
                var primaryLastSaveTime = context.clusterTestUtils.LastSave(0, logger: context.logger);
                context.clusterTestUtils.Checkpoint(0, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(0, primaryLastSaveTime, logger: context.logger);
            }

            // Wait for replication offsets to synchronize
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 2, context.logger);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);

            // Simulate primary crash
            context.nodes[0].Dispose();
            context.nodes[0] = null;

            // Takeover as new primary
            _ = context.clusterTestUtils.ClusterFailover(1, "TAKEOVER", logger: context.logger);

            // Wait for both nodes to enter no failover
            context.clusterTestUtils.WaitForNoFailover(1, context.logger);
            context.clusterTestUtils.WaitForNoFailover(2, context.logger);

            // Wait for replica to recover
            context.clusterTestUtils.WaitForReplicaRecovery(2, context.logger);

            if (takeNewPrimaryCheckpoint)
            {
                var newPrimaryLastSaveTime = context.clusterTestUtils.LastSave(1, logger: context.logger);
                context.clusterTestUtils.Checkpoint(1, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(1, newPrimaryLastSaveTime, logger: context.logger);
            }
            context.clusterTestUtils.WaitForReplicaAofSync(1, 2, context.logger);

            // Check if replica 2 after failover contains keys
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 2, primaryIndex: 1);

            // Send extra keys to primary, verify replica gets the keys
            // This also ensures that the test does not end while failover AOF sync connection is in progress
            context.SendAndValidateKeys(1, 2, keyLength, 5);
        }

        [Test, Order(13)]
        [Category("REPLICATION")]
        public void ClusterReplicationCheckpointCleanupTest([Values] bool performRMW, [Values] bool disableObjects, [Values] bool enableIncrementalSnapshots)
        {
            var replica_count = 1;//Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: disableObjects, lowMemory: true, segmentSize: "4k", EnableIncrementalSnapshots: enableIncrementalSnapshots, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], true, context.logger));
            context.clusterTestUtils.BumpEpoch(0, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(0, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            context.kvPairs = [];
            context.kvPairsObj = [];
            context.checkpointTask = Task.Run(() => context.PopulatePrimaryAndTakeCheckpointTask(performRMW, disableObjects, takeCheckpoint: true));
            var attachReplicaTask = Task.Run(() => context.AttachAndWaitForSync(primary_count, replica_count, disableObjects));

            if (!context.checkpointTask.Wait(TimeSpan.FromSeconds(30)))
                Assert.Fail("checkpointTask timeout");

            if (!attachReplicaTask.Wait(TimeSpan.FromSeconds(30)))
                Assert.Fail("attachReplicaTask timeout");

            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex: 0, secondaryIndex: 1, logger: context.logger);
        }

        [Test, Order(14)]
        [Category("REPLICATION")]
        public void ClusterMainMemoryReplicationAttachReplicas()
        {
            var replica_count = 2; // Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, FastAofTruncate: true, OnDemandCheckpoint: true, CommitFrequencyMs: -1, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);

            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true));
            context.clusterTestUtils.SetConfigEpoch(0, 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(1, 2, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(2, 3, logger: context.logger);

            for (var i = 1; i < nodes_count; i++) context.clusterTestUtils.Meet(0, i);

            for (var i = 0; i < nodes_count; i++)
                for (var j = 0; j < nodes_count; j++)
                    if (i != j) context.clusterTestUtils.WaitUntilNodeIsKnown(i, j, context.logger);

            context.kvPairs = [];
            context.kvPairsObj = [];
            var task = Task.Run(()
                => context.PopulatePrimaryAndTakeCheckpointTask(performRMW: false, disableObjects: true, takeCheckpoint: false, iter: 10));

            var primaryId = context.clusterTestUtils.ClusterMyId(0, context.logger);
            _ = context.clusterTestUtils.ClusterReplicate(1, primaryId, async: true, logger: context.logger);
            _ = context.clusterTestUtils.ClusterReplicate(2, primaryId, async: true, logger: context.logger);

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
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, FastAofTruncate: MainMemoryReplication, OnDemandCheckpoint: onDemandCheckpoint, CommitFrequencyMs: -1, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);

            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, new List<(int, int)>() { (0, 16383) }, true, context.logger));
            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaNodeIndex, 2, logger: context.logger);
            context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryNodeIndex, replicaNodeIndex, logger: context.logger);

            var replicaId = context.clusterTestUtils.ClusterMyId(replicaNodeIndex, logger: context.logger);
            _ = context.clusterTestUtils.ClusterForget(primaryNodeIndex, replicaId, 5, logger: context.logger);

            var primaryId = context.clusterTestUtils.ClusterMyId(primaryNodeIndex, logger: context.logger);
            string resp;
            if (!useReplicaOf)
                resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryId, failEx: false, logger: context.logger);
            else
                resp = context.clusterTestUtils.ReplicaOf(replicaNodeIndex, primaryNodeIndex, failEx: false, logger: context.logger);
            ClassicAssert.IsTrue(resp.StartsWith("PRIMARY-ERR"));

            while (true)
            {
                context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
                context.clusterTestUtils.BumpEpoch(replicaNodeIndex, logger: context.logger);
                var config = context.clusterTestUtils.ClusterNodes(primaryNodeIndex, logger: context.logger);
                if (config.Nodes.Count == 2) break;
                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token);
            }

            _ = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryId, logger: context.logger);

            context.kvPairs = [];
            var keyLength = 32;
            var kvpairCount = keyCount;
            var addCount = 5;
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryNodeIndex);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryNodeIndex, addCount);

            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaNodeIndex);
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
            var set = false;
            var replica_count = 2;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, FastAofTruncate: mainMemoryReplication, CommitFrequencyMs: mainMemoryReplication ? -1 : 0, OnDemandCheckpoint: mainMemoryReplication, FastCommit: fastCommit, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var oldPrimaryIndex = 0;
            var newPrimaryIndex = 1;
            var replicaIndex = 2;

            var keyLength = 8;
            var kvpairCount = 16;
            var addCount = 5;
            context.kvPairs = [];
            context.kvPairsObj = [];

            // Populate Primary
            if (disableObjects)
            {
                if (!performRMW) context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex);
                else context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, addCount);
            }
            else context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, set: set);

            // Take a checkpoint at the original primary
            if (ckptBeforeDivergence)
            {
                var oldPrimaryLastSaveTime = context.clusterTestUtils.LastSave(oldPrimaryIndex, logger: context.logger);
                var newPrimaryLastSaveTime = context.clusterTestUtils.LastSave(newPrimaryIndex, logger: context.logger);
                var replicaLastSaveTime = context.clusterTestUtils.LastSave(replicaIndex, logger: context.logger);
                context.clusterTestUtils.Checkpoint(oldPrimaryIndex, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(oldPrimaryIndex, oldPrimaryLastSaveTime, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(newPrimaryIndex, newPrimaryLastSaveTime, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(replicaIndex, replicaLastSaveTime, logger: context.logger);
            }

            // Wait for replicas to catch up
            context.clusterTestUtils.WaitForReplicaAofSync(oldPrimaryIndex, newPrimaryIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(oldPrimaryIndex, replicaIndex, context.logger);

            // Make this replica of no-one
            _ = context.clusterTestUtils.ReplicaOf(newPrimaryIndex, logger: context.logger);

            // Populate primary to diverge from replica 1 history
            // Use temporary dictionary to populate values lost to replica 1
            Dictionary<string, int> kvPairs2 = [];
            Dictionary<string, List<int>> kvPairsObj2 = [];
            if (disableObjects)
            {
                if (!performRMW) context.PopulatePrimary(ref kvPairs2, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex);
                else context.PopulatePrimaryRMW(ref kvPairs2, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, addCount);
            }
            else context.PopulatePrimaryWithObjects(ref kvPairsObj2, keyLength, kvpairCount, primaryIndex: oldPrimaryIndex, set: set);

            // Take multiple checkpoints after divergence
            if (multiCheckpointAfterDivergence)
            {
                var count = 2;
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

            // Dispose primary
            context.nodes[oldPrimaryIndex].Dispose(false);
            context.nodes[oldPrimaryIndex] = null;

            // Re-assign slots to replica manually since failover option was not            
            _ = context.clusterTestUtils.AddDelSlotsRange(newPrimaryIndex, [(0, 16383)], addslot: false, context.logger);
            _ = context.clusterTestUtils.AddDelSlotsRange(replicaIndex, [(0, 16383)], addslot: false, context.logger);
            _ = context.clusterTestUtils.AddDelSlotsRange(newPrimaryIndex, [(0, 16383)], addslot: true, context.logger);
            context.clusterTestUtils.BumpEpoch(newPrimaryIndex, logger: context.logger);

            // New primary diverges to its own history by new random seed
            kvpairCount <<= 1;
            if (disableObjects)
            {
                if (!performRMW) context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: newPrimaryIndex, randomSeed: 1234);
                else context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryIndex: newPrimaryIndex, addCount, randomSeed: 1234);
            }
            else
                context.PopulatePrimaryWithObjects(ref context.kvPairsObj, keyLength, kvpairCount, primaryIndex: newPrimaryIndex, randomSeed: 1234, set: set);

            if (!ckptBeforeDivergence || multiCheckpointAfterDivergence) context.clusterTestUtils.Checkpoint(newPrimaryIndex, logger: context.logger);

            var newPrimaryId = context.clusterTestUtils.ClusterMyId(newPrimaryIndex, context.logger);
            while (true)
            {
                var replicaConfig = context.clusterTestUtils.ClusterNodes(replicaIndex, context.logger);
                var clusterNode = replicaConfig.GetBySlot(0);
                if (clusterNode != null && clusterNode.NodeId.Equals(newPrimaryId))
                    break;
                _ = Thread.Yield();
            }

            var resp = context.clusterTestUtils.ReplicaOf(replicaIndex, newPrimaryIndex, failEx: false, logger: context.logger);
            // Retry to avoid lock error
            while (string.IsNullOrEmpty(resp) || !resp.Equals("OK"))
            {
                ClusterTestUtils.BackOff(cancellationToken: context.cts.Token);
                resp = context.clusterTestUtils.ReplicaOf(replicaIndex, newPrimaryIndex, failEx: false, logger: context.logger);
            }
            context.clusterTestUtils.WaitForReplicaRecovery(replicaIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(newPrimaryIndex, replicaIndex, context.logger);

            // Check if replica 2 after failover contains keys
            if (disableObjects)
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex: replicaIndex, primaryIndex: newPrimaryIndex);
            else
                context.ValidateNodeObjects(ref context.kvPairsObj, replicaIndex: newPrimaryIndex, set: set);
        }

        [Test, Order(21)]
        [Category("REPLICATION")]
        public void ClusterReplicateFails()
        {
            const string UserName = "temp-user";
            const string Password = "temp-password";

            const string ClusterUserName = "cluster-user";
            const string ClusterPassword = "cluster-password";

            // Setup a cluster (mimicking the style in which this bug was first found)
            ServerCredential clusterCreds = new(ClusterUserName, ClusterPassword, IsAdmin: true, UsedForClusterAuth: true, IsClearText: true);
            ServerCredential userCreds = new(UserName, Password, IsAdmin: true, UsedForClusterAuth: false, IsClearText: true);

            context.GenerateCredentials([userCreds, clusterCreds]);
            context.CreateInstances(2, disableObjects: true, disablePubSub: true, enableAOF: true, clusterCreds: clusterCreds, useAcl: true, FastAofTruncate: true, CommitFrequencyMs: -1, asyncReplay: asyncReplay);
            var primaryEndpoint = (IPEndPoint)context.endpoints.First();
            var replicaEndpoint = (IPEndPoint)context.endpoints.Last();

            ClassicAssert.AreNotEqual(primaryEndpoint, replicaEndpoint, "Should have different endpoints for nodes");

            using var primaryConnection = ConnectionMultiplexer.Connect($"{primaryEndpoint.Address}:{primaryEndpoint.Port},user={UserName},password={Password}");
            var primaryServer = primaryConnection.GetServer(primaryEndpoint);

            ClassicAssert.AreEqual("OK", (string)primaryServer.Execute("CLUSTER", ["ADDSLOTSRANGE", "0", "16383"], flags: CommandFlags.NoRedirect));
            ClassicAssert.AreEqual("OK", (string)primaryServer.Execute("CLUSTER", ["MEET", replicaEndpoint.Address.ToString(), replicaEndpoint.Port.ToString()], flags: CommandFlags.NoRedirect));

            using var replicaConnection = ConnectionMultiplexer.Connect($"{replicaEndpoint.Address}:{replicaEndpoint.Port},user={UserName},password={Password}");
            var replicaServer = replicaConnection.GetServer(replicaEndpoint);

            // Try to replicate from a server that doesn't exist
            var exc = Assert.Throws<RedisServerException>(() => replicaServer.Execute("CLUSTER", ["REPLICATE", Guid.NewGuid().ToString()], flags: CommandFlags.NoRedirect));
            ClassicAssert.IsTrue(exc.Message.StartsWith("ERR I don't know about node "));
        }

        [Test, Order(22)]
        [Category("REPLICATION")]
        public void ClusterReplicationCheckpointAlignmentTest([Values] bool performRMW)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: false, FastAofTruncate: true, CommitFrequencyMs: -1, OnDemandCheckpoint: true, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay);
            context.CreateConnection(useTLS: useTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var keyLength = 32;
            var kvpairCount = keyCount;
            var addCount = 5;
            context.kvPairs = [];

            // Populate Primary
            if (!performRMW)
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryNodeIndex);
            else
                context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryNodeIndex, addCount);

            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);
            for (var i = 0; i < 5; i++)
            {
                var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryNodeIndex, logger: context.logger);
                var replicaLastSaveTime = context.clusterTestUtils.LastSave(replicaNodeIndex, logger: context.logger);
                context.clusterTestUtils.Checkpoint(primaryNodeIndex);
                context.clusterTestUtils.WaitCheckpoint(primaryNodeIndex, primaryLastSaveTime, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(replicaNodeIndex, replicaLastSaveTime, logger: context.logger);
                context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);
            }

            var primaryVersion = context.clusterTestUtils.GetInfo(primaryNodeIndex, "store", "CurrentVersion", logger: context.logger);
            var replicaVersion = context.clusterTestUtils.GetInfo(replicaNodeIndex, "store", "CurrentVersion", logger: context.logger);
            ClassicAssert.AreEqual("6", primaryVersion);
            ClassicAssert.AreEqual(primaryVersion, replicaVersion);

            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaNodeIndex);

            // Dispose primary and delete data
            context.nodes[primaryNodeIndex].Dispose(true);
            // Dispose primary but do not delete data
            context.nodes[replicaNodeIndex].Dispose(false);

            // Restart primary and do not recover
            context.nodes[primaryNodeIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(primaryNodeIndex),
                disableObjects: true,
                tryRecover: false,
                enableAOF: true,
                FastAofTruncate: true,
                CommitFrequencyMs: -1,
                OnDemandCheckpoint: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: true,
                asyncReplay: asyncReplay);
            context.nodes[primaryNodeIndex].Start();

            // Restart secondary and recover
            context.nodes[replicaNodeIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(replicaNodeIndex),
                disableObjects: true,
                tryRecover: true,
                enableAOF: true,
                FastAofTruncate: true,
                CommitFrequencyMs: -1,
                OnDemandCheckpoint: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: true,
                asyncReplay: asyncReplay);
            context.nodes[replicaNodeIndex].Start();
            context.CreateConnection(useTLS: useTLS);

            // Assert primary version is 1 and replica has recovered to previous checkpoint
            primaryVersion = context.clusterTestUtils.GetInfo(primaryNodeIndex, "store", "CurrentVersion", logger: context.logger);
            replicaVersion = context.clusterTestUtils.GetInfo(replicaNodeIndex, "store", "CurrentVersion", logger: context.logger);
            ClassicAssert.AreEqual("1", primaryVersion);
            ClassicAssert.AreEqual("6", replicaVersion);

            // Setup cluster
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger));
            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaNodeIndex, replicaNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            var primaryNodeId = context.clusterTestUtils.ClusterMyId(primaryNodeIndex, logger: context.logger);

            // Enable replication
            context.clusterTestUtils.WaitUntilNodeIdIsKnown(replicaNodeIndex, primaryNodeId, logger: context.logger);
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryNodeIndex, logger: context.logger));

            // Both nodes are at version 1 despite replica recovering to version earlier
            primaryVersion = context.clusterTestUtils.GetInfo(primaryNodeIndex, "store", "CurrentVersion", logger: context.logger);
            replicaVersion = context.clusterTestUtils.GetInfo(replicaNodeIndex, "store", "CurrentVersion", logger: context.logger);
            ClassicAssert.AreEqual("1", primaryVersion);
            ClassicAssert.AreEqual("1", replicaVersion);

            // At this point attached replica should be empty because primary did not have any data because it did not recover
            foreach (var pair in context.kvPairs)
            {
                var resp = context.clusterTestUtils.GetKey(replicaNodeIndex, Encoding.ASCII.GetBytes(pair.Key), out _, out _, out var state, logger: context.logger);
                ClassicAssert.IsNull(resp);
            }
        }

        [Test, Order(24)]
        [Category("REPLICATION")]
        public void ClusterReplicationLua([Values] bool luaTransactionMode)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: false, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, enableLua: true, luaTransactionMode: luaTransactionMode);
            context.CreateConnection(useTLS: useTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            _ = primaryServer.Execute("EVAL", "redis.call('SET', KEYS[1], ARGV[1])", "1", "foo", "bar");
            _ = primaryServer.Execute("EVAL", "redis.call('SET', KEYS[1], ARGV[1])", "1", "fizz", "buzz");

            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);

            var replicaServer = context.clusterTestUtils.GetServer(replicaNodeIndex);
            var res1 = (string)replicaServer.Execute("EVAL", "return redis.call('GET', KEYS[1])", "1", "foo");
            var res2 = (string)replicaServer.Execute("EVAL", "return redis.call('GET', KEYS[1])", "1", "fizz");

            ClassicAssert.AreEqual("bar", res1);
            ClassicAssert.AreEqual("buzz", res2);
        }
    }
}