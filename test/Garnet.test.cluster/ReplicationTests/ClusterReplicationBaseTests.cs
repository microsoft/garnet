// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Internal;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [NonParallelizable]
    public class ClusterReplicationBaseTests
    {
        public (Action, string)[] GetUnitTests()
        {
            List<(Action, string)> testList = [
                new(() => ClusterSRTest(true), "ClusterSRTest(true)"),
                new(() => ClusterSRTest(false), "ClusterSRTest(false)"),
                new(() => ClusterSRNoCheckpointRestartSecondary(false, false), "ClusterSRNoCheckpointRestartSecondary(false, false)"),
                new(() => ClusterSRNoCheckpointRestartSecondary(false, true), "ClusterSRNoCheckpointRestartSecondary(false, true)"),
                new(() => ClusterSRNoCheckpointRestartSecondary(true, false), "ClusterSRNoCheckpointRestartSecondary(true, false)"),
                new(() => ClusterSRNoCheckpointRestartSecondary(true, true), "ClusterSRNoCheckpointRestartSecondary(true, true)"),
                new(() => ClusterSRPrimaryCheckpoint(false, false), "ClusterSRPrimaryCheckpoint(false, false)"),
                new(() => ClusterSRPrimaryCheckpoint(false, true), "ClusterSRPrimaryCheckpoint(false, true)"),
                new(() => ClusterSRPrimaryCheckpoint(true, false), "ClusterSRPrimaryCheckpoint(true, false)"),
                new(() => ClusterSRPrimaryCheckpoint(true, true), "ClusterSRPrimaryCheckpoint(true, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, false, false), "ClusterSRPrimaryCheckpointRetrieve(false, false, false, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, false, true), "ClusterSRPrimaryCheckpointRetrieve(false, false, false, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, false, false), "ClusterSRPrimaryCheckpointRetrieve(false, true, false, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, false, true), "ClusterSRPrimaryCheckpointRetrieve(false, true, false, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, false, false), "ClusterSRPrimaryCheckpointRetrieve(true, false, false, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, false, true), "ClusterSRPrimaryCheckpointRetrieve(true, false, false, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, false, false), "ClusterSRPrimaryCheckpointRetrieve(true, true, false, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, false, true), "ClusterSRPrimaryCheckpointRetrieve(true, true, false, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, true, false), "ClusterSRPrimaryCheckpointRetrieve(false, false, true, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, true, true), "ClusterSRPrimaryCheckpointRetrieve(false, false, true, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, true, false), "ClusterSRPrimaryCheckpointRetrieve(false, true, true, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, true, true), "ClusterSRPrimaryCheckpointRetrieve(false, true, true, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, true, false), "ClusterSRPrimaryCheckpointRetrieve(true, false, true, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, true, true), "ClusterSRPrimaryCheckpointRetrieve(true, false, true, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, true, false), "ClusterSRPrimaryCheckpointRetrieve(true, true, true, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, true, true), "ClusterSRPrimaryCheckpointRetrieve(true, true, true, true)"),
                new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(false, false, false), "ClusterSRAddReplicaAfterPrimaryCheckpoint(false, false, false)"),
                new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(false, false, true), "ClusterSRAddReplicaAfterPrimaryCheckpoint(false, false, true)"),
                new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(false, true, false), "ClusterSRAddReplicaAfterPrimaryCheckpoint(false, true, false)"),
                new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(false, true, true), "ClusterSRAddReplicaAfterPrimaryCheckpoint(false, true, true)"),
                new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(true, false, false), "ClusterSRAddReplicaAfterPrimaryCheckpoint(true, false, false)"),
                new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(true, false, true), "ClusterSRAddReplicaAfterPrimaryCheckpoint(true, false, true)"),
                new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(true, true, false), "ClusterSRAddReplicaAfterPrimaryCheckpoint(true, true, false)"),
                new(() => ClusterSRAddReplicaAfterPrimaryCheckpoint(true, true, true), "ClusterSRAddReplicaAfterPrimaryCheckpoint(true, true, true)"),
                new(() => ClusterSRPrimaryRestart(false, false), "ClusterSRPrimaryRestart(false, false)"),
                new(() => ClusterSRPrimaryRestart(false, true), "ClusterSRPrimaryRestart(false, true)"),
                new(() => ClusterSRPrimaryRestart(true, false), "ClusterSRPrimaryRestart(true, false)"),
                new(() => ClusterSRPrimaryRestart(true, true), "ClusterSRPrimaryRestart(true, true)"),
                new(ClusterSRRedirectWrites, "ClusterSRRedirectWrites()"),
                new(() => ClusterReplicationCheckpointCleanupTest(false, false, false), "ClusterReplicationCheckpointCleanupTest(false, false, false)"),
                new(() => ClusterReplicationCheckpointCleanupTest(false, false, true), "ClusterReplicationCheckpointCleanupTest(false, false, true)"),
                new(() => ClusterReplicationCheckpointCleanupTest(false, true, false), "ClusterReplicationCheckpointCleanupTest(false, true, false)"),
                new(() => ClusterReplicationCheckpointCleanupTest(false, true, true), "ClusterReplicationCheckpointCleanupTest(false, true, true)"),
                new(() => ClusterReplicationCheckpointCleanupTest(true, false, false), "ClusterReplicationCheckpointCleanupTest(true, false, false)"),
                new(() => ClusterReplicationCheckpointCleanupTest(true, false, true), "ClusterReplicationCheckpointCleanupTest(true, false, true)"),
                new(() => ClusterReplicationCheckpointCleanupTest(true, true, false), "ClusterReplicationCheckpointCleanupTest(true, true, false)"),
                new(() => ClusterReplicationCheckpointCleanupTest(true, true, true), "ClusterReplicationCheckpointCleanupTest(true, true, true)")
            ];
            return testList.ToArray();
        }

        protected ClusterTestContext context;

        public TextWriter LogTextWriter { get; set; }

        protected bool useTLS = false;
        protected bool asyncReplay = false;
        readonly int timeout = 60;
        protected int keyCount = 256;
        protected int sublogCount = 1;

        public Dictionary<string, LogLevel> monitorTests = new()
        {
            {"ClusterReplicationSimpleFailover", LogLevel.Warning},
            {"ClusterReplicationMultiRestartRecover", LogLevel.Trace},
            {"ClusterFailoverAttachReplicas", LogLevel.Error},
            {"ClusterReplicationSimpleTransactionTest", LogLevel.Trace}
        };

        [SetUp]
        public virtual void Setup()
        {
            context = new ClusterTestContext();
            if (LogTextWriter != null) context.logTextWriter = LogTextWriter;
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
            var replica_count = 1;
            var primary_count = 1;
            var nodes_count = 2;
            var primaryIndex = 0;
            var replicaIndex = 1;
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, sublogCount: sublogCount);
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
            context.kvPairsObj = [];

            //Populate Primary
            context.SimplePopulateDB(disableObjects, keyLength, kvpairCount, primaryIndex);

            // Wait for replica to sync
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex);

            // Validate database
            context.SimpleValidateDB(disableObjects, replicaIndex);
        }

        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterSRNoCheckpointRestartSecondary([Values] bool performRMW, [Values] bool disableObjects)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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
            context.kvPairsObj = [];

            // Populate Primary
            context.SimplePopulateDB(disableObjects, keyLength, kvpairCount, primaryIndex, performRMW: performRMW, addCount: addCount);

            // Wait for replication offsets to synchronize
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1);
            // Validate database
            context.SimpleValidateDB(disableObjects, replicaIndex);

            // Shutdown secondary
            context.nodes[1].Dispose(false);

            Thread.Sleep(TimeSpan.FromSeconds(2));

            // New insert
            context.SimplePopulateDB(disableObjects, keyLength, kvpairCount, primaryIndex, performRMW: performRMW, addCount: addCount);

            // Restart secondary
            context.nodes[1] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(1),
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false,
                sublogCount: sublogCount);
            context.nodes[1].Start();
            context.CreateConnection(useTLS: useTLS);

            // Validate synchronization was success
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1);
            // Validate database
            context.SimpleValidateDB(disableObjects, replicaIndex);
        }

        [Test, Order(3)]
        [Category("REPLICATION")]
        public void ClusterSRPrimaryCheckpoint([Values] bool performRMW, [Values] bool disableObjects)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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
            context.kvPairsObj = [];

            // Populate Primary
            context.SimplePopulateDB(disableObjects, keyLength, kvpairCount, primaryIndex, performRMW: performRMW, addCount: addCount);

            var primaryLastSaveTime = context.clusterTestUtils.LastSave(0, logger: context.logger);
            var replicaLastSaveTime = context.clusterTestUtils.LastSave(1, logger: context.logger);
            context.clusterTestUtils.Checkpoint(0, logger: context.logger);

            // Populate Primary
            context.SimplePopulateDB(disableObjects, keyLength, kvpairCount, primaryIndex, performRMW: performRMW, addCount: addCount);
            context.SimpleValidateDB(disableObjects, replicaIndex);

            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.clusterTestUtils.WaitCheckpoint(0, primaryLastSaveTime, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(1, replicaLastSaveTime, logger: context.logger);

            // Shutdown secondary
            context.nodes[1].Dispose(false);
            Thread.Sleep(TimeSpan.FromSeconds(2));

            // New insert
            context.SimplePopulateDB(disableObjects, keyLength, kvpairCount, primaryIndex, performRMW: performRMW, addCount: addCount);

            // Restart secondary
            context.nodes[1] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(1),
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false,
                asyncReplay: asyncReplay,
                sublogCount: sublogCount);
            context.nodes[1].Start();
            context.CreateConnection(useTLS: useTLS);

            for (var i = 1; i < replica_count; i++) context.clusterTestUtils.WaitForReplicaRecovery(i, context.logger);
            context.clusterTestUtils.WaitForConnectedReplicaCount(0, replica_count, context.logger);

            // Validate synchronization was success
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1);
            context.SimpleValidateDB(disableObjects, replicaIndex);
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
            context.CreateInstances(
                nodes_count,
                disableObjects: disableObjects,
                lowMemory: lowMemory,
                segmentSize: manySegments ? "4k" : "1g",
                DisableStorageTier: disableStorageTier,
                EnableIncrementalSnapshots: incrementalSnapshots,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                sublogCount: sublogCount);
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
                asyncReplay: asyncReplay,
                sublogCount: sublogCount);
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
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: disableObjects, lowMemory: lowMemory, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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
                asyncReplay: asyncReplay,
                sublogCount: sublogCount);
            context.nodes[0].Start();
            context.CreateConnection(useTLS: useTLS);

            var storeRecoveredAofAddress = context.clusterTestUtils.GetStoreRecoveredAofAddress(0, context.logger);
            ClassicAssert.AreEqual(storeCurrentAofAddress, storeRecoveredAofAddress);
        }

        [Test, Order(9)]
        [Category("REPLICATION")]
        public void ClusterSRRedirectWrites()
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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
            context.CreateInstances(nodes_count, tryRecover: true, disableObjects: true, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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

            var resp = context.clusterTestUtils.ReplicaOf(replicaNodeIndex: 1, primaryNodeIndex: 0, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);
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
            context.CreateInstances(nodes_count, disableObjects: true, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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

            context.clusterTestUtils.WaitForReplicaAofSync(replicaIndex, primaryIndex, context.logger);
        }

        [Test, Order(12)]
        [Category("REPLICATION")]
        public void ClusterFailoverAttachReplicas([Values] bool performRMW, [Values] bool takePrimaryCheckpoint, [Values] bool takeNewPrimaryCheckpoint, [Values] bool enableIncrementalSnapshots)
        {
            var replica_count = 2; // Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, EnableIncrementalSnapshots: enableIncrementalSnapshots, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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
            context.CreateInstances(
                nodes_count,
                tryRecover: true,
                disableObjects: disableObjects,
                lowMemory: true,
                segmentSize: "4k",
                EnableIncrementalSnapshots: enableIncrementalSnapshots,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                deviceType: Tsavorite.core.DeviceType.Native,
                sublogCount: sublogCount);
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

            if (!context.checkpointTask.Wait(TimeSpan.FromSeconds(60)))
                Assert.Fail("checkpointTask timeout");

            if (!attachReplicaTask.Wait(TimeSpan.FromSeconds(60)))
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
            context.CreateInstances(nodes_count, disableObjects: true, FastAofTruncate: true, OnDemandCheckpoint: true, CommitFrequencyMs: -1, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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
            context.CreateInstances(
                nodes_count,
                disableObjects: disableObjects,
                FastAofTruncate: mainMemoryReplication,
                CommitFrequencyMs: mainMemoryReplication ? -1 : 0,
                OnDemandCheckpoint: mainMemoryReplication,
                FastCommit: fastCommit,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                sublogCount: sublogCount);
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
            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                FastAofTruncate: true,
                CommitFrequencyMs: -1,
                OnDemandCheckpoint: true,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                sublogCount: sublogCount);
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
                asyncReplay: asyncReplay,
                sublogCount: sublogCount);
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
                asyncReplay: asyncReplay,
                sublogCount: sublogCount);
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

        [Test, Order(23)]
        [Category("REPLICATION")]
        public void ClusterReplicationLua([Values] bool luaTransactionMode)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;
            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                enableLua: true,
                luaTransactionMode: luaTransactionMode,
                sublogCount: sublogCount);
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

        [Test, Order(24)]
        [Category("REPLICATION")]
        [CancelAfter(30_000)]
        public void ClusterReplicationStoredProc([Values] bool enableDisklessSync, [Values] bool attachFirst)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;

            var expectedKeys = new[] { "X", "Y" };
            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                enableDisklessSync: enableDisklessSync,
                sublogCount: sublogCount);
            context.CreateConnection(useTLS: useTLS);

            var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            var replicaServer = context.clusterTestUtils.GetServer(replicaNodeIndex);

            // Register custom procedure
            context.nodes[primaryNodeIndex].Register.NewTransactionProc("RATELIMIT", () => new RateLimiterTxn(), new RespCommandsInfo { Arity = 4 });
            context.nodes[replicaNodeIndex].Register.NewTransactionProc("RATELIMIT", () => new RateLimiterTxn(), new RespCommandsInfo { Arity = 4 });

            // Setup cluster
            context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaNodeIndex, replicaNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaNodeIndex, primaryNodeIndex, logger: context.logger);

            if (attachFirst)
            {
                // Issue replicate
                context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryNodeIndex, logger: context.logger);
            }

            // Execute custom proc before replicat attach
            ExecuteRateLimit();

            if (!attachFirst)
            {
                // Issue replicate
                var attachResp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryNodeIndex, logger: context.logger);
                ClassicAssert.AreEqual("OK", attachResp);
            }

            // Validate primary keys
            var resp = primaryServer.Execute("KEYS", ["*"]);
            ClassicAssert.AreEqual(expectedKeys, (string[])resp);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);
            replicaServer = context.clusterTestUtils.GetServer(replicaNodeIndex);
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            while (true)
            {
                resp = replicaServer.Execute("KEYS", ["*"]);
                if (expectedKeys.Length == ((string[])resp).Length)
                    break;
                ClusterTestUtils.BackOff(cts.Token);
            }

            var actual = (string[])resp;
            Array.Sort(actual);
            ClassicAssert.AreEqual(expectedKeys, actual);

            void ExecuteRateLimit()
            {
                primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
                var resp = primaryServer.Execute("RATELIMIT", [expectedKeys[0], "1000000000", "1000000000"]);
                ClassicAssert.AreEqual("ALLOWED", (string)resp);
                resp = primaryServer.Execute("RATELIMIT", [expectedKeys[1], "1000000000", "1000000000"]);
                ClassicAssert.AreEqual("ALLOWED", (string)resp);
            }
        }

        [Test, Order(25)]
        [Category("REPLICATION")]
        public void ClusterReplicationManualCheckpointing()
        {
            // Use case here is, outside of the cluster, period COMMITAOFs are requested.
            // Done so in recovery scenarios, if a primary does NOT come back there's still confidence
            // a replica has recent data.
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + primary_count * replica_count;
            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: true,
                tryRecover: false,
                FastAofTruncate: true,
                CommitFrequencyMs: -1,
                sublogCount: sublogCount);
            context.CreateConnection(useTLS: true);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var primaryEndPoint = (IPEndPoint)context.endpoints[0];
            var replicaEndPoint = (IPEndPoint)context.endpoints[1];

            // Check cluster is in good state pre-commit
            {
                var firstVal = Guid.NewGuid().ToString();
                var setPrimaryRes = (string)context.clusterTestUtils.Execute(primaryEndPoint, "SET", ["test-key", firstVal]);
                ClassicAssert.AreEqual("OK", setPrimaryRes);
                var getPrimaryRes = (string)context.clusterTestUtils.Execute(primaryEndPoint, "GET", ["test-key"]);
                ClassicAssert.AreEqual(firstVal, getPrimaryRes);

                context.clusterTestUtils.WaitForReplicaAofSync(0, 1);

                var readonlyReplicaRes = (string)context.clusterTestUtils.Execute(replicaEndPoint, "READONLY", []);
                ClassicAssert.AreEqual("OK", readonlyReplicaRes);
                var getReplicaRes = (string)context.clusterTestUtils.Execute(replicaEndPoint, "GET", ["test-key"]);
                ClassicAssert.AreEqual(firstVal, getReplicaRes);
            }

            // Commit outside of cluster logic
            var primaryCommit = context.nodes[0].Store.CommitAOF();
            ClassicAssert.IsTrue(primaryCommit);
            var secondCommit = context.nodes[1].Store.CommitAOF();
            ClassicAssert.IsFalse(secondCommit);

            // Check cluster remains in good state
            {
                var secondVal = Guid.NewGuid().ToString();
                var setPrimaryRes = (string)context.clusterTestUtils.Execute(primaryEndPoint, "SET", ["test-key2", secondVal]);
                ClassicAssert.AreEqual("OK", setPrimaryRes);
                var getPrimaryRes = (string)context.clusterTestUtils.Execute(primaryEndPoint, "GET", ["test-key2"]);
                ClassicAssert.AreEqual(secondVal, getPrimaryRes);

                context.clusterTestUtils.WaitForReplicaAofSync(0, 1);

                var readonlyReplicaRes = (string)context.clusterTestUtils.Execute(replicaEndPoint, "READONLY", []);
                ClassicAssert.AreEqual("OK", readonlyReplicaRes);
                var getReplicaRes = (string)context.clusterTestUtils.Execute(replicaEndPoint, "GET", ["test-key2"]);
                ClassicAssert.AreEqual(secondVal, getReplicaRes);
            }
        }

        [Test, Order(26)]
        [Category("CLUSTER")]
        [CancelAfter(30_000)]
        [TestCase(ExceptionInjectionType.Divergent_AOF_Stream)]
        [TestCase(ExceptionInjectionType.Aof_Sync_Task_Consume)]
        public async Task ReplicaSyncTaskFaultsRecoverAsync(ExceptionInjectionType faultType, CancellationToken cancellation)
        {
            // Ensure that a fault in ReplicaSyncTask (on the primary, sourced from either side) doesn't leave a replica permanently desynced
            //
            // While a possible cause of this is something actually breaking on the replica (in which case, a retry won't matter)
            // that isn't the only possible cause

#if !DEBUG
            Assert.Ignore($"Depends on {nameof(ExceptionInjectionHelper)}, which is disabled in non-Debug builds");
#endif

            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + primary_count * replica_count;
            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: true,
                tryRecover: false,
                FastAofTruncate: true,
                CommitFrequencyMs: -1,
                clusterReplicationReestablishmentTimeout: 1,
                sublogCount: sublogCount);
            context.CreateConnection(useTLS: true);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            shards = context.clusterTestUtils.ClusterShards(0, context.logger);
            ClassicAssert.AreEqual(1, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            var primaryEndPoint = (IPEndPoint)context.endpoints[0];
            var replicaEndPoint = (IPEndPoint)context.endpoints[1];

            using var primaryInsertCancel = CancellationTokenSource.CreateLinkedTokenSource(cancellation);

            var keyCount = 0;

            var uniqueSuffix = Guid.NewGuid().ToString();

            var continuallyWriteToPrimaryTask =
                Task.Run(
                    async () =>
                    {
                        while (!primaryInsertCancel.IsCancellationRequested)
                        {
                            var setRes = (string)context.clusterTestUtils.Execute(primaryEndPoint, "SET", [$"test-key-{keyCount}", $"{keyCount}-{uniqueSuffix}"]);
                            ClassicAssert.AreEqual("OK", setRes);

                            keyCount++;

                            await Task.Delay(10);
                        }
                    },
                    cancellation
                );

            await Task.Delay(100, cancellation);

            // Force replica to continually fault
            ExceptionInjectionHelper.EnableException(faultType);

            // Give it enough time to die horribly
            await Task.Delay(100, cancellation);

            // Stop primary writes
            primaryInsertCancel.Cancel();
            await continuallyWriteToPrimaryTask;

            // Resolve fault on replica
            ExceptionInjectionHelper.DisableException(faultType);

            // Wait for sync to catch up
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, cancellation: cancellation);

            // Check that replica received all values
            var readonlyRes = (string)context.clusterTestUtils.Execute(replicaEndPoint, "READONLY", []);
            ClassicAssert.AreEqual("OK", readonlyRes);

            for (var i = 0; i < keyCount; i++)
            {
                var getRes = (string)context.clusterTestUtils.Execute(replicaEndPoint, "GET", [$"test-key-{i}"]);
                ClassicAssert.AreEqual($"{i}-{uniqueSuffix}", getRes);

            }
        }

        [Test, Order(27)]
        [Category("REPLICATION")]
        [CancelAfter(30_000)]
        public async Task ClusterReplicationMultiRestartRecover(CancellationToken cancellationToken)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;

            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                cleanClusterConfig: false,
                sublogCount: sublogCount);
            context.CreateConnection(useTLS: useTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            var replicaServer = context.clusterTestUtils.GetServer(replicaNodeIndex);
            var keyCount = 256;

            var taskCount = 4;
            var tasks = new List<Task>();
            for (var i = 0; i < taskCount; i++)
                tasks.Add(Task.Run(() => RunWorkload(i * keyCount, (i + 1) * keyCount)));
            var restartRecover = 4;
            tasks.Add(Task.Run(() => RestartRecover(restartRecover)));

            await Task.WhenAll(tasks);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger, cancellationToken);

            // Validate that replica has the same keys as primary
            ValidateKeys();

            // Run write workload at primary
            void RunWorkload(int start, int count)
            {
                var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
                var end = start + count;
                while (start++ < end)
                {
                    var key = start.ToString();
                    var resp = primaryServer.Execute("SET", [key, key]);
                    ClassicAssert.AreEqual("OK", (string)resp);
                    resp = primaryServer.Execute("GET", key);
                    ClassicAssert.AreEqual(key, (string)resp);
                }
            }

            // Restart and recover replica multiple times
            async Task RestartRecover(int iteration)
            {
                while (iteration-- > 0)
                {
                    context.nodes[replicaNodeIndex].Dispose(false);

                    var items = context.clusterTestUtils.GetReplicationInfo(
                        primaryNodeIndex,
                        [ReplicationInfoItem.CONNECTED_REPLICAS, ReplicationInfoItem.SYNC_DRIVER_COUNT],
                        context.logger);
                    while (!items[0].Item2.Equals("0") || !items[1].Item2.Equals("0"))
                    {
                        items = context.clusterTestUtils.GetReplicationInfo(
                            primaryNodeIndex,
                            [ReplicationInfoItem.CONNECTED_REPLICAS, ReplicationInfoItem.SYNC_DRIVER_COUNT],
                            context.logger);
                        if (cancellationToken.IsCancellationRequested)
                            Assert.Fail("Failed waiting for primary aof sync cleanup!");
                        await Task.Yield();
                    }


                    context.nodes[replicaNodeIndex] = context.CreateInstance(
                        context.clusterTestUtils.GetEndPoint(replicaNodeIndex),
                        disableObjects: false,
                        enableAOF: true,
                        useTLS: useTLS,
                        asyncReplay: asyncReplay,
                        tryRecover: true,
                        cleanClusterConfig: false,
                        sublogCount: sublogCount);
                    context.nodes[replicaNodeIndex].Start();

                    await Task.Yield();
                }
            }

            void ValidateKeys()
            {
                var resp = (string[])primaryServer.Execute("KEYS", ["*"]);
                var resp2 = (string[])replicaServer.Execute("KEYS", ["*"]);
                ClassicAssert.AreEqual(resp.Length, resp2.Length);
                Array.Sort(resp);
                Array.Sort(resp2);
                for (var i = 0; i < resp.Length; i++)
                    ClassicAssert.AreEqual(resp[i], resp2[i]);
            }
        }

        [Test, Order(28)]
        [Category("CLUSTER")]
        [CancelAfter(30_000)]
        public async Task ReplicasRestartAsReplicasAsync(CancellationToken cancellation)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + primary_count * replica_count;
            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: true,
                tryRecover: false,
                FastAofTruncate: true,
                CommitFrequencyMs: -1,
                clusterReplicationReestablishmentTimeout: 1,
                sublogCount: sublogCount);
            context.CreateConnection(useTLS: true);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            //while (sublogCount > 0) { }
            IPEndPoint primary = (IPEndPoint)context.endpoints[0];
            IPEndPoint replica = (IPEndPoint)context.endpoints[1];

            // Make sure role assignment is as expected
            ClassicAssert.AreEqual("master", context.clusterTestUtils.RoleCommand(primary).Value);
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(replica).Value);

            context.ShutdownNode(primary);
            context.ShutdownNode(replica);

            // Intentionally leaving primary offline
            context.RestartNode(replica);

            // Delay a bit for replication init tasks to fire off
            await Task.Delay(100, cancellation);

            // Make sure replica did not promote to Primary
            ClassicAssert.AreEqual("slave", context.clusterTestUtils.RoleCommand(replica).Value);
        }

        [Test, Order(29)]
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
                clusterReplicaResumeWithData: true,
                sublogCount: sublogCount
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

                                    await Task.Delay(10, writeTaskCancel.Token);
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

                                    await Task.Delay(10, writeTaskCancel.Token);
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
                                        _ = await node.Store.CommitAOFAsync(writeTaskCancel.Token);
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
                                    await Task.Delay(100, writeTaskCancel.Token);
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
                    await Task.Delay(10_000, cancellation);
                }
                else
                {
                    var timer = Stopwatch.StartNew();

                    // Things start fine
                    await Task.Delay(1_000, cancellation);

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

                        await Task.Delay(100, cancellation);
                    } while (!replica1Happened || !replica2Happened);

                    // Things fail for a bit
                    ExceptionInjectionHelper.EnableException(faultType);
                    await Task.Delay(2_000, cancellation);

                    // Things recover
                    ExceptionInjectionHelper.DisableException(faultType);

                    timer.Stop();

                    // Wait out the rest of the duration
                    if (timer.ElapsedMilliseconds < 10_000)
                    {
                        await Task.Delay((int)(10_000 - timer.ElapsedMilliseconds), cancellation);
                    }
                }

                // Stop writing
                writeTaskCancel.Cancel();

                // Wait for all our writes and checkpoints to spin down
                await Task.WhenAll(writeToPrimary1Task, writeToPrimary2Task, checkpointTask);
            }

            // Shutdown all primaries
            context.ShutdownNode(primary1);
            context.ShutdownNode(primary2);

            // Sometimes we can intervene post-Primary crash but pre-Replica crash, simulate that
            if (replicaFailoverBeforeShutdown)
            {
                await UpgradeReplicasAsync(context, replica1, replica2, cancellation);
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
                await UpgradeReplicasAsync(context, replica1, replica2, cancellation);
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
                    await Task.Delay(10, cancellation);

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

        [Test, Order(30)]
        [Category("CLUSTER")]
        [Category("REPLICATION")]
        public void ClusterReplicationDivergentHistoryWithoutCheckpoint()
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;
            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                cleanClusterConfig: false,
                sublogCount: sublogCount);
            context.CreateConnection(useTLS: useTLS);
            _ = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            var replicaServer = context.clusterTestUtils.GetServer(replicaNodeIndex);
            var offset = 0;
            var keyCount = 100;

            RunWorkload(primaryServer, offset, keyCount);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);

            // Validate that replica has the same keys as primary
            ValidateKeys();

            // Kill primary
            context.nodes[primaryNodeIndex].Dispose(false);
            // Kill replica
            context.nodes[replicaNodeIndex].Dispose(false);

            // Restart primary and do not recover
            context.nodes[primaryNodeIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(primaryNodeIndex),
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                tryRecover: false,
                cleanClusterConfig: false,
                sublogCount: sublogCount);
            context.nodes[primaryNodeIndex].Start();
            context.clusterTestUtils.Reconnect([primaryNodeIndex]);
            primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);

            offset += keyCount;
            // Populate primary with different history
            RunWorkload(primaryServer, offset, keyCount / 2);

            // Restart replica with recover to be ahead of primary but without checkpoint
            context.nodes[replicaNodeIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(replicaNodeIndex),
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                tryRecover: true,
                cleanClusterConfig: false,
                sublogCount: sublogCount);
            context.nodes[replicaNodeIndex].Start();
            context.clusterTestUtils.Reconnect([primaryNodeIndex, replicaNodeIndex]);
            primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            replicaServer = context.clusterTestUtils.GetServer(replicaNodeIndex);

            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);

            // Validate that replica has the same keys as primary
            ValidateKeys();

            // Run write workload at primary
            void RunWorkload(IServer server, int start, int count)
            {
                var end = start + count;
                while (start++ < end)
                {
                    var key = start.ToString();
                    var resp = server.Execute("SET", [key, key]);
                    ClassicAssert.AreEqual("OK", (string)resp);
                    resp = server.Execute("GET", key);
                    ClassicAssert.AreEqual(key, (string)resp);
                }
            }

            void ValidateKeys()
            {
                var resp = (string[])primaryServer.Execute("KEYS", ["*"]);
                var resp2 = (string[])replicaServer.Execute("KEYS", ["*"]);
                ClassicAssert.AreEqual(resp.Length, resp2.Length);
                Array.Sort(resp);
                Array.Sort(resp2);
                for (var i = 0; i < resp.Length; i++)
                    ClassicAssert.AreEqual(resp[i], resp2[i]);
            }
        }

        [Test, Order(31)]
        [Category("REPLICATION")]
        public void ClusterReplicationSimpleTransactionTest([Values] bool storedProcedure)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;

            context.CreateInstances(nodes_count, disableObjects: false, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
            context.CreateConnection(useTLS: useTLS);

            var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            var replicaServer = context.clusterTestUtils.GetServer(replicaNodeIndex);

            // Register custom procedure
            if (storedProcedure)
            {
                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc(BulkIncrementBy.Name, () => new BulkIncrementBy(), BulkIncrementBy.CommandInfo);
                _ = context.nodes[replicaNodeIndex].Register.NewTransactionProc(BulkIncrementBy.Name, () => new BulkIncrementBy(), BulkIncrementBy.CommandInfo);

                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc(BulkRead.Name, () => new BulkRead(), BulkRead.CommandInfo);
                _ = context.nodes[replicaNodeIndex].Register.NewTransactionProc(BulkRead.Name, () => new BulkRead(), BulkRead.CommandInfo);
            }

            // Setup cluster
            var resp = context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaNodeIndex, replicaNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaNodeIndex, primaryNodeIndex, logger: context.logger);

            // Attach replica
            resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryNodeIndex, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            string[] keys = ["{_}a", "{_}b", "{_}c"];
            string[] increment = ["10", "15", "20"];

            // Run multiple iterations to ensure replay at the replica does not diverge
            // Implicit check on the txnManager
            var iter = 10;
            for (var i = 0; i < iter; i++)
            {
                if (storedProcedure)
                    ClusterTestContext.ExecuteStoredProcBulkIncrement(primaryServer, keys, increment);
                else
                    context.ExecuteTxnBulkIncrement(keys, increment);
            }
            var values = increment.Select(x => (int.Parse(x) * iter).ToString()).ToArray();

            // Check keys at primary
            for (var i = 0; i < keys.Length; i++)
            {
                resp = context.clusterTestUtils.GetKey(primaryNodeIndex, Encoding.ASCII.GetBytes(keys[i]), out _, out _, out _);
                ClassicAssert.AreEqual(values[i], resp);
            }
            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);

            // Check keys at replica
            for (var i = 0; i < keys.Length; i++)
            {
                resp = context.clusterTestUtils.GetKey(replicaNodeIndex, Encoding.ASCII.GetBytes(keys[i]), out _, out _, out _);
                ClassicAssert.AreEqual(values[i], resp);
            }

            string[] result = null;
            if (storedProcedure)
                result = ClusterTestContext.ExecuteBulkReadStoredProc(replicaServer, keys);
            else
                result = context.ExecuteTxnBulkRead(replicaServer, keys);
            ClassicAssert.AreEqual(values, result);

            var primaryPInfo = context.clusterTestUtils.GetPersistenceInfo(primaryNodeIndex, context.logger);
            var replicaPInfo = context.clusterTestUtils.GetPersistenceInfo(replicaNodeIndex, context.logger);
            ClassicAssert.AreEqual(primaryPInfo.TailAddress, replicaPInfo.TailAddress);
        }
    }
}