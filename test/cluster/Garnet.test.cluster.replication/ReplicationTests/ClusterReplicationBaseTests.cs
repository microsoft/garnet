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
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Internal;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicationBaseTests : TestBase
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
                new(() => AsyncUtils.BlockingWait(ClusterSRPrimaryCheckpointAsync(false, false)), "ClusterSRPrimaryCheckpointAsync(false, false)"),
                new(() => AsyncUtils.BlockingWait(ClusterSRPrimaryCheckpointAsync(false, true)), "ClusterSRPrimaryCheckpointAsync(false, true)"),
                new(() => AsyncUtils.BlockingWait(ClusterSRPrimaryCheckpointAsync(true, false)), "ClusterSRPrimaryCheckpointAsync(true, false)"),
                new(() => AsyncUtils.BlockingWait(ClusterSRPrimaryCheckpointAsync(true, true)), "ClusterSRPrimaryCheckpointAsync(true, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, false), "ClusterSRPrimaryCheckpointRetrieve(false, false, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, false, true), "ClusterSRPrimaryCheckpointRetrieve(false, false, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, false), "ClusterSRPrimaryCheckpointRetrieve(false, true, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(false, true, true), "ClusterSRPrimaryCheckpointRetrieve(false, true, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, false), "ClusterSRPrimaryCheckpointRetrieve(true, false, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, false, true), "ClusterSRPrimaryCheckpointRetrieve(true, false, true)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, false), "ClusterSRPrimaryCheckpointRetrieve(true, true, false)"),
                new(() => ClusterSRPrimaryCheckpointRetrieve(true, true, true), "ClusterSRPrimaryCheckpointRetrieve(true, true, true)"),
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
                new(() => ClusterReplicationCheckpointCleanupTest(false, false), "ClusterReplicationCheckpointCleanupTest(false, false)"),
                new(() => ClusterReplicationCheckpointCleanupTest(false, true), "ClusterReplicationCheckpointCleanupTest(false, true)"),
                new(() => ClusterReplicationCheckpointCleanupTest(true, false), "ClusterReplicationCheckpointCleanupTest(true, false)"),
                new(() => ClusterReplicationCheckpointCleanupTest(true, true), "ClusterReplicationCheckpointCleanupTest(true, true)")
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
            {"ClusterReplicationMultiRestartRecover", LogLevel.Error},
            {"ClusterFailoverAttachReplicas", LogLevel.Error},
            {"ClusterReplicationSimpleTransactionTest", LogLevel.Trace},
            {"ClusterReplicationStoredProc", LogLevel.Error}
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
            if (useTLS)
                context.EnableGarnetLoggingEvents([GarnetTestLoggingEventType.LogPrimaryStreamType, GarnetTestLoggingEventType.LogRunAofSyncTask]);

            var replica_count = 1;// Per primary
            var primary_count = 1;
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount, threadPoolMinIOCompletionThreads: 512);
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
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex);
            // Validate database
            context.SimpleValidateDB(disableObjects, replicaIndex);

            // Shutdown secondary
            context.nodes[replicaIndex].Dispose(false);
            context.clusterTestUtils.WaitForAofSyncDriverDipose(primaryIndex);

            // New insert
            context.SimplePopulateDB(disableObjects, keyLength, kvpairCount, primaryIndex, performRMW: performRMW, addCount: addCount);

            // Restart secondary
            context.nodes[replicaIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(replicaIndex),
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false,
                sublogCount: sublogCount,
                threadPoolMinIOCompletionThreads: 512);
            context.nodes[replicaIndex].Start();
            context.CreateConnection(useTLS: useTLS);

            // Validate synchronization was success
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex);
            // Validate database
            context.SimpleValidateDB(disableObjects, replicaIndex);
        }

        [Test, Order(3)]
        [Category("REPLICATION")]
        public async Task ClusterSRPrimaryCheckpointAsync([Values] bool performRMW, [Values] bool disableObjects)
        {
            if (useTLS)
                context.EnableGarnetLoggingEvents([GarnetTestLoggingEventType.LogPrimaryStreamType, GarnetTestLoggingEventType.LogRunAofSyncTask]);

            var replica_count = 1;// Per primary
            var primary_count = 1;
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: disableObjects, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount, threadPoolMinIOCompletionThreads: 512);
            context.CreateConnection(useTLS: useTLS);
            var (shards, _) = context.clusterTestUtils.SimpleSetupCluster(primary_count, replica_count, logger: context.logger);

            var cconfig = context.clusterTestUtils.ClusterNodes(primaryIndex, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            shards = context.clusterTestUtils.ClusterShards(primaryIndex, context.logger);
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

            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            var replicaLastSaveTime = context.clusterTestUtils.LastSave(replicaIndex, logger: context.logger);
            context.clusterTestUtils.Checkpoint(0, logger: context.logger);

            // Populate Primary
            context.SimplePopulateDB(disableObjects, keyLength, kvpairCount, primaryIndex, performRMW: performRMW, addCount: addCount);
            context.SimpleValidateDB(disableObjects, replicaIndex);

            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(replicaIndex, replicaLastSaveTime, logger: context.logger);

            // Shutdown secondary
            context.nodes[replicaIndex].Dispose(false);
            context.clusterTestUtils.WaitForAofSyncDriverDipose(primaryIndex);

            // New insert
            context.SimplePopulateDB(disableObjects, keyLength, kvpairCount, primaryIndex, performRMW: performRMW, addCount: addCount);

            // Restart secondary
            context.nodes[replicaIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(replicaIndex),
                disableObjects: disableObjects,
                tryRecover: true,
                enableAOF: true,
                timeout: timeout,
                useTLS: useTLS,
                cleanClusterConfig: false,
                asyncReplay: asyncReplay,
                sublogCount: sublogCount,
                threadPoolMinIOCompletionThreads: 512);
            context.nodes[replicaIndex].Start();
            context.CreateConnection(useTLS: useTLS);

            for (var i = 1; i < replica_count; i++)
                context.clusterTestUtils.WaitForReplicaRecovery(i, context.logger);
            await context.clusterTestUtils.WaitForConnectedReplicaCountAsync(0, replica_count, context.logger).ConfigureAwait(false);

            // Validate synchronization was success
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex);
            context.SimpleValidateDB(disableObjects, replicaIndex);
        }

        [Test, Order(4)]
        [Category("REPLICATION")]
        public void ClusterCheckpointRetrieveDisableStorageTier([Values] bool performRMW, [Values] bool disableObjects)
        {
            ClusterSRPrimaryCheckpointRetrieve(performRMW, disableObjects, false, true);
        }

        [Test, Order(6)]
        [Category("REPLICATION")]
        public void ClusterSRPrimaryCheckpointRetrieve([Values] bool performRMW, [Values] bool disableObjects, [Values] bool manySegments)
            => ClusterSRPrimaryCheckpointRetrieve(performRMW: performRMW, disableObjects: disableObjects, manySegments: manySegments, false);

        void ClusterSRPrimaryCheckpointRetrieve(bool performRMW, bool disableObjects, bool manySegments, bool disableStorageTier)
        {
            var lowMemory = manySegments;

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
            context.clusterTestUtils.WaitForAofSyncDriverDipose(primaryIndex);

            // Populate Primary
            if (disableObjects)
            {
                if (!performRMW)
                    context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex);
                else
                    context.PopulatePrimaryRMW(ref context.kvPairs, keyLength, kvpairCount, primaryIndex, addCount);
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
        public void ClusterFailoverAttachReplicas([Values] bool performRMW, [Values] bool takePrimaryCheckpoint, [Values] bool takeNewPrimaryCheckpoint)
        {
            var replica_count = 2; // Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);
            context.CreateInstances(nodes_count, disableObjects: true, enableAOF: true, useTLS: useTLS, asyncReplay: asyncReplay, sublogCount: sublogCount);
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
        public void ClusterReplicationCheckpointCleanupTest([Values] bool performRMW, [Values] bool disableObjects)
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var replica_count = 1;//Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(nodes_count,
                tryRecover: true,
                disableObjects: disableObjects,
                lowMemory: true,
                segmentSize: "4k",
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                deviceType: Tsavorite.core.DeviceType.Native,
                sublogCount: sublogCount);
            context.CreateConnection(useTLS: useTLS);

            // Setup cluster
            context.SimplePrimaryReplicaSetup();

            var cconfig = context.clusterTestUtils.ClusterNodes(primaryIndex, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var shards = context.clusterTestUtils.ClusterShards(primaryIndex, context.logger);
            ClassicAssert.AreEqual(2, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            context.kvPairs = [];
            context.kvPairsObj = [];
            context.checkpointTask = Task.Run(() => context.PopulatePrimaryAndTakeCheckpointTask(performRMW, disableObjects, takeCheckpoint: true));
            var attachReplicaTask = Task.Run(() => context.AttachAndWaitForSyncAsync(primaryIndex, primary_count, replica_count, disableObjects));

            var tasks = new Task[] { context.checkpointTask, attachReplicaTask };
            if (!Task.WhenAll(tasks).Wait(TimeSpan.FromSeconds(60)))
                Assert.Fail($"Task timeout - checkpointTask: {context.checkpointTask.Status}, attachReplicaTask: {attachReplicaTask.Status}");

            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex: primaryIndex, secondaryIndex: replicaIndex, logger: context.logger);
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
            fastCommit = sublogCount > 1;
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
        public async Task ClusterReplicationCheckpointAlignmentTestAsync([Values] bool performRMW)
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
            await context.clusterTestUtils.WaitUntilNodeIdIsKnownAsync(replicaNodeIndex, primaryNodeId, logger: context.logger).ConfigureAwait(false);
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
        public void ClusterReplicationStoredProc([Values] bool enableDisklessSync, [Values] bool attachFirst, [Values] bool objectStore)
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
            if (objectStore)
            {
                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc("RATELIMIT", () => new RateLimiterTxn(), new RespCommandsInfo { Arity = 4 });
                _ = context.nodes[replicaNodeIndex].Register.NewTransactionProc("RATELIMIT", () => new RateLimiterTxn(), new RespCommandsInfo { Arity = 4 });
            }
            else
            {
                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc("BULKINCRBY", () => new BulkIncrementBy(), BulkIncrementBy.CommandInfo);
                _ = context.nodes[replicaNodeIndex].Register.NewTransactionProc("BULKINCRBY", () => new BulkIncrementBy(), BulkIncrementBy.CommandInfo);
            }

            // Setup cluster
            context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaNodeIndex, replicaNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaNodeIndex, primaryNodeIndex, logger: context.logger);

            if (attachFirst)
                ClusterReplicate();

            if (objectStore)
                ExecuteRateLimit();
            else
            {
                string[] increment = ["10", "15"];
                ClusterTestContext.ExecuteStoredProcBulkIncrement(primaryServer, [expectedKeys[0]], [increment[0]]);
                ClusterTestContext.ExecuteStoredProcBulkIncrement(primaryServer, [expectedKeys[1]], [increment[1]]);
            }

            if (!attachFirst)
                ClusterReplicate();

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
                var resp = primaryServer.Execute("RATELIMIT", [expectedKeys[0], "1000000000", "1000000000"]);
                ClassicAssert.AreEqual("ALLOWED", (string)resp);
                resp = primaryServer.Execute("RATELIMIT", [expectedKeys[1], "1000000000", "1000000000"]);
                ClassicAssert.AreEqual("ALLOWED", (string)resp);
            }

            void ClusterReplicate()
            {
                // Issue replicate
                var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryNodeIndex, logger: context.logger);
                ClassicAssert.AreEqual("OK", resp);
                context.clusterTestUtils.WaitForReplicaRecovery(replicaNodeIndex, logger: context.logger);
            }
        }

        [Test, Order(25)]
        [Category("REPLICATION")]
        public async Task ClusterReplicationManualCheckpointingAsync()
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
            var primaryCommit = await context.nodes[0].Store.CommitAOFAsync(default);
            ClassicAssert.IsTrue(primaryCommit);
            var secondCommit = await context.nodes[1].Store.CommitAOFAsync(default);
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

                            await Task.Delay(10).ConfigureAwait(false);
                        }
                    },
                    cancellation
                );

            await Task.Delay(100, cancellation).ConfigureAwait(false);

            // Force replica to continually fault
            ExceptionInjectionHelper.EnableException(faultType);

            // Give it enough time to die horribly
            await Task.Delay(100, cancellation).ConfigureAwait(false);

            // Stop primary writes
            primaryInsertCancel.Cancel();
            await continuallyWriteToPrimaryTask.ConfigureAwait(false);

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
        [CancelAfter(60_000)]
        public async Task ClusterReplicationMultiRestartRecover(CancellationToken cancellationToken)
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
            var keyCount = 256;

            var taskCount = 4;
            var tasks = new List<Task>();
            for (var i = 0; i < taskCount; i++)
                tasks.Add(Task.Run(() => RunWorkload(i * keyCount, (i + 1) * keyCount), cancellationToken));
            var restartRecover = 4;
            tasks.Add(Task.Run(async () => await RestartRecover(restartRecover), cancellationToken));

            await Task.WhenAll(tasks).ConfigureAwait(false);
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
                            Assert.Fail($"Failed waiting for primary aof sync cleanup ({iteration}: {items[0]};{items[1]})!");
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
                    context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, cancellation: cancellationToken, logger: context.logger);

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
        [Category("REPLICATION")]
        public async Task ClusterReplicationDivergentHistoryWithoutCheckpointAsync()
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
            await context.clusterTestUtils.ReconnectAsync([primaryNodeIndex]).ConfigureAwait(false);
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
            await context.clusterTestUtils.ReconnectAsync([primaryNodeIndex, replicaNodeIndex]).ConfigureAwait(false);
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

        [Test, Order(32)]
        [Category("REPLICATION")]
        public void ClusterReplicationHlogSegmentCleanupTest([Values] bool performRMW, [Values] bool disableObjects)
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var replica_count = 1;
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            ClassicAssert.IsTrue(primary_count > 0);

            context.CreateInstances(nodes_count,
                tryRecover: true,
                disableObjects: disableObjects,
                lowMemory: true,
                segmentSize: "4k",
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                deviceType: Tsavorite.core.DeviceType.Native,
                sublogCount: sublogCount);
            context.CreateConnection(useTLS: useTLS);

            context.SimplePrimaryReplicaSetup();

            var cconfig = context.clusterTestUtils.ClusterNodes(primaryIndex, context.logger);
            var myself = cconfig.Nodes.First();
            var slotRangesStr = string.Join(",", myself.Slots.Select(x => $"({x.From}-{x.To})").ToList());
            ClassicAssert.AreEqual(1, myself.Slots.Count, $"Setup failed slot ranges count greater than 1 {slotRangesStr}");

            var shards = context.clusterTestUtils.ClusterShards(primaryIndex, context.logger);
            ClassicAssert.AreEqual(2, shards.Count);
            ClassicAssert.AreEqual(1, shards[0].slotRanges.Count);
            ClassicAssert.AreEqual(0, shards[0].slotRanges[0].Item1);
            ClassicAssert.AreEqual(16383, shards[0].slotRanges[0].Item2);

            context.kvPairs = [];
            context.kvPairsObj = [];

            // Populate with more iterations to accumulate multiple hlog segments and trigger segment cleanup
            context.checkpointTask = Task.Run(() => context.PopulatePrimaryAndTakeCheckpointTask(performRMW, disableObjects, takeCheckpoint: true, iter: 10));
            var attachReplicaTask = Task.Run(() => context.AttachAndWaitForSyncAsync(primaryIndex, primary_count, replica_count, disableObjects));

            var tasks = new Task[] { context.checkpointTask, attachReplicaTask };
            if (!Task.WhenAll(tasks).Wait(TimeSpan.FromSeconds(timeout)))
                Assert.Fail($"Task timeout - checkpointTask: {context.checkpointTask.Status}, attachReplicaTask: {attachReplicaTask.Status}");

            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex: primaryIndex, secondaryIndex: replicaIndex, logger: context.logger);
            context.SimpleValidateDB(disableObjects, replicaIndex);
        }
    }
}