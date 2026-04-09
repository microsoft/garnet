// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster.MultiLogTests
{
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicationShardedLog : ClusterReplicationBaseTests
    {
        const int TestSublogCount = 2;
        const int TestReplayTaskCount = 3;

        public Dictionary<string, bool> enabledTests = new()
        {
            {"ClusterSRTest", true},
            {"ClusterSRNoCheckpointRestartSecondary", true},
            {"ClusterSRPrimaryCheckpoint", true},
            {"ClusterCheckpointRetrieveDisableStorageTier", true},
            {"ClusterCheckpointRetrieveDelta", true},
            {"ClusterSRPrimaryCheckpointRetrieve", true},
            {"ClusterSRAddReplicaAfterPrimaryCheckpoint", true},
            {"ClusterSRPrimaryRestart", true},
            {"ClusterSRRedirectWrites", true},
            {"ClusterSRReplicaOfTest", true},
            {"ClusterReplicationSimpleFailover", true},
            {"ClusterFailoverAttachReplicas", true},
            {"ClusterReplicationCheckpointCleanupTest", true},
            {"ClusterMainMemoryReplicationAttachReplicas", true},
            {"ClusterDivergentReplicasTest", true},
            {"ClusterDivergentCheckpointTest", true},
            {"ClusterDivergentReplicasMMTest", true},
            {"ClusterDivergentCheckpointMMTest", true},
            {"ClusterDivergentCheckpointMMFastCommitTest", true},
            {"ClusterReplicationCheckpointAlignmentTest", true},
            {"ClusterReplicationLua", true},
            {"ClusterReplicationStoredProc", true},
            {"ClusterReplicationManualCheckpointing", true},
            {"ReplicaSyncTaskFaultsRecoverAsync", true},
            {"ClusterReplicationMultiRestartRecover", true},
            {"ReplicasRestartAsReplicasAsync", true},
            {"PrimaryUnavailableRecoveryAsync", true},
            {"ClusterReplicationDivergentHistoryWithoutCheckpoint", true},
            {"ClusterReplicationSimpleTransactionTest", true}
        };

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            var methods = typeof(ClusterReplicationShardedLog).GetMethods().Where(static mtd => mtd.GetCustomAttribute<TestAttribute>() != null);
            foreach (var method in methods)
                enabledTests.TryAdd(method.Name, true);
        }

        [SetUp]
        public override void Setup()
        {
            var testName = TestContext.CurrentContext.Test.MethodName;
            if (!enabledTests.TryGetValue(testName, out var isEnabled) || !isEnabled)
            {
                Assert.Ignore($"Skipping {testName} for {nameof(ClusterReplicationShardedLog)}");
            }
            asyncReplay = false;
            sublogCount = TestSublogCount;
            base.Setup();
        }

        [TearDown]
        public override void TearDown()
        {
            var testName = TestContext.CurrentContext.Test.MethodName;
            if (!enabledTests.TryGetValue(testName, out var isEnabled) || !isEnabled)
            {
                Assert.Ignore($"Skipping {testName} for {nameof(ClusterReplicationShardedLog)}");
            }
            base.TearDown();
        }

        [Test, Order(1)]
        [Category("REPLICATION")]
        public void ClusterReplicationShardedLogTxnTest([Values] bool storedProcedure, [Values] bool testParallelReplay)
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;

            context.CreateInstances(nodes_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                sublogCount: testParallelReplay ? 1 : TestSublogCount,
                replayTaskCount: testParallelReplay ? TestReplayTaskCount : 1);
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

            string[] keys = ["{_}a", "{_}b", "{_}c", "{_}x", "{_}y", "{_}z"];
            string[] values = ["10", "15", "20", "25", "30", "35"];

            if (storedProcedure)
                ClusterTestContext.ExecuteStoredProcBulkIncrement(primaryServer, keys, values);
            else
                context.ExecuteTxnBulkIncrement(keys, values);

            // Check keys at primary
            for (var i = 0; i < keys.Length; i++)
            {
                resp = context.clusterTestUtils.GetKey(primaryNodeIndex, Encoding.ASCII.GetBytes(keys[i]), out _, out _, out _);
                ClassicAssert.AreEqual(values[i], resp, "At primary");
            }
            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);

            // Check keys at replica
            for (var i = 0; i < keys.Length; i++)
            {
                resp = context.clusterTestUtils.GetKey(replicaNodeIndex, Encoding.ASCII.GetBytes(keys[i]), out _, out _, out _);
                ClassicAssert.AreEqual(values[i], resp, "At replica");
            }

            if (storedProcedure)
            {
                var result = ClusterTestContext.ExecuteBulkReadStoredProc(replicaServer, keys);
                ClassicAssert.AreEqual(values, result);
            }
            else
            {
                var result = context.ExecuteTxnBulkRead(replicaServer, keys);
                ClassicAssert.AreEqual(values, result);
            }

            var primaryPInfo = context.clusterTestUtils.GetPersistenceInfo(primaryNodeIndex, context.logger);
            var replicaPInfo = context.clusterTestUtils.GetPersistenceInfo(primaryNodeIndex, context.logger);
            ClassicAssert.AreEqual(primaryPInfo.TailAddress, replicaPInfo.TailAddress);
            var primaryReplOffset = context.clusterTestUtils.GetReplicationOffset(0);
            var replicaReplOffset = context.clusterTestUtils.GetReplicationOffset(1);
        }

        [Test, Order(2)]
        [Category("REPLICATION")]
        public void ClusterReplicationShardedLogRecover()
        {
            var primary_count = 1;
            var primaryNodeIndex = 0;

            context.CreateInstances(
                primary_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                sublogCount: TestSublogCount);
            context.CreateConnection(useTLS: useTLS);

            _ = context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);

            var keyLength = 16;
            var kvpairCount = keyCount;
            context.kvPairs = [];

            //Populate Primary
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, 0);

            var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            var expectedKeys = (string[])primaryServer.Execute("KEYS", ["*"]);
            context.nodes[primaryNodeIndex].Store.CommitAOF();

            // Shutdown node
            context.nodes[primaryNodeIndex].Dispose(false);

            // Restart secondary
            context.nodes[primaryNodeIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(primaryNodeIndex),
                tryRecover: true,
                enableAOF: true,
                useTLS: useTLS,
                cleanClusterConfig: false,
                asyncReplay: asyncReplay,
                sublogCount: TestSublogCount);
            context.nodes[primaryNodeIndex].Start();
            context.CreateConnection(useTLS: useTLS);

            primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            var keys = (string[])primaryServer.Execute("KEYS", ["*"]);
            Array.Sort(keys);
            Array.Sort(expectedKeys);
            ClassicAssert.AreEqual(expectedKeys.Length, keys.Length);
            ClassicAssert.AreEqual(expectedKeys, keys);
        }

        [Test, Order(3)]
        [Category("REPLICATION")]
        public void ClusterReplicationSimpleMultiReplay()
        {
            var replica_count = 1;// Per primary
            var primary_count = 1;
            var nodes_count = primary_count + (primary_count * replica_count);
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;

            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                sublogCount: 1,
                replayTaskCount: TestReplayTaskCount);
            context.CreateConnection(useTLS: useTLS);

            // Setup cluster
            context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaNodeIndex, replicaNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaNodeIndex, primaryNodeIndex, logger: context.logger);

            // Attach replica
            var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryNodeIndex, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            var keyLength = 16;
            var kvpairCount = 2;
            context.kvPairs = [];

            // Populate Primary
            context.SimplePopulateDB(disableObjects: true, keyLength, kvpairCount, primaryNodeIndex);

            // Wait for replica to sync
            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex);

            // Validate database
            context.SimpleValidateDB(disableObjects: true, replicaNodeIndex);
        }

        [Test, Order(4)]
        [Category("REPLICATION")]
        public void ClusterParallelReplicationUpgrade([Values] bool upgradeFromSingleLog, [Values] bool disableObjects)
        {
            var nodes_count = 3;
            var singleLogNodeIndex = 0;
            var multiLogNodeIndex = 1;
            var replicaIndex = 2;
            var timeout = 60;
            var sourceIndex = upgradeFromSingleLog ? singleLogNodeIndex : multiLogNodeIndex;
            var targetIndex = upgradeFromSingleLog ? multiLogNodeIndex : singleLogNodeIndex;

            context.nodes = new GarnetServer[nodes_count];
            context.endpoints = TestUtils.GetShardEndPoints(nodes_count, IPAddress.Loopback, 7000);

            context.clusterTestUtils = new ClusterTestUtils(
                context.endpoints,
                context: context,
                textWriter: context.logTextWriter,
                UseTLS: false,
                authUsername: null,
                authPassword: null,
                certificates: null);

            // Create nodes with single log
            context.nodes[singleLogNodeIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(singleLogNodeIndex),
                disableObjects: disableObjects,
                enableAOF: true,
                timeout: timeout,
                sublogCount: 1);
            context.nodes[singleLogNodeIndex].Start();

            // Create nodes with multi-log
            context.nodes[multiLogNodeIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(multiLogNodeIndex),
                disableObjects: disableObjects,
                enableAOF: true,
                timeout: timeout,
                sublogCount: TestSublogCount);
            context.nodes[multiLogNodeIndex].Start();

            // Create replica with single or multi-log configuration
            context.nodes[replicaIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(replicaIndex),
                disableObjects: disableObjects,
                enableAOF: true,
                timeout: timeout,
                sublogCount: upgradeFromSingleLog ? TestSublogCount : 1);
            context.nodes[replicaIndex].Start();

            // Create connection
            context.CreateConnection(useTLS: useTLS);

            // Assign slot to source node
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddSlotsRange(sourceIndex, [(0, 16383)], context.logger));

            // Set config epoch
            for (var i = 0; i < nodes_count; i++)
                context.clusterTestUtils.SetConfigEpoch(i, i + 1, context.logger);

            // Introduce nodes
            for (var i = 1; i < nodes_count; i++)
                context.clusterTestUtils.Meet(0, i, context.logger);

            // Wait for gossip to propagate
            for (var i = 0; i < nodes_count; i++)
            {
                for (var j = 0; j < nodes_count; j++)
                {
                    if (i == j) continue;
                    context.clusterTestUtils.WaitUntilNodeIsKnown(i, j, context.logger);
                    context.clusterTestUtils.WaitUntilNodeIsKnown(j, i, context.logger);
                }
            }

            // Add replica
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.ClusterReplicate(replicaIndex, targetIndex, logger: context.logger));

            var keyLength = 16;
            var kvpairCount = keyCount;
            context.kvPairs = [];
            context.kvPairsObj = [];

            // Populate node
            context.SimplePopulateDB(disableObjects: disableObjects, keyLength, kvpairCount, sourceIndex);

            // Migrate slots
            var sourcePort = context.clusterTestUtils.GetEndPoint(sourceIndex);
            var targetPort = context.clusterTestUtils.GetEndPoint(targetIndex);
            context.clusterTestUtils.MigrateSlots(sourcePort, targetPort, [0, 16383], range: true, logger: context.logger);
            context.clusterTestUtils.WaitForMigrationCleanup(sourceIndex, logger: context.logger);
            context.clusterTestUtils.WaitForSlotOwnership(replicaIndex, context.clusterTestUtils.GetNodeIdFromNode(targetIndex, context.logger), [0, 16383], context.logger);

            // Validate migrated keys
            context.SimpleValidateDB(disableObjects, targetIndex);
            context.SimpleValidateDB(disableObjects, replicaIndex);
        }
    }
}