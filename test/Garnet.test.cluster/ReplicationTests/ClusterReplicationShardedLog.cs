// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test.cluster
{
    public class ClusterReplicationShardedLog : ClusterReplicationBaseTests
    {
        const int TestSublogCount = 2;
        const int TestReplayTaskCount = 2;

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
        public void ClusterReplicationShardedLogTxnTest([Values] bool storedProcedure)
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
                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc("BULKINCRBY", () => new BulkIncrementBy(), BulkIncrementBy.CommandInfo);
                _ = context.nodes[replicaNodeIndex].Register.NewTransactionProc("BULKINCRBY", () => new BulkIncrementBy(), BulkIncrementBy.CommandInfo);

                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc("BULKREAD", () => new BulkRead(), BulkRead.CommandInfo);
                _ = context.nodes[replicaNodeIndex].Register.NewTransactionProc("BULKREAD", () => new BulkRead(), BulkRead.CommandInfo);
            }

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

            string[] keys = ["{_}a", "{_}b", "{_}c"];
            string[] values = ["10", "15", "20"];
            if (storedProcedure)
                ClusterTestContext.ExecuteStoredProcBulkIncrement(primaryServer, keys, values);
            else
                context.ExecuteTxnBulkIncrement(keys, values);

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

            if (storedProcedure)
            {
                var result = ClusterTestContext.ExecuteBulkReadStoredProc(replicaServer, keys);
                ClassicAssert.AreEqual(values, result);
            }
            else
            {
                context.ExecuteTxnBulkRead(replicaServer, keys);
            }

            var primaryPInfo = context.clusterTestUtils.GetPersistenceInfo(primaryNodeIndex, context.logger);
            var replicaPInfo = context.clusterTestUtils.GetPersistenceInfo(primaryNodeIndex, context.logger);
            ClassicAssert.AreEqual(primaryPInfo.TailAddress, replicaPInfo.TailAddress);
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
                sublogCount: 4);
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
                sublogCount: 4);
            context.nodes[primaryNodeIndex].Start();
            context.CreateConnection(useTLS: useTLS);

            primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            var keys = (string[])primaryServer.Execute("KEYS", ["*"]);
            Array.Sort(keys);
            Array.Sort(expectedKeys);
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

            var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
            var replicaServer = context.clusterTestUtils.GetServer(replicaNodeIndex);

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
    }

    public class ClusterReplicationDisklessSyncShardedLog : ClusterReplicationDisklessSyncTests
    {
        const int TestSublogCount = 2;

        public Dictionary<string, bool> enabledTests = new()
        {
            {"ClusterEmptyReplicaDisklessSync", true},
            {"ClusterAofReplayDisklessSync", true},
            {"ClusterDBVersionAlignmentDisklessSync", true},
            {"ClusterDisklessSyncParallelAttach", true},
            {"ClusterDisklessSyncFailover", true},
            {"ClusterDisklessSyncResetSyncManagerCts", true},
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
    }
}