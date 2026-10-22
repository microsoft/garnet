// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
            {"ClusterSRAddReplicaAfterPrimaryCheckpoint", true},
            {"ClusterSRPrimaryRestart", true},
            {"ClusterSRRedirectWrites", false}, // Does not test AOF
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
            {"ClusterReplicationStoredProc", false}, // Duplicate test in this class
            {"ClusterReplicationManualCheckpointing", true},
            {"ReplicaSyncTaskFaultsRecoverAsync", true},
            {"ClusterReplicationMultiRestartRecover", false},
            {"ClusterReplicationDivergentHistoryWithoutCheckpoint", true},
            {"ClusterReplicationSimpleTransactionTest", false} // Duplicate test in this class
        };

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            var methods = typeof(ClusterReplicationShardedLog).GetMethods().Where(static mtd => mtd.GetCustomAttribute<TestAttribute>() != null);
            foreach (var method in methods)
                enabledTests.TryAdd(method.Name, true);

            monitorTests.Add("ClusterReplicationShardedLogTxnTest", LogLevel.Warning);
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
            context.clusterTestUtils.WaitForReplicaRecovery(replicaNodeIndex, logger: context.logger);

            var primaryLog = context.nodes[primaryNodeIndex].Provider.StoreWrapper.appendOnlyFile.Log;
            var keys = KeysSpanningEveryReplayParticipant(primaryLog, ["{_}a", "{_}b", "{_}c", "{_}x", "{_}y", "{_}z"]);
            var values = keys.Select(static (_, i) => (10 + (5 * i)).ToString()).ToArray();

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

        // The (sublog, replay task) pairs that a transaction touches replay it together and wait for one another. Adds
        // keys to the given ones until the transaction touches every pair, all within the hash tag's slot.
        static string[] KeysSpanningEveryReplayParticipant(Garnet.server.GarnetLog log, string[] keys)
        {
            int ParticipantOf(string key) => log.GetVirtualSublogIdx(Garnet.server.GarnetLog.HASH(Encoding.ASCII.GetBytes(key)));

            var spanning = new List<string>(keys);
            var covered = new bool[log.Size * log.ReplayTaskCount];
            foreach (var key in keys)
                covered[ParticipantOf(key)] = true;

            for (var i = 0; covered.Contains(false); i++)
            {
                var key = $"{{_}}p{i}";
                var participant = ParticipantOf(key);
                if (covered[participant])
                    continue;

                covered[participant] = true;
                spanning.Add(key);
            }
            return [.. spanning];
        }

        [Test, Order(2)]
        [Category("REPLICATION")]
        public async Task ClusterReplicationShardedLogRecoverAsync()
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
            var expectedKeys = (string[])primaryServer.Execute(0, "KEYS", ["*"]);
            await context.nodes[primaryNodeIndex].Store.CommitAOFAsync(default).ConfigureAwait(false);

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
            var keys = (string[])primaryServer.Execute(0, "KEYS", ["*"]);
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
            context.clusterTestUtils.WaitForReplicaRecovery(replicaNodeIndex, logger: context.logger);

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
            context.endpoints = TestUtils.GetShardEndPoints(nodes_count, IPAddress.Loopback, ClusterTestContext.Port);

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
            context.clusterTestUtils.WaitForReplicaRecovery(replicaIndex, logger: context.logger);

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

        [Test, Order(5)]
        [Category("REPLICATION")]
        public async Task ClusterAofUpgradeSLtoSLMRRecoverAsync([Values] bool useStoredProcedure)
        {
            var primary_count = 1;
            var primaryNodeIndex = 0;

            // Phase 1: Start in single-log single-replay mode (SL) and write data
            context.CreateInstances(
                primary_count,
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                sublogCount: 1,
                replayTaskCount: 1);
            context.CreateConnection(useTLS: useTLS);

            _ = context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);

            var primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);

            // Register stored proc if needed
            if (useStoredProcedure)
            {
                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc(BulkIncrementBy.Name, () => new BulkIncrementBy(), BulkIncrementBy.CommandInfo);
                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc(BulkRead.Name, () => new BulkRead(), BulkRead.CommandInfo);
            }

            // Write standalone ops
            string[] keys = ["{_}a", "{_}b", "{_}c", "{_}x", "{_}y", "{_}z"];
            string[] values = ["10", "15", "20", "25", "30", "35"];

            if (useStoredProcedure)
            {
                ClusterTestContext.ExecuteStoredProcBulkIncrement(primaryServer, keys, values);
            }
            else
            {
                context.ExecuteTxnBulkIncrement(keys, values);
            }

            // Verify at primary before upgrade
            for (var i = 0; i < keys.Length; i++)
            {
                var resp = context.clusterTestUtils.GetKey(primaryNodeIndex, Encoding.ASCII.GetBytes(keys[i]), out _, out _, out _);
                ClassicAssert.AreEqual(values[i], resp, $"At primary before upgrade, key {keys[i]}");
            }

            // Commit AOF (no checkpoint) and stop
            await context.nodes[primaryNodeIndex].Store.CommitAOFAsync(default).ConfigureAwait(false);
            context.nodes[primaryNodeIndex].Dispose(false);

            // Phase 2: Restart with multi-replay config (SLMR) — AOF replay uses multiple tasks
            context.nodes[primaryNodeIndex] = context.CreateInstance(
                context.clusterTestUtils.GetEndPoint(primaryNodeIndex),
                tryRecover: true,
                enableAOF: true,
                useTLS: useTLS,
                cleanClusterConfig: false,
                asyncReplay: asyncReplay,
                sublogCount: 1,
                replayTaskCount: TestReplayTaskCount);

            if (useStoredProcedure)
            {
                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc(BulkIncrementBy.Name, () => new BulkIncrementBy(), BulkIncrementBy.CommandInfo);
                _ = context.nodes[primaryNodeIndex].Register.NewTransactionProc(BulkRead.Name, () => new BulkRead(), BulkRead.CommandInfo);
            }

            context.nodes[primaryNodeIndex].Start();
            context.CreateConnection(useTLS: useTLS);

            // Verify SL-era data survived the multi-replay recovery
            for (var i = 0; i < keys.Length; i++)
            {
                var resp = context.clusterTestUtils.GetKey(primaryNodeIndex, Encoding.ASCII.GetBytes(keys[i]), out _, out _, out _);
                ClassicAssert.AreEqual(values[i], resp, $"At primary after SLMR upgrade, key {keys[i]}");
            }

            if (useStoredProcedure)
            {
                primaryServer = context.clusterTestUtils.GetServer(primaryNodeIndex);
                var result = ClusterTestContext.ExecuteBulkReadStoredProc(primaryServer, keys);
                ClassicAssert.AreEqual(values, result);
            }
        }

        [Test, Order(6)]
        [Category("REPLICATION")]
        public async Task ClusterReplicationSinglePhysicalLogTailWitnessPulseTest()
        {
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;

            // One physical sublog replayed by several tasks. Multi-log is on, so the primary emits
            // tail witness pulses, but records on a single physical sublog carry no generator
            // sequence number and the replay side times its virtual sublogs by log address.
            context.CreateInstances(2,
                disableObjects: false,
                enableAOF: true,
                useTLS: useTLS,
                asyncReplay: asyncReplay,
                sublogCount: 1,
                replayTaskCount: TestReplayTaskCount);
            context.CreateConnection(useTLS: useTLS);

            var resp = context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaNodeIndex, replicaNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaNodeIndex, primaryNodeIndex, logger: context.logger);

            resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryNodeIndex, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);
            context.clusterTestUtils.WaitForReplicaRecovery(replicaNodeIndex, logger: context.logger);

            // Leave the AOF sync task idle for longer than AofTailWitnessFreqMs (100 ms by default,
            // and the harness does not override it) so it emits at least one pulse before anything is
            // written. Everything below this point depends on the sync task having survived that pulse.
            await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);

            string[] keys = ["{_}a", "{_}b", "{_}c"];
            string[] values = ["10", "15", "20"];
            for (var i = 0; i < keys.Length; i++)
            {
                var state = context.clusterTestUtils.SetKey(primaryNodeIndex, Encoding.ASCII.GetBytes(keys[i]), Encoding.ASCII.GetBytes(values[i]), out _, out _, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, state);
            }

            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);

            for (var i = 0; i < keys.Length; i++)
            {
                var value = context.clusterTestUtils.GetKey(replicaNodeIndex, Encoding.ASCII.GetBytes(keys[i]), out _, out _, out _);
                ClassicAssert.AreEqual(values[i], value, $"At replica, key {keys[i]}");
            }
        }

        [Test, Order(7)]
        [Category("REPLICATION")]
        public async Task ClusterReplicationReadsWaitingForReplayParkTheirSessions([Values] bool scan)
        {
            var primaryNodeIndex = 0;
            var replicaNodeIndex = 1;

            context.CreateInstances(2,
                disableObjects: false,
                enableAOF: true,
                useTLS: false,
                asyncReplay: asyncReplay,
                sublogCount: TestSublogCount);
            context.CreateConnection(useTLS: false);

            var resp = context.clusterTestUtils.AddDelSlotsRange(primaryNodeIndex, [(0, 16383)], addslot: true, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            context.clusterTestUtils.SetConfigEpoch(primaryNodeIndex, primaryNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaNodeIndex, replicaNodeIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryNodeIndex, replicaNodeIndex, logger: context.logger);
            context.clusterTestUtils.WaitUntilNodeIsKnown(replicaNodeIndex, primaryNodeIndex, logger: context.logger);

            resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex, primaryNodeIndex, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);
            context.clusterTestUtils.WaitForReplicaRecovery(replicaNodeIndex, logger: context.logger);

            // Two keys that replay of different virtual sublogs applies
            var appendOnlyFile = context.nodes[replicaNodeIndex].Provider.StoreWrapper.appendOnlyFile;
            long HashOf(string key) => Garnet.server.GarnetLog.HASH(Encoding.ASCII.GetBytes(key)) & long.MaxValue;
            int SublogOf(string key) => appendOnlyFile.Log.GetVirtualSublogIdx(HashOf(key));
            var keyA = "a";
            var sublogA = SublogOf(keyA);
            var keyB = Enumerable.Range(0, 1000).Select(i => $"b{i}").First(key => SublogOf(key) != sublogA);
            var sublogB = SublogOf(keyB);

            var valueA = $"value-{keyA}";
            var valueB = $"value-{keyB}";
            foreach (var (key, value) in new[] { (keyA, valueA), (keyB, valueB) })
            {
                var state = context.clusterTestUtils.SetKey(primaryNodeIndex, Encoding.ASCII.GetBytes(key), Encoding.ASCII.GetBytes(value), out _, out _, logger: context.logger);
                ClassicAssert.AreEqual(ResponseState.OK, state);
            }
            context.clusterTestUtils.WaitForReplicaAofSync(primaryNodeIndex, replicaNodeIndex, context.logger);

            // Replay of A is ahead of replay of B. A session that has read A has to wait before reading B, until replay of B
            // passes the sequence number the session has reached
            const long Ahead = 1L << 60;
            var manager = appendOnlyFile.readConsistencyManager;
            manager.UpdateVirtualSublogKeySequenceNumber(sublogA, HashOf(keyA), Ahead);

            // More sessions wait than the thread pool has threads
            ThreadPool.GetMinThreads(out var minWorkerThreads, out _);
            var sessions = Math.Max(512, 4 * minWorkerThreads);

            var readA = $"*2\r\n$3\r\nGET\r\n${keyA.Length}\r\n{keyA}\r\n";
            var readB = scan ? "*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n" : $"*2\r\n$3\r\nGET\r\n${keyB.Length}\r\n{keyB}\r\n";
            var request = Encoding.ASCII.GetBytes($"*1\r\n$8\r\nREADONLY\r\n{readA}{readB}");

            var answerOfA = $"+OK\r\n${valueA.Length}\r\n{valueA}\r\n";
            string[] expectedReplies = scan
                ?
                [
                    $"{answerOfA}*2\r\n${keyA.Length}\r\n{keyA}\r\n${keyB.Length}\r\n{keyB}\r\n",
                    $"{answerOfA}*2\r\n${keyB.Length}\r\n{keyB}\r\n${keyA.Length}\r\n{keyA}\r\n"
                ]
                : [$"{answerOfA}${valueB.Length}\r\n{valueB}\r\n"];

            var endpoint = context.clusterTestUtils.GetEndPoint(replicaNodeIndex);
            var sockets = new List<Socket>();
            try
            {
                for (var i = 0; i < sessions; i++)
                {
                    var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                    sockets.Add(socket);
                    await socket.ConnectAsync(endpoint).ConfigureAwait(false);
                    _ = await socket.SendAsync(request, SocketFlags.None).ConfigureAwait(false);
                }

                var parkTimer = Stopwatch.StartNew();
                while (manager.ParkedFreshnessWaiterCount() < sessions && parkTimer.Elapsed < TimeSpan.FromSeconds(60))
                    await Task.Delay(10).ConfigureAwait(false);
                ClassicAssert.AreEqual(sessions, manager.ParkedFreshnessWaiterCount(), "Every session should be parked");
                ClassicAssert.AreEqual(0, manager.BlockedFreshnessWaiterCount(), "No session should hold a thread while it waits");

                // The sessions that are parked leave the threads free for others
                using (var fresh = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true })
                {
                    await fresh.ConnectAsync(endpoint).ConfigureAwait(false);
                    _ = await fresh.SendAsync(Encoding.ASCII.GetBytes("*1\r\n$4\r\nPING\r\n"), SocketFlags.None).ConfigureAwait(false);
                    ClassicAssert.AreEqual("+PONG\r\n", await ReceiveAsync(fresh, 7).ConfigureAwait(false));
                }
                ClassicAssert.AreEqual(sessions, manager.ParkedFreshnessWaiterCount());

                // Replay of B passes the sequence number they wait for
                manager.UpdateVirtualSublogMaxSequenceNumber(sublogB, Ahead + 1);

                var replies = await Task.WhenAll(sockets.Select(socket => ReceiveAsync(socket, expectedReplies[0].Length))).ConfigureAwait(false);
                foreach (var reply in replies)
                    CollectionAssert.Contains(expectedReplies, reply);
                ClassicAssert.AreEqual(0, manager.ParkedFreshnessWaiterCount());
            }
            finally
            {
                foreach (var socket in sockets)
                    socket.Dispose();
            }

            static async Task<string> ReceiveAsync(Socket socket, int length)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                var buffer = new byte[length];
                var received = 0;
                try
                {
                    while (received < length)
                    {
                        var count = await socket.ReceiveAsync(buffer.AsMemory(received), SocketFlags.None, timeout.Token).ConfigureAwait(false);
                        if (count == 0)
                            break;
                        received += count;
                    }
                }
                catch (OperationCanceledException)
                {
                    Assert.Fail($"A session was not answered: {Encoding.ASCII.GetString(buffer, 0, received)}");
                }
                return Encoding.ASCII.GetString(buffer, 0, received);
            }
        }
    }
}