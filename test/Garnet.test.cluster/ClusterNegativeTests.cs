// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Allure.NUnit;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using System.Threading.Tasks;

using StackExchange.Redis;


#if DEBUG
using Garnet.common;
#endif

namespace Garnet.test.cluster
{
    [AllureNUnit]
    [TestFixture, NonParallelizable]
    public class ClusterNegativeTests : AllureTestBase
    {
        ClusterTestContext context;

        readonly int timeout = (int)TimeSpan.FromSeconds(30).TotalSeconds;
        const int testTimeout = 60_000;

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
        [Category("CLUSTER")]
        [TestCase("bumpepoch", new int[] { 1, 2, 3 })]
        [TestCase("failover", new int[] { 3, 4 })]
        [TestCase("forget", new int[] { 0, 3, 4 })]
        [TestCase("info", new int[] { 1, 2, 3 })]
        [TestCase("help", new int[] { 1, 2, 3 })]
        [TestCase("meet", new int[] { 0, 1, 3, 4 })]
        [TestCase("myid", new int[] { 1, 2, 3 })]
        [TestCase("myparentid", new int[] { 1, 2, 3 })]
        [TestCase("endpoint", new int[] { 0, 2, 3 })]
        [TestCase("nodes", new int[] { 1, 2, 3 })]
        [TestCase("set-config-epoch", new int[] { 0, 2, 3 })]
        [TestCase("shards", new int[] { 1, 2, 3 })]
        [TestCase("reset", new int[] { 3, 4, 5 })]
        [TestCase("addslots", new int[] { 0, 17000 })]
        [TestCase("addslotsrange", new int[] { 0, 3, 5, 7 })]
        [TestCase("banlist", new int[] { 1, 2, 3, 4 })]
        [TestCase("countkeysinslot", new int[] { 0, 2, 3 })]
        [TestCase("delslots", new int[] { 0, 1700 })]
        [TestCase("delslotsrange", new int[] { 0, 3, 5, 7 })]
        [TestCase("delkeysinslot", new int[] { 0, 2, 3, 4 })]
        [TestCase("delkeysinslotrange", new int[] { 0, 3, 5, 7 })]
        [TestCase("getkeysinslot", new int[] { 0, 1, 3, 4 })]
        [TestCase("keyslot", new int[] { 0, 2, 3, 4 })]
        [TestCase("setslot", new int[] { 0, 1, 4, 5 })]
        [TestCase("setslotsrange", new int[] { 0, 1, 2 })]
        [TestCase("slots", new int[] { 1, 2, 3 })]
        [TestCase("slotstate", new int[] { 0, 2, 3 })]
        [TestCase("MIGRATE", new int[] { 0, 1, 2, 4, 5 })]
        [TestCase("mtasks", new int[] { 1, 2, 3, 4 })]
        [TestCase("replicas", new int[] { 0, 2, 3, 4 })]
        [TestCase("replicate", new int[] { 0, 3, 4 })]
        [TestCase("AOFSYNC", new int[] { 0, 1, 3, 4 })]
        [TestCase("APPENDLOG", new int[] { 0, 1, 2, 3, 4, 6 })]
        [TestCase("INITIATE_REPLICA_SYNC", new int[] { 0, 1, 2, 3, 4, 6 })]
        [TestCase("SEND_CKPT_METADATA", new int[] { 0, 1, 2, 4, 5, 6 })]
        [TestCase("SEND_CKPT_FILE_SEGMENT", new int[] { 0, 1, 2, 3, 4, 6 })]
        [TestCase("BEGIN_REPLICA_RECOVER", new int[] { 0, 1, 2, 3, 4, 5, 6, 8, 9 })]
        [TestCase("FAILSTOPWRITES", new int[] { 0, 2, 3, 4 })]
        [TestCase("FAILREPLICATIONOFFSET", new int[] { 0, 2, 3, 4 })]
        public void ClusterCommandWrongParameters(string subcommand, params int[] invalidCount)
        {
            context.CreateInstances(1);

            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            socket.Connect(IPAddress.Loopback, 7000);

            var clusterCMD = $"$7\r\ncluster\r\n${subcommand.Length}\r\n{subcommand}\r\n";
            var errorCmd = $"cluster|{subcommand.ToLowerInvariant()}";

            var expectedResp = $"-ERR wrong number of arguments for '{errorCmd}' command\r\n";
            foreach (var count in invalidCount)
            {
                var packet = $"*{2 + count}\r\n" + clusterCMD;
                for (var i = 0; i < count; i++)
                    packet += $"$3\r\nabc\r\n";

                var buffer = new byte[1024];
                var packetBytes = Encoding.ASCII.GetBytes(packet);
                var sent = socket.Send(packetBytes);
                ClassicAssert.AreEqual(packetBytes.Length, sent);
                int read;
                if ((read = socket.Receive(buffer)) > 0)
                {
                    var resp = Encoding.ASCII.GetString(buffer, 0, read);
                    ClassicAssert.AreEqual(expectedResp, resp);
                    break;
                }
            }
        }

        [Test, Order(2)]
        [Category("CLUSTER")]
        [TestCase(1024)]
        [TestCase(10240)]
        public void ClusterAddSlotsPartialPackage(int chunkSize)
        {
            context.CreateInstances(1);
            using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
            socket.NoDelay = true;
            socket.Connect(IPAddress.Loopback, 7000);

            var slots = Enumerable.Range(0, 8192).ToList();
            var packet = $"*{2 + slots.Count}\r\n$7\r\ncluster\r\n$8\r\naddslots\r\n";

            foreach (var slot in slots)
                packet += $"${slot.ToString().Length}\r\n{slot}\r\n";

            Span<byte> packetBytes = Encoding.ASCII.GetBytes(packet);
            for (var i = 0; i < packetBytes.Length; i += chunkSize)
            {
                var size = i + chunkSize < packetBytes.Length ? chunkSize : packetBytes.Length - i;
                var slicePacket = packetBytes.Slice(i, size);
                var sent = socket.Send(slicePacket);
                ClassicAssert.AreEqual(slicePacket.Length, sent);
                Thread.Sleep(100);
            }

            var buffer = new byte[1024];
            int read;
            if ((read = socket.Receive(buffer)) > 0)
            {
                var resp = Encoding.ASCII.GetString(buffer, 0, read);
                ClassicAssert.AreEqual("+OK\r\n", resp);
            }
        }

#if DEBUG
        [Test, Order(3)]
        public void ClusterExceptionInjectionAtPrimarySyncSession([Values] bool enableDisklessSync)
        {
            ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Fail_Before_Background_AOF_Stream_Task_Start);

            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = 2;
            context.CreateInstances(nodes_count, disableObjects: false, enableAOF: true, enableDisklessSync: enableDisklessSync, timeout: timeout);
            context.CreateConnection();

            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            // Ensure node is known
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, replicaIndex, logger: context.logger);

            var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, logger: context.logger);
            ClassicAssert.AreEqual($"Exception injection triggered {ExceptionInjectionType.Replication_Fail_Before_Background_AOF_Stream_Task_Start}", resp);

            var role = context.clusterTestUtils.RoleCommand(replicaIndex, logger: context.logger);
            ClassicAssert.AreEqual("master", role.Value);

            resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, logger: context.logger);
            ClassicAssert.AreEqual($"Exception injection triggered {ExceptionInjectionType.Replication_Fail_Before_Background_AOF_Stream_Task_Start}", resp);

            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Fail_Before_Background_AOF_Stream_Task_Start);
        }
#endif

        [Test, Order(4), CancelAfter(testTimeout)]
        public void ClusterFailoverDuringRecovery(CancellationToken cancellationToken)
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = 2;
            context.CreateInstances(nodes_count, disableObjects: false, enableAOF: true, enableDisklessSync: true, timeout: timeout, replicaDisklessSyncDelay: 10);
            context.CreateConnection();

            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            // Ensure node is known
            context.clusterTestUtils.WaitUntilNodeIsKnown(primaryIndex, replicaIndex, logger: context.logger);

            // Issue background replicate
            var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, async: true, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            var infoItem = context.clusterTestUtils.GetReplicationInfo(replicaIndex, [ReplicationInfoItem.RECOVER_STATUS], logger: context.logger);
            ClassicAssert.AreEqual("ClusterReplicate", infoItem[0].Item2);

            // Issue failover at the same time
            resp = context.clusterTestUtils.ClusterFailover(replicaIndex, "TAKEOVER", logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            // Wait until failover gets aborted
            while (true)
            {
                infoItem = context.clusterTestUtils.GetReplicationInfo(replicaIndex, [ReplicationInfoItem.LAST_FAILOVER_STATE], logger: context.logger);
                if (infoItem[0].Item2.Equals("failover-aborted"))
                    break;
                ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: "Waiting for last failover to abort");
            }
            ClassicAssert.AreEqual("failover-aborted", infoItem[0].Item2);

            // Wait until replicate completes
            while (true)
            {
                infoItem = context.clusterTestUtils.GetReplicationInfo(replicaIndex, [ReplicationInfoItem.RECOVER_STATUS], logger: context.logger);
                if (infoItem[0].Item2.Equals("NoRecovery"))
                    break;
                ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: "Waiting for replicate to complete");
            }

            // Issue failover again
            resp = context.clusterTestUtils.ClusterFailover(replicaIndex, "TAKEOVER", logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            // Wait until failover completes
            while (true)
            {
                infoItem = context.clusterTestUtils.GetReplicationInfo(replicaIndex, [ReplicationInfoItem.LAST_FAILOVER_STATE], logger: context.logger);
                if (infoItem[0].Item2.Equals("failover-completed"))
                    break;
                ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: "Waiting for last failover to complete");
            }
            ClassicAssert.AreEqual("failover-completed", infoItem[0].Item2);
        }

#if DEBUG
        [Test, Order(5)]
        public void ClusterCheckpointAcquireTest([Values] bool fastAofTruncate, [Values] bool onDemandCheckpoint)
        {
            try
            {
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Acquire_Checkpoint_Entry_Fail_Condition);

                var primaryIndex = 0;
                var replicaIndex = 1;
                var nodes_count = 2;
                context.CreateInstances(nodes_count, disableObjects: false, enableAOF: true, timeout: timeout, FastAofTruncate: fastAofTruncate, OnDemandCheckpoint: onDemandCheckpoint, CommitFrequencyMs: fastAofTruncate ? -1 : 0);
                context.CreateConnection();

                _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
                context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
                context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
                context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

                var keyLength = 32;
                var kvpairCount = 32;
                context.kvPairs = [];
                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex, null);

                // Take a checkpoint to create an in-memory entry
                var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
                context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);

                // Try to attach to trigger checkpoint acquire condition
                var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, logger: context.logger);
                context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Acquire_Checkpoint_Entry_Fail_Condition);
            }
        }

        [Test, Order(6), CancelAfter(testTimeout)]
        public void ClusterReplicaAttachIntenseWrite(CancellationToken cancellationToken)
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = 2;
            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                timeout: timeout,
                OnDemandCheckpoint: true,
                FastAofTruncate: true,
                CommitFrequencyMs: -1,
                useAofNullDevice: true);
            context.CreateConnection();

            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            var keyLength = 32;
            var kvpairCount = 32;
            context.kvPairs = [];
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex, null);
            var primaryOffset1 = context.clusterTestUtils.GetReplicationOffset(primaryIndex, logger: context.logger);

            // Take a checkpoint to create an in-memory entry
            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);

            try
            {
                // Set wait condition
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Wait_After_Checkpoint_Acquisition);

                // Issue replicate
                var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, async: true, logger: context.logger);
                ClassicAssert.AreEqual("OK", resp);

                // Wait for replica to acquire previous checkpoint
                while (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.Replication_Wait_After_Checkpoint_Acquisition))
                {
                    ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: "Waiting for exception reset signal");
                }

                context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex, null);
                var primaryOffset2 = context.clusterTestUtils.GetReplicationOffset(primaryIndex, logger: context.logger);
                ClassicAssert.Less(primaryOffset1, primaryOffset2);

                // Take another checkpoin to truncate
                primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
                context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
                context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);

                // Re-enable to signal replica to continue
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Wait_After_Checkpoint_Acquisition);

                // Wait for replica to take a new checkpoint after detecting the simulated data loss
                while (ExceptionInjectionHelper.IsEnabled(ExceptionInjectionType.Replication_Wait_After_Checkpoint_Acquisition))
                {
                    ClusterTestUtils.BackOff(cancellationToken: cancellationToken, msg: "Waiting for exception reset signal");
                }

                // Re-enable to signal replica to continue
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Wait_After_Checkpoint_Acquisition);

                context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);
                context.clusterTestUtils.WaitForReplicaRecovery(replicaIndex, context.logger);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Wait_After_Checkpoint_Acquisition);
            }
        }

        [Test, Order(6), CancelAfter(testTimeout)]
        public void ClusterFailedToAddAofSyncTask()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = 2;
            context.CreateInstances(nodes_count, disableObjects: false, enableAOF: true, timeout: timeout);
            context.CreateConnection();

            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            var keyLength = 32;
            var kvpairCount = 32;
            context.kvPairs = [];
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex, null);
            var primaryOffset1 = context.clusterTestUtils.GetReplicationOffset(primaryIndex, logger: context.logger);

            // Take a checkpoint to create an in-memory entry
            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);

            var replicaId = context.clusterTestUtils.ClusterMyId(replicaIndex, logger: context.logger);
            var endpoint = context.clusterTestUtils.GetEndPoint(replicaIndex);
            try
            {
                // Set wait condition
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Failed_To_AddAofSyncTask_UnknownNode);

                var respReplicate = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, logger: context.logger);
                var expected = $"Failed to create AOF sync task for {replicaId} with address {endpoint.Address} and port {endpoint.Port}";
                ClassicAssert.AreEqual(expected, respReplicate);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Failed_To_AddAofSyncTask_UnknownNode);
            }

            var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            context.clusterTestUtils.WaitForReplicaRecovery(replicaIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex);
        }

        [Test, Order(6), CancelAfter(testTimeout)]
        public void ClusterReplicaSyncTimeoutTest()
        {
            var primaryIndex = 0;
            var replicaIndex = 1;
            var nodes_count = 2;
            context.CreateInstances(nodes_count, disableObjects: false, enableAOF: true, timeout: timeout, replicaSyncTimeout: 1);
            context.CreateConnection();

            _ = context.clusterTestUtils.AddDelSlotsRange(primaryIndex, [(0, 16383)], addslot: true, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(primaryIndex, primaryIndex + 1, logger: context.logger);
            context.clusterTestUtils.SetConfigEpoch(replicaIndex, replicaIndex + 1, logger: context.logger);
            context.clusterTestUtils.Meet(primaryIndex, replicaIndex, logger: context.logger);

            var keyLength = 32;
            var kvpairCount = 32;
            context.kvPairs = [];
            context.PopulatePrimary(ref context.kvPairs, keyLength, kvpairCount, primaryIndex, null);
            var primaryOffset1 = context.clusterTestUtils.GetReplicationOffset(primaryIndex, logger: context.logger);

            // Take a checkpoint to create an in-memory entry
            var primaryLastSaveTime = context.clusterTestUtils.LastSave(primaryIndex, logger: context.logger);
            context.clusterTestUtils.Checkpoint(primaryIndex, logger: context.logger);
            context.clusterTestUtils.WaitCheckpoint(primaryIndex, primaryLastSaveTime, logger: context.logger);

            try
            {
                // Set wait condition
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Timeout_On_Receive_Checkpoint);

                var respReplicate = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, logger: context.logger);
                var expected = "The operation has timed out.";
                ClassicAssert.AreEqual(expected, respReplicate);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Timeout_On_Receive_Checkpoint);
            }

            var resp = context.clusterTestUtils.ClusterReplicate(replicaNodeIndex: replicaIndex, primaryNodeIndex: primaryIndex, failEx: false, logger: context.logger);
            ClassicAssert.AreEqual("OK", resp);

            context.clusterTestUtils.WaitForReplicaRecovery(replicaIndex, context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(primaryIndex, replicaIndex, context.logger);

            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, replicaIndex);
        }
#endif

        [Test, CancelAfter(60_000)]
        public async Task ClusterParallelFailoverOnDistinctShards(CancellationToken cancellationToken)
        {
            var nodes_count = 4;
            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                timeout: timeout,
                OnDemandCheckpoint: true,
                FastAofTruncate: true,
                CommitFrequencyMs: -1,
                useAofNullDevice: true);
            context.CreateConnection();

            context.clusterTestUtils.SetConfigEpoch(0, 1);
            context.clusterTestUtils.SetConfigEpoch(1, 2);
            context.clusterTestUtils.SetConfigEpoch(2, 3);
            context.clusterTestUtils.SetConfigEpoch(3, 4);

            context.clusterTestUtils.AddSlotsRange(0, [(0, 8191)], logger: context.logger);
            context.clusterTestUtils.AddSlotsRange(1, [(8192, 16383)], logger: context.logger);

            context.clusterTestUtils.Meet(0, 1, logger: context.logger);
            context.clusterTestUtils.Meet(0, 2, logger: context.logger);
            context.clusterTestUtils.Meet(0, 3, logger: context.logger);

            context.clusterTestUtils.WaitClusterNodesSync(0, 4, context.logger);

            context.clusterTestUtils.ClusterReplicate(2, 0, logger: context.logger);
            context.clusterTestUtils.ClusterReplicate(3, 1, logger: context.logger);

            context.clusterTestUtils.WaitClusterNodesSync(0, 4, context.logger);
            context.clusterTestUtils.WaitClusterNodesSync(1, 4, context.logger);
            context.clusterTestUtils.WaitClusterNodesSync(2, 4, context.logger);
            context.clusterTestUtils.WaitClusterNodesSync(3, 4, context.logger);

            Assert.That("OK" == context.clusterTestUtils.ClusterFailover(2));
            Assert.That("OK" == context.clusterTestUtils.ClusterFailover(3));

            context.clusterTestUtils.WaitForNoFailover(2);
            context.clusterTestUtils.WaitForNoFailover(3);

            for (var i = 0; i < 10; i++)
            {
                await Task.Delay(500);
                var cluster = context.clusterTestUtils.ClusterNodes(3);
                var replicaWithHashSlots = cluster.Nodes.FirstOrDefault(x => x.IsReplica && x.Slots.Count > 0);
                if (replicaWithHashSlots != null)
                {
                    Assert.Fail($"There should be no replica with assigned hashslots.{context.clusterTestUtils.ClusterStatus([0, 1, 2, 3])}");
                }
            }
        }


        [Test, CancelAfter(60_000)]
        public void ClusterMeetFromReplica(CancellationToken cancellationToken)
        {
            var nodes_count = 3;
            context.CreateInstances(
                nodes_count,
                disableObjects: false,
                enableAOF: true,
                timeout: timeout,
                OnDemandCheckpoint: true,
                FastAofTruncate: true,
                CommitFrequencyMs: -1,
                useAofNullDevice: true);
            context.CreateConnection();

            context.clusterTestUtils.SetConfigEpoch(0, 1);
            context.clusterTestUtils.SetConfigEpoch(1, 2);
            context.clusterTestUtils.SetConfigEpoch(2, 3);
            context.clusterTestUtils.Meet(0, 1, logger: context.logger);
            context.clusterTestUtils.WaitForConfigPropagation(1, [0, 1], null);

            context.clusterTestUtils.ClusterReplicate(1, 0, logger: context.logger);
            context.clusterTestUtils.WaitForConfigPropagation(1, [0, 1], null);

            context.clusterTestUtils.AddSlotsRange(0, [(0, 16383)], logger: context.logger);
            context.clusterTestUtils.WaitForConfigPropagation(0, [0, 1], null);

            context.clusterTestUtils.Meet(2, 1, logger: context.logger);
            context.clusterTestUtils.WaitForConfigPropagation(2, [0, 1, 2], null);

            for (int i = 0; i < nodes_count; i++)
            {
                Assert.That(nodes_count, Is.EqualTo(context.clusterTestUtils.ClusterNodes(i).Nodes.Count));
            }
        }
    }
}