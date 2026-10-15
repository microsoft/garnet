// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.common;
using Garnet.server;
using NUnit.Framework;
using StackExchange.Redis;
using Tsavorite.core;
using ClusterNodeRole = Garnet.cluster.NodeRole;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class PrimaryFailoverTests
    {
        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context.TearDown();

        ClusterProvider Provider(int index)
            => (ClusterProvider)context.nodes[index].Provider.StoreWrapper.clusterProvider;

        void SetupReplica(int sublogCount)
        {
            context.CreateInstances(2, disableObjects: true, enableAOF: true, sublogCount: sublogCount);
            context.CreateConnection();
            context.SimplePrimaryReplicaSetup();
            context.kvPairs = [];
            context.PopulatePrimary(ref context.kvPairs, keyLength: 16, kvpairCount: 100, primaryIndex: 0);
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            Assert.That(context.clusterTestUtils.ClusterReplicate(1, primaryId, async: true, logger: context.logger), Is.EqualTo("OK"));
            context.clusterTestUtils.BumpEpoch(1, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            context.clusterTestUtils.WaitForSlotOwnership(1, primaryId, [0, 16383], context.logger);
            context.clusterTestUtils.WaitForConfigPropagation(1, [0], context.logger);
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(4)]
        [CancelAfter(120_000)]
        public void PrimaryFailoverUsesLogTailAfterDemotion(int sublogCount)
        {
            SetupReplica(sublogCount);
            var primary = Provider(0);
            var tail = primary.storeWrapper.appendOnlyFile.Log.TailAddress;
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", 30_000).ToString(), Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForNoFailover(0, context.logger);

            Assert.That(primary.failoverManager.IsWritePaused, Is.False);
            Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA));
            Assert.That(primary.replicationManager.ReplicationOffset.EqualsAll(tail), Is.False,
                "The demoted primary's replica-tracking offset must differ from its final log tail.");
            Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY),
                "The synchronized replica must be promoted using the final log tail, not the stale replica-tracking offset.");
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1, 1);
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(4)]
        [CancelAfter(120_000)]
        public void FailStopWritesReturnsLogTailAfterDemotion(int sublogCount)
        {
            SetupReplica(sublogCount);
            var primary = Provider(0);
            var tail = primary.storeWrapper.appendOnlyFile.Log.TailAddress;
            var replicaId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            var reply = context.clusterTestUtils.GetServer(0).Execute("CLUSTER", "FAILSTOPWRITES", replicaId).ToString();

            Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA));
            Assert.That(primary.replicationManager.ReplicationOffset.EqualsAll(tail), Is.False,
                "The demoted primary's replica-tracking offset must differ from its final log tail.");
            Assert.That(reply, Is.EqualTo(tail.ToString()));
        }

        [Test, CancelAfter(120_000)]
        public void PausedPrimaryRejectsWritesButServesReads()
        {
            SetupReplica(1);
            var primary = Provider(0);
            var config = primary.clusterManager.CurrentConfig;
            var db = context.clusterTestUtils.GetDatabase();
            var key = context.kvPairs.Keys.First();
            var value = db.StringGet(key);
            primary.failoverManager.PauseWrites(true);
            try
            {
                Assert.That(primary.clusterManager.CurrentConfig, Is.SameAs(config));
                Assert.That(db.StringGet(key), Is.EqualTo(value));
                var writeError = Assert.Throws<RedisServerException>(() => db.StringSet(key, "changed"));
                Assert.That(writeError.Message, Does.StartWith("CLUSTERDOWN"));
                var multiWriteError = Assert.Throws<RedisServerException>(() => db.StringSet(
                    [new KeyValuePair<RedisKey, RedisValue>(key, "changed")]));
                Assert.That(multiWriteError.Message, Does.StartWith("CLUSTERDOWN"));
                Assert.That(db.StringGet(key), Is.EqualTo(value));
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }
            Assert.That(db.StringSet(key, value), Is.True);
        }

        [Test, CancelAfter(120_000)]
        public void ReleasingOneWritePausePreservesAnotherOwnersPause()
        {
            SetupReplica(1);
            var manager = Provider(0).failoverManager;
            var db = context.clusterTestUtils.GetDatabase();
            var key = context.kvPairs.Keys.First();
            var value = db.StringGet(key);
            manager.PauseWrites(true);
            try
            {
                manager.PauseWrites(true);
                manager.PauseWrites(false);
                Assert.That(manager.IsWritePaused, Is.True,
                    "Releasing one owner's pause must not release another owner's pause.");
                var error = Assert.Throws<RedisServerException>(() => db.StringSet(key, "changed"));
                Assert.That(error.Message, Does.StartWith("CLUSTERDOWN"));
                Assert.That(db.StringGet(key), Is.EqualTo(value));
            }
            finally
            {
                manager.PauseWrites(false);
            }
            Assert.That(manager.IsWritePaused, Is.False);
            Assert.That(db.StringSet(key, value), Is.True);
        }

        [Test, CancelAfter(120_000)]
        public void ConcurrentWritePauseOwnersPreserveOutstandingPause()
        {
            SetupReplica(1);
            var manager = Provider(0).failoverManager;
            manager.PauseWrites(true);
            try
            {
                Parallel.For(0, 1_000, _ =>
                {
                    manager.PauseWrites(true);
                    manager.PauseWrites(false);
                });
                Assert.That(manager.IsWritePaused, Is.True,
                    "Concurrent acquire/release pairs must preserve the outstanding owner's pause.");
            }
            finally
            {
                manager.PauseWrites(false);
            }
            Assert.That(manager.IsWritePaused, Is.False);
        }

        [Test, CancelAfter(120_000)]
        public void WritePauseAcquireAndReleaseAllocateZeroBytes()
        {
            SetupReplica(1);
            var manager = Provider(0).failoverManager;
            manager.PauseWrites(true);
            manager.PauseWrites(false);
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < 10_000; i++)
            {
                manager.PauseWrites(true);
                manager.PauseWrites(false);
            }
            var bytes = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.That(bytes, Is.Zero);
            Assert.That(manager.IsWritePaused, Is.False);
        }

        [TestCase(false)]
        [TestCase(true)]
        [CancelAfter(120_000)]
        public void PausedPrimaryRejectsDatabaseFlush(bool allDatabases)
        {
            SetupReplica(1);
            var primary = Provider(0);
            primary.failoverManager.PauseWrites(true);
            try
            {
                var server = context.clusterTestUtils.GetServer(0);
                var error = Assert.Throws<RedisServerException>(() =>
                {
                    if (allDatabases)
                        server.FlushAllDatabases();
                    else
                        server.FlushDatabase();
                });
                Assert.That(error.Message, Does.StartWith("CLUSTERDOWN"));
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 0, 0);
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }
        }

        [Test, CancelAfter(120_000)]
        public async Task PrimaryFailoverWaitsForEpochBeforeChangingRole()
        {
            SetupReplica(1);
            var primary = Provider(0);
            var server = (GarnetServerTcp)primary.storeWrapper.Servers[0];
            var existing = server.ActiveClusterSessions().ToHashSet();
            using var connection = new TcpClient();
            await connection.ConnectAsync((IPEndPoint)context.clusterTestUtils.GetEndPoint(0));
            var stream = connection.GetStream();
            await stream.WriteAsync("*1\r\n$4\r\nPING\r\n"u8.ToArray());
            await stream.ReadExactlyAsync(new byte[7]);
            var reader = server.ActiveClusterSessions().OfType<ClusterSession>().Single(s => !existing.Contains(s));
            Assert.That(() => reader.LocalCurrentEpoch, Is.Zero.After(5_000, 10));
            reader.AcquireCurrentEpoch();
            try
            {
                var config = primary.clusterManager.CurrentConfig;
                var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
                Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                    "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", 30_000).ToString(), Is.EqualTo("OK"));
                Assert.That(() => primary.failoverManager.IsWritePaused, Is.True.After(5_000, 10));
                Assert.That(primary.clusterManager.CurrentConfig, Is.SameAs(config),
                    "Pausing and draining clients must not advertise a role or slot handoff.");
                Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
                Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA));
            }
            finally
            {
                reader.ReleaseCurrentEpoch();
            }
            context.clusterTestUtils.WaitForNoFailover(0, context.logger);
            Assert.That(primary.failoverManager.IsWritePaused, Is.False);
            Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
        }

        async Task<ClusterSession> ConnectIdleSessionAsync(TcpClient connection, CancellationToken token)
        {
            var server = (GarnetServerTcp)Provider(0).storeWrapper.Servers[0];
            var existing = server.ActiveClusterSessions().ToHashSet();
            await connection.ConnectAsync((IPEndPoint)context.clusterTestUtils.GetEndPoint(0), token);
            var stream = connection.GetStream();
            await stream.WriteAsync("*1\r\n$4\r\nPING\r\n"u8.ToArray(), token);
            await stream.ReadExactlyAsync(new byte[7], token);
            var session = server.ActiveClusterSessions().OfType<ClusterSession>().Single(s => !existing.Contains(s));
            Assert.That(() => session.LocalCurrentEpoch, Is.Zero.After(5_000, 10));
            return session;
        }

        static unsafe PinnedSpanByte MakePinnedKey(IntPtr pointer, int length)
            => PinnedSpanByte.FromPinnedPointer((byte*)pointer, length);

        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        [CancelAfter(120_000)]
        public async Task MigrationWaitRevalidatesRecoveryBeforeStorageLookup(bool readOnly, bool demote)
        {
            SetupReplica(1);
            var provider = Provider(0);
            var manager = provider.clusterManager;
            var original = manager.CurrentConfig;
            var replicaId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            var target = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var token = deadline.Token;
            var session = await ConnectIdleSessionAsync(connection, token);
            var keyBytes = "migration-recovery-probe"u8.ToArray();
            var pin = GCHandle.Alloc(keyBytes, GCHandleType.Pinned);
            var key = MakePinnedKey(pin.AddrOfPinnedObject(), keyBytes.Length);
            var slot = HashSlotUtils.HashSlot(key);
            var sketch = new Sketch(keyCount: 64);
            sketch.HashAndStore(key);
            sketch.SetStatus(SketchStatus.DELETING);
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
            var apiField = typeof(ClusterSession).GetField("basicGarnetApi", flags);
            var savedApi = apiField.GetValue(session);
            var verify = typeof(ClusterSession).GetMethod("SingleKeySlotVerify", flags);
            MigrateSession migration = null;
            Task<object> pending = null;
            var ownsRecovery = false;
            try
            {
                Assert.That(provider.migrationManager.TryAddMigrationTask(
                    session, original.LocalNodeId, target.Address.ToString(), target.Port, replicaId,
                    null, null, false, false, 5_000, new HashSet<int> { slot }, sketch, TransferOption.KEYS, out migration), Is.True);
                Assert.That(provider.migrationManager.CanAccessKey(key, slot, readOnly), Is.False);
                var migrating = original.UpdateSlotState(slot, ClusterConfig.LOCAL_WORKER_ID, SlotState.MIGRATING);
                manager.UnsafeSetConfig(migrating);
                session.SetReadOnlySession();
                // An unintended lookup fails without accessing storage that recovery could reset.
                apiField.SetValue(session, Activator.CreateInstance(apiField.FieldType));
                object[] args = [migrating, key, readOnly, false, false, (int)slot];
                pending = Task.Run(() =>
                {
                    session.AcquireCurrentEpoch();
                    try
                    {
                        return verify.Invoke(session, args);
                    }
                    finally
                    {
                        session.ReleaseCurrentEpoch();
                    }
                });
                Assert.That(() => session.LocalCurrentEpoch, Is.Not.Zero.After(5_000, 10));
                Assert.That(pending.IsCompleted, Is.False);
                // Cross an epoch while migration blocks, proving the initial recovery check has completed.
                Assert.That(await provider.BumpAndWaitForEpochTransitionAsync().AsTask().WaitAsync(token), Is.True);
                Assert.That(pending.IsCompleted, Is.False);
                ownsRecovery = provider.replicationManager.BeginRecovery(RecoveryStatus.ClusterReplicate, upgradeLock: false);
                Assert.That(ownsRecovery, Is.True);
                if (demote)
                {
                    manager.UnsafeSetConfig(migrating.MakeReplicaOf(replicaId).AssignSlots(
                        [slot], migrating.GetWorkerIdFromNodeId(replicaId), SlotState.STABLE));
                }
                Assert.That(await provider.BumpAndWaitForEpochTransitionAsync().AsTask().WaitAsync(token), Is.True);
                sketch.SetStatus(SketchStatus.MIGRATED);
                var result = (ClusterSlotVerificationResult)await pending.WaitAsync(TimeSpan.FromSeconds(5));
                var verifiedConfig = (ClusterConfig)args[0];
                Assert.That(result.state, Is.EqualTo(verifiedConfig.LocalNodeRole == ClusterNodeRole.REPLICA
                    ? SlotVerifiedState.MOVED : SlotVerifiedState.CLUSTERDOWN));
            }
            finally
            {
                sketch.SetStatus(SketchStatus.MIGRATED);
                try
                {
                    if (pending != null)
                        _ = await pending.WaitAsync(TimeSpan.FromSeconds(5));
                }
                finally
                {
                    apiField.SetValue(session, savedApi);
                    if (ownsRecovery)
                        provider.replicationManager.EndRecovery(RecoveryStatus.NoRecovery, downgradeLock: false);
                    manager.UnsafeSetConfig(original);
                    try
                    {
                        if (migration != null)
                            Assert.That(provider.migrationManager.TryRemoveMigrationTask(migration), Is.True);
                    }
                    finally
                    {
                        if (pending == null || pending.IsCompleted)
                            pin.Free();
                    }
                }
            }
        }

        [Test, CancelAfter(120_000)]
        public async Task AbortedFailoverCleanupPreservesAnotherOwnersPause()
        {
            SetupReplica(1);
            var manager = Provider(0).failoverManager;
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var reader = await ConnectIdleSessionAsync(connection, deadline.Token);
            var sessionField = typeof(FailoverManager).GetField("currentFailoverSession", BindingFlags.Instance | BindingFlags.NonPublic);
            FailoverSession first = null;
            var ownsPause = false;
            reader.AcquireCurrentEpoch();
            try
            {
                Assert.That(manager.TryStartPrimaryFailover(endpoint.Address.ToString(), endpoint.Port,
                    FailoverOption.DEFAULT, TimeSpan.FromSeconds(30)), Is.True);
                Assert.That(() => manager.IsWritePaused, Is.True.After(5_000, 10));
                first = (FailoverSession)sessionField.GetValue(manager);
                manager.TryAbortReplicaFailover();
                manager.PauseWrites(true);
                ownsPause = true;
                reader.ReleaseCurrentEpoch();
                Assert.That(() => first.status, Is.EqualTo(FailoverStatus.NO_FAILOVER).After(10_000, 10));
                Assert.That(manager.IsWritePaused, Is.True,
                    "The aborted worker's delayed cleanup must release only its own write pause.");
            }
            finally
            {
                reader.ReleaseCurrentEpoch();
                try
                {
                    if (first != null)
                        Assert.That(() => first.status, Is.EqualTo(FailoverStatus.NO_FAILOVER).After(10_000, 10));
                }
                finally
                {
                    if (ownsPause)
                        manager.PauseWrites(false);
                }
            }
            Assert.That(manager.IsWritePaused, Is.False);
        }
    }
}