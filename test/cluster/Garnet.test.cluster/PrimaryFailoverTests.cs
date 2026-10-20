// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
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
            context.clusterTestUtils.WaitForNoFailover(1, context.logger);

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
        public async Task PausedWritesResumeOrRedirectAfterFailover([Values] bool multiKey, [Values] bool failover, [Values] bool readOnly)
        {
            SetupReplica(1);
            var primary = Provider(0);
            var config = primary.clusterManager.CurrentConfig;
            var db = context.clusterTestUtils.GetDatabase();
            var key = context.kvPairs.Keys.First();
            var value = db.StringGet(key);
            using var connection = new TcpClient();
            using var lateConnection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var session = await ConnectIdleSessionAsync(connection, deadline.Token);
            var mode = readOnly ? "READONLY" : "READWRITE";
            Assert.That(await ExecuteCommandAsync(connection, $"*1\r\n${mode.Length}\r\n{mode}\r\n", deadline.Token), Is.EqualTo("+OK"));
            Assert.That(session.ReadOnlySession, Is.EqualTo(readOnly));
            Assert.That(session.IsInternalWriteSession, Is.False);
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            Task<string> response = null;
            primary.failoverManager.PauseWrites(true);
            try
            {
                Assert.That(primary.clusterManager.CurrentConfig, Is.SameAs(config));
                var command = multiKey ? "MSET" : "SET";
                response = await StartPausedCommandAsync(session, connection,
                    $"*3\r\n${command.Length}\r\n{command}\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n", deadline.Token);
                Assert.That(db.StringGet(key), Is.EqualTo(value));
                Assert.That(await primary.BumpAndWaitForEpochTransitionAsync().AsTask().WaitAsync(deadline.Token), Is.True,
                    "A paused writer must release its epoch so failover can progress.");
                if (failover)
                {
                    Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                        "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", 20_000).ToString(), Is.EqualTo("OK"));
                    context.clusterTestUtils.WaitForNoFailover(0, context.logger);
                    context.clusterTestUtils.WaitForNoFailover(1, context.logger);
                    Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
                    Assert.That(response.IsCompleted, Is.False,
                        "A writer must wait until every pause owner releases its pause, even after demotion.");
                    var lateSession = await ConnectIdleSessionAsync(lateConnection, deadline.Token);
                    Assert.That(await ExecuteCommandAsync(lateConnection, $"*1\r\n${mode.Length}\r\n{mode}\r\n", deadline.Token), Is.EqualTo("+OK"));
                    Assert.That(lateSession.IsInternalWriteSession, Is.False);
                    Assert.That(await ExecuteCommandAsync(lateConnection,
                        $"*3\r\n${command.Length}\r\n{command}\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n", deadline.Token),
                        Is.EqualTo($"-MOVED {HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key))} {endpoint.Address}:{endpoint.Port}"));
                    Assert.That(primary.failoverManager.IsWritePaused, Is.True,
                        "A write arriving on a replica must redirect before the outstanding pause is released.");
                    Assert.That(response.IsCompleted, Is.False);
                }
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }
            Assert.That(await response.WaitAsync(deadline.Token), Is.EqualTo(failover
                ? $"-MOVED {HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key))} {endpoint.Address}:{endpoint.Port}"
                : "+OK"));
            Assert.That(db.StringGet(key),
                Is.EqualTo(failover ? value : (RedisValue)"changed"));
        }

        [TestCase(false)]
        [TestCase(true)]
        [CancelAfter(120_000)]
        public async Task InternalReplayBypassesPauseRegardlessOfSessionReadMode(bool demote)
        {
            SetupReplica(1);
            var primary = Provider(0);
            var key = context.kvPairs.Keys.First();
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var session = await ConnectIdleSessionAsync(connection, deadline.Token);
            session.SetInternalWriteSession();
            primary.failoverManager.PauseWrites(true);
            try
            {
                if (demote)
                    primary.clusterManager.TryStopWrites(context.clusterTestUtils.GetNodeIdFromNode(1, context.logger));
                foreach (var mode in new[] { "READONLY", "READWRITE" })
                {
                    Assert.That(await ExecuteCommandAsync(connection, $"*1\r\n${mode.Length}\r\n{mode}\r\n", deadline.Token), Is.EqualTo("+OK"));
                    Assert.That(session.ReadOnlySession, Is.EqualTo(mode == "READONLY"));
                    Assert.That(session.IsInternalWriteSession, Is.True,
                        "Connection read-mode commands must not remove the internal replay exemption.");
                    Assert.That(await ExecuteCommandAsync(connection,
                        $"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n", deadline.Token), Is.EqualTo("+OK"));
                    Assert.That(primary.failoverManager.IsWritePaused, Is.True);
                }
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }
        }

        [Test, CancelAfter(120_000)]
        public async Task ReleasingOneWritePausePreservesAnotherOwnersPause()
        {
            SetupReplica(1);
            var manager = Provider(0).failoverManager;
            var db = context.clusterTestUtils.GetDatabase();
            var key = context.kvPairs.Keys.First();
            var value = db.StringGet(key);
            using var connection = new TcpClient();
            using var secondConnection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var session = await ConnectIdleSessionAsync(connection, deadline.Token);
            var secondSession = await ConnectIdleSessionAsync(secondConnection, deadline.Token);
            Task<string> response = null;
            Task<string> secondResponse = null;
            manager.PauseWrites(true);
            try
            {
                manager.PauseWrites(true);
                manager.PauseWrites(false);
                Assert.That(manager.IsWritePaused, Is.True,
                    "Releasing one owner's pause must not release another owner's pause.");
                response = await StartPausedCommandAsync(session, connection,
                    $"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n", deadline.Token);
                secondResponse = await StartPausedCommandAsync(secondSession, secondConnection,
                    $"*3\r\n$4\r\nMSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n", deadline.Token);
                Assert.That(response.IsCompleted, Is.False);
                Assert.That(db.StringGet(key), Is.EqualTo(value));
            }
            finally
            {
                manager.PauseWrites(false);
            }
            Assert.That(manager.IsWritePaused, Is.False);
            Assert.That(await response.WaitAsync(deadline.Token), Is.EqualTo("+OK"));
            Assert.That(await secondResponse.WaitAsync(deadline.Token), Is.EqualTo("+OK"));
            Assert.That(db.StringGet(key), Is.EqualTo((RedisValue)"changed"));
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

        [Test, CancelAfter(120_000)]
        public async Task PausedDatabaseFlushRevalidatesRoleAfterResume([Values] bool allDatabases, [Values] bool demote, [Values] bool readOnly)
        {
            SetupReplica(1);
            var primary = Provider(0);
            using var connection = new TcpClient();
            using var lateConnection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var session = await ConnectIdleSessionAsync(connection, deadline.Token);
            var mode = readOnly ? "READONLY" : "READWRITE";
            Assert.That(await ExecuteCommandAsync(connection, $"*1\r\n${mode.Length}\r\n{mode}\r\n", deadline.Token), Is.EqualTo("+OK"));
            Task<string> response = null;
            primary.failoverManager.PauseWrites(true);
            try
            {
                var command = allDatabases ? "FLUSHALL" : "FLUSHDB";
                response = await StartPausedCommandAsync(session, connection,
                    $"*1\r\n${command.Length}\r\n{command}\r\n", deadline.Token);
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 0, 0);
                if (demote)
                {
                    primary.clusterManager.TryStopWrites(context.clusterTestUtils.GetNodeIdFromNode(1, context.logger));
                    _ = await ConnectIdleSessionAsync(lateConnection, deadline.Token);
                    Assert.That(await ExecuteCommandAsync(lateConnection, $"*1\r\n${mode.Length}\r\n{mode}\r\n", deadline.Token), Is.EqualTo("+OK"));
                    Assert.That(await ExecuteCommandAsync(lateConnection,
                        $"*1\r\n${command.Length}\r\n{command}\r\n", deadline.Token), Is.EqualTo("-ERR You can't write against a read only replica."));
                    Assert.That(primary.failoverManager.IsWritePaused, Is.True);
                    Assert.That(response.IsCompleted, Is.False);
                }
                Assert.That(await primary.BumpAndWaitForEpochTransitionAsync().AsTask().WaitAsync(deadline.Token), Is.True);
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }
            Assert.That(await response.WaitAsync(deadline.Token), Is.EqualTo(demote
                ? "-ERR You can't write against a read only replica." : "+OK"));
            if (demote)
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 0, 0);
            else
                Assert.That(context.clusterTestUtils.GetServer(0).DatabaseSize(), Is.Zero);
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
            context.clusterTestUtils.WaitForNoFailover(1, context.logger);
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

        static async Task<Task<string>> StartPausedCommandAsync(ClusterSession session, TcpClient connection, string command, CancellationToken token)
        {
            // Mark the session active so zero proves the command has released its epoch.
            session.AcquireCurrentEpoch();
            var stream = connection.GetStream();
            await stream.WriteAsync(Encoding.ASCII.GetBytes(command), token);
            var response = ReadResponseAsync(stream, token);
            Assert.That(() => session.LocalCurrentEpoch, Is.Zero.After(5_000, 10));
            Assert.That(() => response.IsCompleted, Is.False.After(100),
                "The command must stay pending while writes are paused.");
            return response;
        }

        static async Task<string> ExecuteCommandAsync(TcpClient connection, string command, CancellationToken token)
        {
            var stream = connection.GetStream();
            await stream.WriteAsync(Encoding.ASCII.GetBytes(command), token);
            return await ReadResponseAsync(stream, token);
        }

        static async Task<string> ReadResponseAsync(NetworkStream stream, CancellationToken token)
        {
            using var reader = new StreamReader(stream, Encoding.ASCII, leaveOpen: true);
            return await reader.ReadLineAsync(token);
        }

        static unsafe PinnedSpanByte MakePinnedKey(IntPtr pointer, int length)
            => PinnedSpanByte.FromPinnedPointer((byte*)pointer, length);

        [Test, CancelAfter(120_000)]
        public async Task PrimaryFailoverDrainsPostPauseSessionsBeforeTakeover()
        {
            SetupReplica(1);
            var primary = Provider(0);
            using var firstConnection = new TcpClient();
            using var incomingConnection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var first = await ConnectIdleSessionAsync(firstConnection, deadline.Token);
            var incoming = await ConnectIdleSessionAsync(incomingConnection, deadline.Token);
            first.AcquireCurrentEpoch();
            try
            {
                var initialEpoch = first.LocalCurrentEpoch;
                var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
                Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                    "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", 30_000).ToString(), Is.EqualTo("OK"));
                Assert.That(() => primary.garnetEpoch.GetCurrentEpoch(), Is.GreaterThan(initialEpoch).After(5_000, 10));
                Assert.That(primary.failoverManager.IsWritePaused, Is.True);

                // This request can retain the primary snapshot without blocking the initial pause barrier.
                incoming.AcquireCurrentEpoch();
                var incomingEpoch = incoming.LocalCurrentEpoch;
                var config = primary.clusterManager.CurrentConfig;
                Assert.That(config.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
                Assert.That(incomingEpoch, Is.GreaterThan(initialEpoch));
                first.ReleaseCurrentEpoch();

                Assert.That(() => primary.clusterManager.CurrentConfig.LocalNodeRole,
                    Is.EqualTo(ClusterNodeRole.REPLICA).After(5_000, 10));
                Assert.That(() => primary.garnetEpoch.GetCurrentEpoch(), Is.GreaterThan(incomingEpoch).After(5_000, 10),
                    "Demotion must start a new epoch barrier for requests that entered after the pause.");
                Assert.That(primary.failoverManager.IsWritePaused, Is.True);
                Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA),
                    "The selected replica must not promote while a request retains the old primary snapshot.");

                var keyBytes = "handoff-paused-write"u8.ToArray();
                var pin = GCHandle.Alloc(keyBytes, GCHandleType.Pinned);
                try
                {
                    var key = MakePinnedKey(pin.AddrOfPinnedObject(), keyBytes.Length);
                    var verify = typeof(ClusterSession).GetMethod("SingleKeySlotVerify", BindingFlags.Instance | BindingFlags.NonPublic);
                    object[] args = [config, key, false, false, false, (int)HashSlotUtils.HashSlot(key)];
                    var pending = Task.Run(() => (ClusterSlotVerificationResult)verify.Invoke(incoming, args));
                    var result = await pending.WaitAsync(deadline.Token);
                    Assert.That(result.state, Is.EqualTo(SlotVerifiedState.MOVED),
                        "A stale primary snapshot must be refreshed after the pause instead of rejecting the write.");
                    Assert.That(((ClusterConfig)args[0]).LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA));
                }
                finally
                {
                    pin.Free();
                }
            }
            finally
            {
                first.ReleaseCurrentEpoch();
                incoming.ReleaseCurrentEpoch();
            }

            context.clusterTestUtils.WaitForNoFailover(0, context.logger);
            context.clusterTestUtils.WaitForNoFailover(1, context.logger);
            Assert.That(primary.failoverManager.IsWritePaused, Is.False);
            Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA));
            Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
        }

        static unsafe string VerifyMultiKeyResponse(ClusterSession session, PinnedSpanByte key, bool readOnly, bool isTxn)
        {
            var parseState = new SessionParseState();
            parseState.InitializeWithArguments(key, key);
            Assert.That(RespCommandsInfo.TryGetSimpleRespCommandInfo(readOnly ? RespCommand.MGET : RespCommand.DEL, out var commandInfo), Is.True);
            var input = new ClusterSlotVerificationInput { readOnly = readOnly, keySpecs = commandInfo.KeySpecs };
            byte* buffer = stackalloc byte[256];
            var current = buffer;
            var end = buffer + 256;
            Assert.That(session.NetworkMultiKeySlotVerify(ref parseState, ref input, ref current, ref end, isTxn), Is.True);
            return Encoding.ASCII.GetString(new ReadOnlySpan<byte>(buffer, (int)(current - buffer)));
        }

        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        [CancelAfter(120_000)]
        public Task MigrationWaitRevalidatesRecoveryBeforeStorageLookup(bool readOnly, bool demote)
            => VerifyMigrationWaitAsync(readOnly, demote, isTxn: false, verifyResponse: false);

        [TestCase(false, false)]
        [TestCase(false, true)]
        [TestCase(true, false)]
        [TestCase(true, true)]
        [CancelAfter(120_000)]
        public Task MigrationWaitRedirectsToNewPrimary(bool readOnly, bool isTxn)
            => VerifyMigrationWaitAsync(readOnly, demote: true, isTxn, verifyResponse: true);

        async Task VerifyMigrationWaitAsync(bool readOnly, bool demote, bool isTxn, bool verifyResponse)
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
                if (!verifyResponse)
                    session.SetReadOnlySession();
                // An unintended lookup fails without accessing storage that recovery could reset.
                apiField.SetValue(session, Activator.CreateInstance(apiField.FieldType));
                object[] args = [migrating, key, readOnly, false, false, (int)slot];
                pending = Task.Run(() =>
                {
                    session.AcquireCurrentEpoch();
                    try
                    {
                        return verifyResponse ? VerifyMultiKeyResponse(session, key, readOnly, isTxn) : verify.Invoke(session, args);
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
                if (!verifyResponse)
                {
                    ownsRecovery = provider.replicationManager.BeginRecovery(RecoveryStatus.ClusterReplicate, upgradeLock: false);
                    Assert.That(ownsRecovery, Is.True);
                }
                if (demote)
                {
                    manager.UnsafeSetConfig(migrating.MakeReplicaOf(replicaId).AssignSlots(
                        [slot], migrating.GetWorkerIdFromNodeId(replicaId), SlotState.STABLE));
                }
                Assert.That(await provider.BumpAndWaitForEpochTransitionAsync().AsTask().WaitAsync(token), Is.True);
                sketch.SetStatus(SketchStatus.MIGRATED);
                var result = await pending.WaitAsync(TimeSpan.FromSeconds(5));
                if (verifyResponse)
                {
                    Assert.That((string)result, Is.EqualTo($"-MOVED {slot} {target.Address}:{target.Port}\r\n"),
                        "The response must use the configuration refreshed after the migration wait, not the old primary endpoint.");
                }
                else
                {
                    var verifiedConfig = (ClusterConfig)args[0];
                    Assert.That(((ClusterSlotVerificationResult)result).state, Is.EqualTo(verifiedConfig.LocalNodeRole == ClusterNodeRole.REPLICA
                        ? SlotVerifiedState.MOVED : SlotVerifiedState.CLUSTERDOWN));
                }
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
            using var writerConnection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var reader = await ConnectIdleSessionAsync(connection, deadline.Token);
            var writer = await ConnectIdleSessionAsync(writerConnection, deadline.Token);
            var key = context.kvPairs.Keys.First();
            Task<string> response = null;
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
                response = await StartPausedCommandAsync(writer, writerConnection,
                    $"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n", deadline.Token);
                manager.TryAbortReplicaFailover();
                manager.PauseWrites(true);
                ownsPause = true;
                reader.ReleaseCurrentEpoch();
                Assert.That(() => first.status, Is.EqualTo(FailoverStatus.NO_FAILOVER).After(10_000, 10));
                Assert.That(manager.IsWritePaused, Is.True,
                    "The aborted worker's delayed cleanup must release only its own write pause.");
                Assert.That(response.IsCompleted, Is.False);
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
            Assert.That(await response.WaitAsync(deadline.Token), Is.EqualTo("+OK"));
            Assert.That(context.clusterTestUtils.GetDatabase().StringGet(key), Is.EqualTo((RedisValue)"changed"));
        }
    }
}