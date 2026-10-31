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
#if DEBUG
using Garnet.cluster.Server.Replication;
#endif
using Garnet.common;
using Garnet.networking;
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

        void SetupReplica(int sublogCount, bool disableObjects = true, bool enableIndexGrowth = false, bool enableLua = false)
        {
            context.CreateInstances(2, disableObjects: disableObjects, enableAOF: true, sublogCount: sublogCount, enableLua: enableLua);
            if (enableIndexGrowth)
                EnableIndexGrowth(0);
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

        void EnableIndexGrowth(int index)
        {
            context.ShutdownNode(index);
            context.nodeOptions[index].IndexMaxMemorySize = "1g";
            context.nodeOptions[index].IndexResizeFrequencySecs = 3600;
            context.RestartNode(index);
            Assert.That(Provider(index).storeWrapper.TaskManager.IsRunning(TaskType.IndexAutoGrowTask), Is.True);
        }

        [Test, CancelAfter(120_000)]
        public async Task CancelingFailoverPreservesIndexGrowth([Values] bool timeout)
        {
            SetupReplica(1, enableIndexGrowth: true);
            var primary = Provider(0);
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var guard = await ConnectIdleSessionAsync(connection, deadline.Token);
            guard.AcquireCurrentEpoch();
            try
            {
                var epoch = guard.LocalCurrentEpoch;
                var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
                Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                    "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", timeout ? 1000 : 10_000).ToString(),
                    Is.EqualTo("OK"));
                Assert.That(() => primary.garnetEpoch.GetCurrentEpoch(), Is.GreaterThan(epoch).After(5_000, 10));
                Assert.That(primary.storeWrapper.TaskManager.IsRunning(TaskType.IndexAutoGrowTask), Is.True,
                    "Draining primary tasks must not stop role-independent index growth.");
                if (!timeout)
                    Assert.That(context.clusterTestUtils.GetServer(0).Execute("FAILOVER", "ABORT").ToString(), Is.EqualTo("OK"));
                Assert.That(() => primary.storeWrapper.TaskManager.IsRegistered(TaskType.FailoverTask), Is.False.After(5_000, 10));
                Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
                Assert.That(primary.storeWrapper.TaskManager.IsRunning(TaskType.IndexAutoGrowTask), Is.True);
            }
            finally
            {
                guard.ReleaseCurrentEpoch();
                primary.failoverManager.TryAbortReplicaFailover();
                await primary.storeWrapper.TaskManager.CancelAsync(TaskType.FailoverTask).WaitAsync(deadline.Token);
            }
        }

        [Test, CancelAfter(120_000)]
        public void ReplicationAndPromotionPreserveIndexGrowth([Values] bool diskless)
        {
            context.CreateInstances(2, enableAOF: true, CommitFrequencyMs: 100, enableDisklessSync: diskless);
            EnableIndexGrowth(1);
            context.CreateConnection();
            context.SimplePrimaryReplicaSetup();
            var candidate = Provider(1);
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            Assert.That(context.clusterTestUtils.ClusterReplicate(1, primaryId, async: false, logger: context.logger),
                Is.EqualTo("OK"));
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            Assert.That(() => candidate.replicationManager.IsRecovering, Is.False.After(5_000, 10));
            Assert.That(candidate.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA));
            Assert.That(candidate.storeWrapper.TaskManager.IsRunning(TaskType.CommitTask), Is.False);
            Assert.That(candidate.storeWrapper.TaskManager.IsRunning(TaskType.IndexAutoGrowTask), Is.True);

            Assert.That(context.clusterTestUtils.GetServer(1).Execute("REPLICAOF", "NO", "ONE").ToString(), Is.EqualTo("OK"));
            Assert.That(candidate.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
            Assert.That(() => candidate.storeWrapper.TaskManager.IsRunning(TaskType.CommitTask), Is.True.After(5_000, 10));
            Assert.That(candidate.storeWrapper.TaskManager.IsRunning(TaskType.IndexAutoGrowTask), Is.True);
        }

#if DEBUG
        [Test, CancelAfter(120_000)]
        [TestCase("primary", false)]
        [TestCase("primary", true)]
        [TestCase("replica", false)]
        [TestCase("replica", true)]
        [TestCase("replicate", false)]
        [TestCase("replicate", true)]
        [TestCase("replicaof", false)]
        [TestCase("replicaof", true)]
        [TestCase("abort", false)]
        [TestCase("timeout", false)]
        public async Task SuspensionDrainsCommitWaiters(string operation, bool diskless)
        {
            var canceled = operation is "abort" or "timeout";
            var failover = operation is "primary" or "replica" || canceled;
            var index = failover ? 0 : 1;
            context.CreateInstances(2, enableAOF: true, enableDisklessSync: diskless);
            context.ShutdownNode(index);
            context.nodeOptions[index].CommitFrequencyMs = 100;
            context.nodeOptions[index].WaitForCommit = true;
            context.RestartNode(index);
            context.CreateConnection();
            context.SimplePrimaryReplicaSetup();
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            if (failover)
            {
                Assert.That(context.clusterTestUtils.ClusterReplicate(1, primaryId, async: false, logger: context.logger),
                    Is.EqualTo("OK"));
                context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            }

            var candidate = Provider(index);
            using var writer = new TcpClient();
            using var control = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var token = deadline.Token;
            var session = await ConnectIdleSessionAsync(writer, token, index);
            await ConnectIdleSessionAsync(control, token, index);
            const ExceptionInjectionType injection = ExceptionInjectionType.Aof_Pause_Before_Periodic_Commit;
            ExceptionInjectionHelper.EnableException(injection);
            try
            {
                Assert.That(() => ExceptionInjectionHelper.IsEnabled(injection), Is.False.After(5_000, 10));
                var log = candidate.storeWrapper.appendOnlyFile.Log;
                var tail = log.TailAddress;
                var write = ExecuteCommandAsync(writer, failover
                    ? "*3\r\n$3\r\nSET\r\n$13\r\n{commit}write\r\n$5\r\nvalue\r\n"
                    : "*1\r\n$7\r\nFLUSHDB\r\n", token);
                Assert.That(() => log.TailAddress.EqualsAll(tail), Is.False.After(5_000, 10));
                Assert.That(write.IsCompleted, Is.False);
                var epoch = session.LocalCurrentEpoch;
                Assert.That(epoch, Is.Not.Zero);

                Task<string> attachment = null;
                if (operation is "primary" or "abort" or "timeout")
                {
                    var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
                    Assert.That(candidate.failoverManager.TryStartPrimaryFailover(endpoint.Address.ToString(), endpoint.Port,
                        FailoverOption.DEFAULT, TimeSpan.FromSeconds(canceled ? 2 : 20)), Is.True);
                }
                else if (operation == "replica")
                    Assert.That(Provider(1).failoverManager.TryStartReplicaFailover(
                        FailoverOption.DEFAULT, TimeSpan.FromSeconds(20)), Is.True);
                else if (operation == "replicate")
                    attachment = ExecuteCommandAsync(control,
                        $"*4\r\n$7\r\nCLUSTER\r\n$9\r\nREPLICATE\r\n${primaryId.Length}\r\n{primaryId}\r\n$4\r\nSYNC\r\n", token);
                else
                {
                    var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
                    var address = endpoint.Address.ToString();
                    var port = endpoint.Port.ToString();
                    attachment = ExecuteCommandAsync(control,
                        $"*3\r\n$9\r\nREPLICAOF\r\n${address.Length}\r\n{address}\r\n${port.Length}\r\n{port}\r\n", token);
                }

                Assert.That(() => candidate.garnetEpoch.GetCurrentEpoch(), Is.GreaterThan(epoch).After(5_000, 10));
                Assert.That(candidate.storeWrapper.TaskManager.IsRegistered(TaskType.CommitTask), Is.True,
                    "Commits must remain available until the admitted writer releases its epoch.");
                Assert.That(session.LocalCurrentEpoch, Is.EqualTo(epoch));
                if (canceled)
                {
                    if (operation == "abort")
                        candidate.failoverManager.TryAbortReplicaFailover();
                    Assert.That(() => candidate.storeWrapper.TaskManager.IsRegistered(TaskType.FailoverTask),
                        Is.False.After(5_000, 10));
                    Assert.That(candidate.failoverManager.IsWritePaused, Is.False);
                }
                ExceptionInjectionHelper.SuspendParking();
                try
                {
                    ExceptionInjectionHelper.DisableException(injection);
                    Assert.That(await write.WaitAsync(token), Is.EqualTo("+OK"));
                    if (attachment != null)
                        Assert.That(await attachment.WaitAsync(token), Is.EqualTo("+OK"));
                    Assert.That(() => candidate.clusterManager.CurrentConfig.LocalNodeRole,
                        Is.EqualTo(canceled ? ClusterNodeRole.PRIMARY : ClusterNodeRole.REPLICA).After(10_000, 10));
                    Assert.That(() => candidate.storeWrapper.TaskManager.IsRegistered(TaskType.CommitTask),
                        Is.EqualTo(canceled).After(10_000, 10));
                    if (failover && !canceled)
                    {
                        Assert.That(() => Provider(1).clusterManager.CurrentConfig.LocalNodeRole,
                            Is.EqualTo(ClusterNodeRole.PRIMARY).After(10_000, 10));
                        Assert.That(() => Provider(1).storeWrapper.TaskManager.IsRegistered(TaskType.FailoverTask),
                            Is.False.After(10_000, 10));
                    }
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }
            }
            finally
            {
                ExceptionInjectionHelper.SuspendParking();
                try
                {
                    ExceptionInjectionHelper.DisableException(injection);
                    for (var i = 0; i < 2; i++)
                    {
                        Provider(i).failoverManager.TryAbortReplicaFailover();
                        await Provider(i).storeWrapper.TaskManager.CancelAsync(TaskType.FailoverTask).WaitAsync(token);
                    }
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }
            }
        }

        [Test, CancelAfter(120_000)]
        public async Task FailedReplicaOfDoesNotRestartReplicaTasks([Values] bool diskless)
        {
            context.CreateInstances(2, enableAOF: true, CommitFrequencyMs: 100, enableDisklessSync: diskless);
            context.nodeOptions[1].EnableVectorSetPreview = true;
            context.nodeOptions[1].VectorSetReplayTaskCount = 1;
            EnableIndexGrowth(1);
            context.CreateConnection();
            context.SimplePrimaryReplicaSetup();
            var candidate = Provider(1);
            Assert.That(candidate.storeWrapper.TaskManager.IsRunning(TaskType.CommitTask), Is.True);
            Assert.That(candidate.storeWrapper.TaskManager.IsRegistered(TaskType.VectorReplicationReplayTask), Is.False);
            var primaryEndpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(0);
            var address = primaryEndpoint.Address.ToString();
            var port = primaryEndpoint.Port.ToString();
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await connection.ConnectAsync((IPEndPoint)context.clusterTestUtils.GetEndPoint(1), deadline.Token);
            const ExceptionInjectionType injection = ExceptionInjectionType.Replication_Fail_After_Primary_Task_Drain;
            ExceptionInjectionHelper.EnableException(injection);
            try
            {
                var response = await ExecuteCommandAsync(connection,
                    $"*3\r\n$9\r\nREPLICAOF\r\n${address.Length}\r\n{address}\r\n${port.Length}\r\n{port}\r\n", deadline.Token);
                Assert.That(response, Does.StartWith("-").And.Contain(injection.ToString()));
                Assert.That(candidate.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
                Assert.That(candidate.replicationManager.IsRecovering, Is.False);
                Assert.That(candidate.storeWrapper.TaskManager.IsRunning(TaskType.CommitTask), Is.True);
                Assert.That(candidate.storeWrapper.TaskManager.IsRunning(TaskType.IndexAutoGrowTask), Is.True);
                Assert.That(candidate.storeWrapper.TaskManager.IsRegistered(TaskType.VectorReplicationReplayTask), Is.False,
                    "The REPLICAOF handler must not restart replica tasks after attachment rolls back to primary.");
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(injection);
            }
        }

        [Test, CancelAfter(120_000)]
        public async Task FailedReplicationRestoresPrimaryMaintenance(
            [Values] bool diskless, [Values] bool background, [Values] bool resetOnFailure)
        {
            context.CreateInstances(2, enableAOF: true, CommitFrequencyMs: 100, enableDisklessSync: diskless);
            EnableIndexGrowth(1);
            context.CreateConnection();
            context.SimplePrimaryReplicaSetup();
            var candidate = Provider(1);
            Assert.That(candidate.storeWrapper.TaskManager.IsRunning(TaskType.CommitTask), Is.True);
            var primaryId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            var initialEpoch = candidate.clusterManager.CurrentConfig.LocalNodeConfigEpoch;
            const ExceptionInjectionType injection = ExceptionInjectionType.Replication_Fail_After_Primary_Task_Drain;
            ExceptionInjectionHelper.EnableException(injection);
            try
            {
                ReplicateSyncOptions options = new(primaryId, Background: background, Force: false,
                    TryAddReplica: true, AllowReplicaResetOnFailure: resetOnFailure, UpgradeLock: false);
                var (success, error) = await (diskless
                    ? candidate.replicationManager.TryReplicateDisklessSyncAsync(null, options)
                    : candidate.replicationManager.TryReplicateDiskbasedSyncAsync(null, options));
                Assert.That(success, Is.EqualTo(background));
                if (!background)
                    Assert.That(Encoding.ASCII.GetString(error.Span), Does.Contain(injection.ToString()));
                Assert.That(() => candidate.clusterManager.CurrentConfig.LocalNodeConfigEpoch,
                    Is.GreaterThan(initialEpoch + (resetOnFailure ? 1 : 0)).After(5_000, 10));
                Assert.That(() => candidate.replicationManager.IsRecovering, Is.False.After(5_000, 10));
                Assert.That(candidate.clusterManager.CurrentConfig.LocalNodeRole,
                    Is.EqualTo(resetOnFailure ? ClusterNodeRole.PRIMARY : ClusterNodeRole.REPLICA));
                Assert.That(() => candidate.storeWrapper.TaskManager.IsRunning(TaskType.CommitTask),
                    Is.EqualTo(resetOnFailure).After(5_000, 10));
                Assert.That(candidate.storeWrapper.TaskManager.IsRunning(TaskType.IndexAutoGrowTask), Is.True);
                if (resetOnFailure)
                    Assert.That(candidate.storeWrapper.TaskManager.IsRegistered(TaskType.VectorReplicationReplayTask), Is.False);
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(injection);
            }
        }
#endif

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
            Assert.That(primary.replicationManager.ReplicationOffset.EqualsAll(tail), Is.True,
                "The reattached old primary must synchronize to the handoff offset.");
            Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY),
                "The synchronized replica must be promoted using the final log tail, not the stale replica-tracking offset.");
            context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 1, 1);
        }

        [TestCase(1)]
        [TestCase(2)]
        [CancelAfter(120_000)]
        public async Task PrimaryFailoverExplicitlyAttachesOldPrimary(int sublogCount)
        {
            SetupReplica(sublogCount);
            for (var i = 0; i < 2; i++)
                Assert.That(context.clusterTestUtils.GetServer(i).Execute(
                    "CONFIG", "SET", "cluster-replication-reestablishment-timeout", "0").ToString(), Is.EqualTo("OK"));

            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", 30_000).ToString(), Is.EqualTo("OK"));
            Assert.That(() => Provider(1).clusterManager.CurrentConfig.LocalNodeRole,
                Is.EqualTo(ClusterNodeRole.PRIMARY).After(10_000, 10));
            Assert.That(() => Provider(1).storeWrapper.TaskManager.IsRegistered(TaskType.FailoverTask),
                Is.False.After(10_000, 10));
            Assert.That(() => Provider(0).storeWrapper.TaskManager.IsRegistered(TaskType.FailoverTask),
                Is.False.After(10_000, 10));
            Assert.That(Provider(0).clusterManager.CurrentConfig.LocalNodePrimaryId,
                Is.EqualTo(context.clusterTestUtils.GetNodeIdFromNode(1, context.logger)));

            using var connection = await ConnectionMultiplexer.ConnectAsync(new ConfigurationOptions
            {
                EndPoints = { endpoint }
            });
            Assert.That(await connection.GetDatabase().StringSetAsync("{handoff}after-promotion", "replicated"), Is.True);
            using var syncDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            context.clusterTestUtils.WaitForReplicaAofSync(1, 0, context.logger, syncDeadline.Token);
            using var replicaConnection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await replicaConnection.ConnectAsync((IPEndPoint)context.clusterTestUtils.GetEndPoint(0), deadline.Token);
            Assert.That(await ExecuteCommandAsync(replicaConnection, "*1\r\n$8\r\nREADONLY\r\n", deadline.Token), Is.EqualTo("+OK"));
            await replicaConnection.GetStream().WriteAsync(
                "*2\r\n$3\r\nGET\r\n$24\r\n{handoff}after-promotion\r\n"u8.ToArray(), deadline.Token);
            Assert.That(await ReadFixedResponseAsync(replicaConnection, "$10\r\nreplicated\r\n".Length, deadline.Token),
                Is.EqualTo("$10\r\nreplicated\r\n"));
        }

#if DEBUG
        [TestCase("primary")]
        [TestCase("replica")]
        [TestCase("abort")]
        [TestCase("timeout")]
        [CancelAfter(120_000)]
        public async Task FailoverDrainsCollectionAndInhibitsRestarts(string operation)
        {
            SetupReplica(1, disableObjects: false);
            var primary = Provider(0);
            var wrapper = primary.storeWrapper;
            var db = context.clusterTestUtils.GetDatabase();
            var hash = "{maintenance}hash";
            var sortedSet = "{maintenance}zset";
            db.HashSet(hash, "field", "value");
            db.SortedSetAdd(sortedSet, "member", 1);
            db.Execute("HEXPIRE", hash, 3600, "FIELDS", 1, "field");
            db.Execute("ZEXPIRE", sortedSet, 3600, "MEMBERS", 1, "member");
            context.clusterTestUtils.GetServer(0).Execute("CONFIG", "SET", "expired-object-collection-freq", 1);
            const ExceptionInjectionType injection = ExceptionInjectionType.Failover_Pause_Background_Object_Collection;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var token = deadline.Token;
            var parkingSuspended = false;
            ExceptionInjectionHelper.EnableException(injection);
            try
            {
                Assert.That(() => ExceptionInjectionHelper.IsEnabled(injection), Is.False.After(5_000, 10));
                var tail = wrapper.appendOnlyFile.Log.TailAddress;
                if (operation == "replica")
                {
                    Assert.That(Provider(1).failoverManager.TryStartReplicaFailover(FailoverOption.DEFAULT, TimeSpan.FromSeconds(30)), Is.True);
                    Assert.That(() => primary.clusterManager.CurrentConfig.LocalNodeRole,
                        Is.EqualTo(ClusterNodeRole.REPLICA).After(5_000, 10));
                }
                else
                {
                    var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
                    var args = new List<object> { "TO", endpoint.Address.ToString(), endpoint.Port };
                    if (operation == "timeout")
                        args.AddRange(["TIMEOUT", 3_000]);
                    Assert.That(context.clusterTestUtils.GetServer(0).Execute("FAILOVER", args.ToArray()).ToString(), Is.EqualTo("OK"));
                    Assert.That(() => primary.failoverManager.IsWritePaused, Is.True.After(5_000, 10));
                }
                Assert.That(() => wrapper.TaskManager.IsRegistered(TaskType.ObjectCollectTask), Is.False.After(5_000, 10),
                    "Cancellation removes the task registration, but failover must still await its in-flight cycle.");
                context.clusterTestUtils.GetServer(0).Execute("CONFIG", "SET", "expired-object-collection-freq", 2);
                context.clusterTestUtils.GetServer(0).Execute("CONFIG", "SET", "checkpoint-freq", 60);
                wrapper.StartPrimaryTasks();
                Assert.That(wrapper.TaskManager.IsRegistered(TaskType.ObjectCollectTask), Is.False);
                Assert.That(wrapper.TaskManager.IsRegistered(TaskType.ScheduledCheckpointTask), Is.False);
                Assert.That(Provider(operation == "replica" ? 1 : 0).storeWrapper.TaskManager.IsRunning(TaskType.FailoverTask), Is.True,
                    "Primary-task suspension must not cancel or await its own control-plane failover worker.");
                Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA),
                    "The replica cannot take over before the final collection appends have drained.");

                if (operation == "abort")
                    Assert.That(context.clusterTestUtils.GetServer(0).Execute("FAILOVER", "ABORT").ToString(), Is.EqualTo("OK"));
                if (operation is "abort" or "timeout")
                {
                    Assert.That(() => primary.failoverManager.IsWritePaused, Is.False.After(10_000, 10),
                        "Cancellation releases writers even when collection has not finished.");
                    Assert.That(wrapper.TaskManager.IsRunning(TaskType.FailoverTask), Is.True,
                        "The worker retains ownership until draining and restoration are complete.");
                    Assert.That(wrapper.TaskManager.IsRegistered(TaskType.ObjectCollectTask), Is.False);
                }

                ExceptionInjectionHelper.SuspendParking();
                parkingSuspended = true;
                ExceptionInjectionHelper.DisableException(injection);
                Assert.That(() => wrapper.appendOnlyFile.Log.TailAddress.EqualsAll(tail), Is.False.After(5_000, 10),
                    "The in-flight hash and sorted-set collection appends must precede the synchronization boundary.");
                Assert.That(() => wrapper.TaskManager.IsRegistered(TaskType.FailoverTask), Is.False.After(10_000, 10));
                Assert.That(() => Provider(1).storeWrapper.TaskManager.IsRegistered(TaskType.FailoverTask), Is.False.After(10_000, 10));
                if (operation is "abort" or "timeout")
                {
                    Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
                    Assert.That(wrapper.TaskManager.IsRunning(TaskType.ObjectCollectTask), Is.True,
                        "A canceled failover must restart collection using the updated runtime configuration.");
                    Assert.That(wrapper.TaskManager.IsRunning(TaskType.ScheduledCheckpointTask), Is.True);
                }
                else
                {
                    Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
                    Assert.That(wrapper.TaskManager.IsRegistered(TaskType.ObjectCollectTask), Is.False);
                    Assert.That(wrapper.TaskManager.IsRegistered(TaskType.ScheduledCheckpointTask), Is.False);
                    using var promotedConnection = await ConnectionMultiplexer.ConnectAsync(new ConfigurationOptions
                    {
                        EndPoints = { context.clusterTestUtils.GetEndPoint(1) }
                    });
                    var promotedDb = promotedConnection.GetDatabase();
                    Assert.That(promotedDb.HashGet(hash, "field").ToString(), Is.EqualTo("value"));
                    Assert.That(promotedDb.SortedSetScore(sortedSet, "member"), Is.EqualTo(1));
                }
            }
            finally
            {
                if (!parkingSuspended)
                    ExceptionInjectionHelper.SuspendParking();
                try
                {
                    ExceptionInjectionHelper.DisableException(injection);
                    primary.failoverManager.TryAbortReplicaFailover();
                    Provider(1).failoverManager.TryAbortReplicaFailover();
                    await Task.WhenAll(wrapper.TaskManager.CancelAsync(TaskType.FailoverTask),
                        Provider(1).storeWrapper.TaskManager.CancelAsync(TaskType.FailoverTask)).WaitAsync(token);
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }
            }
        }
#endif

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

        // SET runs on the script fast path; APPEND runs through RESP dispatch inside the script. The script spins
        // until a deadline so that the pause begins after the script passed its own admission.
        [TestCase("SET")]
        [TestCase("APPEND")]
        [CancelAfter(120_000)]
        public async Task PausedScriptWriteReleasesSessionEpoch(string command)
        {
            SetupReplica(1, enableLua: true);
            var primary = Provider(0);
            var db = context.clusterTestUtils.GetDatabase();
            var key = context.kvPairs.Keys.First();
            var value = (string)db.StringGet(key);
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var session = await ConnectIdleSessionAsync(connection, deadline.Token);
            var script = $"while tonumber(redis.call('TIME')[1]) < tonumber(ARGV[2]) do end redis.call('{command}', KEYS[1], ARGV[1]) return 1";
            var runUntil = DateTimeOffset.UtcNow.AddSeconds(4).ToUnixTimeSeconds();
            Task<string> response = null;
            try
            {
                var stream = connection.GetStream();
                await stream.WriteAsync(Encoding.ASCII.GetBytes(
                    $"*6\r\n$4\r\nEVAL\r\n${script.Length}\r\n{script}\r\n$1\r\n1\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n${runUntil.ToString().Length}\r\n{runUntil}\r\n"),
                    deadline.Token);
                response = ReadResponseAsync(stream, deadline.Token);
                Assert.That(() => session.LocalCurrentEpoch, Is.Not.Zero.After(5_000, 10), "The script must be running.");

                primary.failoverManager.PauseWrites(true);
                Assert.That(() => session.LocalCurrentEpoch, Is.Zero.After(15_000, 10),
                    "A script blocked on the write pause must release the epoch held by its session.");
                Assert.That(response.IsCompleted, Is.False);
                Assert.That(db.StringGet(key), Is.EqualTo(value));
                Assert.That(await primary.BumpAndWaitForEpochTransitionAsync().AsTask().WaitAsync(deadline.Token), Is.True);
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }

            Assert.That(await response.WaitAsync(deadline.Token), Is.EqualTo(":1"));
            Assert.That((string)db.StringGet(key), Is.EqualTo(command == "SET" ? "changed" : value + "changed"));
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
        public void WritePauseCompletionTracksOverlappingOwners()
        {
            SetupReplica(1);
            var manager = Provider(0).failoverManager;
            Assert.That(manager.WaitForWritesToResumeAsync().IsCompletedSuccessfully, Is.True);
            manager.PauseWrites(true);
            var first = manager.WaitForWritesToResumeAsync();
            try
            {
                Assert.That(manager.WaitForWritesToResumeAsync(), Is.SameAs(first),
                    "Paused writers must share one completion task.");
                manager.PauseWrites(true);
                manager.PauseWrites(false);
                Assert.That(first.IsCompleted, Is.False,
                    "Releasing one owner must not wake writers while another owner still holds the pause.");
            }
            finally
            {
                manager.PauseWrites(false);
            }
            Assert.That(first.IsCompletedSuccessfully, Is.True);

            manager.PauseWrites(true);
            try
            {
                var second = manager.WaitForWritesToResumeAsync();
                Assert.That(second, Is.Not.SameAs(first));
                Assert.That(second.IsCompleted, Is.False,
                    "A new pause must not reuse the previous owner's completed signal.");
            }
            finally
            {
                manager.PauseWrites(false);
            }
        }

        [TestCase(false)]
        [TestCase(true)]
        [CancelAfter(120_000)]
        public void ConcurrentPausedWritersAllowFailoverCancellationWithWorkerHeadroom(bool timeout)
        {
            SetupReplica(1);
            // Run the controller outside the pool so it can restore the limits even if server workers stall.
            AsyncUtils.BlockingWait(Task.Factory.StartNew(() =>
            {
                ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
                ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
                var connections = new List<TcpClient>();
                var writers = new List<ClusterSession>();
                var responses = new List<Task<string>>();
                var primary = Provider(0);
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                var token = deadline.Token;
                using var guardConnection = new TcpClient();
                using var probe = new TcpClient();
                var guard = AsyncUtils.BlockingWait(ConnectIdleSessionAsync(guardConnection, token));
                try
                {
                    AsyncUtils.BlockingWait(ConnectIdleSessionAsync(probe, token));
                    const int writerCount = 24;
                    for (var i = 0; i < writerCount; i++)
                    {
                        var connection = new TcpClient();
                        connections.Add(connection);
                        writers.Add(AsyncUtils.BlockingWait(ConnectIdleSessionAsync(connection, token)));
                    }
                    Assert.That(ThreadPool.SetMinThreads(32, minIo), Is.True);
                    Assert.That(ThreadPool.SetMaxThreads(32, maxIo), Is.True);

                    // Hold failover before demotion, leaving writes paused until timeout or client-side ABORT.
                    guard.AcquireCurrentEpoch();
                    var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
                    var args = new List<object> { "TO", endpoint.Address.ToString(), endpoint.Port };
                    if (timeout)
                        args.AddRange(["TIMEOUT", 10_000]);
                    Assert.That(context.clusterTestUtils.GetServer(0).Execute("FAILOVER", args.ToArray()).ToString(), Is.EqualTo("OK"));
                    Assert.That(() => primary.failoverManager.IsWritePaused, Is.True.After(5_000, 10));
                    var key = context.kvPairs.Keys.First();
                    var write = Encoding.ASCII.GetBytes($"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n");
                    for (var i = 0; i < writerCount; i++)
                    {
                        writers[i].AcquireCurrentEpoch();
                        AsyncUtils.BlockingWait(connections[i].GetStream().WriteAsync(write, token));
                        responses.Add(ReadResponseAsync(connections[i].GetStream(), token));
                    }
                    Assert.That(() => writers.All(s => s.LocalCurrentEpoch == 0), Is.True.After(5_000, 10));
                    Assert.That(responses.All(r => !r.IsCompleted), Is.True);
                    Assert.That(AsyncUtils.BlockingWait(ExecuteCommandAsync(probe, "*1\r\n$4\r\nPING\r\n", token)), Is.EqualTo("+PONG"));
                    Assert.That(AsyncUtils.BlockingWait(ExecuteCommandAsync(probe,
                        $"*2\r\n$6\r\nEXISTS\r\n${key.Length}\r\n{key}\r\n", token)), Is.EqualTo(":1"));
                    if (!timeout)
                        Assert.That(AsyncUtils.BlockingWait(ExecuteCommandAsync(probe, "*2\r\n$8\r\nFAILOVER\r\n$5\r\nABORT\r\n", token)), Is.EqualTo("+OK"));
                    var replies = AsyncUtils.BlockingWait(Task.WhenAll(responses).WaitAsync(token));
                    Assert.That(replies, Is.All.EqualTo("+OK"));
                    Assert.That(primary.failoverManager.IsWritePaused, Is.False);
                    Assert.That(guard.LocalCurrentEpoch, Is.Not.Zero,
                        "Timeout and ABORT must release writers without waiting for the stalled session.");
                }
                finally
                {
                    Assert.That(ThreadPool.SetMaxThreads(maxWorkers, maxIo), Is.True);
                    Assert.That(ThreadPool.SetMinThreads(minWorkers, minIo), Is.True);
                    guard.ReleaseCurrentEpoch();
                    primary.failoverManager.TryAbortReplicaFailover();
                    deadline.Cancel();
                    foreach (var connection in connections)
                        connection.Dispose();
                }
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default));
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
        public async Task PausedFlushTransactionRevalidatesBeforeRunning(
            [Values] bool allDatabases, [Values] bool demote, [Values("none", "GET", "SET")] string keyedCommand)
        {
            SetupReplica(1);
            var primary = Provider(0);
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var token = deadline.Token;
            var session = await ConnectIdleSessionAsync(connection, token);
            Assert.That(await ExecuteCommandAsync(connection, "*1\r\n$5\r\nMULTI\r\n", token), Is.EqualTo("+OK"));
            var command = allDatabases ? "FLUSHALL" : "FLUSHDB";
            Assert.That(await ExecuteCommandAsync(connection, $"*1\r\n${command.Length}\r\n{command}\r\n", token),
                Is.EqualTo("+QUEUED"));
            var key = context.kvPairs.Keys.First();
            if (keyedCommand != "none")
                Assert.That(await ExecuteCommandAsync(connection,
                    keyedCommand == "SET"
                        ? $"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n"
                        : $"*2\r\n$3\r\nGET\r\n${key.Length}\r\n{key}\r\n", token), Is.EqualTo("+QUEUED"));

            var expected = demote ? "-ERR You can't write against a read only replica.\r\n" :
                keyedCommand switch
                {
                    "SET" => "*2\r\n+OK\r\n+OK\r\n",
                    "GET" => "*2\r\n+OK\r\n$-1\r\n",
                    _ => "*1\r\n+OK\r\n"
                };
            Task<string> response = null;
            primary.failoverManager.PauseWrites(true);
            try
            {
                session.AcquireCurrentEpoch();
                await connection.GetStream().WriteAsync("*1\r\n$4\r\nEXEC\r\n"u8.ToArray(), token);
                response = ReadFixedResponseAsync(connection, expected.Length, token);
                Assert.That(() => session.LocalCurrentEpoch, Is.Zero.After(5_000, 10));
                Assert.That(() => response.IsCompleted, Is.False.After(100));
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 0, 0);
                Assert.That(await primary.BumpAndWaitForEpochTransitionAsync().AsTask().WaitAsync(token), Is.True);
                if (demote)
                    primary.clusterManager.TryStopWrites(context.clusterTestUtils.GetNodeIdFromNode(1, context.logger));
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }
            Assert.That(await response.WaitAsync(token), Is.EqualTo(expected));
            if (demote)
                context.ValidateKVCollectionAgainstReplica(ref context.kvPairs, 0, 0);
            else
                Assert.That(context.clusterTestUtils.GetServer(0).DatabaseSize(), Is.EqualTo(keyedCommand == "SET" ? 1 : 0));
            Assert.That(await ExecuteCommandAsync(connection, "*1\r\n$4\r\nPING\r\n", token), Is.EqualTo("+PONG"));
        }

        [Test, CancelAfter(120_000)]
        public async Task InternalFlushTransactionBypassesPause([Values] bool allDatabases, [Values] bool demote)
        {
            SetupReplica(1);
            var primary = Provider(0);
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var token = deadline.Token;
            var session = await ConnectIdleSessionAsync(connection, token);
            session.SetInternalWriteSession();
            primary.failoverManager.PauseWrites(true);
            try
            {
                if (demote)
                    primary.clusterManager.TryStopWrites(context.clusterTestUtils.GetNodeIdFromNode(1, context.logger));
                Assert.That(await ExecuteCommandAsync(connection, "*1\r\n$5\r\nMULTI\r\n", token), Is.EqualTo("+OK"));
                var command = allDatabases ? "FLUSHALL" : "FLUSHDB";
                Assert.That(await ExecuteCommandAsync(connection, $"*1\r\n${command.Length}\r\n{command}\r\n", token),
                    Is.EqualTo("+QUEUED"));
                await connection.GetStream().WriteAsync("*1\r\n$4\r\nEXEC\r\n"u8.ToArray(), token);
                Assert.That(await ReadFixedResponseAsync(connection, "*1\r\n+OK\r\n".Length, token),
                    Is.EqualTo("*1\r\n+OK\r\n"));
                Assert.That(primary.failoverManager.IsWritePaused, Is.True);
                Assert.That(context.clusterTestUtils.GetServer(0).DatabaseSize(), Is.Zero);
            }
            finally
            {
                primary.failoverManager.PauseWrites(false);
            }
        }

        [Test, CancelAfter(120_000)]
        public async Task ReplicationReplaysTransactionWhileWritesArePaused()
        {
            SetupReplica(1);
            var replica = Provider(1);
            var db = context.clusterTestUtils.GetDatabase();
            replica.failoverManager.PauseWrites(true);
            try
            {
                var transaction = db.CreateTransaction();
                var flush = transaction.ExecuteAsync("FLUSHDB");
                var first = transaction.StringSetAsync("{replay}first", "one");
                var second = transaction.StringIncrementAsync("{replay}second");
                Assert.That(await transaction.ExecuteAsync(), Is.True);
                Assert.That((await flush).ToString(), Is.EqualTo("OK"));
                Assert.That(await first, Is.True);
                Assert.That(await second, Is.EqualTo(1));
                context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
                using var connection = await ConnectionMultiplexer.ConnectAsync(new ConfigurationOptions
                {
                    EndPoints = { context.clusterTestUtils.GetEndPoint(1) },
                    CommandMap = CommandMap.Create(new HashSet<string> { "CLUSTER" }, available: false)
                });
                var replicaDb = connection.GetDatabase();
                Assert.That(await replicaDb.StringGetAsync("{replay}first", CommandFlags.PreferReplica), Is.EqualTo((RedisValue)"one"));
                Assert.That(await replicaDb.StringGetAsync("{replay}second", CommandFlags.PreferReplica), Is.EqualTo((RedisValue)"1"));
                Assert.That(replica.failoverManager.IsWritePaused, Is.True);
            }
            finally
            {
                replica.failoverManager.PauseWrites(false);
            }
        }

#if DEBUG
        [Test, CancelAfter(120_000)]
        public async Task AdmittedFlushTransactionRetainsEpochUntilCommit([Values] bool allDatabases)
        {
            SetupReplica(1);
            var primary = Provider(0);
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            var token = deadline.Token;
            var session = await ConnectIdleSessionAsync(connection, token);
            Assert.That(await ExecuteCommandAsync(connection, "*1\r\n$5\r\nMULTI\r\n", token), Is.EqualTo("+OK"));
            var command = allDatabases ? "FLUSHALL" : "FLUSHDB";
            Assert.That(await ExecuteCommandAsync(connection, $"*1\r\n${command.Length}\r\n{command}\r\n", token),
                Is.EqualTo("+QUEUED"));
            var key = context.kvPairs.Keys.First();
            Assert.That(await ExecuteCommandAsync(connection,
                $"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n", token), Is.EqualTo("+QUEUED"));
            const ExceptionInjectionType injection = ExceptionInjectionType.Failover_Pause_Before_Transactional_Flush;
            Task<string> response = null;
            Task<bool> barrier = null;
            var writesPaused = false;
            try
            {
                ExceptionInjectionHelper.EnableException(injection);
                await connection.GetStream().WriteAsync("*1\r\n$4\r\nEXEC\r\n"u8.ToArray(), token);
                response = ReadFixedResponseAsync(connection, "*2\r\n+OK\r\n+OK\r\n".Length, token);
                Assert.That(() => ExceptionInjectionHelper.IsEnabled(injection), Is.False.After(5_000, 10));
                var epoch = session.LocalCurrentEpoch;
                Assert.That(epoch, Is.Not.Zero);
                primary.failoverManager.PauseWrites(true);
                writesPaused = true;
                barrier = primary.BumpAndWaitForEpochTransitionAsync(token).AsTask();
                Assert.That(() => primary.garnetEpoch.GetCurrentEpoch(), Is.GreaterThan(epoch).After(5_000, 10));
                Assert.That(barrier.IsCompleted, Is.False);
                Assert.That(session.LocalCurrentEpoch, Is.EqualTo(epoch));
                ExceptionInjectionHelper.EnableException(injection);
                Assert.That(await response.WaitAsync(token), Is.EqualTo("*2\r\n+OK\r\n+OK\r\n"));
                Assert.That(await barrier.WaitAsync(token), Is.True);
                Assert.That(primary.failoverManager.IsWritePaused, Is.True,
                    "An admitted transaction must commit without waiting for the write pause to end.");
                Assert.That(context.clusterTestUtils.GetDatabase().StringGet(key), Is.EqualTo((RedisValue)"changed"));
                context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            }
            finally
            {
                ExceptionInjectionHelper.SuspendParking();
                ExceptionInjectionHelper.DisableException(injection);
                if (writesPaused)
                    primary.failoverManager.PauseWrites(false);
                try
                {
                    if (response != null)
                        await response.WaitAsync(token);
                    if (barrier != null)
                        await barrier.WaitAsync(token);
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }
            }
        }
#endif

        static async Task<string> ReadFixedResponseAsync(TcpClient connection, int length, CancellationToken token)
        {
            var bytes = new byte[length];
            await connection.GetStream().ReadExactlyAsync(bytes, token);
            return Encoding.ASCII.GetString(bytes);
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

        async Task<ClusterSession> ConnectIdleSessionAsync(TcpClient connection, CancellationToken token, int index = 0)
        {
            var server = (GarnetServerTcp)Provider(index).storeWrapper.Servers[0];
            await connection.ConnectAsync((IPEndPoint)context.clusterTestUtils.GetEndPoint(index), token);
            var stream = connection.GetStream();
            await stream.WriteAsync("*1\r\n$4\r\nPING\r\n"u8.ToArray(), token);
            await stream.ReadExactlyAsync(new byte[7], token);
            var senderField = typeof(ClusterSession).GetField("networkSender", BindingFlags.Instance | BindingFlags.NonPublic);
            var endpoint = (IPEndPoint)connection.Client.LocalEndPoint;
            var session = server.ActiveClusterSessions().OfType<ClusterSession>().Single(s =>
            {
                var remote = IPEndPoint.Parse(((INetworkSender)senderField.GetValue(s)).RemoteEndpointName);
                return remote.Port == endpoint.Port && remote.Address.MapToIPv6().Equals(endpoint.Address.MapToIPv6());
            });
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

        // A script's session waits on the epoch held by the session that runs the script
        [TestCase(false)]
        [TestCase(true)]
        [CancelAfter(120_000)]
        public Task MigrationWaitInScriptSessionRevalidatesConfiguration(bool isTxn)
            => VerifyMigrationWaitAsync(readOnly: false, demote: true, isTxn, verifyResponse: true, scriptSession: true);

        async Task VerifyMigrationWaitAsync(bool readOnly, bool demote, bool isTxn, bool verifyResponse, bool scriptSession = false)
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
            using var ownerConnection = new TcpClient();
            var epochSession = session;
            if (scriptSession)
            {
                // The owner runs the script and holds the epoch; `session` is the session the script issues commands through
                epochSession = await ConnectIdleSessionAsync(ownerConnection, token);
                session.SetEpochOwner(epochSession);
            }
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
                    epochSession.AcquireCurrentEpoch();
                    try
                    {
                        return verifyResponse ? VerifyMultiKeyResponse(session, key, readOnly, isTxn) : verify.Invoke(session, args);
                    }
                    finally
                    {
                        epochSession.ReleaseCurrentEpoch();
                    }
                });
                Assert.That(() => epochSession.LocalCurrentEpoch, Is.Not.Zero.After(5_000, 10));
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
        public async Task FailoverWithoutTimeoutWaitsForCheckpointsThenCompletes()
        {
            SetupReplica(1);
            var primary = Provider(0);
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            Assert.That(primary.storeWrapper.TryPauseCheckpoints(), Is.True);
            try
            {
                Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                    "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port).ToString(), Is.EqualTo("OK"));
                Assert.That(() => primary.failoverManager.IsWritePaused, Is.True.After(5_000, 10));
                Assert.That(() => primary.failoverManager.IsWritePaused, Is.True.After(200));
                Assert.That(primary.storeWrapper.TaskManager.IsRunning(TaskType.FailoverTask), Is.True);
                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await primary.storeWrapper.TaskManager.CancelAsync(TaskPlacementCategory.Primary).WaitAsync(deadline.Token);
                Assert.That(primary.storeWrapper.TaskManager.IsRunning(TaskType.FailoverTask), Is.True,
                    "Role-specific task cancellation must not cancel or await failover itself.");
            }
            finally
            {
                primary.storeWrapper.ResumeCheckpoints();
            }
            context.clusterTestUtils.WaitForNoFailover(0, context.logger);
            context.clusterTestUtils.WaitForNoFailover(1, context.logger);
            Assert.That(primary.failoverManager.IsWritePaused, Is.False);
            Assert.That(() => primary.storeWrapper.TaskManager.IsRegistered(TaskType.FailoverTask), Is.False.After(5_000, 10));
            Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
        }

        [TestCase(false, "timeout")]
        [TestCase(true, "timeout")]
        [TestCase(false, "abort")]
        [TestCase(true, "abort")]
        [TestCase(false, "task-manager")]
        [TestCase(true, "task-manager")]
        [CancelAfter(120_000)]
        public async Task CancelingFailoverReleasesWritersWithoutDrainingStalledEpoch(bool demote, string cancellation)
        {
            SetupReplica(1);
            var primary = Provider(0);
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            using var firstConnection = new TcpClient();
            using var incomingConnection = new TcpClient();
            using var writerConnection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var token = deadline.Token;
            var first = await ConnectIdleSessionAsync(firstConnection, token);
            var incoming = await ConnectIdleSessionAsync(incomingConnection, token);
            var writer = await ConnectIdleSessionAsync(writerConnection, token);
            var key = context.kvPairs.Keys.First();
            var config = primary.clusterManager.CurrentConfig;
            first.AcquireCurrentEpoch();
            try
            {
                var initialEpoch = first.LocalCurrentEpoch;
                var args = new List<object> { "TO", endpoint.Address.ToString(), endpoint.Port };
                if (cancellation == "timeout")
                    args.AddRange(["TIMEOUT", 3_000]);
                Assert.That(context.clusterTestUtils.GetServer(0).Execute("FAILOVER", args.ToArray()).ToString(), Is.EqualTo("OK"));
                Assert.That(() => primary.garnetEpoch.GetCurrentEpoch(), Is.GreaterThan(initialEpoch).After(5_000, 10));
                Assert.That(primary.storeWrapper.TaskManager.IsRunning(TaskType.FailoverTask), Is.True);
                var response = await StartPausedCommandAsync(writer, writerConnection,
                    $"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n", token);
                var error = Assert.Throws<RedisServerException>(() => context.clusterTestUtils.GetServer(0).Execute(
                    "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", 1_000));
                Assert.That(error.Message, Does.Contain("failed to start failover"),
                    "A running worker must keep ownership until cancellation cleanup completes.");
                if (demote)
                {
                    incoming.AcquireCurrentEpoch();
                    var epoch = incoming.LocalCurrentEpoch;
                    first.ReleaseCurrentEpoch();
                    Assert.That(() => primary.clusterManager.CurrentConfig.LocalNodeRole,
                        Is.EqualTo(ClusterNodeRole.REPLICA).After(5_000, 10));
                    Assert.That(() => primary.garnetEpoch.GetCurrentEpoch(), Is.GreaterThan(epoch).After(5_000, 10));
                }
                if (cancellation == "abort")
                    Assert.That(context.clusterTestUtils.GetServer(0).Execute("FAILOVER", "ABORT").ToString(), Is.EqualTo("OK"));
                else if (cancellation == "task-manager")
                    await primary.storeWrapper.TaskManager.CancelAsync(TaskType.FailoverTask).WaitAsync(token);

                var reply = await response.WaitAsync(token);
                if (demote)
                    Assert.That(reply, Does.StartWith($"-MOVED {HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key))} "),
                        "Without takeover, ownership can still change through gossip, but the demoted node must not accept the write.");
                else
                    Assert.That(reply, Is.EqualTo("+OK"));
                Assert.That(primary.failoverManager.IsWritePaused, Is.False);
                Assert.That((demote ? incoming : first).LocalCurrentEpoch, Is.Not.Zero,
                    "Cancellation must release writers without waiting for the stalled epoch.");
                Assert.That(() => primary.storeWrapper.TaskManager.IsRegistered(TaskType.FailoverTask), Is.False.After(5_000, 10));
                Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole,
                    Is.EqualTo(demote ? ClusterNodeRole.REPLICA : ClusterNodeRole.PRIMARY));
                Assert.That(Provider(1).clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.REPLICA),
                    "A canceled post-demotion epoch wait must not issue takeover.");

                if (demote)
                {
                    var bytes = Encoding.ASCII.GetBytes(key);
                    var pin = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                    try
                    {
                        var pinnedKey = MakePinnedKey(pin.AddrOfPinnedObject(), bytes.Length);
                        var verify = typeof(ClusterSession).GetMethod("SingleKeySlotVerify", BindingFlags.Instance | BindingFlags.NonPublic);
                        object[] verifyArgs = [config, pinnedKey, false, false, false, (int)HashSlotUtils.HashSlot(pinnedKey)];
                        var result = (ClusterSlotVerificationResult)verify.Invoke(incoming, verifyArgs);
                        Assert.That(result.state, Is.EqualTo(SlotVerifiedState.MOVED),
                            "A retained primary snapshot must not admit writes after cancellation releases the pause.");
                    }
                    finally
                    {
                        pin.Free();
                    }
                }
            }
            finally
            {
                first.ReleaseCurrentEpoch();
                incoming.ReleaseCurrentEpoch();
                primary.failoverManager.TryAbortReplicaFailover();
                await primary.storeWrapper.TaskManager.CancelAsync(TaskType.FailoverTask).WaitAsync(TimeSpan.FromSeconds(5));
            }
        }

#if DEBUG
        [TestCase(false)]
        [TestCase(true)]
        [CancelAfter(120_000)]
        public async Task CancelingDemotedFailoverBetweenAdmissionReadsRedirectsWriter(bool timeout)
        {
            SetupReplica(1);
            var primary = Provider(0);
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            using var guardConnection = new TcpClient();
            using var writerConnection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var token = deadline.Token;
            var guard = await ConnectIdleSessionAsync(guardConnection, token);
            var writer = await ConnectIdleSessionAsync(writerConnection, token);
            var key = context.kvPairs.Keys.First();
            var value = context.clusterTestUtils.GetDatabase().StringGet(key);
            const ExceptionInjectionType injection = ExceptionInjectionType.Failover_Pause_Between_Write_Admission_Reads;
            Task<string> response = null;
            guard.AcquireCurrentEpoch();
            try
            {
                var initialEpoch = guard.LocalCurrentEpoch;
                var args = new List<object> { "TO", endpoint.Address.ToString(), endpoint.Port };
                if (timeout)
                    args.AddRange(["TIMEOUT", 10_000]);
                Assert.That(context.clusterTestUtils.GetServer(0).Execute("FAILOVER", args.ToArray()).ToString(), Is.EqualTo("OK"));
                Assert.That(() => primary.garnetEpoch.GetCurrentEpoch(), Is.GreaterThan(initialEpoch).After(5_000, 10));
                Assert.That(primary.failoverManager.IsWritePaused, Is.True);

                ExceptionInjectionHelper.EnableException(injection);
                await writerConnection.GetStream().WriteAsync(Encoding.ASCII.GetBytes(
                    $"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n"), token);
                response = ReadResponseAsync(writerConnection.GetStream(), token);
                Assert.That(() => ExceptionInjectionHelper.IsEnabled(injection), Is.False.After(5_000, 10));
                Assert.That(writer.LocalCurrentEpoch, Is.GreaterThan(initialEpoch));
                guard.ReleaseCurrentEpoch();
                Assert.That(() => primary.clusterManager.CurrentConfig.LocalNodeRole,
                    Is.EqualTo(ClusterNodeRole.REPLICA).After(5_000, 10));
                if (!timeout)
                    Assert.That(context.clusterTestUtils.GetServer(0).Execute("FAILOVER", "ABORT").ToString(), Is.EqualTo("OK"));
                Assert.That(() => primary.failoverManager.IsWritePaused, Is.False.After(15_000, 10));
                Assert.That(response.IsCompleted, Is.False);

                ExceptionInjectionHelper.EnableException(injection);
                Assert.That(await response.WaitAsync(token),
                    Does.StartWith($"-MOVED {HashSlotUtils.HashSlot(Encoding.ASCII.GetBytes(key))} "));
                Assert.That(context.clusterTestUtils.GetDatabase().StringGet(key), Is.EqualTo(value),
                    "A demoted node must not execute a write after cancellation releases its pause.");
            }
            finally
            {
                // Release an arrived writer and prevent a late arrival from parking during cleanup.
                ExceptionInjectionHelper.SuspendParking();
                try
                {
                    ExceptionInjectionHelper.DisableException(injection);
                    guard.ReleaseCurrentEpoch();
                    primary.failoverManager.TryAbortReplicaFailover();
                    await primary.storeWrapper.TaskManager.CancelAsync(TaskType.FailoverTask).WaitAsync(TimeSpan.FromSeconds(5));
                    if (response != null)
                        await response.WaitAsync(token);
                }
                finally
                {
                    ExceptionInjectionHelper.ResumeParking();
                }
            }
        }
#endif

        [Test, CancelAfter(120_000)]
        public async Task FailoverTimeoutReleasesWritersWhileCheckpointsStayPaused()
        {
            SetupReplica(1);
            var primary = Provider(0);
            var endpoint = (IPEndPoint)context.clusterTestUtils.GetEndPoint(1);
            using var connection = new TcpClient();
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var writer = await ConnectIdleSessionAsync(connection, deadline.Token);
            var key = context.kvPairs.Keys.First();
            Assert.That(primary.storeWrapper.TryPauseCheckpoints(), Is.True);
            try
            {
                Assert.That(context.clusterTestUtils.GetServer(0).Execute(
                    "FAILOVER", "TO", endpoint.Address.ToString(), endpoint.Port, "TIMEOUT", 3_000).ToString(), Is.EqualTo("OK"));
                Assert.That(() => primary.failoverManager.IsWritePaused, Is.True.After(5_000, 10));
                var response = await StartPausedCommandAsync(writer, connection,
                    $"*3\r\n$3\r\nSET\r\n${key.Length}\r\n{key}\r\n$7\r\nchanged\r\n", deadline.Token);
                Assert.That(await response.WaitAsync(deadline.Token), Is.EqualTo("+OK"));
                Assert.That(primary.failoverManager.IsWritePaused, Is.False);
                Assert.That(primary.clusterManager.CurrentConfig.LocalNodeRole, Is.EqualTo(ClusterNodeRole.PRIMARY));
                Assert.That(primary.storeWrapper.TryPauseCheckpoints(), Is.False,
                    "Failover cleanup must not release a checkpoint pause owned by another operation.");
            }
            finally
            {
                primary.storeWrapper.ResumeCheckpoints();
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