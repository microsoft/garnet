// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using NUnit.Framework;

#if DEBUG
using System;
using Garnet.common;
using StackExchange.Redis;
#endif

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class ReplicationStateTests : TestBase
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

#if DEBUG
        [Test, CancelAfter(30_000)]
        public async Task RoleResponseKeepsItsReplicaSnapshotDuringPromotion([Values] bool sharded)
        {
            context.CreateInstances(2, enableAOF: true, sublogCount: sharded ? 2 : 1);
            context.MeetAndAssignSlotsAllNodes(2);
            var provider = (ClusterProvider)context.nodes[1].Provider.StoreWrapper.clusterProvider;
            // Role snapshots need replica metadata, not replay workers requiring the full failover protocol.
            provider.clusterManager.TryStopWrites(context.clusterTestUtils.GetNodeIdFromNode(0, context.logger));
            using var client = ConnectionMultiplexer.Connect(TestUtils.GetConfig([context.endpoints[1]], allowAdmin: true));
            var server = client.GetServer(context.endpoints[1]);
            RedisResult[] response;
            using (ExceptionInjectionHelper.EnabledScope(ExceptionInjectionType.Role_Pause_After_Role_Check))
            {
                var pending = server.ExecuteAsync("ROLE");
                try
                {
                    await ExceptionInjectionHelper.WaitOnClearAsync(ExceptionInjectionType.Role_Pause_After_Role_Check)
                        .WaitAsync(TimeSpan.FromSeconds(5));
                    Assert.That(provider.clusterManager.TryTakeOverForPrimary(), Is.True);
                }
                finally
                {
                    ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Role_Pause_After_Role_Check);
                }
                response = (RedisResult[])await pending.WaitAsync(TimeSpan.FromSeconds(5));
            }

            Assert.That(response, Has.Length.EqualTo(sharded ? 6 : 5));
            Assert.That(response[0].ToString(), Is.EqualTo("slave"));
            Assert.That(response[1].ToString(), Is.EqualTo(context.endpoints[0].ToIPEndPoint().Address.ToString()));
            Assert.That((int)response[2], Is.EqualTo(context.endpoints[0].ToIPEndPoint().Port));
            var current = (RedisResult[])await server.ExecuteAsync("ROLE");
            Assert.That(current[0].ToString(), Is.EqualTo("master"));
        }
#endif

        [Test, CancelAfter(30_000)]
        public async Task AofStreamingDoesNotRejectCompletedRecovery([Values] bool downgrade)
        {
            context.CreateInstances(1, enableAOF: true);
            var manager = ((ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider).replicationManager;
            using var start = new Barrier(2);
            var writer = Task.Factory.StartNew(() =>
            {
                start.SignalAndWait();
                for (var i = 0; i < 1_000_000; i++)
                {
                    manager.currentRecoveryStatus = RecoveryStatus.CheckpointRecoveredAtReplica;
                    Thread.MemoryBarrier();
                    manager.currentRecoveryStatus = downgrade ? RecoveryStatus.ReadRole : RecoveryStatus.NoRecovery;
                    Thread.MemoryBarrier();
                }
                manager.currentRecoveryStatus = RecoveryStatus.NoRecovery;
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            var reader = Task.Factory.StartNew(() =>
            {
                start.SignalAndWait();
                var rejected = 0;
                for (var i = 0; i < 1_000_000; i++)
                {
                    if (manager.CannotStreamAOF)
                        rejected++;
                    Thread.MemoryBarrier();
                }
                return rejected;
            }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            await Task.WhenAll(writer, reader);
            Assert.That(await reader, Is.Zero, "Both sides of recovery completion allow the primary's AOF stream.");
        }
    }
}