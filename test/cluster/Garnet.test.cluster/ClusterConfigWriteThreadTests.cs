// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Garnet.cluster;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using ClusterNodeRole = Garnet.cluster.NodeRole;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A node writes its cluster configuration files, <c>nodes.conf</c> and <c>replication.conf</c>, as the
    /// configuration changes. A write that a pool thread waited for would be a thread the node's clients could not use,
    /// so each test holds the writes of one node until a client has been answered from a single free pool thread.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterConfigWriteThreadTests : TestBase
    {
        const string NodesConf = "nodes.conf";
        const string ReplicationConf = "replication.conf";
        const string Pong = "+PONG\r\n";
        const string Ok = "+OK\r\n";
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);
        static readonly TimeSpan OperationBound = TimeSpan.FromSeconds(60);

        ClusterTestContext context;
        HeldWriteDeviceFactoryCreator held;

        [SetUp]
        public void Setup()
        {
            held = null;
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            held?.ReleaseWrites();
            context?.TearDown();
        }

        /// <summary>Wraps the device factory of <paramref name="node"/> so that the writes of <paramref name="files"/> can be held.</summary>
        Action<int, GarnetServerOptions> HoldWritesOf(int node, params string[] files)
            => (i, options) =>
            {
                if (i == node)
                    options.DeviceFactoryCreator = held = new HeldWriteDeviceFactoryCreator(options.DeviceFactoryCreator, files);
            };

        ClusterProvider Provider(int node) => (ClusterProvider)context.nodes[node].Provider.StoreWrapper.clusterProvider;

        /// <summary>
        /// With batched flushes, the configuration is written by a background task every period.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void ABatchedConfigurationWriteHoldsNoPoolThread()
        {
            context.CreateInstances(1, clusterConfigFlushFrequencyMs: 50, configureNode: HoldWritesOf(0, NodesConf));
            using var session = new RawRespSession(endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, session.Execute("PING"));

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                session.Send("CLUSTER", "BUMPEPOCH");
                ClassicAssert.AreEqual(Ok, ReadReplyWithin(session, "CLUSTER BUMPEPOCH"));

                DriveHeldWrites(session, () => held.TotalWritesHeld >= 3, "three batched configuration writes");
            }
        }

        /// <summary>
        /// <c>REPLICAOF NO ONE</c> resets the replica's configuration and its replication history, writing both files.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void ReplicaOfNoOneHoldsNoPoolThread()
        {
            context.CreateInstances(2, enableAOF: true, disableObjects: true, configureNode: HoldWritesOf(1, NodesConf, ReplicationConf));
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[1]);
            ClassicAssert.AreEqual(Pong, replica.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                replica.Send("REPLICAOF", "NO", "ONE");
                DriveHeldWrites(pinger, () => replica.WaitForReply(TimeSpan.Zero), "REPLICAOF NO ONE");
                ClassicAssert.AreEqual(Ok, replica.ReadReply());
            }

            ClassicAssert.AreEqual(ClusterNodeRole.PRIMARY, Provider(1).clusterManager.CurrentConfig.LocalNodeRole);
        }

        /// <summary>
        /// A replica that fails over takes over its primary's slots and starts a new replication history.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void AReplicaFailoverHoldsNoPoolThread()
        {
            context.CreateInstances(2, enableAOF: true, disableObjects: true, configureNode: HoldWritesOf(1, NodesConf, ReplicationConf));
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[1]);
            ClassicAssert.AreEqual(Pong, replica.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var failover = Provider(1).failoverManager;

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                replica.Send("CLUSTER", "FAILOVER");
                DriveHeldWrites(pinger, () => replica.WaitForReply(TimeSpan.Zero)
                    && failover.lastFailoverStatus is FailoverStatus.FAILOVER_COMPLETED or FailoverStatus.FAILOVER_ABORTED,
                    "the replica's failover");
                ClassicAssert.AreEqual(Ok, replica.ReadReply());
            }

            ClassicAssert.AreEqual(FailoverStatus.FAILOVER_COMPLETED, failover.lastFailoverStatus);
            ClassicAssert.AreEqual(ClusterNodeRole.PRIMARY, Provider(1).clusterManager.CurrentConfig.LocalNodeRole);
        }

        /// <summary>
        /// A primary that fails over to its replica first stops taking writes.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void APrimaryFailoverHoldsNoPoolThread()
        {
            context.CreateInstances(2, enableAOF: true, disableObjects: true, configureNode: HoldWritesOf(0, NodesConf));
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            using var primary = new RawRespSession(endpoint: context.endpoints[0]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, primary.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var noFailover = FailoverUtils.GetFailoverStatus(FailoverStatus.NO_FAILOVER);
            var replica = (IPEndPoint)context.endpoints[1];

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                primary.Send("FAILOVER", "TO", replica.Address.ToString(), replica.Port.ToString(), "TIMEOUT", "30000");
                DriveHeldWrites(pinger,
                    () => primary.WaitForReply(TimeSpan.Zero)
                        && Provider(1).clusterManager.CurrentConfig.LocalNodeRole == ClusterNodeRole.PRIMARY
                        && Provider(0).failoverManager.GetFailoverStatus() == noFailover,
                    "the primary's failover");
                ClassicAssert.AreEqual(Ok, primary.ReadReply());
            }
        }

        /// <summary>
        /// A command that changes the configuration on the session that received it answers once the change is written.
        /// The session waits for the write parked, so another client is answered from the one free pool thread.
        /// </summary>
        /// <param name="setup">A command run before writes are held, or empty; <c>{self}</c> stands for the node's id.</param>
        /// <param name="command">The command tested; <c>{self}</c> stands for the node's id.</param>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        [TestCase("", "CLUSTER ADDSLOTS 1")]
        [TestCase("", "CLUSTER ADDSLOTSRANGE 1 10")]
        [TestCase("CLUSTER ADDSLOTS 0", "CLUSTER DELSLOTS 0")]
        [TestCase("CLUSTER ADDSLOTS 0", "CLUSTER DELSLOTSRANGE 0 0")]
        [TestCase("", "CLUSTER SETSLOT 1 NODE {self}")]
        [TestCase("", "CLUSTER SETSLOTSRANGE STABLE 0 0")]
        [TestCase("", "CLUSTER BUMPEPOCH")]
        [TestCase("", "CLUSTER SET-CONFIG-EPOCH 5")]
        [TestCase("CLUSTER ADDSLOTS 0", "CLUSTER RESET SOFT")]
        [TestCase("", "CONFIG REWRITE")]
        [TestCase("", "FAILOVER ABORT")]
        public void AConfigurationChangeHoldsNoPoolThread(string setup, string command)
        {
            context.CreateInstances(1, enableAOF: true, disableObjects: true, configureNode: HoldWritesOf(0, NodesConf));
            using var session = new RawRespSession(endpoint: context.endpoints[0]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[0]);
            var self = Provider(0).clusterManager.CurrentConfig.LocalNodeId;
            string[] Args(string line) => line.Replace("{self}", self).Split(' ');
            if (setup.Length > 0)
                ClassicAssert.AreEqual(Ok, session.Execute(Args(setup)));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                session.Send(Args(command));
                DriveHeldWrites(pinger, () => session.WaitForReply(TimeSpan.Zero), command);
                ClassicAssert.AreEqual(Ok, session.ReadReply());
            }
        }

        /// <summary>
        /// A <c>CLUSTER FORGET</c> keeps config merges suspended until the configuration it changed is written. Another
        /// that waits for merges to be resumed waits parked, so a third client is answered from the one free pool thread.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void AForgetWaitingForMergesToResumeHoldsNoPoolThread()
        {
            context.CreateInstances(3, configureNode: HoldWritesOf(0, NodesConf));
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(3, 0, logger: context.logger);
            var first = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            var second = context.clusterTestUtils.GetNodeIdFromNode(2, context.logger);

            using var forgetFirst = new RawRespSession(endpoint: context.endpoints[0]);
            using var forgetSecond = new RawRespSession(endpoint: context.endpoints[0]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, forgetFirst.Execute("PING"));
            ClassicAssert.AreEqual(Pong, forgetSecond.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var clusterManager = Provider(0).clusterManager;

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                forgetFirst.Send("CLUSTER", "FORGET", first);
                WaitUntil(() => held.WritesHeld > 0 && !clusterManager.CurrentConfig.IsKnown(first), "the first CLUSTER FORGET to be written");

                forgetSecond.Send("CLUSTER", "FORGET", second);
                Thread.Sleep(100);
                pinger.Send("PING");
                ClassicAssert.AreEqual(Pong, ReadReplyWithin(pinger, "PING, with one free pool thread, while a CLUSTER FORGET waits for merges to resume"));
                ClassicAssert.IsTrue(clusterManager.CurrentConfig.IsKnown(second), "The second CLUSTER FORGET ran while merges were suspended");

                DriveHeldWrites(pinger, () => forgetFirst.WaitForReply(TimeSpan.Zero) && forgetSecond.WaitForReply(TimeSpan.Zero), "both CLUSTER FORGETs");
                ClassicAssert.AreEqual(Ok, forgetFirst.ReadReply());
                ClassicAssert.AreEqual(Ok, forgetSecond.ReadReply());
            }

            ClassicAssert.IsFalse(clusterManager.CurrentConfig.IsKnown(second));
        }

        /// <summary>
        /// A node merges the configuration of a node it meets.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void AMeetHoldsNoPoolThread()
        {
            context.CreateInstances(2, configureNode: HoldWritesOf(0, NodesConf));
            context.CreateConnection();
            var other = context.clusterTestUtils.GetEndPoint(1);
            var otherId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);

            using var session = new RawRespSession(endpoint: context.endpoints[0]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, session.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                session.Send("CLUSTER", "MEET", other.Address.ToString(), other.Port.ToString());
                DriveHeldWrites(pinger,
                    () => session.WaitForReply(TimeSpan.Zero) && Provider(0).clusterManager.CurrentConfig.IsKnown(otherId),
                    "the meet");
                ClassicAssert.AreEqual(Ok, session.ReadReply());
            }
        }

        /// <summary>
        /// A primary that migrates a slot marks it migrating, and gives it up once the target owns it.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void MigratingASlotHoldsNoPoolThread()
        {
            context.CreateInstances(2, configureNode: HoldWritesOf(0, NodesConf));
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(2, 0, logger: context.logger);
            var target = context.clusterTestUtils.GetEndPoint(1);
            var targetId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            ClassicAssert.AreNotEqual(targetId, Provider(0).clusterManager.CurrentConfig.GetNodeIdFromSlot(0));

            using var source = new RawRespSession(endpoint: context.endpoints[0]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, source.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var migrations = Provider(0).migrationManager;

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                source.Send("MIGRATE", target.Address.ToString(), target.Port.ToString(), "", "0", "-1", "SLOTS", "0");
                DriveHeldWrites(pinger,
                    () => source.WaitForReply(TimeSpan.Zero)
                        && Provider(0).clusterManager.CurrentConfig.GetNodeIdFromSlot(0) == targetId
                        && migrations.GetMigrationTaskCount() == 0,
                    "the migration");
                ClassicAssert.AreEqual(Ok, source.ReadReply());
            }
        }

        /// <summary>
        /// A migration that the target refuses returns the source's slots to their state before it.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void AFailedMigrationHoldsNoPoolThread()
        {
            context.CreateInstances(2, configureNode: HoldWritesOf(0, NodesConf));
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(2, 0, logger: context.logger);
            var target = context.clusterTestUtils.GetEndPoint(1);
            var sourceId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            ClassicAssert.AreEqual(sourceId, Provider(0).clusterManager.CurrentConfig.GetNodeIdFromSlot(0));
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.SetSlot(1, 0, "IMPORTING", sourceId, context.logger));

            using var source = new RawRespSession(endpoint: context.endpoints[0]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, source.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var migrations = Provider(0).migrationManager;

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                source.Send("MIGRATE", target.Address.ToString(), target.Port.ToString(), "", "0", "-1", "SLOTS", "0");
                DriveHeldWrites(pinger,
                    () => source.WaitForReply(TimeSpan.Zero) && migrations.GetMigrationTaskCount() == 0,
                    "the failed migration");
                ClassicAssert.AreEqual(Ok, source.ReadReply());
            }

            var config = Provider(0).clusterManager.CurrentConfig;
            ClassicAssert.AreEqual(sourceId, config.GetNodeIdFromSlot(0));
            ClassicAssert.AreEqual(SlotState.STABLE, config.GetState(0));
        }

        /// <summary>
        /// A replica that attaches to a primary records it and starts the primary's replication history.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void AReplicaAttachHoldsNoPoolThread()
        {
            context.CreateInstances(2, enableAOF: true, disableObjects: true, configureNode: HoldWritesOf(1, NodesConf, ReplicationConf));
            context.CreateConnection();
            var utils = context.clusterTestUtils;
            ClassicAssert.AreEqual("OK", utils.AddSlotsRange(0, [(0, 16383)], context.logger));
            utils.SetConfigEpoch(0, 1, context.logger);
            utils.SetConfigEpoch(1, 2, context.logger);
            utils.Meet(0, 1, context.logger);
            utils.WaitUntilNodeIsKnown(0, 1, context.logger);
            utils.WaitUntilNodeIsKnown(1, 0, context.logger);
            var primaryId = utils.GetNodeIdFromNode(0, context.logger);

            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[1]);
            ClassicAssert.AreEqual(Pong, replica.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));

            using (ThreadPoolCap.ApplyFree(1))
            {
                held.HoldWrites = true;
                replica.Send("CLUSTER", "REPLICATE", primaryId, "SYNC");
                DriveHeldWrites(pinger, () => replica.WaitForReply(TimeSpan.Zero), "CLUSTER REPLICATE");
                ClassicAssert.AreEqual(Ok, replica.ReadReply());
            }

            ClassicAssert.AreEqual(ClusterNodeRole.REPLICA, Provider(1).clusterManager.CurrentConfig.LocalNodeRole);
        }

        /// <summary>
        /// A command whose configuration write follows one already in flight from an async caller answers once its own
        /// write completes, which must follow the one in flight without another pool thread.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void AConfigurationWriteWaitedForOnThePoolNeedsNoOtherPoolThreadToFollowTheOneInFlight()
        {
            context.CreateInstances(1, configureNode: HoldWritesOf(0, NodesConf));
            using var session = new RawRespSession(endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, session.Execute("PING"));
            var clusterManager = Provider(0).clusterManager;

            held.HoldWrites = true;
            var inFlight = clusterManager.FlushConfigAsync().AsTask();
            WaitUntil(() => held.WritesHeld == 1, "the async configuration write to be held");

            using (ThreadPoolCap.ApplyFree(1))
            {
                var epoch = clusterManager.CurrentConfig.LocalNodeConfigEpoch;
                session.Send("CLUSTER", "BUMPEPOCH");
                WaitUntil(() => clusterManager.CurrentConfig.LocalNodeConfigEpoch != epoch, "CLUSTER BUMPEPOCH to change the configuration");
                Thread.Sleep(100);

                var clock = Stopwatch.StartNew();
                while (!session.WaitForReply(TimeSpan.FromMilliseconds(10)))
                {
                    ClassicAssert.Less(clock.Elapsed, Bound, "CLUSTER BUMPEPOCH was not answered once the write in flight completed");
                    held.CompleteHeldWrites();
                }

                ClassicAssert.AreEqual(Ok, session.ReadReply());
            }

            ClassicAssert.IsTrue(inFlight.Wait(Bound), "The async configuration write did not complete");
        }

        static void WaitUntil(Func<bool> condition, string what)
        {
            var clock = Stopwatch.StartNew();
            while (!condition())
            {
                ClassicAssert.Less(clock.Elapsed, Bound, $"Timed out waiting for {what}");
                Thread.Sleep(10);
            }
        }

        /// <summary>
        /// Runs until <paramref name="done"/>, and at least until one configuration write has been held. While each
        /// write is held, <paramref name="pinger"/> must be answered, with one free pool thread, before the write is
        /// let complete.
        /// </summary>
        void DriveHeldWrites(RawRespSession pinger, Func<bool> done, string what)
        {
            var clock = Stopwatch.StartNew();
            var pinged = 0;
            while (pinged == 0 || !done())
            {
                ClassicAssert.Less(clock.Elapsed, OperationBound, $"Timed out waiting for {what} ({held.TotalWritesHeld} configuration writes held)");
                if (held.WritesHeld == 0)
                {
                    Thread.Sleep(10);
                    continue;
                }

                pinger.Send("PING");
                ClassicAssert.AreEqual(Pong, ReadReplyWithin(pinger, $"PING, with one free pool thread, while a configuration write is held during {what}"));
                pinged++;
                held.CompleteHeldWrites();
            }
        }

        static string ReadReplyWithin(RawRespSession session, string what)
        {
            ClassicAssert.IsTrue(session.WaitForReply(Bound), $"{what}: no reply within {Bound.TotalSeconds:F0}s");
            return session.ReadReply();
        }
    }
}