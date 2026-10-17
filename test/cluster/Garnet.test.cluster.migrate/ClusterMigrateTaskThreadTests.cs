// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Garnet.cluster;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A slot migration runs as a background task on the pool. A wait inside it that held its thread would be a thread
    /// the node's clients could not use.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterMigrateTaskThreadTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);
        static readonly TimeSpan OperationBound = TimeSpan.FromSeconds(60);

        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown() => context?.TearDown();

        ClusterProvider Provider(int node) => (ClusterProvider)context.nodes[node].Provider.StoreWrapper.clusterProvider;

        /// <summary>
        /// A migration pauses revivification and waits until every thread protected by the store's epoch has observed
        /// the pause, so a thread that stays protected keeps the migration waiting.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void PausingRevivificationHoldsNoPoolThread()
        {
            context.CreateInstances(2);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(2, 0, logger: context.logger);
            var target = context.clusterTestUtils.GetEndPoint(1);
            var sourceId = context.clusterTestUtils.GetNodeIdFromNode(0, context.logger);
            var targetId = context.clusterTestUtils.GetNodeIdFromNode(1, context.logger);
            ClassicAssert.AreEqual(sourceId, Provider(0).clusterManager.CurrentConfig.GetNodeIdFromSlot(0));

            using var source = new RawRespSession(endpoint: context.endpoints[0]);
            using var pinger = new RawRespSession(endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, source.Execute("PING"));
            ClassicAssert.AreEqual(Pong, pinger.Execute("PING"));
            var migrations = Provider(0).migrationManager;

            var epoch = context.nodes[0].Provider.StoreWrapper.DefaultDatabase.Epoch;
            using var protectedBeforePause = new ManualResetEventSlim();
            using var refresh = new ManualResetEventSlim();
            var holder = new Thread(() =>
            {
                epoch.Resume();
                protectedBeforePause.Set();
                refresh.Wait();
                epoch.Suspend();
            });
            holder.Start();
            try
            {
                protectedBeforePause.Wait();
                using (ThreadPoolCap.ApplyFree(1))
                {
                    source.Send("MIGRATE", target.Address.ToString(), target.Port.ToString(), "", "0", "-1", "SLOTS", "0");
                    ClassicAssert.AreEqual(Ok, ReadReplyWithin(source, "MIGRATE"));

                    var clock = Stopwatch.StartNew();
                    while (clock.Elapsed < TimeSpan.FromSeconds(1))
                    {
                        pinger.Send("PING");
                        ClassicAssert.AreEqual(Pong, ReadReplyWithin(pinger, "PING, with one free pool thread, while a migration waits for revivification to pause"));
                        Thread.Sleep(50);
                    }

                    ClassicAssert.AreEqual(1, migrations.GetMigrationTaskCount(), "The migration ended while a protected thread had not observed the pause");
                    ClassicAssert.AreEqual(sourceId, Provider(0).clusterManager.CurrentConfig.GetNodeIdFromSlot(0));
                }

                refresh.Set();
                holder.Join();
                var done = Stopwatch.StartNew();
                while (Provider(0).clusterManager.CurrentConfig.GetNodeIdFromSlot(0) != targetId || migrations.GetMigrationTaskCount() != 0)
                {
                    ClassicAssert.Less(done.Elapsed, OperationBound, "The migration did not finish once the protected thread refreshed");
                    Thread.Sleep(10);
                }
            }
            finally
            {
                refresh.Set();
                holder.Join();
            }
        }

        static string ReadReplyWithin(RawRespSession session, string what)
        {
            ClassicAssert.IsTrue(session.WaitForReply(Bound), $"{what}: no reply within {Bound.TotalSeconds:F0}s");
            return session.ReadReply();
        }
    }
}