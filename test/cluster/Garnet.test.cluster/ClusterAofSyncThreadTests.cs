// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Diagnostics;
using System.Threading;
using Garnet.cluster;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using ClusterNodeRole = Garnet.cluster.NodeRole;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A primary streams its AOF to each replica from tasks on the thread pool. A stream that waits for a replica to read
    /// what it sent, and a node that waits for a stream to exit when it stops streaming, could each hold the pool thread
    /// that the sends they wait for need to complete.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterAofSyncThreadTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        static readonly TimeSpan OperationBound = TimeSpan.FromSeconds(60);
        static readonly TimeSpan StallBound = TimeSpan.FromSeconds(1);
        const ExceptionInjectionType HoldReplay = ExceptionInjectionType.Replication_Hold_Replay_Thread;

        ClusterTestContext context;

        [SetUp]
        public void Setup()
        {
            context = new ClusterTestContext();
            context.Setup([]);
        }

        [TearDown]
        public void TearDown()
        {
            ExceptionInjectionHelper.DisableException(HoldReplay);
            context?.TearDown();
        }

        ClusterProvider Provider(int node) => (ClusterProvider)context.nodes[node].Provider.StoreWrapper.clusterProvider;

        /// <summary>
        /// A primary that becomes a replica stops streaming its AOF to its own replica before it attaches, from a single
        /// free pool thread, while that stream is stalled on a replica that has stopped reading it. The stalled stream
        /// can only finish once a pool thread has completed the sends that stopping it failed.
        /// </summary>
        [Test, CancelAfter(150_000)]
        [Category("REPLICATION")]
        public void APrimaryThatBecomesAReplicaStopsAStalledStreamFromOneFreePoolThread()
        {
            context.CreateInstances(3, enableAOF: true, disableObjects: true, replayTaskCount: 2);
            context.CreateConnection();
            for (var i = 0; i < 3; i++)
                context.clusterTestUtils.SetConfigEpoch(i, i + 1, logger: context.logger);
            context.clusterTestUtils.Meet(0, 1, logger: context.logger);
            context.clusterTestUtils.Meet(0, 2, logger: context.logger);
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, context.logger));
            context.clusterTestUtils.WaitForConfigPropagation(0, [0, 1, 2], context.logger);

            _ = context.clusterTestUtils.ClusterReplicate(1, 0, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            ClassicAssert.AreEqual(1, Provider(0).replicationManager.AofSyncDriverStore.AofSyncDriverCount);

            using var node0 = new RawRespSession(bufferSize: 256 * 1024, endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, node0.Execute("PING"));
            var node2Id = context.clusterTestUtils.ClusterMyId(2, context.logger);

            ExceptionInjectionHelper.EnableException(HoldReplay);
            try
            {
                // More than the connection to the replica buffers, so the stream ends up waiting on its sends
                var value = new string('v', 64 * 1024);
                for (var i = 0; i < 1024; i++)
                    ClassicAssert.AreEqual(Ok, node0.Execute("SET", $"key{i}", value));
                WaitUntilStalled();

                // A primary that owns no slots can become a replica while it streams to its own
                ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: false, context.logger));

                using (ThreadPoolCap.ApplyFree(1))
                {
                    node0.Send("CLUSTER", "REPLICATE", node2Id, "SYNC");
                    ClassicAssert.IsTrue(node0.WaitForReply(OperationBound), $"CLUSTER REPLICATE: no reply within {OperationBound.TotalSeconds:F0}s");
                    ClassicAssert.AreEqual(Ok, node0.ReadReply());
                }
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(HoldReplay);
            }

            ClassicAssert.AreEqual(0, Provider(0).replicationManager.AofSyncDriverStore.AofSyncDriverCount);
            ClassicAssert.AreEqual(ClusterNodeRole.REPLICA, Provider(0).clusterManager.CurrentConfig.LocalNodeRole);
        }

        /// <summary>
        /// A primary keeps serving from a single free pool thread while its stream to a replica that has stopped reading
        /// waits for the replica, and the stream resumes once the replica reads again.
        /// </summary>
        [Test, CancelAfter(150_000)]
        [Category("REPLICATION")]
        public void APrimaryKeepsServingFromOneFreePoolThreadWhileItsStreamWaitsOnAStalledReplica()
        {
            context.CreateInstances(2, enableAOF: true, disableObjects: true, replayTaskCount: 2);
            context.CreateConnection();
            for (var i = 0; i < 2; i++)
                context.clusterTestUtils.SetConfigEpoch(i, i + 1, logger: context.logger);
            context.clusterTestUtils.Meet(0, 1, logger: context.logger);
            ClassicAssert.AreEqual("OK", context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, context.logger));
            context.clusterTestUtils.WaitForConfigPropagation(0, [0, 1], context.logger);

            _ = context.clusterTestUtils.ClusterReplicate(1, 0, logger: context.logger);
            context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);

            using var node0 = new RawRespSession(bufferSize: 256 * 1024, endpoint: context.endpoints[0]);
            ClassicAssert.AreEqual(Pong, node0.Execute("PING"));

            using (ThreadPoolCap.ApplyFree(1))
            {
                ExceptionInjectionHelper.EnableException(HoldReplay);
                try
                {
                    // More than the connection to the replica buffers, so the stream ends up waiting on its sends
                    var value = new string('v', 64 * 1024);
                    for (var i = 0; i < 1024; i++)
                        ClassicAssert.AreEqual(Ok, ExecuteWithinBound(node0, $"SET key{i}", "SET", $"key{i}", value));
                    WaitUntilStalled();

                    ClassicAssert.AreEqual(Pong, ExecuteWithinBound(node0, "PING", "PING"));
                }
                finally
                {
                    ExceptionInjectionHelper.DisableException(HoldReplay);
                }

                context.clusterTestUtils.WaitForReplicaAofSync(0, 1, context.logger);
            }
        }

        static string ExecuteWithinBound(RawRespSession session, string what, params string[] command)
        {
            session.Send(command);
            ClassicAssert.IsTrue(session.WaitForReply(OperationBound), $"{what}: no reply within {OperationBound.TotalSeconds:F0}s");
            return session.ReadReply();
        }

        /// <summary>
        /// Waits until node 0 has stopped shipping its AOF to node 1, with node 1 holding the replay of what it received.
        /// </summary>
        void WaitUntilStalled()
        {
            var clock = Stopwatch.StartNew();
            var shipped = Shipped();
            var since = clock.Elapsed;
            while (ExceptionInjectionHelper.WaitingOnClear(HoldReplay) == 0 || clock.Elapsed - since < StallBound)
            {
                ClassicAssert.Less(clock.Elapsed, OperationBound, "Timed out waiting for the stream to node 1 to stall");
                Thread.Sleep(50);

                var now = Shipped();
                if (now != shipped)
                    (shipped, since) = (now, clock.Elapsed);
            }
        }

        long Shipped()
        {
            var replicationManager = Provider(0).replicationManager;
            return replicationManager.AofSyncDriverStore.GetReplicaInfo(replicationManager.ReplicationOffset)[0].replication_offset[0];
        }
    }
}
#endif