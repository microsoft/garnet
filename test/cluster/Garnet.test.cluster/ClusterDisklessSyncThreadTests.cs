// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.cluster;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using ClusterNodeRole = Garnet.cluster.NodeRole;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A primary syncs a replica without a disk by scanning its store into the connection to the replica, and the scan
    /// waits for the replica to answer what it has sent. The scan runs in the steps of a checkpoint, which must not hold
    /// the pool thread that those answers need to arrive on.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterDisklessSyncThreadTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        const int KeyCount = 1024;
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
        /// A replica attaches to a primary that streams it the store from a single free pool thread. The stream waits for
        /// the replica to answer each buffer it sends, and the answers arrive on that pool thread.
        /// </summary>
        [Test, CancelAfter(150_000)]
        [Category("REPLICATION")]
        public void ADisklessSyncStreamsTheStoreFromOneFreePoolThread()
        {
            context.CreateInstances(2, enableAOF: true, enableDisklessSync: true, replicaDisklessSyncDelay: 0, disableObjects: true);
            context.CreateConnection();
            var utils = context.clusterTestUtils;
            for (var i = 0; i < 2; i++)
                utils.SetConfigEpoch(i, i + 1, logger: context.logger);
            ClassicAssert.AreEqual("OK", utils.AddDelSlotsRange(0, [(0, 16383)], addslot: true, context.logger));
            utils.Meet(0, 1, logger: context.logger);
            utils.WaitForConfigPropagation(0, [0, 1], context.logger);
            var primaryId = utils.ClusterMyId(0, context.logger);

            // More than one buffer of the connection to the replica, so the stream waits on the replica more than once
            using var primary = new RawRespSession(bufferSize: 256 * 1024, endpoint: context.endpoints[0]);
            var value = new string('v', 4096);
            for (var i = 0; i < KeyCount; i++)
                ClassicAssert.AreEqual(Ok, primary.Execute("SET", $"key{i}", value));

            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            ClassicAssert.AreEqual(Pong, replica.Execute("PING"));

            using (ThreadPoolCap.ApplyFree(1))
            {
                replica.Send("CLUSTER", "REPLICATE", primaryId, "SYNC");
                ClassicAssert.IsTrue(replica.WaitForReply(OperationBound), $"CLUSTER REPLICATE: no reply within {OperationBound.TotalSeconds:F0}s");
                ClassicAssert.AreEqual(Ok, replica.ReadReply());
            }

            ClassicAssert.AreEqual(ClusterNodeRole.REPLICA, Provider(1).clusterManager.CurrentConfig.LocalNodeRole);
            ClassicAssert.AreEqual($":{KeyCount}\r\n", replica.Execute("DBSIZE"));
        }
    }
}