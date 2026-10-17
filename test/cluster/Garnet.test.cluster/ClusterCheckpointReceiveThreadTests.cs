// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Garnet.cluster;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A replica receives a checkpoint as a stream of chunks, each written to its file before the chunk is answered. The
    /// commands that carry them run on an executor thread, so each test gives the replica a single executor thread and
    /// holds the write of a chunk until a command that also runs there has been answered.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterCheckpointReceiveThreadTests : TestBase
    {
        const string IndexFile = "ht.dat";
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

        /// <summary>Gives <paramref name="node"/> one executor thread, and the means to hold the writes of its index checkpoints.</summary>
        Action<int, GarnetServerOptions> OneExecutorThreadHoldingIndexWritesOf(int node)
            => (i, options) =>
            {
                if (i != node)
                    return;

                options.OffloadMaxThreads = 1;
                options.DeviceFactoryCreator = held = new HeldWriteDeviceFactoryCreator(options.DeviceFactoryCreator, IndexFile);
            };

        ClusterProvider Provider(int node) => (ClusterProvider)context.nodes[node].Provider.StoreWrapper.clusterProvider;

        /// <summary>
        /// A replica that syncs from its primary's checkpoint receives the index file as <c>CLUSTER SNAPSHOT_DATA</c> chunks.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void ReceivingACheckpointHoldsNoExecutorThread()
        {
            context.CreateInstances(2, enableAOF: true, configureNode: OneExecutorThreadHoldingIndexWritesOf(1));
            context.CreateConnection();
            var utils = context.clusterTestUtils;
            ClassicAssert.AreEqual("OK", utils.AddSlotsRange(0, [(0, 16383)], context.logger));
            utils.SetConfigEpoch(0, 1, context.logger);
            utils.SetConfigEpoch(1, 2, context.logger);
            utils.Meet(0, 1, context.logger);
            utils.WaitUntilNodeIsKnown(0, 1, context.logger);
            utils.WaitUntilNodeIsKnown(1, 0, context.logger);
            context.kvPairs = [];
            context.SimplePopulateDB(disableObjects: true, keyLength: 16, kvpairCount: 100, primaryIndex: 0);
            utils.Checkpoint(0, context.logger);
            var primaryId = utils.GetNodeIdFromNode(0, context.logger);

            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            using var probe = new RawRespSession(endpoint: context.endpoints[1]);
            held.HoldWrites = true;
            replica.Send("CLUSTER", "REPLICATE", primaryId, "SYNC");
            WaitForAHeldWrite(replica, "CLUSTER REPLICATE");

            Probe(1, probe, "while the replica's index checkpoint write is held");

            held.ReleaseWrites();
            ClassicAssert.IsTrue(replica.WaitForReply(OperationBound), "CLUSTER REPLICATE: no reply once the write completed");
            ClassicAssert.AreEqual(Ok, replica.ReadReply());

            utils.WaitForReplicaAofSync(0, 1, context.logger);
            context.SimpleValidateDB(disableObjects: true, replicaIndex: 1);
        }

        /// <summary>
        /// <c>CLUSTER SEND_CKPT_FILE_SEGMENT</c>, which carried checkpoint chunks before <c>CLUSTER SNAPSHOT_DATA</c>, writes
        /// them as <c>CLUSTER SNAPSHOT_DATA</c> does.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void ReceivingACheckpointSegmentHoldsNoExecutorThread()
        {
            context.CreateInstances(1, configureNode: OneExecutorThreadHoldingIndexWritesOf(0));
            var replicationManager = Provider(0).replicationManager;

            // A replica's sync receives through this handler while it lasts
            replicationManager.recvCheckpointHandler = new ReceiveCheckpointHandler(Provider(0));
            try
            {
                using var sender = new RawRespSession(endpoint: context.endpoints[0]);
                using var probe = new RawRespSession(endpoint: context.endpoints[0]);
                const string token = "0123456789abcdef";
                var indexType = ((int)CheckpointFileType.STORE_INDEX).ToString();

                held.HoldWrites = true;
                sender.Send("CLUSTER", "SEND_CKPT_FILE_SEGMENT", token, indexType, "0", new string('x', 1000), "0");
                WaitForAHeldWrite(sender, "CLUSTER SEND_CKPT_FILE_SEGMENT");
                ClassicAssert.IsFalse(sender.WaitForReply(TimeSpan.FromMilliseconds(100)), "The segment was answered before its write completed");

                Probe(0, probe, "while the segment's write is held");

                held.ReleaseWrites();
                ClassicAssert.AreEqual(Ok, ReadReplyWithin(sender, "CLUSTER SEND_CKPT_FILE_SEGMENT, once its write completed"));
                ClassicAssert.AreEqual(Ok, sender.Execute("CLUSTER", "SEND_CKPT_FILE_SEGMENT", token, indexType, "0", "", "0"));
            }
            finally
            {
                replicationManager.recvCheckpointHandler.Dispose();
                replicationManager.recvCheckpointHandler = null;
            }
        }

        /// <summary>Waits for a write to be held, failing if <paramref name="session"/> is answered first.</summary>
        void WaitForAHeldWrite(RawRespSession session, string what)
        {
            var clock = Stopwatch.StartNew();
            while (held.WritesHeld == 0)
            {
                ClassicAssert.Less(clock.Elapsed, OperationBound, $"{what}: no write held within {OperationBound.TotalSeconds:F0}s");
                if (session.WaitForReply(TimeSpan.FromMilliseconds(10)))
                    Assert.Fail($"{what} was answered with no write held: {session.ReadReply()}");
            }
        }

        /// <summary>
        /// Sends INFO to <paramref name="node"/>, where it runs on an executor thread as the node has a storage tier, and
        /// expects it to be answered.
        /// </summary>
        void Probe(int node, RawRespSession probe, string when)
        {
            var executor = context.nodes[node].Provider.StoreWrapper.offloadExecutor;
            var posts = executor.Posts;
            probe.Send("INFO", "server");
            StringAssert.StartsWith("$", ReadReplyWithin(probe, $"INFO, with one executor thread, {when}"));
            ClassicAssert.Greater(executor.Posts, posts, "INFO did not run on an executor thread");
        }

        static string ReadReplyWithin(RawRespSession session, string what)
        {
            ClassicAssert.IsTrue(session.WaitForReply(Bound), $"{what}: no reply within {Bound.TotalSeconds:F0}s");
            return session.ReadReply();
        }
    }
}