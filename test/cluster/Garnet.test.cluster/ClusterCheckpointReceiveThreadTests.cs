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
    /// session that receives a chunk parks while the write runs, so each test holds the write of a chunk until another
    /// client has been answered from a single free pool thread.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterCheckpointReceiveThreadTests : TestBase
    {
        const string IndexFile = "ht.dat";
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        const string Token = "0123456789abcdef";
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

        /// <summary>Wraps the device factory of <paramref name="node"/> so that the writes of its index checkpoints can be held.</summary>
        Action<int, GarnetServerOptions> HoldIndexWritesOf(int node)
            => (i, options) =>
            {
                if (i == node)
                    options.DeviceFactoryCreator = held = new HeldWriteDeviceFactoryCreator(options.DeviceFactoryCreator, IndexFile);
            };

        ClusterProvider Provider(int node) => (ClusterProvider)context.nodes[node].Provider.StoreWrapper.clusterProvider;

        /// <summary>
        /// A replica that syncs from its primary's checkpoint receives each file as <c>CLUSTER SNAPSHOT_DATA</c> chunks.
        /// <c>CLUSTER SEND_CKPT_FILE_SEGMENT</c>, which carried them before, writes them the same way.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("CLUSTER")]
        public void ReceivingACheckpointChunkHoldsNoPoolThread([Values("SNAPSHOT_DATA", "SEND_CKPT_FILE_SEGMENT")] string command)
        {
            context.CreateInstances(1, configureNode: HoldIndexWritesOf(0));
            var replicationManager = Provider(0).replicationManager;

            // A replica's sync receives through this handler while it lasts
            replicationManager.recvCheckpointHandler = new ReceiveCheckpointHandler(Provider(0));
            try
            {
                using var sender = new RawRespSession(endpoint: context.endpoints[0]);
                using var probe = new RawRespSession(endpoint: context.endpoints[0]);
                ClassicAssert.AreEqual(Pong, sender.Execute("PING"));
                ClassicAssert.AreEqual(Pong, probe.Execute("PING"));

                using (ThreadPoolCap.ApplyFree(1))
                {
                    held.HoldWrites = true;
                    sender.Send(Chunk(command, new string('x', 1000)));
                    WaitForAHeldWrite(sender, $"CLUSTER {command}");
                    ClassicAssert.IsFalse(sender.WaitForReply(TimeSpan.FromMilliseconds(100)), "The chunk was answered before its write completed");

                    probe.Send("PING");
                    ClassicAssert.AreEqual(Pong, ReadReplyWithin(probe, "PING, with one free pool thread, while the chunk's write is held"));

                    held.ReleaseWrites();
                    ClassicAssert.AreEqual(Ok, ReadReplyWithin(sender, $"CLUSTER {command}, once its write completed"));
                }

                // An empty chunk ends the file
                ClassicAssert.AreEqual(Ok, sender.Execute(Chunk(command, "")));
            }
            finally
            {
                replicationManager.recvCheckpointHandler.Dispose();
                replicationManager.recvCheckpointHandler = null;
            }
        }

        /// <summary>A chunk of the index file at its start, as <c>CLUSTER</c> <paramref name="command"/> carries it.</summary>
        static string[] Chunk(string command, string data)
        {
            var indexType = ((int)CheckpointFileType.STORE_INDEX).ToString();
            return command == "SNAPSHOT_DATA"
                ? ["CLUSTER", command, Token, indexType, "0", data]
                : ["CLUSTER", command, Token, indexType, "0", data, "0"];
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

        static string ReadReplyWithin(RawRespSession session, string what)
        {
            ClassicAssert.IsTrue(session.WaitForReply(Bound), $"{what}: no reply within {Bound.TotalSeconds:F0}s");
            return session.ReadReply();
        }
    }
}