// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Diagnostics;
using System.Threading;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A replica replays the AOF stream it receives from its primary on threads of its own. Replaying a page can wait, on
    /// the other replay tasks or on a record that waits, and a pool thread that waited for it would be a thread the
    /// replica's clients could not use.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterReplicaReplayThreadTests : TestBase
    {
        const ExceptionInjectionType Hold = ExceptionInjectionType.Replication_Hold_Replay_Thread;
        const string Key = "key";
        const string Pong = "+PONG\r\n";
        const string Nil = "$-1\r\n";
        const string ValueReply = "$5\r\nvalue\r\n";
        static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

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
            ExceptionInjectionHelper.DisableException(Hold);
            context?.TearDown();
        }

        /// <summary>
        /// While the replay of a write is held, the replica still answers its clients from a single free pool thread,
        /// and the write is not visible until the replay goes on. Covers the replay tasks fed by the session that
        /// receives the stream, the background replay of a sublog by itself, and the background replay feeding replay
        /// tasks.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("REPLICATION")]
        [TestCase(false, 2)]
        [TestCase(true, 1)]
        [TestCase(true, 2)]
        public void AReplayThatWaitsHoldsNoPoolThread(bool asyncReplay, int replayTaskCount)
        {
            context.CreateInstances(2, enableAOF: true, disableObjects: true, asyncReplay: asyncReplay, replayTaskCount: replayTaskCount);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Connected and used before the pool is capped, so the cap is not measured against connecting
            using var primary = new RawRespSession(endpoint: context.endpoints[0]);
            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            ClassicAssert.AreEqual(Pong, primary.Execute("PING"));
            ClassicAssert.AreEqual("+OK\r\n", replica.Execute("READONLY"));
            ClassicAssert.AreEqual(Nil, replica.Execute("GET", Key));

            ExceptionInjectionHelper.EnableException(Hold);
            try
            {
                using (ThreadPoolCap.ApplyFree(1))
                {
                    primary.Send("SET", Key, "value");
                    ClassicAssert.AreEqual("+OK\r\n", ReadReplyWithin(primary, "SET on the primary"));

                    WaitUntil(() => ExceptionInjectionHelper.WaitingOnClear(Hold) > 0, "the replica to start replaying the write");

                    replica.Send("PING");
                    ClassicAssert.AreEqual(Pong, ReadReplyWithin(replica, "PING on the replica, with one free pool thread, while the replay is held"));

                    replica.Send("GET", Key);
                    ClassicAssert.AreEqual(Nil, ReadReplyWithin(replica, "GET on the replica while the replay is held"));
                }
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(Hold);
            }

            WaitUntil(() => replica.Execute("GET", Key) == ValueReply, "the write to be replayed once released");
        }

        static string ReadReplyWithin(RawRespSession session, string what)
        {
            ClassicAssert.IsTrue(session.WaitForReply(Bound), $"{what}: no reply within {Bound.TotalSeconds:F0}s");
            return session.ReadReply();
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
    }
}
#endif