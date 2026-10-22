// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#if DEBUG
using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test.cluster
{
    /// <summary>
    /// A replica applies the Vector Set adds it replays on tasks of the thread pool, and a record replayed after such an
    /// add waits for it. Neither the session that receives the primary's stream nor a replay task holds a pool thread
    /// while it waits, since the work it waits for runs on the pool too.
    /// </summary>
    [TestFixture]
    [NonParallelizable]
    public class ClusterVectorReplayThreadTests : TestBase
    {
        const ExceptionInjectionType ReplayHold = ExceptionInjectionType.VectorSet_Hold_Replication_Replay;
        const ExceptionInjectionType DropHold = ExceptionInjectionType.VectorSet_Hold_Index_Drop;
        const string VectorKey = "vector";
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
            ExceptionInjectionHelper.DisableException(ReplayHold);
            ExceptionInjectionHelper.DisableException(DropHold);
            context?.TearDown();
        }

        /// <summary>
        /// While a replicated Vector Set add is held, the replica still answers its clients from a single free pool
        /// thread, and a write that follows the add is not visible until the add has been applied. Covers a page that ends
        /// with the add, which waits for it before advertising its offset, and a page with a write after it, which waits
        /// for it before replaying the write.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("REPLICATION")]
        public void AStreamWaitingForAQueuedVectorSetAddHoldsNoPoolThread([Values] bool followedByWrite)
        {
            context.CreateInstances(2, enableAOF: true, replayTaskCount: 1);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            // Connected and used before the pool is capped, so the cap is not measured against connecting
            using var primary = new RawRespSession(endpoint: context.endpoints[0]);
            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            ClassicAssert.AreEqual(":1\r\n", primary.Execute(Add("a")));
            ClassicAssert.AreEqual("+OK\r\n", replica.Execute("READONLY"));
            WaitUntil(() => replica.Execute("VCARD", VectorKey) == ":1\r\n", "the first add to reach the replica");
            ClassicAssert.AreEqual(Nil, replica.Execute("GET", Key));

            ExceptionInjectionHelper.EnableException(ReplayHold);
            try
            {
                using (ThreadPoolCap.ApplyFree(1))
                {
                    if (followedByWrite)
                    {
                        primary.SendPipeline(Add("b"), ["SET", Key, "value"]);
                        ClassicAssert.AreEqual(":1\r\n", ReadReplyWithin(primary, "VADD on the primary"));
                        ClassicAssert.AreEqual("+OK\r\n", ReadReplyWithin(primary, "SET on the primary"));
                    }
                    else
                    {
                        primary.Send(Add("b"));
                        ClassicAssert.AreEqual(":1\r\n", ReadReplyWithin(primary, "VADD on the primary"));
                    }

                    WaitUntil(() => ExceptionInjectionHelper.WaitingOnClear(ReplayHold) > 0, "the replica to queue the add");

                    replica.Send("PING");
                    ClassicAssert.AreEqual(Pong, ReadReplyWithin(replica, "PING on the replica, with one free pool thread, while the add is held"));

                    if (followedByWrite)
                    {
                        replica.Send("GET", Key);
                        ClassicAssert.AreEqual(Nil, ReadReplyWithin(replica, "GET on the replica while the add before the write is held"));
                    }
                }
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(ReplayHold);
            }

            WaitUntil(() => replica.Execute("VCARD", VectorKey) == ":2\r\n", "the add to be applied once released");
            if (followedByWrite)
                WaitUntil(() => replica.Execute("GET", Key) == ValueReply, "the write to be replayed after the add");
        }

        /// <summary>
        /// A replicated Vector Set add that has to recreate an index whose earlier index is still to be dropped waits for
        /// the drop without holding its replay task's pool thread, so the replica still answers its clients from a single
        /// free one, and the add is applied once the drop is done.
        /// </summary>
        [Test, CancelAfter(100_000)]
        [Category("REPLICATION")]
        public void AReplayedAddWaitingForAnIndexDropHoldsNoPoolThread()
        {
            context.CreateInstances(2, enableAOF: true);
            context.CreateConnection();
            _ = context.clusterTestUtils.SimpleSetupCluster(1, 1, logger: context.logger);

            using var primary = new RawRespSession(endpoint: context.endpoints[0]);
            using var replica = new RawRespSession(endpoint: context.endpoints[1]);
            ClassicAssert.AreEqual(":1\r\n", primary.Execute(Add("a")));
            ClassicAssert.AreEqual("+OK\r\n", replica.Execute("READONLY"));
            WaitUntil(() => replica.Execute("VCARD", VectorKey) == ":1\r\n", "the first add to reach the replica");

            // Evicting the index record on the replica requests a drop of its in-memory index, which the hold keeps pending
            var vectorManager = context.nodes[1].Provider.StoreWrapper.DefaultDatabase.VectorManager;
            ExceptionInjectionHelper.EnableException(DropHold);
            try
            {
                StringAssert.StartsWith("+OK", replica.Execute("DEBUG", "FLUSHANDEVICT"));
                WaitUntil(() => ExceptionInjectionHelper.WaitingOnClear(DropHold) > 0, "the index drop task to reach the hold");
                ClassicAssert.IsTrue(vectorManager.DropRequested(Encoding.ASCII.GetBytes(VectorKey)));

                using (ThreadPoolCap.ApplyFree(1))
                {
                    primary.Send(Add("b"));
                    ClassicAssert.AreEqual(":1\r\n", ReadReplyWithin(primary, "VADD on the primary"));

                    // Nothing neutral shows the replay task reaching the drop, so the replica is kept busy for long enough
                    var clock = Stopwatch.StartNew();
                    do
                    {
                        replica.Send("PING");
                        ClassicAssert.AreEqual(Pong, ReadReplyWithin(replica, "PING on the replica, with one free pool thread, while the add waits for the drop"));
                        Thread.Sleep(10);
                    }
                    while (clock.Elapsed < TimeSpan.FromSeconds(1));

                    ClassicAssert.IsTrue(vectorManager.DropRequested(Encoding.ASCII.GetBytes(VectorKey)), "The drop is still held");
                }
            }
            finally
            {
                ExceptionInjectionHelper.DisableException(DropHold);
            }

            WaitUntil(() => replica.Execute("VCARD", VectorKey) == ":2\r\n", "the add to be applied once the drop is done");
        }

        static string[] Add(string element) => ["VADD", VectorKey, "VALUES", "3", "1", "2", "3", element, "NOQUANT"];

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