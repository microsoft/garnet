// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Exercises <c>--wait-for-commit</c>, where no reply may reach the client before the AOF record behind
    /// it is committed. The wait happens at the end of the batch and suspends the session, and replies
    /// produced before it are held back rather than written to the socket.
    /// </summary>
    [TestFixture]
    public class RespAofCommitWaitTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";

        GarnetServer server;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// The commit a reply waits for is periodic, so the wait is long enough to be measured. Far more
        /// sessions wait on it at once than the thread pool has threads, and they all finish within about one
        /// commit interval: waiting on the network thread would admit only as many waits as there are threads
        /// and take one interval per round.
        /// </summary>
        [Test]
        public void MoreSessionsWaitForACommitThanTheThreadPoolHasThreads()
        {
            const int SessionCount = 256;
            const int CappedWorkers = 32;
            const int CommitMs = 1000;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                enableAOF: true, commitWait: true, commitFrequencyMs: CommitMs);
            server.Start();

            // Capped for the duration of the test. Uncapped, the pool injects a thread per blocked wait and a
            // thread-per-wait implementation would pass on a host this size.
            ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);

            var clients = new RawRespSession[SessionCount];
            try
            {
                if (!ThreadPool.SetMinThreads(Math.Min(minWorkers, CappedWorkers), minIo) ||
                    !ThreadPool.SetMaxThreads(CappedWorkers, maxIo))
                    Assert.Ignore("Could not cap the thread pool, so the session count cannot be made to exceed it.");

                for (var i = 0; i < SessionCount; i++)
                    clients[i] = new RawRespSession();

                var sw = Stopwatch.StartNew();
                for (var i = 0; i < SessionCount; i++)
                    clients[i].Send("SET", $"commit-wait-{i}", "v");

                for (var i = 0; i < SessionCount; i++)
                    ClassicAssert.AreEqual(Ok, clients[i].ReadReply(), $"Session {i} did not get its acknowledgement.");
                sw.Stop();

                // One commit covers every session that is waiting when it fires, so parking them all costs a
                // single interval. Holding a thread per wait admits CappedWorkers at a time, which is
                // SessionCount / CappedWorkers rounds of an interval each.
                ClassicAssert.Less(sw.Elapsed.TotalMilliseconds, CommitMs * 4,
                    $"{SessionCount} sessions waiting for a commit took {sw.Elapsed.TotalSeconds:F1}s against a pool " +
                    $"of {CappedWorkers} threads, which indicates the waits were serialized on those threads.");
            }
            finally
            {
                foreach (var c in clients)
                    c?.Dispose();

                _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                _ = ThreadPool.SetMinThreads(minWorkers, minIo);
            }
        }

        /// <summary>
        /// A reply that outgrows the response buffer flushes from inside the command, below any point that
        /// could wait for the commit, so those bytes are held back and written once it lands. They must
        /// arrive whole, in order, and ahead of what the rest of the batch produced after them.
        /// </summary>
        [Test]
        public void AReplyLargerThanTheResponseBufferSurvivesTheCommitWait()
        {
            const int BufferBytes = 16 * 1024;
            const int ValueBytes = 100 * 1024;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                enableAOF: true, commitWait: true, networkBufferSize: "16k");
            server.Start();

            // Distinct bytes throughout, so a chunk written out of order or dropped cannot go unnoticed.
            var value = new StringBuilder(ValueBytes);
            for (var i = 0; i < ValueBytes; i++)
                value.Append((char)('a' + (i % 26)));
            var payload = value.ToString();

            using var client = new RawRespSession(bufferSize: 4 * (ValueBytes + BufferBytes));

            ClassicAssert.AreEqual(Ok, client.Execute("SET", "big", payload));

            // GETDEL appends to the AOF and replies with the old value, so its reply is both commit-gated and
            // far larger than the buffer it is written through. PING follows it in the same batch and must
            // not overtake it.
            client.SendPipeline(["GETDEL", "big"], ["PING"]);

            ClassicAssert.AreEqual($"${payload.Length}\r\n{payload}\r\n", client.ReadReply(), "The large reply did not arrive intact.");
            ClassicAssert.AreEqual(Pong, client.ReadReply(), "The reply after the large one did not arrive, or arrived out of order.");

            ClassicAssert.AreEqual("$-1\r\n", client.Execute("GET", "big"), "GETDEL did not delete the key.");
        }

        /// <summary>
        /// The acknowledgement still implies durability: a key read back after its reply has landed survives
        /// a restart that recovers from the AOF alone.
        /// </summary>
        [Test]
        public void AnAcknowledgedWriteIsStillDurableAcrossRestart()
        {
            const int Keys = 200;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                enableAOF: true, commitWait: true, commitFrequencyMs: 200, failOnRecoveryError: true);
            server.Start();

            using (var client = new RawRespSession())
            {
                for (var i = 0; i < Keys; i++)
                    ClassicAssert.AreEqual(Ok, client.Execute("SET", $"durable-{i}", $"v{i}"));
            }

            // Dropped without a graceful shutdown, so only what the AOF already holds can come back.
            server.Dispose(false);
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                enableAOF: true, commitWait: true, commitFrequencyMs: 200, tryRecover: true, failOnRecoveryError: true);
            server.Start();

            using (var client = new RawRespSession())
            {
                for (var i = 0; i < Keys; i++)
                    ClassicAssert.AreEqual($"$2\r\nv{i}\r\n".Replace("$2", $"${$"v{i}".Length}"),
                        client.Execute("GET", $"durable-{i}"), $"Key durable-{i} was acknowledged but did not survive.");
            }
        }

        /// <summary>
        /// The commit a batch waits for can land between the moment the batch finds it outstanding and the
        /// moment the suspending body awaits it, which runs that body to completion without ever suspending.
        /// The batch still owes its replies a flush, so they must go out on that path too rather than being
        /// dropped with the response object at the end of the batch.
        /// </summary>
        /// <remarks>
        /// The window is a few instructions wide, so it is reached by volume. A commit interval just long
        /// enough to outlast a write makes nearly every batch wait, and short enough that the commit lands
        /// while it is waiting; the sessions then supply the repetitions. A batch that loses its flush leaves
        /// its session waiting for a reply that will never come, which is what the bounded wait detects.
        /// </remarks>
        [Test]
        public void ACommitLandingWhileTheBatchWaitsForItStillFlushesTheBatch()
        {
            const int SessionCount = 32;
            const int WritesPerSession = 4000;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, commitWait: true, commitFrequencyMs: 1);
            server.Start();

            var clients = new RawRespSession[SessionCount];
            var wrongReplies = new ConcurrentQueue<string>();
            var progress = new int[SessionCount];
            try
            {
                for (var i = 0; i < SessionCount; i++)
                    clients[i] = new RawRespSession();

                var writers = new Task[SessionCount];
                for (var i = 0; i < SessionCount; i++)
                {
                    var id = i;
                    writers[i] = Task.Factory.StartNew(() =>
                    {
                        for (var w = 0; w < WritesPerSession; w++)
                        {
                            var reply = clients[id].Execute("SET", $"flush-{id}-{w}", "v");
                            if (reply != Ok)
                                wrongReplies.Enqueue($"Session {id} write {w} replied {reply}.");
                            progress[id] = w + 1;
                        }
                    }, TaskCreationOptions.LongRunning);
                }

                if (!Task.WaitAll(writers, TimeSpan.FromSeconds(30)))
                {
                    var stalled = string.Join(", ", Enumerable.Range(0, SessionCount)
                        .Where(i => progress[i] < WritesPerSession)
                        .Select(i => $"session {i} at {progress[i]}/{WritesPerSession}"));
                    Assert.Fail($"A commit-gated batch never reached the wire: {stalled}.");
                }

                ClassicAssert.IsEmpty(wrongReplies, string.Join(" ", wrongReplies));
            }
            finally
            {
                foreach (var c in clients)
                    c?.AbortAndDispose();
            }
        }

        /// <summary>
        /// A command that faults without ending the session must not release the batch's acknowledgements:
        /// the writes behind them are still waiting for their commit, and the session carries on afterwards,
        /// so the client would keep using a connection that had told it an uncommitted write was durable.
        /// </summary>
        /// <remarks>
        /// The first write lands on a commit boundary, which puts a full interval between the batch below
        /// and the next commit, so the window the batch is asked to stay silent for is not a race against
        /// one that was already about to fire.
        /// </remarks>
        [Test]
        public void AFaultThatKeepsTheSessionDoesNotReleaseUncommittedAcknowledgements()
        {
            const int CommitMs = 4000;

            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                enableAOF: true, commitWait: true, commitFrequencyMs: CommitMs, disableObjects: true);
            server.Start();

            using var client = new RawRespSession();

            ClassicAssert.AreEqual(Ok, client.Execute("SET", "fault-commit-sync", "v"));

            // LPUSH faults with the object store disabled, which is one of the two faults that leave the
            // session usable, and it lands in the same batch as a write that is owed a commit.
            client.SendPipeline(["SET", "fault-commit", "v"], ["LPUSH", "fault-list", "x"]);

            ClassicAssert.IsFalse(client.WaitForReply(TimeSpan.FromMilliseconds(CommitMs / 2)),
                "The batch was answered before its commit, so the SET was acknowledged as durable while it was not.");

            // The session is still usable, so the whole batch is answered once the commit lands.
            ClassicAssert.AreEqual(Ok, client.ReadReply(), "The acknowledged write lost its reply.");
            ClassicAssert.IsTrue(client.ReadReply().StartsWith("-ERR", StringComparison.Ordinal),
                "The faulting command lost its error.");
            ClassicAssert.AreEqual(Pong, client.Execute("PING"), "The session did not survive the fault.");
        }

        /// <summary>
        /// A push belongs to no batch and carries no AOF-dependent reply, so the hold-back does not apply to
        /// it. A session whose batch parked on an unbounded wait hands the response object back with its
        /// commit still outstanding, which is when another session can write a push into it. Holding that
        /// push's chunks would put its last chunk -- the one the publisher writes out itself -- on the wire
        /// ahead of the rest of the message.
        /// </summary>
        [Test]
        public void APushWrittenWhileARepliesIsHeldArrivesWhole()
        {
            const int CommitMs = 4000;
            const int PayloadBytes = 128 * 1024;
            const int ClientBufferBytes = 1024 * 1024;

            // A buffer the payload cannot fit in, so writing the push has to flush part of it.
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir,
                enableAOF: true, commitWait: true, commitFrequencyMs: CommitMs, networkBufferSize: "16k");
            server.Start();

            using var subscriber = new RawRespSession(bufferSize: ClientBufferBytes);
            using var publisher = new RawRespSession(bufferSize: ClientBufferBytes);

            // RESP3, so one session can both subscribe and write: RESP2 subscriber mode rejects SET.
            StringAssert.StartsWith("%", subscriber.Execute("HELLO", "3"));
            StringAssert.Contains("subscribe", subscriber.Execute("SUBSCRIBE", "ch"));

            // The SET leaves a commit outstanding and DEBUG BLOCK parks the batch, so the response object
            // goes back while the SET's reply is still held.
            subscriber.SendPipeline(["SET", "commit-push", "v"], ["DEBUG", "BLOCK", "1"]);
            Thread.Sleep(200);

            var payload = new string('x', PayloadBytes);
            ClassicAssert.AreEqual(":1\r\n", publisher.Execute("PUBLISH", "ch", payload),
                "The parked session was not subscribed.");

            ClassicAssert.AreEqual($">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n${PayloadBytes}\r\n{payload}\r\n",
                subscriber.ReadReply(), "The push did not arrive whole and in order.");

            // The held replies follow once the commit lands.
            ClassicAssert.AreEqual(Ok, subscriber.ReadReply(), "The acknowledged write lost its reply.");
            ClassicAssert.AreEqual(Ok, subscriber.ReadReply(), "The parked command lost its reply.");
        }
    }
}