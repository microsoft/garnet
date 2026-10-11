// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
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
    }
}