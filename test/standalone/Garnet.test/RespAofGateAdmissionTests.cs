// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.networking;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Garnet.test
{
    /// <summary>
    /// Exercises how a session waits for the AOF to admit appends. A write that arrives while the AOF is further
    /// ahead of its replicas than the lag budget allows parks its session, where stalling in the append would hold
    /// a thread for as long as the replicas take to catch up.
    /// </summary>
    /// <remarks>
    /// The gate is closed and opened by hand. A one byte budget with a watermark of zero closes it as soon as
    /// anything has been appended, and a watermark at the largest address opens it again, which is what the shipper
    /// publishes for a replica that has caught up.
    /// </remarks>
    [TestFixture]
    public class RespAofGateAdmissionTests : TestBase
    {
        const string Ok = "+OK\r\n";
        const string Pong = "+PONG\r\n";
        const string Nil = "$-1\r\n";

        static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

        // Long enough for a reply that was going to arrive to have done so, short enough to repeat per command
        static readonly TimeSpan Quiet = TimeSpan.FromMilliseconds(200);

        GarnetServer server;

        AofBackpressure Gate => server.Provider.StoreWrapper.appendOnlyFile.backpressure;

        [SetUp]
        public void Setup() => TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

        [TearDown]
        public void TearDown()
        {
            // A failed test can leave the gate closed, and a write stalled in the append would then hold up the
            // server's disposal for as long as the gate stays closed
            server?.Provider?.StoreWrapper?.appendOnlyFile?.backpressure?.PublishShippedAddress(0, long.MaxValue);
            server?.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        void StartServer(bool commitWait = false)
        {
            server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableAOF: true, commitWait: commitWait, enableLua: true);

            _ = server.Register.NewCommand("SETIFPM", CommandType.ReadModifyWrite, new SetIfPMCustomCommand());
            _ = server.Register.NewCommand("RDVALUE", CommandType.Read, new ReadValueFunctions());
            var factory = new MyDictFactory();
            _ = server.Register.NewCommand("MYDICTGET", CommandType.Read, factory, new MyDictGet());
            _ = server.Register.NewCommand("MYDICTSET", CommandType.ReadModifyWrite, factory, new MyDictSet());

            server.Start();
        }

        /// <summary>Appends a record, so the log has a tail the gate can measure the lag against.</summary>
        static void Prime()
        {
            using var client = new RawRespSession { ReceiveTimeout = Bound };
            ClassicAssert.AreEqual(Ok, client.Execute("SET", "prime", "v"));
        }

        void CloseGate()
        {
            Gate.SetBudget(1);
            Gate.PublishShippedAddress(0, 0);
            ClassicAssert.IsTrue(Gate.AdmissionClosed(), "The gate did not close, so nothing would be held back");
        }

        void OpenGate() => Gate.PublishShippedAddress(0, long.MaxValue);

        static void WaitUntil(Func<bool> condition, string what)
        {
            if (!SpinWait.SpinUntil(condition, Bound))
                Assert.Fail($"Timed out waiting until {what}.");
        }

        /// <summary>
        /// Far more sessions wait for the gate than the thread pool has threads, and the pool still serves everyone
        /// else. A write that stalled in the append would hold a thread for as long as the gate stayed closed, and
        /// the pool would have no thread left after as many writes as it has.
        /// </summary>
        [Test]
        public void MoreWritesWaitForTheGateThanTheThreadPoolHasThreads()
        {
            const int SessionCount = 128;
            const int CappedWorkers = 32;

            StartServer();
            Prime();
            CloseGate();

            // Capped for the duration of the test. Uncapped, the pool injects a thread per blocked write and a
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
                    clients[i] = new RawRespSession(bufferSize: 4096) { ReceiveTimeout = Bound };

                for (var i = 0; i < SessionCount; i++)
                    clients[i].Send("SET", $"gate-{i}", "v");

                WaitUntil(() => Gate.WaiterCount == SessionCount, $"all {SessionCount} writes were waiting for the gate");

                // A session of its own is served, and none of the writes has been applied
                using var other = new RawRespSession { ReceiveTimeout = Bound };
                ClassicAssert.AreEqual(Pong, other.Execute("PING"));
                ClassicAssert.AreEqual(Nil, other.Execute("GET", "gate-0"));

                OpenGate();
                for (var i = 0; i < SessionCount; i++)
                    ClassicAssert.AreEqual(Ok, clients[i].ReadReply(), $"Session {i}");

                ClassicAssert.AreEqual(0, Gate.WaiterCount);
                ClassicAssert.AreEqual("$1\r\nv\r\n", other.Execute("GET", $"gate-{SessionCount - 1}"));
            }
            finally
            {
                // The pool is restored before the gate opens: a write that was not parked is holding a thread of it
                _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                _ = ThreadPool.SetMinThreads(minWorkers, minIo);
                OpenGate();

                foreach (var client in clients)
                    client?.Dispose();
            }
        }

        /// <summary>
        /// A write is admitted by the append that closes the gate, and the next write of the same batch waits. What
        /// came before it in the batch is answered as it would be if the wait were a slow write, it is not run again
        /// when the wait is over, and what came after the write that waited is not run until it has been.
        /// </summary>
        /// <param name="commitWait">
        /// Whether replies wait for the commit of their record, in which case the replies of a batch are held until
        /// the batch is finished.
        /// </param>
        [TestCase(false)]
        [TestCase(true)]
        public void ABatchIsAnsweredInOrderAcrossAWriteThatHasToWait(bool commitWait)
        {
            StartServer(commitWait);
            Prime();

            // Everything appended so far counts as shipped, so the first write of the batch is admitted and its own
            // record closes the gate for the second
            Gate.SetBudget(1);
            Gate.PublishShippedAddress(0, server.Provider.StoreWrapper.appendOnlyFile.Log.GetTailAddress(0));
            ClassicAssert.IsFalse(Gate.AdmissionClosed(), "The gate was closed before the batch was sent");

            using var client = new RawRespSession { ReceiveTimeout = Bound };
            client.SendPipeline(["INCR", "ordered"], ["INCR", "ordered"], ["GET", "ordered"]);

            WaitUntil(() => Gate.WaiterCount == 1, "the second write was waiting for the gate");

            if (commitWait)
            {
                ClassicAssert.IsFalse(client.WaitForReply(Quiet), "A reply left the server before the batch was finished");
            }
            else
            {
                ClassicAssert.AreEqual(":1\r\n", client.ReadReply(), "The first write was not answered while the second waited");
                ClassicAssert.IsFalse(client.WaitForReply(Quiet), "The write that had to wait was answered");
            }

            ClassicAssert.AreEqual("$1\r\n1\r\n", GetOnAnotherSession("ordered"), "The write that had to wait was applied");

            OpenGate();
            if (commitWait)
                ClassicAssert.AreEqual(":1\r\n", client.ReadReply());
            ClassicAssert.AreEqual(":2\r\n", client.ReadReply(), "The write that waited did not run exactly once");
            ClassicAssert.AreEqual("$1\r\n2\r\n", client.ReadReply(), "The read ran before the write ahead of it");
        }

        static string GetOnAnotherSession(string key)
        {
            using var client = new RawRespSession { ReceiveTimeout = Bound };
            return client.Execute("GET", key);
        }

        /// <summary>
        /// Turning the limit off opens the gate without anything being published. Sessions waiting for the gate
        /// learn that by looking at it, which is also how they learn of a log whose tail has moved back.
        /// </summary>
        [Test]
        public void TurningTheLimitOffReleasesTheWritesThatWaitForIt()
        {
            StartServer();
            Prime();
            CloseGate();

            using var first = new RawRespSession { ReceiveTimeout = Bound };
            using var second = new RawRespSession { ReceiveTimeout = Bound };
            first.Send("SET", "released-1", "v");
            second.Send("SET", "released-2", "v");
            WaitUntil(() => Gate.WaiterCount == 2, "both writes were waiting for the gate");

            using var admin = new RawRespSession { ReceiveTimeout = Bound };
            ClassicAssert.AreEqual(Ok, admin.Execute("CONFIG", "SET", "aof-sync-max-lag-bytes", "-1"));

            ClassicAssert.AreEqual(Ok, first.ReadReply());
            ClassicAssert.AreEqual(Ok, second.ReadReply());
            ClassicAssert.AreEqual(0, Gate.WaiterCount);
        }

        /// <summary>
        /// Only a command that may append waits. Reads, administration and everything else are served while the
        /// gate is closed, and so is a custom command that declares itself a read. A script, a transaction
        /// procedure and a custom command that declares itself a write wait, because they may append.
        /// </summary>
        [Test]
        public void OnlyTheCommandsThatMayAppendWaitForTheGate()
        {
            StartServer();

            // Written while the gate is open, for the reads below to find
            using (var setup = new RawRespSession { ReceiveTimeout = Bound })
            {
                ClassicAssert.AreEqual(Ok, setup.Execute("SET", "existing", "value"));
                ClassicAssert.AreEqual(":1\r\n", setup.Execute("HSET", "hash", "field", "value"));
                ClassicAssert.AreEqual(":1\r\n", setup.Execute("ZADD", "zset", "1", "member"));
                ClassicAssert.AreEqual("+OK\r\n", setup.Execute("MYDICTSET", "dict", "field", "value"));
            }

            (string[] Command, bool Waits)[] rows =
            [
                (["SET", "key", "value"], true),
                (["INCR", "counter"], true),
                (["HSET", "hash", "field2", "value"], true),
                (["LPUSH", "list", "item"], true),
                (["ZADD", "zset", "2", "member2"], true),
                (["DEL", "existing"], true),
                (["EXPIRE", "hash", "100"], true),
                (["GETDEL", "key"], true),
                (["EVAL", "return 1", "0"], true),
                (["EVALSHA", new string('0', 40), "0"], true),
                (["SETIFPM", "key", "value", "val"], true),
                (["MYDICTSET", "dict", "field2", "value"], true),

                (["GET", "key"], false),
                (["HGET", "hash", "field"], false),
                (["ZSCORE", "zset", "member"], false),
                (["PING"], false),
                (["DBSIZE"], false),
                (["CONFIG", "GET", "maxmemory"], false),
                (["EXEC"], false),
                (["MULTI"], false),
                (["RDVALUE", "key"], false),
                (["MYDICTGET", "dict", "field"], false),
            ];

            var failures = new List<string>();
            foreach (var (command, waits) in rows)
            {
                var name = string.Join(' ', command);
                CloseGate();

                using var client = new RawRespSession { ReceiveTimeout = Bound };
                client.Send(command);

                // Either the command is waiting or its answer is on the way, so a command that does not wait is
                // not waited on for the whole bound
                _ = SpinWait.SpinUntil(() => Gate.WaiterCount > 0 || client.WaitForReply(TimeSpan.Zero), Bound);
                var waiting = Gate.WaiterCount == 1;

                if (waits)
                {
                    if (!waiting)
                        failures.Add($"{name}: was not made to wait");
                    else if (client.WaitForReply(Quiet))
                        failures.Add($"{name}: was answered while the gate was closed");
                }
                else if (waiting)
                {
                    failures.Add($"{name}: was made to wait");
                }

                OpenGate();
                try
                {
                    _ = client.ReadReply();
                }
                catch (Exception ex) when (ex is System.IO.IOException or System.Net.Sockets.SocketException)
                {
                    failures.Add($"{name}: was not answered once the gate opened ({ex.GetType().Name})");
                }

                if (Gate.WaiterCount != 0)
                    failures.Add($"{name}: left a waiter registered after being answered");
            }

            ClassicAssert.IsEmpty(failures, string.Join(Environment.NewLine, failures));
        }

        /// <summary>
        /// A transaction waits when it is executed, not when it is queued: what is queued has not run, and what is
        /// run may append.
        /// </summary>
        [Test]
        public void ATransactionWaitsForTheGateWhenItIsExecuted()
        {
            StartServer();
            Prime();
            CloseGate();

            using var client = new RawRespSession { ReceiveTimeout = Bound };
            ClassicAssert.AreEqual(Ok, client.Execute("MULTI"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("SET", "queued", "v"));
            ClassicAssert.AreEqual(0, Gate.WaiterCount, "A command was made to wait as it was queued");

            client.Send("EXEC");
            WaitUntil(() => Gate.WaiterCount == 1, "the transaction was waiting for the gate");
            ClassicAssert.IsFalse(client.WaitForReply(Quiet), "The transaction ran while the gate was closed");
            ClassicAssert.AreEqual(Nil, GetOnAnotherSession("queued"));

            OpenGate();
            ClassicAssert.AreEqual("*1\r\n+OK\r\n", client.ReadReply());
            ClassicAssert.AreEqual("$1\r\nv\r\n", client.Execute("GET", "queued"));
        }

        /// <summary>
        /// Every write runs once and in the order it was sent, while the gate keeps closing and opening under
        /// sessions that are mid-batch. A write that waited and was run again, or skipped, or run ahead of one before
        /// it, shows as a counter that does not count.
        /// </summary>
        [Test]
        public void EveryWriteRunsOnceAndInOrderWhileTheGateKeepsClosingAndOpening()
        {
            const int Sessions = 16;
            const int Rounds = 100;
            const int WritesPerRound = 5;

            StartServer();
            Prime();
            Gate.SetBudget(1);
            OpenGate();

            using var stop = new CancellationTokenSource();
            var waitedAtLeastOnce = 0;
            var flapper = Task.Factory.StartNew(() =>
            {
                // Phases of unequal length, so that the gate is not closed every time the poller looks at it
                var random = new Random(0);
                while (!stop.IsCancellationRequested)
                {
                    Gate.PublishShippedAddress(0, 0);
                    Thread.Sleep(random.Next(1, 4));
                    if (Gate.WaiterCount > 0)
                        Volatile.Write(ref waitedAtLeastOnce, 1);
                    OpenGate();
                    Thread.Sleep(random.Next(1, 4));
                }
            }, TaskCreationOptions.LongRunning);

            var writers = new Task[Sessions];
            for (var s = 0; s < Sessions; s++)
            {
                var key = $"counter-{s}";
                writers[s] = Task.Factory.StartNew(() =>
                {
                    using var client = new RawRespSession(bufferSize: 16 * 1024) { ReceiveTimeout = Bound };
                    var batch = new string[WritesPerRound][];
                    for (var i = 0; i < batch.Length; i++)
                        batch[i] = ["INCR", key];

                    var expected = 0;
                    for (var round = 0; round < Rounds; round++)
                    {
                        client.SendPipeline(batch);
                        for (var i = 0; i < WritesPerRound; i++)
                            ClassicAssert.AreEqual($":{++expected}\r\n", client.ReadReply(), $"{key}, write {expected}");
                    }
                }, TaskCreationOptions.LongRunning);
            }

            try
            {
                ClassicAssert.IsTrue(Task.WaitAll(writers, TimeSpan.FromSeconds(60)), "The writes did not finish");
            }
            finally
            {
                stop.Cancel();
                ClassicAssert.IsTrue(flapper.Wait(Bound));
                OpenGate();
            }

            ClassicAssert.AreEqual(1, Volatile.Read(ref waitedAtLeastOnce), "No write ever waited, so nothing was tested");

            var total = Rounds * WritesPerRound;
            for (var s = 0; s < Sessions; s++)
                ClassicAssert.AreEqual($"${total.ToString().Length}\r\n{total}\r\n", GetOnAnotherSession($"counter-{s}"));
        }

        /// <summary>
        /// A session that is torn down while its write waits stops waiting. The gate is closed and nothing else
        /// would end the wait, and the write would stay parked on the receive buffer it was parsed from.
        /// </summary>
        /// <remarks>
        /// The session is driven directly rather than through a connection. A parked session has no receive posted,
        /// so closing its connection is not noticed until the wait ends.
        /// </remarks>
        [Test]
        public async Task ASessionTornDownWhileItsWriteWaitsForTheGateStopsWaiting()
        {
            StartServer();
            Prime();
            CloseGate();

            var request = "*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$1\r\nv\r\n"u8.ToArray();
            var input = GC.AllocateArray<byte>(request.Length, pinned: true);
            request.CopyTo(input, 0);

            var sender = new ScratchBufferNetworkSender();
            var session = (RespServerSession)server.Provider.GetSession(WireFormat.ASCII, sender);
            ValueTask<int> consume;
            try
            {
                // Off the test thread, so that a write that stalls in the append fails the test rather than hanging it
                var started = Task.Run(() => Consume(session, input));
                if (!started.Wait(Bound))
                {
                    OpenGate();
                    Assert.Fail("The receive did not return while the gate was closed, so the write stalled in the append");
                }

                consume = started.Result;

                ClassicAssert.IsFalse(consume.IsCompleted, "A write was not held while the gate was closed");
                ClassicAssert.AreEqual(1, Gate.WaiterCount);
                ClassicAssert.AreEqual(0, sender.GetResponse().Length, "A write was answered while the gate was closed");
            }
            finally
            {
                session.Dispose();
            }

            // Whether the teardown or the wake it causes reaches the session first decides whether the receive
            // completes or faults, and either ends the wait
            var consumed = consume.AsTask();
            var finished = await Task.WhenAny(consumed, Task.Delay(Bound));
            ClassicAssert.AreSame(consumed, finished, "A session that was torn down kept waiting for the gate");
        }

        /// <summary>
        /// A wait created after the session was torn down is already cancelled. Teardown cancels the wait the
        /// session has, and a consume that was running while it did can still create one afterwards, which nothing
        /// else would then end.
        /// </summary>
        [Test]
        public void AWaitCreatedAfterTheSessionWasTornDownIsAlreadyCancelled()
        {
            StartServer();

            var session = (RespServerSession)server.Provider.GetSession(WireFormat.ASCII, new ScratchBufferNetworkSender());
            session.Dispose();

            var wait = session.AofGateWait;
            wait.Arm();
            var pending = wait.WaitAsync();

            ClassicAssert.IsTrue(pending.IsCompleted, "A wait created after teardown was left waiting");
            _ = Assert.Throws<OperationCanceledException>(() => pending.GetAwaiter().GetResult());
        }

        /// <summary>
        /// Shutting the server down releases the writes that are waiting for the gate rather than waiting for the
        /// replicas to catch up.
        /// </summary>
        [Test]
        public void ShuttingTheServerDownWhileWritesWaitForTheGateDoesNotHang()
        {
            const int SessionCount = 3;

            StartServer();
            Prime();
            CloseGate();

            var clients = new RawRespSession[SessionCount];
            try
            {
                for (var i = 0; i < SessionCount; i++)
                {
                    clients[i] = new RawRespSession { ReceiveTimeout = Bound };
                    clients[i].Send("SET", $"shutdown-{i}", "v");
                }

                WaitUntil(() => Gate.WaiterCount == SessionCount, "the writes were waiting for the gate");

                // Not disposed again by the teardown, which would wait just as long
                var stopping = server;
                server = null;
                var dispose = Task.Run(stopping.Dispose);
                ClassicAssert.IsTrue(dispose.Wait(Bound), "The server did not shut down while writes were waiting for the gate");
            }
            finally
            {
                foreach (var client in clients)
                    client?.Dispose();
            }
        }

        static unsafe ValueTask<int> Consume(RespServerSession session, byte[] pinnedInput)
            => session.TryConsumeMessagesAsync((byte*)Unsafe.AsPointer(ref pinnedInput[0]), pinnedInput.Length);

        /// <summary>A custom command that declares itself a read, which reads the value it is given the key of.</summary>
        sealed class ReadValueFunctions : CustomRawStringFunctions
        {
            public override bool Reader(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> value, ref RespMemoryWriter writer, ref ReadInfo readInfo)
            {
                writer.WriteBulkString(value);
                return true;
            }

            public override int GetInitialLength(ref StringInput input) => throw new NotSupportedException();

            public override int GetLength(ReadOnlySpan<byte> value, ref StringInput input) => throw new NotSupportedException();

            public override bool InitialUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) => throw new NotSupportedException();

            public override bool InPlaceUpdater(ReadOnlySpan<byte> key, ref StringInput input, Span<byte> value, ref int valueLength, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) => throw new NotSupportedException();

            public override bool CopyUpdater(ReadOnlySpan<byte> key, ref StringInput input, ReadOnlySpan<byte> oldValue, Span<byte> newValue, ref RespMemoryWriter writer, ref RMWInfo rmwInfo) => throw new NotSupportedException();
        }
    }
}