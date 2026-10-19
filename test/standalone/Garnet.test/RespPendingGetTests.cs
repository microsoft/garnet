// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Exercises <c>GET</c> when the record has fallen out of memory and the read has to go to the device.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The read is issued without waiting on the device. If it goes pending the session suspends: the batch
    /// unwinds, the thread returns to serving other sessions, and the reply is written when the I/O lands.
    /// The alternative - completing the pending read in place - holds a thread-pool thread for the whole
    /// device round trip, so a working set that does not fit in memory consumes one thread per outstanding
    /// read and the server stops accepting work long before the device is saturated.
    /// </para>
    /// <para>
    /// Reads are made slow and deterministic by <see cref="DelayedReadDeviceFactoryCreator"/> rather than by
    /// relying on real device timing, so the tests measure the shape of the concurrency rather than the speed
    /// of the disk.
    /// </para>
    /// </remarks>
    [TestFixture, NonParallelizable]
    public class RespPendingGetTests : TestBase
    {
        /// <summary>Keys written to push the earliest of them well below the log head address.</summary>
        const int PopulatedKeys = 8192;

        /// <summary>Value length, kept short so one record is one device read.</summary>
        const int ValueLength = 48;

        /// <summary>
        /// Log memory of 64 pages, rather than the low-memory default of 4. The pages in memory are then flushed well past
        /// the head, so an allocation need not wait for a flush, while the populated keys still spill to the device.
        /// </summary>
        const string SpilledLogMemorySize = "256k";

        GarnetServer server;
        DelayedReadDeviceFactoryCreator deviceFactoryCreator;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            StartServer();
        }

        /// <summary>
        /// Starts the server, processing client input on the socket event threads if <paramref name="inlineCompletions"/>
        /// is set, with <paramref name="memorySize"/> of log memory if set.
        /// </summary>
        void StartServer(bool inlineCompletions = false, string memorySize = null)
        {
            if (inlineCompletions && !SocketCompletions.InlineCompletionsSupported)
                Assert.Ignore("Sockets cannot run their completions inline on this platform.");

            // Built through GetGarnetServerOptions rather than CreateGarnetServer because only the options
            // object exposes DeviceFactoryCreator, which is how the read delay is injected.
            var options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                enableLua: true,
                lowMemory: true,
                memorySize: memorySize);

            deviceFactoryCreator = new DelayedReadDeviceFactoryCreator();
            options.DeviceFactoryCreator = deviceFactoryCreator;
            options.NetworkInlineCompletions = inlineCompletions;

            server = new GarnetServer(options);
            server.Start();
        }

        /// <summary>Replaces the server started by <see cref="Setup"/> with one started as <see cref="StartServer"/> does.</summary>
        void RestartServer(bool inlineCompletions = false, string memorySize = null)
        {
            // Cleared as they are disposed, so a restart that StartServer ignores leaves TearDown nothing to dispose twice.
            server.Dispose();
            server = null;
            deviceFactoryCreator.Dispose();
            deviceFactoryCreator = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            StartServer(inlineCompletions, memorySize);
        }

        /// <summary>Commands the server has moved to the thread pool because they would wait on a thread that must not.</summary>
        long Offloads => Interlocked.Read(ref server.Provider.StoreWrapper.offloads);

        /// <summary>
        /// Waits out each of <paramref name="clients"/>' last reply, so the server has a receive outstanding for its
        /// connection and, on a server that runs completions inline, its next request reaches the server through a socket
        /// event thread. A batch that parked, or the first on a connection, can leave the receive loop on a thread-pool thread
        /// until the receive after it is outstanding.
        /// </summary>
        static void Settle(params RawClient[] clients)
        {
            foreach (var client in clients)
                ClassicAssert.AreEqual("+PONG\r\n", client.Execute("PING"));
            Thread.Sleep(50);
        }

        /// <summary>
        /// Waits for the log's flushes to pass its head, so a command that adds a record does not wait for one. Flushes
        /// complete asynchronously, so the log can be waiting on one for a moment after writes that turned its tail.
        /// </summary>
        void WaitUntilAllocationNeedNotWaitForFlush(string what)
        {
            var log = server.Provider.StoreWrapper.store.Log;
            var clock = Stopwatch.StartNew();
            while (log.AllocationMayWaitForFlush)
            {
                ClassicAssert.Less(clock.Elapsed, TimeSpan.FromSeconds(10), what);
                Thread.Sleep(1);
            }
        }

        [TearDown]
        public void TearDown()
        {
            if (deviceFactoryCreator is not null)
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.ReleaseWrites();
            }

            server?.Dispose();
            server = null;
            deviceFactoryCreator?.Dispose();
            deviceFactoryCreator = null;

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.OnTearDown();
        }

        static string Key(int i) => $"pendingget:{i:D6}";

        static string Value(int i) => new((char)('a' + (i % 26)), ValueLength);

        /// <summary>
        /// Writes enough records that the earliest ones are no longer in memory, so reading them back has to
        /// go to the device.
        /// </summary>
        void Populate()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var batch = db.CreateBatch();
            Task last = null;
            for (var i = 0; i < PopulatedKeys; i++)
                last = batch.StringSetAsync(Key(i), Value(i));
            batch.Execute();

            // Replies come back in order, so the last one landing means they all did.
            last.Wait(TimeSpan.FromSeconds(60));

            // Only reads issued from here on belong to the test.
            deviceFactoryCreator.ResetReadCount();
        }

        [Test]
        public void GetReadsBackAValueThatIsNoLongerInMemory()
        {
            Populate();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            for (var i = 0; i < 64; i++)
                ClassicAssert.AreEqual(Value(i), db.StringGet(Key(i)).ToString(), $"Wrong value for {Key(i)}");

            ClassicAssert.Greater(deviceFactoryCreator.ReadCount, 0,
                "No device read was issued, so these keys were still in memory and the test proved nothing.");
        }

        /// <summary>
        /// A value too large for one response buffer has to be written out in chunks after the resume, from
        /// the pooled buffer the completion landed in rather than from the network buffer the read was issued
        /// against.
        /// </summary>
        [Test]
        public void GetReadsBackALargeValueThatIsNoLongerInMemory()
        {
            const string LargeKey = "pendingget:large";
            var largeValue = new string('x', 192 * 1024);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.IsTrue(db.StringSet(LargeKey, largeValue));
            }

            Populate();

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.AreEqual(largeValue, db.StringGet(LargeKey).ToString());
            }

            ClassicAssert.Greater(deviceFactoryCreator.ReadCount, 0,
                "No device read was issued, so the large key was still in memory and the test proved nothing.");
        }

        [Test]
        public void GetOfAMissingKeyThatHashesToTheDeviceReturnsNull()
        {
            Populate();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            for (var i = 0; i < 16; i++)
                ClassicAssert.IsTrue(db.StringGet($"pendingget:absent:{i}").IsNull);
        }

        /// <summary>
        /// A read on a key whose value is a collection must report the type error, not the record. The read
        /// is issued through the pending-capable path now, which reports a type mismatch differently from the
        /// path it replaced.
        /// </summary>
        [Test]
        public void GetOfAnObjectKeyThatIsInMemoryReportsWrongType()
        {
            const string HashKey = "pendingget:hash";

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                _ = db.HashSet(HashKey, "field", "value");
            }

            using var client = new RawClient();
            var reply = client.Execute("GET", HashKey);
            ClassicAssert.IsTrue(reply.StartsWith("-WRONGTYPE", StringComparison.Ordinal),
                $"Expected a WRONGTYPE error but got '{reply}'");
        }

        /// <summary>
        /// A read on a key whose value is a collection must report the type error, not the record, even when
        /// the answer comes back from the device.
        /// </summary>
        [Test]
        public void GetOfAnObjectKeyThatIsNoLongerInMemoryReportsWrongType()
        {
            const string HashKey = "pendingget:hash";

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                _ = db.HashSet(HashKey, "field", "value");
            }

            Populate();

            using var client = new RawClient();
            var reply = client.Execute("GET", HashKey);
            ClassicAssert.IsTrue(reply.StartsWith("-WRONGTYPE", StringComparison.Ordinal),
                $"Expected a WRONGTYPE error but got '{reply}'");
        }

        /// <summary>
        /// A transaction parks for a device read exactly as an ordinary command does, and the queued
        /// commands replayed around the suspension still produce the right replies in the right order. Two
        /// consecutive GETs, so the run takes the scatter-gather path and exercises its suspension rather
        /// than the single-read one.
        /// </summary>
        [Test]
        public void GetInsideATransactionReadsBackAValueThatIsNoLongerInMemory()
        {
            Populate();

            using var client = new RawClient();
            ClassicAssert.AreEqual("+OK\r\n", client.Execute("MULTI"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("GET", Key(0)));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("GET", Key(1)));

            var expected = $"*2\r\n${ValueLength}\r\n{Value(0)}\r\n${ValueLength}\r\n{Value(1)}\r\n";
            ClassicAssert.AreEqual(expected, client.Execute("EXEC"));
        }

        /// <summary>
        /// A run of consecutive reads in one pipelined batch. The run takes the scatter-gather path, which
        /// submits every read and suspends once on their shared completion, and each reply still lands in
        /// order - the first out of the response buffer it was read into, the rest out of scratch slots.
        /// </summary>
        [Test]
        public void PipelinedGetsThatGoToTheDeviceReplyInOrder()
        {
            Populate();

            using var client = new RawClient();

            const int Count = 16;
            var commands = new string[Count][];
            for (var i = 0; i < Count; i++)
                commands[i] = ["GET", Key(i)];

            client.SendPipeline(commands);

            for (var i = 0; i < Count; i++)
                ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(i)}\r\n", client.ReadReply(), $"Reply {i} out of order");
        }

        /// <summary>
        /// A read is issued against the response buffer at the position the writer had reached, and the
        /// suspension keeps that buffer so the completion writes through exactly that position. A reply
        /// written earlier in the same batch moves the batch's start and the issue position apart, so a
        /// resume that mistook one for the other shows up here as a wrong reply.
        /// </summary>
        [Test]
        public void GetThatGoesToTheDeviceAfterEarlierOutputInTheSameBatchRepliesCorrectly()
        {
            Populate();

            using var client = new RawClient();

            for (var i = 0; i < 8; i++)
            {
                client.SendPipeline(["PING"], ["GET", Key(i)]);
                ClassicAssert.AreEqual("+PONG\r\n", client.ReadReply());
                ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(i)}\r\n", client.ReadReply(), $"Wrong value for {Key(i)}");
            }
        }

        /// <summary>
        /// The same separation for a value too large for one response buffer, which is written out in chunks
        /// after the resume rather than left in place.
        /// </summary>
        [Test]
        public void LargeGetThatGoesToTheDeviceAfterEarlierOutputInTheSameBatchRepliesCorrectly()
        {
            const string LargeKey = "pendingget:large";
            var largeValue = new string('x', 192 * 1024);

            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
            {
                var db = redis.GetDatabase(0);
                ClassicAssert.IsTrue(db.StringSet(LargeKey, largeValue));
            }

            Populate();

            using var client = new RawClient();
            client.SendPipeline(["PING"], ["GET", LargeKey]);
            ClassicAssert.AreEqual("+PONG\r\n", client.ReadReply());
            ClassicAssert.AreEqual($"${largeValue.Length}\r\n{largeValue}\r\n", client.ReadReply());
        }

        /// <summary>
        /// A reply written before a command suspends stays in the response buffer until the batch finishes,
        /// rather than being flushed on the way out.
        /// </summary>
        /// <remarks>
        /// Sending is what hands the response object to the socket and drops the session's claim on it, so a
        /// suspension that flushed would be giving away the buffer its own pending read is still writing
        /// into. Holding the reply back is also what the synchronous path does: a read completed in place
        /// writes the whole batch out once, after the device returns.
        /// </remarks>
        [Test]
        public void ReplyWrittenBeforeASuspensionIsNotFlushedUntilTheBatchFinishes()
        {
            const int ReadDelayMs = 1000;

            Populate();

            using var client = new RawClient();

            // Warms the connection so the measurement below does not include first-reply setup.
            ClassicAssert.AreEqual("+PONG\r\n", client.Execute("PING"));

            deviceFactoryCreator.ReadDelayMs = ReadDelayMs;
            try
            {
                var sw = Stopwatch.StartNew();
                client.SendPipeline(["PING"], ["GET", Key(0)]);

                var pong = client.ReadReply();
                var elapsedToPong = sw.Elapsed;

                ClassicAssert.GreaterOrEqual(elapsedToPong.TotalMilliseconds, ReadDelayMs * 0.8,
                    $"The reply written before the suspension arrived after {elapsedToPong.TotalMilliseconds:F0}ms, " +
                    $"well inside the {ReadDelayMs}ms device read that followed it, so the suspension flushed the " +
                    "response buffer it was still writing into.");

                ClassicAssert.AreEqual("+PONG\r\n", pong);
                ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(0)}\r\n", client.ReadReply());
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
            }
        }

        /// <summary>
        /// The same proof for a pipelined run of reads, which takes the scatter-gather path rather than the
        /// single-read path.
        /// </summary>
        /// <remarks>
        /// A run of consecutive GETs is served by submitting every read in the run and then waiting for them
        /// together. Waiting in place holds one thread for the whole run, so a clamped pool serves the runs a
        /// poolful at a time and the elapsed time grows with the ratio of sessions to threads - which the
        /// single-read test cannot show, because a lone GET never enters the scatter-gather path. Parking on
        /// the run's completion holds no thread, so every session's reads are in flight at once.
        /// </remarks>
        [Test]
        public void ManyConcurrentPipelinedRunsOutnumberTheThreadPoolAndStillFinishTogether()
        {
            const int ReadDelayMs = 1000;
            const int GetsPerRun = 2;

            // Linux only, for the reason given on the single-read test above.
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");

            Populate();

            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            ThreadPool.GetMinThreads(out var minWorkers, out _);

            var cappedWorkers = Math.Max(Environment.ProcessorCount, minWorkers);
            var sessionCount = Math.Max(64, cappedWorkers * 4);
            ClassicAssert.LessOrEqual(sessionCount * GetsPerRun, PopulatedKeys,
                "Not enough populated keys to give every session a distinct run.");

            var clients = new RawClient[sessionCount];
            var clamped = false;
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawClient();

                clamped = ThreadPool.SetMaxThreads(cappedWorkers, maxIo);
                ClassicAssert.IsTrue(clamped,
                    $"Could not clamp the worker pool to {cappedWorkers} from ({maxWorkers},{maxIo}) with a " +
                    $"worker minimum of {minWorkers}, so this test cannot demonstrate thread starvation.");

                deviceFactoryCreator.ReadDelayMs = ReadDelayMs;

                // One run on its own, as the yardstick. Its reads are submitted together and complete
                // together, so a whole run costs about one device round trip.
                var single = Stopwatch.StartNew();
                clients[0].SendPipeline(Run(0, GetsPerRun));
                for (var j = 0; j < GetsPerRun; j++)
                    ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(j)}\r\n", clients[0].ReadReply());
                single.Stop();

                ClassicAssert.GreaterOrEqual(single.Elapsed.TotalMilliseconds, ReadDelayMs * 0.8,
                    "A run returned faster than the injected device delay, so it never went to the device.");

                var readsBefore = deviceFactoryCreator.ReadCount;

                var sw = Stopwatch.StartNew();
                for (var i = 0; i < sessionCount; i++)
                    clients[i].SendPipeline(Run(i * GetsPerRun, GetsPerRun));

                try
                {
                    for (var i = 0; i < sessionCount; i++)
                        for (var j = 0; j < GetsPerRun; j++)
                            ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(i * GetsPerRun + j)}\r\n",
                                clients[i].ReadReply(), $"Wrong value for reply {j} of session {i}");
                }
                catch (IOException e)
                {
                    Assert.Fail($"A session produced no reply within the socket timeout, with {sessionCount} " +
                        $"concurrent runs of {GetsPerRun} device reads against {cappedWorkers} worker threads: " +
                        $"the runs are being serialized on thread-pool threads rather than suspended. {e.Message}");
                }
                sw.Stop();

                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore, sessionCount * GetsPerRun,
                    "Fewer device reads than reads requested, so some of these were served from memory.");

                ClassicAssert.Less(sw.Elapsed.TotalMilliseconds, single.Elapsed.TotalMilliseconds * 3,
                    $"{sessionCount} concurrent runs of {GetsPerRun} device reads took {sw.Elapsed.TotalSeconds:F1}s " +
                    $"against a single run of {single.Elapsed.TotalSeconds:F1}s with {cappedWorkers} worker " +
                    "threads, which means the runs were serialized on thread-pool threads rather than suspended.");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                if (clamped)
                    _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                foreach (var c in clients)
                    c?.Dispose();
            }
        }

        /// <summary>Builds an MGET over <paramref name="count"/> keys starting at <paramref name="from"/>.</summary>
        static string[] MGet(int from, int count)
        {
            var args = new string[count + 1];
            args[0] = "MGET";
            for (var j = 0; j < count; j++)
                args[j + 1] = Key(from + j);
            return args;
        }

        /// <summary>Builds the reply an MGET over those keys should produce.</summary>
        static string MGetReply(int from, int count)
        {
            var sb = new StringBuilder($"*{count}\r\n");
            for (var j = 0; j < count; j++)
                sb.Append($"${ValueLength}\r\n{Value(from + j)}\r\n");
            return sb.ToString();
        }

        /// <summary>
        /// An MGET issues all of its reads before draining any of them, so a batch that suspends has already
        /// written the replies for the keys it found in memory into the response buffer. Those have to still
        /// be there, in their original positions, when the device reads land.
        /// </summary>
        [Test]
        public void MGetInterleavesMemoryAndDeviceValuesInKeyOrder()
        {
            Populate();

            using var client = new RawClient();

            // Alternates the oldest keys, which are below the head address, with the newest, which are not.
            const int Pairs = 16;
            var args = new string[(Pairs * 2) + 1];
            var expected = new StringBuilder($"*{Pairs * 2}\r\n");
            args[0] = "MGET";
            for (var i = 0; i < Pairs; i++)
            {
                var onDevice = i;
                var inMemory = PopulatedKeys - 1 - i;
                args[(i * 2) + 1] = Key(onDevice);
                args[(i * 2) + 2] = Key(inMemory);
                expected.Append($"${ValueLength}\r\n{Value(onDevice)}\r\n");
                expected.Append($"${ValueLength}\r\n{Value(inMemory)}\r\n");
            }

            ClassicAssert.AreEqual(expected.ToString(), client.Execute(args));

            ClassicAssert.Greater(deviceFactoryCreator.ReadCount, 0,
                "No device read was issued, so none of these keys had left memory and the test proved nothing.");
        }

        /// <summary>
        /// A missing key inside an MGET that also goes to the device still replies as a null at its own
        /// position, which is the one place where the pending path writes something other than a value.
        /// </summary>
        [Test]
        public void MGetRepliesNullForAMissingKeyAlongsideDeviceReads()
        {
            Populate();

            using var client = new RawClient();

            var reply = client.Execute("MGET", Key(0), "pendingget:absent", Key(1));
            ClassicAssert.AreEqual(
                $"*3\r\n${ValueLength}\r\n{Value(0)}\r\n$-1\r\n${ValueLength}\r\n{Value(1)}\r\n", reply);
        }

        /// <summary>
        /// An MGET inside a transaction parks for its device reads like any other, and the replies it
        /// writes on resume land in key order inside the transaction's reply array.
        /// </summary>
        [Test]
        public void MGetInsideATransactionReadsBackValuesThatAreNoLongerInMemory()
        {
            Populate();

            using var client = new RawClient();

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("MULTI"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute(MGet(0, 8)));

            ClassicAssert.AreEqual($"*1\r\n{MGetReply(0, 8)}", client.Execute("EXEC"));
        }

        /// <summary>
        /// The same guarantee as <see cref="ManyConcurrentDeviceReadsOutnumberTheThreadPoolAndStillFinishTogether"/>,
        /// for the batched read path: an MGET has a whole set of keys outstanding on the device at once, and
        /// draining that set on the network thread would hold the thread for the slowest of them.
        /// </summary>
        [Test]
        public void ManyConcurrentMGetsOutnumberTheThreadPoolAndStillFinishTogether()
        {
            const int ReadDelayMs = 1000;
            const int KeysPerMGet = 8;

            // Linux only, for the reason given on ManyConcurrentDeviceReadsOutnumberTheThreadPoolAndStillFinishTogether.
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");

            Populate();

            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            ThreadPool.GetMinThreads(out var minWorkers, out _);

            var cappedWorkers = Math.Max(Environment.ProcessorCount, minWorkers);
            var sessionCount = Math.Max(64, cappedWorkers * 4);

            var clients = new RawClient[sessionCount];
            var clamped = false;
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawClient();

                clamped = ThreadPool.SetMaxThreads(cappedWorkers, maxIo);
                ClassicAssert.IsTrue(clamped,
                    $"Could not clamp the worker pool to {cappedWorkers} from ({maxWorkers},{maxIo}) with a " +
                    $"worker minimum of {minWorkers}, so this test cannot demonstrate thread starvation.");

                deviceFactoryCreator.ReadDelayMs = ReadDelayMs;

                // One MGET on its own, as the yardstick. Its own reads overlap on the device, so this is the
                // right comparison: what is under test is whether the sessions overlap too.
                var single = Stopwatch.StartNew();
                ClassicAssert.AreEqual(MGetReply(0, KeysPerMGet), clients[0].Execute(MGet(0, KeysPerMGet)));
                single.Stop();

                ClassicAssert.GreaterOrEqual(single.Elapsed.TotalMilliseconds, ReadDelayMs * 0.8,
                    "A single MGET returned faster than the injected device delay, so it never went to the device.");

                var readsBefore = deviceFactoryCreator.ReadCount;

                var sw = Stopwatch.StartNew();
                for (var i = 0; i < sessionCount; i++)
                    clients[i].Send(MGet(Window(i, KeysPerMGet), KeysPerMGet));

                try
                {
                    for (var i = 0; i < sessionCount; i++)
                        ClassicAssert.AreEqual(MGetReply(Window(i, KeysPerMGet), KeysPerMGet), clients[i].ReadReply(),
                            $"Wrong reply for the MGET starting at {Key(Window(i, KeysPerMGet))}");
                }
                catch (IOException e)
                {
                    Assert.Fail($"A session produced no reply within the socket timeout, with {sessionCount} " +
                        $"concurrent MGETs of {KeysPerMGet} device reads against {cappedWorkers} worker threads: " +
                        $"the reads are being drained on thread-pool threads rather than suspended. {e.Message}");
                }
                sw.Stop();

                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore, sessionCount,
                    "Fewer device reads than sessions, so some of these MGETs were served entirely from memory.");

                ClassicAssert.Less(sw.Elapsed.TotalMilliseconds, single.Elapsed.TotalMilliseconds * 3,
                    $"{sessionCount} concurrent MGETs took {sw.Elapsed.TotalSeconds:F1}s against a single MGET of " +
                    $"{single.Elapsed.TotalSeconds:F1}s with {cappedWorkers} worker threads, which means they were " +
                    "serialized on thread-pool threads rather than suspended.");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                if (clamped)
                    _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                foreach (var c in clients)
                    c?.Dispose();
            }
        }

        /// <summary>
        /// The unified-store commands read a record's header rather than its value, so on a working set
        /// larger than memory they go to the device as often as a <c>GET</c> does. Each has to give the same
        /// answer across a suspension as it gives from memory.
        /// </summary>
        /// <remarks>
        /// The control key is written last, so it is still in memory, and is given the same key length and
        /// the same value as the key under test, so even the answers that depend on size - <c>MEMORY
        /// USAGE</c>, <c>OBJECT ENCODING</c> - have to match exactly.
        /// </remarks>
        [Test]
        public void UnifiedCommandsAnswerFromTheDeviceExactlyAsTheyDoFromMemory()
        {
            Populate();

            using var client = new RawClient();

            var onDevice = Key(0);
            var inMemory = $"pendingget:9{0:D5}";
            ClassicAssert.AreEqual(onDevice.Length, inMemory.Length);
            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SET", inMemory, Value(0)));

            void AssertSameAcrossASuspension(params string[] verb)
            {
                var fromMemory = client.Execute([.. verb, inMemory]);

                var readsBefore = deviceFactoryCreator.ReadCount;
                var fromDevice = client.Execute([.. verb, onDevice]);

                ClassicAssert.Greater(deviceFactoryCreator.ReadCount, readsBefore,
                    $"{string.Join(' ', verb)} {onDevice} never went to the device, so it proves nothing.");
                ClassicAssert.AreEqual(fromMemory, fromDevice,
                    $"{string.Join(' ', verb)} answered differently across a suspension.");
            }

            AssertSameAcrossASuspension("TYPE");
            AssertSameAcrossASuspension("TTL");
            AssertSameAcrossASuspension("PTTL");
            AssertSameAcrossASuspension("EXPIRETIME");
            AssertSameAcrossASuspension("PEXPIRETIME");
            AssertSameAcrossASuspension("EXISTS");
            AssertSameAcrossASuspension("OBJECT", "ENCODING");
            AssertSameAcrossASuspension("MEMORY", "USAGE");

            // PERSIST on a key with no timeout reports that it changed nothing, and changes nothing.
            AssertSameAcrossASuspension("PERSIST");
        }

        /// <summary>
        /// <c>EXPIRE</c> and <c>PERSIST</c> are the updates of the family, so beyond answering correctly
        /// they have to leave the record in the state they reported.
        /// </summary>
        [Test]
        public void ExpireAndPersistApplyToAKeyThatIsNoLongerInMemory()
        {
            Populate();

            using var client = new RawClient();
            var key = Key(1);

            var readsBefore = deviceFactoryCreator.ReadCount;
            ClassicAssert.AreEqual(":1\r\n", client.Execute("EXPIRE", key, "1000"));
            ClassicAssert.Greater(deviceFactoryCreator.ReadCount, readsBefore,
                $"EXPIRE {key} never went to the device, so it proves nothing.");

            ClassicAssert.AreEqual(":1000\r\n", client.Execute("TTL", key),
                "EXPIRE reported that it set a timeout it did not set.");
            ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(1)}\r\n", client.Execute("GET", key),
                "EXPIRE lost the value of the record it updated.");

            ClassicAssert.AreEqual(":1\r\n", client.Execute("PERSIST", key));
            ClassicAssert.AreEqual(":-1\r\n", client.Execute("TTL", key),
                "PERSIST reported that it removed a timeout it did not remove.");
        }

        /// <summary>
        /// <c>EXISTS</c> issues every one of its keys before waiting for any of them, so its reads overlap on
        /// the device, and the count it reports has to survive that.
        /// </summary>
        [Test]
        public void ExistsCountsEveryKeyItFindsAcrossOneSuspension()
        {
            Populate();

            using var client = new RawClient();

            // Eight keys on the device, one of them twice - EXISTS counts repeats - and two never written.
            string[] args = ["EXISTS", Key(0), Key(1), Key(2), Key(3), Key(0),
                "pendingget:absent1", Key(4), Key(5), Key(6), Key(7), "pendingget:absent2"];

            var readsBefore = deviceFactoryCreator.ReadCount;
            ClassicAssert.AreEqual(":9\r\n", client.Execute(args));

            ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore, 8,
                "Fewer device reads than keys on the device, so this EXISTS was served from memory.");
        }

        /// <summary>
        /// The unified-store commands park inside a running transaction as they do outside one, and each
        /// still answers correctly when the transaction is replayed around the suspension.
        /// </summary>
        [Test]
        public void UnifiedCommandsInsideATransactionReadBackKeysThatAreNoLongerInMemory()
        {
            Populate();

            using var client = new RawClient();

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("MULTI"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("TYPE", Key(0)));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("TTL", Key(1)));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("EXISTS", Key(2), Key(3), "pendingget:absent"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("EXPIRE", Key(4), "1000"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("TTL", Key(4)));

            ClassicAssert.AreEqual("*5\r\n+string\r\n:-1\r\n:2\r\n:1\r\n:1000\r\n", client.Execute("EXEC"));
        }

        /// <summary>
        /// The point of the design, on the unified-store path. With the thread pool clamped, more sessions
        /// than there are worker threads each issue a unified-store command that has to go to the device, and
        /// they all have to come back in about the time one of them takes. If the reads were drained in place
        /// instead, each would hold a worker thread for its whole device round trip and the sessions would
        /// run in waves of <c>cappedWorkers</c>.
        /// </summary>
        /// <remarks>
        /// Half the sessions send <c>TTL</c> and half send <c>EXISTS</c>, because the two take different
        /// paths out of the dispatch: a single operation whose own output is the reply, and a set of
        /// operations whose count is the reply.
        /// </remarks>
        [Test]
        public void ManyConcurrentUnifiedCommandsOutnumberTheThreadPoolAndStillFinishTogether()
        {
            const int ReadDelayMs = 1000;
            const int KeysPerExists = 4;

            // Linux only, for the reason given on ManyConcurrentDeviceReadsOutnumberTheThreadPoolAndStillFinishTogether.
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");

            Populate();

            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            ThreadPool.GetMinThreads(out var minWorkers, out _);

            var cappedWorkers = Math.Max(Environment.ProcessorCount, minWorkers);
            var sessionCount = Math.Max(64, cappedWorkers * 4);

            // Session i sends TTL of one key when even, and EXISTS of KeysPerExists keys when odd.
            bool IsExists(int i) => (i & 1) == 1;
            string[] Command(int i) => IsExists(i)
                ? [.. new[] { "EXISTS" }, .. MGet(Window(i, KeysPerExists), KeysPerExists)[1..]]
                : ["TTL", Key(Window(i, 1))];
            string Reply(int i) => IsExists(i) ? $":{KeysPerExists}\r\n" : ":-1\r\n";

            var clients = new RawClient[sessionCount];
            var clamped = false;
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawClient();

                clamped = ThreadPool.SetMaxThreads(cappedWorkers, maxIo);
                ClassicAssert.IsTrue(clamped,
                    $"Could not clamp the worker pool to {cappedWorkers} from ({maxWorkers},{maxIo}) with a " +
                    $"worker minimum of {minWorkers}, so this test cannot demonstrate thread starvation.");

                deviceFactoryCreator.ReadDelayMs = ReadDelayMs;

                // One command on its own, as the yardstick.
                var single = Stopwatch.StartNew();
                ClassicAssert.AreEqual(Reply(0), clients[0].Execute(Command(0)));
                single.Stop();

                ClassicAssert.GreaterOrEqual(single.Elapsed.TotalMilliseconds, ReadDelayMs * 0.8,
                    "A single TTL returned faster than the injected device delay, so it never went to the device.");

                var readsBefore = deviceFactoryCreator.ReadCount;

                var sw = Stopwatch.StartNew();
                for (var i = 0; i < sessionCount; i++)
                    clients[i].Send(Command(i));

                try
                {
                    for (var i = 0; i < sessionCount; i++)
                        ClassicAssert.AreEqual(Reply(i), clients[i].ReadReply(),
                            $"Wrong reply for session {i}: {string.Join(' ', Command(i))}");
                }
                catch (IOException e)
                {
                    Assert.Fail($"A session produced no reply within the socket timeout, with {sessionCount} " +
                        $"concurrent unified-store commands against {cappedWorkers} worker threads: the reads " +
                        $"are being drained on thread-pool threads rather than suspended. {e.Message}");
                }
                sw.Stop();

                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore, sessionCount,
                    "Fewer device reads than sessions, so some of these commands were served entirely from memory.");

                ClassicAssert.Less(sw.Elapsed.TotalMilliseconds, single.Elapsed.TotalMilliseconds * 3,
                    $"{sessionCount} concurrent unified-store commands took {sw.Elapsed.TotalSeconds:F1}s against " +
                    $"a single one of {single.Elapsed.TotalSeconds:F1}s with {cappedWorkers} worker threads, which " +
                    "means they were serialized on thread-pool threads rather than suspended.");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                if (clamped)
                    _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                foreach (var c in clients)
                    c?.Dispose();
            }
        }

        /// <summary>
        /// First key of the window session <paramref name="i"/> reads. Windows wrap within the older half of
        /// the populated keys, which keeps every one of them below the head address however many sessions the
        /// clamped pool size works out to.
        /// </summary>
        static int Window(int i, int keysPerRead) => (i % (PopulatedKeys / 2 / keysPerRead)) * keysPerRead;

        /// <summary>Builds a pipelined run of <paramref name="count"/> GETs starting at <paramref name="from"/>.</summary>
        static string[][] Run(int from, int count)
        {
            var commands = new string[count][];
            for (var j = 0; j < count; j++)
                commands[j] = ["GET", Key(from + j)];
            return commands;
        }

        /// <summary>
        /// The point of the design, on the storage path: with the thread pool clamped as small as the runtime
        /// allows, far more sessions read from the device at once than there are threads to hold them, and
        /// they all finish in about the time one read takes.
        /// </summary>
        /// <remarks>
        /// A read completed in place occupies its thread for the whole device round trip, so the clamped pool
        /// would serve these in waves of at most one read per thread and the elapsed time would be a multiple
        /// of a single read. The threshold is expressed against a measured single read rather than against the
        /// injected delay, so a read that needs more than one device round trip does not look like starvation.
        /// </remarks>
        [Test]
        public void ManyConcurrentDeviceReadsOutnumberTheThreadPoolAndStillFinishTogether()
        {
            const int ReadDelayMs = 1000;

            // Linux only, and not an arbitrary restriction. This test proves a negative - that a suspended
            // session is holding no thread-pool thread - by starving the pool that dispatches its work. On
            // Linux socket completions run on worker threads, so clamping workers starves exactly that path.
            // On Windows they run on the I/O-completion pool instead, so clamping workers does not constrain
            // dispatch and proves nothing, while clamping the I/O pool deadlocks the accept path before the
            // first read is issued and takes the whole test host down with it: the pool is process-global and
            // the runtime's own timers and continuations need it too.
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");

            Populate();

            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            ThreadPool.GetMinThreads(out var minWorkers, out _);

            // Worker threads only. The I/O-completion maximum is left alone because on Windows the accept
            // path, the socket receives and the device completions all run on that pool, and clamping it
            // deadlocks the server before a single read is issued. The runtime also refuses a maximum below
            // the processor count or below the current minimum, so this is the smallest worker pool here.
            var cappedWorkers = Math.Max(Environment.ProcessorCount, minWorkers);
            var sessionCount = Math.Max(64, cappedWorkers * 4);

            var clients = new RawClient[sessionCount];
            var clamped = false;
            try
            {
                // Connect before clamping: accepting a connection needs a thread too, and starving that
                // only proves the pool is small.
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawClient();

                clamped = ThreadPool.SetMaxThreads(cappedWorkers, maxIo);
                ClassicAssert.IsTrue(clamped,
                    $"Could not clamp the worker pool to {cappedWorkers} from ({maxWorkers},{maxIo}) with a " +
                    $"worker minimum of {minWorkers}, so this test cannot demonstrate thread starvation.");

                deviceFactoryCreator.ReadDelayMs = ReadDelayMs;

                // One read on its own, as the yardstick for what a single device round trip costs here.
                var single = Stopwatch.StartNew();
                ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(0)}\r\n", clients[0].Execute("GET", Key(0)));
                single.Stop();

                ClassicAssert.GreaterOrEqual(single.Elapsed.TotalMilliseconds, ReadDelayMs * 0.8,
                    "A single read returned faster than the injected device delay, so it never went to the device.");

                var readsBefore = deviceFactoryCreator.ReadCount;

                var sw = Stopwatch.StartNew();
                for (var i = 0; i < sessionCount; i++)
                    clients[i].Send("GET", Key(i));

                try
                {
                    for (var i = 0; i < sessionCount; i++)
                        ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(i)}\r\n", clients[i].ReadReply(), $"Wrong value for {Key(i)}");
                }
                catch (IOException e)
                {
                    Assert.Fail($"A session produced no reply within the socket timeout, with {sessionCount} " +
                        $"concurrent device reads against {cappedWorkers} worker threads: the reads are being " +
                        $"serialized on thread-pool threads rather than suspended. {e.Message}");
                }
                sw.Stop();

                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore, sessionCount,
                    "Fewer device reads than sessions, so some of these reads were served from memory.");

                ClassicAssert.Less(sw.Elapsed.TotalMilliseconds, single.Elapsed.TotalMilliseconds * 3,
                    $"{sessionCount} concurrent device reads took {sw.Elapsed.TotalSeconds:F1}s against a single " +
                    $"read of {single.Elapsed.TotalSeconds:F1}s with {cappedWorkers} worker threads, which means " +
                    "they were serialized on thread-pool threads rather than suspended.");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                if (clamped)
                    _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                foreach (var c in clients)
                    c?.Dispose();
            }
        }

        /// <summary>
        /// The same point inside <c>MULTI</c>/<c>EXEC</c>: a transaction whose read misses memory parks for
        /// the device and gives up its thread, so far more transactions read at once than there are threads
        /// to hold them.
        /// </summary>
        /// <remarks>
        /// A transaction used to drain its pending reads in place, which held a thread for the whole device
        /// round trip. Restoring that makes this test serialize into waves of at most one transaction per
        /// worker thread, and the elapsed time becomes a multiple of a single read rather than close to it.
        /// The threshold is expressed against a measured single transaction, so a read needing more than one
        /// device round trip does not look like starvation. See the Linux note on
        /// <see cref="ManyConcurrentDeviceReadsOutnumberTheThreadPoolAndStillFinishTogether"/>.
        /// </remarks>
        [Test]
        public void ManyConcurrentTransactionsReadingFromTheDeviceOutnumberTheThreadPool()
        {
            const int ReadDelayMs = 1000;

            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");

            Populate();

            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            ThreadPool.GetMinThreads(out var minWorkers, out _);

            var cappedWorkers = Math.Max(Environment.ProcessorCount, minWorkers);
            var sessionCount = Math.Max(64, cappedWorkers * 4);

            var clients = new RawClient[sessionCount];
            var clamped = false;
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawClient();

                clamped = ThreadPool.SetMaxThreads(cappedWorkers, maxIo);
                ClassicAssert.IsTrue(clamped,
                    $"Could not clamp the worker pool to {cappedWorkers} from ({maxWorkers},{maxIo}) with a " +
                    $"worker minimum of {minWorkers}, so this test cannot demonstrate thread starvation.");

                deviceFactoryCreator.ReadDelayMs = ReadDelayMs;

                // One transaction on its own, as the yardstick for what a single device round trip costs here.
                var single = Stopwatch.StartNew();
                ClassicAssert.AreEqual(TxnReply(0), RunTransaction(clients[0], 0));
                single.Stop();

                ClassicAssert.GreaterOrEqual(single.Elapsed.TotalMilliseconds, ReadDelayMs * 0.8,
                    "A single transaction returned faster than the injected device delay, so it never went to the device.");

                var readsBefore = deviceFactoryCreator.ReadCount;

                var sw = Stopwatch.StartNew();
                for (var i = 0; i < sessionCount; i++)
                    SendTransaction(clients[i], i);

                try
                {
                    for (var i = 0; i < sessionCount; i++)
                        ClassicAssert.AreEqual(TxnReply(i), ReadTransactionReply(clients[i]), $"Wrong reply for {Key(i)}");
                }
                catch (IOException e)
                {
                    Assert.Fail($"A session produced no reply within the socket timeout, with {sessionCount} " +
                        $"concurrent transactional device reads against {cappedWorkers} worker threads: the reads " +
                        $"are being drained on thread-pool threads rather than suspended. {e.Message}");
                }
                sw.Stop();

                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore, sessionCount,
                    "Fewer device reads than sessions, so some of these reads were served from memory.");

                ClassicAssert.Less(sw.Elapsed.TotalMilliseconds, single.Elapsed.TotalMilliseconds * 3,
                    $"{sessionCount} concurrent transactional device reads took {sw.Elapsed.TotalSeconds:F1}s against " +
                    $"a single one of {single.Elapsed.TotalSeconds:F1}s with {cappedWorkers} worker threads, which " +
                    "means the transactions were drained on thread-pool threads rather than suspended.");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                if (clamped)
                    _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                foreach (var c in clients)
                    c?.Dispose();
            }

            // A single-key transaction, so the read inside it takes the single-read path rather than the
            // scatter-gather one.
            static void SendTransaction(RawClient client, int i)
                => client.SendPipeline(["MULTI"], ["GET", Key(i)], ["EXEC"]);

            static string ReadTransactionReply(RawClient client)
            {
                ClassicAssert.AreEqual("+OK\r\n", client.ReadReply());
                ClassicAssert.AreEqual("+QUEUED\r\n", client.ReadReply());
                return client.ReadReply();
            }

            static string RunTransaction(RawClient client, int i)
            {
                SendTransaction(client, i);
                return ReadTransactionReply(client);
            }

            static string TxnReply(int i) => $"*1\r\n${ValueLength}\r\n{Value(i)}\r\n";
        }

        /// <summary>
        /// A script's commands run on a nested consume with no receive loop to unwind to, so a read that
        /// misses memory is waited out on that thread rather than parked. The values still have to come
        /// back through the script, and the connection has to stay usable afterwards.
        /// </summary>
        /// <remarks>
        /// Parking here would strand the reply: the resume re-enters the parse loop on the script's own
        /// request buffer and finishes that batch, which is not the batch the client is waiting on. The
        /// three commands cover the three pending paths a script reaches -- string, object and unified --
        /// since <c>GET</c> and <c>SET</c> are served by the script engine directly.
        /// </remarks>
        [Test]
        public void ScriptedReadsFromTheDeviceAnswerWithoutParkingTheSession()
        {
            Populate();
            PopulateObjects();

            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var readsBefore = deviceFactoryCreator.ReadCount;

            var strings = (RedisResult[])db.ScriptEvaluate(
                "return redis.call('MGET', KEYS[1], KEYS[2])", [Key(0), Key(1)]);
            ClassicAssert.AreEqual(Value(0), (string)strings[0]);
            ClassicAssert.AreEqual(Value(1), (string)strings[1]);

            var field = db.ScriptEvaluate("return redis.call('HGET', KEYS[1], ARGV[1])", [(RedisKey)HashKey(0)], ["f0"]);
            ClassicAssert.AreEqual("0-0", (string)field);

            var exists = db.ScriptEvaluate("return redis.call('EXISTS', KEYS[1], KEYS[2])", [Key(2), Key(3)]);
            ClassicAssert.AreEqual(2, (long)exists);

            ClassicAssert.Greater(deviceFactoryCreator.ReadCount - readsBefore, 0,
                "No device reads, so none of these scripted reads left memory.");

            // The nested consume must not have disturbed the connection it borrowed.
            ClassicAssert.AreEqual(Value(4), (string)db.StringGet(Key(4)));
        }

        /// <summary>
        /// A scripted read that misses memory must not need a second thread to finish. Waiting it out on
        /// the thread that issued it needs no help; parking it would block that thread until some other
        /// thread picks the resume up, so with the pool clamped the scripts outnumber the threads able to
        /// release them.
        /// </summary>
        /// <remarks>
        /// This is the deadlock vector the transport gate closes, and the reason a nested consume completes
        /// its device reads in place rather than parking. See the Linux note on
        /// <see cref="ManyConcurrentDeviceReadsOutnumberTheThreadPoolAndStillFinishTogether"/>.
        /// </remarks>
        [Test]
        public void ManyConcurrentScriptedDeviceReadsDoNotNeedASecondThreadToFinish()
        {
            const int ReadDelayMs = 200;

            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");

            Populate();

            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            ThreadPool.GetMinThreads(out var minWorkers, out _);

            var cappedWorkers = Math.Max(Environment.ProcessorCount, minWorkers);
            var sessionCount = Math.Max(64, cappedWorkers * 4);

            var clients = new RawClient[sessionCount];
            var clamped = false;
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawClient();

                clamped = ThreadPool.SetMaxThreads(cappedWorkers, maxIo);
                ClassicAssert.IsTrue(clamped, "Could not clamp the worker pool, so this test proves nothing.");

                deviceFactoryCreator.ReadDelayMs = ReadDelayMs;

                var readsBefore = deviceFactoryCreator.ReadCount;

                for (var i = 0; i < sessionCount; i++)
                    clients[i].Send("EVAL", "return redis.call('MGET', KEYS[1])", "1", Key(i));

                try
                {
                    for (var i = 0; i < sessionCount; i++)
                        ClassicAssert.AreEqual($"*1\r\n${ValueLength}\r\n{Value(i)}\r\n", clients[i].ReadReply(),
                            $"Wrong value for {Key(i)}");
                }
                catch (IOException e)
                {
                    Assert.Fail($"A script produced no reply within the socket timeout, with {sessionCount} " +
                        $"scripted device reads against {cappedWorkers} worker threads: a scripted read is " +
                        $"parking and waiting for another thread to resume it. {e.Message}");
                }

                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore, sessionCount,
                    "Fewer device reads than scripts, so some of these reads were served from memory.");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                if (clamped)
                    _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                foreach (var c in clients)
                    c?.Dispose();
            }
        }

        /// <summary>
        /// On a server that processes client input on the socket event threads, a command that completes its device reads
        /// on the thread it runs on, rather than parking for them, moves to the thread pool once the store has spilled to
        /// the device, and answers as it would from memory.
        /// </summary>
        /// <remarks>
        /// The device counts the reads issued off the thread pool. Every command here completes its reads in place, so
        /// that count has to stay at zero: run where they are received, these commands would hold an event thread, and
        /// every connection it serves, for each device round trip. The <c>GET</c> at the end is the control. It parks on
        /// its read, which it issues on the event thread that received it, so the count has to move.
        /// </remarks>
        [Test]
        public void CommandsThatWaitForTheDeviceInPlaceLeaveAnEventThread()
        {
            RestartServer(inlineCompletions: true);
            Populate();

            using var client = new RawClient();
            Settle(client);

            void AssertFromTheDevice(string expected, params string[] argv)
            {
                var readsBefore = deviceFactoryCreator.ReadCount;
                ClassicAssert.AreEqual(expected, client.Execute(argv), $"Wrong reply to {argv[0]}.");
                ClassicAssert.Greater(deviceFactoryCreator.ReadCount, readsBefore,
                    $"{argv[0]} never went to the device, so it proves nothing.");
            }

            AssertFromTheDevice($"*1\r\n:{(int)Value(10)[0]}\r\n", "BITFIELD_RO", Key(10), "GET", "u8", "0");
            AssertFromTheDevice($"*1\r\n:{Value(11)[0] + 1}\r\n", "BITFIELD", Key(11), "INCRBY", "u8", "0", "1");
            AssertFromTheDevice($"${ValueLength}\r\n{Value(13)}\r\n", "LCS", Key(13), Key(13 + 26));
            AssertFromTheDevice($"*1\r\n${Key(20).Length}\r\n{Key(20)}\r\n", "KEYS", Key(20));

            ClassicAssert.AreEqual(0, deviceFactoryCreator.ReadsOffThreadPool,
                "A command that completes its device reads in place issued them off the thread pool.");

            Settle(client);
            AssertFromTheDevice($"${ValueLength}\r\n{Value(21)}\r\n", "GET", Key(21));
            ClassicAssert.Greater(deviceFactoryCreator.ReadsOffThreadPool, 0,
                "A GET parks on its read where it is received, yet no read was counted off the thread pool, so the count proves nothing.");
        }

        /// <summary>
        /// On a server that hands client input to the thread pool, a command that waits on its thread runs on the pool
        /// thread that received it, as every command does there: the pool adds threads while its own are held.
        /// </summary>
        [Test]
        public void CommandsThatWaitInPlaceRunOnThePoolThreadThatReceivedThem()
        {
            if (TestUtils.InlineCompletionsForcedByEnvironment)
                Assert.Ignore("The environment runs every socket's completions inline.");

            Populate();

            using var client = new RawClient();
            var readsBefore = deviceFactoryCreator.ReadCount;
            ClassicAssert.AreEqual($"*1\r\n:{(int)Value(10)[0]}\r\n", client.Execute("BITFIELD_RO", Key(10), "GET", "u8", "0"));
            ClassicAssert.Greater(deviceFactoryCreator.ReadCount, readsBefore, "BITFIELD_RO never went to the device, so it proves nothing.");
            ClassicAssert.AreEqual("+OK\r\n", client.Execute("FLUSHDB"));

            ClassicAssert.AreEqual(0, Offloads, "A command received on a thread-pool thread was moved to the thread pool.");
        }

        /// <summary>
        /// A write that does not read the record it replaces runs on the event thread that receives it, even with records
        /// spilled to the device.
        /// </summary>
        [Test]
        public void BlindWritesRunWhereTheyAreReceived()
        {
            RestartServer(inlineCompletions: true, memorySize: SpilledLogMemorySize);

            using var client = new RawClient();
            ClassicAssert.AreEqual(":1\r\n", client.Execute("HSET", HashKey(10), "f", "v"));
            ClassicAssert.AreEqual(":1\r\n", client.Execute("RPUSH", ListKey(11), "x"));
            Populate();
            Settle(client);

            WaitUntilAllocationNeedNotWaitForFlush("The log has to wait for a flush to add a record, so every write moves to the thread pool.");

            void AssertInPlace(string expected, params string[] argv)
            {
                var readsBefore = deviceFactoryCreator.ReadCount;
                var offloadsBefore = Offloads;
                ClassicAssert.AreEqual(expected, client.Execute(argv), $"Wrong reply to {argv[0]}.");
                ClassicAssert.AreEqual(readsBefore, deviceFactoryCreator.ReadCount, $"{argv[0]} read from the device.");
                ClassicAssert.AreEqual(offloadsBefore, Offloads, $"{argv[0]} was moved to the thread pool.");
            }

            AssertInPlace("+OK\r\n", "SET", Key(10), "v");
            AssertInPlace("+OK\r\n", "MSET", Key(11), "v", Key(12), "v");
            AssertInPlace("+OK\r\n", "SETEX", Key(13), "100", "v");
            AssertInPlace("+OK\r\n", "PSETEX", Key(14), "100000", "v");
            AssertInPlace(":0\r\n", "DEL", Key(15), HashKey(10));
            AssertInPlace(":0\r\n", "UNLINK", Key(16), ListKey(11));

            var offloads = Offloads;
            ClassicAssert.AreEqual($"*1\r\n:{(int)Value(17)[0]}\r\n", client.Execute("BITFIELD_RO", Key(17), "GET", "u8", "0"));
            ClassicAssert.Greater(Offloads, offloads, "BITFIELD_RO reads the device in place, yet was not counted as moved.");
        }

        /// <summary>
        /// On a server that processes client input on the socket event threads, a write moves to the thread pool while
        /// the log cannot turn a page until a flush completes, as does one a transaction runs, and another connection is
        /// served meanwhile.
        /// </summary>
        /// <remarks>
        /// The log's flushes are held from its first, so no page can be freed for the writes, and none has been evicted, so
        /// nothing is read from the device: the wait for the flush alone moves the commands. A write run where it is
        /// received would wait for the flush on the event thread.
        /// </remarks>
        [Test]
        public void WritesThatMayWaitForAFlushLeaveAnEventThread()
        {
            const int Transactions = 4;

            // More records than fit on a page, so one of them has to turn the tail onto a new page
            const int WritesPastAPage = 64;

            RestartServer(inlineCompletions: true, memorySize: SpilledLogMemorySize);

            var log = server.Provider.StoreWrapper.store.Log;
            ClassicAssert.IsFalse(log.AllocationMayWaitForFlush,
                "The log has to wait for a flush before any is held, so every write moves to the thread pool.");

            using var writer = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };
            using var probe = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(10) };
            var transactions = new RawClient[Transactions];
            try
            {
                for (var i = 0; i < Transactions; i++)
                    transactions[i] = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };
                Settle([writer, probe, .. transactions]);

                deviceFactoryCreator.HoldWrites = true;

                // Each page the tail turns onto makes another read-only, and its flush is held, until no page in memory
                // is flushed past the head
                var next = 0;
                while (!log.AllocationMayWaitForFlush)
                {
                    ClassicAssert.Less(next, PopulatedKeys, "The log never came to wait for a flush.");
                    ClassicAssert.AreEqual("+OK\r\n", writer.Execute("SET", Key(next), Value(next)), $"Wrong reply for {Key(next)}");
                    next++;
                }

                ClassicAssert.Greater(deviceFactoryCreator.WritesHeld, 0, "No flush was held, yet the log has to wait for one.");
                ClassicAssert.AreEqual(log.BeginAddress, log.HeadAddress, "The log evicted records, so a device read could move the commands.");
                Settle(writer);

                var writes = new string[WritesPastAPage][];
                for (var i = 0; i < WritesPastAPage; i++)
                    writes[i] = ["SET", Key(next + i), Value(next + i)];

                var offloads = Offloads;
                writer.SendPipeline(writes);
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => Offloads > offloads, TimeSpan.FromSeconds(10)),
                    "A write that may wait for a flush ran on the event thread that received it.");

                offloads = Offloads;
                for (var i = 0; i < Transactions; i++)
                    transactions[i].SendPipeline(["MULTI"], ["SET", Key(i), "x"], ["EXEC"]);
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => Offloads >= offloads + Transactions, TimeSpan.FromSeconds(10)),
                    "A transaction's write that may wait for a flush ran on the event thread that received it.");

                ClassicAssert.IsFalse(writer.HasReply, "The writes finished without waiting for the held flush, so they prove nothing.");
                ClassicAssert.AreEqual("+PONG\r\n", probe.Execute("PING"));

                deviceFactoryCreator.ReleaseWrites();

                for (var i = 0; i < WritesPastAPage; i++)
                    ClassicAssert.AreEqual("+OK\r\n", writer.ReadReply(), $"Wrong reply for {Key(next + i)}");
                for (var i = 0; i < Transactions; i++)
                {
                    ClassicAssert.AreEqual("+OK\r\n", transactions[i].ReadReply());
                    ClassicAssert.AreEqual("+QUEUED\r\n", transactions[i].ReadReply());
                    ClassicAssert.AreEqual("*1\r\n+OK\r\n", transactions[i].ReadReply(), $"Wrong reply for {Key(i)}");
                }
            }
            finally
            {
                deviceFactoryCreator.ReleaseWrites();
                foreach (var c in transactions)
                    c?.Dispose();
            }
        }

        /// <summary>
        /// A session that resumes from a device read, and has next to run a command that waits for the device in place,
        /// does not run that command on the thread that completed the read.
        /// </summary>
        /// <remarks>
        /// The <c>GET</c> parks on its read, with the <c>BITFIELD_RO</c> queued behind it in the same batch. The read's
        /// completion queues the session's resume to the thread pool, where the <c>BITFIELD_RO</c> waits for its own read
        /// in place. Run on the device's completion thread, it would wait for its read on the one thread that completes
        /// it, and never reply.
        /// </remarks>
        [Test]
        public void ACommandThatWaitsForTheDeviceInPlaceLeavesTheThreadThatCompletedARead()
        {
            Populate();

            using var client = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(10) };
            try
            {
                deviceFactoryCreator.ReadDelayMs = 200;

                client.SendPipeline(["GET", Key(0)], ["BITFIELD_RO", Key(1), "GET", "u8", "0"]);
                ClassicAssert.AreEqual($"${ValueLength}\r\n{Value(0)}\r\n", client.ReadReply());
                ClassicAssert.AreEqual($"*1\r\n:{(int)Value(1)[0]}\r\n", client.ReadReply());
                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount, 2, "A command never went to the device, so this proves nothing.");
            }
            catch (IOException e)
            {
                Assert.Fail($"BITFIELD_RO waited for its device read on the thread that completes it. {e.Message}");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReadsHere();
            }
        }

        /// <summary>
        /// A transaction that resumes from a device read, and has next to run a command that waits for the device in
        /// place, does not run that command on the thread that completed the read.
        /// </summary>
        /// <remarks>
        /// The transaction parks on the device for its <c>GET</c>, and the read's completion queues its resume to the
        /// thread pool, where its <c>BITFIELD</c> waits for its own read in place with the transaction's locks held. Run
        /// on the device's completion thread, the <c>BITFIELD</c> would wait for its read on the one thread that completes
        /// it, and never reply.
        /// </remarks>
        [Test]
        public void ATransactionThatWaitsForTheDeviceInPlaceLeavesTheThreadThatCompletedARead()
        {
            Populate();

            using var transaction = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(10) };
            try
            {
                deviceFactoryCreator.ReadDelayMs = 600_000;

                transaction.SendPipeline(["MULTI"], ["GET", Key(0)], ["BITFIELD", Key(1), "INCRBY", "u8", "0", "1"], ["EXEC"]);
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > 0, TimeSpan.FromSeconds(30)),
                    "The transaction's GET never reached the device.");

                // Delays the BITFIELD's read too, so it completes on the thread the transaction resumes on
                deviceFactoryCreator.ReadDelayMs = 100;
                deviceFactoryCreator.CompleteHeldReads();

                ClassicAssert.AreEqual("+OK\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual("+QUEUED\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual("+QUEUED\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual($"*2\r\n${ValueLength}\r\n{Value(0)}\r\n*1\r\n:{Value(1)[0] + 1}\r\n", transaction.ReadReply());
            }
            catch (IOException e)
            {
                Assert.Fail($"The transaction waited for its BITFIELD's device read on the thread that completes it. {e.Message}");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReadsHere();
            }
        }

        /// <summary>
        /// On a server that processes client input on the socket event threads, with more commands that complete their
        /// device reads in place than the thread pool has threads, all waiting on the device, another connection is
        /// still served.
        /// </summary>
        /// <remarks>
        /// Connections join the event threads in turn, and there are twice as many commands as worker threads, so run
        /// where they are received these commands would hold every event thread for the device round trip, and nothing
        /// would be left to serve the <c>PING</c>. Moved to the thread pool, they wait there, as many at once as it has
        /// threads, while the event threads serve the other connections.
        /// </remarks>
        [Test]
        public void AnEventThreadIsFreeWhileMoreCommandsThanThreadsWaitOnTheDeviceInPlace()
        {
            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            ThreadPool.GetMinThreads(out var minWorkers, out _);

            var cappedWorkers = Math.Max(Environment.ProcessorCount, minWorkers);
            var sessionCount = cappedWorkers * 2;

            RestartServer(inlineCompletions: true);
            Populate();

            var clients = new RawClient[sessionCount];
            RawClient probe = null;
            var clamped = false;
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };
                probe = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(10) };
                Settle([.. clients, probe]);

                clamped = ThreadPool.SetMaxThreads(cappedWorkers, maxIo);
                ClassicAssert.IsTrue(clamped, "Could not clamp the worker pool, so this test proves nothing.");

                deviceFactoryCreator.ReadDelayMs = 600_000;

                for (var i = 0; i < sessionCount; i++)
                    clients[i].Send("BITFIELD_RO", Key(i), "GET", "u8", "0");

                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > 0, TimeSpan.FromSeconds(30)),
                    "No command reached the device.");
                Thread.Sleep(200);

                try
                {
                    ClassicAssert.AreEqual("+PONG\r\n", probe.Execute("PING"));
                }
                catch (IOException e)
                {
                    Assert.Fail($"No event thread served a PING while {sessionCount} commands waited on the device. {e.Message}");
                }

                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();

                for (var i = 0; i < sessionCount; i++)
                    ClassicAssert.AreEqual($"*1\r\n:{(int)Value(i)[0]}\r\n", clients[i].ReadReply(), $"Wrong reply for {Key(i)}");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();
                if (clamped)
                    _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                foreach (var c in clients)
                    c?.Dispose();
                probe?.Dispose();
            }
        }

        /// <summary>
        /// With a transaction holding a key's lock across a device read, and more commands waiting for that lock than
        /// the thread pool has threads, another connection is still served, and every waiting command completes once the
        /// transaction releases the lock.
        /// </summary>
        /// <remarks>
        /// The transaction's <c>GET</c> waits on the device for as long as the test holds the read, and the transaction
        /// keeps its lock on <c>H</c> until it completes. A <c>GET</c> of <c>H</c> runs where it is received, so waiting
        /// there for the lock would hold a thread-pool thread for the whole device read, and with the pool clamped
        /// nothing would be left to serve the <c>PING</c>.
        /// </remarks>
        [Test]
        public void ANetworkThreadIsFreeWhileMoreCommandsThanThreadsWaitForALockHeldAcrossADeviceRead()
        {
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");

            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            ThreadPool.GetMinThreads(out var minWorkers, out _);

            var cappedWorkers = Math.Max(Environment.ProcessorCount, minWorkers);
            var sessionCount = cappedWorkers * 2;
            const string LockedKey = "pendingget:locked";

            RestartServer();
            Populate();

            var clients = new RawClient[sessionCount];
            RawClient transaction = null;
            RawClient probe = null;
            var clamped = false;
            try
            {
                transaction = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(60) };
                ClassicAssert.AreEqual("+OK\r\n", transaction.Execute("SET", LockedKey, "before"));
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(60) };
                probe = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(10) };

                clamped = ThreadPool.SetMaxThreads(cappedWorkers, maxIo);
                ClassicAssert.IsTrue(clamped, "Could not clamp the worker pool, so this test proves nothing.");

                deviceFactoryCreator.ReadDelayMs = 600_000;

                transaction.SendPipeline(["MULTI"], ["SET", LockedKey, "after"], ["GET", Key(0)], ["EXEC"]);
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > 0, TimeSpan.FromSeconds(30)),
                    "The transaction's GET never reached the device.");

                foreach (var client in clients)
                    client.Send("GET", LockedKey);

                // Long enough for every pool thread to have picked up a waiting GET
                Thread.Sleep(1000);

                try
                {
                    ClassicAssert.AreEqual("+PONG\r\n", probe.Execute("PING"));
                }
                catch (IOException e)
                {
                    Assert.Fail($"No thread served a PING while {sessionCount} commands waited, against {cappedWorkers} " +
                        $"worker threads, for a lock held across a device read. {e.Message}");
                }

                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();

                ClassicAssert.AreEqual("+OK\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual("+QUEUED\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual("+QUEUED\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual($"*2\r\n+OK\r\n${ValueLength}\r\n{Value(0)}\r\n", transaction.ReadReply());
                for (var i = 0; i < sessionCount; i++)
                    ClassicAssert.AreEqual("$5\r\nafter\r\n", clients[i].ReadReply(), $"Wrong reply for session {i}");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();
                if (clamped)
                    _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
                foreach (var c in clients)
                    c?.Dispose();
                transaction?.Dispose();
                probe?.Dispose();
            }
        }

        /// <summary>
        /// On a server that processes client input on the socket event threads, a flush, which waits on the store's log as
        /// it resets it, moves to the thread pool whether or not the store has spilled to the device, while a command that
        /// waits on nothing does not.
        /// </summary>
        [Test]
        public void AFlushLeavesAnEventThread()
        {
            RestartServer(inlineCompletions: true);

            using var client = new RawClient();
            Settle(client);

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SET", "k", "v"));
            ClassicAssert.AreEqual("$1\r\nv\r\n", client.Execute("GET", "k"));
            ClassicAssert.AreEqual(0, Offloads, "A command that waits on nothing was moved to the thread pool.");

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("FLUSHDB"));
            ClassicAssert.AreEqual(1, Offloads, "FLUSHDB ran on the event thread that received it.");
            ClassicAssert.AreEqual("$-1\r\n", client.Execute("GET", "k"));
        }

        /// <summary>
        /// On a server that processes client input on the socket event threads, selecting a database for the first time,
        /// which creates it with its log and device, moves to the thread pool; selecting one that exists does not.
        /// </summary>
        [Test]
        public void SelectingANewDatabaseLeavesAnEventThread()
        {
            RestartServer(inlineCompletions: true);

            using var client = new RawClient();
            Settle(client);

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SELECT", "0"));
            ClassicAssert.AreEqual(0, Offloads, "Selecting the current database was moved to the thread pool.");

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SELECT", "1"));
            ClassicAssert.AreEqual(1, Offloads, "Creating a database ran on the event thread that received it.");

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SET", "k", "v"));
            ClassicAssert.AreEqual("$1\r\nv\r\n", client.Execute("GET", "k"));
        }

        /// <summary>
        /// On a server that processes client input on the socket event threads, of the <c>DEBUG</c> subcommands only the
        /// one that waits for the log to flush moves to the thread pool; the blocking ones park the session themselves.
        /// </summary>
        [Test]
        public void OnlyADebugSubcommandThatWaitsForTheDeviceLeavesAnEventThread()
        {
            RestartServer(inlineCompletions: true);

            using var client = new RawClient();
            Settle(client);

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("DEBUG", "LOG", "x"));
            ClassicAssert.AreEqual("+OK\r\n", client.Execute("DEBUG", "BLOCK", "0"));
            ClassicAssert.AreEqual(0, Offloads, "A DEBUG subcommand that waits on nothing in place was moved to the thread pool.");

            Settle(client);
            StringAssert.StartsWith("+OK head=", client.Execute("DEBUG", "FLUSHANDEVICT"));
            ClassicAssert.AreEqual(1, Offloads, "DEBUG FLUSHANDEVICT ran on the event thread that received it.");
        }

        /// <summary>
        /// Parking on a device read must not allocate an async state machine per read, or a
        /// larger-than-memory workload pays for a garbage collection on every miss.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Measured as the gap between a <c>GET</c> that goes to the device and one served from memory, so
        /// the client, the parser and the reply path cancel out. What is left is the pending machinery -
        /// the completed-output iterator and the pooled buffer the completion lands in, both of which the
        /// blocking implementation pays for too - plus the suspension, which is the part under test.
        /// </para>
        /// <para>
        /// A park costs two things: the waiter it blocks on, and an async state machine for every frame it
        /// suspends through. Neither is allocated here. The waiter is reused - Tsavorite's <c>AsyncQueue</c>
        /// serves an uncancellable wait from a single-waiter <c>IValueTaskSource</c> instead of allocating a
        /// semaphore waiter node - and the frames between the session and that wait were replaced by a pooled
        /// driver that is itself the <c>IThreadPoolWorkItem</c> the queue resumes, so the park produces no
        /// task, no state machine and no thread-pool callback wrapper.
        /// </para>
        /// <para>
        /// Calibration, all from Linux, which is where the bound is enforced. Both the floor and the spread
        /// depend on the runtime and on what else the machine is doing, so the bound is set from the loaded
        /// numbers, which are the ones CI reproduces.
        /// </para>
        /// <list type="table">
        /// <listheader><term>variant</term><description>net10.0</description></listheader>
        /// <item><term>as written, machine busy</term><description>23-34 B</description></item>
        /// <item><term>as written, machine idle</term><description>-29 to -43 B</description></item>
        /// <item><term>driver parked on the <c>ValueTask</c> wait instead of registering itself as a work
        /// item, so completing a plain delegate makes the thread pool box it; idle</term>
        /// <description>-4 to 2 B</description></item>
        /// <item><term>the same, busy</term><description>53-65 B</description></item>
        /// <item><term>driver replaced by the two compiler-generated async frames it subsumes; busy</term>
        /// <description>52-74 B</description></item>
        /// <item><term>uncancellable wait routed back through the semaphore; busy</term>
        /// <description>153-172 B</description></item>
        /// </list>
        /// <para>
        /// The whole table shifts by tens of bytes with load, because load changes how often a receive
        /// completes synchronously, and on an idle machine that leaves the device read below the memory read.
        /// What does not shift is the gap between a row and its regression, roughly 30 B either way. The
        /// bounds are therefore set from the busy rows and are slack when run idle; the sharp, load-free
        /// guarantee for the part that used to dominate this number lives in
        /// <see cref="ConcurrentParksReuseTheirConnectionBoxes"/>, which counts boxes instead of weighing them.
        /// </para>
        /// <para>
        /// This measures the server and nothing else, which is why
        /// <see cref="DelayedReadDeviceFactoryCreator"/> wraps a read without allocating. An earlier wrapper
        /// captured the callback in a closure and put 202 B per read - two closures and two delegates of its
        /// own - inside the number, nearly doubling it and leaving the bound too slack to catch much.
        /// </para>
        /// </remarks>
        [Test]
        public void SuspendingOnADiskReadReusesItsWaiterAndStateMachines()
        {
#if DEBUG
            // Roslyn emits an async state machine as a class in Debug and as a struct in Release, so in Debug
            // every park heap-allocates its state machine at the call site, before the pooled builder runs.
            Assert.Ignore("Async state machines are classes in DEBUG builds, so a park always allocates one.");
#else
            Populate();

            using var client = new RawClient();

            var diskKey = Key(0);
            var memoryKey = Key(PopulatedKeys - 1);
            var diskExpected = $"${ValueLength}\r\n{Value(0)}\r\n";
            var memoryExpected = $"${ValueLength}\r\n{Value(PopulatedKeys - 1)}\r\n";

            // Warm every pool: the state-machine boxes, the completion sources, the buffers.
            for (var i = 0; i < 1024; i++)
            {
                ClassicAssert.AreEqual(diskExpected, client.Execute("GET", diskKey));
                ClassicAssert.AreEqual(memoryExpected, client.Execute("GET", memoryKey));
            }

            var readsBefore = deviceFactoryCreator.ReadCount;

            const int Iterations = 20_000;
            var fromMemory = MeasurePerOp(Iterations, () => client.Execute("GET", memoryKey));
            var fromDisk = MeasurePerOp(Iterations, () => client.Execute("GET", diskKey));

            ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore, Iterations,
                "The 'disk' key stopped going to the device part way through, so this measured nothing.");

            var perRead = fromDisk - fromMemory;
            TestContext.Out.WriteLine($"disk={fromDisk:F0} B  memory={fromMemory:F0} B  per disk read={perRead:F0} B");

            // Asserted only where the bound was calibrated, against the busy rows of the table above, because
            // those are what a shared CI runner reproduces. Detection power is unaffected on other platforms:
            // a regression is a regression everywhere, so the Linux legs catch it, and elsewhere the value is
            // still printed, which is what a future calibration would start from.
            if (!OperatingSystem.IsLinux())
                return;

            const int Bound = 45;
            ClassicAssert.Less(perRead, Bound,
                "A park on the pending-read path allocates more than it should: the reusable waiter was bypassed, the pooled driver was replaced by async frames, or the driver stopped parking as a work item.");
#endif
        }


        /// <summary>
        /// A connection must keep reusing one state machine box across parks, however the thread pool happens
        /// to schedule the resumes.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Counted rather than weighed, because the weight of a box is lost in the noise of a round trip. The
        /// count is also the only way to see the failure this guards: a box pooled per thread is rented on the
        /// thread that suspends a park and released on the thread that resumes it, so whether it is reused at
        /// all depends on how the pool happens to hand the resume out. On an idle machine the two threads are
        /// usually the same and a per-thread pool looks perfect; add concurrency and it misses, which is
        /// exactly when the allocation is least affordable.
        /// </para>
        /// <para>
        /// Each round trip is a park of its own - the connection suspends, resumes, replies, and only then
        /// receives the next command - so this performs one rent and one release per round, and a box held
        /// per connection makes every one of them a hit.
        /// </para>
        /// </remarks>
        [Test]
        public void ConcurrentParksReuseTheirConnectionBoxes()
        {
            Populate();

            const int Connections = 32;
            const int RoundsPerConnection = 64;

            var clients = new RawClient[Connections];
            var failures = new string[Connections];

            try
            {
                for (var i = 0; i < Connections; i++)
                    clients[i] = new RawClient();

                // Give each connection its own key, far enough apart to stay below the head address, so no
                // two connections race onto one record and turn a device read into a memory hit.
                string KeyFor(int c) => Key(c * 8);
                string ExpectedFor(int c) => $"${ValueLength}\r\n{Value(c * 8)}\r\n";

                // Warm up: every connection parks once, allocating the single box it then reuses.
                for (var i = 0; i < Connections; i++)
                    ClassicAssert.AreEqual(ExpectedFor(i), clients[i].Execute("GET", KeyFor(i)));

                var readsBefore = deviceFactoryCreator.ReadCount;
                var allocatedBefore = Interlocked.Read(ref Garnet.common.FireAndForgetBox.Allocations);

                // Dedicated threads, so the measured phase does not depend on the pool having a thread free
                // for a blocking client at the moment the server needs one for a resume.
                var start = new ManualResetEventSlim(false);
                var threads = new Thread[Connections];
                for (var i = 0; i < Connections; i++)
                {
                    var index = i;
                    threads[i] = new Thread(() =>
                    {
                        try
                        {
                            start.Wait();
                            for (var r = 0; r < RoundsPerConnection; r++)
                            {
                                var reply = clients[index].Execute("GET", KeyFor(index));
                                if (reply != ExpectedFor(index))
                                {
                                    failures[index] = $"Connection {index} round {r} read {reply}";
                                    return;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            failures[index] = $"Connection {index} threw {ex.GetType().Name}: {ex.Message}";
                        }
                    })
                    { IsBackground = true };
                    threads[i].Start();
                }

                start.Set();
                foreach (var t in threads)
                {
                    if (!t.Join(TimeSpan.FromSeconds(120)))
                        Assert.Fail("A connection did not finish its rounds within the timeout.");
                }

                foreach (var failure in failures)
                {
                    if (failure is not null)
                        Assert.Fail(failure);
                }

                var allocated = Interlocked.Read(ref Garnet.common.FireAndForgetBox.Allocations) - allocatedBefore;
                TestContext.Out.WriteLine(
                    $"{Connections * RoundsPerConnection} parks allocated {allocated} boxes");

                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore,
                    Connections * RoundsPerConnection,
                    "The keys stopped going to the device part way through, so this measured nothing.");

                // Not zero, because releasing a box trails the receive that starts the next park: the body
                // arms the next receive and only then returns, so a connection whose next command is already
                // waiting can rent before the release lands and correctly take a fresh box rather than one
                // still being cleared. That loses a handful of handoffs, never the pooling itself, which is
                // what the gap between this bound and a per-thread pool's thousands measures.
                ClassicAssert.LessOrEqual(allocated, Connections,
                    $"{Connections * RoundsPerConnection} parks across {Connections} connections allocated " +
                    $"{allocated} state machine boxes, so a park is no longer reusing the box its connection " +
                    "holds.");
            }
            finally
            {
                foreach (var c in clients)
                    c?.Dispose();
            }
        }

        static double MeasurePerOp(int iterations, Action op)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            var before = GC.GetTotalAllocatedBytes(precise: true);
            for (var i = 0; i < iterations; i++)
                op();

            return (GC.GetTotalAllocatedBytes(precise: true) - before) / (double)iterations;
        }

        /// <summary>
        /// A <c>GET</c> against a key of the wrong type answers the same way whether or not the batch shape
        /// routes it through the scatter-gather path. <c>NextCommandMaybeGet</c> picks that path whenever
        /// another <c>GET</c> is already in the buffer behind this one, so the two differ only by pipelining.
        /// </summary>
        [Test]
        public void AWrongTypeGetAnswersTheSameWhetherOrNotItIsBatched()
        {
            const string ListKey = "wrongtype-list";
            const string StringKey = "wrongtype-string";
            const string StringValue = "value";

            using var client = new RawClient();

            ClassicAssert.AreEqual(":1\r\n", client.Execute("LPUSH", ListKey, "item"));
            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SET", StringKey, StringValue));

            // Not followed by a GET, so this takes the single-read path.
            var alone = client.Execute("GET", ListKey);

            // Followed by a GET, so this takes the scatter-gather path.
            client.SendPipeline(["GET", ListKey], ["GET", StringKey]);
            var batched = client.ReadReply();
            var trailing = client.ReadReply();

            ClassicAssert.IsTrue(alone.StartsWith("-WRONGTYPE", StringComparison.Ordinal),
                $"Unbatched GET of a list answered {alone}");
            ClassicAssert.AreEqual(alone, batched,
                "A batched GET of a list answered differently from an unbatched one");
            ClassicAssert.AreEqual($"${StringValue.Length}\r\n{StringValue}\r\n", trailing,
                "The GET behind the wrong-type one lost its reply");
        }

        /// <summary>Hash <paramref name="i"/>, read back after it has left memory.</summary>
        static string HashKey(int i) => $"pendinghash:{i:D6}";

        /// <summary>Sorted set <paramref name="i"/>, read back after it has left memory.</summary>
        static string ZsetKey(int i) => $"pendingzset:{i:D6}";

        /// <summary>List <paramref name="i"/>, read back after it has left memory.</summary>
        static string ListKey(int i) => $"pendinglist:{i:D6}";

        /// <summary>Set <paramref name="i"/>, read back after it has left memory.</summary>
        static string SetKey(int i) => $"pendingset:{i:D6}";

        /// <summary>Fields written into every hash, and members into every sorted set.</summary>
        const int ObjectFields = 3;

        /// <summary>
        /// Writes enough of every object type that the earliest of them are no longer in memory, so
        /// reading them back has to go to the device.
        /// </summary>
        void PopulateObjects()
        {
            using var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var db = redis.GetDatabase(0);

            var batch = db.CreateBatch();
            Task last = null;
            for (var i = 0; i < PopulatedObjects; i++)
            {
                var entries = new HashEntry[ObjectFields];
                var members = new SortedSetEntry[ObjectFields];
                var values = new RedisValue[ObjectFields];
                for (var f = 0; f < ObjectFields; f++)
                {
                    entries[f] = new HashEntry($"f{f}", $"{i}-{f}");
                    members[f] = new SortedSetEntry($"m{f}", (i * ObjectFields) + f);
                    values[f] = $"m{f}";
                }

                _ = batch.HashSetAsync(HashKey(i), entries);
                _ = batch.ListRightPushAsync(ListKey(i), values);
                _ = batch.SetAddAsync(SetKey(i), values);
                last = batch.SortedSetAddAsync(ZsetKey(i), members);
            }
            batch.Execute();

            last.Wait(TimeSpan.FromSeconds(60));

            deviceFactoryCreator.ResetReadCount();
        }

        /// <summary>Objects of each type written by <see cref="PopulateObjects"/>.</summary>
        const int PopulatedObjects = 4096;

        /// <summary>
        /// The object-store reads answer the same whether their record is in memory or on the device. Each
        /// is asked of a record that has left memory and of an identical one that has not, and the two
        /// replies have to match byte for byte.
        /// </summary>
        /// <remarks>
        /// The control is written last, so it is still in memory, and is given the same key length and the
        /// same contents as the key under test.
        /// </remarks>
        [Test]
        public void ObjectReadsAnswerFromTheDeviceExactlyAsTheyDoFromMemory()
        {
            PopulateObjects();

            using var client = new RawClient();

            // The first read of a record copies it back into memory, so every comparison takes an object
            // that has not been read yet. Its twin is written here and so is still in memory.
            var next = 0;

            void AssertSameAcrossASuspension(string type, params string[] rest)
            {
                var i = ++next;
                var onDevice = $"pending{type}:{i:D6}";
                var inMemory = $"pending{type}:9{i:D5}";

                for (var f = 0; f < ObjectFields; f++)
                {
                    var score = ((i * ObjectFields) + f).ToString(CultureInfo.InvariantCulture);
                    _ = type switch
                    {
                        "hash" => client.Execute("HSET", inMemory, $"f{f}", $"{i}-{f}"),
                        "zset" => client.Execute("ZADD", inMemory, score, $"m{f}"),
                        "list" => client.Execute("RPUSH", inMemory, $"m{f}"),
                        _ => client.Execute("SADD", inMemory, $"m{f}"),
                    };
                }

                var verb = rest[0];
                string[] ArgsFor(string key) => [verb, key, .. rest[1..]];

                var fromMemory = client.Execute(ArgsFor(inMemory));

                var readsBefore = deviceFactoryCreator.ReadCount;
                var fromDevice = client.Execute(ArgsFor(onDevice));

                ClassicAssert.Greater(deviceFactoryCreator.ReadCount, readsBefore,
                    $"{verb} {onDevice} never went to the device, so it proves nothing.");
                ClassicAssert.AreEqual(fromMemory, fromDevice,
                    $"{verb} answered differently across a suspension.");
            }

            void Hash(params string[] rest) => AssertSameAcrossASuspension("hash", rest);
            void Zset(params string[] rest) => AssertSameAcrossASuspension("zset", rest);
            void List(params string[] rest) => AssertSameAcrossASuspension("list", rest);
            void Set(params string[] rest) => AssertSameAcrossASuspension("set", rest);

            // Hits, misses and the multi-field shapes, whose miss reply is as long as the argument list.
            Hash("HGET", "f0");
            Hash("HGET", "absent");
            Hash("HGETALL");
            Hash("HMGET", "f0", "f1", "f2");
            Hash("HMGET", "f0", "absent");
            Hash("HLEN");
            Hash("HSTRLEN", "f0");
            Hash("HSTRLEN", "absent");
            Hash("HEXISTS", "f0");
            Hash("HEXISTS", "absent");
            Hash("HKEYS");
            Hash("HVALS");
            Hash("HTTL", "FIELDS", "2", "f0", "absent");

            Zset("ZSCORE", "m0");
            Zset("ZSCORE", "absent");
            Zset("ZMSCORE", "m0", "m1", "m2");
            Zset("ZMSCORE", "m0", "absent");
            Zset("ZRANK", "m1");
            Zset("ZRANK", "absent");
            Zset("ZCOUNT", "-inf", "+inf");
            Zset("ZRANGE", "0", "-1");
            Zset("ZRANGE", "0", "-1", "WITHSCORES");
            Zset("ZCARD");
            Zset("ZTTL", "MEMBERS", "2", "m0", "absent");

            // GEODIST and GEOPOS read the sorted set the GEO commands are built on.
            Zset("GEODIST", "m0", "m1");
            Zset("GEOPOS", "m0", "m1");

            List("LLEN");
            List("LRANGE", "0", "-1");
            List("LINDEX", "0");
            List("LINDEX", "99");
            List("LPOS", "m1");
            List("LPOS", "absent");
            List("LPOS", "m1", "COUNT", "0");

            Set("SCARD");
            Set("SMEMBERS");
            Set("SISMEMBER", "m0");
            Set("SISMEMBER", "absent");
            Set("SMISMEMBER", "m0", "absent");

            // A count of zero answers without reading the key at all, so it must not reach the device.
            foreach (var argv in new[] { new[] { "HRANDFIELD", HashKey(0), "0" }, ["ZRANDMEMBER", ZsetKey(0), "0"], ["SRANDMEMBER", SetKey(0), "0"] })
            {
                var readsBefore = deviceFactoryCreator.ReadCount;
                ClassicAssert.AreEqual("*0\r\n", client.Execute(argv));
                ClassicAssert.AreEqual(readsBefore, deviceFactoryCreator.ReadCount,
                    $"{argv[0]} with a count of zero read the device.");
            }

            // A read of a key that was never written, which has to answer the miss reply rather than wait.
            ClassicAssert.AreEqual("$-1\r\n", client.Execute("HGET", "pendinghash:absent", "f0"));
            ClassicAssert.AreEqual("*0\r\n", client.Execute("HGETALL", "pendinghash:absent"));
            ClassicAssert.AreEqual(":0\r\n", client.Execute("ZCOUNT", "pendingzset:absent", "-inf", "+inf"));
            ClassicAssert.AreEqual(":0\r\n", client.Execute("SCARD", "pendingset:absent"));
            ClassicAssert.AreEqual("$-1\r\n", client.Execute("LINDEX", "pendinglist:absent", "0"));
        }

        /// <summary>
        /// An object read inside a running transaction parks for the device like any other read, and still
        /// returns the object's contents once the transaction resumes.
        /// </summary>
        [Test]
        public void ObjectReadsInsideATransactionStillCompleteFromTheDevice()
        {
            PopulateObjects();

            using var client = new RawClient();

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("MULTI"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("HGET", HashKey(2), "f0"));
            ClassicAssert.AreEqual("+QUEUED\r\n", client.Execute("ZSCORE", ZsetKey(2), "m0"));

            var readsBefore = deviceFactoryCreator.ReadCount;
            var exec = client.Execute("EXEC");

            ClassicAssert.Greater(deviceFactoryCreator.ReadCount, readsBefore,
                "The transaction never went to the device, so it proves nothing.");
            ClassicAssert.AreEqual("*2\r\n$3\r\n2-0\r\n$1\r\n6\r\n", exec,
                "A transaction lost or reordered the replies of its object reads.");
        }

        /// <summary>
        /// An object that has left memory takes two device reads to bring back: its record from the main log, then
        /// its serialized form from the object log. A session waiting on the second holds no thread either. The pool
        /// is cut to a single free worker, which a wait in place on the object log would hold, leaving another
        /// session's PING no thread to run on.
        /// </summary>
        [Test]
        public void AnObjectLogReadDoesNotStallOtherSessions()
        {
            // Linux only, for the reason given on the single-read test above.
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");

            // Longer than the test waits, so a read is held until it is released.
            const int HeldMs = 600_000;
            var stallLimit = TimeSpan.FromSeconds(5);

            PopulateObjects();

            using var reader = new RawClient();
            using var pinger = new RawClient();

            // Warm both sessions so neither measurement includes a first-command cost.
            ClassicAssert.AreEqual("+PONG\r\n", reader.Execute("PING"));
            ClassicAssert.AreEqual("+PONG\r\n", pinger.Execute("PING"));

            using (ThreadPoolCap.ApplyFree(1))
            {
                try
                {
                    deviceFactoryCreator.ReadDelayMs = HeldMs;
                    reader.Send("HGET", HashKey(7), "f0");

                    ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > 0, stallLimit),
                        "The HGET never read its record from the main log.");
                    ClassicAssert.AreEqual(0, deviceFactoryCreator.ObjectLogReadCount,
                        "The HGET read the object log before its record.");

                    // Release the main log read only: the object log read it leads to is held. A read is held once the device
                    // completes it, which can be after it is counted, and one released before then would be held in turn.
                    ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadsHeld > 0, stallLimit),
                        "The HGET's main log read was never held.");
                    deviceFactoryCreator.CompleteHeldReads();
                    ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ObjectLogReadCount > 0, stallLimit),
                        "The HGET never read its object from the object log, so the test proves nothing.");

                    pinger.ReceiveTimeout = stallLimit;
                    pinger.Send("PING");
                    try
                    {
                        ClassicAssert.AreEqual("+PONG\r\n", pinger.ReadReply());
                    }
                    catch (IOException e)
                    {
                        Assert.Fail($"PING was not answered within {stallLimit.TotalSeconds:F0}s while another session " +
                            "was reading the object log against a pool of one thread, which indicates the read was " +
                            $"holding it. {e.Message}");
                    }

                    ClassicAssert.IsFalse(reader.HasReply, "The HGET answered before its object log read completed.");
                }
                finally
                {
                    deviceFactoryCreator.ReadDelayMs = 0;
                    deviceFactoryCreator.CompleteHeldReads();
                }

                ClassicAssert.AreEqual("$3\r\n7-0\r\n", reader.ReadReply());
            }
        }

        /// <summary>Objects of each type written to expire before <see cref="PopulateObjects"/> pushes them to the device.</summary>
        const int ExpiredObjects = 32;

        /// <summary>Object <paramref name="i"/> of <paramref name="type"/>, which has expired and left memory.</summary>
        static string ExpiredKey(string type, int i) => $"pendinggone{type}:{i:D6}";

        /// <summary>
        /// Writes <see cref="ExpiredObjects"/> objects of every type and expires them at once. Written before
        /// <see cref="PopulateObjects"/>, they leave memory, so an update of one finds an expired record on the device.
        /// </summary>
        static void WriteExpiredObjects(RawClient client)
        {
            for (var i = 0; i < ExpiredObjects; i++)
            {
                ClassicAssert.AreEqual(":1\r\n", client.Execute("HSET", ExpiredKey("hash", i), "f0", "v"));
                ClassicAssert.AreEqual(":1\r\n", client.Execute("RPUSH", ExpiredKey("list", i), "m0"));
                ClassicAssert.AreEqual(":1\r\n", client.Execute("SADD", ExpiredKey("set", i), "m0"));
                ClassicAssert.AreEqual(":1\r\n", client.Execute("ZADD", ExpiredKey("zset", i), "0", "m0"));
                foreach (var type in new[] { "hash", "list", "set", "zset" })
                    ClassicAssert.AreEqual(":1\r\n", client.Execute("PEXPIRE", ExpiredKey(type, i), "1"));
            }
        }

        /// <summary>
        /// The object-store updates park where they are received when their record is on the device, and answer, and
        /// leave the object, exactly as they do when it is in memory.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Each update is applied to a record that has left memory and to an identical one that has not, and the
        /// replies, and then the objects, have to match byte for byte. A miss is an update of an object that expired
        /// on the device, matched against one never written.
        /// </para>
        /// <para>
        /// On a server that hands client input to the thread pool, the device read is held while another session's PING
        /// runs against a pool of one free thread, which an update waiting for the device in place would hold. On one
        /// that processes it on the socket event threads, the read is issued off the thread pool, so the update was not
        /// moved to the thread pool to wait there.
        /// </para>
        /// </remarks>
        [Test]
        public void ObjectUpdatesFromTheDeviceParkWhereReceivedAndAnswerAsFromMemory([Values] bool inlineCompletions)
        {
            // Longer than the test waits, so a read is held until it is released.
            const int HeldMs = 600_000;
            var stallLimit = TimeSpan.FromSeconds(5);

            // Log memory that is flushed past the head, so no update has to wait to allocate, which would move it to
            // the thread pool
            RestartServer(inlineCompletions, SpilledLogMemorySize);

            using var client = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };
            using var pinger = new RawClient { ReceiveTimeout = stallLimit };

            WriteExpiredObjects(client);
            PopulateObjects();
            WaitUntilAllocationNeedNotWaitForFlush("The log has to wait for a flush to add a record, so an update could be moved for that alone.");

            ClassicAssert.AreEqual("+PONG\r\n", pinger.Execute("PING"));

            // Starving the worker pool only constrains session dispatch on Linux, for the reason given on the single-read test.
            using var cap = !inlineCompletions && OperatingSystem.IsLinux() ? ThreadPoolCap.ApplyFree(1) : null;

            var inSeconds = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);
            var inMilliseconds = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

            // Every update takes an object that has not been touched yet: the first update of a record brings it into memory
            var hits = new Dictionary<string, int>();
            var misses = new Dictionary<string, int>();

            static int Next(Dictionary<string, int> taken, string type)
            {
                _ = taken.TryGetValue(type, out var i);
                taken[type] = i + 1;
                return i;
            }

            string[] StateOf(string type, string key) => type switch
            {
                "hash" => ["HGETALL", key],
                "list" => ["LRANGE", key, "0", "-1"],
                "zset" => ["ZRANGE", key, "0", "-1", "WITHSCORES"],
                _ => ["SMEMBERS", key],
            };

            void AssertSameAcrossASuspension(string type, bool miss, string[] rest, Func<string, string> canonical = null, string[] state = null)
            {
                string onDevice, inMemory;
                if (miss)
                {
                    var i = Next(misses, type);
                    ClassicAssert.Less(i, ExpiredObjects, $"More misses of {type} than objects written to expire.");
                    onDevice = ExpiredKey(type, i);
                    inMemory = $"pendingnever{type}:{i:D6}";
                }
                else
                {
                    var i = Next(hits, type) + 1;
                    onDevice = $"pending{type}:{i:D6}";
                    inMemory = $"pending{type}:9{i:D5}";

                    for (var f = 0; f < ObjectFields; f++)
                    {
                        var score = ((i * ObjectFields) + f).ToString(CultureInfo.InvariantCulture);
                        _ = type switch
                        {
                            "hash" => client.Execute("HSET", inMemory, $"f{f}", $"{i}-{f}"),
                            "zset" => client.Execute("ZADD", inMemory, score, $"m{f}"),
                            "list" => client.Execute("RPUSH", inMemory, $"m{f}"),
                            _ => client.Execute("SADD", inMemory, $"m{f}"),
                        };
                    }
                }

                var verb = rest[0];
                string[] ArgsFor(string key) => [verb, key, .. rest[1..]];
                string[] StateFor(string key) => state is null ? StateOf(type, key) : [state[0], key, .. state[1..]];
                canonical ??= static reply => reply;

                var fromMemory = client.Execute(ArgsFor(inMemory));
                var memoryState = client.Execute(StateFor(inMemory));

                // The receive loop is left on the thread that completed the last device read until the server waits for
                // more input, so the command reaches the server through an event thread once it does
                if (inlineCompletions)
                    Thread.Sleep(50);

                var readsBefore = deviceFactoryCreator.ReadCount;
                var offThreadPoolBefore = deviceFactoryCreator.ReadsOffThreadPool;
                deviceFactoryCreator.ReadDelayMs = HeldMs;
                try
                {
                    client.Send(ArgsFor(onDevice));
                    ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > readsBefore, stallLimit),
                        $"{verb} {onDevice} never went to the device, so it proves nothing.");

                    if (cap is not null)
                    {
                        pinger.Send("PING");
                        try
                        {
                            ClassicAssert.AreEqual("+PONG\r\n", pinger.ReadReply());
                        }
                        catch (IOException e)
                        {
                            Assert.Fail($"PING was not answered within {stallLimit.TotalSeconds:F0}s while {verb} waited for the " +
                                $"device against a pool of one thread, so {verb} was holding it. {e.Message}");
                        }
                    }

                    ClassicAssert.IsFalse(client.HasReply, $"{verb} answered before its device read completed.");
                }
                finally
                {
                    deviceFactoryCreator.ReadDelayMs = 0;
                    deviceFactoryCreator.CompleteHeldReads();
                }

                var fromDevice = client.ReadReply();

                if (inlineCompletions)
                {
                    ClassicAssert.Greater(deviceFactoryCreator.ReadsOffThreadPool, offThreadPoolBefore,
                        $"{verb} was moved to the thread pool to wait for the device, rather than parking where it was received.");
                }
                ClassicAssert.AreEqual(canonical(fromMemory), canonical(fromDevice),
                    $"{verb} {(miss ? "of an expired object " : "")}answered differently across a suspension.");
                ClassicAssert.AreEqual(memoryState, client.Execute(StateFor(onDevice)),
                    $"{verb} {(miss ? "of an expired object " : "")}left the object different across a suspension.");
            }

            void Hash(params string[] rest) => AssertSameAcrossASuspension("hash", miss: false, rest);
            void List(params string[] rest) => AssertSameAcrossASuspension("list", miss: false, rest);
            void Set(params string[] rest) => AssertSameAcrossASuspension("set", miss: false, rest);
            void Zset(params string[] rest) => AssertSameAcrossASuspension("zset", miss: false, rest);

            void HashMiss(params string[] rest) => AssertSameAcrossASuspension("hash", miss: true, rest);
            void ListMiss(params string[] rest) => AssertSameAcrossASuspension("list", miss: true, rest);
            void SetMiss(params string[] rest) => AssertSameAcrossASuspension("set", miss: true, rest);
            void ZsetMiss(params string[] rest) => AssertSameAcrossASuspension("zset", miss: true, rest);

            // SPOP picks its members at random, so only how many it popped can match
            static string AnyMember(string reply) => reply.Replace("m0", "m?").Replace("m1", "m?").Replace("m2", "m?");
            string[] count = ["SCARD"];

            Hash("HSET", "f0", "x");
            Hash("HSET", "f9", "x");
            Hash("HSETNX", "f0", "x");
            Hash("HSETNX", "f9", "x");
            Hash("HMSET", "f0", "x", "f9", "y");
            Hash("HDEL", "f0", "absent");
            Hash("HINCRBY", "n", "5");
            Hash("HINCRBY", "f0", "1");
            Hash("HINCRBYFLOAT", "n", "1.5");
            Hash("HEXPIRE", "100", "FIELDS", "2", "f0", "absent");
            Hash("HPEXPIRE", "100000", "FIELDS", "1", "f1");
            Hash("HEXPIREAT", inSeconds, "FIELDS", "1", "f0");
            Hash("HPEXPIREAT", inMilliseconds, "FIELDS", "1", "f0");
            Hash("HPERSIST", "FIELDS", "2", "f0", "absent");

            List("LPUSH", "x");
            List("LPUSHX", "x");
            List("RPUSH", "x", "y");
            List("RPUSHX", "x");
            List("LPOP");
            List("LPOP", "2");
            List("RPOP");
            List("RPOP", "5");
            List("LTRIM", "1", "-1");
            List("LTRIM", "5", "9");
            List("LINSERT", "BEFORE", "m1", "x");
            List("LINSERT", "AFTER", "absent", "x");
            List("LREM", "0", "m1");
            List("LREM", "1", "absent");
            List("LSET", "1", "x");
            List("LSET", "9", "x");

            Set("SADD", "m0", "x");
            Set("SREM", "m0", "absent");
            AssertSameAcrossASuspension("set", miss: false, ["SPOP"], AnyMember, count);
            AssertSameAcrossASuspension("set", miss: false, ["SPOP", "2"], AnyMember, count);
            AssertSameAcrossASuspension("set", miss: false, ["SPOP", "5"], AnyMember, count);

            Zset("ZADD", "100", "x");
            Zset("ZADD", "XX", "CH", "100", "m0");
            Zset("ZADD", "INCR", "5", "m0");
            Zset("ZADD", "NX", "1", "m0");
            Zset("ZREM", "m0", "absent");
            Zset("ZPOPMIN");
            Zset("ZPOPMAX", "2");
            Zset("ZINCRBY", "5", "m1");
            Zset("ZINCRBY", "5", "x");
            Zset("ZREMRANGEBYLEX", "[m0", "[m1");
            Zset("ZREMRANGEBYLEX", "x", "y");
            Zset("ZLEXCOUNT", "-", "+");
            Zset("ZLEXCOUNT", "x", "y");
            Zset("ZREMRANGEBYRANK", "0", "0");
            Zset("ZREMRANGEBYSCORE", "-inf", "+inf");
            Zset("ZEXPIRE", "100", "MEMBERS", "2", "m0", "absent");
            Zset("ZPEXPIRE", "100000", "MEMBERS", "1", "m1");
            Zset("ZEXPIREAT", inSeconds, "MEMBERS", "1", "m0");
            Zset("ZPEXPIREAT", inMilliseconds, "MEMBERS", "1", "m0");
            Zset("ZPERSIST", "MEMBERS", "2", "m0", "absent");
            Zset("GEOADD", "13.361389", "38.115556", "palermo");

            HashMiss("HSET", "f0", "x");
            HashMiss("HSETNX", "f0", "x");
            HashMiss("HMSET", "f0", "x");
            HashMiss("HDEL", "f0");
            HashMiss("HINCRBY", "n", "5");
            HashMiss("HINCRBYFLOAT", "n", "1.5");
            HashMiss("HEXPIRE", "100", "FIELDS", "2", "f0", "absent");
            HashMiss("HPEXPIRE", "100000", "FIELDS", "1", "f0");
            HashMiss("HEXPIREAT", inSeconds, "FIELDS", "1", "f0");
            HashMiss("HPEXPIREAT", inMilliseconds, "FIELDS", "1", "f0");
            HashMiss("HPERSIST", "FIELDS", "2", "f0", "absent");

            ListMiss("LPUSH", "x");
            ListMiss("LPUSHX", "x");
            ListMiss("RPUSH", "x");
            ListMiss("RPUSHX", "x");
            ListMiss("LPOP");
            ListMiss("LPOP", "2");
            ListMiss("RPOP");
            ListMiss("LTRIM", "0", "1");
            ListMiss("LINSERT", "BEFORE", "m0", "x");
            ListMiss("LREM", "0", "m0");
            ListMiss("LSET", "0", "x");

            SetMiss("SADD", "x");
            SetMiss("SREM", "m0");
            SetMiss("SPOP");
            SetMiss("SPOP", "2");

            ZsetMiss("ZADD", "1", "x");
            ZsetMiss("ZADD", "INCR", "5", "x");
            ZsetMiss("ZREM", "m0");
            ZsetMiss("ZPOPMIN");
            ZsetMiss("ZPOPMAX", "2");
            ZsetMiss("ZINCRBY", "5", "x");
            ZsetMiss("ZREMRANGEBYLEX", "-", "+");
            ZsetMiss("ZLEXCOUNT", "-", "+");
            ZsetMiss("ZREMRANGEBYRANK", "0", "-1");
            ZsetMiss("ZREMRANGEBYSCORE", "-inf", "+inf");
            ZsetMiss("ZEXPIRE", "100", "MEMBERS", "2", "m0", "absent");
            ZsetMiss("ZPEXPIRE", "100000", "MEMBERS", "1", "m0");
            ZsetMiss("ZEXPIREAT", inSeconds, "MEMBERS", "1", "m0");
            ZsetMiss("ZPEXPIREAT", inMilliseconds, "MEMBERS", "1", "m0");
            ZsetMiss("ZPERSIST", "MEMBERS", "1", "m0");
            ZsetMiss("GEOADD", "13.361389", "38.115556", "palermo");
        }

        /// <summary>String records of each kind written to the device, and as many to memory.</summary>
        const int StringRecords = 40;

        /// <summary>The value of every string record that holds text.</summary>
        const string StringText = "garnet-string-value";

        /// <summary>String record <paramref name="i"/> of <paramref name="kind"/>, on the device or in memory.</summary>
        static string StringKey(string kind, bool onDevice, int i) => $"pendingstr{kind}:{(onDevice ? "device" : "memory")}:{i:D4}";

        /// <summary>
        /// Writes a record of <paramref name="kind"/> at <paramref name="key"/>: text, a number, a number with a leading
        /// zero, text that expires in an hour, text with an ETag, a hash, or text that expires at once.
        /// </summary>
        static void WriteStringRecord(RawClient client, string kind, string key)
        {
            var reply = kind switch
            {
                "text" => client.Execute("SET", key, StringText),
                "number" => client.Execute("SET", key, "10"),
                "leadingzero" => client.Execute("SET", key, "01.5"),
                "ttl" => client.Execute("SET", key, StringText, "EX", "3600"),
                "etag" => client.Execute("SETWITHETAG", key, StringText),
                "hash" => client.Execute("HSET", key, "f", "v"),
                "gone" => client.Execute("SET", key, StringText, "PX", "1"),
                _ => throw new ArgumentOutOfRangeException(nameof(kind)),
            };
            ClassicAssert.AreNotEqual('-', reply[0], $"Writing a {kind} record failed: {reply}");
        }

        /// <summary>
        /// Writes <see cref="StringRecords"/> records of every kind and then enough others that they leave memory.
        /// </summary>
        void PopulateStringRecords(RawClient client)
        {
            foreach (var kind in (string[])["text", "number", "leadingzero", "ttl", "etag", "hash", "gone"])
            {
                for (var i = 0; i < StringRecords; i++)
                    WriteStringRecord(client, kind, StringKey(kind, onDevice: true, i));
            }

            Populate();
        }

        /// <summary>
        /// The string commands other than <c>GET</c> park where they are received when their record is on the device,
        /// and answer, and leave the record, exactly as they do when it is in memory.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Each command is applied to a record that has left memory and to an identical one that has not, and the
        /// replies, and then the records, have to match byte for byte. A miss is a command on a record that expired on
        /// the device, matched against one never written, and a wrong type is a command on a hash.
        /// </para>
        /// <para>
        /// On a server that hands client input to the thread pool, the device read is held while another session's PING
        /// runs against a pool of one free thread, which a command waiting for the device in place would hold. On one
        /// that processes it on the socket event threads, the read is issued off the thread pool, so the command was not
        /// moved to the thread pool to wait there.
        /// </para>
        /// </remarks>
        [Test]
        public void StringCommandsFromTheDeviceParkWhereReceivedAndAnswerAsFromMemory([Values] bool inlineCompletions)
        {
            // Longer than the test waits, so a read is held until it is released.
            const int HeldMs = 600_000;
            var stallLimit = TimeSpan.FromSeconds(5);

            // Log memory that is flushed past the head, so no command has to wait to allocate, which would move it to
            // the thread pool
            RestartServer(inlineCompletions, SpilledLogMemorySize);

            using var client = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };
            using var pinger = new RawClient { ReceiveTimeout = stallLimit };

            WriteStringRecord(client, "text", "pendingstrdump");
            byte[] payload;
            using (var redis = ConnectionMultiplexer.Connect(TestUtils.GetConfig()))
                payload = redis.GetDatabase(0).KeyDump("pendingstrdump");

            PopulateStringRecords(client);
            WaitUntilAllocationNeedNotWaitForFlush("The log has to wait for a flush to add a record, so an update could be moved for that alone.");

            ClassicAssert.AreEqual("+PONG\r\n", pinger.Execute("PING"));

            // Starving the worker pool only constrains session dispatch on Linux, for the reason given on the single-read test.
            using var cap = !inlineCompletions && OperatingSystem.IsLinux() ? ThreadPoolCap.ApplyFree(1) : null;

            // Every command takes a record that has not been touched yet: the first update of a record brings it into memory
            var taken = new Dictionary<string, int>();

            static byte[] Bytes(object arg) => arg as byte[] ?? Encoding.UTF8.GetBytes((string)arg);

            // The value with its ETag, the type, and whether it expires: all that a string command can change
            string StateOf(string key)
            {
                var ttl = client.Execute("TTL", key);
                return client.Execute("GETWITHETAG", key) + client.Execute("TYPE", key) + (ttl is ":-1\r\n" or ":-2\r\n" ? ttl : "expires");
            }

            void AssertSameAcrossASuspension(string kind, object[] command)
            {
                _ = taken.TryGetValue(kind, out var i);
                taken[kind] = i + 1;
                ClassicAssert.Less(i, StringRecords, $"More commands on {kind} records than records written.");

                var onDevice = StringKey(kind, onDevice: true, i);
                var inMemory = StringKey(kind, onDevice: false, i);
                if (kind != "gone")
                    WriteStringRecord(client, kind, inMemory);

                var verb = (string)command[0];
                byte[][] ArgsFor(string key) => [Bytes(verb), Bytes(key), .. Array.ConvertAll(command[1..], Bytes)];

                var fromMemory = client.Execute(ArgsFor(inMemory));
                var memoryState = StateOf(inMemory);

                // The receive loop is left on the thread that completed the last device read until the server waits for
                // more input, so the command reaches the server through an event thread once it does
                if (inlineCompletions)
                    Thread.Sleep(50);

                var readsBefore = deviceFactoryCreator.ReadCount;
                var offThreadPoolBefore = deviceFactoryCreator.ReadsOffThreadPool;
                deviceFactoryCreator.ReadDelayMs = HeldMs;
                try
                {
                    client.Send(ArgsFor(onDevice));
                    ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > readsBefore, stallLimit),
                        $"{verb} of a {kind} record never went to the device, so it proves nothing.");

                    if (cap is not null)
                    {
                        pinger.Send("PING");
                        try
                        {
                            ClassicAssert.AreEqual("+PONG\r\n", pinger.ReadReply());
                        }
                        catch (IOException e)
                        {
                            Assert.Fail($"PING was not answered within {stallLimit.TotalSeconds:F0}s while {verb} waited for the " +
                                $"device against a pool of one thread, so {verb} was holding it. {e.Message}");
                        }
                    }

                    ClassicAssert.IsFalse(client.HasReply, $"{verb} answered before its device read completed.");
                }
                finally
                {
                    deviceFactoryCreator.ReadDelayMs = 0;
                    deviceFactoryCreator.CompleteHeldReads();
                }

                var fromDevice = client.ReadReply();

                if (inlineCompletions)
                {
                    ClassicAssert.Greater(deviceFactoryCreator.ReadsOffThreadPool, offThreadPoolBefore,
                        $"{verb} was moved to the thread pool to wait for the device, rather than parking where it was received.");
                }
                ClassicAssert.AreEqual(fromMemory, fromDevice, $"{verb} of a {kind} record answered differently across a suspension.");
                ClassicAssert.AreEqual(memoryState, StateOf(onDevice), $"{verb} of a {kind} record left it different across a suspension.");
            }

            void Text(params object[] command) => AssertSameAcrossASuspension("text", command);
            void Number(params object[] command) => AssertSameAcrossASuspension("number", command);
            void LeadingZero(params object[] command) => AssertSameAcrossASuspension("leadingzero", command);
            void Expiring(params object[] command) => AssertSameAcrossASuspension("ttl", command);
            void Tagged(params object[] command) => AssertSameAcrossASuspension("etag", command);
            void Hash(params object[] command) => AssertSameAcrossASuspension("hash", command);
            void Gone(params object[] command) => AssertSameAcrossASuspension("gone", command);

            Text("GETEX");
            Text("GETEX", "EX", "100");
            Expiring("GETEX", "PERSIST");
            Text("GETRANGE", "0", "3");
            Text("GETRANGE", "-5", "-1");
            Text("SUBSTR", "2", "4");
            Text("STRLEN");
            Text("GETDEL");
            Text("SETNX", "x");
            Text("SET", "x", "NX");
            Text("SET", "x", "XX");
            Text("SET", "x", "GET");
            Text("SET", "x", "XX", "GET");
            Text("SET", "x", "EX", "100", "XX");
            Expiring("SET", "x", "KEEPTTL");
            Expiring("SET", "x", "XX", "KEEPTTL");
            Text("GETSET", "x");
            Text("APPEND", "-more");
            Text("SETRANGE", "2", "xyz");
            Text("SETRANGE", "30", "x");
            Text("SETBIT", "7", "1");
            Text("SETBIT", "1", "0");
            Text("GETBIT", "1");
            Text("GETBIT", "1000");
            Text("BITCOUNT");
            Text("BITCOUNT", "0", "1");
            Text("BITPOS", "1");
            Text("BITPOS", "0");
            Text("BITPOS", "1", "100");
            Text("INCR");
            Text("INCRBYFLOAT", "1.5");
            Text("RESTORE", "0", payload);
            Text("GETWITHETAG");
            Text("GETIFNOTMATCH", "1");
            Text("SETWITHETAG", "x");
            Text("DELIFGREATER", "5");

            Number("INCR");
            Number("DECR");
            Number("INCRBY", "5");
            Number("DECRBY", "5");
            Number("INCRBYFLOAT", "1.5");
            Number("INCRBYFLOAT", "-0.25");
            LeadingZero("INCRBYFLOAT", "1.5");

            Tagged("GETWITHETAG");
            Tagged("GETIFNOTMATCH", "1");
            Tagged("GETIFNOTMATCH", "7");
            Tagged("SETIFMATCH", "x", "1");
            Tagged("SETIFMATCH", "x", "7");
            Tagged("SETIFGREATER", "x", "5");
            Tagged("SETIFGREATER", "x", "0");
            Tagged("SETWITHETAG", "x");
            Tagged("DELIFGREATER", "5");
            Tagged("DELIFGREATER", "0");

            Hash("GETEX");
            Hash("GETRANGE", "0", "3");
            Hash("STRLEN");
            Hash("GETDEL");
            Hash("SETNX", "x");
            Hash("SET", "x", "NX");
            Hash("SET", "x", "XX");
            Hash("SET", "x", "GET");
            Hash("GETSET", "x");
            Hash("INCR");
            Hash("INCRBYFLOAT", "1.5");
            Hash("APPEND", "x");
            Hash("SETRANGE", "0", "x");
            Hash("SETBIT", "7", "1");
            Hash("GETBIT", "1");
            Hash("BITCOUNT");
            Hash("BITPOS", "1");
            Hash("RESTORE", "0", payload);
            Hash("GETWITHETAG");
            Hash("SETWITHETAG", "x");
            Hash("SETIFMATCH", "x", "1");
            Hash("DELIFGREATER", "5");

            Gone("GETEX");
            Gone("GETRANGE", "0", "3");
            Gone("STRLEN");
            Gone("GETDEL");
            Gone("SETNX", "x");
            Gone("SET", "x", "NX");
            Gone("SET", "x", "XX");
            Gone("SET", "x", "EX", "100", "NX");
            Gone("SET", "x", "GET");
            Gone("GETSET", "x");
            Gone("INCR");
            Gone("INCRBY", "5");
            Gone("INCRBYFLOAT", "1.5");
            Gone("APPEND", "x");
            Gone("SETRANGE", "3", "x");
            Gone("SETBIT", "100", "1");
            Gone("GETBIT", "1");
            Gone("BITCOUNT");
            Gone("BITPOS", "1");
            Gone("BITPOS", "0");
            Gone("RESTORE", "0", payload);
            Gone("GETWITHETAG");
            Gone("GETIFNOTMATCH", "1");
            Gone("SETWITHETAG", "x");
            Gone("SETIFMATCH", "x", "1");
            Gone("SETIFGREATER", "x", "1");
            Gone("DELIFGREATER", "5");
        }

        /// <summary>
        /// The string commands that read their record from the device answer as from memory inside a transaction, which
        /// parks on each read, and inside a script, which waits each read out in place.
        /// </summary>
        /// <remarks>
        /// Each command takes a record of its own, so each one goes to the device. The same commands run against
        /// identical records in memory, and the replies, and then the records, have to match byte for byte.
        /// </remarks>
        [Test]
        public void StringCommandsFromTheDeviceAnswerAsFromMemoryInsideATransactionOrAScript()
        {
            RestartServer(memorySize: SpilledLogMemorySize);

            using var client = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };
            PopulateStringRecords(client);

            (string Kind, string[] Command)[] commands =
            [
                ("number", ["INCR"]), ("number", ["INCRBYFLOAT", "1.5"]), ("text", ["APPEND", "x"]),
                ("text", ["SETRANGE", "0", "y"]), ("text", ["GETRANGE", "0", "3"]), ("text", ["STRLEN"]),
                ("text", ["SETBIT", "7", "1"]), ("text", ["BITCOUNT"]), ("text", ["BITPOS", "1"]),
                ("text", ["SET", "z", "XX", "GET"]), ("text", ["GETDEL"]), ("gone", ["SETNX", "x"]),
                ("gone", ["SET", "x", "NX"]), ("hash", ["INCR"]), ("hash", ["SET", "x", "XX"]),
                ("etag", ["SETIFMATCH", "x", "1"]), ("ttl", ["GETEX", "PERSIST"]),
            ];

            var taken = new Dictionary<string, int>();
            string[] KeysFor(bool onDevice)
            {
                var keys = new string[commands.Length];
                for (var c = 0; c < commands.Length; c++)
                {
                    var kind = commands[c].Kind;
                    _ = taken.TryGetValue((onDevice ? "device" : "memory") + kind, out var i);
                    taken[(onDevice ? "device" : "memory") + kind] = i + 1;
                    keys[c] = StringKey(kind, onDevice, i);
                    if (!onDevice && kind != "gone")
                        WriteStringRecord(client, kind, keys[c]);
                }
                return keys;
            }

            string StateOf(string[] keys)
            {
                var state = new StringBuilder();
                foreach (var key in keys)
                    _ = state.Append(client.Execute("GETWITHETAG", key)).Append(client.Execute("TYPE", key)).Append(client.Execute("PERSIST", key));
                return state.ToString();
            }

            string InATransaction(string[] keys)
            {
                var pipeline = new string[commands.Length + 2][];
                pipeline[0] = ["MULTI"];
                for (var c = 0; c < commands.Length; c++)
                    pipeline[c + 1] = [commands[c].Command[0], keys[c], .. commands[c].Command[1..]];
                pipeline[^1] = ["EXEC"];
                client.SendPipeline(pipeline);

                for (var c = 0; c <= commands.Length; c++)
                    _ = client.ReadReply();
                return client.ReadReply();
            }

            string InAScript(string[] keys)
            {
                var script = new StringBuilder("return {");
                for (var c = 0; c < commands.Length; c++)
                {
                    _ = script.Append("redis.pcall('").Append(commands[c].Command[0]).Append("', KEYS[").Append(c + 1).Append(']');
                    foreach (var arg in commands[c].Command[1..])
                        _ = script.Append(", '").Append(arg).Append('\'');
                    _ = script.Append("), ");
                }
                _ = script.Append('}');
                return client.Execute(["EVAL", script.ToString(), keys.Length.ToString(CultureInfo.InvariantCulture), .. keys]);
            }

            foreach (var (name, run) in ((string, Func<string[], string>)[])[("a transaction", InATransaction), ("a script", InAScript)])
            {
                var inMemory = KeysFor(onDevice: false);
                var fromMemory = run(inMemory);
                var memoryState = StateOf(inMemory);

                var onDevice = KeysFor(onDevice: true);
                var readsBefore = deviceFactoryCreator.ReadCount;
                var fromDevice = run(onDevice);
                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount - readsBefore, commands.Length,
                    $"Fewer device reads than commands in {name}, so some of them never left memory.");

                ClassicAssert.AreEqual(fromMemory, fromDevice, $"The commands in {name} answered differently from the device.");
                ClassicAssert.AreEqual(memoryState, StateOf(onDevice), $"The commands in {name} left their records different from the device.");
            }
        }

        /// <summary>
        /// A pop that waits for the device in place passes over a list or sorted set that expired on the device, as it
        /// passes over a missing one, and pops the next key.
        /// </summary>
        [Test]
        public void PopsWaitingForTheDeviceInPlacePassOverAnObjectThatExpiredThere()
        {
            RestartServer(memorySize: SpilledLogMemorySize);

            using var client = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };

            WriteExpiredObjects(client);
            PopulateObjects();

            ClassicAssert.AreEqual(":1\r\n", client.Execute("RPUSH", "pendinglive:list", "a"));
            ClassicAssert.AreEqual(":1\r\n", client.Execute("ZADD", "pendinglive:zset", "1", "a"));

            ClassicAssert.AreEqual("*2\r\n$16\r\npendinglive:list\r\n*1\r\n$1\r\na\r\n",
                client.Execute("LMPOP", "2", ExpiredKey("list", 0), "pendinglive:list", "LEFT"));
            ClassicAssert.AreEqual("*2\r\n$16\r\npendinglive:zset\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n",
                client.Execute("ZMPOP", "2", ExpiredKey("zset", 0), "pendinglive:zset", "MIN"));
            ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount, 2,
                "The expired objects were read from memory, not the device.");
        }

        /// <summary>
        /// A push that goes to the device for its list wakes a client blocked on that list once the push is applied.
        /// </summary>
        /// <remarks>
        /// The list expired on the device, so both the blocked client's first look at it and the push have to read it
        /// from there. Both reads are held until the push has been sent, so the client is waiting on the list whichever
        /// read completes first, and only the push's notice that it added an item can wake it. A notice given before the
        /// push is applied finds the list still empty, and none follows.
        /// </remarks>
        [Test]
        public void APushFromTheDeviceWakesAClientBlockedOnItsList()
        {
            const int HeldMs = 600_000;
            var stallLimit = TimeSpan.FromSeconds(10);

            RestartServer(memorySize: SpilledLogMemorySize);

            using var blocked = new RawClient { ReceiveTimeout = stallLimit };
            using var pusher = new RawClient { ReceiveTimeout = stallLimit };

            WriteExpiredObjects(pusher);
            PopulateObjects();

            var key = ExpiredKey("list", 0);
            try
            {
                deviceFactoryCreator.ReadDelayMs = HeldMs;

                var reads = deviceFactoryCreator.ReadCount;
                blocked.Send("BLPOP", key, "0");
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > reads, stallLimit),
                    "The BLPOP never looked for its list on the device.");

                reads = deviceFactoryCreator.ReadCount;
                pusher.Send("LPUSH", key, "b");
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > reads, stallLimit),
                    "The LPUSH never went to the device, so it proves nothing.");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();
            }

            ClassicAssert.AreEqual(":1\r\n", pusher.ReadReply(), "The LPUSH of an expired list did not create it.");
            try
            {
                ClassicAssert.AreEqual($"*2\r\n${key.Length}\r\n{key}\r\n$1\r\nb\r\n", blocked.ReadReply());
            }
            catch (IOException e)
            {
                Assert.Fail($"The BLPOP was not woken within {stallLimit.TotalSeconds:F0}s by the LPUSH that went to the device. {e.Message}");
            }
        }

        /// <summary>
        /// A CLIENT UNBLOCK that arrives while a blocked pop is reading its collection from the device ends the
        /// pop with nothing taken: the unblock reports success, the client is answered as on a timeout, and every
        /// item is still in the collection.
        /// </summary>
        /// <remarks>
        /// The collection is on the device with items in it, so the blocked client's first look at it takes them
        /// through a copy of the record once the read completes. The read is held until the unblock has answered,
        /// so the take runs after the unblock has already reported success, and has to leave the items behind.
        /// </remarks>
        [TestCase("list")]
        [TestCase("zset")]
        public void AnUnblockDuringABlockedPopsDeviceReadLeavesTheItems(string type)
        {
            const int HeldMs = 600_000;
            var stallLimit = TimeSpan.FromSeconds(10);

            RestartServer(memorySize: SpilledLogMemorySize);

            using var blocked = new RawClient { ReceiveTimeout = stallLimit };
            using var unblocker = new RawClient { ReceiveTimeout = stallLimit };

            PopulateObjects();

            var id = blocked.Execute("CLIENT", "ID");
            StringAssert.StartsWith(":", id);
            id = id[1..^2];

            var isList = type == "list";
            var key = isList ? ListKey(0) : ZsetKey(0);
            try
            {
                deviceFactoryCreator.ReadDelayMs = HeldMs;

                var reads = deviceFactoryCreator.ReadCount;
                blocked.Send(isList ? "BLPOP" : "BZPOPMIN", key, "0");
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > reads, stallLimit),
                    "The blocked pop never looked for its collection on the device.");

                ClassicAssert.AreEqual(":1\r\n", unblocker.Execute("CLIENT", "UNBLOCK", id),
                    "The unblock did not find the client blocked.");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();
            }

            ClassicAssert.AreEqual(isList ? "*-1\r\n" : "$-1\r\n", blocked.ReadReply(), "The unblocked client was answered with items.");
            ClassicAssert.AreEqual("*3\r\n$2\r\nm0\r\n$2\r\nm1\r\n$2\r\nm2\r\n",
                unblocker.Execute(isList ? "LRANGE" : "ZRANGE", key, "0", "-1"),
                "The pop took items from a client it had already reported unblocked.");
        }

        /// <summary>
        /// Minimal synchronous RESP client. Unlike a multiplexer it never reorders or coalesces, and it needs
        /// no thread-pool thread of its own, so it can be used while the pool is deliberately exhausted.
        /// </summary>
        sealed class RawClient : IDisposable
        {
            readonly Socket socket; readonly NetworkStream stream;
            readonly byte[] writeBuffer = new byte[1 << 16];
            byte[] received = new byte[4096];
            int receivedLength;

            internal RawClient()
            {
                var endpoint = TestUtils.EndPoint;
                socket = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };

                // Every blocking call gets a deadline. A test that wedges a socket should fail with a
                // diagnostic, not sit until the CI job is killed.
                socket.ReceiveTimeout = (int)TimeSpan.FromSeconds(120).TotalMilliseconds;
                socket.SendTimeout = (int)TimeSpan.FromSeconds(120).TotalMilliseconds;

                if (!socket.ConnectAsync(endpoint).Wait(TimeSpan.FromSeconds(120)))
                    throw new IOException($"Timed out connecting to {endpoint}.");

                stream = new NetworkStream(socket, ownsSocket: false);
            }

            /// <summary>How long a read waits for the server before it fails.</summary>
            internal TimeSpan ReceiveTimeout { set => socket.ReceiveTimeout = (int)value.TotalMilliseconds; }

            /// <summary>Whether a reply has arrived and not been read.</summary>
            internal bool HasReply => receivedLength > 0 || socket.Available > 0;

            internal string Execute(params string[] args)
            {
                Send(args);
                return ReadReply();
            }

            internal void Send(params string[] args)
                => Send(Array.ConvertAll(args, Encoding.UTF8.GetBytes));

            /// <summary>Executes a command whose arguments need not be text.</summary>
            internal string Execute(params byte[][] args)
            {
                Send(args);
                return ReadReply();
            }

            /// <summary>Sends a command whose arguments need not be text.</summary>
            internal void Send(params byte[][] args)
            {
                var n = Encode(args, 0);
                stream.Write(writeBuffer, 0, n);
                stream.Flush();
            }

            internal void SendPipeline(params string[][] commands)
            {
                var n = 0;
                foreach (var cmd in commands)
                    n = Encode(cmd, n);
                stream.Write(writeBuffer, 0, n);
                stream.Flush();
            }

            int Encode(string[] args, int at)
                => Encode(Array.ConvertAll(args, Encoding.UTF8.GetBytes), at);

            int Encode(byte[][] args, int at)
            {
                writeBuffer[at++] = (byte)'*';
                at += WriteInt(args.Length, at);
                at = WriteCrLf(at);
                foreach (var a in args)
                {
                    writeBuffer[at++] = (byte)'$';
                    at += WriteInt(a.Length, at);
                    at = WriteCrLf(at);
                    a.CopyTo(writeBuffer.AsSpan(at));
                    at += a.Length;
                    at = WriteCrLf(at);
                }
                return at;
            }

            int WriteInt(int value, int at)
            {
                _ = value.TryFormat(writeBuffer.AsSpan(at), out var written, provider: CultureInfo.InvariantCulture);
                return written;
            }

            int WriteCrLf(int at)
            {
                writeBuffer[at++] = (byte)'\r';
                writeBuffer[at++] = (byte)'\n';
                return at;
            }

            /// <summary>Reads exactly one RESP reply, including its framing.</summary>
            internal string ReadReply()
            {
                while (true)
                {
                    var consumed = TryFrame(received.AsSpan(0, receivedLength));
                    if (consumed > 0)
                    {
                        var reply = Encoding.UTF8.GetString(received, 0, consumed);
                        receivedLength -= consumed;
                        received.AsSpan(consumed, receivedLength).CopyTo(received);
                        return reply;
                    }

                    if (receivedLength == received.Length)
                        Array.Resize(ref received, received.Length * 2);

                    var n = stream.Read(received, receivedLength, received.Length - receivedLength);
                    if (n <= 0)
                        throw new IOException("Connection closed while waiting for a reply");
                    receivedLength += n;
                }
            }

            /// <summary>
            /// Returns the length of the complete RESP reply at the start of <paramref name="buffer"/>, or 0
            /// if it has not all arrived.
            /// </summary>
            static int TryFrame(ReadOnlySpan<byte> buffer)
            {
                if (buffer.Length == 0)
                    return 0;

                var lineEnd = IndexOfCrLf(buffer);
                if (lineEnd < 0)
                    return 0;

                var headerLength = lineEnd + 2;
                switch (buffer[0])
                {
                    case (byte)'$':
                        var size = ParseInt(buffer[1..lineEnd]);
                        if (size < 0)
                            return headerLength;
                        var total = headerLength + size + 2;
                        return buffer.Length >= total ? total : 0;

                    case (byte)'*':
                        var count = ParseInt(buffer[1..lineEnd]);
                        if (count < 0)
                            return headerLength;
                        var at = headerLength;
                        for (var i = 0; i < count; i++)
                        {
                            var element = TryFrame(buffer[at..]);
                            if (element == 0)
                                return 0;
                            at += element;
                        }
                        return at;

                    default:
                        return headerLength;
                }
            }

            static int ParseInt(ReadOnlySpan<byte> digits)
            {
                _ = int.TryParse(digits, CultureInfo.InvariantCulture, out var value);
                return value;
            }

            static int IndexOfCrLf(ReadOnlySpan<byte> buffer)
            {
                for (var i = 0; i + 1 < buffer.Length; i++)
                {
                    if (buffer[i] == (byte)'\r' && buffer[i + 1] == (byte)'\n')
                        return i;
                }
                return -1;
            }

            public void Dispose()
            {
                stream?.Dispose();
                socket?.Dispose();
            }
        }
    }
}