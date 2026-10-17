// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
        /// Starts the server, with at most <paramref name="offloadMaxThreads"/> executor threads if not 0, and
        /// <paramref name="memorySize"/> of log memory if set.
        /// </summary>
        void StartServer(int offloadMaxThreads = 0, string memorySize = null)
        {
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
            options.OffloadMaxThreads = offloadMaxThreads;

            server = new GarnetServer(options);
            server.Start();
        }

        /// <summary>Replaces the server started by <see cref="Setup"/> with one started as <see cref="StartServer"/> does.</summary>
        void RestartServer(int offloadMaxThreads = 0, string memorySize = null)
        {
            server.Dispose();
            deviceFactoryCreator.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            StartServer(offloadMaxThreads, memorySize);
        }

        /// <summary>Executor threads started and not yet retired, which they do after 30 seconds idle.</summary>
        int OffloadThreads => server.Provider.StoreWrapper.offloadExecutor.ThreadCount;

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
        /// A command that completes its device reads on the thread it runs on, rather than parking for them, runs on
        /// an executor thread once the store has spilled to the device, and answers as it would from memory.
        /// </summary>
        /// <remarks>
        /// The device counts the reads issued by a command running on a thread other than an executor thread. Every
        /// command here completes its reads in place, so that count has to stay at zero: run where they are received,
        /// these commands would hold a network thread for each device round trip. The <c>GET</c> at the end is the
        /// control. It parks on its read, which it issues on the thread that received it, so the count has to move.
        /// </remarks>
        [Test]
        public void CommandsThatWaitForTheDeviceInPlaceRunOnAnExecutorThread()
        {
            Populate();
            PopulateObjects();

            using var client = new RawClient();

            void AssertFromTheDevice(string expected, params string[] argv)
            {
                var readsBefore = deviceFactoryCreator.ReadCount;
                ClassicAssert.AreEqual(expected, client.Execute(argv), $"Wrong reply to {argv[0]}.");
                ClassicAssert.Greater(deviceFactoryCreator.ReadCount, readsBefore,
                    $"{argv[0]} never went to the device, so it proves nothing.");
            }

            AssertFromTheDevice(":48\r\n", "STRLEN", Key(10));
            AssertFromTheDevice($"$4\r\n{Value(11)[..4]}\r\n", "GETRANGE", Key(11), "0", "3");
            AssertFromTheDevice(":49\r\n", "APPEND", Key(12), "x");
            AssertFromTheDevice(":48\r\n", "SETRANGE", Key(13), "0", "z");
            AssertFromTheDevice($"${ValueLength}\r\n{Value(14)}\r\n", "GETDEL", Key(14));
            AssertFromTheDevice(":1\r\n", "HSET", HashKey(10), "f9", "v");
            AssertFromTheDevice(":1\r\n", "HINCRBY", HashKey(11), "n", "1");
            AssertFromTheDevice(":4\r\n", "LPUSH", ListKey(12), "x");
            AssertFromTheDevice(":1\r\n", "SADD", SetKey(13), "x");
            AssertFromTheDevice(":1\r\n", "ZADD", ZsetKey(14), "100", "x");
            AssertFromTheDevice(":3\r\n", "ZLEXCOUNT", ZsetKey(15), "-", "+");
            AssertFromTheDevice($"*1\r\n${Key(20).Length}\r\n{Key(20)}\r\n", "KEYS", Key(20));

            ClassicAssert.AreEqual(0, deviceFactoryCreator.ReadsOffExecutor,
                "A command that completes its device reads in place issued them on a thread other than an executor thread.");

            AssertFromTheDevice($"${ValueLength}\r\n{Value(21)}\r\n", "GET", Key(21));
            ClassicAssert.Greater(deviceFactoryCreator.ReadsOffExecutor, 0,
                "A GET parks on its read where it is received, yet no read was counted off the executor, so the count proves nothing.");
        }

        /// <summary>
        /// A write that does not read the record it replaces runs where it is received, even with records spilled to
        /// the device.
        /// </summary>
        [Test]
        public void BlindWritesRunWhereTheyAreReceived()
        {
            RestartServer(memorySize: SpilledLogMemorySize);

            using var client = new RawClient();
            ClassicAssert.AreEqual(":1\r\n", client.Execute("HSET", HashKey(10), "f", "v"));
            ClassicAssert.AreEqual(":1\r\n", client.Execute("RPUSH", ListKey(11), "x"));
            Populate();

            var executor = server.Provider.StoreWrapper.offloadExecutor;
            ClassicAssert.IsFalse(server.Provider.StoreWrapper.store.Log.AllocationMayWaitForFlush,
                "The log has to wait for a flush to add a record, so every write runs on an executor thread.");

            void AssertInPlace(string expected, params string[] argv)
            {
                var readsBefore = deviceFactoryCreator.ReadCount;
                var postsBefore = executor.Posts;
                ClassicAssert.AreEqual(expected, client.Execute(argv), $"Wrong reply to {argv[0]}.");
                ClassicAssert.AreEqual(readsBefore, deviceFactoryCreator.ReadCount, $"{argv[0]} read from the device.");
                ClassicAssert.AreEqual(postsBefore, executor.Posts, $"{argv[0]} was moved to an executor thread.");
            }

            AssertInPlace("+OK\r\n", "SET", Key(10), "v");
            AssertInPlace("+OK\r\n", "MSET", Key(11), "v", Key(12), "v");
            AssertInPlace("+OK\r\n", "SETEX", Key(13), "100", "v");
            AssertInPlace("+OK\r\n", "PSETEX", Key(14), "100000", "v");
            AssertInPlace(":0\r\n", "DEL", Key(15), HashKey(10));
            AssertInPlace(":0\r\n", "UNLINK", Key(16), ListKey(11));

            var posts = executor.Posts;
            ClassicAssert.AreEqual(":48\r\n", client.Execute("STRLEN", Key(17)));
            ClassicAssert.Greater(executor.Posts, posts, "STRLEN reads the device in place, yet was not counted as moved.");
        }

        /// <summary>
        /// While the log cannot turn a page until a flush completes, a write runs on an executor thread, and a transaction
        /// moves there at <c>EXEC</c>, before it locks anything. Another connection is served meanwhile.
        /// </summary>
        /// <remarks>
        /// The log's flushes are held from its first, so no page can be freed for the writes, and none has been evicted, so
        /// nothing is read from the device: the wait for the flush alone moves the commands. A write run where it is
        /// received would wait for the flush on that thread. Were a transaction to run where <c>EXEC</c> is received, its write would have to
        /// move once the transaction holds its locks, and a command that holds locks is given a thread past the limit. The
        /// executor's one thread is waiting for the flush with the first connection's writes, so each such transaction would
        /// start another.
        /// </remarks>
        [Test]
        public void WritesThatMayWaitForAFlushRunOnAnExecutorThread()
        {
            const int Transactions = 4;

            // More records than fit on a page, so one of them has to turn the tail onto a new page
            const int WritesPastAPage = 64;

            RestartServer(offloadMaxThreads: 1, memorySize: SpilledLogMemorySize);

            var log = server.Provider.StoreWrapper.store.Log;
            var executor = server.Provider.StoreWrapper.offloadExecutor;
            ClassicAssert.IsFalse(log.AllocationMayWaitForFlush,
                "The log has to wait for a flush before any is held, so every write runs on an executor thread.");

            using var writer = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };
            using var probe = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(10) };
            var transactions = new RawClient[Transactions];
            try
            {
                for (var i = 0; i < Transactions; i++)
                    transactions[i] = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };

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

                var writes = new string[WritesPastAPage][];
                for (var i = 0; i < WritesPastAPage; i++)
                    writes[i] = ["SET", Key(next + i), Value(next + i)];

                var posts = executor.Posts;
                writer.SendPipeline(writes);
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => executor.Posts > posts, TimeSpan.FromSeconds(10)),
                    "A write that may wait for a flush ran where it was received.");

                posts = executor.Posts;
                for (var i = 0; i < Transactions; i++)
                    transactions[i].SendPipeline(["MULTI"], ["SET", Key(i), "x"], ["EXEC"]);
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => executor.Posts >= posts + Transactions, TimeSpan.FromSeconds(10)),
                    "The transactions' writes never reached the executor.");

                ClassicAssert.AreEqual(1, OffloadThreads,
                    "Transactions started executor threads past the limit, so they moved there only after locking.");
                ClassicAssert.IsFalse(writer.HasReply,
                    "The writes finished without waiting for the held flush, so the executor's thread was free for the transactions.");

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
        /// A transaction that queued a command which completes its device reads in place moves to an executor thread
        /// at <c>EXEC</c>, and so stays within the executor's limit.
        /// </summary>
        /// <remarks>
        /// Were it to run where <c>EXEC</c> is received, each queued command would have to move once the transaction
        /// holds its locks, and a command that holds locks is given a thread past the limit. With the limit at one,
        /// concurrent transactions would then start a thread each.
        /// </remarks>
        [Test]
        public void TransactionsThatMayReadFromTheDeviceStartOnTheExecutorWithinItsLimit()
        {
            const int Sessions = 8;

            RestartServer(offloadMaxThreads: 1);
            Populate();

            var clients = new RawClient[Sessions];
            try
            {
                for (var i = 0; i < Sessions; i++)
                    clients[i] = new RawClient();

                deviceFactoryCreator.ReadDelayMs = 200;

                for (var i = 0; i < Sessions; i++)
                    clients[i].SendPipeline(["MULTI"], ["APPEND", Key(i), "x"], ["EXEC"]);

                for (var i = 0; i < Sessions; i++)
                {
                    ClassicAssert.AreEqual("+OK\r\n", clients[i].ReadReply());
                    ClassicAssert.AreEqual("+QUEUED\r\n", clients[i].ReadReply());
                    ClassicAssert.AreEqual("*1\r\n:49\r\n", clients[i].ReadReply(), $"Wrong reply for {Key(i)}");
                }

                ClassicAssert.GreaterOrEqual(deviceFactoryCreator.ReadCount, Sessions,
                    "Fewer device reads than transactions, so some of them never left memory.");
                ClassicAssert.AreEqual(0, deviceFactoryCreator.ReadsOffExecutor,
                    "A transaction completed a device read in place on a thread other than an executor thread.");
                ClassicAssert.AreEqual(1, OffloadThreads,
                    "Transactions started executor threads past the limit, so they moved there only after locking.");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                foreach (var c in clients)
                    c?.Dispose();
            }
        }

        /// <summary>
        /// A transaction that has to move to the executor while it holds its locks gets a thread even when the only one
        /// is taken by a command waiting for one of those locks.
        /// </summary>
        /// <remarks>
        /// The transaction moves to the executor at <c>EXEC</c>, parks on the device for its <c>GET</c>, and resumes
        /// on whichever thread completes the read, so its <c>APPEND</c> has to move to the executor again, with its
        /// locks held. Meanwhile another session's <c>APPEND</c> of the same key has taken the one executor thread and
        /// waits there for the lock. Queued behind it, the transaction would wait for the thread and the thread for
        /// the transaction.
        /// </remarks>
        [Test]
        public void ATransactionHoldingLocksGetsAnExecutorThreadPastTheLimit()
        {
            RestartServer(offloadMaxThreads: 1);
            Populate();

            using var transaction = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };
            using var contender = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(30) };
            try
            {
                deviceFactoryCreator.ReadDelayMs = 600_000;

                transaction.SendPipeline(["MULTI"], ["GET", Key(0)], ["APPEND", Key(1), "x"], ["EXEC"]);
                ClassicAssert.IsTrue(SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount > 0, TimeSpan.FromSeconds(30)),
                    "The transaction's GET never reached the device.");

                // Takes the one executor thread and holds it, waiting for the transaction's lock on the key
                contender.Send("APPEND", Key(1), "y");
                Thread.Sleep(500);

                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();

                ClassicAssert.AreEqual("+OK\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual("+QUEUED\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual("+QUEUED\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual($"*2\r\n${ValueLength}\r\n{Value(0)}\r\n:49\r\n", transaction.ReadReply());
                ClassicAssert.AreEqual(":50\r\n", contender.ReadReply());
                ClassicAssert.AreEqual(0, deviceFactoryCreator.ReadsOffExecutor,
                    "The transaction resumed from its GET and completed its APPEND's device read in place, off the executor.");
            }
            catch (IOException e)
            {
                Assert.Fail($"The transaction and the command waiting for its lock deadlocked over the one executor thread. {e.Message}");
            }
            finally
            {
                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();

                // Runs anything still queued on the thread pool, so a deadlock here fails the test instead of the teardown
                server.Provider.StoreWrapper.offloadExecutor.Dispose();
            }
        }

        /// <summary>
        /// With every command that completes its device read in place waiting on the device at once, and more of them
        /// than the thread pool has threads, another connection is still served.
        /// </summary>
        /// <remarks>
        /// Run where they are received, these commands would each hold a thread-pool thread until the device answers,
        /// so with the pool clamped no more than the pool's size could reach the device at once, and nothing would be
        /// left to serve the <c>PING</c>. See the Linux note on
        /// <see cref="ManyConcurrentDeviceReadsOutnumberTheThreadPoolAndStillFinishTogether"/>.
        /// </remarks>
        [Test]
        public void ANetworkThreadIsFreeWhileMoreCommandsThanThreadsWaitOnTheDeviceInPlace()
        {
            if (!OperatingSystem.IsLinux())
                Assert.Ignore("Starving the worker pool only constrains session dispatch on Linux.");

            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            ThreadPool.GetMinThreads(out var minWorkers, out _);

            var cappedWorkers = Math.Max(Environment.ProcessorCount, minWorkers);
            var sessionCount = cappedWorkers * 2;

            // Enough executor threads for every session, so all of them wait on the device at once
            RestartServer(offloadMaxThreads: sessionCount);
            Populate();

            var clients = new RawClient[sessionCount];
            RawClient probe = null;
            var clamped = false;
            try
            {
                for (var i = 0; i < sessionCount; i++)
                    clients[i] = new RawClient();
                probe = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(10) };

                clamped = ThreadPool.SetMaxThreads(cappedWorkers, maxIo);
                ClassicAssert.IsTrue(clamped, "Could not clamp the worker pool, so this test proves nothing.");

                deviceFactoryCreator.ReadDelayMs = 600_000;

                for (var i = 0; i < sessionCount; i++)
                    clients[i].Send("STRLEN", Key(i));

                _ = SpinWait.SpinUntil(() => deviceFactoryCreator.ReadCount >= sessionCount, TimeSpan.FromSeconds(30));
                ClassicAssert.AreEqual(sessionCount, deviceFactoryCreator.ReadCount,
                    $"Only {deviceFactoryCreator.ReadCount} of {sessionCount} commands reached the device at once with " +
                    $"{cappedWorkers} worker threads, so they wait for it on thread-pool threads.");

                try
                {
                    ClassicAssert.AreEqual("+PONG\r\n", probe.Execute("PING"));
                }
                catch (IOException e)
                {
                    Assert.Fail($"No thread served a PING while {sessionCount} commands waited on the device against " +
                        $"{cappedWorkers} worker threads. {e.Message}");
                }

                deviceFactoryCreator.ReadDelayMs = 0;
                deviceFactoryCreator.CompleteHeldReads();

                for (var i = 0; i < sessionCount; i++)
                    ClassicAssert.AreEqual($":{ValueLength}\r\n", clients[i].ReadReply(), $"Wrong reply for {Key(i)}");
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
        /// A flush, which waits on the store's log as it resets it, runs on an executor thread whether or not the store
        /// has spilled to the device, while a command that waits on nothing does not.
        /// </summary>
        [Test]
        public void AFlushRunsOnAnExecutorThread()
        {
            using var client = new RawClient();

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SET", "k", "v"));
            ClassicAssert.AreEqual("$1\r\nv\r\n", client.Execute("GET", "k"));
            ClassicAssert.AreEqual(0, OffloadThreads, "A command that waits on nothing started an executor thread.");

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("FLUSHDB"));
            ClassicAssert.AreEqual(1, OffloadThreads, "FLUSHDB did not run on an executor thread.");
            ClassicAssert.AreEqual("$-1\r\n", client.Execute("GET", "k"));
        }

        /// <summary>
        /// Once the executor is disposed, a command that would be moved to it runs on the thread it resumes on.
        /// </summary>
        [Test]
        public void ACommandRunsInPlaceOnceTheExecutorIsDisposed()
        {
            using var client = new RawClient { ReceiveTimeout = TimeSpan.FromSeconds(10) };

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SET", "k", "v"));
            server.Provider.StoreWrapper.offloadExecutor.Dispose();

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("FLUSHDB"));
            ClassicAssert.AreEqual("$-1\r\n", client.Execute("GET", "k"));
        }

        /// <summary>
        /// Selecting a database for the first time, which creates it with its log and device, runs on an executor
        /// thread; selecting one that exists does not.
        /// </summary>
        [Test]
        public void SelectingANewDatabaseRunsOnAnExecutorThread()
        {
            using var client = new RawClient();

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SELECT", "0"));
            ClassicAssert.AreEqual(0, OffloadThreads, "Selecting the current database started an executor thread.");

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SELECT", "1"));
            ClassicAssert.AreEqual(1, OffloadThreads, "Creating a database did not run on an executor thread.");

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("SET", "k", "v"));
            ClassicAssert.AreEqual("$1\r\nv\r\n", client.Execute("GET", "k"));
        }

        /// <summary>
        /// Of the <c>DEBUG</c> subcommands, only the one that waits for the log to flush runs on an executor thread;
        /// the blocking ones park the session themselves.
        /// </summary>
        [Test]
        public void OnlyADebugSubcommandThatWaitsForTheDeviceRunsOnAnExecutorThread()
        {
            using var client = new RawClient();
            var executor = server.Provider.StoreWrapper.offloadExecutor;

            ClassicAssert.AreEqual("+OK\r\n", client.Execute("DEBUG", "LOG", "x"));
            ClassicAssert.AreEqual("+OK\r\n", client.Execute("DEBUG", "BLOCK", "0"));
            ClassicAssert.AreEqual(0, executor.Posts, "A DEBUG subcommand that waits on nothing in place was moved to an executor thread.");

            StringAssert.StartsWith("+OK head=", client.Execute("DEBUG", "FLUSHANDEVICT"));
            ClassicAssert.AreEqual(1, executor.Posts, "DEBUG FLUSHANDEVICT did not run on an executor thread.");
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

                    // Release the main log read only: the object log read it leads to is held.
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
            {
                writeBuffer[at++] = (byte)'*';
                at += WriteInt(args.Length, at);
                at = WriteCrLf(at);
                foreach (var a in args)
                {
                    writeBuffer[at++] = (byte)'$';
                    at += WriteInt(Encoding.UTF8.GetByteCount(a), at);
                    at = WriteCrLf(at);
                    at += Encoding.UTF8.GetBytes(a, writeBuffer.AsSpan(at));
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