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

        GarnetServer server;
        DelayedReadDeviceFactoryCreator deviceFactoryCreator;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Built through GetGarnetServerOptions rather than CreateGarnetServer because only the options
            // object exposes DeviceFactoryCreator, which is how the read delay is injected.
            var options = TestUtils.GetGarnetServerOptions(
                checkpointDir: TestUtils.MethodTestDir,
                logDir: TestUtils.MethodTestDir,
                endpoint: TestUtils.EndPoint,
                enableCluster: false,
                lowMemory: true);

            deviceFactoryCreator = new DelayedReadDeviceFactoryCreator();
            options.DeviceFactoryCreator = deviceFactoryCreator;

            server = new GarnetServer(options);
            server.Start();
        }

        [TearDown]
        public void TearDown()
        {
            if (deviceFactoryCreator is not null)
                deviceFactoryCreator.ReadDelayMs = 0;

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
        /// A transaction cannot suspend: it holds its key locks for the whole wait. The read is completed in
        /// place instead, which must still produce the right reply.
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
        /// Several reads in one pipelined batch, so the session suspends and resumes part way through a batch
        /// repeatedly and each reply still lands in order.
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
        /// A read is issued against the response buffer at the position the writer had reached, but the
        /// suspension gives that buffer back and the resume starts a new one at its head. A reply written
        /// earlier in the same batch moves those two positions apart, so a completion that still writes
        /// through the issue-time position shows up here as a wrong reply.
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
        /// Calibration, all from Linux, which is where the bound is enforced. The floor differs by runtime, so
        /// each leg carries its own bound; both separate cleanly from the same set of regressions.
        /// </para>
        /// <list type="table">
        /// <listheader><term>variant</term><description>net10.0 / net8.0</description></listheader>
        /// <item><term>as written</term><description>23-34 B / 62-71 B</description></item>
        /// <item><term>driver parked on the <c>ValueTask</c> wait instead of registering itself as a work
        /// item, so completing a plain delegate makes the thread pool box it</term>
        /// <description>53-65 B / 89-100 B</description></item>
        /// <item><term>driver replaced by the two compiler-generated async frames it subsumes</term>
        /// <description>52-74 B / not measured</description></item>
        /// <item><term>uncancellable wait routed back through the semaphore</term>
        /// <description>153-172 B / not measured</description></item>
        /// </list>
        /// <para>
        /// Each bound sits above its own range and below every regression measured on that leg, so any one of
        /// them fails here. The spread within a range is sampling noise plus the pooled builders' per-thread
        /// caches, which miss whenever a completion lands on a different thread than the one that rented the
        /// box - which is most of the time here, since suspending and resuming is the entire point.
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

            // Asserted only where the bound was calibrated. The pooled builders cache per thread, so the hit
            // rate - and with it the measurement - depends on how the platform schedules completions, and the
            // ranges above were measured on Linux. Detection power is unaffected: a regression is a regression
            // on every platform, so the Linux legs catch it. Other platforms still print the value, which is
            // what a future calibration would start from.
            if (!OperatingSystem.IsLinux())
                return;

#if NET8_0
            const int Bound = 80;
#else
            const int Bound = 45;
#endif
            ClassicAssert.Less(perRead, Bound,
                "A park on the pending-read path allocates more than it should: the reusable waiter was bypassed, the pooled driver was replaced by async frames, or the driver stopped parking as a work item.");
#endif
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
        /// Minimal synchronous RESP client. Unlike a multiplexer it never reorders or coalesces, and it needs
        /// no thread-pool thread of its own, so it can be used while the pool is deliberately exhausted.
        /// </summary>
        sealed class RawClient : IDisposable
        {
            readonly Socket socket;
            readonly NetworkStream stream;
            readonly byte[] writeBuffer = new byte[4096];
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