// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using StackExchange.Redis;

namespace Garnet.test
{
    /// <summary>
    /// Tests for <see cref="GarnetClient"/> request/completion rings, memory admission, reconnect,
    /// graceful disposal, and the RESP, GOSSIP and pub/sub surface used by multi-threaded producers.
    /// </summary>
    [TestFixture]
    public class GarnetClientNetworkWriterTests : TestBase
    {
        static readonly byte[] SET = Encoding.ASCII.GetBytes("$3\r\nSET\r\n");
        static readonly byte[] GET = Encoding.ASCII.GetBytes("$3\r\nGET\r\n");
        static readonly byte[] ECHO = Encoding.ASCII.GetBytes("$4\r\nECHO\r\n");
        static readonly byte[] INCR = Encoding.ASCII.GetBytes("$4\r\nINCR\r\n");
        static readonly byte[] INCRBY = Encoding.ASCII.GetBytes("$6\r\nINCRBY\r\n");

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        [Test]
        public unsafe void CommandSerializationPreservesCollectionWireFormat(
            [Values(0, 8, 40960)] int payloadLength, [Values(0, 1, 2)] int collectionKind)
        {
            var payload = new string('v', payloadLength);
            Memory<byte>[] arguments = [Encoding.ASCII.GetBytes(payload), Memory<byte>.Empty];
            ICollection<Memory<byte>> collection = collectionKind switch
            {
                0 => arguments,
                1 => new List<Memory<byte>>(arguments),
                _ => new LinkedList<Memory<byte>>(arguments),
            };
            var expected = Encoding.ASCII.GetBytes($"*3\r\n$4\r\nECHO\r\n${payloadLength}\r\n{payload}\r\n$0\r\n\r\n");
            var length = GarnetClient.GetCommandLength(ECHO, collection);
            ClassicAssert.AreEqual(expected.Length, length);
            var actual = new byte[length];
            fixed (byte* destination = actual)
                GarnetClient.WriteCommand(destination, length, ECHO, collection);
            CollectionAssert.AreEqual(expected, actual);
        }

        [Test]
        public unsafe void ArrayAndListSerializationDoesNotAllocate([Values] bool useList)
        {
            Memory<byte>[] arguments = [Encoding.ASCII.GetBytes("hello"), Memory<byte>.Empty];
            ICollection<Memory<byte>> collection = useList ? new List<Memory<byte>>(arguments) : arguments;
            var length = GarnetClient.GetCommandLength(ECHO, collection);
            var buffer = new byte[length];
            fixed (byte* destination = buffer)
            {
                for (var i = 0; i < 100; i++)
                    GarnetClient.WriteCommand(destination, GarnetClient.GetCommandLength(ECHO, collection), ECHO, collection);

                // The runtime can allocate once on this thread mid-window under a busy suite; a per-call
                // allocation shows in every round, so require one clean round rather than the first.
                var allocated = long.MaxValue;
                for (var round = 0; round < 5 && allocated != 0; round++)
                {
                    var before = GC.GetAllocatedBytesForCurrentThread();
                    for (var i = 0; i < 1000; i++)
                        GarnetClient.WriteCommand(destination, GarnetClient.GetCommandLength(ECHO, collection), ECHO, collection);
                    allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                }
                ClassicAssert.AreEqual(0, allocated);
            }
        }

        [Test]
        public async Task SimplePingTest([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            using var db = TestUtils.GetGarnetClient(useTLS: useTLS);
            await db.ConnectAsync().ConfigureAwait(false);

            var result = await db.PingAsync().ConfigureAwait(false);
            ClassicAssert.AreEqual("PONG", result);
        }

        [Test]
        public async Task UpdatedAuthenticationIsUsedOnReconnect()
        {
            const string password = "correct-password";
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, defaultPassword: password);
            server.Start();

            using var db = new GarnetClient(TestUtils.EndPoint, authPassword: "wrong-password");
            Assert.ThrowsAsync<Exception>(async () => await db.ConnectAsync().ConfigureAwait(false));

            db.UpdateAuth(username: null, password: password);
            await db.ReconnectAsync().ConfigureAwait(false);

            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
        }

        [Test]
        public async Task SimpleSetGetTest([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            using var db = TestUtils.GetGarnetClient(useTLS: useTLS);
            await db.ConnectAsync().ConfigureAwait(false);

            var origValue = "abcdefg";
            var setResult = await db.ExecuteForStringResultAsync(SET, ["mykey", origValue]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", setResult);

            var getResult = await db.ExecuteForStringResultAsync(GET, ["mykey"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(origValue, getResult);

            var missing = await db.ExecuteForStringResultAsync(GET, ["nokey"]).ConfigureAwait(false);
            ClassicAssert.IsNull(missing);
        }

        [Test]
        public async Task SimpleIncrTest([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            using var db = TestUtils.GetGarnetClient(useTLS: useTLS);
            await db.ConnectAsync().ConfigureAwait(false);

            var n = await db.ExecuteForLongResultAsync(INCR, ["counter"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(1, n);

            n = await db.ExecuteForLongResultAsync(INCRBY, ["counter", "10"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(11, n);
        }

        [Test]
        public async Task SimpleMemoryResultTest([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            using var db = TestUtils.GetGarnetClient(useTLS: useTLS);
            await db.ConnectAsync().ConfigureAwait(false);

            var payload = new byte[] { 0x00, 0x01, 0x02, 0xFE, 0xFF };
            _ = await db.ExecuteForStringResultAsync(SET, [(Memory<byte>)Encoding.ASCII.GetBytes("binkey"), (Memory<byte>)payload]).ConfigureAwait(false);

            using var result = await db.ExecuteForMemoryResultAsync(GET, [(Memory<byte>)Encoding.ASCII.GetBytes("binkey")]).ConfigureAwait(false);
            CollectionAssert.AreEqual(payload, result.Span.ToArray());
        }

        [Test]
        public async Task ConcurrentProducersIncrTest([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            using var db = TestUtils.GetGarnetClient(useTLS: useTLS);
            await db.ConnectAsync().ConfigureAwait(false);

            const int producers = 8;
            const int perProducer = 200;

            var tasks = new Task[producers];
            for (var p = 0; p < producers; p++)
            {
                tasks[p] = Task.Run(async () =>
                {
                    for (var i = 0; i < perProducer; i++)
                        _ = await db.ExecuteForLongResultAsync(INCR, ["shared"]).ConfigureAwait(false);
                });
            }
            await Task.WhenAll(tasks).ConfigureAwait(false);

            // GET returns the value as a bulk string; ExecuteForLongResultAsync parses that as an integer.
            var final = await db.ExecuteForLongResultAsync(GET, ["shared"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(producers * perProducer, final);
        }

        [Test, NonParallelizable]
        public async Task CompletionBackpressureStressTest([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            const int completionCapacity = 8;
            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 16,
                maxOutstandingCompletions: completionCapacity,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 0,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(
                useTLS: useTLS,
                networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            const int producers = 8;
            const int perProducer = 200;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var producerTasks = new Task[producers];

            for (var p = 0; p < producers; p++)
            {
                var producer = p;
                producerTasks[p] = Task.Run(async () =>
                {
                    var pending = new Task<string>[perProducer];
                    var expected = new string[perProducer];

                    for (var i = 0; i < perProducer; i++)
                    {
                        var value = $"{producer}:{i}:{new string((char)('a' + producer), i % 384)}";
                        expected[i] = value;
                        pending[i] = db.ExecuteForStringResultWithCancellationAsync(ECHO, [value], cts.Token);
                    }

                    var actual = await Task.WhenAll(pending).ConfigureAwait(false);
                    for (var i = 0; i < perProducer; i++)
                        ClassicAssert.AreEqual(expected[i], actual[i], $"Completion mismatch for producer {producer}, request {i}.");
                }, cts.Token);
            }

            await Task.WhenAll(producerTasks).ConfigureAwait(false);
        }

        [Test]
        public async Task PeerDisconnectFaultsPublishedCompletion()
        {
            using var listener = new TcpListener(IPAddress.Loopback, TestUtils.TestPort);
            listener.Start();

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var acceptTask = listener.AcceptSocketAsync(cts.Token).AsTask();
            using var db = new GarnetClient(new IPEndPoint(IPAddress.Loopback, TestUtils.TestPort));
            var connectTask = db.ConnectAsync(cts.Token);
            using var peer = await acceptTask.ConfigureAwait(false);
            await connectTask.ConfigureAwait(false);

            var pending = db.ExecuteForStringResultWithCancellationAsync(ECHO, ["pending"], cts.Token);
            var receiveBuffer = new byte[256];
            ClassicAssert.Greater(
                await peer.ReceiveAsync(receiveBuffer, SocketFlags.None, cts.Token).ConfigureAwait(false),
                0,
                "The request was not published before the peer disconnected.");

            peer.Dispose();

            Assert.ThrowsAsync<GarnetClientDisposedException>(async () =>
                await pending.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false));
        }

        [Test]
        public async Task OutOfLineAdmissionQueueIsCreatedOnFirstOutOfLineRequest()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 8,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 4096,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var writer = GetNetworkWriter(db);
            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
            ClassicAssert.IsNull(GetMemoryThrottle(writer), "An inline-only client must not allocate the out-of-line admission queue.");

            var value = new string('x', 300);
            ClassicAssert.AreEqual(value, await db.ExecuteForStringResultAsync(ECHO, [value]).ConfigureAwait(false));
            var queue = GetMemoryThrottle(writer);
            ClassicAssert.IsNotNull(queue, "An out-of-line request must create the admission queue.");
            ClassicAssert.AreSame(queue, GetMemoryThrottle(writer));
        }

        [Test]
        public async Task OutOfLineRentalsUsePayloadSizeAndReuseBurst()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();
            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 131072,
                requestPageSizeBytes: 8192,
                requestPageCount: 4,
                maxOutstandingRequests: 64,
                maxOutstandingCompletions: 64,
                maxConcurrentNetworkSends: 64,
                maxOutOfLineRentedBytes: 0,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var client = TestUtils.GetGarnetClient(networkWriterOptions: options);
            await client.ConnectAsync().ConfigureAwait(false);
            var writer = GetNetworkWriter(client);
            ClassicAssert.IsNull(typeof(NetworkWriter).GetField("requestPool", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(writer));
            ClassicAssert.AreEqual(65536, writer.GetRequestBufferAllocationSize(40960));
            var rented = new RequestContext[32];
            var entries = new HashSet<PoolEntry>();
            for (var i = 0; i < rented.Length; i++)
            {
                rented[i] = writer.RentRequestBuffer(40960);
                ClassicAssert.AreEqual(65536, rented[i].Buffer.Length);
                entries.Add(rented[i].poolEntry);
            }
            for (var i = 0; i < rented.Length; i++)
                rented[i].Dispose();
            for (var i = 0; i < rented.Length; i++)
            {
                rented[i] = writer.RentRequestBuffer(40960);
                ClassicAssert.IsTrue(entries.Contains(rented[i].poolEntry), "The warmed burst must reuse every payload buffer.");
            }
            for (var i = 0; i < rented.Length; i++)
                rented[i].Dispose();

            for (var i = 0; i < 10; i++)
                RentAndReturn();
#if !DEBUG
            var before = GC.GetAllocatedBytesForCurrentThread();
#endif
            for (var i = 0; i < 100; i++)
                RentAndReturn();
#if !DEBUG
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            ClassicAssert.AreEqual(0, allocated);
#endif

            void RentAndReturn()
            {
                for (var i = 0; i < rented.Length; i++)
                    rented[i] = writer.RentRequestBuffer(40960);
                for (var i = 0; i < rented.Length; i++)
                    rented[i].Dispose();
            }
        }

        [Test]
        public async Task OutOfLineAdmissionAfterRetireIsRejectedWhenQueueWasNeverCreated()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetClient();
            await db.ConnectAsync().ConfigureAwait(false);

            var writer = GetNetworkWriter(db);
            ClassicAssert.IsNull(GetMemoryThrottle(writer));
            ClassicAssert.IsTrue(writer.TryRetire());

            Assert.Throws<ObjectDisposedException>(() => writer.AdmitOutOfLineRental(64, CancellationToken.None));
            Assert.ThrowsAsync<ObjectDisposedException>(async () => await writer.RentMemoryThrottle(64, CancellationToken.None).ConfigureAwait(false));
        }

        [Test]
        public async Task ReconnectFaultsOperationWaitingOnMemoryAdmission([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            const int memoryCapacity = 512;
            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 8,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: memoryCapacity,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(useTLS: useTLS, networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var writer = GetNetworkWriter(db);
            var allocationSize = writer.GetRequestBufferAllocationSize(300);
            ClassicAssert.AreEqual(memoryCapacity, allocationSize);
            ClassicAssert.IsTrue(writer.AdmitOutOfLineRental(allocationSize, CancellationToken.None));

            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var pending = db.ExecuteForStringResultWithCancellationAsync(ECHO, [new string('x', 300)], cts.Token);
                var memoryThrottle = GetMemoryThrottle(writer);
                ClassicAssert.IsTrue(
                    SpinWait.SpinUntil(() => memoryThrottle.WaiterCount == 1, TimeSpan.FromSeconds(5)),
                    "The operation did not wait for out-of-line memory admission.");

                await AssertReconnectUnblocksPendingOperation(db, pending, cts.Token).ConfigureAwait(false);
            }
            finally
            {
                writer.ReleaseOutOfLineRental(allocationSize);
            }
        }

        [Test]
        public async Task ReconnectFaultsNoResponseOperationWaitingOnMemoryAdmission([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true, enableTLS: useTLS);
            server.Start();

            const int memoryCapacity = 512;
            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 8,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: memoryCapacity,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(useTLS: useTLS, networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var writer = GetNetworkWriter(db);
            var allocationSize = writer.GetRequestBufferAllocationSize(300);
            ClassicAssert.AreEqual(memoryCapacity, allocationSize);
            ClassicAssert.IsTrue(writer.AdmitOutOfLineRental(allocationSize, CancellationToken.None));

            try
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var pending = Task.Run(
                    () => db.ClusterPublishNoResponse("channel"u8.ToArray(), Encoding.ASCII.GetBytes(new string('x', 300))),
                    cts.Token);
                var memoryThrottle = GetMemoryThrottle(writer);
                ClassicAssert.IsTrue(
                    SpinWait.SpinUntil(() => memoryThrottle.WaiterCount == 1, TimeSpan.FromSeconds(5)),
                    "The no-response operation did not wait for out-of-line memory admission.");

                await db.ReconnectAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(5), cts.Token).ConfigureAwait(false);
                Assert.ThrowsAsync<ObjectDisposedException>(async () =>
                    await pending.WaitAsync(TimeSpan.FromSeconds(5), cts.Token).ConfigureAwait(false));
                ClassicAssert.AreEqual(
                    "PONG",
                    await db.PingAsync(cts.Token).WaitAsync(TimeSpan.FromSeconds(5), cts.Token).ConfigureAwait(false));
            }
            finally
            {
                writer.ReleaseOutOfLineRental(allocationSize);
            }
        }

        [Test]
        public async Task ReconnectFaultsOperationWaitingOnCompletionCapacity([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 2,
                maxOutstandingCompletions: 1,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 0,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(useTLS: useTLS, networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var writer = GetNetworkWriter(db);
            var recordSize = writer.GetRecordSize(1, out _);
            writer.epoch.Resume();
            try
            {
                ClassicAssert.IsTrue(writer.TryScheduleSend(
                    recordSize,
                    expectsResponse: true,
                    out _,
                    out _));
            }
            finally
            {
                writer.epoch.Suspend();
            }

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var pending = db.PingAsync(cts.Token);
            await Task.Delay(50, cts.Token).ConfigureAwait(false);
            ClassicAssert.IsFalse(pending.IsCompleted, "The operation did not wait for completion-lane capacity.");

            await AssertReconnectUnblocksPendingOperation(db, pending, cts.Token).ConfigureAwait(false);
        }

        [Test]
        public void BufferPoolIdleByteCapDropsExcessEntries()
        {
            using var pool = new LimitedFixedBufferPool(
                minAllocationSize: 64,
                maxEntriesPerLevel: 16,
                numLevels: 4,
                maxPooledBytes: 192);

            var small = pool.Get(64);
            var medium = pool.Get(128);
            var large = pool.Get(256);
            small.Dispose();
            medium.Dispose();
            large.Dispose();

            ClassicAssert.AreEqual(192, pool.PooledBytes);

            var reused = pool.Get(128);
            ClassicAssert.AreEqual(64, pool.PooledBytes);
            reused.Dispose();
            ClassicAssert.AreEqual(192, pool.PooledBytes);

            pool.Purge();
            ClassicAssert.AreEqual(0, pool.PooledBytes);

            Parallel.For(0, 10_000, i =>
            {
                var size = 64 << (i % 3);
                pool.Get(size).Dispose();
            });
            ClassicAssert.LessOrEqual(pool.PooledBytes, 192);
            pool.Purge();
            ClassicAssert.AreEqual(0, pool.PooledBytes);
        }

        [Test]
        public void DefaultNetworkWriterMemoryFootprint()
        {
            var options = NetworkWriterOptions.Default;

            ClassicAssert.AreEqual(64L << 20, options.MaxOutOfLineRentedBytes);
            ClassicAssert.AreEqual(options.MaxOutstandingCompletions, options.MaxOutstandingRequests);
            ClassicAssert.AreEqual(16, options.MaxOutstandingRequests);
            ClassicAssert.AreEqual(4096, options.RequestPageSizeBytes);
            ClassicAssert.AreEqual(9_728, options.MinMemoryFootprint());
            ClassicAssert.AreEqual(10_624, options.MaxMemoryFootprint());

            var perOperationOptions = new NetworkWriterOptions(
                networkBufferSizeBytes: 1 << 13,
                requestPageSizeBytes: 1 << 12,
                requestPageCount: 2,
                maxOutstandingRequests: 1 << 4,
                maxOutstandingCompletions: 1 << 4,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 64L << 20,
                flushResultAllocationMode: FlushResultAllocationMode.PerOperation);
            ClassicAssert.AreEqual(9_728, perOperationOptions.MinMemoryFootprint());
            ClassicAssert.AreEqual(10_624, perOperationOptions.MaxMemoryFootprint());

            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => new NetworkWriterOptions(
                networkBufferSizeBytes: 1 << 13,
                requestPageSizeBytes: 1 << 12,
                requestPageCount: 2,
                maxOutstandingRequests: 1 << 4,
                maxOutstandingCompletions: 1 << 4,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 64L << 20,
                flushResultAllocationMode: (FlushResultAllocationMode)(-1)));
            ClassicAssert.AreEqual("flushResultAllocationMode", exception.ParamName);

            var boundedRequestOptions = new NetworkWriterOptions(
                networkBufferSizeBytes: 1 << 13,
                requestPageSizeBytes: 1 << 10,
                requestPageCount: 2,
                maxOutstandingRequests: 8,
                maxOutstandingCompletions: 1 << 6,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 64L << 20,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            ClassicAssert.AreEqual(6_400, boundedRequestOptions.MinMemoryFootprint());
            ClassicAssert.AreEqual(6_848, boundedRequestOptions.MaxMemoryFootprint());

            // Request capacity counts requests, independent of page geometry; the side table rounds up to a power of two.
            var oddRequestOptions = new NetworkWriterOptions(
                1 << 13, 1 << 10, 2, 5, 1 << 6, 8, 1024, FlushResultAllocationMode.Buffered);
            ClassicAssert.AreEqual(5, oddRequestOptions.MaxOutstandingRequests);
            ClassicAssert.AreEqual(6_400, oddRequestOptions.MinMemoryFootprint());

            var positionalOptions = new NetworkWriterOptions(
                1 << 13, 1 << 10, 2, 64, 64, 8, 1024, FlushResultAllocationMode.Buffered);
            ClassicAssert.AreEqual(64, positionalOptions.MaxOutstandingRequests);
            ClassicAssert.AreEqual(64, positionalOptions.MaxOutstandingCompletions);
            ClassicAssert.AreEqual(8, positionalOptions.MaxConcurrentNetworkSends);
            ClassicAssert.AreEqual(1024, positionalOptions.MaxOutOfLineRentedBytes);
        }

        [Test]
        public async Task ClientMemoryUsageTracksOutOfLineReservations()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            const int maxOutOfLineRentedBytes = 512;
            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 8,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: maxOutOfLineRentedBytes,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(networkWriterOptions: options);

            ClassicAssert.AreEqual(0, db.ActiveMemoryUsageBytes);
            ClassicAssert.AreEqual(
                options.MaxMemoryFootprint() + maxOutOfLineRentedBytes,
                db.MaxMemoryUsageBytes);

            await db.ConnectAsync().ConfigureAwait(false);
            ClassicAssert.AreEqual(options.MinMemoryFootprint(), db.ActiveMemoryUsageBytes);

            var writer = GetNetworkWriter(db);
            ClassicAssert.IsTrue(writer.AdmitOutOfLineRental(maxOutOfLineRentedBytes, CancellationToken.None));
            try
            {
                ClassicAssert.AreEqual(
                    options.MinMemoryFootprint() + maxOutOfLineRentedBytes,
                    db.ActiveMemoryUsageBytes);
            }
            finally
            {
                writer.ReleaseOutOfLineRental(maxOutOfLineRentedBytes);
            }

            ClassicAssert.AreEqual(options.MinMemoryFootprint(), db.ActiveMemoryUsageBytes);
            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
            ClassicAssert.Greater(db.ActiveMemoryUsageBytes, options.MinMemoryFootprint());
            ClassicAssert.LessOrEqual(db.ActiveMemoryUsageBytes, options.MaxMemoryFootprint());
        }

        [Test]
        public async Task PerOperationFlushResultMemoryReturnsToBaseline()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 8,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 512,
                flushResultAllocationMode: FlushResultAllocationMode.PerOperation);
            using var db = TestUtils.GetGarnetClient(networkWriterOptions: options);

            await db.ConnectAsync().ConfigureAwait(false);
            var baseline = options.MinMemoryFootprint();
            ClassicAssert.AreEqual(baseline, db.ActiveMemoryUsageBytes);

            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
            ClassicAssert.AreEqual(baseline, db.ActiveMemoryUsageBytes);
        }

        [Test]
        public void RequestPageSizeMustFitRecordHeader()
        {
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: DuplexRingRecordFormat.HeaderSize - 1,
                requestPageCount: 2,
                maxOutstandingRequests: 2,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 0,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered));

            ClassicAssert.AreEqual("requestPageSizeBytes", exception.ParamName);

            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: DuplexRingRecordFormat.HeaderSize,
                requestPageCount: 2,
                maxOutstandingRequests: 2,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 0,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            ClassicAssert.AreEqual(DuplexRingRecordFormat.HeaderSize, options.RequestPageSizeBytes);
        }

        [TestCase(0)]
        [TestCase(-1)]
        [TestCase(1 << PageAndOffset.kRequestBits)]
        public void MaxOutstandingRequestsMustFitRequestIdSpace(int maxOutstandingRequests)
        {
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => CreateRequestCountOptions(maxOutstandingRequests));
            ClassicAssert.AreEqual("maxOutstandingRequests", exception.ParamName);
        }

        [TestCase(1)]
        [TestCase(3)]
        [TestCase(10)]
        [TestCase(512)]
        [TestCase(PageAndOffset.MaxOutstandingRequests)]
        public void MaxOutstandingRequestsIsIndependentOfPageGeometry(int maxOutstandingRequests)
            => ClassicAssert.AreEqual(maxOutstandingRequests, CreateRequestCountOptions(maxOutstandingRequests).MaxOutstandingRequests);

        [TestCase(0)]
        [TestCase(3)]
        [TestCase(PageAndOffset.MaxOutstandingCompletions + 1)]
        [TestCase(1 << PageAndOffset.kTaskBits)]
        public void MaxOutstandingCompletionsRejectsInvalidCapacity(int maxOutstandingCompletions)
        {
            var exception = Assert.Throws<ArgumentOutOfRangeException>(() => CreateCompletionCountOptions(maxOutstandingCompletions));
            ClassicAssert.AreEqual("maxOutstandingCompletions", exception.ParamName);
        }

        [Test]
        public void MaxOutstandingCompletionsAcceptsLargestDistinguishableCapacity()
            => ClassicAssert.AreEqual(
                PageAndOffset.MaxOutstandingCompletions,
                CreateCompletionCountOptions(PageAndOffset.MaxOutstandingCompletions).MaxOutstandingCompletions);

        static NetworkWriterOptions CreateCompletionCountOptions(int maxOutstandingCompletions)
            => new(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 8,
                maxOutstandingCompletions: maxOutstandingCompletions,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 512,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);

        static NetworkWriterOptions CreateRequestCountOptions(int maxOutstandingRequests)
            => new(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: maxOutstandingRequests,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 512,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);

        [Test]
        public async Task RequestMemoryThrottleRejectsOversizedOutOfLineRequestWithoutLeak([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 8,
                maxOutstandingCompletions: 8,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 512,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(useTLS: useTLS, networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var exception = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await db.ExecuteForStringResultAsync(ECHO, [new string('x', 700)]).ConfigureAwait(false));

            StringAssert.Contains("1024 bytes exceeds the configured maximum of 512 bytes", exception.Message);
            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
        }

        [Test]
        public async Task RequestMemoryThrottleStabilizesConcurrentOutOfLineRequests([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            const int capacityBytes = 2048;
            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 16,
                maxOutstandingCompletions: 16,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: capacityBytes,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(useTLS: useTLS, networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            const int producers = 8;
            const int requestsPerProducer = 50;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var tasks = new Task[producers];
            for (var producer = 0; producer < producers; producer++)
            {
                var id = producer;
                tasks[producer] = Task.Run(async () =>
                {
                    for (var request = 0; request < requestsPerProducer; request++)
                    {
                        var value = $"{id}:{request}:{new string((char)('a' + id), 700)}";
                        var result = await db.ExecuteForStringResultWithCancellationAsync(ECHO, [value], cts.Token).ConfigureAwait(false);
                        ClassicAssert.AreEqual(value, result);
                    }
                }, cts.Token);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            var finalValue = new string('z', 700);
            ClassicAssert.AreEqual(
                finalValue,
                await db.ExecuteForStringResultWithCancellationAsync(ECHO, [finalValue], cts.Token).ConfigureAwait(false));
        }

        [Test]
        public async Task ChunkedLargePayloadTest([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            // Force small pages so the payload spans several chunks/pages.
            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 1 << 17,
                requestPageSizeBytes: 1 << 12,
                requestPageCount: 2,
                maxOutstandingRequests: 64,
                maxOutstandingCompletions: PageAndOffset.MaxOutstandingCompletions,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 0,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(useTLS: useTLS, networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var value = new string('x', 1 << 16);
            var setResult = await db.ExecuteForStringResultAsync(SET, ["bigkey", value]).ConfigureAwait(false);
            ClassicAssert.AreEqual("OK", setResult);

            var getResult = await db.ExecuteForStringResultAsync(GET, ["bigkey"]).ConfigureAwait(false);
            ClassicAssert.AreEqual(value, getResult);
        }

        [Test]
        public async Task PublishNoSubscribersReturnsZero()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var db = TestUtils.GetGarnetClient();
            await db.ConnectAsync().ConfigureAwait(false);

            var received = await db.PublishAsync(Encoding.ASCII.GetBytes("channel"), Encoding.ASCII.GetBytes("hello")).ConfigureAwait(false);
            ClassicAssert.AreEqual(0, received);
        }

        [Test]
        public async Task PublishDeliversToSubscriber()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            using var subscriber = ConnectionMultiplexer.Connect(TestUtils.GetConfig());
            var messages = new BlockingCollection<string>();
            await subscriber.GetSubscriber().SubscribeAsync(RedisChannel.Literal("news"),
                (_, message) => messages.Add(message)).ConfigureAwait(false);

            using var db = TestUtils.GetGarnetClient();
            await db.ConnectAsync().ConfigureAwait(false);

            long received = 0;
            // The subscription registration on the server may momentarily lag the SUBSCRIBE round-trip.
            for (var attempt = 0; attempt < 50 && received == 0; attempt++)
            {
                received = await db.PublishAsync(Encoding.ASCII.GetBytes("news"), Encoding.ASCII.GetBytes("breaking")).ConfigureAwait(false);
                if (received == 0) await Task.Delay(20).ConfigureAwait(false);
            }

            ClassicAssert.AreEqual(1, received);
            ClassicAssert.IsTrue(messages.TryTake(out var delivered, TimeSpan.FromSeconds(5)));
            ClassicAssert.AreEqual("breaking", delivered);
        }

        [Test]
        public async Task NoResponsePreservesCompletionAlignment(
            [Values(16, 512)] int messageLength,
            [Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true, enableTLS: useTLS);
            server.Start();

            using var subscriber = ConnectionMultiplexer.Connect(TestUtils.GetConfig(useTLS: useTLS));
            var messages = new BlockingCollection<string>();
            await subscriber.GetSubscriber().SubscribeAsync(RedisChannel.Literal("no-response-news"),
                (_, message) => messages.Add(message)).ConfigureAwait(false);

            var options = new NetworkWriterOptions(
                networkBufferSizeBytes: 256,
                requestPageSizeBytes: 256,
                requestPageCount: 2,
                maxOutstandingRequests: 64,
                maxOutstandingCompletions: 1 << 12,
                maxConcurrentNetworkSends: 8,
                maxOutOfLineRentedBytes: 1024,
                flushResultAllocationMode: FlushResultAllocationMode.Buffered);
            using var db = TestUtils.GetGarnetClient(useTLS: useTLS, networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            var channel = Encoding.ASCII.GetBytes("no-response-news");
            var sent = new HashSet<string>();
            string delivered = null;
            for (var attempt = 0; attempt < 10 && delivered == null; attempt++)
            {
                var message = $"{attempt:D2}-{new string('x', messageLength)}";
                sent.Add(message);
                db.ClusterPublishNoResponse(channel, Encoding.ASCII.GetBytes(message));

                // PING is ordered after the no-response command, so PONG confirms both that the publish was
                // processed and that it did not consume or misalign a completion ticket.
                ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
                if (messages.TryTake(out var candidate, TimeSpan.FromMilliseconds(100)) && sent.Contains(candidate))
                    delivered = candidate;
            }

            ClassicAssert.IsNotNull(delivered, "No fire-and-forget publish reached the subscriber.");
        }

        [Test]
        public async Task DisabledClusterPublishProducesNoResponse([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true, disablePubSub: true, enableTLS: useTLS);
            server.Start();

            using var db = TestUtils.GetGarnetClient(useTLS: useTLS);
            await db.ConnectAsync().ConfigureAwait(false);

            db.ClusterPublishNoResponse("channel"u8.ToArray(), "message"u8.ToArray());
            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));
        }

        [Test]
        public async Task GossipWithMeetReturnsConfig([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableCluster: true, enableTLS: useTLS);
            server.Start();

            using var db = TestUtils.GetGarnetClient(useTLS: useTLS);
            await db.ConnectAsync().ConfigureAwait(false);

            // WITHMEET always elicits a response carrying the node's serialized configuration, exercising the
            // binary-safe (MemoryResult) reply path even with an empty outbound payload.
            using var response = await db.GossipWithMeetAsync(Memory<byte>.Empty).ConfigureAwait(false);
            ClassicAssert.Greater(response.Length, 0);
        }

        /// <summary>
        /// Graceful disposal on an idle client completes promptly, marks the client disposed, and
        /// rejects any subsequent operation with <see cref="GarnetClientDisposedException"/> — the same
        /// contract the gossip node relies on when it retires a connection.
        /// </summary>
        [Test]
        public async Task DisposeGracefullyAsync_CompletesAndRejectsNewOperations()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            var db = TestUtils.GetGarnetClient();
            await db.ConnectAsync().ConfigureAwait(false);
            ClassicAssert.AreEqual("PONG", await db.PingAsync().ConfigureAwait(false));

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await db.DisposeGracefullyAsync(cts.Token).ConfigureAwait(false);

            ClassicAssert.IsTrue(db.Disposed);
            _ = Assert.ThrowsAsync<GarnetClientDisposedException>(async () =>
                await db.ExecuteForStringResultAsync(GET, ["anykey"]).ConfigureAwait(false));
        }

        /// <summary>
        /// Graceful disposal must not abort or strand requests that are already in flight. Requests admitted to
        /// the ring before retirement flush and return their replies intact and in order; requests still queued
        /// behind backpressure observe the retirement signal and are rejected cleanly with
        /// <see cref="GarnetClientDisposedException"/> rather than aborted mid-flight or left to hang.
        /// </summary>
        [Test]
        public async Task DisposeGracefullyAsync_DrainsInFlightRequests([Values] bool useTLS)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            var db = TestUtils.GetGarnetClient(useTLS: useTLS);
            await db.ConnectAsync().ConfigureAwait(false);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            const int requestCount = 256;
            var expected = new string[requestCount];
            var pending = new Task<string>[requestCount];
            for (var i = 0; i < requestCount; i++)
            {
                var value = $"{i}:{new string((char)('a' + (i % 26)), 200)}";
                expected[i] = value;
                pending[i] = db.ExecuteForStringResultWithCancellationAsync(ECHO, [value], cts.Token);
            }

            // Start graceful disposal while the batch is still outstanding; already-admitted requests must
            // drain, and any still queued behind backpressure must reject cleanly rather than abort or hang.
            var disposeTask = db.DisposeGracefullyAsync(cts.Token).AsTask();

            var completed = 0;
            for (var i = 0; i < requestCount; i++)
            {
                try
                {
                    var result = await pending[i].WaitAsync(TimeSpan.FromSeconds(30), cts.Token).ConfigureAwait(false);
                    ClassicAssert.AreEqual(expected[i], result);
                    completed++;
                }
                catch (GarnetClientDisposedException)
                {
                    // Accepted but still queued behind backpressure at retirement: rejected cleanly.
                }
            }

            await disposeTask.WaitAsync(TimeSpan.FromSeconds(30), cts.Token).ConfigureAwait(false);

            ClassicAssert.Greater(completed, 0);
            ClassicAssert.IsTrue(db.Disposed);
        }

        /// <summary>
        /// Graceful disposal is safe to call on an already-disposed client and returns without throwing.
        /// </summary>
        [Test]
        public async Task DisposeGracefullyAsync_OnDisposedClientIsNoOp()
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir);
            server.Start();

            var db = TestUtils.GetGarnetClient();
            await db.ConnectAsync().ConfigureAwait(false);
            db.Dispose();

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            Assert.DoesNotThrowAsync(async () => await db.DisposeGracefullyAsync(cts.Token).ConfigureAwait(false));
            ClassicAssert.IsTrue(db.Disposed);
        }

        /// <summary>
        /// End-to-end stress across plain TCP and TLS. Concurrent producers pipeline a mix of inline requests,
        /// requests straddling the inline threshold, out-of-line requests, and payloads larger than a TLS record,
        /// alongside a shared counter. A small ring forces inline-buffer wrap, coalesced inline runs interleaved
        /// with out-of-line sends, and request/completion backpressure. Every reply must match its request and the
        /// connection must remain usable after a reconnect.
        /// </summary>
        [Test, NonParallelizable]
        public async Task MixedPayloadPipelineStressTest([Values] bool useTLS, [Values] bool smallRing)
        {
            using var server = TestUtils.CreateGarnetServer(TestUtils.MethodTestDir, enableTLS: useTLS);
            server.Start();

            NetworkWriterOptions? options = smallRing
                ? new NetworkWriterOptions(
                    networkBufferSizeBytes: 256,
                    requestPageSizeBytes: 256,
                    requestPageCount: 2,
                    maxOutstandingRequests: 32,
                    maxOutstandingCompletions: 64,
                    maxConcurrentNetworkSends: 8,
                    maxOutOfLineRentedBytes: 0,
                    flushResultAllocationMode: FlushResultAllocationMode.Buffered)
                : null;
            using var db = TestUtils.GetGarnetClient(useTLS: useTLS, networkWriterOptions: options);
            await db.ConnectAsync().ConfigureAwait(false);

            const int producers = 8;
            const int batchesPerProducer = 25;
            const int batchSize = 24;
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
            var incrCount = 0L;
            var tasks = new Task[producers];

            for (var p = 0; p < producers; p++)
            {
                var producer = p;
                tasks[p] = Task.Run(async () =>
                {
                    var random = new Random(producer + 1);
                    var pending = new Task<string>[batchSize];
                    var expected = new string[batchSize];

                    for (var batch = 0; batch < batchesPerProducer; batch++)
                    {
                        for (var i = 0; i < batchSize; i++)
                        {
                            var kind = random.Next(100);
                            if (kind < 10)
                            {
                                _ = Interlocked.Increment(ref incrCount);
                                expected[i] = null;
                                pending[i] = IncrAsString(db, cts.Token);
                                continue;
                            }

                            var length = kind switch
                            {
                                < 55 => random.Next(0, 64),
                                < 75 => random.Next(200, 320),
                                < 95 => random.Next(400, 4096),
                                _ => random.Next(16 << 10, 40 << 10),
                            };
                            var value = $"{producer}:{batch}:{i}:{new string((char)('a' + ((producer + i) % 26)), length)}";
                            expected[i] = value;
                            pending[i] = db.ExecuteForStringResultWithCancellationAsync(ECHO, [value], cts.Token);
                        }

                        var results = await Task.WhenAll(pending).ConfigureAwait(false);
                        for (var i = 0; i < batchSize; i++)
                        {
                            if (expected[i] != null)
                                ClassicAssert.AreEqual(expected[i], results[i], $"Reply mismatch for producer {producer}, batch {batch}, request {i}.");
                        }
                    }
                }, cts.Token);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);

            ClassicAssert.AreEqual(
                Interlocked.Read(ref incrCount),
                await db.ExecuteForLongResultAsync(GET, ["stress-counter"]).ConfigureAwait(false));

            await db.ReconnectAsync(cts.Token).ConfigureAwait(false);
            var finalValue = new string('z', 20 << 10);
            ClassicAssert.AreEqual(
                finalValue,
                await db.ExecuteForStringResultWithCancellationAsync(ECHO, [finalValue], cts.Token).ConfigureAwait(false));

            static async Task<string> IncrAsString(GarnetClient client, CancellationToken token)
            {
                var value = await client.ExecuteForLongResultWithCancellationAsync(INCR, ["stress-counter"], token).ConfigureAwait(false);
                ClassicAssert.Greater(value, 0);
                return null;
            }
        }

        static NetworkWriter GetNetworkWriter(GarnetClient client)
            => (NetworkWriter)typeof(GarnetClient)
                .GetField("networkWriter", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(client);

        static WaiterQueue<MemoryThrottle, int> GetMemoryThrottle(NetworkWriter writer)
            => (WaiterQueue<MemoryThrottle, int>)typeof(NetworkWriter)
                .GetField("memoryThrottle", BindingFlags.NonPublic | BindingFlags.Instance)
                .GetValue(writer);

        static async Task AssertReconnectUnblocksPendingOperation(
            GarnetClient client,
            Task<string> pending,
            CancellationToken token)
        {
            await client.ReconnectAsync(token).WaitAsync(TimeSpan.FromSeconds(5), token).ConfigureAwait(false);

            try
            {
                _ = await pending.WaitAsync(TimeSpan.FromSeconds(5), token).ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
            }

            ClassicAssert.AreEqual(
                "PONG",
                await client.PingAsync(token).WaitAsync(TimeSpan.FromSeconds(5), token).ConfigureAwait(false));
        }
    }
}