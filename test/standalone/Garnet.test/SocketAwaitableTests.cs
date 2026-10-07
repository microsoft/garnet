// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Covers <see cref="GarnetSocketAwaitableEventArgs"/> against a real loopback socket pair. The
    /// properties under test are the ones the async receive loop depends on: a synchronously satisfied
    /// receive must not engage the completion source, one instance must serve a connection's whole
    /// lifetime, and a pending receive must resume exactly once when data arrives.
    /// </summary>
    [TestFixture]
    public class SocketAwaitableTests : TestBase
    {
        Socket listener;
        Socket client;
        Socket server;

        [SetUp]
        public void Setup()
        {
            listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            listener.Listen(1);

            client = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            client.Connect((IPEndPoint)listener.LocalEndPoint);
            server = listener.Accept();
        }

        [TearDown]
        public void TearDown()
        {
            client?.Dispose();
            server?.Dispose();
            listener?.Dispose();
            TestUtils.OnTearDown();
        }

        /// <summary>
        /// Data already buffered must complete the receive inline, with no completion source engaged --
        /// this is the property that keeps the common path free of async machinery.
        /// </summary>
        [Test]
        public async Task ADataReadyReceiveCompletesSynchronously()
        {
            client.Send([1, 2, 3, 4]);

            // Give the loopback stack a moment to make the bytes locally available.
            SpinWait.SpinUntil(() => server.Available >= 4, TimeSpan.FromSeconds(5));
            ClassicAssert.GreaterOrEqual(server.Available, 4, "test setup: bytes never arrived");

            using var awaitable = new GarnetSocketAwaitableEventArgs();
            var buffer = new byte[16];
            var vt = awaitable.ReceiveAsync(server, buffer);

            ClassicAssert.IsTrue(vt.IsCompletedSuccessfully,
                "A receive satisfied from the socket's own buffer must not go pending.");

            var result = await vt;
            ClassicAssert.IsFalse(result.HasError);
            ClassicAssert.AreEqual(4, result.BytesTransferred);
        }

        /// <summary>
        /// One instance must serve many receives; the async loop keeps exactly one per direction for the
        /// life of the connection.
        /// </summary>
        [Test]
        public async Task AnAwaitableIsReusedAcrossManyReceives()
        {
            using var awaitable = new GarnetSocketAwaitableEventArgs();
            var buffer = new byte[16];

            for (var i = 0; i < 50; i++)
            {
                client.Send([(byte)i]);
                var result = await awaitable.ReceiveAsync(server, buffer);
                ClassicAssert.IsFalse(result.HasError);
                ClassicAssert.AreEqual(1, result.BytesTransferred);
                ClassicAssert.AreEqual((byte)i, buffer[0]);
            }
        }

        /// <summary>
        /// A receive issued before any data exists must suspend and then resume exactly once, delivering
        /// the bytes that eventually arrive.
        /// </summary>
        [Test]
        public async Task APendingReceiveResumesWhenDataArrives()
        {
            using var awaitable = new GarnetSocketAwaitableEventArgs();
            var buffer = new byte[16];

            var vt = awaitable.ReceiveAsync(server, buffer);
            ClassicAssert.IsFalse(vt.IsCompletedSuccessfully,
                "With no data buffered the receive must go pending.");

            client.Send([7, 7, 7]);

            var result = await vt;
            ClassicAssert.IsFalse(result.HasError);
            ClassicAssert.AreEqual(3, result.BytesTransferred);
            ClassicAssert.AreEqual(7, buffer[0]);
        }

        /// <summary>
        /// A graceful remote close surfaces as a zero-byte receive rather than an error, which is how the
        /// loop detects end of stream.
        /// </summary>
        [Test]
        public async Task AGracefulCloseSurfacesAsZeroBytes()
        {
            using var awaitable = new GarnetSocketAwaitableEventArgs();
            var buffer = new byte[16];

            var vt = awaitable.ReceiveAsync(server, buffer);
            client.Shutdown(SocketShutdown.Both);
            client.Close();

            var result = await vt;
            ClassicAssert.IsFalse(result.HasError);
            ClassicAssert.AreEqual(0, result.BytesTransferred);
        }

        /// <summary>
        /// A receive the socket can satisfy immediately must allocate nothing. This is the property the
        /// hot path depends on: data already buffered costs no task, no box, and no delegate. The
        /// measured window contains only the awaitable, so socket sends and assertions cannot be
        /// mistaken for its cost.
        /// </summary>
        [Test]
        public void ASynchronousReceiveAllocatesNothing()
        {
            using var awaitable = new GarnetSocketAwaitableEventArgs();
            var buffer = new byte[1];

            const int Warmup = 50;
            const int Iterations = 200;

            // Pre-load every byte both passes will consume so the measured loop only receives.
            FillAvailable(Warmup + Iterations);

            for (var i = 0; i < Warmup; i++)
                _ = awaitable.ReceiveAsync(server, buffer).GetAwaiter().GetResult();

            var total = 0;
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (var i = 0; i < Iterations; i++)
            {
                var vt = awaitable.ReceiveAsync(server, buffer);
                if (!vt.IsCompletedSuccessfully)
                    break;
                total += vt.GetAwaiter().GetResult().BytesTransferred;
            }
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

            ClassicAssert.AreEqual(Iterations, total,
                "Every receive must have been satisfied synchronously from the socket buffer.");
            ClassicAssert.AreEqual(0, allocated,
                $"{Iterations} synchronous receives allocated {allocated} B; they must allocate nothing.");
        }

        /// <summary>
        /// Sends <paramref name="count"/> bytes and waits until the peer can see all of them.
        /// </summary>
        void FillAvailable(int count)
        {
            client.Send(new byte[count]);
            var deadline = Environment.TickCount64 + 10_000;
            while (server.Available < count && Environment.TickCount64 < deadline)
                Thread.Sleep(1);
            ClassicAssert.AreEqual(count, server.Available, "test setup: bytes never became available");
        }
    }
}