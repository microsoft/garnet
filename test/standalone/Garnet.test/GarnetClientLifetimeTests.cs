// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture, NonParallelizable]
    public class GarnetClientLifetimeTests : TestBase
    {
        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [Test]
        public async Task ReplyMemoryHasExactlyOneOwner([Values] bool array, [Values] bool cancel)
        {
            using var listener = new TcpListener(IPAddress.Loopback, TestUtils.TestPort);
            listener.Start();
            using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var cancellation = new CancellationTokenSource();
            using var pool = new TrackingPool();
            using var client = new GarnetClient(listener.LocalEndpoint, memoryPool: pool, useTimeoutChecker: false);
            var accept = listener.AcceptSocketAsync(guard.Token);
            await client.ConnectAsync(guard.Token);
            using var peer = await accept;
            using var stream = new NetworkStream(peer, ownsSocket: false);

            var pending = array
                ? client.ExecuteForMemoryResultArrayWithCancellationAsync("PING", token: cancellation.Token)
                : ReadScalarAsync();
            ClassicAssert.Greater(await peer.ReceiveAsync(new byte[256], SocketFlags.None, guard.Token), 0);
            if (cancel)
            {
                cancellation.Cancel();
                Assert.ThrowsAsync<TaskCanceledException>(async () => await pending.WaitAsync(guard.Token));
            }

            var next = client.ExecuteForStringResultAsync("PING");
            var reply = array ? "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n+PONG\r\n" : "$3\r\none\r\n+PONG\r\n";
            await stream.WriteAsync(Encoding.ASCII.GetBytes(reply), guard.Token);
            ClassicAssert.AreEqual("PONG", await next.WaitAsync(guard.Token));
            ClassicAssert.AreEqual(array ? 2 : 1, pool.Owners.Count);
            if (!cancel)
            {
                var results = await pending.WaitAsync(guard.Token);
                ClassicAssert.IsTrue(pool.Owners.All(owner => owner.Returns == 0));
                foreach (var result in results)
                    result.Dispose();
            }
            ClassicAssert.IsTrue(pool.Owners.All(owner => owner.Returns == 1));

            async Task<MemoryResult<byte>[]> ReadScalarAsync()
                => [await client.ExecuteForMemoryResultWithCancellationAsync("PING", token: cancellation.Token)];
        }

        [Test]
        public async Task MissingMemoryCallbackReturnsParsedBuffers([Values] bool array)
        {
            using var listener = new TcpListener(IPAddress.Loopback, TestUtils.TestPort);
            listener.Start();
            using var guard = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var pool = new TrackingPool();
            using var client = new GarnetClient(listener.LocalEndpoint, memoryPool: pool, useTimeoutChecker: false);
            var accept = listener.AcceptSocketAsync(guard.Token);
            await client.ConnectAsync(guard.Token);
            using var peer = await accept;
            using var stream = new NetworkStream(peer, ownsSocket: false);
            if (array)
                client.ExecuteForMemoryResultArray(null, 0, "PING");
            else
                client.ExecuteForMemoryResult(null, 0, "PING");
            var next = client.ExecuteForStringResultAsync("PING");
            var reply = array ? "*2\r\n$3\r\none\r\n$3\r\ntwo\r\n+PONG\r\n" : "$3\r\none\r\n+PONG\r\n";
            await stream.WriteAsync(Encoding.ASCII.GetBytes(reply), guard.Token);
            ClassicAssert.AreEqual("PONG", await next.WaitAsync(guard.Token));
            ClassicAssert.AreEqual(array ? 2 : 1, pool.Owners.Count);
            ClassicAssert.IsTrue(pool.Owners.All(owner => owner.Returns == 1));
        }

        sealed class TrackingPool : MemoryPool<byte>
        {
            internal readonly ConcurrentBag<Owner> Owners = [];
            public override int MaxBufferSize => int.MaxValue;

            public override IMemoryOwner<byte> Rent(int minBufferSize = -1)
            {
                var owner = new Owner(Math.Max(1, minBufferSize));
                Owners.Add(owner);
                return owner;
            }

            protected override void Dispose(bool disposing) { }

            internal sealed class Owner(int length) : IMemoryOwner<byte>
            {
                readonly byte[] buffer = new byte[length];
                int returns;
                internal int Returns => Volatile.Read(ref returns);
                public Memory<byte> Memory => Returns == 0 ? buffer : throw new ObjectDisposedException(nameof(Owner));
                public void Dispose() => Interlocked.Increment(ref returns);
            }
        }
    }
}