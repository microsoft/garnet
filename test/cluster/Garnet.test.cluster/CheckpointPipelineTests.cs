// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.cluster;
using Garnet.common;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    [TestFixture]
    public class CheckpointPipelineTests : TestBase
    {
        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [TestCase(1)]
        [TestCase(3)]
        [TestCase(8)]
        public async Task WindowIsBoundedAndEndMarkerWaitsForEveryAcknowledgement(int depth)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            using var client = new GarnetClientSession(listener.LocalEndpoint, new NetworkBufferSettings(1 << 20, 4096));
            var accept = listener.AcceptTcpClientAsync(timeout.Token);
            await client.ConnectAsync(token: timeout.Token);
            using var peer = await accept;
            using var stream = peer.GetStream();
            var source = new TestSource(depth * 2 + 1);
            using var transmitter = new FileTransmitSource(source, pipelineDepth: depth);
            var transfer = transmitter.TransmitAsync(client, TimeSpan.FromSeconds(10), timeout.Token);
            long outstandingBytes = 0;
            long maximumOutstandingBytes = 0;
            for (var received = 0; received < source.ChunkCount; received++)
            {
                var (address, data) = await ReadChunkAsync(stream, timeout.Token);
                Assert.That(address, Is.EqualTo(received * TestSource.ChunkSize));
                Assert.That(data.Length, Is.EqualTo(source.GetChunkLength(received)));
                Assert.That(data, Is.All.EqualTo((byte)(received + 1)));
                outstandingBytes += data.Length;
                maximumOutstandingBytes = Math.Max(maximumOutstandingBytes, outstandingBytes);

                if (received + 1 >= depth)
                {
                    if (received + 1 == depth)
                    {
                        await Task.Delay(50, timeout.Token);
                        Assert.That(source.ReadCount, Is.EqualTo(depth), "Reading beyond the outstanding window");
                        Assert.That(peer.Available, Is.Zero, "Sending beyond the outstanding window");
                    }
                    if (received + 1 < source.ChunkCount)
                    {
                        var acknowledged = received + 1 - depth;
                        await ReplyAsync(stream, "+OK\r\n", timeout.Token);
                        outstandingBytes -= source.GetChunkLength(acknowledged);
                    }
                }
            }
            Assert.That(maximumOutstandingBytes, Is.LessThanOrEqualTo((long)depth * TestSource.ChunkSize));
            Assert.That(maximumOutstandingBytes, Is.GreaterThan((long)(depth - 1) * TestSource.ChunkSize));
            await Task.Delay(50, timeout.Token);
            Assert.That(peer.Available, Is.Zero, "End marker sent before all chunks were acknowledged");
            Assert.That(transfer.IsCompleted, Is.False);
            for (var remaining = 0; remaining < depth; remaining++)
                await ReplyAsync(stream, "+OK\r\n", timeout.Token);
            var (endAddress, endData) = await ReadChunkAsync(stream, timeout.Token);
            Assert.That(endAddress, Is.EqualTo(source.EndOffset));
            Assert.That(endData, Is.Empty);
            Assert.That(transfer.IsCompleted, Is.False, "End marker must also be acknowledged");
            await ReplyAsync(stream, "+OK\r\n", timeout.Token);
            await transfer.WaitAsync(timeout.Token);
        }

        [TestCase("error")]
        [TestCase("unexpected")]
        [TestCase("disconnect")]
        [TestCase("timeout")]
        [TestCase("cancel")]
        public async Task FailedWindowAbortsWithoutEndMarker(string failure)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            using var cancel = new CancellationTokenSource();
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            using var client = new GarnetClientSession(listener.LocalEndpoint, new NetworkBufferSettings(1 << 20, 4096));
            var accept = listener.AcceptTcpClientAsync(timeout.Token);
            await client.ConnectAsync(token: timeout.Token);
            using var peer = await accept;
            using var stream = peer.GetStream();
            var source = new TestSource(12);
            using var transmitter = new FileTransmitSource(source, pipelineDepth: 3);
            var transfer = transmitter.TransmitAsync(client, failure == "timeout" ? TimeSpan.FromSeconds(1) : TimeSpan.FromSeconds(10), cancel.Token);
            for (var i = 0; i < 3; i++)
                _ = await ReadChunkAsync(stream, timeout.Token);
            switch (failure)
            {
                case "error":
                    await ReplyAsync(stream, "-ERR injected checkpoint failure\r\n", timeout.Token);
                    break;
                case "unexpected":
                    await ReplyAsync(stream, "+NOT_OK\r\n", timeout.Token);
                    break;
                case "disconnect":
                    peer.Dispose();
                    break;
                case "cancel":
                    await cancel.CancelAsync();
                    break;
            }
            var exception = await CaptureFailureAsync(transfer.WaitAsync(timeout.Token));
            Assert.That(source.ReadCount, Is.EqualTo(3));
            if (failure == "timeout")
                Assert.That(exception, Is.TypeOf<TimeoutException>());
            if (failure == "cancel")
                Assert.That(exception, Is.InstanceOf<OperationCanceledException>());
            if (failure is "error" or "unexpected")
                Assert.That(exception.Message, Does.Contain(failure == "error" ? "injected checkpoint failure" : "NOT_OK"));
            if (failure != "disconnect")
            {
                var trailing = new byte[1];
                try
                {
                    Assert.That(await stream.ReadAsync(trailing, timeout.Token), Is.Zero, "Connection remains open or sent data after failure");
                }
                catch (IOException ex) when (ex.InnerException is SocketException { SocketErrorCode: SocketError.ConnectionReset or SocketError.ConnectionAborted })
                {
                    // Closing a socket with unacknowledged responses may reset rather than gracefully close it.
                }
            }
        }

        [TestCase("timeout", false)]
        [TestCase("timeout", true)]
        [TestCase("error", true)]
        [TestCase("cancel", true)]
        public async Task AcknowledgementFailureCancelsAnOverlappingRead(string failure, bool acknowledgeFirst)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            using var cancel = new CancellationTokenSource();
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            using var client = new GarnetClientSession(listener.LocalEndpoint, new NetworkBufferSettings(1 << 20, 4096));
            var accept = listener.AcceptTcpClientAsync(timeout.Token);
            await client.ConnectAsync(token: timeout.Token);
            using var peer = await accept;
            using var stream = peer.GetStream();
            var source = new TestSource(6) { BlockReadAt = acknowledgeFirst ? 3 : 2 };
            using var transmitter = new FileTransmitSource(source, pipelineDepth: 3);
            var transfer = transmitter.TransmitAsync(client,
                failure == "timeout" ? TimeSpan.FromSeconds(1) : TimeSpan.FromSeconds(10), cancel.Token);
            _ = await ReadChunkAsync(stream, timeout.Token);
            _ = await ReadChunkAsync(stream, timeout.Token);
            if (acknowledgeFirst)
            {
                _ = await ReadChunkAsync(stream, timeout.Token);
                await ReplyAsync(stream, "+OK\r\n", timeout.Token);
            }
            await source.ReadBlocked.Task.WaitAsync(timeout.Token);
            if (failure == "error")
                await ReplyAsync(stream, "-ERR overlapping read\r\n", timeout.Token);
            if (failure == "cancel")
                await cancel.CancelAsync();
            var exception = await CaptureFailureAsync(transfer.WaitAsync(timeout.Token));
            if (failure == "timeout")
            {
                Assert.That(exception, Is.TypeOf<TimeoutException>(), "Preserve the acknowledgement failure instead of the read's cancellation");
                Assert.That(exception.Message, Does.Contain("Checkpoint acknowledgement timed out"));
            }
            if (failure == "error")
                Assert.That(exception.Message, Does.Contain("overlapping read"));
            if (failure == "cancel")
                Assert.That(exception, Is.InstanceOf<OperationCanceledException>());
            Assert.That(source.ReadCount, Is.EqualTo(source.BlockReadAt));
        }

        [TestCase(false)]
        [TestCase(true)]
        public async Task StalledSocketSendCanBeAborted(bool cancelTransfer)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            using var cancel = new CancellationTokenSource();
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Server.ReceiveBufferSize = 1024;
            listener.Start();
            using var client = new GarnetClientSession(listener.LocalEndpoint, new NetworkBufferSettings(512 << 10, 4096));
            var accept = listener.AcceptTcpClientAsync(timeout.Token);
            await client.ConnectAsync(token: timeout.Token);
            using var peer = await accept;
            var source = new TestSource(256, 256 << 10);
            using var transmitter = new FileTransmitSource(source, pipelineDepth: 64);
            var transfer = Task.Run(() => transmitter.TransmitAsync(client,
                cancelTransfer ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(1), cancel.Token));
            await source.FirstRead.Task.WaitAsync(timeout.Token);
            if (cancelTransfer)
            {
                await Task.Delay(100, timeout.Token);
                await cancel.CancelAsync();
            }
            var exception = await CaptureFailureAsync(transfer.WaitAsync(timeout.Token));
            Assert.That(exception, cancelTransfer ? Is.InstanceOf<OperationCanceledException>() : Is.TypeOf<TimeoutException>());
            Assert.That(source.ReadCount, Is.LessThan(64), "The unread socket must stall submission before filling the acknowledgement window");
        }

        [Test]
        public async Task ReadFailureIsNotReplacedByCancelledAcknowledgements()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            using var client = new GarnetClientSession(listener.LocalEndpoint, new NetworkBufferSettings(1 << 20, 4096));
            var accept = listener.AcceptTcpClientAsync(timeout.Token);
            await client.ConnectAsync(token: timeout.Token);
            using var peer = await accept;
            using var stream = peer.GetStream();
            var expected = new IOException("Injected checkpoint read failure");
            var source = new TestSource(6) { BlockReadAt = 2, ReadException = expected };
            using var transmitter = new FileTransmitSource(source, pipelineDepth: 3);
            var transfer = transmitter.TransmitAsync(client, TimeSpan.FromSeconds(10), timeout.Token);
            _ = await ReadChunkAsync(stream, timeout.Token);
            _ = await ReadChunkAsync(stream, timeout.Token);
            await source.ReadBlocked.Task.WaitAsync(timeout.Token);
            source.ResumeRead.SetResult();
            Assert.That(await CaptureFailureAsync(transfer.WaitAsync(timeout.Token)), Is.SameAs(expected));
            Assert.That(source.ReadCount, Is.EqualTo(2));
        }

        private static async Task<Exception> CaptureFailureAsync(Task task)
        {
            try
            {
                await task;
            }
            catch (Exception ex)
            {
                return ex;
            }
            Assert.Fail("Expected checkpoint transfer to fail");
            return null;
        }

        private static async Task ReplyAsync(NetworkStream stream, string response, CancellationToken token)
            => await stream.WriteAsync(Encoding.ASCII.GetBytes(response), token);

        private static async Task<(long Address, byte[] Data)> ReadChunkAsync(NetworkStream stream, CancellationToken token)
        {
            Assert.That(await ReadLineAsync(stream, token), Is.EqualTo("*6"));
            var args = new byte[6][];
            for (var i = 0; i < args.Length; i++)
            {
                var header = await ReadLineAsync(stream, token);
                Assert.That(header[0], Is.EqualTo('$'));
                var data = new byte[int.Parse(header.AsSpan(1))];
                await stream.ReadExactlyAsync(data, token);
                Assert.That(await ReadLineAsync(stream, token), Is.Empty);
                args[i] = data;
            }
            Assert.That(Encoding.ASCII.GetString(args[0]), Is.EqualTo("CLUSTER"));
            Assert.That(Encoding.ASCII.GetString(args[1]), Is.EqualTo("SNAPSHOT_DATA"));
            Assert.That(args[2], Is.EqualTo(Guid.Empty.ToByteArray()));
            Assert.That(int.Parse(Encoding.ASCII.GetString(args[3])), Is.EqualTo((int)CheckpointFileType.STORE_HLOG));
            return (long.Parse(Encoding.ASCII.GetString(args[4])), args[5]);
        }

        private static async Task<string> ReadLineAsync(NetworkStream stream, CancellationToken token)
        {
            var line = new StringBuilder();
            var one = new byte[1];
            for (; ; )
            {
                await stream.ReadExactlyAsync(one, token);
                if (one[0] == '\r')
                {
                    await stream.ReadExactlyAsync(one, token);
                    Assert.That(one[0], Is.EqualTo((byte)'\n'));
                    return line.ToString();
                }
                line.Append((char)one[0]);
                Assert.That(line.Length, Is.LessThan(128));
            }
        }

        private sealed class TestSource(int chunkCount, int chunkSize = TestSource.ChunkSize) : ISnapshotDataSource
        {
            internal const int ChunkSize = 4096;
            readonly SectorAlignedBufferPool pool = new(1, 512);
            internal readonly TaskCompletionSource FirstRead = new(TaskCreationOptions.RunContinuationsAsynchronously);
            internal readonly TaskCompletionSource ReadBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
            internal readonly TaskCompletionSource ResumeRead = new(TaskCreationOptions.RunContinuationsAsynchronously);
            internal int BlockReadAt = -1;
            internal Exception ReadException;
            internal int ReadCount;
            internal int ChunkCount => chunkCount;
            internal int GetChunkLength(int index) => index == chunkCount - 1 ? 512 : chunkSize;
            public CheckpointFileType Type => CheckpointFileType.STORE_HLOG;
            public Guid Token => Guid.Empty;
            public long StartOffset => 0;
            public long CurrentOffset { get; private set; }
            public long EndOffset => (chunkCount - 1L) * chunkSize + 512;
            public bool HasNextChunk => ReadCount < chunkCount;

            public async Task<DataSourceReadResult> ReadNextChunkAsync(CancellationToken cancellationToken = default)
            {
                if (ReadCount == BlockReadAt)
                {
                    ReadBlocked.SetResult();
                    await ResumeRead.Task.WaitAsync(cancellationToken);
                    if (ReadException != null)
                        throw ReadException;
                }
                cancellationToken.ThrowIfCancellationRequested();
                var size = GetChunkLength(ReadCount);
                var buffer = pool.Get(size);
                buffer.GetSlice(size).Fill((byte)(ReadCount + 1));
                var result = new DataSourceReadResult(buffer, size, CurrentOffset);
                CurrentOffset += size;
                ReadCount++;
                _ = FirstRead.TrySetResult();
                return result;
            }

            public void Dispose() => pool.Free();
        }
    }
}