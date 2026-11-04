// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.cluster;
using Garnet.common;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    /// <summary>
    /// Replica side of disk-based sync with overlapped file writes: <see cref="OverlappedFileDataSink"/> and the
    /// deferred CLUSTER SNAPSHOT_DATA replies that wait for its writes.
    /// </summary>
    [TestFixture, NonParallelizable]
    public class ReplicaCheckpointWriteTests : TestBase
    {
        const int Chunk = 4096;

        [TearDown]
        public void TearDown()
        {
            ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Hold_Snapshot_Write);
            TestUtils.OnTearDown();
        }

        [Test]
        public async Task WritesOneAtATimeInOrderAndCompletesEachChunkAfterItsOwnWrite()
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers: 3);
                var written = Enumerable.Range(0, 3).Select(i => sink.WriteChunkAsync(i * Chunk, ChunkData(i))).ToArray();

                for (var i = 0; i < 3; i++)
                {
                    var write = await device.NextWriteAsync();
                    Assert.That(write.Address, Is.EqualTo(i * Chunk));
                    Assert.That(write.Data, Is.EqualTo(ChunkData(i)));

                    // Give an early completion, or a second concurrent write, a chance to show.
                    await Task.Delay(50);
                    Assert.That(written[i].IsCompleted, Is.False, "A chunk completes only after its own write");
                    Assert.That(device.WritesIssued, Is.EqualTo(i + 1), "The next write waits for this one");

                    write.Complete();
                    await written[i];
                }

                await sink.CompleteAsync();
                Assert.That(device.Disposed, Is.True);
            }
            finally
            {
                pool.Free();
            }
        }

        [Test]
        public async Task BlocksTheReceiverWhileEveryBufferIsInUse()
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers: 1);
                var first = sink.WriteChunkAsync(0, ChunkData(0));
                var firstWrite = await device.NextWriteAsync();

                // The only buffer holds the first chunk until its write completes, so the second must wait to be copied.
                var queueSecond = Task.Factory.StartNew(() => sink.WriteChunkAsync(Chunk, ChunkData(1)),
                    CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                await Task.Delay(100);
                Assert.That(queueSecond.IsCompleted, Is.False);

                firstWrite.Complete();
                var second = await queueSecond;
                await first;
                var secondWrite = await device.NextWriteAsync();
                Assert.That(secondWrite.Data, Is.EqualTo(ChunkData(1)));
                secondWrite.Complete();
                await second;
                await sink.CompleteAsync();
            }
            finally
            {
                pool.Free();
            }
        }

        [Test]
        public async Task AFailedWriteFailsItsChunkAndEveryLaterChunk()
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers: 2);
                var first = sink.WriteChunkAsync(0, ChunkData(0));
                var second = sink.WriteChunkAsync(Chunk, ChunkData(1));
                (await device.NextWriteAsync()).Complete(errorCode: 5);

                var error = Assert.ThrowsAsync<GarnetException>(async () => await first);
                Assert.That(error.Message, Does.StartWith("I/O error 5 writing STORE_HLOG checkpoint file at address 0"));
                Assert.That(Assert.ThrowsAsync<GarnetException>(async () => await second), Is.SameAs(error));
                Assert.That(device.WritesIssued, Is.EqualTo(1), "No write is issued after a failure");
                Assert.That(Assert.Throws<GarnetException>(() => sink.WriteChunkAsync(2 * Chunk, ChunkData(2))), Is.SameAs(error));
                Assert.That(Assert.ThrowsAsync<GarnetException>(sink.CompleteAsync), Is.SameAs(error));
                Assert.That(device.Disposed, Is.True);
            }
            finally
            {
                pool.Free();
            }
        }

        [Test]
        public async Task AWriteThatDoesNotCompleteTimesOut()
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers: 1, timeout: TimeSpan.FromMilliseconds(200));
                var written = sink.WriteChunkAsync(0, ChunkData(0));
                _ = await device.NextWriteAsync();

                var error = Assert.ThrowsAsync<GarnetException>(async () => await written);
                Assert.That(error.Message, Does.StartWith("Timed out writing STORE_HLOG checkpoint file at address 0"));

                // The timed-out write keeps its buffer, but the receiving thread is not left waiting for it.
                Assert.That(Assert.Throws<GarnetException>(() => sink.WriteChunkAsync(Chunk, ChunkData(1))), Is.SameAs(error));
            }
            finally
            {
                pool.Free();
            }
        }

        [Test, CancelAfter(60_000)]
        public async Task SnapshotDataRepliesWaitForTheirWritesAndPrecedeLaterReplies(CancellationToken cancellationToken)
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            var context = new ClusterTestContext();
            context.Setup([]);
            ReceiveCheckpointHandler handler = null;
            ClusterProvider provider = null;
            try
            {
                // Two buffers let the second chunk queue behind the held write, so PING is processed, and its reply has
                // to wait, while the first write is still in progress.
                context.CreateInstances(1, replicaDiskBasedSyncWriteBuffers: 2);

                // Stand in for a sync attempt, which creates the handler before asking the primary for its checkpoint.
                provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
                handler = new ReceiveCheckpointHandler(provider);
                provider.replicationManager.recvCheckpointHandler = handler;

                using var client = new GarnetClientSession(context.endpoints[0], new NetworkBufferSettings(1 << 20, 1 << 17));
                await client.ConnectAsync(token: cancellationToken);
                var token = Guid.NewGuid().ToByteArray();

                // A snapshot file is new for each token; an HLOG chunk would be written into this node's own log.
                const int type = (int)CheckpointFileType.STORE_SNAPSHOT;

                Task<string> first, second, ping;
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Hold_Snapshot_Write);
                try
                {
                    first = client.ExecuteClusterSnapshotData(token, type, 0, ChunkData(0));
                    second = client.ExecuteClusterSnapshotData(token, type, Chunk, ChunkData(1));
                    ping = client.ExecuteAsync("PING");

                    // The first write is held, so nothing may be acknowledged yet, and PING's reply must wait its turn.
                    await Task.Delay(200, cancellationToken);
                    Assert.That(first.IsCompleted || second.IsCompleted || ping.IsCompleted, Is.False);
                }
                finally
                {
                    ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Hold_Snapshot_Write);
                }

                // Replies are matched to commands in order, so PONG overtaking an OK would fail these.
                Assert.That(await first.WaitAsync(cancellationToken), Is.EqualTo("OK"));
                Assert.That(await second.WaitAsync(cancellationToken), Is.EqualTo("OK"));
                Assert.That(await ping.WaitAsync(cancellationToken), Is.EqualTo("PONG"));

                // The end-of-file marker is acknowledged once the file is closed.
                Assert.That(await client.ExecuteClusterSnapshotData(token, type, 2 * Chunk, []).WaitAsync(cancellationToken), Is.EqualTo("OK"));
            }
            finally
            {
                if (provider != null)
                    provider.replicationManager.recvCheckpointHandler = null;
                handler?.Dispose();
                context.TearDown();
            }
        }

        private static OverlappedFileDataSink CreateSink(IDevice device, SectorAlignedBufferPool pool, int writeBuffers, TimeSpan? timeout = null)
            => new(CheckpointFileType.STORE_HLOG, Guid.NewGuid(), device, pool, writeBuffers, timeout ?? TimeSpan.FromSeconds(10), CancellationToken.None);

        private static byte[] ChunkData(int index) => Enumerable.Repeat((byte)(index + 1), Chunk).ToArray();

        /// <summary>
        /// A device whose writes complete only when the test completes them.
        /// </summary>
        private sealed class GatedDevice() : StorageDeviceBase("gated", 512, Devices.CAPACITY_UNSPECIFIED)
        {
            private readonly Channel<IssuedWrite> issued = Channel.CreateUnbounded<IssuedWrite>();
            private int writesIssued;

            internal int WritesIssued => Volatile.Read(ref writesIssued);
            internal bool Disposed { get; private set; }

            internal async Task<IssuedWrite> NextWriteAsync()
                => await issued.Reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            public override unsafe void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
            {
                _ = Interlocked.Increment(ref writesIssued);
                var data = new ReadOnlySpan<byte>((void*)sourceAddress, (int)numBytesToWrite).ToArray();
                _ = issued.Writer.TryWrite(new IssuedWrite((long)destinationAddress, data, callback, context));
            }

            public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context) => throw new NotSupportedException();

            public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result) => callback(result);

            public override void Dispose() => Disposed = true;
        }

        private sealed record IssuedWrite(long Address, byte[] Data, DeviceIOCompletionCallback Callback, object Context)
        {
            public void Complete(uint errorCode = 0) => Callback(errorCode, errorCode == 0 ? (uint)Data.Length : 0, Context, null);
        }
    }
}