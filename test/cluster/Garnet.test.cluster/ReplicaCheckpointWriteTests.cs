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
    /// CLUSTER SNAPSHOT_DATA acceptance replies and end-of-file write barrier.
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

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(4)]
        public async Task AcceptsChunksBeforeWritingAndCompletesOnlyAfterEveryWrite(int writeBuffers)
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers);
                for (var i = 0; i < writeBuffers; i++)
                    sink.WriteChunk(i * Chunk, ChunkData(i));
                var completion = sink.CompleteAsync();

                var writes = new IssuedWrite[writeBuffers];
                for (var i = 0; i < writes.Length; i++)
                {
                    writes[i] = await device.NextWriteAsync();
                    Assert.That(writes[i].Address, Is.EqualTo(i * Chunk));
                    Assert.That(writes[i].Data, Is.EqualTo(ChunkData(i)));
                }

                for (var i = writes.Length - 1; i >= 0; i--)
                {
                    await Task.Delay(50);
                    Assert.That(completion.IsCompleted, Is.False, "Completion waits for every queued write");
                    Assert.That(device.Disposed, Is.False, "The device stays open until every write finishes");
                    writes[i].Complete();
                }

                await completion;
                Assert.That(device.WritesIssued, Is.EqualTo(writeBuffers));
                Assert.That(device.Disposed, Is.True);
            }
            finally
            {
                pool.Free();
            }
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(4)]
        public async Task BlocksTheReceiverWhileEveryBufferIsInUse(int writeBuffers)
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers);
                var writes = new IssuedWrite[writeBuffers];
                for (var i = 0; i < writes.Length; i++)
                {
                    sink.WriteChunk(i * Chunk, ChunkData(i));
                    writes[i] = await device.NextWriteAsync();
                }

                var queueNext = Task.Factory.StartNew(() => sink.WriteChunk(writeBuffers * Chunk, ChunkData(writeBuffers)),
                    CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                await Task.Delay(100);
                Assert.That(queueNext.IsCompleted, Is.False);
                Assert.That(device.WritesIssued, Is.EqualTo(writeBuffers), "In-flight writes are bounded by buffer count");

                // Any completion releases a buffer, even while earlier writes are still outstanding.
                writes[^1].Complete();
                await queueNext.WaitAsync(TimeSpan.FromSeconds(10));
                var nextWrite = await device.NextWriteAsync();
                Assert.That(nextWrite.Data, Is.EqualTo(ChunkData(writeBuffers)));
                nextWrite.Complete();
                for (var i = 0; i < writes.Length - 1; i++)
                {
                    Assert.That(writes[i].ReadData(), Is.EqualTo(ChunkData(i)), "In-flight buffers cannot be reused");
                    writes[i].Complete();
                }
                await sink.CompleteAsync();
            }
            finally
            {
                pool.Free();
            }
        }

        [TestCase(0)]
        [TestCase(1)]
        public async Task AFailedWriteFailsCompletionButWaitsForOtherIssuedWrites(int failedIndex)
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers: 2);
                sink.WriteChunk(0, ChunkData(0));
                sink.WriteChunk(Chunk, ChunkData(1));
                var writes = new[] { await device.NextWriteAsync(), await device.NextWriteAsync() };
                writes[failedIndex].Complete(errorCode: 5);

                var completion = sink.CompleteAsync();
                await Task.Delay(100);
                Assert.That(completion.IsCompleted, Is.False, "A failed write does not bypass other outstanding writes");
                Assert.That(device.Disposed, Is.False);
                writes[1 - failedIndex].Complete();
                var error = Assert.ThrowsAsync<GarnetException>(async () => await completion);
                Assert.That(error.Message, Does.StartWith($"I/O error 5 writing STORE_HLOG checkpoint file at address {failedIndex * Chunk}"));
                Assert.That(device.WritesIssued, Is.EqualTo(2));
                Assert.That(Assert.Throws<GarnetException>(() => sink.WriteChunk(2 * Chunk, ChunkData(2))), Is.SameAs(error));
                Assert.That(Assert.ThrowsAsync<GarnetException>(sink.CompleteAsync), Is.SameAs(error));
                Assert.That(device.Disposed, Is.True);
            }
            finally
            {
                pool.Free();
            }
        }

        [TestCase(0, Chunk)]
        [TestCase(512, Chunk)]
        [TestCase(512, 513)]
        [TestCase(-512, Chunk)]
        public async Task RejectsOverlappingChunksIncludingSectorPadding(long address, int length)
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers: 2);
                sink.WriteChunk(0, new byte[length]);
                var write = await device.NextWriteAsync();
                try
                {
                    Assert.Throws<ArgumentException>(() => sink.WriteChunk(address, ChunkData(1)));
                    Assert.That(device.WritesIssued, Is.EqualTo(1));
                }
                finally
                {
                    write.Complete();
                }
                await sink.CompleteAsync();
            }
            finally
            {
                pool.Free();
            }
        }

        [Test]
        public async Task DisposalWaitsForEveryOutstandingWrite()
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers: 2);
                sink.WriteChunk(0, ChunkData(0));
                sink.WriteChunk(Chunk, ChunkData(1));
                var first = await device.NextWriteAsync();
                var second = await device.NextWriteAsync();
                var disposal = Task.Factory.StartNew(sink.Dispose,
                    CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                try
                {
                    second.Complete();
                    await Task.Delay(100);
                    Assert.That(disposal.IsCompleted, Is.False);
                    Assert.That(device.Disposed, Is.False);
                }
                finally
                {
                    first.Complete();
                }
                await disposal.WaitAsync(TimeSpan.FromSeconds(10));
                Assert.That(device.Disposed, Is.True);
            }
            finally
            {
                pool.Free();
            }
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(4)]
        public async Task AWriteThatDoesNotCompleteTimesOut(int writeBuffers)
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers, timeout: TimeSpan.FromSeconds(1));
                for (var i = 0; i < writeBuffers; i++)
                    sink.WriteChunk(i * Chunk, ChunkData(i));
                var held = await device.NextWriteAsync();
                for (var i = 1; i < writeBuffers; i++)
                    (await device.NextWriteAsync()).Complete();

                var error = Assert.ThrowsAsync<GarnetException>(sink.CompleteAsync);
                Assert.That(error.Message, Does.StartWith("Timed out writing STORE_HLOG checkpoint file at address 0"));

                // The timed-out write keeps its buffer, but the receiving thread is not left waiting for it.
                Assert.That(Assert.Throws<GarnetException>(() => sink.WriteChunk(writeBuffers * Chunk, ChunkData(writeBuffers))), Is.SameAs(error));
                Assert.That(held.ReadData(), Is.EqualTo(ChunkData(0)));
                held.Complete();
            }
            finally
            {
                pool.Free();
            }
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(4)]
        public async Task CancellationPreservesEveryInFlightBufferUntilItsCallback(int writeBuffers)
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            using var cancellation = new CancellationTokenSource();
            try
            {
                using var sink = new OverlappedFileDataSink(CheckpointFileType.STORE_HLOG, Guid.NewGuid(), device,
                    pool, writeBuffers, TimeSpan.FromSeconds(10), cancellation.Token, writeBuffersConcurrently: true);
                var writes = new IssuedWrite[writeBuffers];
                for (var i = 0; i < writes.Length; i++)
                {
                    sink.WriteChunk(i * Chunk, ChunkData(i));
                    writes[i] = await device.NextWriteAsync();
                }
                await cancellation.CancelAsync();
                Assert.CatchAsync<OperationCanceledException>(sink.CompleteAsync);
                Assert.That(device.Disposed, Is.True);
                pool.Free();
                pool = null;
                for (var i = writes.Length - 1; i >= 0; i--)
                {
                    Assert.That(writes[i].ReadData(), Is.EqualTo(ChunkData(i)));
                    writes[i].Complete();
                }
            }
            finally
            {
                pool?.Free();
            }
        }

        [Test, CancelAfter(60_000)]
        public async Task SnapshotDataRepliesAcknowledgeQueueingAndEndMarkerWaitsForWrites(CancellationToken cancellationToken)
        {
            TestUtils.IgnoreIfExceptionInjectionDisabled();

            var context = new ClusterTestContext();
            context.Setup([]);
            ReceiveCheckpointHandler handler = null;
            ClusterProvider provider = null;
            try
            {
                // Both chunks fit in the queue while the writer is held.
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

                Task<string> completion;
                ExceptionInjectionHelper.EnableException(ExceptionInjectionType.Replication_Hold_Snapshot_Write);
                try
                {
                    var first = client.ExecuteClusterSnapshotData(token, type, 0, ChunkData(0));
                    var second = client.ExecuteClusterSnapshotData(token, type, Chunk, ChunkData(1));
                    var ping = client.ExecuteAsync("PING");
                    Assert.That(await first.WaitAsync(cancellationToken), Is.EqualTo("OK"));
                    Assert.That(await second.WaitAsync(cancellationToken), Is.EqualTo("OK"));
                    Assert.That(await ping.WaitAsync(cancellationToken), Is.EqualTo("PONG"));

                    completion = client.ExecuteClusterSnapshotData(token, type, 2 * Chunk, []);
                    await Task.Delay(200, cancellationToken);
                    Assert.That(completion.IsCompleted, Is.False, "End marker must wait for disk writes, not just acceptance");
                }
                finally
                {
                    ExceptionInjectionHelper.DisableException(ExceptionInjectionType.Replication_Hold_Snapshot_Write);
                }

                Assert.That(await completion.WaitAsync(cancellationToken), Is.EqualTo("OK"));
            }
            finally
            {
                if (provider != null)
                    provider.replicationManager.recvCheckpointHandler = null;
                handler?.Dispose();
                context.TearDown();
            }
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(4)]
        [TestCase(8)]
        public async Task SequentialPolicyQueuesBuffersButIssuesOneWriteAtATime(int writeBuffers)
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, writeBuffers, concurrently: false);
                for (var i = 0; i < writeBuffers; i++)
                    sink.WriteChunk(i * Chunk, ChunkData(i));
                var completion = sink.CompleteAsync();
                for (var i = 0; i < writeBuffers; i++)
                {
                    var write = await device.NextWriteAsync();
                    Assert.That(write.Address, Is.EqualTo(i * Chunk));
                    await Task.Delay(50);
                    Assert.That(device.WritesIssued, Is.EqualTo(i + 1));
                    Assert.That(completion.IsCompleted, Is.False);
                    write.Complete();
                }
                await completion;
                Assert.That(device.Disposed, Is.True);
            }
            finally
            {
                pool.Free();
            }
        }

        private static OverlappedFileDataSink CreateSink(IDevice device, SectorAlignedBufferPool pool, int writeBuffers, TimeSpan? timeout = null, bool concurrently = true)
            => new(CheckpointFileType.STORE_HLOG, Guid.NewGuid(), device, pool, writeBuffers, timeout ?? TimeSpan.FromSeconds(10), CancellationToken.None,
                writeBuffersConcurrently: concurrently);

        [Test]
        public async Task SequentialFailureDiscardsUnissuedQueuedWrites()
        {
            var device = new GatedDevice();
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            try
            {
                using var sink = CreateSink(device, pool, 4, concurrently: false);
                for (var i = 0; i < 4; i++)
                    sink.WriteChunk(i * Chunk, ChunkData(i));
                var first = await device.NextWriteAsync();
                first.Complete(errorCode: 5);
                Assert.ThrowsAsync<GarnetException>(sink.CompleteAsync);
                Assert.That(device.WritesIssued, Is.EqualTo(1));
                Assert.That(device.Disposed, Is.True);
            }
            finally
            {
                pool.Free();
            }
        }

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
                _ = issued.Writer.TryWrite(new IssuedWrite((long)destinationAddress, data, sourceAddress, callback, context));
            }

            public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context) => throw new NotSupportedException();

            public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result) => callback(result);

            public override void Dispose() => Disposed = true;
        }

        private sealed record IssuedWrite(long Address, byte[] Data, IntPtr Source, DeviceIOCompletionCallback Callback, object Context)
        {
            public unsafe byte[] ReadData() => new ReadOnlySpan<byte>((void*)Source, Data.Length).ToArray();

            public void Complete(uint errorCode = 0) => Callback(errorCode, errorCode == 0 ? (uint)Data.Length : 0, Context, null);
        }
    }
}