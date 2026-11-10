// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Garnet.cluster;
using Garnet.server;
using NUnit.Framework;
using Tsavorite.core;

namespace Garnet.test.cluster
{
    [TestFixture, NonParallelizable]
    public class DiskBasedCheckpointChunkTests : TestBase
    {
        [TearDown]
        public void TearDown() => TestUtils.OnTearDown();

        [TestCase(nameof(CheckpointFileType.STORE_HLOG), 1L << 33)]
        [TestCase(nameof(CheckpointFileType.STORE_SNAPSHOT), 1L << 33)]
        [TestCase(nameof(CheckpointFileType.STORE_HLOG_OBJ), 1L << 30)]
        [TestCase(nameof(CheckpointFileType.STORE_SNAPSHOT_OBJ), 1L << 30)]
        [TestCase(nameof(CheckpointFileType.STORE_INDEX), -1L)]
        public void PhysicalSegmentSizeIsIndependentOfTransferChunks(string type, long expected)
        {
            var options = new GarnetServerOptions { SegmentSize = "8g", ObjectLogSegmentSize = "1g", ReplicaDiskBasedSyncChunkSize = "4m" };
            Assert.That(TsavoriteSnapshotReader.GetSegmentSize(Enum.Parse<CheckpointFileType>(type), options), Is.EqualTo(expected));
        }

        [Test]
        public void UnsupportedCheckpointTypesHaveNoSegmentSize()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                TsavoriteSnapshotReader.GetSegmentSize((CheckpointFileType)byte.MaxValue, new GarnetServerOptions()));
        }

        [TestCase("128k", 128 << 10)]
        [TestCase("4m", 4 << 20)]
        public async Task CheckpointReaderAndClientUseConfiguredChunkSize(string size, int chunkSize)
        {
            var context = new ClusterTestContext();
            context.Setup([]);
            try
            {
                context.CreateInstances(1, enableAOF: true, lowMemory: true, memorySize: "4m", pageSize: "1m",
                    segmentSize: "4m", replicaDiskBasedSyncChunkSize: size);
                context.CreateConnection();
                Assert.That(context.clusterTestUtils.AddDelSlotsRange(0, [(0, 16383)], true), Is.EqualTo("OK"));
                context.clusterTestUtils.BumpEpoch(0);
                var server = context.clusterTestUtils.GetServer(0);
                var value = new string('v', 256 << 10);
                for (var i = 0; i < 32; i++)
                {
                    _ = server.Execute(0, "SET", [$"string:{i}", value]);
                    _ = server.Execute(0, "HSET", [$"hash:{i}", "field", value]);
                }
                context.clusterTestUtils.Checkpoint(0);

                var provider = (ClusterProvider)context.nodes[0].Provider.StoreWrapper.clusterProvider;
                var manager = provider.replicationManager;
                Assert.That(manager.GetRSSNetworkBufferSettings.sendBufferSize, Is.EqualTo(Math.Max(1 << 20, 2 * chunkSize)));
                Assert.That(manager.GetNetworkPool.Validate(ReplicationManager.CreateRSSNetworkBufferSettings(
                    GarnetServerOptions.ReplicaDiskBasedSyncChunkSizeBytesMax)), Is.True);
                Assert.That(manager.GetNetworkPool.LiveBytes + manager.GetNetworkPool.PooledBytes, Is.Zero,
                    "Supporting larger runtime size classes must not allocate their buffers at startup");
                Assert.That(manager.TryGetLatestCheckpointEntryFromMemory(out var entry), Is.True);
                try
                {
                    Assert.That(manager.TryAcquireSettledMetadataForMainStore(entry, out var logInfo, out var indexSize), Is.True);
                    using var reader = new TsavoriteSnapshotReader(provider, entry, logInfo, indexSize, TimeSpan.FromSeconds(5));
                    var newChunkSize = chunkSize == 128 << 10 ? 4 << 20 : 128 << 10;
                    var runtimeConfig = provider.storeWrapper.runtimeConfig;
                    Assert.That(runtimeConfig.TrySet(ServerConfigType.REPL_DISK_BASED_SYNC_CHUNK_SIZE, newChunkSize.ToString(), out var error),
                        Is.True, error);
                    Assert.That(runtimeConfig.TrySet(ServerConfigType.REPL_DISK_BASED_SYNC_PIPELINE_DEPTH, "3", out error), Is.True, error);
                    var newSettings = manager.GetRSSNetworkBufferSettings;
                    Assert.That(newSettings.sendBufferSize, Is.EqualTo(Math.Max(1 << 20, 2 * newChunkSize)));
                    Assert.That(manager.GetNetworkPool.Validate(newSettings), Is.True, "Runtime sizes must remain recyclable");
                    var seen = new HashSet<CheckpointFileType>();
                    foreach (var source in reader.GetTransmitSources())
                    {
                        if (source is not FileTransmitSource || !source.DataSource.HasNextChunk)
                            continue;

                        var dataSource = source.DataSource;
                        var segment = TsavoriteSnapshotReader.GetSegmentSize(dataSource.Type, context.nodeOptions[0]);
                        var expected = Math.Min(chunkSize, dataSource.EndOffset - dataSource.CurrentOffset);
                        if (segment > 0)
                            expected = Math.Min(expected, segment - dataSource.CurrentOffset % segment);
                        var result = await dataSource.ReadNextChunkAsync();
                        try
                        {
                            Assert.That(result.BytesRead, Is.EqualTo((expected + 511) & ~511L), dataSource.Type.ToString());
                            seen.Add(dataSource.Type);
                        }
                        finally
                        {
                            result.Buffer.Return();
                        }
                    }
                    Assert.That(seen, Does.Contain(CheckpointFileType.STORE_HLOG));
                    Assert.That(seen, Does.Contain(CheckpointFileType.STORE_INDEX));
                    Assert.That(seen, Does.Contain(CheckpointFileType.STORE_SNAPSHOT_OBJ));
                    using var newReader = new TsavoriteSnapshotReader(provider, entry, logInfo, indexSize, TimeSpan.FromSeconds(5));
                    foreach (var source in newReader.GetTransmitSources())
                    {
                        if (source is not FileTransmitSource || !source.DataSource.HasNextChunk)
                            continue;
                        var dataSource = source.DataSource;
                        var segment = TsavoriteSnapshotReader.GetSegmentSize(dataSource.Type, context.nodeOptions[0]);
                        var expected = Math.Min(newChunkSize, dataSource.EndOffset - dataSource.CurrentOffset);
                        if (segment > 0)
                            expected = Math.Min(expected, segment - dataSource.CurrentOffset % segment);
                        var result = await dataSource.ReadNextChunkAsync();
                        try
                        {
                            Assert.That(result.BytesRead, Is.EqualTo((expected + 511) & ~511L), dataSource.Type.ToString());
                        }
                        finally
                        {
                            result.Buffer.Return();
                        }
                    }
                }
                finally
                {
                    entry.RemoveReader();
                }
            }
            finally
            {
                context.TearDown();
            }
        }

        [TestCase(128 << 10, 0, 3 * (1 << 20))]
        [TestCase(4 << 20, 512, 3 * (1 << 20) + 1000)]
        [TestCase(4 << 20, (1 << 20) - 512, 3 * (1 << 20))]
        public async Task ReadsRespectChunkSectorAndSegmentBoundaries(int chunkSize, long start, long end)
        {
            const int segmentSize = 1 << 20;
            using var device = new LocalMemoryDevice(4 * segmentSize, segmentSize, parallelism: 0);
            device.Initialize(segmentSize);
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            using var signal = new SemaphoreSlim(0);
            try
            {
                var seed = pool.Get(segmentSize);
                try
                {
                    for (var segment = 0; segment < 4; segment++)
                    {
                        seed.GetSlice(segmentSize).Fill((byte)(segment + 1));
                        WriteSegment(device, seed, segment * (long)segmentSize, segmentSize);
                    }
                }
                finally
                {
                    seed.Return();
                }

                using var source = new FileDataSource(CheckpointFileType.STORE_HLOG, Guid.NewGuid(), device,
                    start, end, chunkSize, TimeSpan.FromSeconds(5), pool, signal);
                var offset = start;
                var sizes = new List<int>();
                while (source.HasNextChunk)
                {
                    var result = await source.ReadNextChunkAsync();
                    try
                    {
                        var requested = Math.Min(Math.Min(end - offset, chunkSize), segmentSize - offset % segmentSize);
                        var expected = (int)((requested + 511) & ~511L);
                        Assert.That(result.ChunkStartAddress, Is.EqualTo(offset));
                        Assert.That(result.BytesRead, Is.EqualTo(expected));
                        Assert.That(result.BytesRead, Is.LessThanOrEqualTo(chunkSize));
                        Assert.That(offset % segmentSize + result.BytesRead, Is.LessThanOrEqualTo(segmentSize));
                        Assert.That(result.Buffer.GetSlice(result.BytesRead).IndexOfAnyExcept((byte)(offset / segmentSize + 1)), Is.EqualTo(-1));
                        sizes.Add(result.BytesRead);
                        offset += result.BytesRead;
                    }
                    finally
                    {
                        result.Buffer.Return();
                    }
                }
                Assert.That(source.CurrentOffset, Is.EqualTo((end + 511) & ~511L));
                if (chunkSize > segmentSize)
                    Assert.That(sizes, Does.Contain(segmentSize));
            }
            finally
            {
                pool.Free();
            }
        }

        [TestCase(511)]
        [TestCase(513)]
        public void RejectsChunksNotAlignedToTheDeviceSector(int size)
        {
            using var device = new LocalMemoryDevice(1 << 20, 1 << 20, parallelism: 0);
            Assert.Throws<ArgumentException>(() => new FileDataSource(CheckpointFileType.STORE_HLOG, Guid.NewGuid(),
                device, 0, 1024, size, TimeSpan.FromSeconds(5), null, null));
        }

        [Test]
        public async Task UnsegmentedIndexUsesLargeChunksAndAShortFinalChunk()
        {
            const int chunkSize = 4 << 20;
            using var device = new LocalMemoryDevice(16 << 20, 16 << 20, parallelism: 0);
            device.Initialize(-1);
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            using var signal = new SemaphoreSlim(0);
            try
            {
                var seed = pool.Get(16 << 20);
                try
                {
                    seed.GetSlice(16 << 20).Fill(42);
                    WriteSegment(device, seed, 0, 16 << 20);
                }
                finally
                {
                    seed.Return();
                }

                using var source = new FileDataSource(CheckpointFileType.STORE_INDEX, Guid.NewGuid(), device,
                    0, 2 * chunkSize + 512, chunkSize, TimeSpan.FromSeconds(5), pool, signal);
                foreach (var size in new[] { chunkSize, chunkSize, 512 })
                {
                    var result = await source.ReadNextChunkAsync();
                    try
                    {
                        Assert.That(result.BytesRead, Is.EqualTo(size));
                        Assert.That(result.Buffer.GetSlice(size).IndexOfAnyExcept((byte)42), Is.EqualTo(-1));
                    }
                    finally
                    {
                        result.Buffer.Return();
                    }
                }
                Assert.That(source.HasNextChunk, Is.False);
            }
            finally
            {
                pool.Free();
            }
        }

        [TestCase(0, 0)]
        [TestCase(0, 512)]
        [TestCase(100, 0)]
        [TestCase(100, 100)]
        [TestCase(100, 512)]
        [TestCase(512, 0)]
        [TestCase(512, 512)]
        public async Task ReadsPreserveDataAndZeroPaddingWithOptionalByteCounts(int available, int reportedBytes)
        {
            using var device = new ShortReadDevice(available, fill: 1, reportedBytes);
            var pool = new SectorAlignedBufferPool(1, (int)device.SectorSize);
            using var signal = new SemaphoreSlim(0);
            try
            {
                // Leave a dirty sector-sized buffer in this thread's pool cache, where the read finds it next.
                var dirty = pool.Get(512, clearOnReturn: false);
                dirty.GetSlice(512).Fill(0xFF);
                dirty.Return();

                using var source = new FileDataSource(CheckpointFileType.STORE_HLOG, Guid.NewGuid(), device,
                    0, 512, 512, TimeSpan.FromSeconds(5), pool, signal);
                var result = await source.ReadNextChunkAsync();
                try
                {
                    Assert.That(result.BytesRead, Is.EqualTo(512));
                    var data = result.Buffer.GetSlice(512).ToArray();
                    Assert.That(data[..available], Is.All.EqualTo((byte)1));
                    Assert.That(data[available..], Is.All.EqualTo((byte)0), "The unread tail of a short read is sent as zeros");
                }
                finally
                {
                    result.Buffer.Return();
                }
            }
            finally
            {
                pool.Free();
            }
        }

        private static unsafe void WriteSegment(IDevice device, SectorAlignedMemory buffer, long address, int size)
        {
            device.WriteAsync((IntPtr)buffer.aligned_pointer, (ulong)address, (uint)size,
                (error, _, _, exception) =>
                {
                    Assert.That(error, Is.Zero);
                    Assert.That(exception, Is.Null);
                }, null);
        }

        /// <summary>
        /// A device holding only <paramref name="available"/> bytes, so a longer read ends early, as at the end of a file.
        /// </summary>
        private sealed class ShortReadDevice(int available, byte fill, int reportedBytes) : StorageDeviceBase("short", 512, Devices.CAPACITY_UNSPECIFIED)
        {
            public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context)
            {
                var count = (int)Math.Min(readLength, (uint)available);
                new Span<byte>((void*)destinationAddress, count).Fill(fill);
                callback(0, (uint)reportedBytes, context, null);
            }

            public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context) => throw new NotSupportedException();

            public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result) => callback(result);

            public override void Dispose() { }
        }
    }
}