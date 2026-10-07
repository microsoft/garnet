// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class MallocFixedPageSizeTests : TestBase
    {
        public enum AllocMode { Single, Bulk };

        [Test]
        [Category(MallocFixedPageSizeCategory), Category(SmokeTestCategory)]
        public unsafe void BasicHashBucketMallocFPSTest([Values] AllocMode allocMode)
        {
            DeleteDirectory(MethodTestDir, wait: true);

            // Each chunk allocation is:
            // Single:  HashBucket
            // Bulk:    HashBucket[kAllocateChunkSize]
            // where HashBucket contains its own array of entries.

            var allocator = new MallocFixedPageSize<HashBucket>();
            ClassicAssert.IsTrue(MallocFixedPageSize<HashBucket>.IsBlittable);  // HashBucket is a blittable struct, so it can be pinned
            var chunkSize = allocMode == AllocMode.Single ? 1 : MallocFixedPageSize<IHeapContainer<Value>>.AllocateChunkSize;
            var numChunks = 2 * allocator.GetPageSize() / chunkSize;

            for (var iter = 0; iter < 2; ++iter)
            {
                long getEntryValue(long recordAddress, int iEntry) => recordAddress * Constants.kOverflowBucketIndex * 10 + iEntry;

                // Populate; the second iteration should go through the freelist.
                var chunkAddresses = new long[numChunks];
                for (int iChunk = 0; iChunk < numChunks; iChunk++)
                {
                    long chunkAddress = allocator.Allocate();
                    chunkAddresses[iChunk] = chunkAddress;
                    for (var iRecord = 0; iRecord < chunkSize; ++iRecord)
                    {
                        var recordAddress = chunkAddress + iRecord;
                        var bucket = (HashBucket*)allocator.GetPhysicalAddress(recordAddress);
                        for (int iEntry = 0; iEntry < Constants.kOverflowBucketIndex; iEntry++)
                            bucket->bucket_entries[iEntry] = getEntryValue(recordAddress, iEntry);
                    }
                }

                // Verify and free
                for (int iChunk = 0; iChunk < numChunks; iChunk++)
                {
                    long chunkAddress = chunkAddresses[iChunk];
                    for (var iRecord = 0; iRecord < chunkSize; ++iRecord)
                    {
                        var recordAddress = chunkAddress + iRecord;
                        var bucketPointer = (HashBucket*)allocator.GetPhysicalAddress(recordAddress);
                        for (int iEntry = 0; iEntry < Constants.kOverflowBucketIndex; iEntry++)
                            ClassicAssert.AreEqual(getEntryValue(recordAddress, iEntry), bucketPointer->bucket_entries[iEntry], $"iter {iter}, iChunk {iChunk}, iEntry {iEntry}");
                    }
                    allocator.Free(chunkAddress);
                    ClassicAssert.AreEqual(iChunk + 1, allocator.FreeListCount);
                }
                ClassicAssert.AreEqual(numChunks, allocator.FreeListCount);
            }
            allocator.Dispose();
        }

        internal class Value
        {
            public long value;

            public Value(long value) => this.value = value;

            public override string ToString() => value.ToString();
        }

        [Test]
        [Category(MallocFixedPageSizeCategory), Category(SmokeTestCategory)]
        public unsafe void BasicIHeapContainerMallocFPSTest([Values] AllocMode allocMode)
        {
            DeleteDirectory(MethodTestDir, wait: true);

            // Each chunk allocation is:
            // Single:  Value
            // Bulk:    Value[kAllocateChunkSize]

            var allocator = new MallocFixedPageSize<IHeapContainer<Value>>();
            ClassicAssert.IsFalse(MallocFixedPageSize<IHeapContainer<Value>>.IsBlittable); // IHeapContainer itself prevents pinning, regardless of its <T>
            var chunkSize = allocMode == AllocMode.Single ? 1 : MallocFixedPageSize<IHeapContainer<Value>>.AllocateChunkSize;
            var numChunks = 2 * allocator.GetPageSize() / chunkSize;

            for (var iter = 0; iter < 2; ++iter)
            {
                // Populate; the second iteration should go through the freelist.
                var chunkAddresses = new long[numChunks];
                for (int iChunk = 0; iChunk < numChunks; iChunk++)
                {
                    long chunkAddress = allocMode == AllocMode.Single ? allocator.Allocate() : allocator.BulkAllocate();
                    chunkAddresses[iChunk] = chunkAddress;
                    for (var iRecord = 0; iRecord < chunkSize; ++iRecord)
                    {
                        var recordAddress = chunkAddress + iRecord;
                        var vector = new Value(recordAddress);
                        var heapContainer = new StandardHeapContainer<Value>(ref vector) as IHeapContainer<Value>;
                        allocator.Set(recordAddress, ref heapContainer);
                    }
                }

                // Verify and free
                for (int iChunk = 0; iChunk < numChunks; iChunk++)
                {
                    long chunkAddress = chunkAddresses[iChunk];
                    for (var iRecord = 0; iRecord < chunkSize; ++iRecord)
                    {
                        var recordAddress = chunkAddress + iRecord;
                        ref var valueRef = ref allocator.Get(recordAddress);
                        ClassicAssert.AreEqual(recordAddress, valueRef.Get().value);
                    }
                    allocator.Free(chunkAddress);
                    ClassicAssert.AreEqual(iChunk + 1, allocator.FreeListCount);
                }
                ClassicAssert.AreEqual(numChunks, allocator.FreeListCount);
            }
            allocator.Dispose();
        }

        [Test]
        [Category(MallocFixedPageSizeCategory), Category(SmokeTestCategory)]
        public void AllocatorThrowsWhenPageTableIsExhausted()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            // A small level count exercises the same exhaustion path as the production level count without allocating its capacity.
            const int LevelCount = 3;
            using var allocator = new MallocFixedPageSize<byte>(LevelCount);

            var capacity = allocator.GetPageSize() * (long)LevelCount;
            ClassicAssert.AreEqual(capacity, allocator.MaxAllocationCount);

            // The constructor consumes one bulk chunk, so that many fewer are available here.
            var expectedAllocations = capacity - MallocFixedPageSize<byte>.AllocateChunkSize;

            long allocations = 0;
            long lastAddress = 0;
            _ = Assert.Throws<TsavoriteException>(() =>
            {
                while (true)
                {
                    lastAddress = allocator.Allocate();
                    allocations++;
                }
            });

            ClassicAssert.AreEqual(expectedAllocations, allocations);

            // The last allocation before the limit is a usable record, not a torn or out-of-range one.
            ClassicAssert.AreEqual(capacity - 1, lastAddress);
            byte marker = 42;
            allocator.Set(lastAddress, ref marker);
            ClassicAssert.AreEqual(42, allocator.Get(lastAddress));

            // Freeing makes room again: the free list is consulted before the capacity check.
            allocator.Free(lastAddress);
            ClassicAssert.AreEqual(lastAddress, allocator.Allocate());
            _ = Assert.Throws<TsavoriteException>(() => allocator.Allocate());

            // A rejected allocation leaves no trace. count is both the high-water mark reported to metrics and the value
            // BeginCheckpoint derives page-table indices from, so letting rejections advance it would index past the table.
            for (var i = 0; i < 8; i++)
                _ = Assert.Throws<TsavoriteException>(() => allocator.Allocate());
            ClassicAssert.AreEqual(capacity, allocator.GetMaxValidAddress());
        }

        [Test]
        [Category(MallocFixedPageSizeCategory), Category(SmokeTestCategory)]
        public void BulkAllocateStopsAtTheEndOfThePageTable()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            const int LevelCount = 2;
            using var allocator = new MallocFixedPageSize<byte>(LevelCount);

            var capacity = allocator.GetPageSize() * (long)LevelCount;
            var chunkSize = MallocFixedPageSize<byte>.AllocateChunkSize;

            // The whole block is reserved at once, so a chunk is either fully within the page table or refused. The chunk
            // size divides the page size, so the last chunk ends exactly at capacity rather than straddling it.
            ClassicAssert.AreEqual(0, allocator.GetPageSize() % chunkSize);

            long lastAddress = 0;
            _ = Assert.Throws<TsavoriteException>(() =>
            {
                while (true)
                    lastAddress = allocator.BulkAllocate();
            });

            ClassicAssert.AreEqual(capacity - chunkSize, lastAddress);
            ClassicAssert.AreEqual(capacity, allocator.GetMaxValidAddress());

            // Every record of the final chunk is addressable, so the refused chunk did not truncate a usable one.
            for (var i = 0; i < chunkSize; i++)
            {
                byte value = (byte)i;
                allocator.Set(lastAddress + i, ref value);
                ClassicAssert.AreEqual(i, allocator.Get(lastAddress + i));
            }

            _ = Assert.Throws<TsavoriteException>(() => allocator.BulkAllocate());
            ClassicAssert.AreEqual(capacity, allocator.GetMaxValidAddress());
        }

        [Test]
        [Category(MallocFixedPageSizeCategory), Category(SmokeTestCategory)]
        public void CheckpointSucceedsAfterAllocatorIsExhausted()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            const int LevelCount = 2;
            using var allocator = new MallocFixedPageSize<HashBucket>(LevelCount);

            var capacity = allocator.GetPageSize() * (long)LevelCount;
            while (allocator.GetMaxValidAddress() < capacity)
                _ = allocator.Allocate();

            for (var i = 0; i < 4; i++)
                _ = Assert.Throws<TsavoriteException>(() => allocator.Allocate());

            // BeginCheckpoint turns count into a page-table index range, so a count past capacity would read out of range.
            using var device = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ExhaustedCheckpoint.dat"), deleteOnClose: true);
            allocator.BeginCheckpoint(device, 0, out var numBytes);
            allocator.IsCheckpointCompletedAsync().AsTask().GetAwaiter().GetResult();
            ClassicAssert.AreEqual((ulong)(capacity * MallocFixedPageSize<HashBucket>.RecordSize), numBytes);
        }

        [Test]
        [Category(MallocFixedPageSizeCategory), Category(SmokeTestCategory)]
        public void OverflowBucketCapacityIsSixteenGibibytes()
        {
            // The hash index allocates one of these per store for overflow buckets, and nothing frees a bucket when records
            // are deleted, so this is a hard ceiling on the number of hash entries that can spill out of the main bucket array.
            // It is asserted here so that a change to the page table geometry is a test failure rather than a production throw.
            using var allocator = new MallocFixedPageSize<HashBucket>();
            ClassicAssert.AreEqual(1L << 28, allocator.MaxAllocationCount);
            ClassicAssert.AreEqual(16L * 1024 * 1024 * 1024, allocator.MaxAllocationCount * MallocFixedPageSize<HashBucket>.RecordSize);
        }
    }
}