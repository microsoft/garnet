// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    [TestFixture]
    internal class MallocFixedPageSizeTests
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
            Assert.IsTrue(allocator.IsPinned);  // HashBucket is a struct containing no managed types, so can be pinned
            var chunkSize = allocMode == AllocMode.Single ? 1 : MallocFixedPageSize<IHeapContainer<Value>>.kAllocateChunkSize;
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
                            Assert.AreEqual(getEntryValue(recordAddress, iEntry), bucketPointer->bucket_entries[iEntry], $"iter {iter}, iChunk {iChunk}, iEntry {iEntry}");
                    }
                    allocator.Free(chunkAddress);
                    Assert.AreEqual(iChunk + 1, allocator.FreeListCount);
                }
                Assert.AreEqual(numChunks, allocator.FreeListCount);
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
            Assert.IsFalse(allocator.IsPinned); // IHeapContainer itself prevents pinning, regardless of its <T>
            var chunkSize = allocMode == AllocMode.Single ? 1 : MallocFixedPageSize<IHeapContainer<Value>>.kAllocateChunkSize;
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
                        Assert.AreEqual(recordAddress, valueRef.Get().value);
                    }
                    allocator.Free(chunkAddress);
                    Assert.AreEqual(iChunk + 1, allocator.FreeListCount);
                }
                Assert.AreEqual(numChunks, allocator.FreeListCount);
            }
            allocator.Dispose();
        }
    }
}