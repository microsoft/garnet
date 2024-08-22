// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.recovery
{
    [TestFixture]
    public class ComponentRecoveryTests
    {
        private static unsafe void Setup_MallocFixedPageSizeRecoveryTest(out int seed, out IDevice device, out int numBucketsToAdd, out long[] logicalAddresses, out ulong numBytesWritten)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            seed = 123;
            var rand1 = new Random(seed);
            device = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "MallocFixedPageSizeRecoveryTest.dat"), deleteOnClose: true);
            var allocator = new MallocFixedPageSize<HashBucket>();

            //do something
            numBucketsToAdd = 2 * allocator.GetPageSize();
            logicalAddresses = new long[numBucketsToAdd];
            for (int i = 0; i < numBucketsToAdd; i++)
            {
                long logicalAddress = allocator.Allocate();
                logicalAddresses[i] = logicalAddress;
                var bucket = (HashBucket*)allocator.GetPhysicalAddress(logicalAddress);
                for (int j = 0; j < Constants.kOverflowBucketIndex; j++)
                {
                    bucket->bucket_entries[j] = rand1.Next();
                }
            }

            //issue call to checkpoint
            allocator.BeginCheckpoint(device, 0, out numBytesWritten);
            //wait until complete
            allocator.IsCheckpointCompletedAsync().AsTask().Wait();

            allocator.Dispose();
        }

        private static unsafe void Finish_MallocFixedPageSizeRecoveryTest(int seed, IDevice device, int numBucketsToAdd, long[] logicalAddresses, ulong numBytesWritten, MallocFixedPageSize<HashBucket> recoveredAllocator, ulong numBytesRead)
        {
            ClassicAssert.AreEqual(numBytesRead, numBytesWritten);

            var rand2 = new Random(seed);
            for (int i = 0; i < numBucketsToAdd; i++)
            {
                var logicalAddress = logicalAddresses[i];
                var bucket = (HashBucket*)recoveredAllocator.GetPhysicalAddress(logicalAddress);
                for (int j = 0; j < Constants.kOverflowBucketIndex; j++)
                {
                    ClassicAssert.AreEqual(rand2.Next(), bucket->bucket_entries[j]);
                }
            }

            recoveredAllocator.Dispose();
            device.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void MallocFixedPageSizeRecoveryTest()
        {
            Setup_MallocFixedPageSizeRecoveryTest(out int seed, out IDevice device, out int numBucketsToAdd, out long[] logicalAddresses, out ulong numBytesWritten);

            var recoveredAllocator = new MallocFixedPageSize<HashBucket>();
            //issue call to recover
            recoveredAllocator.BeginRecovery(device, 0, numBucketsToAdd, numBytesWritten, out ulong numBytesRead);
            //wait until complete
            recoveredAllocator.IsRecoveryCompleted(true);

            Finish_MallocFixedPageSizeRecoveryTest(seed, device, numBucketsToAdd, logicalAddresses, numBytesWritten, recoveredAllocator, numBytesRead);
        }

        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async Task MallocFixedPageSizeRecoveryAsyncTest()
        {
            Setup_MallocFixedPageSizeRecoveryTest(out int seed, out IDevice device, out int numBucketsToAdd, out long[] logicalAddresses, out ulong numBytesWritten);

            var recoveredAllocator = new MallocFixedPageSize<HashBucket>();
            ulong numBytesRead = await recoveredAllocator.RecoverAsync(device, 0, numBucketsToAdd, numBytesWritten, cancellationToken: default);

            Finish_MallocFixedPageSizeRecoveryTest(seed, device, numBucketsToAdd, logicalAddresses, numBytesWritten, recoveredAllocator, numBytesRead);
        }

        private static unsafe void Setup_FuzzyIndexRecoveryTest(out int seed, out int size, out long numAdds, out IDevice ht_device, out IDevice ofb_device, out TsavoriteBase hash_table1, out ulong ht_num_bytes_written, out ulong ofb_num_bytes_written, out int num_ofb_buckets)
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            seed = 123;
            size = 1 << 16;
            numAdds = 1L << 18;
            ht_device = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "TestFuzzyIndexRecoveryht.dat"), deleteOnClose: true);
            ofb_device = Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "TestFuzzyIndexRecoveryofb.dat"), deleteOnClose: true);
            hash_table1 = new TsavoriteBase();
            hash_table1.Initialize(size, 512);

            //do something
            var keyGenerator1 = new Random(seed);
            var valueGenerator = new Random(seed + 1);
            for (int i = 0; i < numAdds; i++)
            {
                long key = keyGenerator1.Next();
                HashEntryInfo hei = new(Utility.GetHashCode(key));
                hash_table1.FindOrCreateTag(ref hei, 0);

                hash_table1.UpdateSlot(hei.bucket, hei.slot, hei.entry.word, valueGenerator.Next(), out long found_word);
            }

            //issue checkpoint call
            hash_table1.TakeIndexFuzzyCheckpoint(0, ht_device, out ht_num_bytes_written,
                ofb_device, out ofb_num_bytes_written, out num_ofb_buckets);

            //wait until complete
            hash_table1.IsIndexFuzzyCheckpointCompletedAsync().AsTask().Wait();
        }

        private static unsafe void Finish_FuzzyIndexRecoveryTest(int seed, long numAdds, IDevice ht_device, IDevice ofb_device, TsavoriteBase hash_table1, TsavoriteBase hash_table2)
        {
            var keyGenerator2 = new Random(seed);

            for (int i = 0; i < 2 * numAdds; i++)
            {
                long key = keyGenerator2.Next();
                HashEntryInfo hei1 = new(Utility.GetHashCode(key));
                HashEntryInfo hei2 = new(hei1.hash);

                var exists1 = hash_table1.FindTag(ref hei1);
                var exists2 = hash_table2.FindTag(ref hei2);

                ClassicAssert.AreEqual(exists2, exists1);

                if (exists1)
                {
                    ClassicAssert.AreEqual(hei2.entry.word, hei1.entry.word);
                }
            }

            hash_table1.Free();
            hash_table2.Free();

            ht_device.Dispose();
            ofb_device.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public unsafe void FuzzyIndexRecoveryTest()
        {
            Setup_FuzzyIndexRecoveryTest(out int seed, out int size, out long numAdds, out IDevice ht_device, out IDevice ofb_device, out TsavoriteBase hash_table1,
                                         out ulong ht_num_bytes_written, out ulong ofb_num_bytes_written, out int num_ofb_buckets);

            var hash_table2 = new TsavoriteBase();
            hash_table2.Initialize(size, 512);

            //issue recover call
            hash_table2.RecoverFuzzyIndex(0, ht_device, ht_num_bytes_written, ofb_device, num_ofb_buckets, ofb_num_bytes_written);
            //wait until complete
            hash_table2.IsFuzzyIndexRecoveryComplete(true);

            Finish_FuzzyIndexRecoveryTest(seed, numAdds, ht_device, ofb_device, hash_table1, hash_table2);
        }

        [Test]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async Task FuzzyIndexRecoveryAsyncTest()
        {
            Setup_FuzzyIndexRecoveryTest(out int seed, out int size, out long numAdds, out IDevice ht_device, out IDevice ofb_device, out TsavoriteBase hash_table1,
                                         out ulong ht_num_bytes_written, out ulong ofb_num_bytes_written, out int num_ofb_buckets);

            var hash_table2 = new TsavoriteBase();
            hash_table2.Initialize(size, 512);

            await hash_table2.RecoverFuzzyIndexAsync(0, ht_device, ht_num_bytes_written, ofb_device, num_ofb_buckets, ofb_num_bytes_written, cancellationToken: default);

            Finish_FuzzyIndexRecoveryTest(seed, numAdds, ht_device, ofb_device, hash_table1, hash_table2);
        }
    }
}