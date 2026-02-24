// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LockTable
{
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>;

    internal class SingleBucketComparer : IKeyComparer
    {
        public bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => k1.AsRef<long>() == k2.AsRef<long>();

        public long GetHashCode64(ReadOnlySpan<byte> k) => 42L;
    }

    // Used to signal Setup to use the SingleBucketComparer
    public enum UseSingleBucketComparer { UseSingleBucket }

    [AllureNUnit]
    [TestFixture]
    internal class OverflowBucketLockTableTests : AllureTestBase
    {
        IKeyComparer comparer = new LongKeyComparer();
        long singleBucketKey = 1;   // We use a single bucket here for most tests so this lets us use 'ref' easily

        // For OverflowBucketLockTable, we need an instance of TsavoriteKV
        private TsavoriteKV<LongStoreFunctions, SpanByteAllocator<LongStoreFunctions>> store;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir);

            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false, recoverDevice: false);

            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is UseSingleBucketComparer)
                {
                    comparer = new SingleBucketComparer();
                    break;
                }
            }
            comparer ??= new LongKeyComparer();

            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                PageSize = 1L << 12,
                MemorySize = 1L << 22
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = default;
            log?.Dispose();
            log = default;
            comparer = default;
            DeleteDirectory(MethodTestDir);
        }

        void TryLock(long key, LockType lockType, int expectedCurrentReadLocks, bool expectedLockResult)
        {
            HashEntryInfo hei = new(comparer.GetHashCode64(SpanByte.FromPinnedVariable(ref key)));
            PopulateHei(ref hei);

            // Check for existing lock
            var lockState = store.LockTable.GetLockState(ref hei);
            ClassicAssert.AreEqual(expectedCurrentReadLocks, lockState.NumLockedShared);

            if (lockType == LockType.Shared)
                ClassicAssert.AreEqual(expectedLockResult, store.LockTable.TryLockShared(ref hei));
            else
                ClassicAssert.AreEqual(expectedLockResult, store.LockTable.TryLockExclusive(ref hei));
        }

        void Unlock(long key, LockType lockType)
        {
            HashEntryInfo hei = new(comparer.GetHashCode64(SpanByte.FromPinnedVariable(ref key)));
            PopulateHei(ref hei);
            if (lockType == LockType.Shared)
                store.LockTable.UnlockShared(ref hei);
            else
                store.LockTable.UnlockExclusive(ref hei);
        }

        internal void PopulateHei(ref HashEntryInfo hei) => PopulateHei(store, ref hei);

        internal static void PopulateHei<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, ref HashEntryInfo hei)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => store.FindOrCreateTag(ref hei, store.Log.BeginAddress);

        internal void AssertLockCounts(ref HashEntryInfo hei, bool expectedX, long expectedS)
        {
            var lockState = store.LockTable.GetLockState(ref hei);
            ClassicAssert.AreEqual(expectedX, lockState.IsLockedExclusive);
            ClassicAssert.AreEqual(expectedS, lockState.NumLockedShared);
        }

        internal static void AssertLockCounts<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, ReadOnlySpan<byte> key, bool expectedX, int expectedS)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(key));
            PopulateHei(store, ref hei);
            var lockState = store.LockTable.GetLockState(ref hei);
            ClassicAssert.AreEqual(expectedX, lockState.IsLockedExclusive, "XLock mismatch");
            ClassicAssert.AreEqual(expectedS, lockState.NumLockedShared, "SLock mismatch");
        }

        internal static void AssertLockCounts<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, ReadOnlySpan<byte> key, bool expectedX, bool expectedS)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(key));
            PopulateHei(store, ref hei);
            var lockState = store.LockTable.GetLockState(ref hei);
            ClassicAssert.AreEqual(expectedX, lockState.IsLockedExclusive, "XLock mismatch");
            ClassicAssert.AreEqual(expectedS, lockState.NumLockedShared > 0, "SLock mismatch");
        }

        internal static void AssertLockCounts<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, ref FixedLengthTransactionalKeyStruct keyStruct, bool expectedX, bool expectedS)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            HashEntryInfo hei = new(keyStruct.KeyHash);
            PopulateHei(store, ref hei);
            var lockState = store.LockTable.GetLockState(ref hei);
            ClassicAssert.AreEqual(expectedX, lockState.IsLockedExclusive, "XLock mismatch");
            ClassicAssert.AreEqual(expectedS, lockState.NumLockedShared > 0, "SLock mismatch");
        }

        internal unsafe void AssertTotalLockCounts(long expectedX, long expectedS)
            => AssertTotalLockCounts(store, expectedX, expectedS);

        internal static unsafe void AssertTotalLockCounts<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, long expectedX, long expectedS)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            HashBucket* buckets = store.state[store.resizeInfo.version].tableAligned;
            var count = store.LockTable.NumBuckets;
            long xcount = 0, scount = 0;
            for (var ii = 0; ii < count; ++ii)
            {
                if (HashBucket.IsLatchedExclusive(buckets + ii))
                    ++xcount;
                scount += HashBucket.NumLatchedShared(buckets + ii);
            }
            ClassicAssert.AreEqual(expectedX, xcount);
            ClassicAssert.AreEqual(expectedS, scount);
        }

        internal void AssertBucketLockCount(ref FixedLengthTransactionalKeyStruct keyStruct, long expectedX, long expectedS) => AssertBucketLockCount(store, ref keyStruct, expectedX, expectedS);

        internal static unsafe void AssertBucketLockCount<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, ref FixedLengthTransactionalKeyStruct key, long expectedX, long expectedS)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var bucketIndex = store.LockTable.GetBucketIndex(key.KeyHash);
            var bucket = store.state[store.resizeInfo.version].tableAligned + bucketIndex;
            ClassicAssert.AreEqual(expectedX == 1, HashBucket.IsLatchedExclusive(bucket));
            ClassicAssert.AreEqual(expectedS, HashBucket.NumLatchedShared(bucket));
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void SingleKeyTest([Values] UseSingleBucketComparer /* justToSignalSetup */ _)
        {
            HashEntryInfo hei = new(comparer.GetHashCode64(SpanByte.FromPinnedVariable(ref singleBucketKey)));
            PopulateHei(ref hei);
            AssertLockCounts(ref hei, false, 0);

            // No entries
            long key = 1;
            TryLock(key, LockType.Shared, expectedCurrentReadLocks: 0, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 1);

            // Add a non-transient lock
            TryLock(key, LockType.Shared, expectedCurrentReadLocks: 1, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 2);

            // Now both transient and manual x locks with the same key should fail
            TryLock(key, LockType.Exclusive, expectedCurrentReadLocks: 2, expectedLockResult: false);
            AssertLockCounts(ref hei, false, 2);
            TryLock(key, LockType.Exclusive, expectedCurrentReadLocks: 2, expectedLockResult: false);
            AssertLockCounts(ref hei, false, 2);

            // Now unlock
            Unlock(key, LockType.Shared);
            AssertLockCounts(ref hei, false, 1);
            Unlock(key, LockType.Shared);
            AssertLockCounts(ref hei, false, 0);

            // Now exclusive should succeed
            TryLock(key, LockType.Exclusive, expectedCurrentReadLocks: 0, expectedLockResult: true);
            AssertLockCounts(ref hei, true, 0);
            Unlock(key, LockType.Exclusive);
            AssertLockCounts(ref hei, false, 0);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreeKeyTest([Values] UseSingleBucketComparer /* justToSignalSetup */ _)
        {
            HashEntryInfo hei = new(comparer.GetHashCode64(SpanByte.FromPinnedVariable(ref singleBucketKey)));
            PopulateHei(ref hei);
            AssertLockCounts(ref hei, false, 0);

            TryLock(1, LockType.Shared, expectedCurrentReadLocks: 0, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 1);

            TryLock(2, LockType.Shared, expectedCurrentReadLocks: 1, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 2);

            TryLock(3, LockType.Shared, expectedCurrentReadLocks: 2, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 3);

            // Exclusive lock should fail
            TryLock(4, LockType.Exclusive, expectedCurrentReadLocks: 3, expectedLockResult: false);
            AssertLockCounts(ref hei, false, 3);

            // Now unlock
            Unlock(3, LockType.Shared);
            AssertLockCounts(ref hei, false, 2);
            Unlock(2, LockType.Shared);
            AssertLockCounts(ref hei, false, 1);
            Unlock(1, LockType.Shared);
            AssertLockCounts(ref hei, false, 0);

            // Now exclusive should succeed
            TryLock(4, LockType.Exclusive, expectedCurrentReadLocks: 0, expectedLockResult: true);
            AssertLockCounts(ref hei, true, 0);
            Unlock(4, LockType.Exclusive);
            AssertLockCounts(ref hei, false, 0);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTest1Thread()
        {
            List<Task> tasks = [];
            var lastTid = 0;
            AddThreads(tasks, ref lastTid, numThreads: 1, maxNumKeys: 5, lowKey: 1, highKey: 5, LockType.Exclusive);
            Task.WaitAll([.. tasks]);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTestMultiThreadsNoContention([Values(3, 8)] int numThreads)
        {
            List<Task> tasks = [];
            var lastTid = 0;
            for (var ii = 0; ii < numThreads; ++ii)
                AddThreads(tasks, ref lastTid, numThreads: 1, maxNumKeys: 5, lowKey: 1 + 10 * ii, highKey: 5 + 10 * ii, LockType.Exclusive);
            Task.WaitAll([.. tasks]);
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTestMultiThreadsFullContention([Values(3, 8)] int numThreads, [Values] LockType lockType)
        {
            List<Task> tasks = [];
            var lastTid = 0;
            AddThreads(tasks, ref lastTid, numThreads: numThreads, maxNumKeys: 5, lowKey: 1, highKey: 5, lockType);
            Task.WaitAll([.. tasks]);
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTestMultiThreadsRandomContention([Values(3, 8)] int numThreads, [Values] LockType lockType)
        {
            List<Task> tasks = [];
            var lastTid = 0;
            AddThreads(tasks, ref lastTid, numThreads: numThreads, maxNumKeys: 5, lowKey: 1, highKey: 10 * (numThreads / 2), lockType);
            Task.WaitAll([.. tasks]);
            AssertTotalLockCounts(0, 0);
        }

        // Creates numRecords random keys in the range [0,numKeys) and returns both the array of key structs and the pinned array of longs used as storage for the keys.
        (FixedLengthTransactionalKeyStruct[], long[]) CreateKeys(Random rng, int numKeys, int numRecords)
        {
            // This needs to return the pinned source of the key longs so its lifetime is at least that of the output KeyStruct[].
            var keyNums = GC.AllocateArray<long>(numRecords, pinned: true);
            FixedLengthTransactionalKeyStruct createKey(int recordNum)
            {
                keyNums[recordNum] = rng.Next(numKeys);
                var key = SpanByte.FromPinnedVariable(ref keyNums[recordNum]);
                var keyHash = store.GetKeyHash(key);
                return new()
                {
                    Key = PinnedSpanByte.FromPinnedSpan(key),
                    // LockType.None means split randomly between Shared and Exclusive
                    LockType = rng.Next(0, 100) < 25 ? LockType.Exclusive : LockType.Shared,
                    KeyHash = keyHash,
                };
            }
            return ([.. Enumerable.Range(0, numRecords).Select(createKey)], keyNums);
        }

        void AssertSorted(FixedLengthTransactionalKeyStruct[] keys, int count)
        {
            long prevCode = default;
            long lastXcode = default;
            LockType lastLockType = default;

            for (var ii = 0; ii < count; ++ii)
            {
                ref var key = ref keys[ii];
                if (ii == 0)
                {
                    prevCode = key.KeyHash;
                    lastXcode = key.LockType == LockType.Exclusive ? key.KeyHash : -2;
                    lastLockType = key.LockType;
                    continue;
                }

                ClassicAssert.GreaterOrEqual(store.LockTable.CompareKeyHashes(key, keys[ii - 1]), 0);
                if (key.KeyHash != prevCode)
                {
                    // The BucketIndex of the keys must be nondecreasing, and may be equal but the first in such an equal sequence must be Exclusive.
                    ClassicAssert.Greater(store.LockTable.GetBucketIndex(key.KeyHash), store.LockTable.GetBucketIndex(prevCode));
                    lastXcode = key.LockType == LockType.Exclusive ? key.KeyHash : -2;
                }
                else
                {
                    // Identical BucketIndex sequence must start with an exclusive lock, followed by any number of exclusive locks, followed by any number of shared locks.
                    // (Enumeration will take only the first).
                    ClassicAssert.AreEqual(lastXcode, key.KeyHash);
                    if (key.LockType == LockType.Exclusive)
                        ClassicAssert.AreNotEqual(LockType.Shared, lastLockType);
                    lastLockType = key.LockType;
                }
            }
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void FullArraySortTest()
        {
            const int numRecords = 1000;
            var (keys, keyNums) = CreateKeys(new Random(101), 100, numRecords);
            Assert.That(keyNums.Length, Is.EqualTo(numRecords));
            store.LockTable.SortKeyHashes(keys);
            AssertSorted(keys, keys.Length);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void PartialArraySortTest()
        {
            var numRecords = 1000;
            var (keys, keyNums) = CreateKeys(new Random(101), 100, numRecords);
            Assert.That(keyNums.Length, Is.EqualTo(numRecords));
            const int count = 800;

            // Make the later elements invalid.
            for (var ii = count; ii < numRecords; ++ii)
                keys[ii].KeyHash = -ii;

            store.LockTable.SortKeyHashes(keys.AsSpan()[..count]);
            AssertSorted(keys, count);

            // Verify later elements were untouched.
            for (var ii = count; ii < numRecords; ++ii)
                keys[ii].KeyHash = -ii;
        }

        const int NumTestIterations = 15;
        const int MaxSleepMs = 5;

        private void AddThreads(List<Task> tasks, ref int lastTid, int numThreads, int maxNumKeys, int lowKey, int highKey, LockType lockType)
        {
            void runThread(int tid)
            {
                Random rng = new(101 * tid);

                // maxNumKeys < 0 means use random number of keys. SpanByte requires persistent storage so we need the threadKeyNums vector in parallel with threadStructs.
                int numKeys = maxNumKeys < 0 ? rng.Next(1, -maxNumKeys) : maxNumKeys;
                var threadKeyNums = GC.AllocateArray<long>(numKeys, pinned: true);
                var threadStructs = new FixedLengthTransactionalKeyStruct[numKeys];

                long getNextKey()
                {
                    while (true)
                    {
                        var key = rng.Next(lowKey, highKey + 1);    // +1 because the end # is not included
                        if (!Array.Exists(threadStructs, it => it.Key.Length > 0 && it.Key.ReadOnlySpan.AsRef<long>() == key))
                            return key;
                    }
                }

                for (var iteration = 0; iteration < NumTestIterations; ++iteration)
                {
                    // Create key structs
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        threadKeyNums[ii] = getNextKey();
                        var key = SpanByte.FromPinnedVariable(ref threadKeyNums[ii]);   // storage for the SpanByte in the pinned array
                        threadStructs[ii] = new()
                        {
                            Key = PinnedSpanByte.FromPinnedSpan(key),
                            // LockType.None means split randomly between Shared and Exclusive
                            LockType = lockType == LockType.None ? (rng.Next(0, 100) > 50 ? LockType.Shared : LockType.Exclusive) : lockType,
                            KeyHash = comparer.GetHashCode64(key),
                        };
                        threadStructs[ii].KeyHash = store.GetKeyHash(key);
                    }

                    // Sort and lock
                    store.LockTable.SortKeyHashes(threadStructs);
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        HashEntryInfo hei = new(threadStructs[ii].KeyHash);
                        PopulateHei(ref hei);
                        if (threadStructs[ii].LockType == LockType.Shared)
                            while (!store.LockTable.TryLockShared(ref hei)) { }
                        else
                            while (!store.LockTable.TryLockExclusive(ref hei)) { }
                    }

                    // Pretend to do work
                    Thread.Sleep(rng.Next(MaxSleepMs));

                    // Unlock
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        HashEntryInfo hei = new(threadStructs[ii].KeyHash);
                        PopulateHei(ref hei);
                        if (threadStructs[ii].LockType == LockType.Shared)
                            store.LockTable.UnlockShared(ref hei);
                        else
                            store.LockTable.UnlockExclusive(ref hei);
                    }
                    Array.Clear(threadStructs);
                }
            }

            for (int t = 1; t <= numThreads; t++)
            {
                var tid = ++lastTid;
                tasks.Add(Task.Factory.StartNew(() => runThread(tid)));
            }
        }
    }
}