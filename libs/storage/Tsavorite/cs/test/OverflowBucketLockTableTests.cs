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
    using LongStoreFunctions = StoreFunctions<long, long, LongKeyComparer, DefaultRecordDisposer<long, long>>;

    internal class SingleBucketComparer : IKeyComparer<long>
    {
        public bool Equals(ref long k1, ref long k2) => k1 == k2;

        public long GetHashCode64(ref long k) => 42L;
    }

    // Used to signal Setup to use the SingleBucketComparer
    public enum UseSingleBucketComparer { UseSingleBucket }

    [AllureNUnit]
    [TestFixture]
    internal class OverflowBucketLockTableTests : AllureTestBase
    {
        IKeyComparer<long> comparer = new LongKeyComparer();
        long SingleBucketKey = 1;   // We use a single bucket here for most tests so this lets us use 'ref' easily

        // For OverflowBucketLockTable, we need an instance of TsavoriteKV
        private TsavoriteKV<long, long, LongStoreFunctions, BlittableAllocator<long, long, LongStoreFunctions>> store;
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
            }, StoreFunctions<long, long>.Create(LongKeyComparer.Instance)
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
            OnTearDown();
        }

        void TryLock(long key, LockType lockType, int expectedCurrentReadLocks, bool expectedLockResult)
        {
            HashEntryInfo hei = new(comparer.GetHashCode64(ref key));
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
            HashEntryInfo hei = new(comparer.GetHashCode64(ref key));
            PopulateHei(ref hei);
            if (lockType == LockType.Shared)
                store.LockTable.UnlockShared(ref hei);
            else
                store.LockTable.UnlockExclusive(ref hei);
        }

        internal void PopulateHei(ref HashEntryInfo hei) => PopulateHei(store, ref hei);

        internal static void PopulateHei<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref HashEntryInfo hei)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
            => store.FindOrCreateTag(ref hei, store.Log.BeginAddress);

        internal void AssertLockCounts(ref HashEntryInfo hei, bool expectedX, long expectedS)
        {
            var lockState = store.LockTable.GetLockState(ref hei);
            ClassicAssert.AreEqual(expectedX, lockState.IsLockedExclusive);
            ClassicAssert.AreEqual(expectedS, lockState.NumLockedShared);
        }

        internal static void AssertLockCounts<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, TKey key, bool expectedX, int expectedS)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
            => AssertLockCounts(store, ref key, expectedX, expectedS);

        internal static void AssertLockCounts<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref TKey key, bool expectedX, int expectedS)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(ref key));
            PopulateHei(store, ref hei);
            var lockState = store.LockTable.GetLockState(ref hei);
            ClassicAssert.AreEqual(expectedX, lockState.IsLockedExclusive, "XLock mismatch");
            ClassicAssert.AreEqual(expectedS, lockState.NumLockedShared, "SLock mismatch");
        }

        internal static void AssertLockCounts<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref TKey key, bool expectedX, bool expectedS)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            FixedLengthLockableKeyStruct<TKey> keyStruct = new()
            {
                Key = key,
                KeyHash = store.storeFunctions.GetKeyHashCode64(ref key),
                LockType = LockType.None,   // Not used for this call
            };
            keyStruct.KeyHash = store.GetKeyHash(ref key);
            AssertLockCounts(store, ref keyStruct, expectedX, expectedS);
        }


        internal static void AssertLockCounts<TKey, TValue, TStoreFunctions, TAllocator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref FixedLengthLockableKeyStruct<TKey> key, bool expectedX, bool expectedS)
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            HashEntryInfo hei = new(key.KeyHash);
            PopulateHei(store, ref hei);
            var lockState = store.LockTable.GetLockState(ref hei);
            ClassicAssert.AreEqual(expectedX, lockState.IsLockedExclusive, "XLock mismatch");
            ClassicAssert.AreEqual(expectedS, lockState.NumLockedShared > 0, "SLock mismatch");
        }

        internal unsafe void AssertTotalLockCounts(long expectedX, long expectedS)
            => AssertTotalLockCounts(store, expectedX, expectedS);

        internal static unsafe void AssertTotalLockCounts<TStoreFunctions, TAllocator>(TsavoriteKV<long, long, TStoreFunctions, TAllocator> store, long expectedX, long expectedS)
            where TStoreFunctions : IStoreFunctions<long, long>
            where TAllocator : IAllocator<long, long, TStoreFunctions>
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

        internal void AssertBucketLockCount(ref FixedLengthLockableKeyStruct<long> key, long expectedX, long expectedS) => AssertBucketLockCount(store, ref key, expectedX, expectedS);

        internal static unsafe void AssertBucketLockCount<TStoreFunctions, TAllocator>(TsavoriteKV<long, long, TStoreFunctions, TAllocator> store, ref FixedLengthLockableKeyStruct<long> key, long expectedX, long expectedS)
            where TStoreFunctions : IStoreFunctions<long, long>
            where TAllocator : IAllocator<long, long, TStoreFunctions>
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
            HashEntryInfo hei = new(comparer.GetHashCode64(ref SingleBucketKey));
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
            HashEntryInfo hei = new(comparer.GetHashCode64(ref SingleBucketKey));
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

        FixedLengthLockableKeyStruct<long>[] CreateKeys(Random rng, int numKeys, int numRecords)
        {
            FixedLengthLockableKeyStruct<long> createKey()
            {
                long key = rng.Next(numKeys);
                var keyHash = store.GetKeyHash(ref key);
                return new()
                {
                    Key = key,
                    // LockType.None means split randomly between Shared and Exclusive
                    LockType = rng.Next(0, 100) < 25 ? LockType.Exclusive : LockType.Shared,
                    KeyHash = keyHash,
                };
            }
            return [.. Enumerable.Range(0, numRecords).Select(ii => createKey())];
        }

        void AssertSorted(FixedLengthLockableKeyStruct<long>[] keys, int count)
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
            var keys = CreateKeys(new Random(101), 100, 1000);
            store.LockTable.SortKeyHashes<FixedLengthLockableKeyStruct<long>>(keys);
            AssertSorted(keys, keys.Length);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void PartialArraySortTest()
        {
            var numRecords = 1000;
            var keys = CreateKeys(new Random(101), 100, numRecords);
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

                // maxNumKeys < 0 means use random number of keys
                int numKeys = maxNumKeys < 0 ? rng.Next(1, -maxNumKeys) : maxNumKeys;
                var threadStructs = new FixedLengthLockableKeyStruct<long>[numKeys];

                long getNextKey()
                {
                    while (true)
                    {
                        var key = rng.Next(lowKey, highKey + 1);    // +1 because the end # is not included
                        if (!Array.Exists(threadStructs, it => it.Key == key))
                            return key;
                    }
                }

                for (var iteration = 0; iteration < NumTestIterations; ++iteration)
                {
                    // Create key structs
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        var key = getNextKey();
                        threadStructs[ii] = new()   // local var for debugging
                        {
                            Key = key,
                            // LockType.None means split randomly between Shared and Exclusive
                            LockType = lockType == LockType.None ? (rng.Next(0, 100) > 50 ? LockType.Shared : LockType.Exclusive) : lockType,
                            KeyHash = comparer.GetHashCode64(ref key),
                        };
                        threadStructs[ii].KeyHash = store.GetKeyHash(ref key);
                    }

                    // Sort and lock
                    store.LockTable.SortKeyHashes<FixedLengthLockableKeyStruct<long>>(threadStructs);
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