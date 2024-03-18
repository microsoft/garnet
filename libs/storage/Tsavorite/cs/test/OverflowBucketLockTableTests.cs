// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LockTable
{
    internal class SingleBucketComparer : ITsavoriteEqualityComparer<long>
    {
        public bool Equals(ref long k1, ref long k2) => k1 == k2;

        public long GetHashCode64(ref long k) => 42L;
    }

    // Used to signal Setup to use the SingleBucketComparer
    public enum UseSingleBucketComparer { UseSingleBucket }

    [TestFixture]
    internal class OverflowBucketLockTableTests
    {
        ITsavoriteEqualityComparer<long> comparer = new LongTsavoriteEqualityComparer();
        long SingleBucketKey = 1;   // We use a single bucket here for most tests so this lets us use 'ref' easily

        // For OverflowBucketLockTable, we need an instance of TsavoriteKV
        private TsavoriteKV<long, long> store;
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
            comparer ??= new LongTsavoriteEqualityComparer();

            store = new TsavoriteKV<long, long>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 22 },
                                            comparer: comparer, concurrencyControlMode: ConcurrencyControlMode.LockTable);
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

        void TryLock(long key, LockType lockType, bool transient, int expectedCurrentReadLocks, bool expectedLockResult)
        {
            HashEntryInfo hei = new(comparer.GetHashCode64(ref key));
            PopulateHei(ref hei);

            // Check for existing lock
            var lockState = store.LockTable.GetLockState(ref key, ref hei);
            Assert.AreEqual(expectedCurrentReadLocks, lockState.NumLockedShared);

            if (transient)
                Assert.AreEqual(expectedLockResult, store.LockTable.TryLockTransient(ref key, ref hei, lockType));
            else
                Assert.AreEqual(expectedLockResult, store.LockTable.TryLockManual(ref key, ref hei, lockType));
        }

        void Unlock(long key, LockType lockType)
        {
            HashEntryInfo hei = new(comparer.GetHashCode64(ref key));
            PopulateHei(ref hei);
            store.LockTable.Unlock(ref key, ref hei, lockType);
        }

        internal void PopulateHei(ref HashEntryInfo hei) => PopulateHei(store, ref hei);

        internal static void PopulateHei<TKey, TValue>(TsavoriteKV<TKey, TValue> store, ref HashEntryInfo hei) => store.FindOrCreateTag(ref hei, store.Log.BeginAddress);

        internal void AssertLockCounts(ref HashEntryInfo hei, bool expectedX, long expectedS)
        {
            var lockState = store.LockTable.GetLockState(ref SingleBucketKey, ref hei);
            Assert.AreEqual(expectedX, lockState.IsLockedExclusive);
            Assert.AreEqual(expectedS, lockState.NumLockedShared);
        }

        internal static void AssertLockCounts<TKey, TValue>(TsavoriteKV<TKey, TValue> store, TKey key, bool expectedX, int expectedS)
            => AssertLockCounts(store, ref key, expectedX, expectedS);

        internal static void AssertLockCounts<TKey, TValue>(TsavoriteKV<TKey, TValue> store, ref TKey key, bool expectedX, int expectedS)
        {
            HashEntryInfo hei = new(store.comparer.GetHashCode64(ref key));
            PopulateHei(store, ref hei);
            var lockState = store.LockTable.GetLockState(ref key, ref hei);
            Assert.AreEqual(expectedX, lockState.IsLockedExclusive, "XLock mismatch");
            Assert.AreEqual(expectedS, lockState.NumLockedShared, "SLock mismatch");
        }

        internal static void AssertLockCounts<TKey, TValue>(TsavoriteKV<TKey, TValue> store, ref TKey key, bool expectedX, bool expectedS)
        {
            FixedLengthLockableKeyStruct<TKey> keyStruct = new()
            {
                Key = key,
                KeyHash = store.comparer.GetHashCode64(ref key),
                LockType = LockType.None,   // Not used for this call
            };
            keyStruct.KeyHash = store.GetKeyHash(ref key);
            AssertLockCounts(store, ref keyStruct, expectedX, expectedS);
        }


        internal static void AssertLockCounts<TKey, TValue>(TsavoriteKV<TKey, TValue> store, ref FixedLengthLockableKeyStruct<TKey> key, bool expectedX, bool expectedS)
        {
            HashEntryInfo hei = new(key.KeyHash);
            PopulateHei(store, ref hei);
            var lockState = store.LockTable.GetLockState(ref key.Key, ref hei);
            Assert.AreEqual(expectedX, lockState.IsLockedExclusive, "XLock mismatch");
            Assert.AreEqual(expectedS, lockState.NumLockedShared > 0, "SLock mismatch");
        }

        internal unsafe void AssertTotalLockCounts(long expectedX, long expectedS)
            => AssertTotalLockCounts(store, expectedX, expectedS);

        internal static unsafe void AssertTotalLockCounts(TsavoriteKV<long, long> store, long expectedX, long expectedS)
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
            Assert.AreEqual(expectedX, xcount);
            Assert.AreEqual(expectedS, scount);
        }

        internal void AssertBucketLockCount(ref FixedLengthLockableKeyStruct<long> key, long expectedX, long expectedS) => AssertBucketLockCount(store, ref key, expectedX, expectedS);

        internal static unsafe void AssertBucketLockCount(TsavoriteKV<long, long> store, ref FixedLengthLockableKeyStruct<long> key, long expectedX, long expectedS)
        {
            var bucketIndex = store.LockTable.GetBucketIndex(key.KeyHash);
            var bucket = store.state[store.resizeInfo.version].tableAligned + bucketIndex;
            Assert.AreEqual(expectedX == 1, HashBucket.IsLatchedExclusive(bucket));
            Assert.AreEqual(expectedS, HashBucket.NumLatchedShared(bucket));
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
            TryLock(key, LockType.Shared, transient: true, expectedCurrentReadLocks: 0, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 1);

            // Add a non-transient lock
            TryLock(key, LockType.Shared, transient: false, expectedCurrentReadLocks: 1, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 2);

            // Now both transient and manual x locks with the same key should fail
            TryLock(key, LockType.Exclusive, transient: true, expectedCurrentReadLocks: 2, expectedLockResult: false);
            AssertLockCounts(ref hei, false, 2);
            TryLock(key, LockType.Exclusive, transient: false, expectedCurrentReadLocks: 2, expectedLockResult: false);
            AssertLockCounts(ref hei, false, 2);

            // Now unlock
            Unlock(key, LockType.Shared);
            AssertLockCounts(ref hei, false, 1);
            Unlock(key, LockType.Shared);
            AssertLockCounts(ref hei, false, 0);

            // Now exclusive should succeed
            TryLock(key, LockType.Exclusive, transient: false, expectedCurrentReadLocks: 0, expectedLockResult: true);
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

            TryLock(1, LockType.Shared, transient: false, expectedCurrentReadLocks: 0, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 1);

            TryLock(2, LockType.Shared, transient: false, expectedCurrentReadLocks: 1, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 2);

            TryLock(3, LockType.Shared, transient: false, expectedCurrentReadLocks: 2, expectedLockResult: true);
            AssertLockCounts(ref hei, false, 3);

            // Exclusive lock should fail
            TryLock(4, LockType.Exclusive, transient: false, expectedCurrentReadLocks: 3, expectedLockResult: false);
            AssertLockCounts(ref hei, false, 3);

            // Now unlock
            Unlock(3, LockType.Shared);
            AssertLockCounts(ref hei, false, 2);
            Unlock(2, LockType.Shared);
            AssertLockCounts(ref hei, false, 1);
            Unlock(1, LockType.Shared);
            AssertLockCounts(ref hei, false, 0);

            // Now exclusive should succeed
            TryLock(4, LockType.Exclusive, transient: false, expectedCurrentReadLocks: 0, expectedLockResult: true);
            AssertLockCounts(ref hei, true, 0);
            Unlock(4, LockType.Exclusive);
            AssertLockCounts(ref hei, false, 0);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTest1Thread()
        {
            List<Task> tasks = new();
            var lastTid = 0;
            AddThreads(tasks, ref lastTid, numThreads: 1, maxNumKeys: 5, lowKey: 1, highKey: 5, LockType.Exclusive);
            Task.WaitAll(tasks.ToArray());
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTestMultiThreadsNoContention([Values(3, 8)] int numThreads)
        {
            List<Task> tasks = new();
            var lastTid = 0;
            for (var ii = 0; ii < numThreads; ++ii)
                AddThreads(tasks, ref lastTid, numThreads: 1, maxNumKeys: 5, lowKey: 1 + 10 * ii, highKey: 5 + 10 * ii, LockType.Exclusive);
            Task.WaitAll(tasks.ToArray());
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTestMultiThreadsFullContention([Values(3, 8)] int numThreads, [Values] LockType lockType)
        {
            List<Task> tasks = new();
            var lastTid = 0;
            AddThreads(tasks, ref lastTid, numThreads: numThreads, maxNumKeys: 5, lowKey: 1, highKey: 5, lockType);
            Task.WaitAll(tasks.ToArray());
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ThreadedLockStressTestMultiThreadsRandomContention([Values(3, 8)] int numThreads, [Values] LockType lockType)
        {
            List<Task> tasks = new();
            var lastTid = 0;
            AddThreads(tasks, ref lastTid, numThreads: numThreads, maxNumKeys: 5, lowKey: 1, highKey: 10 * (numThreads / 2), lockType);
            Task.WaitAll(tasks.ToArray());
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
            return Enumerable.Range(0, numRecords).Select(ii => createKey()).ToArray();
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

                Assert.GreaterOrEqual(store.LockTable.CompareKeyHashes(key, keys[ii - 1]), 0);
                if (key.KeyHash != prevCode)
                {
                    // The BucketIndex of the keys must be nondecreasing, and may be equal but the first in such an equal sequence must be Exclusive.
                    Assert.Greater(store.LockTable.GetBucketIndex(key.KeyHash), store.LockTable.GetBucketIndex(prevCode));
                    lastXcode = key.LockType == LockType.Exclusive ? key.KeyHash : -2;
                }
                else
                {
                    // Identical BucketIndex sequence must start with an exclusive lock, followed by any number of exclusive locks, followed by any number of shared locks.
                    // (Enumeration will take only the first).
                    Assert.AreEqual(lastXcode, key.KeyHash);
                    if (key.LockType == LockType.Exclusive)
                        Assert.AreNotEqual(LockType.Shared, lastLockType);
                    lastLockType = key.LockType;
                }
            }
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void FullArraySortTest()
        {
            var keys = CreateKeys(new Random(101), 100, 1000);
            store.LockTable.SortKeyHashes(keys);
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

            store.LockTable.SortKeyHashes(keys, 0, count);
            AssertSorted(keys, count);

            // Verify later elements were untouched.
            for (var ii = count; ii < numRecords; ++ii)
                keys[ii].KeyHash = -ii;
        }

        const int NumTestIterations = 15;
        const int maxSleepMs = 5;

        private void AddThreads(List<Task> tasks, ref int lastTid, int numThreads, int maxNumKeys, int lowKey, int highKey, LockType lockType)
        {
            void runThread(int tid)
            {
                Random rng = new(101 * tid);

                // maxNumKeys < 0 means use random number of keys
                int numKeys = maxNumKeys < 0 ? rng.Next(1, -maxNumKeys) : maxNumKeys;
                FixedLengthLockableKeyStruct<long>[] threadStructs = new FixedLengthLockableKeyStruct<long>[numKeys];

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
                    store.LockTable.SortKeyHashes(threadStructs);
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        HashEntryInfo hei = new(threadStructs[ii].KeyHash);
                        PopulateHei(ref hei);
                        while (!store.LockTable.TryLockManual(ref threadStructs[ii].Key, ref hei, threadStructs[ii].LockType))
                            ;
                    }

                    // Pretend to do work
                    Thread.Sleep(rng.Next(maxSleepMs));

                    // Unlock
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        HashEntryInfo hei = new(threadStructs[ii].KeyHash);
                        PopulateHei(ref hei);
                        store.LockTable.Unlock(ref threadStructs[ii].Key, ref hei, threadStructs[ii].LockType);
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