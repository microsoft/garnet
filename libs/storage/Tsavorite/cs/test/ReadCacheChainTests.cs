// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using Tsavorite.test.LockableUnsafeContext;
using Tsavorite.test.LockTable;
using static Tsavorite.test.TestUtils;

#pragma warning disable IDE0060 // Remove unused parameter (used for Setup)

namespace Tsavorite.test.ReadCacheTests
{
    class ChainTests
    {
        private TsavoriteKV<long, long> store;
        private IDevice log;
        const long lowChainKey = 40;
        const long midChainKey = lowChainKey + chainLen * (mod / 2);
        const long highChainKey = lowChainKey + chainLen * (mod - 1);
        const int mod = 10;
        const int chainLen = 10;
        const int valueAdd = 1_000_000;

        // -1 so highChainKey is first in the chain.
        const long numKeys = highChainKey + mod - 1;

        // Insert into chain.
        const long spliceInNewKey = highChainKey + mod * 2;
        const long spliceInExistingKey = highChainKey - mod;
        const long immutableSplitKey = numKeys / 2;

        // This is the record after the first readcache record we insert; it lets us limit the range to ReadCacheEvict
        // so we get outsplicing rather than successively overwriting the hash table entry on ReadCacheEvict.
        long readCacheBelowMidChainKeyEvictionAddress;

        internal class ChainComparer : ITsavoriteEqualityComparer<long>
        {
            readonly int mod;

            internal ChainComparer(int mod) => this.mod = mod;

            public bool Equals(ref long k1, ref long k2) => k1 == k2;

            public long GetHashCode64(ref long k) => k % mod;
        }

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 9 };
            log = Devices.CreateLogDevice(MethodTestDir + "/NativeReadCacheTests.log", deleteOnClose: true);

            var concurrencyControlMode = ConcurrencyControlMode.None;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ConcurrencyControlMode ccm)
                {
                    concurrencyControlMode = ccm;
                    continue;
                }
            }

            store = new TsavoriteKV<long, long>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings },
                comparer: new ChainComparer(mod), concurrencyControlMode: concurrencyControlMode);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        public enum RecordRegion { Immutable, OnDisk, Mutable };

        void PopulateAndEvict(RecordRegion recordRegion = RecordRegion.OnDisk)
        {
            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());

            if (recordRegion != RecordRegion.Immutable)
            {
                for (int key = 0; key < numKeys; key++)
                    session.Upsert(key, key + valueAdd);
                session.CompletePending(true);
                if (recordRegion == RecordRegion.OnDisk)
                    store.Log.FlushAndEvict(true);
                return;
            }

            // Two parts, so we can have some evicted (and bring them into the readcache), and some in immutable (readonly).
            for (int key = 0; key < immutableSplitKey; key++)
                session.Upsert(key, key + valueAdd);
            session.CompletePending(true);
            store.Log.FlushAndEvict(true);

            for (long key = immutableSplitKey; key < numKeys; key++)
                session.Upsert(key, key + valueAdd);
            session.CompletePending(true);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
        }

        void CreateChain(RecordRegion recordRegion = RecordRegion.OnDisk)
        {
            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
            long output = -1;
            bool expectPending(long key) => recordRegion == RecordRegion.OnDisk || (recordRegion == RecordRegion.Immutable && key < immutableSplitKey);

            // Pass1: PENDING reads and populate the cache
            for (long ii = 0; ii < chainLen; ++ii)
            {
                var key = lowChainKey + ii * mod;
                var status = session.Read(key, out _);
                if (expectPending(key))
                {
                    Assert.IsTrue(status.IsPending, status.ToString());
                    session.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                    Assert.IsTrue(status.Record.CopiedToReadCache, status.ToString());
                }
                Assert.IsTrue(status.Found, status.ToString());
                if (key < midChainKey)
                    readCacheBelowMidChainKeyEvictionAddress = store.ReadCache.TailAddress;
            }

            // Pass2: non-PENDING reads from the cache
            for (var ii = 0; ii < chainLen; ++ii)
            {
                var status = session.Read(lowChainKey + ii * mod, out _);
                Assert.IsTrue(!status.IsPending && status.Found, status.ToString());
            }

            // Pass 3: Put in bunch of extra keys into the cache so when we FlushAndEvict we get all the ones of interest.
            for (var key = 0; key < numKeys; ++key)
            {
                if ((key % mod) != 0)
                {
                    var status = session.Read(key, out _);
                    if (expectPending(key))
                    {
                        Assert.IsTrue(status.IsPending);
                        session.CompletePendingWithOutputs(out var outputs, wait: true);
                        (status, output) = GetSinglePendingResult(outputs);
                        Assert.IsTrue(status.Record.CopiedToReadCache, status.ToString());
                    }
                    Assert.IsTrue(status.Found, status.ToString());
                    session.CompletePending(wait: true);
                }
            }
        }

        unsafe bool GetRecordInInMemoryHashChain(long key, out bool isReadCache)
        {
            // returns whether the key was found before we'd go pending
            var (la, pa) = GetHashChain(store, key, out long recordKey, out bool invalid, out isReadCache);
            while (isReadCache || la >= store.hlog.HeadAddress)
            {
                if (recordKey == key && !invalid)
                    return true;
                (la, pa) = NextInChain(store, pa, out recordKey, out invalid, ref isReadCache);
            }
            return false;
        }

        internal bool FindRecordInReadCache(long key, out bool invalid, out long logicalAddress, out long physicalAddress)
        {
            // returns whether the key was found before we'd go pending
            (logicalAddress, physicalAddress) = GetHashChain(store, key, out long recordKey, out invalid, out bool isReadCache);
            while (isReadCache)
            {
                if (recordKey == key)
                    return true;
                (logicalAddress, physicalAddress) = NextInChain(store, physicalAddress, out recordKey, out invalid, ref isReadCache);
            }
            return false;
        }

        internal static (long logicalAddress, long physicalAddress) GetHashChain(TsavoriteKV<long, long> store, long key, out long recordKey, out bool invalid, out bool isReadCache)
        {
            var tagExists = store.FindHashBucketEntryForKey(ref key, out var entry);
            Assert.IsTrue(tagExists);

            isReadCache = entry.ReadCache;
            var log = isReadCache ? store.readcache : store.hlog;
            var pa = log.GetPhysicalAddress(entry.Address & ~Constants.kReadCacheBitMask);
            recordKey = log.GetKey(pa);
            invalid = log.GetInfo(pa).Invalid;

            return (entry.Address, pa);
        }

        (long logicalAddress, long physicalAddress) NextInChain(long physicalAddress, out long recordKey, out bool invalid, ref bool isReadCache)
            => NextInChain(store, physicalAddress, out recordKey, out invalid, ref isReadCache);

        internal static (long logicalAddress, long physicalAddress) NextInChain(TsavoriteKV<long, long> store, long physicalAddress, out long recordKey, out bool invalid, ref bool isReadCache)
        {
            var log = isReadCache ? store.readcache : store.hlog;
            var info = log.GetInfo(physicalAddress);
            var la = info.PreviousAddress;

            isReadCache = new HashBucketEntry { word = la }.ReadCache;
            log = isReadCache ? store.readcache : store.hlog;
            la &= ~Constants.kReadCacheBitMask;
            var pa = log.GetPhysicalAddress(la);
            recordKey = log.GetKey(pa);
            invalid = log.GetInfo(pa).Invalid;
            return (la, pa);
        }

        (long logicalAddress, long physicalAddress) ScanReadCacheChain(long[] omitted = null, bool evicted = false, bool deleted = false)
        {
            omitted ??= Array.Empty<long>();

            var (la, pa) = GetHashChain(store, lowChainKey, out long actualKey, out bool invalid, out bool isReadCache);
            for (var expectedKey = highChainKey; expectedKey >= lowChainKey; expectedKey -= mod)
            {
                // We evict from readcache only to just below midChainKey
                if (!evicted || expectedKey >= midChainKey)
                    Assert.IsTrue(isReadCache);

                if (isReadCache)
                {
                    Assert.AreEqual(expectedKey, actualKey);
                    if (omitted.Contains(expectedKey))
                        Assert.IsTrue(invalid);
                }
                else if (omitted.Contains(actualKey))
                {
                    Assert.AreEqual(deleted, store.hlog.GetInfo(pa).Tombstone);
                }

                (la, pa) = NextInChain(pa, out actualKey, out invalid, ref isReadCache);
                if (!isReadCache && la < store.hlog.HeadAddress)
                    break;
            }
            Assert.IsFalse(isReadCache);
            return (la, pa);
        }

        (long logicalAddress, long physicalAddress) SkipReadCacheChain(long key)
            => SkipReadCacheChain(store, key);

        internal static (long logicalAddress, long physicalAddress) SkipReadCacheChain(TsavoriteKV<long, long> store, long key)
        {
            var (la, pa) = GetHashChain(store, key, out _, out _, out bool isReadCache);
            while (isReadCache)
                (la, pa) = NextInChain(store, pa, out _, out _, ref isReadCache);
            return (la, pa);
        }

        void VerifySplicedInKey(long expectedKey)
        {
            // Scan to the end of the readcache chain and verify we inserted the value.
            var (_, pa) = SkipReadCacheChain(expectedKey);
            var storedKey = store.hlog.GetKey(pa);
            Assert.AreEqual(expectedKey, storedKey);
        }

        static void ClearCountsOnError(ClientSession<long, long, long, long, Empty, SimpleFunctions<long, long>> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        void AssertTotalLockCounts(long expectedX, long expectedS) => OverflowBucketLockTableTests.AssertTotalLockCounts(store, expectedX, expectedS);

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void ChainVerificationTest()
        {
            PopulateAndEvict();
            CreateChain();

            ScanReadCacheChain();
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteCacheRecordTest([Values] ConcurrencyControlMode concurrencyControlMode)
        {
            PopulateAndEvict();
            CreateChain();
            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());

            void doTest(long key)
            {
                var status = session.Delete(key);
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                status = session.Read(key, out var value);
                Assert.IsFalse(status.Found, status.ToString());
            }

            doTest(lowChainKey);
            doTest(highChainKey);
            doTest(midChainKey);
            ScanReadCacheChain(new[] { lowChainKey, midChainKey, highChainKey }, evicted: false);

            store.ReadCacheEvict(store.ReadCache.BeginAddress, readCacheBelowMidChainKeyEvictionAddress);
            ScanReadCacheChain(new[] { lowChainKey, midChainKey, highChainKey }, evicted: true, deleted: true);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteHalfOfAllCacheRecordsTest([Values] ConcurrencyControlMode concurrencyControlMode)
        {
            PopulateAndEvict();
            CreateChain();
            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());

            void doTest(long key)
            {
                var status = session.Delete(key);
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                status = session.Read(key, out var value);
                Assert.IsFalse(status.Found, status.ToString());
            }

            // Should be found in the readcache before deletion
            Assert.IsTrue(GetRecordInInMemoryHashChain(lowChainKey, out bool isReadCache));
            Assert.IsTrue(isReadCache);
            Assert.IsTrue(GetRecordInInMemoryHashChain(midChainKey, out isReadCache));
            Assert.IsTrue(isReadCache);
            Assert.IsTrue(GetRecordInInMemoryHashChain(highChainKey, out isReadCache));
            Assert.IsTrue(isReadCache);

            // Delete all keys in the readcache chain below midChainKey.
            for (var ii = lowChainKey; ii < midChainKey; ++ii)
                doTest(ii);

            // LowChainKey should not be found in the readcache after deletion to just below midChainKey, but mid- and highChainKey should not be affected.
            Assert.IsTrue(GetRecordInInMemoryHashChain(lowChainKey, out isReadCache));
            Assert.IsFalse(isReadCache);
            Assert.IsTrue(GetRecordInInMemoryHashChain(midChainKey, out isReadCache));
            Assert.IsTrue(isReadCache);
            Assert.IsTrue(GetRecordInInMemoryHashChain(highChainKey, out isReadCache));
            Assert.IsTrue(isReadCache);

            store.ReadCacheEvict(store.ReadCache.BeginAddress, readCacheBelowMidChainKeyEvictionAddress);

            // Following deletion to just below midChainKey:
            //  lowChainKey's tombstone should still be found in the mutable portion of the log
            //  midChainKey and highChainKey should be found in the readcache
            Assert.IsTrue(GetRecordInInMemoryHashChain(lowChainKey, out isReadCache));
            Assert.IsFalse(isReadCache);
            Assert.IsTrue(GetRecordInInMemoryHashChain(midChainKey, out isReadCache));
            Assert.IsTrue(isReadCache);
            Assert.IsTrue(GetRecordInInMemoryHashChain(highChainKey, out isReadCache));
            Assert.IsTrue(isReadCache);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void UpsertCacheRecordTest([Values] ConcurrencyControlMode concurrencyControlMode)
        {
            DoUpdateTest(useRMW: false);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void RMWCacheRecordTest([Values] ConcurrencyControlMode concurrencyControlMode)
        {
            DoUpdateTest(useRMW: true);
        }

        void DoUpdateTest(bool useRMW)
        {
            PopulateAndEvict();
            CreateChain();
            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());

            void doTest(long key)
            {
                var status = session.Read(key, out var value);
                Assert.IsTrue(status.Found, status.ToString());

                if (useRMW)
                {
                    // RMW will use the readcache entry for its source and then invalidate it.
                    status = session.RMW(key, value + valueAdd);
                    Assert.IsTrue(status.Found && status.Record.CopyUpdated, status.ToString());

                    Assert.IsTrue(FindRecordInReadCache(key, out bool invalid, out _, out _));
                    Assert.IsTrue(invalid);
                }
                else
                {
                    status = session.Upsert(key, value + valueAdd);
                    Assert.IsTrue(status.Record.Created, status.ToString());
                }

                status = session.Read(key, out value);
                Assert.IsTrue(status.Found, status.ToString());
                Assert.AreEqual(key + valueAdd * 2, value);
            }

            doTest(lowChainKey);
            doTest(highChainKey);
            doTest(midChainKey);
            ScanReadCacheChain(new[] { lowChainKey, midChainKey, highChainKey }, evicted: false);

            store.ReadCacheEvict(store.ReadCache.BeginAddress, readCacheBelowMidChainKeyEvictionAddress);
            ScanReadCacheChain(new[] { lowChainKey, midChainKey, highChainKey }, evicted: true);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpliceInFromCTTTest([Values] ConcurrencyControlMode concurrencyControlMode)
        {
            PopulateAndEvict();
            CreateChain();

            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
            long input = 0, output = 0, key = lowChainKey - mod; // key must be in evicted region for this test
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };

            var status = session.Read(ref key, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePending(wait: true);

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpliceInFromUpsertTest([Values] RecordRegion recordRegion, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            PopulateAndEvict(recordRegion);
            CreateChain(recordRegion);

            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
            long key = -1;

            if (recordRegion == RecordRegion.Immutable || recordRegion == RecordRegion.OnDisk)
            {
                key = spliceInExistingKey;
                var status = session.Upsert(key, key + valueAdd);
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }
            else
            {
                key = spliceInNewKey;
                var status = session.Upsert(key, key + valueAdd);
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpliceInFromRMWTest([Values] RecordRegion recordRegion, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            PopulateAndEvict(recordRegion);
            CreateChain(recordRegion);

            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
            long key = -1, output = -1;

            if (recordRegion == RecordRegion.Immutable || recordRegion == RecordRegion.OnDisk)
            {
                // Existing key
                key = spliceInExistingKey;
                var status = session.RMW(key, key + valueAdd);

                // If OnDisk, this used the readcache entry for its source and then invalidated it.
                Assert.IsTrue(status.Found && status.Record.CopyUpdated, status.ToString());
                if (recordRegion == RecordRegion.OnDisk)
                {
                    Assert.IsTrue(FindRecordInReadCache(key, out bool invalid, out _, out _));
                    Assert.IsTrue(invalid);
                }

                { // New key
                    key = spliceInNewKey;
                    status = session.RMW(key, key + valueAdd);

                    // This NOTFOUND key will return PENDING because we have to trace back through the collisions.
                    Assert.IsTrue(status.IsPending, status.ToString());
                    session.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                    Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                }
            }
            else
            {
                key = spliceInNewKey;
                var status = session.RMW(key, key + valueAdd);
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpliceInFromDeleteTest([Values] RecordRegion recordRegion, [Values] ConcurrencyControlMode concurrencyControlMode)
        {
            PopulateAndEvict(recordRegion);
            CreateChain(recordRegion);

            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
            long key = -1;

            if (recordRegion == RecordRegion.Immutable || recordRegion == RecordRegion.OnDisk)
            {
                key = spliceInExistingKey;
                var status = session.Delete(key);
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }
            else
            {
                key = spliceInNewKey;
                var status = session.Delete(key);
                Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyLockCountsAfterReadCacheEvict([Values(ConcurrencyControlMode.LockTable)] ConcurrencyControlMode concurrencyControlMode)
        {
            PopulateAndEvict();
            CreateChain();

            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long>>(new SimpleFunctions<long, long>());
            var luContext = session.LockableUnsafeContext;

            var keys = new[]
            {
                new FixedLengthLockableKeyStruct<long>(lowChainKey, LockType.Exclusive, luContext),
                new FixedLengthLockableKeyStruct<long>(midChainKey, LockType.Shared, luContext),
                new FixedLengthLockableKeyStruct<long>(highChainKey, LockType.Exclusive, luContext)
            };

            luContext.BeginUnsafe();
            luContext.BeginLockable();

            try
            {
                luContext.SortKeyHashes(keys);

                // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
                luContext.Lock(keys);

                store.ReadCache.FlushAndEvict(wait: true);

                int xlocks = 0, slocks = 0;
                foreach (var idx in LockableUnsafeContextTests.EnumActionKeyIndices(keys, LockableUnsafeContextTests.LockOperationType.Unlock))
                {
                    if (keys[idx].LockType == LockType.Exclusive)
                        ++xlocks;
                    else
                        ++slocks;
                }
                AssertTotalLockCounts(xlocks, slocks);

                foreach (var idx in LockableUnsafeContextTests.EnumActionKeyIndices(keys, LockableUnsafeContextTests.LockOperationType.Unlock))
                {
                    ref var key = ref keys[idx];
                    HashEntryInfo hei = new(store.comparer.GetHashCode64(ref key.Key));
                    OverflowBucketLockTableTests.PopulateHei(store, ref hei);

                    var lockState = store.LockTable.GetLockState(ref key.Key, ref hei);
                    Assert.IsTrue(lockState.IsFound);
                    Assert.AreEqual(key.LockType == LockType.Exclusive, lockState.IsLockedExclusive);
                    Assert.AreEqual(key.LockType != LockType.Exclusive, lockState.NumLockedShared > 0);

                    luContext.Unlock(keys, idx, 1);
                    lockState = store.LockTable.GetLockState(ref key.Key, ref hei);
                    Assert.IsFalse(lockState.IsLockedExclusive);
                    Assert.AreEqual(0, lockState.NumLockedShared);
                }
                AssertTotalLockCounts(0, 0);
            }
            catch (Exception)
            {
                ClearCountsOnError(session);
                throw;
            }
            finally
            {
                luContext.EndLockable();
                luContext.EndUnsafe();
            }

            AssertTotalLockCounts(0, 0);
        }
    }

    class LongStressChainTests
    {
        private TsavoriteKV<long, long> store;
        private IDevice log;
        const long valueAdd = 1_000_000_000;

        const long numKeys = 2_000;

        struct LongComparerModulo : ITsavoriteEqualityComparer<long>
        {
            readonly HashModulo modRange;

            internal LongComparerModulo(HashModulo mod) => modRange = mod;

            public bool Equals(ref long k1, ref long k2) => k1 == k2;

            // Force collisions to create a chain
            public long GetHashCode64(ref long k)
            {
                long value = Utility.GetHashCode(k);
                return modRange != HashModulo.NoMod ? value % (long)modRange : value;
            }
        }

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            string filename = MethodTestDir + $"/{GetType().Name}.log";
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is DeviceType deviceType)
                {
                    log = CreateTestDevice(deviceType, filename, deleteOnClose: true);
                    continue;
                }
            }
            log ??= Devices.CreateLogDevice(filename, deleteOnClose: true);

            // Make the main log small enough that we force the readcache
            ReadCacheSettings readCacheSettings = new() { MemorySizeBits = 15, PageSizeBits = 9 };
            var logSettings = new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings };

            HashModulo modRange = HashModulo.NoMod;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo cr)
                {
                    modRange = cr;
                    continue;
                }
            }

            store = new TsavoriteKV<long, long>(1L << 20, logSettings, comparer: new LongComparerModulo(modRange),
                concurrencyControlMode: ConcurrencyControlMode.RecordIsolation);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        internal class RmwLongFunctions : SimpleFunctions<long, long, Empty>
        {
            /// <inheritdoc/>
            public override bool ConcurrentWriter(ref long key, ref long input, ref long src, ref long dst, ref long output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                dst = output = src;
                return true;
            }

            /// <inheritdoc/>
            public override bool SingleWriter(ref long key, ref long input, ref long src, ref long dst, ref long output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                dst = output = src;
                return true;
            }

            /// <inheritdoc/>
            public override bool CopyUpdater(ref long key, ref long input, ref long oldValue, ref long newValue, ref long output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                newValue = output = input;
                return true;
            }

            /// <inheritdoc/>
            public override bool InPlaceUpdater(ref long key, ref long input, ref long value, ref long output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                value = output = input;
                return true;
            }

            /// <inheritdoc/>
            public override bool InitialUpdater(ref long key, ref long input, ref long value, ref long output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                Assert.Fail("For these tests, InitialUpdater should never be called");
                return false;
            }
        }

        unsafe void PopulateAndEvict()
        {
            using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long, Empty>>(new SimpleFunctions<long, long, Empty>());

            for (long ii = 0; ii < numKeys; ii++)
            {
                long key = ii;
                var status = session.Upsert(ref key, ref key);
                Assert.IsFalse(status.IsPending);
                Assert.IsTrue(status.Record.Created, $"key {key}, status {status}");
            }
            session.CompletePending(true);
            store.Log.FlushAndEvict(true);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(StressTestCategory)]
        //[Repeat(300)]
#pragma warning disable IDE0060 // Remove unused parameter (modRange is used by Setup())
        public void LongRcMultiThreadTest([Values] HashModulo modRange, [Values(0, 1, 2, 8)] int numReadThreads, [Values(0, 1, 2, 8)] int numWriteThreads,
                                          [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
#if WINDOWS
                                              [Values(DeviceType.LSD
#else
                                              [Values(DeviceType.MLSD
#endif
                                              )] DeviceType deviceType)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            if (numReadThreads == 0 && numWriteThreads == 0)
                Assert.Ignore("Skipped due to 0 threads for both read and update");
            if ((numReadThreads > 2 || numWriteThreads > 2) && IsRunningAzureTests)
                Assert.Ignore("Skipped because > 2 threads when IsRunningAzureTests");
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            PopulateAndEvict();

            const int numIterations = 1;
            unsafe void runReadThread(int tid)
            {
                using var session = store.NewSession<long, long, Empty, SimpleFunctions<long, long, Empty>>(new SimpleFunctions<long, long, Empty>());

                Random rng = new(tid * 101);
                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        long key = ii, output = 0;
                        var status = session.Read(ref key, ref output);
                        bool wasPending = status.IsPending;
                        if (wasPending)
                        {
                            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            (status, output) = GetSinglePendingResult(completedOutputs, out var recordMetadata);
                            Assert.AreEqual(recordMetadata.Address == Constants.kInvalidAddress, status.Record.CopiedToReadCache, $"key {ii}: {status}");
                        }
                        Assert.IsTrue(status.Found, $"key {key}, status {status}, wasPending {wasPending}");
                        Assert.AreEqual(ii, output % valueAdd);
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                using var session = store.NewSession<long, long, Empty, RmwLongFunctions>(new RmwLongFunctions());

                Random rng = new(tid * 101);
                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        long key = ii, input = ii + valueAdd * tid, output = 0;
                        var status = updateOp == UpdateOp.RMW
                                        ? session.RMW(ref key, ref input, ref output)
                                        : session.Upsert(ref key, ref input, ref input, ref output);
                        bool wasPending = status.IsPending;
                        if (wasPending)
                        {
                            Assert.AreNotEqual(UpdateOp.Upsert, updateOp, "Upsert should not go pending");
                            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            (status, output) = GetSinglePendingResult(completedOutputs);

                            // Record may have been updated in-place if a CTT was done during the pending operation.
                            // Assert.IsTrue(status.Record.CopyUpdated, $"Expected Record.CopyUpdated but was: {status}");
                        }
                        if (updateOp == UpdateOp.RMW)   // Upsert will not try to find records below HeadAddress, but it may find them in-memory
                            Assert.IsTrue(status.Found, $"key {key}, status {status}, wasPending {wasPending}");
                        Assert.AreEqual(ii + valueAdd * tid, output);
                    }
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exceptions.
            for (int t = 1; t <= numReadThreads + numWriteThreads; t++)
            {
                var tid = t;
                if (t <= numReadThreads)
                    tasks.Add(Task.Factory.StartNew(() => runReadThread(tid)));
                else
                    tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }

    class SpanByteStressChainTests
    {
        private TsavoriteKV<SpanByte, SpanByte> store;
        private IDevice log;
        const long valueAdd = 1_000_000_000;

        const long numKeys = 2_000;

        struct SpanByteComparerModulo : ITsavoriteEqualityComparer<SpanByte>
        {
            readonly HashModulo modRange;

            internal SpanByteComparerModulo(HashModulo mod) => modRange = mod;

            public readonly bool Equals(ref SpanByte k1, ref SpanByte k2) => SpanByteComparer.StaticEquals(ref k1, ref k2);

            // Force collisions to create a chain
            public long GetHashCode64(ref SpanByte k)
            {
                var value = SpanByteComparer.StaticGetHashCode64(ref k);
                return modRange != HashModulo.NoMod ? value % (long)modRange : value;
            }
        }

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            string filename = MethodTestDir + $"/{GetType().Name}.log";
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is DeviceType deviceType)
                {
                    log = CreateTestDevice(deviceType, filename, deleteOnClose: true);
                    continue;
                }
            }
            log ??= Devices.CreateLogDevice(filename, deleteOnClose: true);

            // Make the main log small enough that we force the readcache
            var readCacheSettings = new ReadCacheSettings { MemorySizeBits = 15, PageSizeBits = 9 };
            var logSettings = new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10, ReadCacheSettings = readCacheSettings };

            HashModulo modRange = HashModulo.NoMod;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo cr)
                {
                    modRange = cr;
                    continue;
                }
            }

            store = new TsavoriteKV<SpanByte, SpanByte>(1L << 20, logSettings, comparer: new SpanByteComparerModulo(modRange),
                concurrencyControlMode: ConcurrencyControlMode.LockTable);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir);
        }

        internal class RmwSpanByteFunctions : SpanByteFunctions<Empty>
        {
            /// <inheritdoc/>
            public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                src.CopyTo(ref dst);
                src.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                src.CopyTo(ref dst);
                src.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                input.CopyTo(ref newValue);
                input.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                // The default implementation of IPU simply writes input to destination, if there is space
                base.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
                input.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                Assert.Fail("For these tests, InitialUpdater should never be called");
                return false;
            }
        }

        unsafe void PopulateAndEvict()
        {
            using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());

            Span<byte> keyVec = stackalloc byte[sizeof(long)];
            var key = SpanByte.FromFixedSpan(keyVec);

            for (long ii = 0; ii < numKeys; ii++)
            {
                Assert.IsTrue(BitConverter.TryWriteBytes(keyVec, ii));
                var status = session.Upsert(ref key, ref key);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }
            session.CompletePending(true);
            store.Log.FlushAndEvict(true);
        }

        static void ClearCountsOnError(ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, IFunctions<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty>> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(StressTestCategory)]
        //[Repeat(300)]
        public void SpanByteRcMultiThreadTest([Values] HashModulo modRange, [Values(0, 1, 2, 8)] int numReadThreads, [Values(0, 1, 2, 8)] int numWriteThreads,
                                              [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
#if WINDOWS
                                              [Values(DeviceType.LSD
#else
                                              [Values(DeviceType.MLSD
#endif
                                              )] DeviceType deviceType)
        {
            if (numReadThreads == 0 && numWriteThreads == 0)
                Assert.Ignore("Skipped due to 0 threads for both read and update");
            if ((numReadThreads > 2 || numWriteThreads > 2) && IsRunningAzureTests)
                Assert.Ignore("Skipped because > 2 threads when IsRunningAzureTests");
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            PopulateAndEvict();

            const int numIterations = 1;
            unsafe void runReadThread(int tid)
            {
                using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());

                Span<byte> keyVec = stackalloc byte[sizeof(long)];
                var key = SpanByte.FromFixedSpan(keyVec);

                Random rng = new(tid * 101);
                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        SpanByteAndMemory output = default;

                        Assert.IsTrue(BitConverter.TryWriteBytes(keyVec, ii));
                        var status = session.Read(ref key, ref output);
                        bool wasPending = status.IsPending;
                        if (wasPending)
                        {
                            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            (status, output) = GetSinglePendingResult(completedOutputs, out var recordMetadata);
                            Assert.AreEqual(recordMetadata.Address == Constants.kInvalidAddress, status.Record.CopiedToReadCache, $"key {ii}: {status}");
                        }
                        Assert.IsTrue(status.Found, $"tid {tid}, key {ii}, {status}, wasPending {wasPending}, pt 1");

                        Assert.IsNotNull(output.Memory, $"tid {tid}, key {ii}, wasPending {wasPending}, pt 2");
                        Assert.IsNotNull(output.Memory.Memory, $"tid {tid}, key {ii}, wasPending {wasPending}, pt 3");
                        long value = BitConverter.ToInt64(output.Memory.Memory.Span);
                        Assert.AreEqual(ii, value % valueAdd, $"tid {tid}, key {ii}, wasPending {wasPending}, pt 4");

                        output.Memory.Dispose();
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                using var session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new RmwSpanByteFunctions());

                Span<byte> keyVec = stackalloc byte[sizeof(long)];
                var key = SpanByte.FromFixedSpan(keyVec);
                Span<byte> inputVec = stackalloc byte[sizeof(long)];
                var input = SpanByte.FromFixedSpan(inputVec);

                Random rng = new(tid * 101);
                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = 0; ii < numKeys; ++ii)
                    {
                        SpanByteAndMemory output = default;

                        Assert.IsTrue(BitConverter.TryWriteBytes(keyVec, ii));
                        Assert.IsTrue(BitConverter.TryWriteBytes(inputVec, ii + valueAdd));
                        var status = updateOp == UpdateOp.RMW
                                        ? session.RMW(ref key, ref input, ref output)
                                        : session.Upsert(ref key, ref input, ref input, ref output);
                        bool wasPending = status.IsPending;
                        if (wasPending)
                        {
                            Assert.AreNotEqual(UpdateOp.Upsert, updateOp, "Upsert should not go pending");
                            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            (status, output) = GetSinglePendingResult(completedOutputs);

                            // Record may have been updated in-place if a CTT was done during the pending operation.
                            // Assert.IsTrue(status.Record.CopyUpdated, $"Expected Record.CopyUpdated but was: {status}");
                        }
                        if (updateOp == UpdateOp.RMW)   // Upsert will not try to find records below HeadAddress, but it may find them in-memory
                            Assert.IsTrue(status.Found, $"tid {tid}, key {ii}, {status}");

                        long value = BitConverter.ToInt64(output.Memory.Memory.Span);
                        Assert.AreEqual(ii + valueAdd, value, $"tid {tid}, key {ii}, wasPending {wasPending}");

                        output.Memory.Dispose();
                    }
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 1; t <= numReadThreads + numWriteThreads; t++)
            {
                var tid = t;
                if (t <= numReadThreads)
                    tasks.Add(Task.Factory.StartNew(() => runReadThread(tid)));
                else
                    tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}