// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.test.LockTable;
using Tsavorite.test.TransactionalUnsafeContext;
using static Tsavorite.core.LogAddress;
using static Tsavorite.test.TestUtils;

#pragma warning disable  // Add parentheses for clarity

namespace Tsavorite.test.ReadCacheTests
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparerModulo, SpanByteRecordDisposer>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparerModulo, SpanByteRecordDisposer>;
    using SpanByteStoreFunctions = StoreFunctions<SpanByteKeyComparerModulo, SpanByteRecordDisposer>;

    internal static class RcTestGlobals
    {
        internal const int PendingMod = 16;
    }

    [AllureNUnit]
    [TestFixture]
    class ChainTests : AllureTestBase
    {
        private TsavoriteKV<LongStoreFunctions, LongAllocator> store;
        private IDevice log;
        private LongKeyComparerModulo comparer;

        const long LowChainKey = 40;
        const long MidChainKey = LowChainKey + ChainLen * (HashMod / 2);
        const long HighChainKey = LowChainKey + ChainLen * (HashMod - 1);
        const long HashMod = 10;
        const int ChainLen = 10;
        const int ValueAdd = 1_000_000;

        // -1 so highChainKey is first in the chain.
        const long NumKeys = HighChainKey + HashMod - 1;

        // Insert into chain.
        const long SpliceInNewKey = HighChainKey + HashMod * 2;
        const long SpliceInExistingKey = HighChainKey - HashMod;
        const long ImmutableSplitKey = NumKeys / 2;

        // This is the record after the first readcache record we insert; it lets us limit the range to ReadCacheEvict
        // so we get outsplicing rather than successively overwriting the hash table entry on ReadCacheEvict.
        long readCacheBelowMidChainKeyEvictionAddress;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "NativeReadCacheTests.log"), deleteOnClose: true);

            comparer = new LongKeyComparerModulo(HashMod);
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 10,
                ReadCacheMemorySize = 1L << 15,
                ReadCachePageSize = 1L << 9,
                ReadCacheEnabled = true
            }, StoreFunctions.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
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
            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            long keyVal = 0, valueVal = 0;
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyVal), value = SpanByte.FromPinnedVariable(ref valueVal);

            if (recordRegion != RecordRegion.Immutable)
            {
                for (int keyNum = 0; keyNum < NumKeys; keyNum++)
                    _ = bContext.Upsert(key.SetSlice(keyNum), value.SetSlice(keyNum + ValueAdd));
                _ = bContext.CompletePending(true);
                if (recordRegion == RecordRegion.OnDisk)
                    store.Log.FlushAndEvict(true);
                return;
            }

            // Two parts, so we can have some evicted (and bring them into the readcache), and some in immutable (readonly).
            for (int keyNum = 0; keyNum < ImmutableSplitKey; keyNum++)
                _ = bContext.Upsert(key.SetSlice(keyNum), value.SetSlice(keyNum + ValueAdd));
            _ = bContext.CompletePending(true);
            store.Log.FlushAndEvict(true);

            for (long keyNum = ImmutableSplitKey; keyNum < NumKeys; keyNum++)
                _ = bContext.Upsert(key.SetSlice(keyNum), value.SetSlice(keyNum + ValueAdd));
            _ = bContext.CompletePending(true);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
        }

        void CreateChain(RecordRegion recordRegion = RecordRegion.OnDisk)
        {
            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            long output = -1;
            bool expectPending(long key) => recordRegion == RecordRegion.OnDisk || (recordRegion == RecordRegion.Immutable && key < ImmutableSplitKey);

            long keyVal = 0, valueVal = 0;
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyVal), value = SpanByte.FromPinnedVariable(ref valueVal);

            // Pass1: PENDING reads and populate the cache
            for (long ii = 0; ii < ChainLen; ++ii)
            {
                var keyNum = LowChainKey + ii * HashMod;
                key.Set((long)keyNum);
                var status = bContext.Read(key, ref output);
                if (expectPending(keyNum))
                {
                    ClassicAssert.IsTrue(status.IsPending, status.ToString());
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                    ClassicAssert.IsTrue(status.Record.CopiedToReadCache, status.ToString());
                }
                ClassicAssert.IsTrue(status.Found, status.ToString());
                if (keyNum < MidChainKey)
                    readCacheBelowMidChainKeyEvictionAddress = store.ReadCache.TailAddress;
            }

            // Pass2: non-PENDING reads from the cache
            for (var ii = 0; ii < ChainLen; ++ii)
            {
                var status = bContext.Read(key.Set((long)LowChainKey + ii * HashMod), ref output);
                ClassicAssert.IsTrue(!status.IsPending && status.Found, status.ToString());
            }

            // Pass 3: Put in bunch of extra keys into the cache so when we FlushAndEvict we get all the ones of interest.
            for (var keyNum = 0; keyNum < NumKeys; ++keyNum)
            {
                if ((keyNum % HashMod) != 0)
                {
                    key.Set((long)keyNum);
                    var status = bContext.Read(key, ref output);
                    if (expectPending(keyNum))
                    {
                        ClassicAssert.IsTrue(status.IsPending);
                        _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                        (status, output) = GetSinglePendingResult(outputs);
                        ClassicAssert.IsTrue(status.Record.CopiedToReadCache, status.ToString());
                    }
                    ClassicAssert.IsTrue(status.Found, status.ToString());
                    _ = bContext.CompletePending(wait: true);
                }
            }
        }

        unsafe bool GetRecordInInMemoryHashChain(long keyNum, out bool isReadCache)
        {
            // returns whether the key was found before we'd go pending
            var (la, pa) = GetHashChain(store, SpanByte.FromPinnedVariable(ref keyNum), out var recordKey, out bool invalid, out isReadCache);
            while (isReadCache || la >= store.hlogBase.HeadAddress)
            {
                if (recordKey.ReadOnlySpan.AsRef<long>() == keyNum && !invalid)
                    return true;
                (la, pa) = NextInChain(store, pa, out recordKey, out invalid, ref isReadCache);
            }
            return false;
        }

        internal bool FindRecordInReadCache(ReadOnlySpan<byte> key, out bool invalid, out long logicalAddress, out long physicalAddress)
        {
            // returns whether the key was found before we'd go pending
            (logicalAddress, physicalAddress) = GetHashChain(store, key, out var recordKey, out invalid, out bool isReadCache);
            while (isReadCache)
            {
                if (recordKey.ReadOnlySpan.AsRef<long>() == key.AsRef<long>())
                    return true;
                (logicalAddress, physicalAddress) = NextInChain(store, physicalAddress, out recordKey, out invalid, ref isReadCache);
            }
            return false;
        }

        internal static (long logicalAddress, long physicalAddress) GetHashChain<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, ReadOnlySpan<byte> key, out PinnedSpanByte recordKey, out bool invalid, out bool isReadCache)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var tagExists = store.FindHashBucketEntryForKey(key, out var entry);
            ClassicAssert.IsTrue(tagExists);

            isReadCache = entry.IsReadCache;
            var log = isReadCache ? store.readcacheBase : store.hlogBase;
            var pa = log.GetPhysicalAddress(entry.Address);
            recordKey = PinnedSpanByte.FromPinnedSpan(LogRecord.GetInlineKey(pa));  // Must return PinnedSpanByte to avoid scope issues with ReadOnlySpan
            invalid = LogRecord.GetInfo(pa).Invalid;

            return (entry.Address, pa);
        }

        (long logicalAddress, long physicalAddress) NextInChain(long physicalAddress, out PinnedSpanByte recordKey, out bool invalid, ref bool isReadCache)
            => NextInChain(store, physicalAddress, out recordKey, out invalid, ref isReadCache);

        internal static (long logicalAddress, long physicalAddress) NextInChain<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, long physicalAddress, out PinnedSpanByte recordKey, out bool invalid, ref bool isReadCache)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var log = isReadCache ? store.readcacheBase : store.hlogBase;
            var info = LogRecord.GetInfo(physicalAddress);
            var la = info.PreviousAddress;

            isReadCache = IsReadCache(la);
            log = isReadCache ? store.readcacheBase : store.hlogBase;
            var pa = log.GetPhysicalAddress(la);
            recordKey = PinnedSpanByte.FromPinnedSpan(LogRecord.GetInlineKey(pa));  // Must return PinnedSpanByte to avoid scope issues with ReadOnlySpan
            invalid = LogRecord.GetInfo(pa).Invalid;
            return (la, pa);
        }

        (long logicalAddress, long physicalAddress) ScanReadCacheChain(long[] omitted = null, bool evicted = false, bool deleted = false)
        {
            omitted ??= [];

            long keyVal = 0, valueVal = 0;
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyVal), value = SpanByte.FromPinnedVariable(ref valueVal);

            var (la, pa) = GetHashChain(store, key.Set((long)LowChainKey), out var actualKey, out bool invalid, out bool isReadCache);
            for (long expectedKey = HighChainKey; expectedKey >= LowChainKey; expectedKey -= HashMod)
            {
                // We evict from readcache only to just below midChainKey
                if (!evicted || expectedKey >= MidChainKey)
                    ClassicAssert.IsTrue(isReadCache);

                if (isReadCache)
                {
                    ClassicAssert.AreEqual(expectedKey, actualKey.ReadOnlySpan.AsRef<long>());
                    if (omitted.Contains(expectedKey))
                        ClassicAssert.IsTrue(invalid);
                }
                else if (omitted.Contains(actualKey.ReadOnlySpan.AsRef<long>()))
                {
                    ClassicAssert.AreEqual(deleted, LogRecord.GetInfo(pa).Tombstone);
                }

                (la, pa) = NextInChain(pa, out actualKey, out invalid, ref isReadCache);
                if (!isReadCache && la < store.hlogBase.HeadAddress)
                    break;
            }
            ClassicAssert.IsFalse(isReadCache);
            return (la, pa);
        }

        (long logicalAddress, long physicalAddress) SkipReadCacheChain(ReadOnlySpan<byte> key)
            => SkipReadCacheChain(store, key);

        internal static (long logicalAddress, long physicalAddress) SkipReadCacheChain<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, ReadOnlySpan<byte> key)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var (la, pa) = GetHashChain(store, key, out _, out _, out bool isReadCache);
            while (isReadCache)
                (la, pa) = NextInChain(store, pa, out _, out _, ref isReadCache);
            return (la, pa);
        }

        void VerifySplicedInKey(ReadOnlySpan<byte> expectedKey)
        {
            // Scan to the end of the readcache chain and verify we inserted the value.
            var (_, pa) = SkipReadCacheChain(expectedKey);
            var storedKey = LogRecord.GetInlineKey(pa);
            ClassicAssert.AreEqual(expectedKey.AsRef<long>(), storedKey.AsRef<long>());
        }

        static void ClearCountsOnError(ClientSession<long, long, Empty, SimpleLongSimpleFunctions, LongStoreFunctions, LongAllocator> luContext)
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
            _ = ScanReadCacheChain();
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteCacheRecordTest()
        {
            PopulateAndEvict();
            CreateChain();
            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            void doTest(long keyNum)
            {
                long keyVal = 0, valueVal = 0;
                Span<byte> key = SpanByte.FromPinnedVariable(ref keyVal), value = SpanByte.FromPinnedVariable(ref valueVal);

                key.Set((long)keyNum);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                status = bContext.Read(key, ref valueVal);
                ClassicAssert.IsFalse(status.Found, status.ToString());
            }

            doTest(LowChainKey);
            doTest(HighChainKey);
            doTest(MidChainKey);
            _ = ScanReadCacheChain([LowChainKey, MidChainKey, HighChainKey], evicted: false);

            store.ReadCacheEvict(store.ReadCache.BeginAddress, readCacheBelowMidChainKeyEvictionAddress);
            _ = ScanReadCacheChain([LowChainKey, MidChainKey, HighChainKey], evicted: true, deleted: true);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteHalfOfAllReadCacheRecordsTest()
        {
            PopulateAndEvict();
            CreateChain();
            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            void doTest(long keyNum)
            {
                long keyVal = 0, valueVal = 0;
                Span<byte> key = SpanByte.FromPinnedVariable(ref keyVal), value = SpanByte.FromPinnedVariable(ref valueVal);

                key.Set((long)keyNum);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());

                status = bContext.Read(key, ref valueVal);
                ClassicAssert.IsFalse(status.Found, status.ToString());
            }

            // Should be found in the readcache before deletion
            ClassicAssert.IsTrue(GetRecordInInMemoryHashChain(LowChainKey, out bool isReadCache));
            ClassicAssert.IsTrue(isReadCache);
            ClassicAssert.IsTrue(GetRecordInInMemoryHashChain(MidChainKey, out isReadCache));
            ClassicAssert.IsTrue(isReadCache);
            ClassicAssert.IsTrue(GetRecordInInMemoryHashChain(HighChainKey, out isReadCache));
            ClassicAssert.IsTrue(isReadCache);

            // Delete all keys in the readcache chain below midChainKey.
            for (var ii = LowChainKey; ii < MidChainKey; ++ii)
                doTest(ii);

            // LowChainKey should not be found in the readcache after deletion to just below midChainKey, but mid- and highChainKey should not be affected.
            ClassicAssert.IsTrue(GetRecordInInMemoryHashChain(LowChainKey, out isReadCache));
            ClassicAssert.IsFalse(isReadCache);
            ClassicAssert.IsTrue(GetRecordInInMemoryHashChain(MidChainKey, out isReadCache));
            ClassicAssert.IsTrue(isReadCache);
            ClassicAssert.IsTrue(GetRecordInInMemoryHashChain(HighChainKey, out isReadCache));
            ClassicAssert.IsTrue(isReadCache);

            store.ReadCacheEvict(store.ReadCache.BeginAddress, readCacheBelowMidChainKeyEvictionAddress);

            // Following deletion to just below midChainKey:
            //  lowChainKey's tombstone should still be found in the mutable portion of the log
            //  midChainKey and highChainKey should be found in the readcache
            ClassicAssert.IsTrue(GetRecordInInMemoryHashChain(LowChainKey, out isReadCache));
            ClassicAssert.IsFalse(isReadCache);
            ClassicAssert.IsTrue(GetRecordInInMemoryHashChain(MidChainKey, out isReadCache));
            ClassicAssert.IsTrue(isReadCache);
            ClassicAssert.IsTrue(GetRecordInInMemoryHashChain(HighChainKey, out isReadCache));
            ClassicAssert.IsTrue(isReadCache);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void UpsertCacheRecordTest()
        {
            DoUpdateTest(useRMW: false);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void RMWCacheRecordTest()
        {
            DoUpdateTest(useRMW: true);
        }

        void DoUpdateTest(bool useRMW)
        {
            PopulateAndEvict();
            CreateChain();
            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            void doTest(long keyNum)
            {
                long keyVal = 0, valueVal = 0;
                Span<byte> key = SpanByte.FromPinnedVariable(ref keyVal), value = SpanByte.FromPinnedVariable(ref valueVal);

                key.Set((long)keyNum);
                var status = bContext.Read(key, ref valueVal);
                ClassicAssert.IsTrue(status.Found, status.ToString());

                long input = valueVal + ValueAdd;
                if (useRMW)
                {
                    // RMW will use the readcache entry for its source and then invalidate it.
                    status = bContext.RMW(key, ref input);
                    ClassicAssert.IsTrue(status.Found && status.Record.CopyUpdated, status.ToString());

                    ClassicAssert.IsTrue(FindRecordInReadCache(key, out bool invalid, out _, out _));
                    ClassicAssert.IsTrue(invalid);
                }
                else
                {
                    status = bContext.Upsert(key, value.Set(input));
                    ClassicAssert.IsTrue(status.Record.Created, status.ToString());
                }

                status = bContext.Read(key, ref valueVal);
                ClassicAssert.IsTrue(status.Found, status.ToString());
                ClassicAssert.AreEqual(keyNum + ValueAdd * 2, valueVal);
            }

            doTest(LowChainKey);
            doTest(HighChainKey);
            doTest(MidChainKey);
            _ = ScanReadCacheChain([LowChainKey, MidChainKey, HighChainKey], evicted: false);

            store.ReadCacheEvict(store.ReadCache.BeginAddress, readCacheBelowMidChainKeyEvictionAddress);
            _ = ScanReadCacheChain([LowChainKey, MidChainKey, HighChainKey], evicted: true);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpliceInFromCTTTest()
        {
            PopulateAndEvict();
            CreateChain();

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            long input = 0, output = 0, keyNum = LowChainKey - HashMod; // key must be in evicted region for this test
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyNum);
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };

            var status = bContext.Read(key, ref input, ref output, ref readOptions, out _);
            ClassicAssert.IsTrue(status.IsPending, status.ToString());
            _ = bContext.CompletePending(wait: true);

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpliceInFromUpsertTest([Values] RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion);
            CreateChain(recordRegion);

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;

            long keyNum = -1, valueNum = 0;
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);

            if (recordRegion is RecordRegion.Immutable or RecordRegion.OnDisk)
            {
                keyNum = SpliceInExistingKey;
                var status = bContext.Upsert(key, value.Set(keyNum + ValueAdd));
                ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }
            else
            {
                keyNum = SpliceInNewKey;
                var status = bContext.Upsert(key, value.Set(keyNum + ValueAdd));
                ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpliceInFromRMWTest([Values] RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion);
            CreateChain(recordRegion);

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;
            long keyNum = -1, valueNum = 0, output = -1;

            Span<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);

            long input = keyNum + ValueAdd;
            if (recordRegion is RecordRegion.Immutable or RecordRegion.OnDisk)
            {
                // Existing key
                keyNum = SpliceInExistingKey;
                var status = bContext.RMW(key, ref input);

                // If OnDisk, this used the readcache entry for its source and then invalidated it.
                ClassicAssert.IsTrue(status.Found && status.Record.CopyUpdated, status.ToString());
                if (recordRegion == RecordRegion.OnDisk)
                {
                    ClassicAssert.IsTrue(FindRecordInReadCache(key, out bool invalid, out _, out _));
                    ClassicAssert.IsTrue(invalid);
                }

                { // New key
                    keyNum = SpliceInNewKey;
                    status = bContext.RMW(key, ref input);

                    // This NOTFOUND key will return PENDING because we have to trace back through the collisions.
                    ClassicAssert.IsTrue(status.IsPending, status.ToString());
                    _ = bContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                    ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                }
            }
            else
            {
                keyNum = SpliceInNewKey;
                var status = bContext.RMW(key, ref input);
                ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void SpliceInFromDeleteTest([Values] RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion);
            CreateChain(recordRegion);

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var bContext = session.BasicContext;
            long keyNum = -1;
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyNum);

            if (recordRegion is RecordRegion.Immutable or RecordRegion.OnDisk)
            {
                keyNum = SpliceInExistingKey;
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }
            else
            {
                keyNum = SpliceInNewKey;
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
            }

            VerifySplicedInKey(key);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyLockCountsAfterReadCacheEvict()
        {
            PopulateAndEvict();
            CreateChain();

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;

            var keyNums = GC.AllocateArray<long>(3, pinned: true);
            keyNums[0] = LowChainKey;
            keyNums[1] = MidChainKey;
            keyNums[2] = HighChainKey;

            var keys = new[]
            {
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref keyNums[0]), LockType.Exclusive, luContext),
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref keyNums[1]), LockType.Shared, luContext),
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref keyNums[2]), LockType.Exclusive, luContext)
            };

            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            try
            {
                luContext.SortKeyHashes<FixedLengthTransactionalKeyStruct>(keys);

                // For this single-threaded test, the locking does not really have to be in order, but for consistency do it.
                luContext.Lock<FixedLengthTransactionalKeyStruct>(keys);

                store.ReadCache.FlushAndEvict(wait: true);

                int xlocks = 0, slocks = 0;
                foreach (var idx in TransactionalUnsafeContextTests.EnumActionKeyIndices(keys, TransactionalUnsafeContextTests.LockOperationType.Unlock))
                {
                    if (keys[idx].LockType == LockType.Exclusive)
                        ++xlocks;
                    else
                        ++slocks;
                }
                AssertTotalLockCounts(xlocks, slocks);

                foreach (var idx in TransactionalUnsafeContextTests.EnumActionKeyIndices(keys, TransactionalUnsafeContextTests.LockOperationType.Unlock))
                {
                    ref var key = ref keys[idx];
                    HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(key.Key.ReadOnlySpan));
                    OverflowBucketLockTableTests.PopulateHei(store, ref hei);

                    var lockState = store.LockTable.GetLockState(ref hei);
                    ClassicAssert.IsTrue(lockState.IsFound);
                    ClassicAssert.AreEqual(key.LockType == LockType.Exclusive, lockState.IsLockedExclusive);
                    ClassicAssert.AreEqual(key.LockType != LockType.Exclusive, lockState.NumLockedShared > 0);

                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(keys.AsSpan().Slice(idx, 1));
                    lockState = store.LockTable.GetLockState(ref hei);
                    ClassicAssert.IsFalse(lockState.IsLockedExclusive);
                    ClassicAssert.AreEqual(0, lockState.NumLockedShared);
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
                luContext.EndTransaction();
                luContext.EndUnsafe();
            }

            AssertTotalLockCounts(0, 0);
        }
    }

    [AllureNUnit]
    [TestFixture]
    class LongStressChainTests : AllureTestBase
    {
        private TsavoriteKV<LongStoreFunctions, LongAllocator> store;
        private IDevice log;
        private LongKeyComparerModulo comparer;
        const long ValueAdd = 1_000_000_000;

        const long NumKeys = 2_000;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            string filename = Path.Join(MethodTestDir, $"{GetType().Name}.log");
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is TestDeviceType deviceType)
                {
                    log = CreateTestDevice(deviceType, filename, deleteOnClose: true);
                    continue;
                }
            }
            log ??= Devices.CreateLogDevice(filename, deleteOnClose: true);

            HashModulo modRange = HashModulo.NoMod;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo cr)
                {
                    modRange = cr;
                    continue;
                }
            }

            comparer = new LongKeyComparerModulo((long)modRange);

            // Make the main log small enough that we force the readcache
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 10,
                ReadCacheMemorySize = 1L << 15,
                ReadCachePageSize = 1L << 9,
                ReadCacheEnabled = true
            }, StoreFunctions.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
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

        internal class RmwLongFunctions : SimpleLongSimpleFunctions
        {
            /// <inheritdoc/>
            public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
            {
                Assert.Fail("For these tests, InitialUpdater should never be called");
                return false;
            }
        }

        unsafe void PopulateAndEvict()
        {
            using var session = store.NewSession<long, long, Empty, RmwLongFunctions>(new RmwLongFunctions());
            var bContext = session.BasicContext;

            for (long ii = 0; ii < NumKeys; ii++)
            {
                long key = ii;
                var status = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref key));
                ClassicAssert.IsFalse(status.IsPending);
                ClassicAssert.IsTrue(status.Record.Created, $"key {key}, status {status}");
            }
            _ = bContext.CompletePending(true);
            store.Log.FlushAndEvict(true);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(StressTestCategory)]
        //[Repeat(10000)]
#pragma warning disable IDE0060 // Remove unused parameter (modRange is used by Setup())
        public void LongRcMultiThreadTest([Values] HashModulo modRange, [Values(0, 1, 2, 8)] int numReadThreads, [Values(0, 1, 2, 8)] int numWriteThreads,
                                          [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            if (numReadThreads == 0 && numWriteThreads == 0)
                Assert.Ignore("Skipped due to 0 threads for both read and update");
            if ((numReadThreads > 2 || numWriteThreads > 2) && IsRunningAzureTests)
                Assert.Ignore("Skipped because > 2 threads when IsRunningAzureTests");
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1}, name = {TestContext.CurrentContext.Test.Name} ***");

            PopulateAndEvict();

            const int numIterations = 1;
            unsafe void runReadThread(int tid)
            {
                using var session = store.NewSession<long, long, Empty, RmwLongFunctions>(new RmwLongFunctions());
                var bContext = session.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    var numCompleted = 0;
                    for (var ii = 0; ii < NumKeys; ++ii)
                    {
                        long key = ii, output = 0;
                        var status = bContext.Read(SpanByte.FromPinnedVariable(ref key), ref output);

                        var numPending = ii - numCompleted;
                        if (status.IsPending)
                            ++numPending;
                        else
                        {
                            ++numCompleted;
                            ClassicAssert.IsTrue(status.Found, $"key {key}, status {status}, wasPending {false}");
                            ClassicAssert.AreEqual(ii, output % ValueAdd);
                        }

                        if (numPending > 0 && ((numPending % RcTestGlobals.PendingMod == 0) || ii == NumKeys - 1))
                        {
                            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            using (completedOutputs)
                            {
                                while (completedOutputs.Next())
                                {
                                    ++numCompleted;

                                    status = completedOutputs.Current.Status;
                                    output = completedOutputs.Current.Output;
                                    key = completedOutputs.Current.Key.AsRef<long>();

                                    ClassicAssert.IsTrue(status.Found, $"key {key}, status {status}, wasPending {true}");
                                    ClassicAssert.AreEqual(key, output % ValueAdd);
                                }
                            }
                        }
                    }
                    ClassicAssert.AreEqual(NumKeys, numCompleted, "numCompleted");
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                using var session = store.NewSession<long, long, Empty, RmwLongFunctions>(new RmwLongFunctions());
                var bContext = session.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    var numCompleted = 0;
                    for (var ii = 0; ii < NumKeys; ++ii)
                    {
                        long key = ii, input = ii + ValueAdd * tid, output = 0;
                        var status = updateOp == UpdateOp.RMW
                                        ? bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref input, ref output)
                                        : bContext.Upsert(SpanByte.FromPinnedVariable(ref key), ref input, SpanByte.FromPinnedVariable(ref input), ref output);

                        var numPending = ii - numCompleted;
                        if (status.IsPending)
                        {
                            ClassicAssert.AreNotEqual(UpdateOp.Upsert, updateOp, "Upsert should not go pending");
                            ++numPending;
                        }
                        else
                        {
                            ++numCompleted;
                            if (updateOp == UpdateOp.RMW)   // Upsert will not try to find records below HeadAddress, but it may find them in-memory
                                ClassicAssert.IsTrue(status.Found, $"key {key}, status {status}, wasPending {false}");
                            ClassicAssert.AreEqual(ii + ValueAdd * tid, output);
                        }

                        if (numPending > 0 && ((numPending % RcTestGlobals.PendingMod == 0) || ii == NumKeys - 1))
                        {
                            _ = bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            using (completedOutputs)
                            {
                                while (completedOutputs.Next())
                                {
                                    ++numCompleted;
                                    if (updateOp == UpdateOp.RMW)   // Upsert will not try to find records below HeadAddress, but it may find them in-memory
                                        ClassicAssert.IsTrue(completedOutputs.Current.Status.Found, $"key {completedOutputs.Current.Key.ToShortString()}, status {completedOutputs.Current.Status}, wasPending {true}");
                                    ClassicAssert.AreEqual(completedOutputs.Current.Key.AsRef<long>() + ValueAdd * tid, completedOutputs.Current.Output);
                                }
                            }
                        }
                    }

                    ClassicAssert.AreEqual(NumKeys, numCompleted, "numCompleted");
                }
            }

            List<Task> tasks = [];   // Task rather than Thread for propagation of exceptions.
            for (int t = 1; t <= numReadThreads + numWriteThreads; t++)
            {
                var tid = t;
                if (t <= numReadThreads)
                    tasks.Add(Task.Factory.StartNew(() => runReadThread(tid)));
                else
                    tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll([.. tasks]);
        }
    }

    [AllureNUnit]
    [TestFixture]
    class SpanByteStressChainTests : AllureTestBase
    {
        private TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        private IDevice log;
        SpanByteKeyComparerModulo comparer;

        const long ValueAdd = 1_000_000_000;

        const long NumKeys = 2_000;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            string filename = Path.Join(MethodTestDir, $"{GetType().Name}.log");
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is TestDeviceType deviceType)
                {
                    log = CreateTestDevice(deviceType, filename, deleteOnClose: true);
                    continue;
                }
            }
            log ??= Devices.CreateLogDevice(filename, deleteOnClose: true);

            HashModulo modRange = HashModulo.NoMod;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is HashModulo cr)
                {
                    modRange = cr;
                    continue;
                }
            }

            comparer = new SpanByteKeyComparerModulo(modRange);

            // Make the main log small enough that we force the readcache
            store = new(new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 10,
                ReadCacheMemorySize = 1L << 15,
                ReadCachePageSize = 1L << 9,
                ReadCacheEnabled = true
            }, StoreFunctions.Create(comparer, SpanByteRecordDisposer.Instance)
            , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
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
            public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                if (!base.InPlaceWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo))
                    return false;
                srcValue.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                if (!base.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo))
                    return false;
                srcValue.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(input, in sizeInfo))
                    return false;
                input.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                if (!logRecord.TrySetValueSpanAndPrepareOptionals(input, in sizeInfo))
                    return false;
                input.CopyTo(ref output, memoryPool);
                return true;
            }

            /// <inheritdoc/>
            public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                Assert.Fail("For these tests, InitialUpdater should never be called");
                return false;
            }
        }

        unsafe void PopulateAndEvict()
        {
            using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
            var bContext = session.BasicContext;

            Span<byte> key = stackalloc byte[sizeof(long)];

            for (long ii = 0; ii < NumKeys; ii++)
            {
                ClassicAssert.IsTrue(BitConverter.TryWriteBytes(key, ii));
                var status = bContext.Upsert(key, key);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
            bContext.CompletePending(true);
            store.Log.FlushAndEvict(true);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(StressTestCategory)]
        //[Repeat(10000)]
        public void SpanByteRcMultiThreadTest([Values] HashModulo modRange, [Values(0, 1, 2, 8)] int numReadThreads, [Values(0, 1, 2, 8)] int numWriteThreads,
                                              [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
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
                using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
                var bContext = session.BasicContext;

                Span<byte> key = stackalloc byte[sizeof(long)];

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    var numCompleted = 0;
                    for (var ii = 0; ii < NumKeys; ++ii)
                    {
                        SpanByteAndMemory output = default;

                        ClassicAssert.IsTrue(BitConverter.TryWriteBytes(key, ii));
                        var status = bContext.Read(key, ref output);

                        var numPending = ii - numCompleted;
                        if (status.IsPending)
                            ++numPending;
                        else
                        {
                            ++numCompleted;

                            ClassicAssert.IsTrue(status.Found, $"tid {tid}, key {ii}, {status}, wasPending {false}, pt 1");
                            ClassicAssert.IsNotNull(output.Memory, $"tid {tid}, key {ii}, wasPending {false}, pt 2");
                            long value = BitConverter.ToInt64(output.ReadOnlySpan);
                            ClassicAssert.AreEqual(ii, value % ValueAdd, $"tid {tid}, key {ii}, wasPending {false}, pt 3");
                            output.Memory.Dispose();
                        }

                        if (numPending > 0 && ((numPending % RcTestGlobals.PendingMod == 0) || ii == NumKeys - 1))
                        {
                            bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            using (completedOutputs)
                            {
                                while (completedOutputs.Next())
                                {
                                    ++numCompleted;

                                    status = completedOutputs.Current.Status;
                                    output = completedOutputs.Current.Output;
                                    // Note: do NOT overwrite 'key' here
                                    long keyLong = BitConverter.ToInt64(completedOutputs.Current.Key);

                                    ClassicAssert.IsTrue(status.Found, $"pending: tid {tid}, key {keyLong}, {status}, wasPending {true}, pt 1");
                                    ClassicAssert.IsNotNull(output.Memory, $"pending: tid {tid}, key {keyLong}, wasPending {true}, pt 2");
                                    long value = BitConverter.ToInt64(output.ReadOnlySpan);
                                    ClassicAssert.AreEqual(keyLong, value % ValueAdd, $"pending: tid {tid}, key {keyLong}, wasPending {true}, pt 3");
                                    output.Memory.Dispose();
                                }
                            }
                        }
                    }
                    ClassicAssert.AreEqual(NumKeys, numCompleted, "numCompleted");
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                using var session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new RmwSpanByteFunctions());
                var bContext = session.BasicContext;

                Span<byte> key = stackalloc byte[sizeof(long)];
                Span<byte> input = stackalloc byte[sizeof(long)];
                var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    var numCompleted = 0;
                    for (var ii = 0; ii < NumKeys; ++ii)
                    {
                        SpanByteAndMemory output = default;

                        ClassicAssert.IsTrue(BitConverter.TryWriteBytes(key, ii));
                        ClassicAssert.IsTrue(BitConverter.TryWriteBytes(input, ii + ValueAdd));
                        var status = updateOp == UpdateOp.RMW
                                        ? bContext.RMW(key, ref pinnedInputSpan, ref output)
                                        : bContext.Upsert(key, ref pinnedInputSpan, input, ref output);

                        var numPending = ii - numCompleted;
                        if (status.IsPending)
                        {
                            ClassicAssert.AreNotEqual(UpdateOp.Upsert, updateOp, "Upsert should not go pending");
                            ++numPending;
                        }
                        else
                        {
                            ++numCompleted;
                            if (updateOp == UpdateOp.RMW)   // Upsert will not try to find records below HeadAddress, but it may find them in-memory
                                ClassicAssert.IsTrue(status.Found, $"tid {tid}, key {ii}, {status}");

                            long value = BitConverter.ToInt64(output.ReadOnlySpan);
                            ClassicAssert.AreEqual(ii + ValueAdd, value, $"tid {tid}, key {ii}, wasPending {false}");

                            output.Dispose();
                        }

                        if (numPending > 0 && ((numPending % RcTestGlobals.PendingMod == 0) || ii == NumKeys - 1))
                        {
                            bContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                            using (completedOutputs)
                            {
                                while (completedOutputs.Next())
                                {
                                    ++numCompleted;

                                    status = completedOutputs.Current.Status;
                                    output = completedOutputs.Current.Output;
                                    // Note: do NOT overwrite 'key' here
                                    long keyLong = BitConverter.ToInt64(completedOutputs.Current.Key);

                                    if (updateOp == UpdateOp.RMW)   // Upsert will not try to find records below HeadAddress, but it may find them in-memory
                                        ClassicAssert.IsTrue(status.Found, $"tid {tid}, key {keyLong}, {status}");

                                    long value = BitConverter.ToInt64(output.ReadOnlySpan);
                                    ClassicAssert.AreEqual(keyLong + ValueAdd, value, $"tid {tid}, key {keyLong}, wasPending {true}");

                                    output.Dispose();
                                }
                            }
                        }
                    }
                    ClassicAssert.AreEqual(NumKeys, numCompleted, "numCompleted");
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