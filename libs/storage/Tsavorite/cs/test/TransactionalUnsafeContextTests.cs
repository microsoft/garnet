// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using Tsavorite.test.LockTable;
using Tsavorite.test.ReadCacheTests;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.TransactionalUnsafeContext
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    internal class TransactionalUnsafeComparer : IKeyComparer
    {
        internal int maxSleepMs;
        readonly Random rng = new(101);

        public bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => k1.AsRef<long>() == k2.AsRef<long>();

        public long GetHashCode64(ReadOnlySpan<byte> k)
        {
            if (maxSleepMs > 0)
                Thread.Sleep(rng.Next(maxSleepMs));
            return Utility.GetHashCode(k.AsRef<long>());
        }
    }
}
namespace Tsavorite.test.TransactionalUnsafeContext
{
    using LongAllocator = SpanByteAllocator<StoreFunctions<TransactionalUnsafeComparer, SpanByteRecordDisposer>>;
    using LongStoreFunctions = StoreFunctions<TransactionalUnsafeComparer, SpanByteRecordDisposer>;

    // Functions for the "Simple lock transaction" case, e.g.:
    //  - Lock key1, key2, key3, keyResult
    //  - Do some operation on value1, value2, value3 and write the result to valueResult
    internal class TransactionalUnsafeFunctions : SimpleLongSimpleFunctions
    {
        internal long recordAddress;

        public override void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            recordAddress = deleteInfo.Address;
        }

        public override bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
        {
            recordAddress = deleteInfo.Address;
            return true;
        }
    }

    public enum ResultLockTarget { MutableLock, LockTable }

    internal struct BucketLockTracker
    {
        internal readonly Dictionary<long /* bucketIndex */, (int x, int s)> buckets;

        public BucketLockTracker()
        {
            buckets = [];
        }

        internal readonly void Increment(FixedLengthTransactionalKeyStruct key) => Increment(ref key); // easier with 'foreach' because iteration vars can't be passed by 'ref'
        internal readonly void Increment(ref FixedLengthTransactionalKeyStruct key)
        {
            if (key.LockType == LockType.Exclusive)
                IncrementX(ref key);
            else
                IncrementS(ref key);
        }
        internal readonly void Decrement(FixedLengthTransactionalKeyStruct key) => Decrement(ref key);
        internal readonly void Decrement(ref FixedLengthTransactionalKeyStruct key)
        {
            if (key.LockType == LockType.Exclusive)
                DecrementX(ref key);
            else
                DecrementS(ref key);
        }

        internal readonly void IncrementX(ref FixedLengthTransactionalKeyStruct key) => AddX(ref key, 1);
        internal readonly void DecrementX(ref FixedLengthTransactionalKeyStruct key) => AddX(ref key, -1);
        internal readonly void IncrementS(ref FixedLengthTransactionalKeyStruct key) => AddS(ref key, 1);
        internal readonly void DecrementS(ref FixedLengthTransactionalKeyStruct key) => AddS(ref key, -1);

        private readonly void AddX(ref FixedLengthTransactionalKeyStruct key, int addend)
        {
            if (!buckets.TryGetValue(key.KeyHash, out var counts))
                counts = default;
            counts.x += addend;
            ClassicAssert.GreaterOrEqual(counts.x, 0);
            buckets[key.KeyHash] = counts;
        }

        private readonly void AddS(ref FixedLengthTransactionalKeyStruct key, int addend)
        {
            if (!buckets.TryGetValue(key.KeyHash, out var counts))
                counts = default;
            counts.s += addend;
            ClassicAssert.GreaterOrEqual(counts.s, 0);
            buckets[key.KeyHash] = counts;
        }

        internal readonly bool GetLockCounts(ref FixedLengthTransactionalKeyStruct key, out (int x, int s) counts)
        {
            if (!buckets.TryGetValue(key.KeyHash, out counts))
            {
                counts = default;
                return false;
            }
            return true;
        }

        internal readonly (int x, int s) GetLockCounts()
        {
            var xx = 0;
            var ss = 0;
            foreach (var kvp in buckets)
            {
                xx += kvp.Value.x;
                ss += kvp.Value.s;
            }
            return (xx, ss);
        }

        internal readonly void AssertNoLocks()
        {
            foreach (var kvp in buckets)
            {
                ClassicAssert.AreEqual(0, kvp.Value.x);
                ClassicAssert.AreEqual(0, kvp.Value.s);
            }
        }
    }

    [AllureNUnit]
    [TestFixture]
    class TransactionalUnsafeContextTests : AllureTestBase
    {
        const int NumRecords = 1000;
        const int UseNewKey = 1010;
        const int UseExistingKey = 200;

        const int ValueMult = 1_000_000;

        TransactionalUnsafeFunctions functions;
        TransactionalUnsafeComparer comparer;

        private TsavoriteKV<LongStoreFunctions, LongAllocator> store;
        private ClientSession<long, long, Empty, TransactionalUnsafeFunctions, LongStoreFunctions, LongAllocator> session;
        private BasicContext<long, long, Empty, TransactionalUnsafeFunctions, LongStoreFunctions, LongAllocator> bContext;
        private IDevice log;

        [SetUp]
        public void Setup() => Setup(forRecovery: false);

        public void Setup(bool forRecovery)
        {
            if (!forRecovery)
            {
                DeleteDirectory(MethodTestDir, wait: true);
            }
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: false, recoverDevice: forRecovery);

            var kvSettings = new KVSettings()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                PageSize = 1L << 12,
                MemorySize = 1L << 22
            };

            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                    {
                        kvSettings.ReadCachePageSize = 1L << 12;
                        kvSettings.ReadCacheMemorySize = 1L << 22;
                        kvSettings.ReadCacheEnabled = true;
                    }
                    break;
                }
                if (arg is CheckpointType)
                {
                    kvSettings.CheckpointDir = MethodTestDir;
                    break;
                }
            }

            comparer = new TransactionalUnsafeComparer();
            functions = new TransactionalUnsafeFunctions();

            store = new(kvSettings
                , StoreFunctions.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            session = store.NewSession<long, long, Empty, TransactionalUnsafeFunctions>(functions);
            bContext = session.BasicContext;
        }

        [TearDown]
        public void TearDown() => TearDown(forRecovery: false);

        public void TearDown(bool forRecovery)
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;

            if (!forRecovery)
            {
                DeleteDirectory(MethodTestDir);
            }
        }

        void Populate()
        {
            for (long key = 0; key < NumRecords; key++)
            {
                var value = key * ValueMult;
                ClassicAssert.IsFalse(bContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value)).IsPending);
            }
        }

        void AssertIsLocked(FixedLengthTransactionalKeyStruct key, bool xlock, bool slock)
            => OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);
        void AssertIsLocked(ref FixedLengthTransactionalKeyStruct key, bool xlock, bool slock)
            => OverflowBucketLockTableTests.AssertLockCounts(store, ref key, xlock, slock);

        void PrepareRecordLocation(FlushMode recordLocation) => PrepareRecordLocation(store, recordLocation);

        static void PrepareRecordLocation(TsavoriteKV<LongStoreFunctions, LongAllocator> store, FlushMode recordLocation)
        {
            if (recordLocation == FlushMode.ReadOnly)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            else if (recordLocation == FlushMode.OnDisk)
                store.Log.FlushAndEvict(wait: true);
        }

        static void ClearCountsOnError(ClientSession<long, long, Empty, TransactionalUnsafeFunctions, LongStoreFunctions, LongAllocator> luContext)
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        static void ClearCountsOnError<TFunctions>(ClientSession<long, long, Empty, TFunctions, LongStoreFunctions, LongAllocator> luContext)
            where TFunctions : ISessionFunctions<long, long, Empty>
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            luContext.sharedLockCount = 0;
            luContext.exclusiveLockCount = 0;
        }

        void PopulateHei(ref HashEntryInfo hei) => OverflowBucketLockTableTests.PopulateHei(store, ref hei);

        void AssertTotalLockCounts(long expectedX, long expectedS) => OverflowBucketLockTableTests.AssertTotalLockCounts(store, expectedX, expectedS);

        unsafe void AssertTotalLockCounts(ref BucketLockTracker blt)
        {
            var (expectedX, expectedS) = blt.GetLockCounts();
            AssertTotalLockCounts(expectedX, expectedS);

            foreach (var kvp in blt.buckets)
            {
                var hashBucket = store.LockTable.GetBucket(kvp.Key);
                ClassicAssert.AreEqual(kvp.Value.s, HashBucket.NumLatchedShared(hashBucket));
                ClassicAssert.AreEqual(kvp.Value.x == 1, HashBucket.IsLatchedExclusive(hashBucket));
            }
        }

        void AssertNoLocks(ref BucketLockTracker blt)
        {
            blt.AssertNoLocks();
            AssertTotalLockCounts(0, 0);
        }

        internal void AssertBucketLockCount(ref FixedLengthTransactionalKeyStruct key, long expectedX, long expectedS) => OverflowBucketLockTableTests.AssertBucketLockCount(store, ref key, expectedX, expectedS);

        internal enum LockOperationType { Lock, Unlock }

        internal static IEnumerable<int> EnumActionKeyIndices(FixedLengthTransactionalKeyStruct[] keys, LockOperationType lockOpType)
        {
            // "Action" means the keys that will actually be locked or unlocked.
            // See comments in TransactionalContext.DoInternalLockOp. Apps shouldn't need to do this; key sorting and enumeration
            // should be a black-box to them, so this code is just for test.
            if (lockOpType == LockOperationType.Lock)
            {
                for (int ii = 0; ii < keys.Length; ++ii)
                {
                    if (ii == 0 || keys[ii].KeyHash != keys[ii - 1].KeyHash)
                        yield return ii;
                }
                yield break;
            }

            // LockOperationType.Unlock
            for (int ii = keys.Length - 1; ii >= 0; --ii)
            {
                if (ii == 0 || keys[ii].KeyHash != keys[ii - 1].KeyHash)
                    yield return ii;
            }
        }

        [Test]
        [Category(LockTestCategory), Category(LockTableTestCategory), Category(SmokeTestCategory)]
        public void ManualLockCollidingHashCodes([Values] UseSingleBucketComparer /* justToSignalSetup */ _)
        {
            // GetBucketIndex does a mask of lower bits.
            uint bucketIndex = 42;
            long genHashCode(uint uniquifier) => ((long)uniquifier << 30) | bucketIndex;

            var lContext = session.TransactionalContext;
            lContext.BeginTransaction();

            long key1 = 101L, key2 = 102L, key3 = 103L;
            var keys = new[]
            {
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref key1), genHashCode(1), LockType.Exclusive, lContext),
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref key2), genHashCode(2), LockType.Exclusive, lContext),
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref key3), genHashCode(3), LockType.Exclusive, lContext),
            };

            for (var ii = 0; ii < keys.Length; ++ii)
                ClassicAssert.AreEqual(bucketIndex, store.LockTable.GetBucketIndex(keys[ii].KeyHash), $"BucketIndex mismatch on key {ii}");

            lContext.Lock<FixedLengthTransactionalKeyStruct>(keys);
            lContext.Unlock<FixedLengthTransactionalKeyStruct>(keys);

            lContext.EndTransaction();
        }

        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public async Task TestShiftHeadAddressLUC([Values] CompletionSyncMode syncMode)
        {
            long input = 0;
            const int RandSeed = 10;
            const int RandRange = NumRecords;
            const int NumRecs = 200;

            Random rng = new(RandSeed);
            var sw = Stopwatch.StartNew();

            // Copied from UnsafeContextTests.
            var luContext = session.TransactionalUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            var keyVec = new FixedLengthTransactionalKeyStruct[1];

            try
            {
                for (int c = 0; c < NumRecs; c++)
                {
                    long rand = rng.Next(RandRange);
                    keyVec[0] = new(SpanByte.FromPinnedVariable(ref rand), LockType.Exclusive, luContext);
                    luContext.Lock<FixedLengthTransactionalKeyStruct>(keyVec);
                    AssertBucketLockCount(ref keyVec[0], 1, 0);

                    var value = keyVec[0].Key.ReadOnlySpan.AsRef<long>() + NumRecords;
                    _ = luContext.Upsert(keyVec[0].Key.ReadOnlySpan, SpanByte.FromPinnedVariable(ref value), Empty.Default);
                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);
                    AssertBucketLockCount(ref keyVec[0], 0, 0);
                }

                AssertTotalLockCounts(0, 0);

                rng = new Random(RandSeed);
                sw.Restart();

                for (int c = 0; c < NumRecs; c++)
                {
                    long rand = rng.Next(RandRange);
                    keyVec[0] = new(SpanByte.FromPinnedVariable(ref rand), LockType.Shared, luContext);
                    var value = keyVec[0].Key.ReadOnlySpan.AsRef<long>() + NumRecords;
                    long output = 0;

                    luContext.Lock<FixedLengthTransactionalKeyStruct>(keyVec);
                    AssertBucketLockCount(ref keyVec[0], 0, 1);
                    Status status = luContext.Read(keyVec[0].Key.ReadOnlySpan, ref input, ref output, Empty.Default);
                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);
                    AssertBucketLockCount(ref keyVec[0], 0, 0);
                    ClassicAssert.IsFalse(status.IsPending);
                }

                AssertTotalLockCounts(0, 0);

                if (syncMode == CompletionSyncMode.Sync)
                {
                    _ = luContext.CompletePending(true);
                }
                else
                {
                    luContext.EndUnsafe();
                    await luContext.CompletePendingAsync();
                    luContext.BeginUnsafe();
                }

                // Shift head and retry - should not find in main memory now
                store.Log.FlushAndEvict(true);

                rng = new Random(RandSeed);
                sw.Restart();

                // Since we do random selection with replacement, we may not lock all keys--so need to track which we do
                // Similarly, we need to track bucket counts.
                BucketLockTracker blt = new();

                // Must have a pinned array to keep the key values present.
                var lockLongs = GC.AllocateArray<long>(NumRecs, pinned: true);
                var lockKeys = Enumerable.Range(0, NumRecs)
                                         .Select(ii =>
                                         {
                                             lockLongs[ii] = rng.Next(RandRange);
                                             return new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref lockLongs[ii]), LockType.Shared, luContext);
                                         }).ToArray();

                luContext.SortKeyHashes<FixedLengthTransactionalKeyStruct>(lockKeys);
                luContext.Lock<FixedLengthTransactionalKeyStruct>(lockKeys);

                var expectedS = 0;
                foreach (var idx in EnumActionKeyIndices(lockKeys, LockOperationType.Lock))
                {
                    ++expectedS;
                    long output = 0;
                    blt.IncrementS(ref lockKeys[idx]);
                    Status foundStatus = luContext.Read(lockKeys[idx].Key.ReadOnlySpan, ref input, ref output, Empty.Default);
                    ClassicAssert.IsTrue(foundStatus.IsPending);
                }

                // We did not lock all keys, only the "Action" ones - one lock per bucket, all shared in this test
                AssertTotalLockCounts(0, expectedS);

                CompletedOutputIterator<long, long, Empty> outputs;
                if (syncMode == CompletionSyncMode.Sync)
                {
                    _ = luContext.CompletePendingWithOutputs(out outputs, wait: true);
                }
                else
                {
                    luContext.EndUnsafe();
                    outputs = await luContext.CompletePendingWithOutputsAsync();
                    luContext.BeginUnsafe();
                }

                foreach (var idx in EnumActionKeyIndices(lockKeys, LockOperationType.Unlock))
                {
                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(lockKeys.AsSpan().Slice(idx, 1));
                    blt.DecrementS(ref lockKeys[idx]);
                }

                blt.AssertNoLocks();
                AssertTotalLockCounts(0, 0);

                int count = 0;
                while (outputs.Next())
                {
                    count++;
                    ClassicAssert.AreEqual(outputs.Current.Key.AsRef<long>() + NumRecords, outputs.Current.Output);
                }
                outputs.Dispose();
                ClassicAssert.AreEqual(expectedS, count);
            }
            finally
            {
                luContext.EndTransaction();
                luContext.EndUnsafe();
            }
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void InMemorySimpleLockTxnTest([Values] ResultLockTarget resultLockTarget,
                                              [Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase,
                                              [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            // SetUp also reads this to determine whether to supply ReadCache settings. If ReadCache is specified it wins over CopyToTail.
            var useRMW = updateOp == UpdateOp.RMW;
            long readKey24 = 24, readKey51 = 51, resultValue = -1;
            long resultKey = resultLockTarget == ResultLockTarget.LockTable ? NumRecords + 1 : readKey24 + readKey51;
            long expectedResult = (readKey24 + readKey51) * ValueMult;
            Status status;
            BucketLockTracker blt = new();

            var luContext = session.TransactionalUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            var keys = new[]
            {
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref readKey24), LockType.Shared, luContext),      // Source, shared
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref readKey51), LockType.Shared, luContext),      // Source, shared
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref resultKey), LockType.Exclusive, luContext),   // Destination, exclusive
            };
            luContext.SortKeyHashes<FixedLengthTransactionalKeyStruct>(keys);

            try
            {
                luContext.Lock<FixedLengthTransactionalKeyStruct>(keys);

                // Verify locks. Note that while we do not increment lock counts for multiple keys (each bucket gets a single lock per thread,
                // shared or exclusive), each key mapping to that bucket will report 'locked'.
                foreach (var key in keys)
                {
                    if (key.Key.ReadOnlySpan.AsRef<long>() == resultKey)
                        AssertIsLocked(key, xlock: true, slock: false);
                    else
                        AssertIsLocked(key, xlock: false, slock: true);
                }

                // Use blt because the counts are not 1:1 with keys if there are multiple keys in the same bucket
                foreach (var idx in EnumActionKeyIndices(keys, LockOperationType.Lock))
                    blt.Increment(ref keys[idx]);
                AssertTotalLockCounts(ref blt);

                // Re-get source values, to verify (e.g. they may be in readcache now).
                // We just locked this above, but for FlushMode.OnDisk it will be in the LockTable and will still be PENDING.
                long output = -1;
                status = luContext.Read(SpanByte.FromPinnedVariable(ref readKey24), ref output);
                if (flushMode == FlushMode.OnDisk)
                {
                    if (status.IsPending)
                    {
                        _ = luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        ClassicAssert.True(completedOutputs.Next());
                        output = completedOutputs.Current.Output;
                        ClassicAssert.AreEqual(24 * ValueMult, output);
                        ClassicAssert.False(completedOutputs.Next());
                        completedOutputs.Dispose();
                    }
                }
                else
                {
                    ClassicAssert.IsFalse(status.IsPending, status.ToString());
                }

                status = luContext.Read(SpanByte.FromPinnedVariable(ref readKey51), ref output);
                if (flushMode == FlushMode.OnDisk)
                {
                    if (status.IsPending)
                    {
                        _ = luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        ClassicAssert.True(completedOutputs.Next());
                        output = completedOutputs.Current.Output;
                        ClassicAssert.AreEqual(51 * ValueMult, output);
                        ClassicAssert.False(completedOutputs.Next());
                        completedOutputs.Dispose();
                    }
                }
                else
                {
                    ClassicAssert.IsFalse(status.IsPending, status.ToString());
                }

                // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
                long dummyInOut = 0;
                status = useRMW
                    ? luContext.RMW(SpanByte.FromPinnedVariable(ref resultKey), ref expectedResult, ref dummyInOut)
                    : luContext.Upsert(SpanByte.FromPinnedVariable(ref resultKey), ref dummyInOut, SpanByte.FromPinnedVariable(ref expectedResult), ref dummyInOut);
                if (flushMode == FlushMode.OnDisk)
                {
                    if (status.IsPending)
                    {
                        _ = luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        ClassicAssert.True(completedOutputs.Next());
                        resultValue = completedOutputs.Current.Output;
                        ClassicAssert.AreEqual(expectedResult, resultValue);
                        ClassicAssert.False(completedOutputs.Next());
                        completedOutputs.Dispose();
                    }
                }
                else
                {
                    ClassicAssert.IsFalse(status.IsPending, status.ToString());
                }

                // Reread the destination to verify
                status = luContext.Read(SpanByte.FromPinnedVariable(ref resultKey), ref resultValue);
                ClassicAssert.IsFalse(status.IsPending, status.ToString());
                ClassicAssert.AreEqual(expectedResult, resultValue);

                luContext.Unlock<FixedLengthTransactionalKeyStruct>(keys);

                foreach (var idx in EnumActionKeyIndices(keys, LockOperationType.Lock))
                    blt.Decrement(ref keys[idx]);
                AssertNoLocks(ref blt);
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

            // Verify reading the destination from the BasicContext.
            status = bContext.Read(SpanByte.FromPinnedVariable(ref resultKey), ref resultValue);
            ClassicAssert.IsFalse(status.IsPending, status.ToString());
            ClassicAssert.AreEqual(expectedResult, resultValue);
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void InMemoryLongLockTest([Values] ResultLockTarget resultLockTarget, [Values] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase,
                                         [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();
            PrepareRecordLocation(flushMode);

            bool initialDestWillBeLockTable = resultLockTarget == ResultLockTarget.LockTable || flushMode == FlushMode.OnDisk;
            long readKey24 = 24, readKey51 = 51, valueMult2 = 10, resultValue = -1;
            long resultKey = initialDestWillBeLockTable ? NumRecords + 1 : readKey24 + readKey51;
            long expectedResult = (readKey24 + readKey51) * ValueMult * valueMult2;
            var useRMW = updateOp == UpdateOp.RMW;
            Status status;
            BucketLockTracker blt = new();

            var luContext = session.TransactionalUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            var keys = new[]
            {
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref readKey24), LockType.Shared, luContext),      // Source, shared
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref readKey51), LockType.Shared, luContext),      // Source, shared
                new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref resultKey), LockType.Exclusive, luContext),   // Destination, exclusive
            };

            luContext.SortKeyHashes<FixedLengthTransactionalKeyStruct>(keys);

            var buckets = keys.Select(key => store.LockTable.GetBucketIndex(key.KeyHash)).ToArray();

            try
            {
                luContext.Lock<FixedLengthTransactionalKeyStruct>(keys);

                // Verify locks. Note that while we do not increment lock counts for multiple keys (each bucket gets a single lock per thread,
                // shared or exclusive), each key mapping to that bucket will report 'locked'.
                foreach (var key in keys)
                {
                    if (key.Key.ReadOnlySpan.AsRef<long>() == resultKey)
                        AssertIsLocked(key, xlock: true, slock: false);
                    else
                        AssertIsLocked(key, xlock: false, slock: true);
                }

                // Use blt because the counts are not 1:1 with keys if there are multiple keys in the same bucket
                foreach (var idx in EnumActionKeyIndices(keys, LockOperationType.Lock))
                    blt.Increment(ref keys[idx]);
                AssertTotalLockCounts(ref blt);

                long read24Output = 0, read51Output = 0;
                status = luContext.Read(SpanByte.FromPinnedVariable(ref readKey24), ref read24Output);
                if (flushMode == FlushMode.OnDisk)
                {
                    ClassicAssert.IsTrue(status.IsPending, status.ToString());
                    _ = luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, read24Output) = GetSinglePendingResult(completedOutputs, out var recordMetadata);
                    ClassicAssert.IsTrue(status.Found, status.ToString());
                }
                else
                    ClassicAssert.IsFalse(status.IsPending, status.ToString());
                ClassicAssert.AreEqual(readKey24 * ValueMult, read24Output);

                // We just locked this above, but for FlushMode.OnDisk it will still be PENDING.
                status = luContext.Read(SpanByte.FromPinnedVariable(ref readKey51), ref read51Output);
                if (flushMode == FlushMode.OnDisk)
                {
                    ClassicAssert.IsTrue(status.IsPending, status.ToString());
                    _ = luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    ClassicAssert.True(completedOutputs.Next());
                    read51Output = completedOutputs.Current.Output;
                    ClassicAssert.False(completedOutputs.Next());
                    completedOutputs.Dispose();
                }
                else
                    ClassicAssert.IsFalse(status.IsPending, status.ToString());
                ClassicAssert.AreEqual(readKey51 * ValueMult, read51Output);

                if (!initialDestWillBeLockTable)
                {
                    long initialResultValue = 0;
                    status = luContext.Read(SpanByte.FromPinnedVariable(ref resultKey), ref initialResultValue);
                    if (flushMode == FlushMode.OnDisk)
                    {
                        ClassicAssert.IsTrue(status.IsPending, status.ToString());
                        _ = luContext.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, initialResultValue) = GetSinglePendingResult(completedOutputs, out var recordMetadata);
                        ClassicAssert.IsTrue(status.Found, status.ToString());
                    }
                    else
                        ClassicAssert.IsFalse(status.IsPending, status.ToString());
                    ClassicAssert.AreEqual(resultKey * ValueMult, initialResultValue);
                }

                // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
                resultValue = (read24Output + read51Output) * valueMult2;
                status = useRMW
                    ? luContext.RMW(SpanByte.FromPinnedVariable(ref resultKey), ref resultValue) // value is 'input' for RMW
                    : luContext.Upsert(SpanByte.FromPinnedVariable(ref resultKey), SpanByte.FromPinnedVariable(ref resultValue));
                ClassicAssert.IsFalse(status.IsPending, status.ToString());

                status = luContext.Read(SpanByte.FromPinnedVariable(ref resultKey), ref resultValue);
                ClassicAssert.IsFalse(status.IsPending, status.ToString());
                ClassicAssert.AreEqual(expectedResult, resultValue);

                luContext.Unlock<FixedLengthTransactionalKeyStruct>(keys);

                foreach (var idx in EnumActionKeyIndices(keys, LockOperationType.Lock))
                    blt.Decrement(ref keys[idx]);
                AssertNoLocks(ref blt);
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

            // Verify from the full Basic Context
            var value = 0L;
            status = bContext.Read(SpanByte.FromPinnedVariable(ref resultKey), ref value);
            ClassicAssert.IsFalse(status.IsPending, status.ToString());
            ClassicAssert.AreEqual(expectedResult, resultValue);
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
#pragma warning disable IDE0060 // Remove unused parameter: readCopyDestination is used by Setup
        public void InMemoryDeleteTest([Values] ResultLockTarget resultLockTarget, [Values] ReadCopyDestination readCopyDestination,
                                       [Values(FlushMode.NoFlush, FlushMode.ReadOnly)] FlushMode flushMode, [Values(Phase.REST, Phase.PREPARE)] Phase phase)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            // Phase.INTERMEDIATE is to test the non-Phase.REST blocks
            Populate();
            PrepareRecordLocation(flushMode);

            BucketLockTracker blt = new();

            // SetUp also reads this to determine whether to supply ReadCache settings. If ReadCache is specified it wins over CopyToTail.
            long resultKeyVal = resultLockTarget == ResultLockTarget.LockTable ? NumRecords + 1 : 75, output = 0, resultValue = -1;
            Span<byte> resultKey = SpanByte.FromPinnedVariable(ref resultKeyVal);
            Status status;

            var luContext = session.TransactionalUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            var keyVec = new[] { new FixedLengthTransactionalKeyStruct(resultKey, LockType.Exclusive, luContext) };

            try
            {
                // Lock destination value.
                luContext.Lock<FixedLengthTransactionalKeyStruct>(keyVec);
                AssertIsLocked(ref keyVec[0], xlock: true, slock: false);

                blt.Increment(ref keyVec[0]);
                AssertTotalLockCounts(ref blt);

                // Set the phase to Phase.INTERMEDIATE to test the non-Phase.REST blocks
                session.ctx.SessionState = SystemState.Make(phase, session.ctx.version);
                status = luContext.Delete(resultKey);
                ClassicAssert.IsFalse(status.IsPending, status.ToString());

                // Reread the destination to verify
                status = luContext.Read(resultKey, ref output);
                ClassicAssert.IsFalse(status.Found, status.ToString());

                luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);
                blt.Decrement(ref keyVec[0]);

                AssertNoLocks(ref blt);
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

            // Verify reading the destination from the full Basic Context
            status = bContext.Read(resultKey, ref resultValue);
            ClassicAssert.IsFalse(status.Found, status.ToString());
            AssertTotalLockCounts(0, 0);
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void StressManualLocks([Values(1, 8)] int numLockThreads, [Values(0, 1, 8)] int numOpThreads)
        {
            Populate();

            // Lock in ordered sequence (avoiding deadlocks)
            const int baseKey = 42;
            const int numKeys = 20;
            const int numIncrement = 5;
            const int numIterations = 1000;

            IEnumerable<long> enumKeys(Random rng)
            {
                for (var key = baseKey + rng.Next(numIncrement); key < baseKey + numKeys; key += rng.Next(1, numIncrement))
                    yield return key;
            }

            void runManualLockThread(int tid)
            {
                BucketLockTracker blt = new();

                Random rng = new(tid + 101);

                using var localSession = store.NewSession<long, long, Empty, TransactionalUnsafeFunctions>(new TransactionalUnsafeFunctions());
                var luContext = localSession.TransactionalUnsafeContext;
                luContext.BeginUnsafe();
                luContext.BeginTransaction();

                IEnumerable<FixedLengthTransactionalKeyStruct> enumKeysToLock()
                {
                    foreach (var key in enumKeys(rng))
                    {
                        var lockType = rng.Next(100) < 60 ? LockType.Shared : LockType.Exclusive;
                        var keyNum = key;
                        yield return new(SpanByte.FromPinnedVariable(ref keyNum), lockType, luContext);
                    }
                }

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    var keys = enumKeysToLock().ToArray();
                    FixedLengthTransactionalKeyStruct.Sort(keys, luContext);
                    luContext.Lock<FixedLengthTransactionalKeyStruct>(keys);
                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(keys);
                }

                luContext.EndTransaction();
                luContext.EndUnsafe();
            }

            void runLTransientLockOpThread(int tid)
            {
                Random rng = new(tid + 101);

                using var localSession = store.NewSession<long, long, Empty, TransactionalUnsafeFunctions>(new TransactionalUnsafeFunctions());
                var basicContext = localSession.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    foreach (var key0 in enumKeys(rng))
                    {
                        long key = key0, value = key * ValueMult;
                        _ = rng.Next(100) switch
                        {
                            int rand when rand < 33 => basicContext.Read(SpanByte.FromPinnedVariable(ref key)).status,
                            int rand when rand < 66 => basicContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref value)),
                            _ => basicContext.RMW(SpanByte.FromPinnedVariable(ref key), ref value)
                        };
                    }
                }
            }

            // Run a mix of luContext and normal ClientSession operations
            int numThreads = numLockThreads + numOpThreads;
            Task[] tasks = new Task[numThreads];   // Task rather than Thread for propagation of exceptions.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (t <= numLockThreads)
                    tasks[t] = Task.Factory.StartNew(() => runManualLockThread(tid));
                else
                    tasks[t] = Task.Factory.StartNew(() => runLTransientLockOpThread(tid));
            }
            Task.WaitAll(tasks);

            AssertTotalLockCounts(0, 0);
        }

        FixedLengthTransactionalKeyStruct AddLockTableEntry<TFunctions>(TransactionalUnsafeContext<long, long, Empty, TFunctions, LongStoreFunctions, LongAllocator> luContext, ReadOnlySpan<byte> key)
            where TFunctions : ISessionFunctions<long, long, Empty>
        {
            var keyVec = new[] { new FixedLengthTransactionalKeyStruct(key, LockType.Exclusive, luContext) };
            luContext.Lock<FixedLengthTransactionalKeyStruct>(keyVec);

            HashEntryInfo hei = new(comparer.GetHashCode64(key));
            PopulateHei(ref hei);

            var lockState = store.LockTable.GetLockState(ref hei);

            ClassicAssert.IsTrue(lockState.IsFound);
            ClassicAssert.IsTrue(lockState.IsLockedExclusive);
            return keyVec[0];
        }

        void VerifyAndUnlockSplicedInKey<TFunctions>(TransactionalUnsafeContext<long, long, Empty, TFunctions, LongStoreFunctions, LongAllocator> luContext, ReadOnlySpan<byte> expectedKey)
            where TFunctions : ISessionFunctions<long, long, Empty>
        {
            // Scan to the end of the readcache chain and verify we inserted the value.
            var (_, pa) = ChainTests.SkipReadCacheChain(store, expectedKey);
            var storedKey = LogRecord.GetInlineKey(pa);
            ClassicAssert.AreEqual(expectedKey.AsRef<long>(), storedKey.AsRef<long>());

            var keyVec = new[] { new FixedLengthTransactionalKeyStruct(expectedKey, LockType.Exclusive, luContext) };
            luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyLocksAfterReadAndCTTTest()
        {
            Populate();
            store.Log.FlushAndEvict(wait: true);

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;
            long input = 0, output = 0, keyVal = 24;
            var key = SpanByte.FromPinnedVariable(ref keyVal);
            ReadOptions readOptions = new() { CopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog) };
            BucketLockTracker blt = new();

            luContext.BeginUnsafe();
            luContext.BeginTransaction();
            try
            {
                var keyStruct = AddLockTableEntry(luContext, key);
                blt.Increment(ref keyStruct);
                AssertTotalLockCounts(ref blt);

                var status = luContext.Read(key, ref input, ref output, ref readOptions, out _);
                ClassicAssert.IsTrue(status.IsPending, status.ToString());
                _ = luContext.CompletePending(wait: true);

                VerifyAndUnlockSplicedInKey(luContext, key);
                blt.Decrement(ref keyStruct);
                AssertNoLocks(ref blt);
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountsAfterFlushAndEvict()
        {
            Populate();

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;
            BucketLockTracker blt = new();
            long key = 24;

            luContext.BeginUnsafe();
            luContext.BeginTransaction();
            try
            {
                var keyVec = new[] { new FixedLengthTransactionalKeyStruct(SpanByte.FromPinnedVariable(ref key), LockType.Exclusive, luContext) };
                luContext.Lock<FixedLengthTransactionalKeyStruct>(keyVec);
                blt.Increment(ref keyVec[0]);
                AssertTotalLockCounts(ref blt);

                store.Log.FlushAndEvict(wait: true);
                AssertTotalLockCounts(1, 0);

                luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);
                blt.Decrement(ref keyVec[0]);

                blt.AssertNoLocks();
                AssertNoLocks(ref blt);
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
        }

        void PopulateAndEvict(bool immutable = false)
        {
            Populate();

            if (immutable)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            else
                store.Log.FlushAndEvict(true);
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountAfterUpsertToTailTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;
            BucketLockTracker blt = new();
            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            FixedLengthTransactionalKeyStruct keyStruct = default;
            try
            {
                long keyNum = 0;
                var key = SpanByte.FromPinnedVariable(ref keyNum);
                if (recordRegion is ChainTests.RecordRegion.Immutable or ChainTests.RecordRegion.OnDisk)
                    keyStruct = AddLockTableEntry(luContext, key.Set((long)UseExistingKey));
                else
                    keyStruct = AddLockTableEntry(luContext, key.Set((long)UseNewKey));

                blt.Increment(ref keyStruct);
                var status = luContext.Upsert(key, key);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());

                VerifyAndUnlockSplicedInKey(luContext, keyStruct.Key.ReadOnlySpan);
                blt.Decrement(ref keyStruct);
                AssertNoLocks(ref blt);
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountAfterRMWToTailTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;
            BucketLockTracker blt = new();
            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            FixedLengthTransactionalKeyStruct keyStruct = default;
            try
            {
                long keyVal = 0;
                var key = SpanByte.FromPinnedVariable(ref keyVal);
                if (recordRegion is ChainTests.RecordRegion.Immutable or ChainTests.RecordRegion.OnDisk)
                {
                    keyStruct = AddLockTableEntry(luContext, key.Set((long)UseExistingKey));
                    var input = keyStruct.Key.ReadOnlySpan.AsRef<long>() * ValueMult;
                    var status = luContext.RMW(keyStruct.Key.ReadOnlySpan, ref input);
                    ClassicAssert.IsTrue(recordRegion == ChainTests.RecordRegion.OnDisk ? status.IsPending : status.Found);
                    _ = luContext.CompletePending(wait: true);
                }
                else
                {
                    keyStruct = AddLockTableEntry(luContext, key.Set((long)UseNewKey));
                    var input = keyStruct.Key.ReadOnlySpan.AsRef<long>() * ValueMult;
                    var status = luContext.RMW(keyStruct.Key.ReadOnlySpan, ref input);
                    ClassicAssert.IsFalse(status.Found, status.ToString());
                }
                blt.Increment(ref keyStruct);

                VerifyAndUnlockSplicedInKey(luContext, keyStruct.Key.ReadOnlySpan);
                blt.Decrement(ref keyStruct);
                AssertNoLocks(ref blt);
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountAfterDeleteToTailTest([Values] ChainTests.RecordRegion recordRegion)
        {
            PopulateAndEvict(recordRegion == ChainTests.RecordRegion.Immutable);

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;
            BucketLockTracker blt = new();
            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            FixedLengthTransactionalKeyStruct keyStruct = default;
            try
            {
                long keyVal = 0;
                var key = SpanByte.FromPinnedVariable(ref keyVal);
                if (recordRegion is ChainTests.RecordRegion.Immutable or ChainTests.RecordRegion.OnDisk)
                {
                    keyStruct = AddLockTableEntry(luContext, key.Set((long)UseExistingKey));
                    blt.Increment(ref keyStruct);
                    var status = luContext.Delete(keyStruct.Key.ReadOnlySpan);

                    // Delete does not search outside mutable region so the key will not be found
                    ClassicAssert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                }
                else
                {
                    keyStruct = AddLockTableEntry(luContext, key.Set((long)UseNewKey));
                    blt.Increment(ref keyStruct);
                    var status = luContext.Delete(keyStruct.Key.ReadOnlySpan);
                    ClassicAssert.IsFalse(status.Found, status.ToString());
                }

                VerifyAndUnlockSplicedInKey(luContext, keyStruct.Key.ReadOnlySpan);
                blt.Decrement(ref keyStruct);
                AssertNoLocks(ref blt);
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void LockAndUnlockInLockTableOnlyTest()
        {
            // For this, just don't load anything, and it will happen in lock table.
            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;
            BucketLockTracker blt = new();

            FixedLengthTransactionalKeyStruct createKey(ReadOnlySpan<byte> key) => new(key, (key.AsRef<long>() & 1) == 0 ? LockType.Exclusive : LockType.Shared, luContext);

            // Need a pinned array for SpanByteFrom
            var keys = GC.AllocateArray<long>(NumRecords, pinned: true);

            var rng = new Random(101);
            var keyVec = Enumerable.Range(0, NumRecords).Select(ii => { keys[ii] = rng.Next(NumRecords); return createKey(SpanByte.FromPinnedVariable(ref keys[ii])); }).ToArray();

            luContext.BeginUnsafe();
            luContext.BeginTransaction();
            try
            {
                store.LockTable.SortKeyHashes<FixedLengthTransactionalKeyStruct>(keyVec);
                luContext.Lock<FixedLengthTransactionalKeyStruct>(keyVec);
                foreach (var idx in EnumActionKeyIndices(keyVec, LockOperationType.Lock))
                    blt.Increment(ref keyVec[idx]);
                AssertTotalLockCounts(ref blt);

                foreach (var idx in EnumActionKeyIndices(keyVec, LockOperationType.Lock))
                {
                    ref var key = ref keyVec[idx];
                    HashEntryInfo hei = new(key.KeyHash);
                    PopulateHei(ref hei);
                    var lockState = store.LockTable.GetLockState(ref hei);
                    ClassicAssert.IsTrue(lockState.IsFound);
                    ClassicAssert.AreEqual(key.LockType == LockType.Exclusive, lockState.IsLockedExclusive);
                    if (key.LockType == LockType.Shared)
                        ClassicAssert.IsTrue(lockState.IsLocked);    // Could be either shared or exclusive; we only lock the bucket once per Lock() call

                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec.AsSpan().Slice(idx, 1));
                    blt.Decrement(ref key);
                }

                blt.AssertNoLocks();
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void VerifyCountAfterReadOnlyToUpdateRecordTest([Values] UpdateOp updateOp)
        {
            Populate();
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            static long getValue(long key) => key + ValueMult;

            var luContext = session.TransactionalUnsafeContext;
            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            long key42Val = 42L;
            var key42 = SpanByte.FromPinnedVariable(ref key42Val);
            var keyVec = new[] { new FixedLengthTransactionalKeyStruct(key42, LockType.Exclusive, luContext) };

            try
            {
                luContext.Lock<FixedLengthTransactionalKeyStruct>(keyVec);

                long valueVal = getValue(key42Val);
                var value = SpanByte.FromPinnedVariable(ref valueVal);

                var status = updateOp switch
                {
                    UpdateOp.Upsert => luContext.Upsert(key42, value),
                    UpdateOp.RMW => luContext.RMW(keyVec[0].Key.ReadOnlySpan, ref valueVal),
                    UpdateOp.Delete => luContext.Delete(key42),
                    _ => new(StatusCode.Error)
                };
                ClassicAssert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
                if (updateOp == UpdateOp.RMW)
                    ClassicAssert.IsTrue(status.Record.CopyUpdated, status.ToString());
                else
                    ClassicAssert.IsTrue(status.Record.Created, status.ToString());

                OverflowBucketLockTableTests.AssertLockCounts(store, keyVec[0].Key.ReadOnlySpan, true, 0);

                luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);
                OverflowBucketLockTableTests.AssertLockCounts(store, keyVec[0].Key, false, 0);
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        public void LockNewRecordThenUpdateAndUnlockTest([Values] UpdateOp updateOp)
        {
            const int numNewRecords = 100;

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;

            long getValue(long kk) => kk + ValueMult;

            long keyVal = 0, valueVal = 0;
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyVal);
            Span<byte> value = SpanByte.FromPinnedVariable(ref valueVal);

            // If we are testing Delete, then we need to have the records ON-DISK first; Delete is a no-op for unfound records.
            if (updateOp == UpdateOp.Delete)
            {
                for (long keyNum = NumRecords; keyNum < NumRecords + numNewRecords; ++keyNum)
                    ClassicAssert.IsFalse(bContext.Upsert(key.Set(keyNum), value.Set(keyVal * ValueMult)).IsPending);
                store.Log.FlushAndEvict(wait: true);
            }

            // Now populate the main area of the log.
            Populate();
            BucketLockTracker blt = new();

            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            var keyVec = new FixedLengthTransactionalKeyStruct[1];

            try
            {
                // We don't sleep in this test
                comparer.maxSleepMs = 0;

                for (long keyNum = NumRecords; keyNum < NumRecords + numNewRecords; ++keyNum)
                {
                    keyVec[0] = new(key.Set(keyNum), LockType.Exclusive, luContext);
                    luContext.Lock<FixedLengthTransactionalKeyStruct>(keyVec);
                    for (var iter = 0; iter < 2; ++iter)
                    {
                        OverflowBucketLockTableTests.AssertLockCounts(store, key, true, 0);
                        updater(key, iter);
                    }
                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);
                    OverflowBucketLockTableTests.AssertLockCounts(store, key, false, 0);
                }
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

            void updater(ReadOnlySpan<byte> key, int iter)
            {
                var localValueNum = getValue(key.AsRef<long>());
                try
                {
                    Status status;
                    switch (updateOp)
                    {
                        case UpdateOp.Upsert:
                            status = luContext.Upsert(key, SpanByte.FromPinnedVariable(ref localValueNum));
                            if (iter == 0)
                                ClassicAssert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                            else
                                ClassicAssert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                            break;
                        case UpdateOp.RMW:
                            status = luContext.RMW(key, ref localValueNum);
                            if (iter == 0)
                                ClassicAssert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                            else
                                ClassicAssert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                            break;
                        case UpdateOp.Delete:
                            status = luContext.Delete(key);
                            ClassicAssert.IsTrue(status.NotFound, status.ToString());
                            if (iter == 0)
                                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
                            break;
                        default:
                            Assert.Fail($"Unexpected updateOp {updateOp}");
                            return;
                    }
                    ;
                    ClassicAssert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
                }
                catch (Exception)
                {
                    ClearCountsOnError(session);
                    throw;
                }
            }
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        //[Repeat(100)]
        public void LockNewRecordThenUnlockThenUpdateTest([Values] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            const int numNewRecords = 50;

            using var lockSession = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var lockLuContext = lockSession.TransactionalUnsafeContext;

            using var updateSession = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var basicContext = updateSession.BasicContext;

            long getValue(long kk) => kk + ValueMult;

            // If we are testing Delete, then we need to have the records ON-DISK first; Delete is a no-op for unfound records.
            // The actual value here is not important as we don't test it later.
            if (updateOp == UpdateOp.Delete)
            {
                for (long keyNum = NumRecords; keyNum < NumRecords + numNewRecords; ++keyNum)
                    ClassicAssert.IsFalse(bContext.Upsert(SpanByte.FromPinnedVariable(ref keyNum), SpanByte.FromPinnedVariable(ref keyNum)).IsPending);
                store.Log.FlushAndEvict(wait: true);
            }

            // Now populate the main area of the log.
            Populate();

            lockLuContext.BeginUnsafe();
            lockLuContext.BeginTransaction();

            // These are for debugging
            long[] lastLockerKeys = new long[6], lastUpdaterKeys = new long[3];

            // Randomize the start and lock-hold wait times
            int maxSleepMs = 10;
            Random lockRng = new(101), updateRng = new(107);

            var lockKeyVec = new FixedLengthTransactionalKeyStruct[1];

            try
            {
                for (long keyNum = NumRecords; keyNum < NumRecords + numNewRecords; ++keyNum)
                {
                    for (var iter = 0; iter < 2; ++iter)
                    {
                        // Use Task instead of Thread because this propagates exceptions (such as Assert.* failures) back to this thread.
                        // BasicContext's transient lock will wait for the lock/unlock combo to complete, or the lock/unlock will wait for basicContext to finish if it wins.
                        Task.WaitAll(Task.Run(() => locker(keyNum)), Task.Run(() => updater(keyNum, iter)));
                    }

                    AssertBucketLockCount(ref lockKeyVec[0], 0, 0);
                }
            }
            catch (Exception)
            {
                ClearCountsOnError(lockSession);
                throw;
            }
            finally
            {
                lockLuContext.EndTransaction();
                lockLuContext.EndUnsafe();
            }

            void locker(long keyNum)
            {
                lockKeyVec[0] = new(SpanByte.FromPinnedVariable(ref keyNum), LockType.Exclusive, lockLuContext);
                try
                {
                    // Begin/EndTransaction are called outside this function; we could not EndTransaction in here as the lock lifetime is beyond that.
                    // (BeginTransaction's scope is the session; BeginUnsafe's scope is the thread. The session is still "mono-threaded" here because
                    // only one thread at a time is making calls on it.)
                    lastLockerKeys[0] = keyNum;
                    lockLuContext.BeginUnsafe();
                    lastLockerKeys[1] = keyNum;
                    Thread.Sleep(lockRng.Next(maxSleepMs));
                    lastLockerKeys[2] = keyNum;
                    lockLuContext.Lock<FixedLengthTransactionalKeyStruct>(lockKeyVec);
                    lastLockerKeys[3] = keyNum;
                    Thread.Sleep(lockRng.Next(maxSleepMs));
                    lastLockerKeys[4] = keyNum;
                    lockLuContext.Unlock<FixedLengthTransactionalKeyStruct>(lockKeyVec);
                    lastLockerKeys[5] = keyNum;
                }
                catch (Exception)
                {
                    ClearCountsOnError(lockSession);
                    throw;
                }
                finally
                {
                    lockLuContext.EndUnsafe();
                }
            }

            void updater(long keyNum, int iter)
            {
                try
                {
                    lastUpdaterKeys[0] = keyNum;
                    Thread.Sleep(updateRng.Next(maxSleepMs));
                    lastUpdaterKeys[1] = keyNum;
                    Status status;
                    var localValueNum = getValue(keyNum);
                    Span<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref localValueNum);
                    switch (updateOp)
                    {
                        case UpdateOp.Upsert:
                            status = basicContext.Upsert(key, value);
                            if (iter == 0)
                                ClassicAssert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                            else
                                ClassicAssert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                            break;
                        case UpdateOp.RMW:
                            status = basicContext.RMW(key, ref localValueNum);
                            if (iter == 0)
                                ClassicAssert.IsTrue(status.NotFound && status.Record.Created, status.ToString());
                            else
                                ClassicAssert.IsTrue(status.Found && status.Record.InPlaceUpdated, status.ToString());
                            break;
                        case UpdateOp.Delete:
                            status = basicContext.Delete(key);
                            ClassicAssert.IsTrue(status.NotFound, status.ToString());
                            if (iter == 0)
                                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
                            break;
                        default:
                            Assert.Fail($"Unexpected updateOp {updateOp}");
                            return;
                    }
                    ;
                    ClassicAssert.IsFalse(status.IsFaulted, $"Unexpected UpdateOp {updateOp}, status {status}");
                    lastUpdaterKeys[2] = keyNum;
                }
                catch (Exception)
                {
                    ClearCountsOnError(lockSession);
                    throw;
                }
            }
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void MultiSharedLockTest()
        {
            Populate();

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;

            long keyNum = 42;
            var key = SpanByte.FromPinnedVariable(ref keyNum);
            var maxLocks = 63;

            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            var keyVec = new FixedLengthTransactionalKeyStruct[1];

            try
            {
                for (var ii = 0; ii < maxLocks; ++ii)
                {
                    keyVec[0] = new(key, LockType.Shared, luContext);
                    luContext.Lock<FixedLengthTransactionalKeyStruct>(keyVec);
                    OverflowBucketLockTableTests.AssertLockCounts(store, key, false, ii + 1);
                }

                for (var ii = 0; ii < maxLocks; ++ii)
                {
                    keyVec[0] = new(key, LockType.Shared, luContext);
                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);
                    OverflowBucketLockTableTests.AssertLockCounts(store, key, false, maxLocks - ii - 1);
                }
                OverflowBucketLockTableTests.AssertLockCounts(store, key, false, 0);
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void TryLockTimeSpanLimitTest()
        {
            Populate();

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;

            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            var keys = GC.AllocateArray<long>(3, pinned: true);
            keys[0] = 42;
            keys[1] = 43;
            keys[2] = 44;

            var keyVec = new FixedLengthTransactionalKeyStruct[]
            {
                new(SpanByte.FromPinnedVariable(ref keys[0]), LockType.Exclusive, luContext),
                new(SpanByte.FromPinnedVariable(ref keys[1]), LockType.Exclusive, luContext),
                new(SpanByte.FromPinnedVariable(ref keys[2]), LockType.Exclusive, luContext)
            };

            // First ensure things work with no blocking locks.
            ClassicAssert.IsTrue(luContext.TryLock<FixedLengthTransactionalKeyStruct>(keyVec));
            luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);

            var blockingVec = new FixedLengthTransactionalKeyStruct[1];

            try
            {
                for (var blockingIdx = 0; blockingIdx < keyVec.Length; ++blockingIdx)
                {
                    // This key blocks the lock. Test all positions in keyVec to ensure rollback of locks on failure.
                    blockingVec[0] = keyVec[blockingIdx];
                    luContext.Lock<FixedLengthTransactionalKeyStruct>(blockingVec);

                    // Now try the lock, and verify there are no locks left after (any taken must be rolled back on failure).
                    ClassicAssert.IsFalse(luContext.TryLock<FixedLengthTransactionalKeyStruct>(keyVec, TimeSpan.FromMilliseconds(20)));
                    foreach (var k in keyVec)
                    {
                        if (k.Key.ReadOnlySpan.AsRef<long>() != blockingVec[0].Key.ReadOnlySpan.AsRef<long>())
                            OverflowBucketLockTableTests.AssertLockCounts(store, k.Key.ReadOnlySpan, false, 0);
                    }

                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(blockingVec);
                }
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void TryLockCancellationTest()
        {
            Populate();

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;

            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            var keys = GC.AllocateArray<long>(3, pinned: true);
            keys[0] = 42;
            keys[1] = 43;
            keys[2] = 44;

            var keyVec = new FixedLengthTransactionalKeyStruct[]
            {
                new(SpanByte.FromPinnedVariable(ref keys[0]), LockType.Exclusive, luContext),
                new(SpanByte.FromPinnedVariable(ref keys[1]), LockType.Exclusive, luContext),
                new(SpanByte.FromPinnedVariable(ref keys[2]), LockType.Exclusive, luContext)
            };

            // First ensure things work with no blocking locks.
            ClassicAssert.IsTrue(luContext.TryLock<FixedLengthTransactionalKeyStruct>(keyVec));
            luContext.Unlock<FixedLengthTransactionalKeyStruct>(keyVec);

            var blockingVec = new FixedLengthTransactionalKeyStruct[1];

            try
            {
                for (var blockingIdx = 0; blockingIdx < keyVec.Length; ++blockingIdx)
                {
                    // This key blocks the lock. Test all positions in keyVec to ensure rollback of locks on failure.
                    blockingVec[0] = keyVec[blockingIdx];
                    luContext.Lock<FixedLengthTransactionalKeyStruct>(blockingVec);

                    using var cts = new CancellationTokenSource(20);

                    // Now try the lock, and verify there are no locks left after (any taken must be rolled back on failure).
                    ClassicAssert.IsFalse(luContext.TryLock<FixedLengthTransactionalKeyStruct>(keyVec, cts.Token));
                    foreach (var k in keyVec)
                    {
                        if (k.Key.ReadOnlySpan.AsRef<long>() != blockingVec[0].Key.ReadOnlySpan.AsRef<long>())
                            OverflowBucketLockTableTests.AssertLockCounts(store, k.Key.ReadOnlySpan, false, 0);
                    }

                    luContext.Unlock<FixedLengthTransactionalKeyStruct>(blockingVec);
                }
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void TryPromoteLockTimeSpanLimitTest()
        {
            Populate();

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;

            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            long keyNum = 42;
            var key = SpanByte.FromPinnedVariable(ref keyNum);

            var exclusiveVec = new FixedLengthTransactionalKeyStruct[] { new(key, LockType.Exclusive, luContext) };
            var sharedVec = new FixedLengthTransactionalKeyStruct[] { new(key, LockType.Shared, luContext) };

            try
            {
                // Lock twice so it is blocked by the second reader
                ClassicAssert.IsTrue(luContext.TryLock<FixedLengthTransactionalKeyStruct>(sharedVec));
                ClassicAssert.IsTrue(luContext.TryLock<FixedLengthTransactionalKeyStruct>(sharedVec));

                ClassicAssert.IsFalse(luContext.TryPromoteLock(exclusiveVec[0], TimeSpan.FromMilliseconds(20)));

                // Unlock one of the readers and verify successful promotion
                luContext.Unlock<FixedLengthTransactionalKeyStruct>(sharedVec);
                ClassicAssert.IsTrue(luContext.TryPromoteLock(exclusiveVec[0]));
                luContext.Unlock<FixedLengthTransactionalKeyStruct>(exclusiveVec);
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
        }

        [Test]
        [Category(TransactionalUnsafeContextTestCategory)]
        [Category(SmokeTestCategory)]
        public void TryPromoteLockCancellationTest()
        {
            Populate();

            using var session = store.NewSession<long, long, Empty, SimpleLongSimpleFunctions>(new SimpleLongSimpleFunctions());
            var luContext = session.TransactionalUnsafeContext;

            luContext.BeginUnsafe();
            luContext.BeginTransaction();

            long keyNum = 42;
            var key = SpanByte.FromPinnedVariable(ref keyNum);

            var exclusiveVec = new FixedLengthTransactionalKeyStruct[] { new(key, LockType.Exclusive, luContext) };
            var sharedVec = new FixedLengthTransactionalKeyStruct[] { new(key, LockType.Shared, luContext) };

            try
            {
                // Lock twice so it is blocked by the second reader
                ClassicAssert.IsTrue(luContext.TryLock<FixedLengthTransactionalKeyStruct>(sharedVec));
                ClassicAssert.IsTrue(luContext.TryLock<FixedLengthTransactionalKeyStruct>(sharedVec));

                using var cts = new CancellationTokenSource(20);
                ClassicAssert.IsFalse(luContext.TryPromoteLock(exclusiveVec[0], cts.Token));

                // Unlock one of the readers and verify successful promotion
                luContext.Unlock<FixedLengthTransactionalKeyStruct>(sharedVec);
                ClassicAssert.IsTrue(luContext.TryPromoteLock(exclusiveVec[0]));
                luContext.Unlock<FixedLengthTransactionalKeyStruct>(exclusiveVec);
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
        }
    }
}