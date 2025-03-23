﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.core.Utility;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Revivification
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    internal readonly struct RevivificationSpanByteComparer : IKeyComparer
    {
        private readonly SpanByteComparer defaultComparer;
        private readonly int collisionRange;

        internal RevivificationSpanByteComparer(CollisionRange range)
        {
            defaultComparer = new SpanByteComparer();
            collisionRange = (int)range;
        }

        public bool Equals(SpanByte k1, SpanByte k2) => defaultComparer.Equals(k1, k2);

        // The hash code ends with 0 so mod Ten isn't so helpful, so shift
        public long GetHashCode64(SpanByte k) => (defaultComparer.GetHashCode64(k) >> 4) % collisionRange;
    }
}

namespace Tsavorite.test.Revivification
{
#if LOGRECORD_TODO
    using ClassAllocator = GenericAllocator<MyKey, MyValue, StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>>;
    using ClassStoreFunctions = StoreFunctions<MyKey, MyValue, MyKey.Comparer, DefaultRecordDisposer<MyKey, MyValue>>;
#endif // LOGRECORD_TODO

    using LongAllocator = SpanByteAllocator<StoreFunctions<SpanByte, LongKeyComparer, SpanByteRecordDisposer>>;
    using LongStoreFunctions = StoreFunctions<SpanByte, LongKeyComparer, SpanByteRecordDisposer>;

    using SpanByteStoreFunctions = StoreFunctions<SpanByte, RevivificationSpanByteComparer, SpanByteRecordDisposer>;

    public enum DeleteDest { FreeList, InChain }

    public enum CollisionRange { Ten = 10, None = int.MaxValue }

    public enum RevivificationEnabled { Reviv, NoReviv }

    public enum RevivifiableFraction { Half }

    public enum RecordElision { Elide, NoElide }

    struct RevivificationTestUtils
    {
        internal static RevivificationSettings FixedLengthBins = new()
        {
            FreeRecordBins =
            [
                new RevivificationBin()
                {
                    RecordSize = RoundUp(RecordInfo.GetLength() + 2 * (sizeof(int) + sizeof(long)), Constants.kRecordAlignment), // We have "fixed length" for these integer bins, with long Key and Value
                    BestFitScanLimit = RevivificationBin.UseFirstFit
                }
            ]
        };

        internal const double HalfOfMutableFraction = 0.5;   // Half of the mutable region

        internal static double GetRevivifiableFraction(RevivifiableFraction frac)
            => frac switch
            {
                RevivifiableFraction.Half => HalfOfMutableFraction,
                _ => throw new InvalidOperationException($"Invalid RevivifiableFraction enum value {frac}")
            };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static RMWInfo CopyToRMWInfo(ref UpsertInfo upsertInfo)
            => new()
            {
                Version = upsertInfo.Version,
                SessionID = upsertInfo.SessionID,
                Address = upsertInfo.Address,
                KeyHash = upsertInfo.KeyHash,
                Action = RMWAction.Default,
            };

        internal static FreeRecordPool<TStoreFunctions, TAllocator> CreateSingleBinFreeRecordPool<TStoreFunctions, TAllocator>(
                TsavoriteKV<TStoreFunctions, TAllocator> store, RevivificationBin binDef)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => new(store, new RevivificationSettings() { FreeRecordBins = [binDef] });

        internal static bool HasRecords<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => HasRecords(store.RevivificationManager.FreeRecordPool);

        internal static bool HasRecords<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, FreeRecordPool<TStoreFunctions, TAllocator> pool)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => HasRecords(pool ?? store.RevivificationManager.FreeRecordPool);

        internal static bool HasRecords<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            if (pool is not null)
            {
                foreach (var bin in pool.bins)
                {
                    if (!bin.isEmpty)
                        return true;
                }
            }
            return false;
        }

        internal static FreeRecordPool<TStoreFunctions, TAllocator> SwapFreeRecordPool<TStoreFunctions, TAllocator>(
                TsavoriteKV<TStoreFunctions, TAllocator> store, FreeRecordPool<TStoreFunctions, TAllocator> inPool)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            var pool = store.RevivificationManager.FreeRecordPool;
            store.RevivificationManager.FreeRecordPool = inPool;
            return pool;
        }

        internal const int DefaultRecordWaitTimeoutMs = 2000;

        internal static bool GetBinIndex<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool, int recordSize, out int binIndex)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => pool.GetBinIndex(recordSize, out binIndex);

        internal static int GetBinCount<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => pool.bins.Length;

        internal static int GetRecordCount<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool, int binIndex)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => pool.bins[binIndex].recordCount;

        internal static int GetMaxRecordSize<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool, int binIndex)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => pool.bins[binIndex].maxRecordSize;

        internal static unsafe bool IsSet<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool, int binIndex, int recordIndex)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => pool.bins[binIndex].records[recordIndex].IsSet;

        internal static bool TryTakeFromBin<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool, int binIndex, ref RecordSizeInfo sizeInfo, long minAddress,
                TsavoriteKV<TStoreFunctions, TAllocator> store, out long address, ref RevivificationStats revivStats)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => pool.bins[binIndex].TryTake(ref sizeInfo, minAddress, store, out address, ref revivStats);

        internal static int GetSegmentStart<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool, int binIndex, int recordSize)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => pool.bins[binIndex].GetSegmentStart(recordSize);

        internal static void WaitForRecords<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, bool want, FreeRecordPool<TStoreFunctions, TAllocator> pool = default)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            pool ??= store.RevivificationManager.FreeRecordPool;

            // Wait until CheckEmptyWorker or TryAdd() has set the bin counters.
            var sw = new Stopwatch();
            sw.Start();
            if (pool is not null)
            {
                while (HasRecords(pool) != want)
                {
                    if (sw.ElapsedMilliseconds >= DefaultRecordWaitTimeoutMs)
                        ClassicAssert.Less(sw.ElapsedMilliseconds, DefaultRecordWaitTimeoutMs, $"Timeout while waiting for Pool.WaitForRecords to be {want}");
                    _ = Thread.Yield();
                }
                return;
            }
        }

        internal static unsafe int GetFreeRecordCount<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => GetFreeRecordCount(store.RevivificationManager.FreeRecordPool);

        internal static unsafe int GetFreeRecordCount<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            // This returns the count of all records, not just the free ones.
            var count = 0;
            if (pool is not null)
            {
                foreach (var bin in pool.bins)
                {
                    for (var ii = 0; ii < bin.recordCount; ++ii)
                    {
                        if ((bin.records + ii)->IsSet)
                            ++count;
                    }
                }
            }
            return count;
        }

        internal static void AssertElidable<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, SpanByte key)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(store.storeFunctions.GetKeyHashCode64(key));
            ClassicAssert.IsTrue(store.FindTag(ref stackCtx.hei), $"AssertElidable: Cannot find key {key}");
            var recordInfo = LogRecord.GetInfo(store.hlog.GetPhysicalAddress(stackCtx.hei.Address));
            ClassicAssert.Less(recordInfo.PreviousAddress, store.hlogBase.BeginAddress, "AssertElidable: expected elidable key");
        }

        internal static int GetRevivifiableRecordCount<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, int numRecords)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => (int)(numRecords * store.RevivificationManager.revivifiableFraction);

        internal static int GetMinRevivifiableKey<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, int numRecords)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => numRecords - GetRevivifiableRecordCount(store, numRecords);
    }

    [TestFixture]
    class RevivificationFixedLenTests
    {
        internal class RevivificationFixedLenFunctions : SimpleLongSimpleFunctions
        {
        }

        const int NumRecords = 1000;
        internal const int ValueMult = 1_000_000;

        RevivificationFixedLenFunctions functions;

        private TsavoriteKV<LongStoreFunctions, LongAllocator> store;
        private ClientSession<SpanByte, long, long, Empty, RevivificationFixedLenFunctions, LongStoreFunctions, LongAllocator> session;
        private BasicContext<SpanByte, long, long, Empty, RevivificationFixedLenFunctions, LongStoreFunctions, LongAllocator> bContext;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            double? revivifiableFraction = default;
            RecordElision? recordElision = default;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is RevivifiableFraction frac)
                {
                    revivifiableFraction = RevivificationTestUtils.GetRevivifiableFraction(frac);
                    continue;
                }
                if (arg is RecordElision re)
                {
                    recordElision = re;
                    continue;
                }
            }

            var revivificationSettings = RevivificationTestUtils.FixedLengthBins.Clone();
            if (revivifiableFraction.HasValue)
                revivificationSettings.RevivifiableFraction = revivifiableFraction.Value;
            if (recordElision.HasValue)
                revivificationSettings.RestoreDeletedRecordsIfBinIsFull = recordElision.Value == RecordElision.NoElide;
            store = new(new()
            {
                IndexSize = 1L << 24,
                LogDevice = log,
                PageSize = 1L << 12,
                MemorySize = 1L << 20,
                RevivificationSettings = revivificationSettings
            }, StoreFunctions<SpanByte>.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
            functions = new RevivificationFixedLenFunctions();
            session = store.NewSession<long, long, Empty, RevivificationFixedLenFunctions>(functions);
            bContext = session.BasicContext;
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;

            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            for (long keyNum = 0; keyNum < NumRecords; keyNum++)
            {
                long valueNum = keyNum * ValueMult;
                var status = bContext.Upsert(SpanByteFrom(ref keyNum), SpanByteFrom(ref valueNum));
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleFixedLenTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;
            if (stayInChain)
                _ = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            long deleteKeyNum = RevivificationTestUtils.GetMinRevivifiableKey(store, NumRecords);
            SpanByte deleteKey = SpanByteFrom(ref deleteKeyNum);
            if (!stayInChain)
                RevivificationTestUtils.AssertElidable(store, deleteKey);
            var tailAddress = store.Log.TailAddress;

            _ = bContext.Delete(deleteKey);
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);

            long updateKeyNum = deleteDest == DeleteDest.InChain ? deleteKeyNum : NumRecords + 1;
            var updateValueNum = updateKeyNum + ValueMult;
            SpanByte updateKey = SpanByteFrom(ref updateKeyNum), updateValue = SpanByteFrom(ref updateValueNum);

            if (!stayInChain)
            {
                ClassicAssert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
                RevivificationTestUtils.WaitForRecords(store, want: true);
            }

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(updateKey, updateValue) : bContext.RMW(updateKey, updateValueNum);

            if (!stayInChain)
                RevivificationTestUtils.WaitForRecords(store, want: false);
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress, "Expected tail address not to grow (record was revivified)");
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void UnelideTest([Values] RecordElision elision, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            // First delete all keys. This will overflow the bin.
            for (long keyNum = 0; keyNum < NumRecords; ++keyNum)
            {
                _ = bContext.Delete(SpanByteFrom(ref keyNum));
                ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            }

            // The NumberOfRecords will be adjusted upward so the partition is cache-line aligned, so this may be higher than specified.
            ClassicAssert.LessOrEqual(RevivificationBin.DefaultRecordsPerBin, RevivificationTestUtils.GetFreeRecordCount(store));
            RevivificationTestUtils.WaitForRecords(store, want: true);

            // Now re-add the keys.
            for (long keyNum = 0; keyNum < NumRecords; ++keyNum)
            {
                long valueNum = keyNum + ValueMult;
                SpanByte key = SpanByteFrom(ref keyNum), value = SpanByteFrom(ref valueNum);
                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, value) : bContext.RMW(key, valueNum);
            }

            // Now re-add the keys. For the elision case, we should see tailAddress grow sharply as only the records in the bin are available
            // for revivification. For In-Chain, we will revivify records that were unelided after the bin overflowed. But we have some records
            // ineligible for revivification due to revivifiableFraction.
            var recordSize = RoundUp(RecordInfo.GetLength() + (sizeof(int) + sizeof(long)) * 2, Constants.kRecordAlignment);
            var numIneligibleRecords = NumRecords - RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords);
            var noElisionExpectedTailAddress = tailAddress + numIneligibleRecords * recordSize;

            if (elision == RecordElision.NoElide)
                ClassicAssert.AreEqual(noElisionExpectedTailAddress, store.Log.TailAddress, "Expected tail address not to grow (records were revivified)");
            else
                ClassicAssert.Less(noElisionExpectedTailAddress, store.Log.TailAddress, "Expected tail address to grow (records were not revivified)");
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
#pragma warning disable IDE0060 // Remove unused parameter (used by setup)
        public void SimpleMinAddressAddTest([Values] RevivifiableFraction revivifiableFraction)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            Populate();

            // This should not go to FreeList because it's below the RevivifiableFraction
            long keyNum = 2;
            ClassicAssert.IsTrue(bContext.Delete(SpanByteFrom(ref keyNum)).Found);
            ClassicAssert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store));

            // This should go to FreeList because it's above the RevivifiableFraction
            keyNum = NumRecords - 1;
            ClassicAssert.IsTrue(bContext.Delete(SpanByteFrom(ref keyNum)).Found);
            ClassicAssert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
#pragma warning disable IDE0060 // Remove unused parameter (used by setup)
        public void SimpleMinAddressTakeTest([Values] RevivifiableFraction revivifiableFraction, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            Populate();

            // This should go to FreeList because it's above the RevivifiableFraction
            long keyNum = NumRecords - 1;
            ClassicAssert.IsTrue(bContext.Delete(SpanByteFrom(ref keyNum)).Found);
            ClassicAssert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
            RevivificationTestUtils.WaitForRecords(store, want: true);

            // Detach the pool temporarily so the records aren't revivified by the next insertions.
            var pool = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            // Now add a bunch of records to drop the FreeListed address below the RevivifiableFraction
            long maxRecord = NumRecords * 2, valueNum;
            for (keyNum = NumRecords; keyNum < maxRecord; keyNum++)
            {
                valueNum = keyNum * ValueMult;
                var status = bContext.Upsert(SpanByteFrom(ref keyNum), SpanByteFrom(ref valueNum));
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }

            // Restore the pool
            _ = RevivificationTestUtils.SwapFreeRecordPool(store, pool);

            var tailAddress = store.Log.TailAddress;
            valueNum = maxRecord * ValueMult;
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(SpanByteFrom(ref maxRecord), SpanByteFrom(ref valueNum)) : bContext.RMW(SpanByteFrom(ref maxRecord), valueNum);

            ClassicAssert.Less(tailAddress, store.Log.TailAddress, "Expected tail address to grow (record was not revivified)");
        }
    }

    [TestFixture]
    class RevivificationSpanByteTests
    {
        const int KeyLength = 10;
        const int InitialLength = 50;
        const int GrowLength = InitialLength + 75;      // Must be large enough to go to next bin
        const int ShrinkLength = InitialLength - 25;    // Must be small enough to go to previous bin

        const int OversizeLength = RevivificationBin.MaxInlineRecordSize + 42;

        internal class RevivificationSpanByteFunctions : SpanByteFunctions<Empty>
        {
            private readonly TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;

            // Must be set after session is created
            internal ClientSession<SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> session;

            internal int expectedInputLength = InitialLength;

            // This is a queue rather than a single value because there may be calls to, for example, ConcurrentWriter with one length
            // followed by SingleWriter with another.
            internal Queue<int> expectedValueLengths = new();

            internal bool readCcCalled, rmwCcCalled;

            internal RevivificationSpanByteFunctions(TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store)
            {
                this.store = store;
            }

            private void AssertInfoValid(ref UpsertInfo updateInfo)
            {
                ClassicAssert.AreEqual(session.ctx.version, updateInfo.Version);
            }
            private void AssertInfoValid(ref RMWInfo rmwInfo)
            {
                ClassicAssert.AreEqual(session.ctx.version, rmwInfo.Version);
            }
            private void AssertInfoValid(ref DeleteInfo deleteInfo)
            {
                ClassicAssert.AreEqual(session.ctx.version, deleteInfo.Version);
            }

            private static void VerifyKeyAndValue(SpanByte functionsKey, SpanByte functionsValue)
            {
                int valueOffset = 0, valueLengthRemaining = functionsValue.Length;
                ClassicAssert.Less(functionsKey.Length, valueLengthRemaining);
                while (valueLengthRemaining > 0)
                {
                    var compareLength = Math.Min(functionsKey.Length, valueLengthRemaining);
                    Span<byte> valueSpan = functionsValue.AsSpan().Slice(valueOffset, compareLength);
                    Span<byte> keySpan = functionsKey.AsSpan()[..compareLength];
                    ClassicAssert.IsTrue(valueSpan.SequenceEqual(keySpan), $"functionsValue (offset {valueOffset}, len {compareLength}: {SpanByte.FromPinnedSpan(valueSpan)}) does not match functionsKey ({SpanByte.FromPinnedSpan(keySpan)})");
                    valueLengthRemaining -= compareLength;
                }
            }

            void CheckExpectedLengthsBefore(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, long recordAddress, bool isIPU = false)
            {
                var expectedValueLength = expectedValueLengths.Dequeue();

                // If the logRecord is from new record creation it has not had its overflow set yet; it has just been initialized to inline length of SpanField.OverflowDataPtrSize,
                // and we'll call SpanField.ConvertToOverflow later in this ISessionFunctions call to do the actual overflow allocation.
                if (!logRecord.Info.ValueIsInline || (sizeInfo.IsSet && !sizeInfo.ValueIsInline))
                    ClassicAssert.AreEqual(SpanField.OverflowInlineSize, SpanField.GetTotalSizeOfInlineField(logRecord.ValueAddress) - SpanField.FieldLengthPrefixSize);
                if (sizeInfo.ValueIsInline)
                    ClassicAssert.AreEqual(expectedValueLength, logRecord.ValueSpan.Length);
                else
                    ClassicAssert.AreEqual(logRecord.Info.ValueIsInline ? expectedValueLength : SpanField.OverflowInlineSize, logRecord.ValueSpan.Length);

                ClassicAssert.GreaterOrEqual(recordAddress, store.hlogBase.ReadOnlyAddress);

                // !IsSet means it is from Delete which does not receive a RecordSizeInfo. isIPU is an in-place update and thus the new value may legitimately be larger than the record.
                if (sizeInfo.IsSet && !isIPU)
                {
                    var (actual, allocated) = logRecord.GetInlineRecordSizes();
                    ClassicAssert.AreEqual(sizeInfo.ActualInlineRecordSize, actual);
                    ClassicAssert.AreEqual(sizeInfo.AllocatedInlineRecordSize, allocated);
                }
            }

            public override bool SingleWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                CheckExpectedLengthsBefore(ref logRecord, ref sizeInfo, upsertInfo.Address);
                return base.SingleWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo, reason);
            }

            public override bool ConcurrentWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                CheckExpectedLengthsBefore(ref logRecord, ref sizeInfo, upsertInfo.Address, isIPU: true);
                return base.ConcurrentWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }

            public override bool InitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                ClassicAssert.AreEqual(expectedInputLength, input.Length);

                CheckExpectedLengthsBefore(ref logRecord, ref sizeInfo, rmwInfo.Address);
                return logRecord.TrySetValueSpan(input, ref sizeInfo);
            }

            public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                ClassicAssert.AreEqual(expectedInputLength, input.Length);

                CheckExpectedLengthsBefore(ref dstLogRecord, ref sizeInfo, rmwInfo.Address);
                return dstLogRecord.TrySetValueSpan(input, ref sizeInfo);
            }

            public override bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                ClassicAssert.AreEqual(expectedInputLength, input.Length);

                CheckExpectedLengthsBefore(ref logRecord, ref sizeInfo, rmwInfo.Address, isIPU: true);
                VerifyKeyAndValue(logRecord.Key, logRecord.ValueSpan);

                return logRecord.TrySetValueSpan(input, ref sizeInfo);
            }

            public override bool SingleDeleter(ref LogRecord<SpanByte> logRecord, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);

                RecordSizeInfo sizeInfo = default;
                CheckExpectedLengthsBefore(ref logRecord, ref sizeInfo, deleteInfo.Address);

                return base.SingleDeleter(ref logRecord, ref deleteInfo);
            }

            public override bool ConcurrentDeleter(ref LogRecord<SpanByte> logRecord, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);

                RecordSizeInfo sizeInfo = default;
                CheckExpectedLengthsBefore(ref logRecord, ref sizeInfo, deleteInfo.Address);

                return base.ConcurrentDeleter(ref logRecord, ref deleteInfo);
            }

            public override bool PostCopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                return base.PostCopyUpdater(ref srcLogRecord, ref dstLogRecord, ref sizeInfo, ref input, ref output, ref rmwInfo);
            }

            public override void PostInitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                base.PostInitialUpdater(ref logRecord, ref sizeInfo, ref input, ref output, ref rmwInfo);
            }

            public override void PostSingleWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason writeReason)
            {
                AssertInfoValid(ref upsertInfo);
                base.PostSingleWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo, writeReason);
            }

            public override void PostSingleDeleter(ref LogRecord<SpanByte> logRecord, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);
                base.PostSingleDeleter(ref logRecord, ref deleteInfo);
            }

            public override void ReadCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref SpanByte input, ref SpanByteAndMemory output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                readCcCalled = true;
                base.ReadCompletionCallback(ref diskLogRecord, ref input, ref output, ctx, status, recordMetadata);
            }

            public override void RMWCompletionCallback(ref DiskLogRecord<SpanByte> diskLogRecord, ref SpanByte input, ref SpanByteAndMemory output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                rmwCcCalled = true;
                base.RMWCompletionCallback(ref diskLogRecord, ref input, ref output, ctx, status, recordMetadata);
            }

            // Override the default SpanByteFunctions impelementation; for these tests, we always want the input length.
            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref SpanByte input)
                => new() { KeyDataSize = srcLogRecord.Key.Length, ValueDataSize = input.Length };
            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWInitialFieldInfo(SpanByte key, ref SpanByte input)
                => new() { KeyDataSize = key.Length, ValueDataSize = input.Length };
            /// <inheritdoc/>
            public override RecordFieldInfo GetUpsertFieldInfo(SpanByte key, SpanByte value, ref SpanByte input)
                => new() { KeyDataSize = key.Length, ValueDataSize = input.Length };
        }

        const int NumRecords = 200;

        RevivificationSpanByteFunctions functions;
        RevivificationSpanByteComparer comparer;

        private TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        private ClientSession<SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> session;
        private BasicContext<SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            CollisionRange collisionRange = CollisionRange.None;

            var kvSettings = new KVSettings()
            {
                IndexSize = 1L << 24,
                LogDevice = log,
                PageSize = 1L << 17,
                MemorySize = 1L << 20,
                MaxInlineValueSize = 1024,
                RevivificationSettings = RevivificationSettings.PowerOf2Bins
            };

            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is CollisionRange cr)
                {
                    collisionRange = cr;
                    continue;
                }
                if (arg is PendingOp)
                {
                    kvSettings.ReadCopyOptions = new(ReadCopyFrom.Device, ReadCopyTo.MainLog);
                    continue;
                }
                if (arg is RevivificationEnabled revivEnabled)
                {
                    if (revivEnabled == RevivificationEnabled.NoReviv)
                        kvSettings.RevivificationSettings = default;
                    continue;
                }
            }

            comparer = new RevivificationSpanByteComparer(collisionRange);
            store = new(kvSettings
                , StoreFunctions<SpanByte>.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            functions = new RevivificationSpanByteFunctions(store);
            session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions>(functions);
            bContext = session.BasicContext;
            functions.session = session;
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;

            DeleteDirectory(MethodTestDir);
        }

        void Populate() => Populate(0, NumRecords);

        void Populate(int from, int to)
        {
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromPinnedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);

            SpanByteAndMemory output = new();

            for (int ii = from; ii < to; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);
                functions.expectedValueLengths.Enqueue(input.Length);
                var status = bContext.Upsert(key, ref input, input, ref output);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
                ClassicAssert.IsEmpty(functions.expectedValueLengths);
            }
        }

        public enum Growth { None, Grow, Shrink };

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteNoRevivLengthTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] Growth growth)
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromPinnedSpan(keyVec);

            // Do NOT delete; this is a no-reviv test of lengths

            functions.expectedInputLength = growth switch
            {
                Growth.None => InitialLength,
                Growth.Grow => GrowLength,
                Growth.Shrink => ShrinkLength,
                _ => -1
            };

            Span<byte> inputVec = stackalloc byte[functions.expectedInputLength];
            var input = SpanByte.FromPinnedSpan(inputVec);
            inputVec.Fill(fillByte);

            // For Grow, we won't be able to satisfy the request with a revivification, and the new value length will be GrowLength
            functions.expectedValueLengths.Enqueue(InitialLength);
            if (growth == Growth.Grow)
                functions.expectedValueLengths.Enqueue(GrowLength);

            SpanByteAndMemory output = new();
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);

            ClassicAssert.IsEmpty(functions.expectedValueLengths);

            if (growth == Growth.Shrink)
            {
                inputVec = stackalloc byte[InitialLength / 2];  // Shrink this from InitialLength to ShrinkLength
                input = SpanByte.FromPinnedSpan(inputVec);
                inputVec.Fill(fillByte);

                functions.expectedInputLength = input.Length;
                functions.expectedValueLengths.Enqueue(input.Length);

                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);
                ClassicAssert.IsEmpty(functions.expectedValueLengths);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteSimpleTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromPinnedSpan(keyVec);

            functions.expectedValueLengths.Enqueue(InitialLength);
            var status = bContext.Delete(key);
            ClassicAssert.IsTrue(status.Found, status.ToString());

            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);
            inputVec.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedValueLengths.Enqueue(InitialLength);

            RevivificationTestUtils.WaitForRecords(store, want: true);

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteIPUGrowAndRevivifyTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromPinnedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[GrowLength];
            var input = SpanByte.FromPinnedSpan(inputVec);
            inputVec.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = GrowLength;

            functions.expectedValueLengths.Enqueue(InitialLength);
            functions.expectedValueLengths.Enqueue(GrowLength);

            // Get a free record from a failed IPU.
            if (updateOp == UpdateOp.Upsert)
            {
                var status = bContext.Upsert(key, ref input, input, ref output);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
            else if (updateOp == UpdateOp.RMW)
            {
                var status = bContext.RMW(key, ref input);
                ClassicAssert.IsTrue(status.Record.CopyUpdated, status.ToString());
            }

            ClassicAssert.Less(tailAddress, store.Log.TailAddress);
            ClassicAssert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
            output.Memory?.Dispose();
            output.Memory = null;
            tailAddress = store.Log.TailAddress;

            RevivificationTestUtils.WaitForRecords(store, want: true);

            // Get a new key and shrink the requested length so we revivify the free record from the failed IPU.
            keyVec.Fill(NumRecords + 1);
            input = SpanByte.FromPinnedSpan(inputVec.Slice(0, InitialLength));

            functions.expectedInputLength = InitialLength;
            functions.expectedValueLengths.Enqueue(InitialLength);

            if (updateOp == UpdateOp.Upsert)
            {
                var status = bContext.Upsert(key, ref input, input, ref output);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
            else if (updateOp == UpdateOp.RMW)
            {
                var status = bContext.RMW(key, ref input);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }

            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            ClassicAssert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store));
            output.Memory?.Dispose();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteReadOnlyMinAddressTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromPinnedSpan(keyVec);

            functions.expectedValueLengths.Enqueue(InitialLength);
            var status = bContext.Delete(key);
            ClassicAssert.IsTrue(status.Found, status.ToString());

            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);
            inputVec.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedValueLengths.Enqueue(InitialLength);

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);
            ClassicAssert.Greater(store.Log.TailAddress, tailAddress);
        }

        public enum UpdateKey { Unfound, DeletedAboveRO, DeletedBelowRO, CopiedBelowRO };

        const byte Unfound = NumRecords + 2;
        const byte DelBelowRO = NumRecords / 2 - 4;
        const byte CopiedBelowRO = NumRecords / 2 - 5;

        private long PrepareDeletes(bool stayInChain, byte delAboveRO, FlushMode flushMode, CollisionRange collisionRange)
        {
            Populate(0, NumRecords / 2);

            var pool = stayInChain ? RevivificationTestUtils.SwapFreeRecordPool(store, null) : null;

            // Delete key below (what will be) the readonly line. This is for a target for the test; the record should not be revivified.
            Span<byte> keyVecDelBelowRO = stackalloc byte[KeyLength];
            keyVecDelBelowRO.Fill(DelBelowRO);
            var delKeyBelowRO = SpanByte.FromPinnedSpan(keyVecDelBelowRO);

            functions.expectedValueLengths.Enqueue(InitialLength);
            var status = bContext.Delete(delKeyBelowRO);
            ClassicAssert.IsTrue(status.Found, status.ToString());

            if (flushMode == FlushMode.ReadOnly)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            else if (flushMode == FlushMode.OnDisk)
                store.Log.FlushAndEvict(wait: true);

            Populate(NumRecords / 2 + 1, NumRecords);

            var tailAddress = store.Log.TailAddress;

            // Delete key above the readonly line. This is the record that will be revivified.
            // If not stayInChain, this also puts two elements in the free list; one should be skipped over on Take() as it is below readonly.
            Span<byte> keyVecDelAboveRO = stackalloc byte[KeyLength];
            keyVecDelAboveRO.Fill(delAboveRO);
            var delKeyAboveRO = SpanByte.FromPinnedSpan(keyVecDelAboveRO);

            if (!stayInChain && collisionRange == CollisionRange.None)  // CollisionRange.Ten has a valid .PreviousAddress so won't be moved to FreeList
                RevivificationTestUtils.AssertElidable(store, delKeyAboveRO);

            functions.expectedValueLengths.Enqueue(InitialLength);
            status = bContext.Delete(delKeyAboveRO);
            ClassicAssert.IsTrue(status.Found, status.ToString());

            if (stayInChain)
            {
                ClassicAssert.IsFalse(RevivificationTestUtils.HasRecords(pool), "Expected empty pool");
                pool = RevivificationTestUtils.SwapFreeRecordPool(store, pool);
            }
            else if (collisionRange == CollisionRange.None)     // CollisionRange.Ten has a valid .PreviousAddress so won't be moved to FreeList
            {
                RevivificationTestUtils.WaitForRecords(store, want: true);
            }

            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            return tailAddress;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        //[Repeat(300)]
        public void SpanByteUpdateRevivifyTest([Values] DeleteDest deleteDest, [Values] UpdateKey updateKey,
                                          [Values] CollisionRange collisionRange, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            bool stayInChain = deleteDest == DeleteDest.InChain || collisionRange != CollisionRange.None;   // Collisions make the key inelidable

            byte delAboveRO = (byte)(NumRecords - (stayInChain
                ? (int)CollisionRange.Ten + 3       // Will remain in chain
                : 2));                              // Will be sent to free list

            long tailAddress = PrepareDeletes(stayInChain, delAboveRO, FlushMode.ReadOnly, collisionRange);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);

            SpanByteAndMemory output = new();

            Span<byte> keyVecToTest = stackalloc byte[KeyLength];
            var keyToTest = SpanByte.FromPinnedSpan(keyVecToTest);

            bool expectReviv;
            if (updateKey is UpdateKey.Unfound or UpdateKey.CopiedBelowRO)
            {
                // Unfound key should be satisfied from the freelist if !stayInChain, else will allocate a new record as it does not match the key chain.
                // CopiedBelowRO should be satisfied from the freelist if !stayInChain, else will allocate a new record as it does not match the key chain
                //      (but exercises a different code path than Unfound).
                // CollisionRange.Ten has a valid PreviousAddress so it is not elided from the cache.
                byte fillByte = updateKey == UpdateKey.Unfound ? Unfound : CopiedBelowRO;
                keyVecToTest.Fill(fillByte);
                inputVec.Fill(fillByte);
                expectReviv = !stayInChain && collisionRange != CollisionRange.Ten;
            }
            else if (updateKey == UpdateKey.DeletedBelowRO)
            {
                // DeletedBelowRO will not match the key for the in-chain above-RO slot, and we cannot reviv below RO or retrieve below-RO from the
                // freelist, so we will always allocate a new record unless we're using the freelist.
                byte fillByte = DelBelowRO;
                keyVecToTest.Fill(fillByte);
                inputVec.Fill(fillByte);
                expectReviv = !stayInChain && collisionRange != CollisionRange.Ten;
            }
            else if (updateKey == UpdateKey.DeletedAboveRO)
            {
                // DeletedAboveRO means we will reuse an in-chain record, or will get it from the freelist if deleteDest is FreeList.
                byte fillByte = delAboveRO;
                keyVecToTest.Fill(fillByte);
                inputVec.Fill(fillByte);
                expectReviv = true;
            }
            else
            {
                Assert.Fail($"Unexpected updateKey {updateKey}");
                expectReviv = false;    // make the compiler happy
            }

            functions.expectedInputLength = InitialLength;
            functions.expectedValueLengths.Enqueue(InitialLength);

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(keyToTest, ref input, input, ref output) : bContext.RMW(keyToTest, ref input);

            if (expectReviv)
                ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            else
                ClassicAssert.Greater(store.Log.TailAddress, tailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleRevivifyTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;
            if (stayInChain)
                _ = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            // This freed record stays in the hash chain.
            byte chainKey = NumRecords / 2 - 1;
            Span<byte> keyVec = stackalloc byte[KeyLength];
            keyVec.Fill(chainKey);
            var key = SpanByte.FromPinnedSpan(keyVec);
            if (!stayInChain)
                RevivificationTestUtils.AssertElidable(store, key);

            functions.expectedValueLengths.Enqueue(InitialLength);
            var status = bContext.Delete(key);
            ClassicAssert.IsTrue(status.Found, status.ToString());

            var tailAddress = store.Log.TailAddress;

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);
            inputVec.Fill(chainKey);

            SpanByteAndMemory output = new();

            if (!stayInChain)
                RevivificationTestUtils.WaitForRecords(store, want: true);

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            functions.expectedValueLengths.Enqueue(InitialLength);
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);

            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteEntireChainAndRevivifyTest([Values(CollisionRange.Ten)] CollisionRange collisionRange, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            // These freed records stay in the hash chain; we even skip the first one to ensure nothing goes into the free list.
            byte chainKey = 5;
            Span<byte> keyVec = stackalloc byte[KeyLength];
            keyVec.Fill(chainKey);
            var key = SpanByte.FromPinnedSpan(keyVec);
            var hash = comparer.GetHashCode64(key);

            List<byte> deletedSlots = [];
            for (int ii = chainKey + 1; ii < NumRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                if (comparer.GetHashCode64(key) != hash)
                    continue;

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, status.ToString());
                if (ii > RevivificationTestUtils.GetMinRevivifiableKey(store, NumRecords))
                    deletedSlots.Add((byte)ii);
            }

            // For this test we're still limiting to byte repetition
            ClassicAssert.Greater(255 - NumRecords, deletedSlots.Count);
            RevivificationTestUtils.WaitForRecords(store, want: false);
            ClassicAssert.IsFalse(RevivificationTestUtils.HasRecords(store), "Expected empty pool");
            ClassicAssert.Greater(deletedSlots.Count, 5);    // should be about Ten
            var tailAddress = store.Log.TailAddress;

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);
            inputVec.Fill(chainKey);

            SpanByteAndMemory output = new();

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            for (int ii = 0; ii < deletedSlots.Count; ++ii)
            {
                keyVec.Fill(deletedSlots[ii]);

                functions.expectedValueLengths.Enqueue(InitialLength);
                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);
                ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteAllRecordsAndRevivifyTest([Values(CollisionRange.None)] CollisionRange collisionRange, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            long tailAddress = store.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromPinnedSpan(keyVec);

            // "sizeof(int) +" because SpanByte has an int length prefix
            var recordSize = RecordInfo.GetLength() + RoundUp(sizeof(int) + keyVec.Length, 8) + RoundUp(sizeof(int) + InitialLength, 8);

            // Delete
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, status.ToString());
            }
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            ClassicAssert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords), RevivificationTestUtils.GetFreeRecordCount(store), $"Expected numRecords ({NumRecords}) free records");

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);

            SpanByteAndMemory output = new();

            // These come from the existing initial allocation so keep the full length
            functions.expectedInputLength = InitialLength;

            // Revivify
            var revivifiableKeyCount = RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords);
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);
                if (ii < revivifiableKeyCount)
                    ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress, $"unexpected new record for key {ii}");
                else
                    ClassicAssert.Less(tailAddress, store.Log.TailAddress, $"unexpected revivified record for key {ii}");

                var status = bContext.Read(key, ref output);
                ClassicAssert.IsTrue(status.Found, $"Expected to find key {ii}; status == {status}");
            }

            ClassicAssert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store), "expected no free records remaining");
            RevivificationTestUtils.WaitForRecords(store, want: false);

            // Confirm
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                var status = bContext.Read(key, ref output);
                ClassicAssert.IsTrue(status.Found, $"Expected to find key {ii}; status == {status}");
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteAllRecordsAndTakeSnapshotTest()
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromPinnedSpan(keyVec);

            // Delete
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, status.ToString());
            }
            ClassicAssert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords), RevivificationTestUtils.GetFreeRecordCount(store), $"Expected numRecords ({NumRecords}) free records");

            _ = store.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).GetAwaiter().GetResult();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteAllRecordsAndIterateTest()
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromPinnedSpan(keyVec);

            // Delete
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                RevivificationTestUtils.AssertElidable(store, key);

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, status.ToString());
            }
            ClassicAssert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords), RevivificationTestUtils.GetFreeRecordCount(store), $"Expected numRecords ({NumRecords}) free records");

            using var iterator = session.Iterate();
            while (iterator.GetNext())
                ;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void BinSelectionTest()
        {
            var pool = store.RevivificationManager.FreeRecordPool;
            int expectedBin = 0, recordSize = RevivificationTestUtils.GetMaxRecordSize(pool, expectedBin);
            while (true)
            {
                ClassicAssert.IsTrue(pool.GetBinIndex(recordSize - 1, out int actualBin));
                ClassicAssert.AreEqual(expectedBin, actualBin);
                ClassicAssert.IsTrue(pool.GetBinIndex(recordSize, out actualBin));
                ClassicAssert.AreEqual(expectedBin, actualBin);

                if (++expectedBin == RevivificationTestUtils.GetBinCount(pool))
                {
                    ClassicAssert.IsFalse(pool.GetBinIndex(recordSize + 1, out actualBin));
                    ClassicAssert.AreEqual(-1, actualBin);
                    break;
                }
                ClassicAssert.IsTrue(pool.GetBinIndex(recordSize + 1, out actualBin));
                ClassicAssert.AreEqual(expectedBin, actualBin);
                recordSize = RevivificationTestUtils.GetMaxRecordSize(pool, expectedBin);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        //[Repeat(3000)]
        public unsafe void LiveBinWrappingTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] WaitMode waitMode, [Values] DeleteDest deleteDest)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            Populate();

            // Note: this test assumes no collisions (every delete goes to the FreeList)

            var pool = store.RevivificationManager.FreeRecordPool;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromPinnedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);

            // "sizeof(int) +" because SpanByte has an int length prefix.
            var recordSize = RecordInfo.GetLength() + RoundUp(sizeof(int) + keyVec.Length, 8) + RoundUp(sizeof(int) + InitialLength, 8);
            ClassicAssert.IsTrue(pool.GetBinIndex(recordSize, out int binIndex));
            ClassicAssert.AreEqual(3, binIndex);

            // We should have a recordSize > min size record in the bin, to test wrapping.
            ClassicAssert.AreNotEqual(0, RevivificationTestUtils.GetSegmentStart(pool, binIndex, recordSize), "SegmentStart should not be 0, to test wrapping");

            // Delete 
            functions.expectedInputLength = InitialLength;
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, $"{status} for key {ii}");
                //ClassicAssert.AreEqual(ii + 1, RevivificationTestUtils.GetFreeRecordCount(store), $"mismatched free record count for key {ii}, pt 1");
            }

            if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait)
            {
                var actualNumRecords = RevivificationTestUtils.GetFreeRecordCount(store);
                ClassicAssert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords), actualNumRecords, $"mismatched free record count");
            }

            // Revivify
            functions.expectedInputLength = InitialLength;
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                long tailAddress = store.Log.TailAddress;

                SpanByteAndMemory output = new();
                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);
                output.Memory?.Dispose();

                if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait && tailAddress != store.Log.TailAddress)
                {
                    var expectedReviv = ii < RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords);
                    if (expectedReviv != (tailAddress == store.Log.TailAddress))
                    {
                        var freeRecs = RevivificationTestUtils.GetFreeRecordCount(store);
                        if (expectedReviv)
                            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress, $"failed to revivify record for key {ii}, freeRecs {freeRecs}");
                        else
                            ClassicAssert.Less(tailAddress, store.Log.TailAddress, $"Unexpectedly revivified record for key {ii}, freeRecs {freeRecs}");
                    }
                }
            }

            if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait)
            {
                ClassicAssert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store), "expected no free records remaining");
                RevivificationTestUtils.WaitForRecords(store, want: false);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void LiveBinWrappingNoRevivTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values(RevivificationEnabled.NoReviv)] RevivificationEnabled revivEnabled)
        {
            // For a comparison to the reviv version above.
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromPinnedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);

            for (var iter = 0; iter < 100; ++iter)
            {
                // Delete 
                functions.expectedInputLength = InitialLength;
                for (var ii = 0; ii < NumRecords; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    inputVec.Fill((byte)ii);

                    functions.expectedValueLengths.Enqueue(iter == 0 ? InitialLength : InitialLength);
                    var status = bContext.Delete(key);
                    ClassicAssert.IsTrue(status.Found, $"{status} for key {ii}, iter {iter}");
                }

                for (var ii = 0; ii < NumRecords; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    inputVec.Fill((byte)ii);

                    functions.expectedValueLengths.Enqueue(InitialLength);

                    SpanByteAndMemory output = new();
                    _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);
                    output.Memory?.Dispose();
                }
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleOversizeRevivifyTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;

            // Both in and out of chain revivification of oversize should have the same lengths.
            if (stayInChain)
                _ = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            byte chainKey = NumRecords + 1;
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromPinnedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[OversizeLength];
            var input = SpanByte.FromPinnedSpan(inputVec);

            SpanByteAndMemory output = new();

            keyVec.Fill(chainKey);
            inputVec.Fill(chainKey);

            // Oversize records in this test do not go to "next higher" bin (there is no next-higher bin in the default PowersOf2 bins we use)
            // and they become an out-of-line pointer.
            functions.expectedInputLength = OversizeLength;
            functions.expectedValueLengths.Enqueue(SpanField.OverflowInlineSize);

            // Initial insert of the oversize record
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);

            // Delete it
            functions.expectedValueLengths.Enqueue(OversizeLength);
            var status = bContext.Delete(key);
            ClassicAssert.IsTrue(status.Found, status.ToString());
            if (!stayInChain)
                RevivificationTestUtils.WaitForRecords(store, want: true);

            var tailAddress = store.Log.TailAddress;

            // Revivify in the chain. Because this is oversize, the expectedFullValueLength remains the same
            functions.expectedValueLengths.Enqueue(OversizeLength);
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input, ref output) : bContext.RMW(key, ref input);

            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
        }

        public enum PendingOp { Read, RMW };

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimplePendingOpsRevivifyTest([Values(CollisionRange.None)] CollisionRange collisionRange, [Values] PendingOp pendingOp)
        {
            byte delAboveRO = NumRecords - 2;   // Will be sent to free list
            byte targetRO = NumRecords / 2 - 15;

            long tailAddress = PrepareDeletes(stayInChain: false, delAboveRO, FlushMode.OnDisk, collisionRange);

            // We always want freelist for this test.
            var pool = store.RevivificationManager.FreeRecordPool;
            ClassicAssert.IsTrue(RevivificationTestUtils.HasRecords(pool));

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;

            // Use a different key below RO than we deleted; this will go pending to retrieve it
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromPinnedSpan(keyVec);

            if (pendingOp == PendingOp.Read)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromPinnedSpan(inputVec);

                keyVec.Fill(targetRO);
                inputVec.Fill(targetRO);

                functions.expectedInputLength = InitialLength;

                var spanSlice = inputVec[..InitialLength];
                var inputSlice = SpanByte.FromPinnedSpan(spanSlice);

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Read(key, ref inputSlice, ref output);
                ClassicAssert.IsTrue(status.IsPending, status.ToString());
                _ = bContext.CompletePending(wait: true);
                ClassicAssert.IsTrue(functions.readCcCalled);
            }
            else if (pendingOp == PendingOp.RMW)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromPinnedSpan(inputVec);

                keyVec.Fill(targetRO);
                inputVec.Fill(targetRO);

                functions.expectedValueLengths.Enqueue(InitialLength);

                _ = bContext.RMW(key, ref input);
                _ = bContext.CompletePending(wait: true);
                ClassicAssert.IsTrue(functions.rmwCcCalled);
            }
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
        }
    }

#if LOGRECORD_TODO
    [TestFixture]
    class RevivificationObjectTests
    {
        const int NumRecords = 1000;
        internal const int ValueMult = 1_000_000;

        private MyFunctions functions;
        private TsavoriteKV<MyKey, MyValue, ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions, ClassStoreFunctions, ClassAllocator> bContext;
        private IDevice log;
        private IDevice objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                MemorySize = 1L << 22,
                PageSize = 1L << 12,
                RevivificationSettings = RevivificationSettings.DefaultFixedLength
            }, StoreFunctions<MyKey, MyValue>.Create(new MyKey.Comparer(), () => new MyKeySerializer(), () => new MyValueSerializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            functions = new MyFunctions();
            session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(functions);
            bContext = session.BasicContext;
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            for (int key = 0; key < NumRecords; key++)
            {
                var keyObj = new MyKey { key = key };
                var valueObj = new MyValue { value = key + ValueMult };
                var status = bContext.Upsert(keyObj, valueObj);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleObjectTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var deleteKey = RevivificationTestUtils.GetMinRevivifiableKey(store, NumRecords);
            var tailAddress = store.Log.TailAddress;
            _ = bContext.Delete(new MyKey { key = deleteKey });
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : NumRecords + 1;

            var key = new MyKey { key = updateKey };
            var value = new MyValue { value = key.key + ValueMult };
            var input = new MyInput { value = value.value };

            RevivificationTestUtils.WaitForRecords(store, want: true);
            ClassicAssert.IsTrue(RevivificationTestUtils.HasRecords(store.RevivificationManager.FreeRecordPool), "Expected a free record after delete and WaitForRecords");

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, value) : bContext.RMW(key, input);

            RevivificationTestUtils.WaitForRecords(store, want: false);
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress, "Expected tail address not to grow (record was revivified)");
        }
    }
#endif // LOGRECORD_TODO

    [TestFixture]
    class RevivificationSpanByteStressTests
    {
        const int KeyLength = 10;
        const int InitialLength = 50;

        internal class RevivificationStressFunctions : SpanByteFunctions<Empty>
        {
            internal IKeyComparer keyComparer;                          // non-null if we are doing key comparisons (and thus expectedKey is non-default)
            internal SpanByte expectedKey = default;                    // Set for each operation by the calling thread
            internal bool isFirstLap = true;                            // For first 

            internal RevivificationStressFunctions(IKeyComparer keyComparer) => this.keyComparer = keyComparer;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void VerifyKey(SpanByte functionsKey)
            {
                if (keyComparer is not null)
                    ClassicAssert.IsTrue(keyComparer.Equals(expectedKey, functionsKey));
            }

            private void VerifyKeyAndValue(SpanByte functionsKey, SpanByte functionsValue)
            {
                if (keyComparer is not null)
                    ClassicAssert.IsTrue(keyComparer.Equals(expectedKey, functionsKey), "functionsKey does not equal expectedKey");

                // Even in CompletePending(), we can verify internal consistency of key/value
                int valueOffset = 0, valueLengthRemaining = functionsValue.Length;
                ClassicAssert.Less(functionsKey.Length, valueLengthRemaining);
                while (valueLengthRemaining > 0)
                {
                    var compareLength = Math.Min(functionsKey.Length, valueLengthRemaining);
                    Span<byte> valueSpan = functionsValue.AsSpan().Slice(valueOffset, compareLength);
                    Span<byte> keySpan = functionsKey.AsSpan()[..compareLength];
                    ClassicAssert.IsTrue(valueSpan.SequenceEqual(keySpan), $"functionsValue (offset {valueOffset}, len {compareLength}: {SpanByte.FromPinnedSpan(valueSpan)}) does not match functionsKey ({SpanByte.FromPinnedSpan(keySpan)})");
                    valueOffset += compareLength;
                    valueLengthRemaining -= compareLength;
                }
            }

            public override bool SingleWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                VerifyKey(logRecord.Key);
                return base.SingleWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo, reason);
            }

            public override bool ConcurrentWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, SpanByte srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                VerifyKeyAndValue(logRecord.Key, srcValue);
                return base.ConcurrentWriter(ref logRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }

            public override bool InitialUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKey(logRecord.Key);
                return logRecord.TrySetValueSpan(input, ref sizeInfo);
            }

            public override bool CopyUpdater<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKeyAndValue(srcLogRecord.Key, srcLogRecord.ValueSpan);
                return dstLogRecord.TrySetValueSpan(srcLogRecord.ValueSpan, ref sizeInfo);
            }

            public override bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref SpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKeyAndValue(logRecord.Key, logRecord.ValueSpan);
                return logRecord.TrySetValueSpan(input, ref sizeInfo);
            }

            public override bool SingleDeleter(ref LogRecord<SpanByte> logRecord, ref DeleteInfo deleteInfo)
                => base.SingleDeleter(ref logRecord, ref deleteInfo);

            public override unsafe bool ConcurrentDeleter(ref LogRecord<SpanByte> logRecord, ref DeleteInfo deleteInfo)
                => base.ConcurrentDeleter(ref logRecord, ref deleteInfo);
        }

        const int NumRecords = 200;
        const int DefaultMaxRecsPerBin = 1024;

        RevivificationStressFunctions functions;
        RevivificationSpanByteComparer comparer;

        private TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        private ClientSession<SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> session;
        private BasicContext<SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            CollisionRange collisionRange = CollisionRange.None;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is CollisionRange cr)
                {
                    collisionRange = cr;
                    continue;
                }
            }

            comparer = new RevivificationSpanByteComparer(collisionRange);
            store = new(new()
            {
                IndexSize = 1L << 24,
                LogDevice = log,
                PageSize = 1L << 17,
                MemorySize = 1L << 20,
                RevivificationSettings = RevivificationSettings.PowerOf2Bins
            }, StoreFunctions<SpanByte>.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            functions = new RevivificationStressFunctions(keyComparer: null);
            session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(functions);
            bContext = session.BasicContext;
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;

            DeleteDirectory(MethodTestDir);
        }

        unsafe void Populate()
        {
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromPinnedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromPinnedSpan(inputVec);

            SpanByteAndMemory output = new();

            for (int ii = 0; ii < NumRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                var status = bContext.Upsert(key, ref input, input, ref output);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        const int AddressIncrement = 1_000_000; // must be > ReadOnlyAddress

#if LOGRECORD_TODO  // Artificial bins - switch to SpanByte to ensure First/Best fit are tested
        [Test]
        [Category(RevivificationCategory)]
        [TestCase(20, 1, 1)]
        [TestCase(20, 5, 10)]
        [TestCase(20, 10, 5)]
        [TestCase(20, 10, 10)]
        //[Repeat(100)]
        public void ArtificialFreeBinThreadStressTest(int numIterations, int numAddThreads, int numTakeThreads)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");
            const int numRecordsPerThread = 1000;
            const int recordSize = 48;    // size doesn't matter in this test, but must be a multiple of 8
            int maxRecords = numRecordsPerThread * numAddThreads;
            int numTotalThreads = numAddThreads + numTakeThreads;
            const long Unadded = 0;
            const long Added = 1;
            const long RemovedBase = 10000; // tid will be added to this to help diagnose any failures

            // For this test we are bypassing the FreeRecordPool in store.
            var binDef = new RevivificationBin()
            {
                RecordSize = recordSize,
                NumberOfRecords = maxRecords
            };
            var flags = new long[maxRecords];
            List<int> strayFlags = [], strayRecords = [];

            using var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(store, binDef);

            int iteration = 0;
            int totalTaken = 0;

            void beginIteration()
            {
                totalTaken = 0;
                for (var ii = 0; ii < flags.Length; ++ii)
                    flags[ii] = 0;
                strayFlags.Clear();
                strayRecords.Clear();
            }

            void enumerateStrayFlags()
            {
                for (var ii = 0; ii < flags.Length; ++ii)
                {
                    // We should have added and removed all addresses from 0 -> MaxRecords
                    if (flags[ii] < RemovedBase)
                        strayFlags.Add(ii);
                }
            }

            unsafe void enumerateStrayRecords()
            {
                var bin = freeRecordPool.bins[0];
                for (var ii = 0; ii < bin.recordCount; ++ii)
                {
                    if ((bin.records + ii)->IsSet)
                        strayRecords.Add(ii);
                }
            }

            void endIteration()
            {
                enumerateStrayFlags();
                enumerateStrayRecords();
                ClassicAssert.IsTrue(strayFlags.Count == 0 && strayRecords.Count == 0 && maxRecords == totalTaken,
                              $"maxRec/taken {maxRecords}/{totalTaken}, strayflags {strayFlags.Count}, strayRecords {strayRecords.Count}, iteration {iteration}");
            }

            void runAddThread(int tid)
            {
                RevivificationStats revivStats = new();
                for (var ii = 0; ii < numRecordsPerThread; ++ii)
                {
                    var addressBase = ii + tid * numRecordsPerThread;
                    var flag = flags[addressBase];
                    ClassicAssert.AreEqual(Unadded, flag, $"Invalid flag {flag} trying to add addressBase {addressBase}, tid {tid}, iteration {iteration}");
                    flags[addressBase] = 1;
                    ClassicAssert.IsTrue(freeRecordPool.TryAdd(addressBase + AddressIncrement, recordSize, ref revivStats), $"Failed to add addressBase {addressBase}, tid {tid}, iteration {iteration}");
                }
            }

            void runTakeThread(int tid)
            {
                RevivificationStats revivStats = new();
                while (totalTaken < maxRecords)
                {
                    if (freeRecordPool.bins[0].TryTake(recordSize, 0, store, out long address, ref revivStats))
                    {
                        var addressBase = address - AddressIncrement;
                        var prevFlag = Interlocked.CompareExchange(ref flags[addressBase], RemovedBase + tid, Added);
                        ClassicAssert.AreEqual(1, prevFlag, $"Take() found unexpected addressBase {addressBase} (flag {prevFlag}), tid {tid}, iteration {iteration}");
                        _ = Interlocked.Increment(ref totalTaken);
                    }
                }
            }

            // Task rather than Thread for propagation of exception.
            List<Task> tasks = [];

            // Make iteration 1-based to make the termination check easier in the threadprocs
            for (iteration = 1; iteration <= numIterations; ++iteration)
            {
                Debug.WriteLine($"Beginning iteration {iteration}");
                beginIteration();

                for (int t = 0; t < numAddThreads; t++)
                {
                    var tid = t;    // Use 0 for the first TID
                    tasks.Add(Task.Factory.StartNew(() => runAddThread(tid)));
                }
                for (int t = 0; t < numTakeThreads; t++)
                {
                    var tid = t + numAddThreads;
                    tasks.Add(Task.Factory.StartNew(() => runTakeThread(tid)));
                }

                try
                {
                    var timeoutSec = 5;     // 5s per iteration should be plenty
                    ClassicAssert.IsTrue(Task.WaitAll([.. tasks], TimeSpan.FromSeconds(timeoutSec)), $"Task timeout at {timeoutSec} sec, maxRec/taken {maxRecords}/{totalTaken}, iteration {iteration}");
                    endIteration();
                }
                finally
                {
                    // This tests runs multiple iterations so can create many tasks, so dispose them at the end of each iteration.
                    foreach (var task in tasks)
                    {
                        // A non-completed task throws an exception on Dispose(), which masks the initial exception.
                        // If it's not completed when we get here, the test has already failed.
                        if (task.IsCompleted)
                            task.Dispose();
                    }
                    tasks.Clear();
                }
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialSimpleTest()
        {
            var binDef = new RevivificationBin()
            {
                RecordSize = TakeSize + 8,
                NumberOfRecords = 64,
                BestFitScanLimit = RevivificationBin.UseFirstFit
            };
            var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(store, binDef);

            RevivificationStats revivStats = new();
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(AddressIncrement + 1, TakeSize, ref revivStats));
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress: AddressIncrement, out var address, ref revivStats));

            ClassicAssert.AreEqual(AddressIncrement + 1, address, "out address");
            ClassicAssert.AreEqual(1, revivStats.successfulAdds, "Successful Adds");
            ClassicAssert.AreEqual(1, revivStats.successfulTakes, "Successful Takes");
            _ = revivStats.Dump();
        }

        public enum WrapMode { Wrap, NoWrap };
        const int TakeSize = 40;

        private FreeRecordPool<SpanByte, SpanByte, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> CreateBestFitTestPool(int scanLimit, WrapMode wrapMode, ref RevivificationStats revivStats)
        {
            var binDef = new RevivificationBin()
            {
                RecordSize = TakeSize + 8,
                NumberOfRecords = 64,
                BestFitScanLimit = scanLimit
            };
            var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(store, binDef);

            const int minAddress = AddressIncrement - 10;
            int expectedAdds = 0, expectedTakes = 0;
            if (wrapMode == WrapMode.Wrap)
            {
                // Add too-small records to wrap around the end of the bin records. Use lower addresses so we don't mix up the "real" results.
                const int smallSize = TakeSize - 4;
                for (var ii = 0; ii < freeRecordPool.bins[0].recordCount - 2; ++ii, ++expectedAdds)
                    ClassicAssert.IsTrue(freeRecordPool.TryAdd(minAddress + ii + 1, smallSize, ref revivStats));

                // Now take out the four at the beginning.
                for (var ii = 0; ii < 4; ++ii, ++expectedTakes)
                    ClassicAssert.IsTrue(freeRecordPool.TryTake(smallSize, minAddress, out _, ref revivStats));
            }

            long address = AddressIncrement;
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize + 1, ref revivStats));
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize + 2, ref revivStats));
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize + 3, ref revivStats));
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize, ref revivStats));    // 4
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize, ref revivStats));    // 5 
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize, ref revivStats));
            expectedAdds += 6;

            ClassicAssert.AreEqual(expectedAdds, revivStats.successfulAdds, "Successful Adds");
            ClassicAssert.AreEqual(expectedTakes, revivStats.successfulTakes, "Successful Takes");

            return freeRecordPool;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialBestFitTest([Values] WrapMode wrapMode)
        {
            // We should first Take the first 20-length due to exact fit, then skip over the empty to take the next 20, then we have
            // no exact fit within the scan limit, so we grab the best fit before that (21).
            RevivificationStats revivStats = new();
            using var freeRecordPool = CreateBestFitTestPool(scanLimit: 4, wrapMode, ref revivStats);
            var expectedTakes = revivStats.successfulTakes;
            var minAddress = AddressIncrement;
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out var address, ref revivStats));
            ClassicAssert.AreEqual(4, address -= AddressIncrement);
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(5, address -= AddressIncrement);
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(1, address -= AddressIncrement);

            // Now that we've taken the first item, the new first-fit will be moved up one, which brings the last exact-fit into scanLimit range.
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(6, address -= AddressIncrement);

            // Now Take will return them in order until we have no more
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(2, address -= AddressIncrement);
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(3, address -= AddressIncrement);
            ClassicAssert.IsFalse(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            expectedTakes += 6; // Plus one failure

            ClassicAssert.AreEqual(expectedTakes, revivStats.successfulTakes, "Successful Takes");
            ClassicAssert.AreEqual(1, revivStats.failedTakes, "Failed Takes");
            var statsString = revivStats.Dump();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialFirstFitTest([Values] WrapMode wrapMode)
        {
            // We should Take the addresses in order.
            RevivificationStats revivStats = new();
            using var freeRecordPool = CreateBestFitTestPool(scanLimit: RevivificationBin.UseFirstFit, wrapMode, ref revivStats);
            var expectedSuccessfulTakes = revivStats.successfulTakes;
            var expectedFailedTakes = revivStats.failedTakes;
            var minAddress = AddressIncrement;

            long address = -1;
            for (var ii = 0; ii < 6; ++ii, ++expectedSuccessfulTakes)
            {
                if (!freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats))
                    Assert.Fail($"Take failed at ii {ii}: pool.HasRecords {RevivificationTestUtils.HasRecords(freeRecordPool)}");
                ClassicAssert.AreEqual(ii + 1, address -= AddressIncrement, $"address comparison failed at ii {ii}");
            }
            ClassicAssert.IsFalse(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));

            ClassicAssert.AreEqual(expectedSuccessfulTakes, revivStats.successfulTakes, "Successful Takes");
            ClassicAssert.AreEqual(1, revivStats.failedTakes, "Failed Takes");
            if (wrapMode == WrapMode.NoWrap)
                ClassicAssert.AreEqual(1, revivStats.takeEmptyBins, "Empty Bins");
            else
                ClassicAssert.AreEqual(1, revivStats.takeRecordSizeFailures, "Record Size");
            var statsString = revivStats.Dump();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public unsafe void ArtificialThreadContentionOnOneRecordTest()
        {
            var binDef = new RevivificationBin()
            {
                RecordSize = 32,
                NumberOfRecords = 32
            };
            var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(store, binDef);
            const long TestAddress = AddressIncrement, minAddress = AddressIncrement - 10;
            long counter = 0, globalAddress = 0;
            const int size = 20;
            const int numIterations = 10000;

            unsafe void runThread(int tid)
            {
                RevivificationStats revivStats = new();
                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    if (freeRecordPool.TryTake(size, minAddress, out long address, ref revivStats))
                    {
                        ++counter;
                    }
                    else if (globalAddress == TestAddress && Interlocked.CompareExchange(ref globalAddress, 0, TestAddress) == TestAddress)
                    {
                        ClassicAssert.IsTrue(freeRecordPool.TryAdd(TestAddress, size, ref revivStats), $"Failed TryAdd on iter {iteration}");
                        ++counter;
                    }
                }
            }

            List<Task> tasks = [];   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < 8; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runThread(tid)));
            }
            Task.WaitAll([.. tasks]);

            ClassicAssert.IsTrue(counter == 0);
        }
#endif // LOGRECORD_TODO  // Artificial bins - switch to SpanByte to ensure First/Best fit are tested

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(3000)]
        public void LiveThreadContentionOnOneRecordTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            const int numIterations = 2000;
            const int numDeleteThreads = 5, numUpdateThreads = 5;
            const int keyRange = numDeleteThreads;

            unsafe void runDeleteThread(int tid)
            {
                Random rng = new(tid * 101);

                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(new RevivificationStressFunctions(keyComparer: null));
                var localbContext = localSession.BasicContext;

                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromPinnedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numDeleteThreads)
                    {
                        var kk = rng.Next(keyRange);
                        keyVec.Fill((byte)kk);
                        _ = localbContext.Delete(key);
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromPinnedSpan(keyVec);

                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromPinnedSpan(inputVec);

                Random rng = new(tid * 101);

                RevivificationStressFunctions localFunctions = new(keyComparer: comparer);
                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(localFunctions);
                var localbContext = localSession.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numUpdateThreads)
                    {
                        var kk = rng.Next(keyRange);
                        keyVec.Fill((byte)kk);
                        inputVec.Fill((byte)kk);

                        localSession.functions.expectedKey = key;
                        _ = updateOp == UpdateOp.Upsert ? localbContext.Upsert(key, input) : localbContext.RMW(key, input);
                        localSession.functions.expectedKey = default;
                    }

                    // Clear keyComparer so it does not try to validate during CompletePending (when it doesn't have an expectedKey)
                    localFunctions.keyComparer = null;
                    _ = localbContext.CompletePending(wait: true);
                    localFunctions.keyComparer = comparer;
                }
            }

            List<Task> tasks = [];   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numDeleteThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runDeleteThread(tid)));
            }
            for (int t = 0; t < numUpdateThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll([.. tasks]);
        }

        public enum ThreadingPattern { SameKeys, RandomKeys };

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(3000)]
        public void LiveFreeListThreadStressTest([Values] CollisionRange collisionRange,
                                             [Values] ThreadingPattern threadingPattern, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            int numIterations = 100;
            const int numDeleteThreads = 5, numUpdateThreads = 5;

            unsafe void runDeleteThread(int tid)
            {
                Random rng = new(tid * 101);

                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(new RevivificationStressFunctions(keyComparer: null));
                var localbContext = localSession.BasicContext;

                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromPinnedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numDeleteThreads)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(NumRecords) : ii;
                        keyVec.Fill((byte)kk);
                        _ = localbContext.Delete(key);
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromPinnedSpan(keyVec);

                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromPinnedSpan(inputVec);

                Random rng = new(tid * 101);

                RevivificationStressFunctions localFunctions = new(keyComparer: comparer);
                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(localFunctions);
                var localbContext = localSession.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numUpdateThreads)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(NumRecords) : ii;
                        keyVec.Fill((byte)kk);
                        inputVec.Fill((byte)kk);

                        localSession.functions.expectedKey = key;
                        _ = updateOp == UpdateOp.Upsert ? localbContext.Upsert(key, input) : localbContext.RMW(key, input);
                        localSession.functions.expectedKey = default;
                    }

                    // Clear keyComparer so it does not try to validate during CompletePending (when it doesn't have an expectedKey)
                    localFunctions.keyComparer = null;
                    _ = localbContext.CompletePending(wait: true);
                    localFunctions.keyComparer = comparer;
                }
            }

            List<Task> tasks = [];   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numDeleteThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runDeleteThread(tid)));
            }
            for (int t = 0; t < numUpdateThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll([.. tasks]);
        }

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(30)]
        public void LiveInChainThreadStressTest([Values(CollisionRange.Ten)] CollisionRange collisionRange, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            // Turn off freelist.
            _ = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            const int numIterations = 500;
            const int numDeleteThreads = 5, numUpdateThreads = 5;

            unsafe void runDeleteThread(int tid)
            {
                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(new RevivificationStressFunctions(keyComparer: null));
                var localbContext = localSession.BasicContext;

                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromPinnedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numDeleteThreads)
                    {
                        keyVec.Fill((byte)ii);
                        _ = localbContext.Delete(key);
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromPinnedSpan(keyVec);

                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromPinnedSpan(inputVec);

                RevivificationStressFunctions localFunctions = new RevivificationStressFunctions(keyComparer: null);
                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(localFunctions);
                var localbContext = localSession.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numUpdateThreads)
                    {
                        keyVec.Fill((byte)ii);
                        inputVec.Fill((byte)ii);

                        localSession.functions.expectedKey = key;
                        _ = updateOp == UpdateOp.Upsert ? localbContext.Upsert(key, input) : localbContext.RMW(key, input);
                        localSession.functions.expectedKey = default;
                    }

                    // Clear keyComparer so it does not try to validate during CompletePending (when it doesn't have an expectedKey)
                    localFunctions.keyComparer = null;
                    _ = localbContext.CompletePending(wait: true);
                    localFunctions.keyComparer = comparer;
                }
            }

            List<Task> tasks = [];   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numDeleteThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runDeleteThread(tid)));
            }
            for (int t = 0; t < numUpdateThreads; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runUpdateThread(tid)));
            }
            Task.WaitAll([.. tasks]);
        }
    }
}