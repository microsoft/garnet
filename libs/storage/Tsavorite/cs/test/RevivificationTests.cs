// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

        public bool Equals(ReadOnlySpan<byte> k1, ReadOnlySpan<byte> k2) => defaultComparer.Equals(k1, k2);

        // The hash code ends with 0 so mod Ten isn't so helpful, so shift
        public long GetHashCode64(ReadOnlySpan<byte> k) => (defaultComparer.GetHashCode64(k) >> 4) % collisionRange;
    }
}

namespace Tsavorite.test.Revivification
{
    using static VarbyteLengthUtility;

    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordDisposer>;

    using LongAllocator = SpanByteAllocator<StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>>;
    using LongStoreFunctions = StoreFunctions<LongKeyComparer, SpanByteRecordDisposer>;

    using SpanByteStoreFunctions = StoreFunctions<RevivificationSpanByteComparer, SpanByteRecordDisposer>;

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
                    RecordSize = RoundUp(RecordInfo.Size + 2 * (sizeof(int) + sizeof(long)), Constants.kRecordAlignment), // We have "fixed length" for these integer bins, with long Key and Value
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
            => HasRecords(store.RevivificationManager.freeRecordPool);

        internal static bool HasRecords<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, FreeRecordPool<TStoreFunctions, TAllocator> pool)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => HasRecords(pool ?? store.RevivificationManager.freeRecordPool);

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
            var pool = store.RevivificationManager.freeRecordPool;
            store.RevivificationManager.freeRecordPool = inPool;
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

        internal static bool TryTakeFromBin<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool, int binIndex, in RecordSizeInfo sizeInfo, long minAddress,
                TsavoriteKV<TStoreFunctions, TAllocator> store, out long address, ref RevivificationStats revivStats)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => pool.bins[binIndex].TryTake(sizeInfo.ActualInlineRecordSize, minAddress, store, out address, ref revivStats);

        internal static int GetSegmentStart<TStoreFunctions, TAllocator>(FreeRecordPool<TStoreFunctions, TAllocator> pool, int binIndex, int recordSize)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => pool.bins[binIndex].GetSegmentStart(recordSize);

        internal static void WaitForRecords<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, bool want, FreeRecordPool<TStoreFunctions, TAllocator> pool = default)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            pool ??= store.RevivificationManager.freeRecordPool;

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

        internal static int GetFreeRecordCount<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => GetFreeRecordCount(store.RevivificationManager.freeRecordPool);

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

        internal static void AssertElidable<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, ReadOnlySpan<byte> key)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
        {
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(store.storeFunctions.GetKeyHashCode64(key));
            ClassicAssert.IsTrue(store.FindTag(ref stackCtx.hei), $"AssertElidable: Cannot find ii {key.ToShortString()}");
            var recordInfo = LogRecord.GetInfo(store.hlogBase.GetPhysicalAddress(stackCtx.hei.Address));
            ClassicAssert.Less(recordInfo.PreviousAddress, store.hlogBase.BeginAddress, "AssertElidable: expected elidable ii");
        }

        internal static int GetRevivifiableRecordCount<TStoreFunctions, TAllocator>(TsavoriteKV<TStoreFunctions, TAllocator> store, int numRecords)
            where TStoreFunctions : IStoreFunctions
            where TAllocator : IAllocator<TStoreFunctions>
            => (int)(numRecords * store.RevivificationManager.revivifiableFraction);  // Add extra for rounding issues

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
        private ClientSession<long, long, Empty, RevivificationFixedLenFunctions, LongStoreFunctions, LongAllocator> session;
        private BasicContext<long, long, Empty, RevivificationFixedLenFunctions, LongStoreFunctions, LongAllocator> bContext;
        private IDevice log;

        private int recordSize;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            // Records all have a Span<byte> corresponding to a 'long' ii and value, which means one length byte.
            recordSize = RoundUp(RecordInfo.Size + NumIndicatorBytes + 2 + sizeof(long) * 2, Constants.kRecordAlignment);

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
            }, StoreFunctions.Create(LongKeyComparer.Instance, SpanByteRecordDisposer.Instance)
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
                var status = bContext.Upsert(SpanByte.FromPinnedVariable(ref keyNum), SpanByte.FromPinnedVariable(ref valueNum));
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

            long deleteKeyNum = RevivificationTestUtils.GetMinRevivifiableKey(store, NumRecords) + 2;       // +2 to allow for page headers and rounding
            var deleteKey = SpanByte.FromPinnedVariable(ref deleteKeyNum);
            if (!stayInChain)
                RevivificationTestUtils.AssertElidable(store, deleteKey);
            var tailAddress = store.Log.TailAddress;

            _ = bContext.Delete(deleteKey);
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);

            long updateKeyNum = deleteDest == DeleteDest.InChain ? deleteKeyNum : NumRecords + 1;
            var updateValueNum = updateKeyNum + ValueMult;
            Span<byte> updateKey = SpanByte.FromPinnedVariable(ref updateKeyNum), updateValue = SpanByte.FromPinnedVariable(ref updateValueNum);

            if (!stayInChain)
            {
                ClassicAssert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
                RevivificationTestUtils.WaitForRecords(store, want: true);
            }

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(updateKey, updateValue) : bContext.RMW(updateKey, ref updateValueNum);

            if (!stayInChain)
                RevivificationTestUtils.WaitForRecords(store, want: false);
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress, "Expected tail address not to grow (recordPtr was revivified)");
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
                _ = bContext.Delete(SpanByte.FromPinnedVariable(ref keyNum));
                ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            }

            // The NumberOfRecords will be adjusted upward so the partition is cache-line aligned, so this may be higher than specified.
            ClassicAssert.LessOrEqual(RevivificationBin.DefaultRecordsPerBin, RevivificationTestUtils.GetFreeRecordCount(store));
            RevivificationTestUtils.WaitForRecords(store, want: true);

            // Now re-add the keys.
            for (long keyNum = 0; keyNum < NumRecords; ++keyNum)
            {
                long valueNum = keyNum + ValueMult;
                Span<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum);
                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, value) : bContext.RMW(key, ref valueNum);
            }

            // Now re-add the keys. For the elision case, we should see tailAddress grow sharply as only the records in the bin are available
            // for revivification. For In-Chain, we will revivify records that were unelided after the bin overflowed. But we have some records
            // ineligible for revivification due to revivifiableFraction.
            var numIneligibleRecords = NumRecords - RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords);
            var noElisionExpectedTailAddress = tailAddress + numIneligibleRecords * recordSize;

            if (elision == RecordElision.NoElide)   // Add 4 to account for page headers and rounding
                ClassicAssert.GreaterOrEqual(noElisionExpectedTailAddress + 4 * recordSize, store.Log.TailAddress, "Expected tail address not to grow (records were revivified)");
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
            ClassicAssert.IsTrue(bContext.Delete(SpanByte.FromPinnedVariable(ref keyNum)).Found);
            ClassicAssert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store));

            // This should go to FreeList because it's above the RevivifiableFraction
            keyNum = NumRecords - 1;
            ClassicAssert.IsTrue(bContext.Delete(SpanByte.FromPinnedVariable(ref keyNum)).Found);
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
            ClassicAssert.IsTrue(bContext.Delete(SpanByte.FromPinnedVariable(ref keyNum)).Found);
            ClassicAssert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
            RevivificationTestUtils.WaitForRecords(store, want: true);

            // Detach the pool temporarily so the records aren't revivified by the next insertions.
            var pool = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            // Now add a bunch of records to drop the FreeListed address below the RevivifiableFraction
            long maxRecord = NumRecords * 2, valueNum;
            for (keyNum = NumRecords; keyNum < maxRecord; keyNum++)
            {
                valueNum = keyNum * ValueMult;
                var status = bContext.Upsert(SpanByte.FromPinnedVariable(ref keyNum), SpanByte.FromPinnedVariable(ref valueNum));
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }

            // Restore the pool
            _ = RevivificationTestUtils.SwapFreeRecordPool(store, pool);

            var tailAddress = store.Log.TailAddress;
            valueNum = maxRecord * ValueMult;
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(SpanByte.FromPinnedVariable(ref maxRecord), SpanByte.FromPinnedVariable(ref valueNum)) : bContext.RMW(SpanByte.FromPinnedVariable(ref maxRecord), ref valueNum);

            ClassicAssert.Less(tailAddress, store.Log.TailAddress, "Expected tail address to grow (recordPtr was not revivified)");
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
            internal ClientSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> session;

            internal int expectedInputLength = InitialLength;

            // used to configurably change RMW behavior to test tombstoning via RMW route.
            internal bool deleteInIpu = false;
            internal bool deleteInNCU = false;
            internal bool deleteInCU = false;
            internal bool forceSkipIpu = false;

            // This is a queue rather than a single value because there may be calls to, for example, InPlaceWriter with one length
            // followed by InitialWriter with another.
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

            private static void VerifyKeyAndValue(ReadOnlySpan<byte> functionsKey, ReadOnlySpan<byte> functionsValue)
            {
                int valueOffset = 0, valueLengthRemaining = functionsValue.Length;
                ClassicAssert.Less(functionsKey.Length, valueLengthRemaining);
                while (valueLengthRemaining > 0)
                {
                    var compareLength = Math.Min(functionsKey.Length, valueLengthRemaining);
                    var valueSpan = functionsValue.Slice(valueOffset, compareLength);
                    var keySpan = functionsKey[..compareLength];
                    ClassicAssert.IsTrue(valueSpan.SequenceEqual(keySpan), $"functionsValue (offset {valueOffset}, len {compareLength}: {valueSpan.ToShortString()}) does not match functionsKey ({keySpan.ToShortString()})");
                    valueLengthRemaining -= compareLength;
                }
            }

            unsafe void CheckExpectedLengthsBefore(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, long recordAddress, bool isIPU = false)
            {
                var expectedValueLength = expectedValueLengths.Dequeue();

                // If an overflow logRecord is from new recordPtr creation it has not had its overflow set yet; it has just been initialized to inline length of ObjectIdMap.ObjectIdSize,
                // and we'll call LogField.ConvertToOverflow later in this ISessionFunctions call to do the actual overflow allocation.
                if (!logRecord.Info.ValueIsInline || (sizeInfo.IsSet && !sizeInfo.ValueIsInline))
                {
                    var (valueLength, _ /*valueAddress*/) = new RecordDataHeader((byte*)logRecord.DataHeaderAddress).GetValueFieldInfo(logRecord.Info);
                    ClassicAssert.AreEqual(ObjectIdMap.ObjectIdSize, (int)valueLength);
                }
                if (sizeInfo.ValueIsInline)
                    ClassicAssert.AreEqual(expectedValueLength, logRecord.ValueSpan.Length);
                else
                    ClassicAssert.AreEqual(logRecord.Info.ValueIsInline ? expectedValueLength : ObjectIdMap.ObjectIdSize, logRecord.ValueSpan.Length);

                ClassicAssert.GreaterOrEqual(recordAddress, store.hlogBase.ReadOnlyAddress);

                // !IsSet means it is from Delete which does not receive a RecordSizeInfo. isIPU is an in-place update and thus the new value may legitimately be larger than the recordPtr.
                if (sizeInfo.IsSet && !isIPU)
                {
                    var allocated = logRecord.AllocatedSize;
                    var actual = logRecord.ActualSize;
                    ClassicAssert.AreEqual(sizeInfo.ActualInlineRecordSize, actual);
                    ClassicAssert.AreEqual(sizeInfo.AllocatedInlineRecordSize, allocated);
                }
            }

            public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                CheckExpectedLengthsBefore(ref logRecord, in sizeInfo, upsertInfo.Address);
                return base.InitialWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }

            public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                CheckExpectedLengthsBefore(ref logRecord, in sizeInfo, upsertInfo.Address, isIPU: true);
                return base.InPlaceWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }

            public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                if (deleteInNCU)
                {
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;
                }
                return base.NeedCopyUpdate(in srcLogRecord, ref input, ref output, ref rmwInfo);
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                if (deleteInCU)
                {
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;
                }

                AssertInfoValid(ref rmwInfo);
                ClassicAssert.AreEqual(expectedInputLength, input.Length);

                CheckExpectedLengthsBefore(ref dstLogRecord, in sizeInfo, rmwInfo.Address);
                return dstLogRecord.TrySetValueSpanAndPrepareOptionals(input.ReadOnlySpan, in sizeInfo);
            }

            public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);

                if (forceSkipIpu)
                    return false;

                if (deleteInIpu)
                {
                    rmwInfo.Action = RMWAction.ExpireAndStop;
                    return false;
                }

                ClassicAssert.AreEqual(expectedInputLength, input.Length);

                CheckExpectedLengthsBefore(ref logRecord, in sizeInfo, rmwInfo.Address, isIPU: true);
                VerifyKeyAndValue(logRecord.Key, logRecord.ValueSpan);

                return logRecord.TrySetValueSpanAndPrepareOptionals(input.ReadOnlySpan, in sizeInfo);
            }

            public override bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);

                RecordSizeInfo sizeInfo = default;
                CheckExpectedLengthsBefore(ref logRecord, in sizeInfo, deleteInfo.Address);

                return base.InitialDeleter(ref logRecord, ref deleteInfo);
            }

            public override bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);

                RecordSizeInfo sizeInfo = default;
                CheckExpectedLengthsBefore(ref logRecord, in sizeInfo, deleteInfo.Address);

                return base.InPlaceDeleter(ref logRecord, ref deleteInfo);
            }

            public override bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                return base.PostCopyUpdater(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
            }

            public override void PostInitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                base.PostInitialUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
            }

            public override void PostInitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                AssertInfoValid(ref upsertInfo);
                base.PostInitialWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }

            public override void PostInitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);
                base.PostInitialDeleter(ref logRecord, ref deleteInfo);
            }

            public override void ReadCompletionCallback(ref DiskLogRecord diskLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                readCcCalled = true;
                base.ReadCompletionCallback(ref diskLogRecord, ref input, ref output, ctx, status, recordMetadata);
            }

            public override void RMWCompletionCallback(ref DiskLogRecord diskLogRecord, ref PinnedSpanByte input, ref SpanByteAndMemory output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                rmwCcCalled = true;
                base.RMWCompletionCallback(ref diskLogRecord, ref input, ref output, ctx, status, recordMetadata);
            }

            // Override the default SpanByteFunctions impelementation; for these tests, we always want the input length.
            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWModifiedFieldInfo<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref PinnedSpanByte input)
                => new() { KeySize = srcLogRecord.Key.Length, ValueSize = input.Length };
            /// <inheritdoc/>
            public override RecordFieldInfo GetRMWInitialFieldInfo(ReadOnlySpan<byte> key, ref PinnedSpanByte input)
                => new() { KeySize = key.Length, ValueSize = input.Length };
            /// <inheritdoc/>
            public override RecordFieldInfo GetUpsertFieldInfo(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref PinnedSpanByte input)
                => new() { KeySize = key.Length, ValueSize = input.Length };
        }

        const int NumRecords = 200;

        RevivificationSpanByteFunctions functions;
        RevivificationSpanByteComparer comparer;

        private TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        private ClientSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> session;
        private BasicContext<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext;
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
                , StoreFunctions.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            functions = new RevivificationSpanByteFunctions(store);
            session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions>(functions);
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
            Span<byte> key = stackalloc byte[KeyLength];
            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

            SpanByteAndMemory output = new();

            for (int ii = from; ii < to; ++ii)
            {
                key.Fill((byte)ii);
                input.Fill((byte)ii);
                functions.expectedValueLengths.Enqueue(pinnedInputSpan.Length);
                var status = bContext.Upsert(key, ref pinnedInputSpan, input, ref output);
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

            Span<byte> key = stackalloc byte[KeyLength];
            byte fillByte = 42;
            key.Fill(fillByte);

            // Do NOT delete; this is a no-reviv test of lengths

            functions.expectedInputLength = growth switch
            {
                Growth.None => InitialLength,
                Growth.Grow => GrowLength,
                Growth.Shrink => ShrinkLength,
                _ => -1
            };

            Span<byte> input = stackalloc byte[functions.expectedInputLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);
            input.Fill(fillByte);

            // For Grow, we won't be able to satisfy the request with a revivification, and the new value length will be GrowLength
            functions.expectedValueLengths.Enqueue(InitialLength);
            if (growth == Growth.Grow)
                functions.expectedValueLengths.Enqueue(GrowLength);

            SpanByteAndMemory output = new();
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);

            ClassicAssert.IsEmpty(functions.expectedValueLengths);

            if (growth == Growth.Shrink)
            {
                input = stackalloc byte[InitialLength / 2];  // Shrink this from InitialLength to ShrinkLength
                input.Fill(fillByte);

                functions.expectedInputLength = input.Length;
                functions.expectedValueLengths.Enqueue(input.Length);

                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);
                ClassicAssert.IsEmpty(functions.expectedValueLengths);
            }
        }

        internal enum DeletionRoutes
        {
            DELETE,
            RMW_IPU,
            RMW_NCU,
            RMW_CU
        }

        private Status DeleteViaRMW(ReadOnlySpan<byte> key, Span<byte> mockInputVec, byte fillByte)
        {
            var mockInput = PinnedSpanByte.FromPinnedSpan(mockInputVec);
            mockInputVec.Fill(fillByte);
            return bContext.RMW(key, ref mockInput);
        }

        private Status PerformDeletion(DeletionRoutes deletionRoute, ReadOnlySpan<byte> key, byte fillByte)
        {
            Status status;
            switch (deletionRoute)
            {
                case DeletionRoutes.DELETE:
                    return bContext.Delete(key);
                case DeletionRoutes.RMW_IPU:
                    functions.deleteInIpu = true;
                    Span<byte> mockInputVec = stackalloc byte[InitialLength];
                    status = DeleteViaRMW(key, mockInputVec, fillByte);
                    functions.deleteInIpu = false;
                    break;
                case DeletionRoutes.RMW_NCU:
                    functions.deleteInNCU = true;
                    mockInputVec = stackalloc byte[InitialLength];
                    status = DeleteViaRMW(key, mockInputVec, fillByte);
                    functions.deleteInNCU = false;
                    break;
                case DeletionRoutes.RMW_CU:
                    functions.deleteInCU = true;
                    mockInputVec = stackalloc byte[InitialLength];
                    status = DeleteViaRMW(key, mockInputVec, fillByte);
                    functions.deleteInCU = false;
                    break;
                default:
                    throw new Exception("Unhandled deletion logic");
            }

            return status;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteSimpleTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values(DeletionRoutes.DELETE, DeletionRoutes.RMW_IPU)] DeletionRoutes deletionRoute)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            Span<byte> key = stackalloc byte[KeyLength];
            byte fillByte = 42;
            key.Fill(fillByte);

            functions.expectedValueLengths.Enqueue(InitialLength);
            Status status = PerformDeletion(deletionRoute, key, fillByte);

            //if (deletionRoute == DeletionRoutes.DELETE)
            ClassicAssert.IsTrue(status.Found, status.ToString());
            //else
            //    ClassicAssert.IsTrue(status.NotFound && status.ShouldExpire, status.ToString());

            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);

            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);
            input.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedValueLengths.Enqueue(InitialLength);

            RevivificationTestUtils.WaitForRecords(store, want: true);

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteDeletionViaRMWRCURevivifiesOriginalRecordAfterTombstoning(
            [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values(DeletionRoutes.RMW_NCU, DeletionRoutes.RMW_CU)] DeletionRoutes deletionRoute)
        {
            Populate();

            Span<byte> key = stackalloc byte[KeyLength];
            byte fillByte = 42;
            key.Fill(fillByte);

            functions.expectedValueLengths.Enqueue(InitialLength);

            functions.forceSkipIpu = true;
            var status = PerformDeletion(deletionRoute, key, fillByte);
            functions.forceSkipIpu = false;

            RevivificationTestUtils.WaitForRecords(store, want: true);

            ClassicAssert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));

            //ClassicAssert.IsTrue(status.NotFound && status.ShouldExpire, status.ToString());
            ClassicAssert.IsTrue(status.Found && status.IsExpired, status.ToString());

            var tailAddress = store.Log.TailAddress;

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = PinnedSpanByte.FromPinnedSpan(inputVec);
            inputVec.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedValueLengths.Enqueue(InitialLength);

            // brand new value so we try to use a recordPtr out of free list
            fillByte = 255;
            key.Fill(fillByte);

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref input, input.ReadOnlySpan, ref output) : bContext.RMW(key, ref input);

            // since above would use revivification free list we should see no change of tail address.
            ClassicAssert.AreEqual(store.Log.TailAddress, tailAddress);
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            ClassicAssert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store));
            output.Memory?.Dispose();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SpanByteIPUGrowAndRevivifyTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            Span<byte> key = stackalloc byte[KeyLength];
            byte fillByte = 42;
            key.Fill(fillByte);

            Span<byte> input = stackalloc byte[GrowLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);
            input.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = GrowLength;

            functions.expectedValueLengths.Enqueue(InitialLength);
            functions.expectedValueLengths.Enqueue(GrowLength);

            // Get a free recordPtr from a failed IPU.
            if (updateOp == UpdateOp.Upsert)
            {
                var status = bContext.Upsert(key, ref pinnedInputSpan, input, ref output);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
            else if (updateOp == UpdateOp.RMW)
            {
                var status = bContext.RMW(key, ref pinnedInputSpan);
                ClassicAssert.IsTrue(status.Record.CopyUpdated, status.ToString());
            }

            ClassicAssert.Less(tailAddress, store.Log.TailAddress);
            ClassicAssert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
            output.Memory?.Dispose();
            output.Memory = null;
            tailAddress = store.Log.TailAddress;

            RevivificationTestUtils.WaitForRecords(store, want: true);

            // Get a new ii and shrink the requested length so we revivify the free recordPtr from the failed IPU.
            key.Fill(NumRecords + 1);
            input = input.Slice(0, InitialLength);
            pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

            functions.expectedInputLength = InitialLength;
            functions.expectedValueLengths.Enqueue(InitialLength);

            if (updateOp == UpdateOp.Upsert)
            {
                var status = bContext.Upsert(key, ref pinnedInputSpan, input, ref output);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
            else if (updateOp == UpdateOp.RMW)
            {
                var status = bContext.RMW(key, ref pinnedInputSpan);
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

            Span<byte> key = stackalloc byte[KeyLength];
            byte fillByte = 42;
            key.Fill(fillByte);

            functions.expectedValueLengths.Enqueue(InitialLength);
            var status = bContext.Delete(key);
            ClassicAssert.IsTrue(status.Found, status.ToString());

            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);
            input.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedValueLengths.Enqueue(InitialLength);

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);
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

            // Delete ii below (what will be) the readonly line. This is for a target for the test; the recordPtr should not be revivified.
            Span<byte> delKeyBelowRO = stackalloc byte[KeyLength];
            delKeyBelowRO.Fill(DelBelowRO);

            functions.expectedValueLengths.Enqueue(InitialLength);
            var status = bContext.Delete(delKeyBelowRO);
            ClassicAssert.IsTrue(status.Found, status.ToString());

            if (flushMode == FlushMode.ReadOnly)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            else if (flushMode == FlushMode.OnDisk)
                store.Log.FlushAndEvict(wait: true);

            Populate(NumRecords / 2 + 1, NumRecords);

            var tailAddress = store.Log.TailAddress;

            // Delete ii above the readonly line. This is the recordPtr that will be revivified.
            // If not stayInChain, this also puts two elements in the free list; one should be skipped over on Take() as it is below readonly.
            Span<byte> delKeyAboveRO = stackalloc byte[KeyLength];
            delKeyAboveRO.Fill(delAboveRO);

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

            bool stayInChain = deleteDest == DeleteDest.InChain || collisionRange != CollisionRange.None;   // Collisions make the ii inelidable

            byte delAboveRO = (byte)(NumRecords - (stayInChain
                ? (int)CollisionRange.Ten + 3       // Will remain in chain
                : 2));                              // Will be sent to free list

            long tailAddress = PrepareDeletes(stayInChain, delAboveRO, FlushMode.ReadOnly, collisionRange);

            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

            SpanByteAndMemory output = new();

            Span<byte> keyToTest = stackalloc byte[KeyLength];

            bool expectReviv;
            if (updateKey is UpdateKey.Unfound or UpdateKey.CopiedBelowRO)
            {
                // Unfound ii should be satisfied from the freelist if !stayInChain, else will allocate a new recordPtr as it does not match the ii chain.
                // CopiedBelowRO should be satisfied from the freelist if !stayInChain, else will allocate a new recordPtr as it does not match the ii chain
                //      (but exercises a different code path than Unfound).
                // CollisionRange.Ten has a valid PreviousAddress so it is not elided from the cache.
                byte fillByte = updateKey == UpdateKey.Unfound ? Unfound : CopiedBelowRO;
                keyToTest.Fill(fillByte);
                input.Fill(fillByte);
                expectReviv = !stayInChain && collisionRange != CollisionRange.Ten;
            }
            else if (updateKey == UpdateKey.DeletedBelowRO)
            {
                // DeletedBelowRO will not match the ii for the in-chain above-RO slot, and we cannot reviv below RO or retrieve below-RO from the
                // freelist, so we will always allocate a new recordPtr unless we're using the freelist.
                byte fillByte = DelBelowRO;
                keyToTest.Fill(fillByte);
                input.Fill(fillByte);
                expectReviv = !stayInChain && collisionRange != CollisionRange.Ten;
            }
            else if (updateKey == UpdateKey.DeletedAboveRO)
            {
                // DeletedAboveRO means we will reuse an in-chain recordPtr, or will get it from the freelist if deleteDest is FreeList.
                byte fillByte = delAboveRO;
                keyToTest.Fill(fillByte);
                input.Fill(fillByte);
                expectReviv = true;
            }
            else
            {
                Assert.Fail($"Unexpected updateKey {updateKey}");
                expectReviv = false;    // make the compiler happy
            }

            functions.expectedInputLength = InitialLength;
            functions.expectedValueLengths.Enqueue(InitialLength);

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(keyToTest, ref pinnedInputSpan, input, ref output) : bContext.RMW(keyToTest, ref pinnedInputSpan);

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

            // This freed recordPtr stays in the hash chain.
            byte chainKey = NumRecords / 2 - 1;
            Span<byte> key = stackalloc byte[KeyLength];
            key.Fill(chainKey);
            if (!stayInChain)
                RevivificationTestUtils.AssertElidable(store, key);

            functions.expectedValueLengths.Enqueue(InitialLength);
            var status = bContext.Delete(key);
            ClassicAssert.IsTrue(status.Found, status.ToString());

            var tailAddress = store.Log.TailAddress;

            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);
            input.Fill(chainKey);

            SpanByteAndMemory output = new();

            if (!stayInChain)
                RevivificationTestUtils.WaitForRecords(store, want: true);

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            functions.expectedValueLengths.Enqueue(InitialLength);
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);

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
            Span<byte> key = stackalloc byte[KeyLength];
            key.Fill(chainKey);
            var hash = comparer.GetHashCode64(key);

            List<byte> deletedSlots = [];
            for (int ii = chainKey + 1; ii < NumRecords; ++ii)
            {
                key.Fill((byte)ii);
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

            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);
            input.Fill(chainKey);

            SpanByteAndMemory output = new();

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            for (int ii = 0; ii < deletedSlots.Count; ++ii)
            {
                key.Fill(deletedSlots[ii]);

                functions.expectedValueLengths.Enqueue(InitialLength);
                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);
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

            Span<byte> key = stackalloc byte[KeyLength];

            // Delete
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                key.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, status.ToString());
            }
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
            ClassicAssert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords), RevivificationTestUtils.GetFreeRecordCount(store), $"Expected numRecords ({NumRecords}) free records");

            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

            SpanByteAndMemory output = new();

            // These come from the existing initial allocation so keep the full length
            functions.expectedInputLength = InitialLength;

            // Revivify
            var revivifiableKeyCount = RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords);
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                key.Fill((byte)ii);
                input.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);
                if (ii < revivifiableKeyCount)
                    ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress, $"unexpected new recordPtr for ii {ii}");
                else
                    ClassicAssert.Less(tailAddress, store.Log.TailAddress, $"unexpected revivified recordPtr for ii {ii}");

                var status = bContext.Read(key, ref output);
                ClassicAssert.IsTrue(status.Found, $"Expected to find ii {ii}; status == {status}");
            }

            ClassicAssert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store), "expected no free records remaining");
            RevivificationTestUtils.WaitForRecords(store, want: false);

            // Confirm
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                key.Fill((byte)ii);
                var status = bContext.Read(key, ref output);
                ClassicAssert.IsTrue(status.Found, $"Expected to find ii {ii}; status == {status}");
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteAllRecordsAndTakeSnapshotTest()
        {
            Populate();

            Span<byte> key = stackalloc byte[KeyLength];

            // Delete
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                key.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, status.ToString());
            }
            ClassicAssert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords), RevivificationTestUtils.GetFreeRecordCount(store), $"Expected numRecords ({NumRecords}) free records");

#pragma warning disable CA2012 // Use ValueTasks correctly
            _ = store.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).GetAwaiter().GetResult();
#pragma warning restore CA2012 // Use ValueTasks correctly
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void DeleteAllRecordsAndIterateTest()
        {
            Populate();

            Span<byte> key = stackalloc byte[KeyLength];

            // Delete
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                key.Fill((byte)ii);

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
            var pool = store.RevivificationManager.freeRecordPool;
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
        public void LiveBinWrappingTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] WaitMode waitMode, [Values] DeleteDest deleteDest)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            Populate();

            // Note: this test assumes no collisions (every delete goes to the FreeList)

            var pool = store.RevivificationManager.freeRecordPool;

            Span<byte> key = stackalloc byte[KeyLength];
            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

            // "sizeof(int) +" because SpanByte has an int length prefix.
            var recordSize = RecordInfo.Size + RoundUp(sizeof(int) + key.Length, Constants.kRecordAlignment) + RoundUp(sizeof(int) + InitialLength, Constants.kRecordAlignment);
            ClassicAssert.IsTrue(pool.GetBinIndex(recordSize, out int binIndex));
            ClassicAssert.AreEqual(3, binIndex);

            // We should have a addRecordSize > min smallSize recordPtr in the bin, to test wrapping.
            ClassicAssert.AreNotEqual(0, RevivificationTestUtils.GetSegmentStart(pool, binIndex, recordSize), "SegmentStart should not be 0, to test wrapping");

            // Delete 
            functions.expectedInputLength = InitialLength;
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                key.Fill((byte)ii);
                input.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Delete(key);
                ClassicAssert.IsTrue(status.Found, $"{status} for ii {ii}");
                //ClassicAssert.AreEqual(ii + 1, RevivificationTestUtils.GetFreeRecordCount(store), $"mismatched free recordPtr count for ii {ii}, pt 1");
            }

            if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait)
            {
                var actualNumRecords = RevivificationTestUtils.GetFreeRecordCount(store);
                ClassicAssert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords), actualNumRecords, $"mismatched free recordPtr count");
            }

            // Revivify
            functions.expectedInputLength = InitialLength;
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                key.Fill((byte)ii);
                input.Fill((byte)ii);

                functions.expectedValueLengths.Enqueue(InitialLength);
                long tailAddress = store.Log.TailAddress;

                SpanByteAndMemory output = new();
                _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);
                output.Memory?.Dispose();

                if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait && tailAddress != store.Log.TailAddress)
                {
                    var expectedReviv = ii < RevivificationTestUtils.GetRevivifiableRecordCount(store, NumRecords);
                    if (expectedReviv != (tailAddress == store.Log.TailAddress))
                    {
                        var freeRecs = RevivificationTestUtils.GetFreeRecordCount(store);
                        if (expectedReviv)
                            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress, $"failed to revivify recordPtr for ii {ii}, freeRecs {freeRecs}");
                        else
                            ClassicAssert.Less(tailAddress, store.Log.TailAddress, $"Unexpectedly revivified recordPtr for ii {ii}, freeRecs {freeRecs}");
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

            Span<byte> key = stackalloc byte[KeyLength];
            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

            for (var iter = 0; iter < 100; ++iter)
            {
                // Delete 
                functions.expectedInputLength = InitialLength;
                for (var ii = 0; ii < NumRecords; ++ii)
                {
                    key.Fill((byte)ii);
                    input.Fill((byte)ii);

                    functions.expectedValueLengths.Enqueue(iter == 0 ? InitialLength : InitialLength);

                    var status = bContext.Delete(key);
                    ClassicAssert.IsTrue(status.Found, $"{status} for ii {ii}, iter {iter}");
                }

                for (var ii = 0; ii < NumRecords; ++ii)
                {
                    key.Fill((byte)ii);
                    input.Fill((byte)ii);

                    functions.expectedValueLengths.Enqueue(InitialLength);

                    SpanByteAndMemory output = new();
                    _ = updateOp == UpdateOp.Upsert
                        ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output)
                        : bContext.RMW(key, ref pinnedInputSpan);
                    output.Dispose();
                }
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void SimpleOverflowRevivifyTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            Assert.Ignore("Test ignored because SpanByteAllocatore currently does not support overflow.");
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;

            // Both in and out of chain revivification of oversize should have the same lengths.
            if (stayInChain)
                _ = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            byte chainKey = NumRecords + 1;
            Span<byte> key = stackalloc byte[KeyLength];

            Span<byte> input = stackalloc byte[OversizeLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

            SpanByteAndMemory output = new();

            key.Fill(chainKey);
            input.Fill(chainKey);

            // Oversize records in this test do not go to "next higher" bin (there is no next-higher bin in the default PowersOf2 bins we use)
            // and they become an out-of-line pointer.
            functions.expectedInputLength = OversizeLength;
            functions.expectedValueLengths.Enqueue(ObjectIdMap.ObjectIdSize);

            // Initial insert of the oversize recordPtr
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);

            // Delete it
            functions.expectedValueLengths.Enqueue(OversizeLength);
            var status = bContext.Delete(key);
            ClassicAssert.IsTrue(status.Found, status.ToString());
            if (!stayInChain)
                RevivificationTestUtils.WaitForRecords(store, want: true);

            var tailAddress = store.Log.TailAddress;

            // Revivify in the chain. Because this is oversize, the expectedFullValueLength remains the same
            functions.expectedValueLengths.Enqueue(OversizeLength);
            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(key, ref pinnedInputSpan, input, ref output) : bContext.RMW(key, ref pinnedInputSpan);

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
            var pool = store.RevivificationManager.freeRecordPool;
            ClassicAssert.IsTrue(RevivificationTestUtils.HasRecords(pool));

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;

            // Use a different ii below RO than we deleted; this will go pending to retrieve it
            Span<byte> key = stackalloc byte[KeyLength];

            if (pendingOp == PendingOp.Read)
            {
                Span<byte> input = stackalloc byte[InitialLength];

                key.Fill(targetRO);
                input.Fill(targetRO);

                functions.expectedInputLength = InitialLength;

                var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input[..InitialLength]);

                functions.expectedValueLengths.Enqueue(InitialLength);
                var status = bContext.Read(key, ref pinnedInputSpan, ref output);
                ClassicAssert.IsTrue(status.IsPending, status.ToString());
                _ = bContext.CompletePending(wait: true);
                ClassicAssert.IsTrue(functions.readCcCalled);
            }
            else if (pendingOp == PendingOp.RMW)
            {
                Span<byte> input = stackalloc byte[InitialLength];
                var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

                key.Fill(targetRO);
                input.Fill(targetRO);

                functions.expectedValueLengths.Enqueue(InitialLength);

                _ = bContext.RMW(key, ref pinnedInputSpan);
                _ = bContext.CompletePending(wait: true);
                ClassicAssert.IsTrue(functions.rmwCcCalled);
            }
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);
        }
    }

    [TestFixture]
    class RevivificationObjectTests
    {
        const int NumRecords = 1000;
        internal const int ValueMult = 1_000_000;

        private TestObjectFunctions functions;
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private ClientSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> session;
        private BasicContext<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions, ClassStoreFunctions, ClassAllocator> bContext;
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
                RevivificationSettings = RevivificationSettings.PowerOf2Bins
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer())
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            functions = new TestObjectFunctions();
            session = store.NewSession<TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(functions);
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
            for (int ii = 0; ii < NumRecords; ii++)
            {
                var key = new TestObjectKey { key = ii };
                var valueObj = new TestObjectValue { value = ii + ValueMult };
                var status = bContext.Upsert(SpanByte.FromPinnedVariable(ref key), valueObj);
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
            var key = new TestObjectKey { key = deleteKey };
            _ = bContext.Delete(SpanByte.FromPinnedVariable(ref key));
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress);

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : NumRecords + 1;

            key = new TestObjectKey { key = updateKey };
            var value = new TestObjectValue { value = key.key + ValueMult };
            var input = new TestObjectInput { value = value.value };

            RevivificationTestUtils.WaitForRecords(store, want: true);
            ClassicAssert.IsTrue(RevivificationTestUtils.HasRecords(store.RevivificationManager.freeRecordPool), "Expected a free recordPtr after delete and WaitForRecords");

            _ = updateOp == UpdateOp.Upsert ? bContext.Upsert(SpanByte.FromPinnedVariable(ref key), value) : bContext.RMW(SpanByte.FromPinnedVariable(ref key), ref input);

            RevivificationTestUtils.WaitForRecords(store, want: false);
            ClassicAssert.AreEqual(tailAddress, store.Log.TailAddress, "Expected tail address not to grow (recordPtr was revivified)");
        }
    }

    [TestFixture]
    class RevivificationSpanByteStressTests
    {
        const int KeyLength = 10;
        const int InitialLength = 50;

        internal class RevivificationStressFunctions : SpanByteFunctions<Empty>
        {
            internal IKeyComparer keyComparer;                          // non-null if we are doing ii comparisons (and thus expectedKey is non-default)
            internal PinnedSpanByte expectedKey = default;              // Set for each operation by the calling thread
            internal bool isFirstLap = true;                            // For first 

            internal RevivificationStressFunctions(IKeyComparer keyComparer) => this.keyComparer = keyComparer;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void VerifyKey(ReadOnlySpan<byte> functionsKey)
            {
                if (keyComparer is not null)
                    ClassicAssert.IsTrue(keyComparer.Equals(expectedKey.ReadOnlySpan, functionsKey));
            }

            private void VerifyKeyAndValue(ReadOnlySpan<byte> functionsKey, ReadOnlySpan<byte> functionsValue)
            {
                if (keyComparer is not null)
                    ClassicAssert.IsTrue(keyComparer.Equals(expectedKey.ReadOnlySpan, functionsKey), "functionsKey does not equal expectedKey");

                // Even in CompletePending(), we can verify internal consistency of ii/value
                int valueOffset = 0, valueLengthRemaining = functionsValue.Length;
                ClassicAssert.Less(functionsKey.Length, valueLengthRemaining);
                while (valueLengthRemaining > 0)
                {
                    var compareLength = Math.Min(functionsKey.Length, valueLengthRemaining);
                    var valueSpan = functionsValue.Slice(valueOffset, compareLength);
                    var keySpan = functionsKey[..compareLength];
                    ClassicAssert.IsTrue(valueSpan.SequenceEqual(keySpan), $"functionsValue (offset {valueOffset}, len {compareLength}: {valueSpan.ToShortString()}) does not match functionsKey ({keySpan.ToShortString()})");
                    valueOffset += compareLength;
                    valueLengthRemaining -= compareLength;
                }
            }

            public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                VerifyKey(logRecord.Key);
                return base.InitialWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }

            public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ReadOnlySpan<byte> srcValue, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo)
            {
                VerifyKeyAndValue(logRecord.Key, srcValue);
                return base.InPlaceWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }

            public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKey(logRecord.Key);
                return logRecord.TrySetValueSpanAndPrepareOptionals(input.ReadOnlySpan, in sizeInfo);
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKeyAndValue(srcLogRecord.Key, srcLogRecord.ValueSpan);
                return dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcLogRecord.ValueSpan, in sizeInfo);
            }

            public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref PinnedSpanByte input, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                VerifyKeyAndValue(logRecord.Key, logRecord.ValueSpan);
                return logRecord.TrySetValueSpanAndPrepareOptionals(input.ReadOnlySpan, in sizeInfo);
            }

            public override bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
                => base.InitialDeleter(ref logRecord, ref deleteInfo);

            public override bool InPlaceDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
                => base.InPlaceDeleter(ref logRecord, ref deleteInfo);
        }

        const int NumRecords = 200;
        const int DefaultMaxRecsPerBin = 1024;

        RevivificationStressFunctions functions;
        RevivificationSpanByteComparer comparer;

        private TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;
        private ClientSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> session;
        private BasicContext<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions, SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> bContext;
        private IDevice log;
        private ArtificialFreeBinAllocator artificialFreeBinAllocator;

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

            if (TestContext.CurrentContext.Test.Name.StartsWith("Artificial"))
                artificialFreeBinAllocator = new(maxRecords: 10);

            comparer = new RevivificationSpanByteComparer(collisionRange);
            store = new(new()
            {
                IndexSize = 1L << 24,
                LogDevice = log,
                PageSize = 1L << 17,
                MemorySize = 1L << 20,
                RevivificationSettings = RevivificationSettings.PowerOf2Bins
            }, StoreFunctions.Create(comparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );

            functions = new RevivificationStressFunctions(keyComparer: null);
            session = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(functions);
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
            artificialFreeBinAllocator?.Dispose();
            artificialFreeBinAllocator = null;

            DeleteDirectory(MethodTestDir);
        }

        void Populate()
        {
            Span<byte> key = stackalloc byte[KeyLength];
            Span<byte> input = stackalloc byte[InitialLength];
            var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

            SpanByteAndMemory output = new();

            for (int ii = 0; ii < NumRecords; ++ii)
            {
                key.Fill((byte)ii);
                input.Fill((byte)ii);

                var status = bContext.Upsert(key, ref pinnedInputSpan, input, ref output);
                ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        public enum WrapMode { Wrap, NoWrap };
        const int TakeRecordSize = 40;          // Just below AddRecordSize
        const int AddRecordSize = 48;           // smallSize doesn't matter in this test, but must be a multiple of 8

        const int AddressIncrement = 1_000_000; // must be > ReadOnlyAddress

        /// <summary>
        /// For the artificial bin tests, the LogRecord needs only a RecordInfo and a RecordDataHeader; no actual data operations are done,
        /// only "get allocated smallSize", so we can reuse LogRecords of the same "record smallSize".
        /// </summary>
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        internal unsafe struct RecordStub
        {
            internal static int Size => sizeof(RecordStub);

            internal RecordInfo recordInfo;

            // All recordSizes in this test fit into a single byte, so the RecordDataHeader is less than a long.
            readonly long headerWord;

            // All recordSizes in this test fit into a single byte, so the RecordDataHeader is less than a long and thus the key will cross the
            // boundary into the first long.
            private readonly long l1, l2, l3, l4, l5, l6, l7;

            /// <summary>Create an in-place initialization of a stubbed LogRecord</summary>
            /// <param name="recordPtr">Record address of 'this', from the pinned array</param>
            /// <param name="recordSize">Size of the recordPtr</param>
            internal static void Initialize(RecordStub* recordPtr, int recordSize)
            {
                recordPtr->recordInfo = RecordInfo.InitialValid;

                const int DefaultKeySize = sizeof(long);
                var sizeInfo = new RecordSizeInfo()
                {
                    FieldInfo = new()
                    {
                        KeySize = DefaultKeySize,
                        ValueSize = recordSize - DefaultKeySize - RecordInfo.Size - RecordDataHeader.MinHeaderBytes
                    },
                    KeyIsInline = true,
                    ValueIsInline = true
                };

                Assert.That(sizeInfo.InlineValueSize > 0, $"RecordSize {recordSize} is too small; sizeInfo.InlineValueSize {sizeInfo.InlineValueSize} must be greater than zero");
                sizeInfo.CalculateSizes(sizeInfo.FieldInfo.KeySize, sizeInfo.FieldInfo.ValueSize);

                // We don't use the key in these artificial bin tests, but verify we stored the address (it is a useful debugging tool).
                long key = (long)recordPtr;
                var logRecord = new LogRecord((long)recordPtr);
                logRecord.InitializeRecord(SpanByte.FromPinnedVariable(ref key), in sizeInfo);
                Assert.That(logRecord.Key.AsRef<long>(), Is.EqualTo(key));

                var dataHeader = new RecordDataHeader((byte*)&recordPtr->headerWord);
                Assert.That(dataHeader.GetActualRecordSize(recordPtr->recordInfo), Is.EqualTo(sizeInfo.ActualInlineRecordSize));
                Assert.That(dataHeader.GetAllocatedRecordSize(), Is.EqualTo(sizeInfo.AllocatedInlineRecordSize));
            }
        }

        unsafe class ArtificialFreeBinAllocator : IDisposable
        {
            RecordStub* records;
            readonly int maxRecords;
            int nextFreeRecord = 0;

            internal ArtificialFreeBinAllocator(int maxRecords)
            {
                this.maxRecords = maxRecords;
                var bufferSizeInBytes = (nuint)(sizeof(RecordStub) * maxRecords);
                records = (RecordStub*)NativeMemory.AlignedAlloc(bufferSizeInBytes, Constants.kCacheLineBytes);
                NativeMemory.Clear(records, bufferSizeInBytes);
            }

            internal RecordStub* AllocateRecordAndInitializeSize(int recordSize)
            {
                Assert.That(nextFreeRecord, Is.LessThan(maxRecords + 1), $"ArtificialFreeBinAllocator out of records (maxRecords {maxRecords})");
                Assert.That(recordSize, Is.LessThanOrEqualTo(RecordStub.Size), $"RecordSize {recordSize} exceeds RecordStub.Size {RecordStub.Size}");
                var recordPtr = records + nextFreeRecord++;
                RecordStub.Initialize(recordPtr, recordSize);
                return recordPtr;
            }

            internal LogRecord AllocateLogRecord(int recordSize)
            {
                var recordInfoAndHeader = AllocateRecordAndInitializeSize(recordSize);
                var logRecord = new LogRecord((long)recordInfoAndHeader);
                Assert.That(logRecord.ActualSize, Is.EqualTo(recordSize), "Allocated LogRecord has unexpected smallSize");
                Assert.That(logRecord.AllocatedSize, Is.EqualTo(RoundUp(recordSize, Constants.kRecordAlignment)), "Allocated LogRecord has unexpected aligned smallSize");
                return logRecord;
            }

            public void Dispose()
            {
                if (records != null)
                {
                    NativeMemory.AlignedFree(records);
                    records = null;
                }
            }
        }

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

            // Set up the fake sizeInfo and logRecord; they only have to return lengths.
            int maxRecords = numRecordsPerThread * numAddThreads;
            int numTotalThreads = numAddThreads + numTakeThreads;
            const long Unadded = 0;
            const long Added = 1;
            const long RemovedBase = 10000; // tid will be added to this to help diagnose any failures

            // For this test we are bypassing the FreeRecordPool in store.
            var binDef = new RevivificationBin()
            {
                RecordSize = AddRecordSize,
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

            var logRecord_AddSize = artificialFreeBinAllocator.AllocateLogRecord(AddRecordSize);

            void runAddThread(int tid)
            {
                RevivificationStats revivStats = new();
                for (var ii = 0; ii < numRecordsPerThread; ++ii)
                {
                    var addressBase = ii + tid * numRecordsPerThread;
                    var flag = flags[addressBase];
                    ClassicAssert.AreEqual(Unadded, flag, $"Invalid flag {flag} trying to add addressBase {addressBase}, tid {tid}, iteration {iteration}");
                    flags[addressBase] = 1;
                    ClassicAssert.IsTrue(freeRecordPool.TryAdd(addressBase + AddressIncrement, ref logRecord_AddSize, ref revivStats), $"Failed to add addressBase {addressBase}, tid {tid}, iteration {iteration}");
                }
            }

            void runTakeThread(int tid)
            {
                RevivificationStats revivStats = new();
                while (totalTaken < maxRecords)
                {
                    if (freeRecordPool.bins[0].TryTake(AddRecordSize, 0, store, out long address, ref revivStats))
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
        public void ArtificialSimpleTest()
        {
            var binDef = new RevivificationBin()
            {
                RecordSize = TakeRecordSize + Constants.kRecordAlignment,
                NumberOfRecords = 64,
                BestFitScanLimit = RevivificationBin.UseFirstFit
            };
            var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(store, binDef);

            var logRecord_TakeSize = artificialFreeBinAllocator.AllocateLogRecord(TakeRecordSize);

            RevivificationStats revivStats = new();
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(AddressIncrement + 1, ref logRecord_TakeSize, ref revivStats));
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeRecordSize, minAddress: AddressIncrement, out var address, ref revivStats));

            ClassicAssert.AreEqual(AddressIncrement + 1, address, "out address");
            ClassicAssert.AreEqual(1, revivStats.successfulAdds, "Successful Adds");
            ClassicAssert.AreEqual(1, revivStats.successfulTakes, "Successful Takes");
            _ = revivStats.Dump();
        }

        private FreeRecordPool<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> CreateBestFitTestPool(int scanLimit, WrapMode wrapMode, ref RevivificationStats revivStats)
        {
            // "Wrap Mode" means we are going to split the segment for our Take records across the end of the bin, wrapping around to the beginning.
            // So e.g. for the 64-record bin, we'll put 62 "don't want" records in; our segment start for the Take records will be before that (currently 48)
            // and will scan forward to (currently) slot 62. This means a small range of record sizes will let us test the wrap for both TakeSize
            // and TakeSize + Constants.kRecordAlignment.
            // For non-Wrap, our best-fit test requires that TakeSize and TakeSize + Constants.kRecordAlignment start on the same segment;
            // and the bin record size range will be from 16 (see FreeRecordPool.cs) to the RecordSize we specify here. So make the RecordSize
            // large, for coarse-grained chunks, so both TakeSize and TakeSize + Constants.kRecordAlignment map to the same segment.
            var binDef = new RevivificationBin()
            {
                RecordSize = TakeRecordSize + (wrapMode == WrapMode.Wrap ? Constants.kRecordAlignment : 128),
                NumberOfRecords = 64,
                BestFitScanLimit = scanLimit
            };
            var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(store, binDef);

            int expectedAdds = 0, expectedTakes = 0;
            if (wrapMode == WrapMode.Wrap)
            {
                int minAddress = AddressIncrement - freeRecordPool.bins[0].recordCount - 10;

                // Add too-small records to wrap around the end of the bin records. Use lower addresses so we don't mix up the "real" results.
                const int smallSize = 22;   // min required size for 8-byte key is 22
                var logRecord_smallSize = artificialFreeBinAllocator.AllocateLogRecord(smallSize);
                for (var ii = 0; ii < freeRecordPool.bins[0].recordCount - 2; ++ii, ++expectedAdds)
                    ClassicAssert.IsTrue(freeRecordPool.TryAdd(minAddress + ii + 1, ref logRecord_smallSize, ref revivStats));

                // Now take out the four at the beginning.
                for (var ii = 0; ii < 4; ++ii, ++expectedTakes)
                    ClassicAssert.IsTrue(freeRecordPool.TryTake(smallSize, minAddress, out _, ref revivStats));
            }

            long address = AddressIncrement;
            var logRecord_TakeSize = artificialFreeBinAllocator.AllocateLogRecord(TakeRecordSize);
            var logRecord_TakeSizePlus1 = artificialFreeBinAllocator.AllocateLogRecord(TakeRecordSize + 1);
            var logRecord_TakeSizePlus2 = artificialFreeBinAllocator.AllocateLogRecord(TakeRecordSize + 2);
            var logRecord_TakeSizePlus3 = artificialFreeBinAllocator.AllocateLogRecord(TakeRecordSize + 3);
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, ref logRecord_TakeSizePlus1, ref revivStats));
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, ref logRecord_TakeSizePlus2, ref revivStats));
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, ref logRecord_TakeSizePlus3, ref revivStats));
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, ref logRecord_TakeSize, ref revivStats));     // 4
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, ref logRecord_TakeSize, ref revivStats));     // 5 
            ClassicAssert.IsTrue(freeRecordPool.TryAdd(++address, ref logRecord_TakeSize, ref revivStats));
            expectedAdds += 6;

            ClassicAssert.AreEqual(expectedAdds, revivStats.successfulAdds, "Successful Adds");
            ClassicAssert.AreEqual(expectedTakes, revivStats.successfulTakes, "Successful Takes");

            return freeRecordPool;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void ArtificialBestFitTest([Values] WrapMode wrapMode)
        {
            // We should first Take the first TakeSize-length due to exact fit, then skip over the empty to take the next TakeSize, then we have
            // no exact fit within the scan limit, so we grab the best fit before that (TakeSize + 1).
            RevivificationStats revivStats = new();
            using var freeRecordPool = CreateBestFitTestPool(scanLimit: 4, wrapMode, ref revivStats);
            var expectedTakes = revivStats.successfulTakes;
            var minAddress = AddressIncrement;
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeRecordSize, minAddress, out var address, ref revivStats));
            ClassicAssert.AreEqual(4, address - AddressIncrement);
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeRecordSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(5, address - AddressIncrement);
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeRecordSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(1, address - AddressIncrement);

            // Now that we've taken the first item, the new first-fit will be moved up one, which brings the last exact-fit into scanLimit range.
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeRecordSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(6, address - AddressIncrement);
            // Now Take will return them in order until we have no more
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeRecordSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(2, address - AddressIncrement);
            ClassicAssert.IsTrue(freeRecordPool.TryTake(TakeRecordSize, minAddress, out address, ref revivStats));
            ClassicAssert.AreEqual(3, address - AddressIncrement);
            ClassicAssert.IsFalse(freeRecordPool.TryTake(TakeRecordSize, minAddress, out address, ref revivStats));
            expectedTakes += 6; // Plus one failure

            ClassicAssert.AreEqual(expectedTakes, revivStats.successfulTakes, "Successful Takes");
            ClassicAssert.AreEqual(1, revivStats.failedTakes, "Failed Takes");
            var statsString = revivStats.Dump();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        public void ArtificialFirstFitTest([Values] WrapMode wrapMode)
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
                if (!freeRecordPool.TryTake(TakeRecordSize, minAddress, out address, ref revivStats))
                    Assert.Fail($"Take failed at ii {ii}: pool.HasRecords {RevivificationTestUtils.HasRecords(freeRecordPool)}");
                ClassicAssert.AreEqual(ii + 1, address -= AddressIncrement, $"address comparison failed at ii {ii}");
            }
            ClassicAssert.IsFalse(freeRecordPool.TryTake(TakeRecordSize, minAddress, out address, ref revivStats));

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
        public void ArtificialThreadContentionOnOneRecordTest()
        {
            var binDef = new RevivificationBin()
            {
                RecordSize = 32,
                NumberOfRecords = 32
            };
            var freeRecordPool = RevivificationTestUtils.CreateSingleBinFreeRecordPool(store, binDef);
            const long TestAddress = AddressIncrement, minAddress = AddressIncrement - 10;
            long counter = 0, globalAddress = 0;
            const int smallSize = 22;   // min required size for 8-byte key is 22
            var logRecord_smallSize = artificialFreeBinAllocator.AllocateLogRecord(smallSize);
            const int numIterations = 10000;

            void runThread(int tid)
            {
                RevivificationStats revivStats = new();
                long localCounter = 0;
                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    if (freeRecordPool.TryTake(smallSize, minAddress, out long address, ref revivStats))
                    {
                        globalAddress = TestAddress;
                        --localCounter;
                    }
                    else if (globalAddress == TestAddress && Interlocked.CompareExchange(ref globalAddress, 0, TestAddress) == TestAddress)
                    {
                        ClassicAssert.IsTrue(freeRecordPool.TryAdd(TestAddress, ref logRecord_smallSize, ref revivStats), $"Failed TryAdd on iter {iteration}");
                        ++localCounter;
                    }
                }
                _ = Interlocked.Add(ref counter, localCounter);
            }

            List<Task> tasks = [];   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < 8; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runThread(tid)));
            }
            Task.WaitAll([.. tasks]);

            Assert.That(counter, Is.EqualTo(0));
        }

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

            void runDeleteThread(int tid)
            {
                Random rng = new(tid * 101);

                using var localSession = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(new RevivificationStressFunctions(keyComparer: null));
                var localbContext = localSession.BasicContext;

                Span<byte> key = stackalloc byte[KeyLength];

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numDeleteThreads)
                    {
                        var kk = rng.Next(keyRange);
                        key.Fill((byte)kk);
                        _ = localbContext.Delete(key);
                    }
                }
            }

            void runUpdateThread(int tid)
            {
                Span<byte> key = stackalloc byte[KeyLength];

                Span<byte> input = stackalloc byte[InitialLength];
                var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

                Random rng = new(tid * 101);

                RevivificationStressFunctions localFunctions = new(keyComparer: comparer);
                using var localSession = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(localFunctions);
                var localbContext = localSession.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numUpdateThreads)
                    {
                        var kk = rng.Next(keyRange);
                        key.Fill((byte)kk);
                        input.Fill((byte)kk);

                        localSession.functions.expectedKey = PinnedSpanByte.FromPinnedSpan(key);
                        _ = updateOp == UpdateOp.Upsert ? localbContext.Upsert(key, input) : localbContext.RMW(key, ref pinnedInputSpan);
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

            void runDeleteThread(int tid)
            {
                Random rng = new(tid * 101);

                using var localSession = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(new RevivificationStressFunctions(keyComparer: null));
                var localbContext = localSession.BasicContext;

                Span<byte> key = stackalloc byte[KeyLength];

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numDeleteThreads)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(NumRecords) : ii;
                        key.Fill((byte)kk);
                        _ = localbContext.Delete(key);
                    }
                }
            }

            void runUpdateThread(int tid)
            {
                Span<byte> key = stackalloc byte[KeyLength];
                Span<byte> input = stackalloc byte[InitialLength];
                var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

                Random rng = new(tid * 101);

                RevivificationStressFunctions localFunctions = new(keyComparer: comparer);
                using var localSession = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(localFunctions);
                var localbContext = localSession.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numUpdateThreads)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(NumRecords) : ii;
                        key.Fill((byte)kk);
                        input.Fill((byte)kk);

                        localSession.functions.expectedKey = PinnedSpanByte.FromPinnedSpan(key);
                        _ = updateOp == UpdateOp.Upsert ? localbContext.Upsert(key, input) : localbContext.RMW(key, ref pinnedInputSpan);
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

            void runDeleteThread(int tid)
            {
                using var localSession = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(new RevivificationStressFunctions(keyComparer: null));
                var localbContext = localSession.BasicContext;

                Span<byte> key = stackalloc byte[KeyLength];

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numDeleteThreads)
                    {
                        key.Fill((byte)ii);
                        _ = localbContext.Delete(key);
                    }
                }
            }

            void runUpdateThread(int tid)
            {
                Span<byte> key = stackalloc byte[KeyLength];
                Span<byte> input = stackalloc byte[InitialLength];
                var pinnedInputSpan = PinnedSpanByte.FromPinnedSpan(input);

                RevivificationStressFunctions localFunctions = new RevivificationStressFunctions(keyComparer: null);
                using var localSession = store.NewSession<PinnedSpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(localFunctions);
                var localbContext = localSession.BasicContext;

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < NumRecords; ii += numUpdateThreads)
                    {
                        key.Fill((byte)ii);
                        input.Fill((byte)ii);

                        localSession.functions.expectedKey = PinnedSpanByte.FromPinnedSpan(key);
                        _ = updateOp == UpdateOp.Upsert ? localbContext.Upsert(key, input) : localbContext.RMW(key, ref pinnedInputSpan);
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