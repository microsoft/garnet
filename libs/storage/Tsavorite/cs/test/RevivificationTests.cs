// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.core.Utility;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.Revivification
{
    public enum DeleteDest { FreeList, InChain }

    public enum CollisionRange { Ten = 10, None = int.MaxValue }

    public enum RevivificationEnabled { Reviv, NoReviv }

    public enum RevivifiableFraction { Half }

    public enum RecordElision { Elide, NoElide }

    struct RevivificationTestUtils
    {
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
                UsedValueLength = upsertInfo.UsedValueLength,
                FullValueLength = upsertInfo.FullValueLength,
                Action = RMWAction.Default,
            };

        internal static FreeRecordPool<TKey, TValue> CreateSingleBinFreeRecordPool<TKey, TValue>(TsavoriteKV<TKey, TValue> store, RevivificationBin binDef, int fixedRecordLength = 0)
            => new(store, new RevivificationSettings() { FreeRecordBins = new[] { binDef } }, fixedRecordLength);

        internal static bool HasRecords<TKey, TValue>(TsavoriteKV<TKey, TValue> store)
            => HasRecords(store.RevivificationManager.FreeRecordPool);

        internal static bool HasRecords<TKey, TValue>(TsavoriteKV<TKey, TValue> store, FreeRecordPool<TKey, TValue> pool)
            => HasRecords(pool ?? store.RevivificationManager.FreeRecordPool);

        internal static bool HasRecords<TKey, TValue>(FreeRecordPool<TKey, TValue> pool)
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

        internal static FreeRecordPool<TKey, TValue> SwapFreeRecordPool<TKey, TValue>(TsavoriteKV<TKey, TValue> store, FreeRecordPool<TKey, TValue> inPool)
        {
            var pool = store.RevivificationManager.FreeRecordPool;
            store.RevivificationManager.FreeRecordPool = inPool;
            return pool;
        }

        internal const int DefaultRecordWaitTimeoutMs = 2000;

        internal static bool GetBinIndex<TKey, TValue>(FreeRecordPool<TKey, TValue> pool, int recordSize, out int binIndex) => pool.GetBinIndex(recordSize, out binIndex);

        internal static int GetBinCount<TKey, TValue>(FreeRecordPool<TKey, TValue> pool) => pool.bins.Length;

        internal static int GetRecordCount<TKey, TValue>(FreeRecordPool<TKey, TValue> pool, int binIndex) => pool.bins[binIndex].recordCount;

        internal static int GetMaxRecordSize<TKey, TValue>(FreeRecordPool<TKey, TValue> pool, int binIndex) => pool.bins[binIndex].maxRecordSize;

        internal static unsafe bool IsSet<TKey, TValue>(FreeRecordPool<TKey, TValue> pool, int binIndex, int recordIndex) => pool.bins[binIndex].records[recordIndex].IsSet;

        internal static bool TryTakeFromBin<TKey, TValue>(FreeRecordPool<TKey, TValue> pool, int binIndex, int recordSize, long minAddress, TsavoriteKV<TKey, TValue> store, out long address, ref RevivificationStats revivStats)
            => pool.bins[binIndex].TryTake(recordSize, minAddress, store, out address, ref revivStats);

        internal static int GetSegmentStart<TKey, TValue>(FreeRecordPool<TKey, TValue> pool, int binIndex, int recordSize) => pool.bins[binIndex].GetSegmentStart(recordSize);

        internal static void WaitForRecords<TKey, TValue>(TsavoriteKV<TKey, TValue> store, bool want, FreeRecordPool<TKey, TValue> pool = default)
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
                        Assert.Less(sw.ElapsedMilliseconds, DefaultRecordWaitTimeoutMs, $"Timeout while waiting for Pool.WaitForRecords to be {want}");
                    Thread.Yield();
                }
                return;
            }
        }

        internal static unsafe int GetFreeRecordCount<TKey, TValue>(TsavoriteKV<TKey, TValue> store) => GetFreeRecordCount(store.RevivificationManager.FreeRecordPool);

        internal static unsafe int GetFreeRecordCount<TKey, TValue>(FreeRecordPool<TKey, TValue> pool)
        {
            // This returns the count of all records, not just the free ones.
            int count = 0;
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

        internal static void AssertElidable<TKey, TValue>(TsavoriteKV<TKey, TValue> store, TKey key) => AssertElidable(store, ref key);
        internal static void AssertElidable<TKey, TValue>(TsavoriteKV<TKey, TValue> store, ref TKey key)
        {
            OperationStackContext<TKey, TValue> stackCtx = new(store.comparer.GetHashCode64(ref key));
            Assert.IsTrue(store.FindTag(ref stackCtx.hei), $"AssertElidable: Cannot find key {key}");
            var recordInfo = store.hlog.GetInfo(store.hlog.GetPhysicalAddress(stackCtx.hei.Address));
            Assert.Less(recordInfo.PreviousAddress, store.hlog.BeginAddress, "AssertElidable: expected elidable key");
        }

        internal static int GetRevivifiableRecordCount<TKey, TValue>(TsavoriteKV<TKey, TValue> store, int numRecords)
            => (int)(numRecords * store.RevivificationManager.revivifiableFraction);

        internal static int GetMinRevivifiableKey<TKey, TValue>(TsavoriteKV<TKey, TValue> store, int numRecords)
            => numRecords - GetRevivifiableRecordCount(store, numRecords);
    }

    internal readonly struct RevivificationSpanByteComparer : ITsavoriteEqualityComparer<SpanByte>
    {
        private readonly SpanByteComparer defaultComparer;
        private readonly int collisionRange;

        internal RevivificationSpanByteComparer(CollisionRange range)
        {
            defaultComparer = new SpanByteComparer();
            collisionRange = (int)range;
        }

        public bool Equals(ref SpanByte k1, ref SpanByte k2) => defaultComparer.Equals(ref k1, ref k2);

        // The hash code ends with 0 so mod Ten isn't so helpful, so shift
        public long GetHashCode64(ref SpanByte k) => (defaultComparer.GetHashCode64(ref k) >> 4) % collisionRange;
    }

    [TestFixture]
    class RevivificationFixedLenTests
    {
        internal class RevivificationFixedLenFunctions : SimpleFunctions<int, int>
        {
        }

        const int numRecords = 1000;
        internal const int valueMult = 1_000_000;

        RevivificationFixedLenFunctions functions;

        private TsavoriteKV<int, int> store;
        private ClientSession<int, int, int, int, Empty, RevivificationFixedLenFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            var concurrencyControlMode = ConcurrencyControlMode.LockTable;
            double? revivifiableFraction = default;
            RecordElision? recordElision = default;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ConcurrencyControlMode ccm)
                {
                    concurrencyControlMode = ccm;
                    continue;
                }
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

            var revivificationSettings = RevivificationSettings.DefaultFixedLength.Clone();
            if (revivifiableFraction.HasValue)
                revivificationSettings.RevivifiableFraction = revivifiableFraction.Value;
            if (recordElision.HasValue)
                revivificationSettings.RestoreDeletedRecordsIfBinIsFull = recordElision.Value == RecordElision.NoElide;
            store = new TsavoriteKV<int, int>(1L << 18, new LogSettings { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 12, MemorySizeBits = 20 },
                                            concurrencyControlMode: concurrencyControlMode, revivificationSettings: revivificationSettings);
            functions = new RevivificationFixedLenFunctions();
            session = store.NewSession<int, int, Empty, RevivificationFixedLenFunctions>(functions);
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
            for (int key = 0; key < numRecords; key++)
            {
                var status = session.Upsert(key, key * valueMult);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SimpleFixedLenTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;
            if (stayInChain)
                _ = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            var deleteKey = RevivificationTestUtils.GetMinRevivifiableKey(store, numRecords);
            if (!stayInChain)
                RevivificationTestUtils.AssertElidable(store, deleteKey);
            var tailAddress = store.Log.TailAddress;

            session.Delete(deleteKey);
            Assert.AreEqual(tailAddress, store.Log.TailAddress);

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : numRecords + 1;
            var updateValue = updateKey + valueMult;

            if (!stayInChain)
            {
                Assert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
                RevivificationTestUtils.WaitForRecords(store, want: true);
            }

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(updateKey, updateValue);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(updateKey, updateValue);

            if (!stayInChain)
                RevivificationTestUtils.WaitForRecords(store, want: false);
            Assert.AreEqual(tailAddress, store.Log.TailAddress, "Expected tail address not to grow (record was revivified)");
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void UnelideTest([Values] RecordElision elision, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            // First delete all keys. This will overflow the bin.
            for (var key = 0; key < numRecords; ++key)
            {
                session.Delete(key);
                Assert.AreEqual(tailAddress, store.Log.TailAddress);
            }

            Assert.AreEqual(RevivificationBin.DefaultRecordsPerBin, RevivificationTestUtils.GetFreeRecordCount(store));
            RevivificationTestUtils.WaitForRecords(store, want: true);

            // Now re-add the keys.
            for (var key = 0; key < numRecords; ++key)
            {
                var value = key + valueMult;
                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(key, value);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(key, value);
            }

            // Now re-add the keys. For the elision case, we should see tailAddress grow sharply as only the records in the bin are available
            // for revivification. For In-Chain, we will revivify records that were unelided after the bin overflowed. But we have some records
            // ineligible for revivification due to revivifiableFraction.
            var recordSize = RecordInfo.GetLength() + sizeof(int) * 2;
            var numIneligibleRecords = numRecords - RevivificationTestUtils.GetRevivifiableRecordCount(store, numRecords);
            var noElisionExpectedTailAddress = tailAddress + numIneligibleRecords * recordSize;

            if (elision == RecordElision.NoElide)
                Assert.AreEqual(noElisionExpectedTailAddress, store.Log.TailAddress, "Expected tail address not to grow (records were revivified)");
            else
                Assert.Less(noElisionExpectedTailAddress, store.Log.TailAddress, "Expected tail address to grow (records were not revivified)");
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "revivifiableFraction and concurrencyControlMode are used by Setup")]
        public void SimpleMinAddressAddTest([Values] RevivifiableFraction revivifiableFraction,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            // This should not go to FreeList because it's below the RevivifiableFraction
            Assert.IsTrue(session.Delete(2).Found);
            Assert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store));

            // This should go to FreeList because it's above the RevivifiableFraction
            Assert.IsTrue(session.Delete(numRecords - 1).Found);
            Assert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "revivifiableFraction and concurrencyControlMode are used by Setup")]
        public void SimpleMinAddressTakeTest([Values] RevivifiableFraction revivifiableFraction, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            // This should go to FreeList because it's above the RevivifiableFraction
            Assert.IsTrue(session.Delete(numRecords - 1).Found);
            Assert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
            RevivificationTestUtils.WaitForRecords(store, want: true);

            // Detach the pool temporarily so the records aren't revivified by the next insertions.
            var pool = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            // Now add a bunch of records to drop the FreeListed address below the RevivifiableFraction
            int maxRecord = numRecords * 2;
            for (int key = numRecords; key < maxRecord; key++)
            {
                var status = session.Upsert(key, key * valueMult);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }

            // Restore the pool
            RevivificationTestUtils.SwapFreeRecordPool(store, pool);

            var tailAddress = store.Log.TailAddress;

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(maxRecord, maxRecord * valueMult);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(maxRecord, maxRecord * valueMult);

            Assert.Less(tailAddress, store.Log.TailAddress, "Expected tail address to grow (record was not revivified)");
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
            private readonly TsavoriteKV<SpanByte, SpanByte> store;

            // Must be set after session is created
            internal ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions> session;

            internal int expectedConcurrentDestLength = InitialLength;
            internal int expectedSingleDestLength = InitialLength;
            internal int expectedConcurrentFullValueLength = -1;
            internal int expectedSingleFullValueLength = -1;
            internal int expectedInputLength = InitialLength;

            // This is a queue rather than a single value because there may be calls to, for example, ConcurrentWriter with one length
            // followed by SingleWriter with another.
            internal Queue<int> expectedUsedValueLengths = new();

            internal bool readCcCalled, rmwCcCalled;

            internal RevivificationSpanByteFunctions(TsavoriteKV<SpanByte, SpanByte> store)
            {
                this.store = store;
            }

            private void AssertInfoValid(ref UpsertInfo updateInfo)
            {
                Assert.AreEqual(session.ctx.version, updateInfo.Version);
            }
            private void AssertInfoValid(ref RMWInfo rmwInfo)
            {
                Assert.AreEqual(session.ctx.version, rmwInfo.Version);
            }
            private void AssertInfoValid(ref DeleteInfo deleteInfo)
            {
                Assert.AreEqual(session.ctx.version, deleteInfo.Version);
            }

            private static void VerifyKeyAndValue(ref SpanByte functionsKey, ref SpanByte functionsValue)
            {
                int valueOffset = 0, valueLengthRemaining = functionsValue.Length;
                Assert.Less(functionsKey.Length, valueLengthRemaining);
                while (valueLengthRemaining > 0)
                {
                    var compareLength = Math.Min(functionsKey.Length, valueLengthRemaining);
                    Span<byte> valueSpan = functionsValue.AsSpan().Slice(valueOffset, compareLength);
                    Span<byte> keySpan = functionsKey.AsSpan()[..compareLength];
                    Assert.IsTrue(valueSpan.SequenceEqual(keySpan), $"functionsValue (offset {valueOffset}, len {compareLength}: {SpanByte.FromFixedSpan(valueSpan)}) does not match functionsKey ({SpanByte.FromFixedSpan(keySpan)})");
                    valueLengthRemaining -= compareLength;
                }
            }

            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                var rmwInfo = RevivificationTestUtils.CopyToRMWInfo(ref upsertInfo);
                var result = InitialUpdater(ref key, ref input, ref dst, ref output, ref rmwInfo, ref recordInfo);
                upsertInfo.UsedValueLength = rmwInfo.UsedValueLength;
                return result;
            }

            public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                var rmwInfo = RevivificationTestUtils.CopyToRMWInfo(ref upsertInfo);
                var result = InPlaceUpdater(ref key, ref input, ref dst, ref output, ref rmwInfo, ref recordInfo);
                upsertInfo.UsedValueLength = rmwInfo.UsedValueLength;
                return result;
            }

            public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                AssertInfoValid(ref rmwInfo);
                Assert.AreEqual(expectedInputLength, input.Length);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();

                if (value.Length == 0)
                {
                    Assert.AreEqual(expectedUsedValueLength, rmwInfo.UsedValueLength);   // for the length header
                    Assert.AreEqual(SpanByteAllocator.kRecordAlignment, rmwInfo.FullValueLength); // This should be the "added record for Delete" case, so a "default" value
                }
                else
                {
                    Assert.AreEqual(expectedSingleDestLength, value.Length);
                    Assert.AreEqual(expectedSingleFullValueLength, rmwInfo.FullValueLength);
                    Assert.AreEqual(expectedUsedValueLength, rmwInfo.UsedValueLength);
                    Assert.GreaterOrEqual(rmwInfo.Address, store.hlog.ReadOnlyAddress);
                }
                return base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
            }

            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                AssertInfoValid(ref rmwInfo);
                Assert.AreEqual(expectedInputLength, input.Length);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();

                if (newValue.Length == 0)
                {
                    Assert.AreEqual(sizeof(int), rmwInfo.UsedValueLength);   // for the length header
                    Assert.AreEqual(SpanByteAllocator.kRecordAlignment, rmwInfo.FullValueLength); // This should be the "added record for Delete" case, so a "default" value
                }
                else
                {
                    Assert.AreEqual(expectedSingleDestLength, newValue.Length);
                    Assert.AreEqual(expectedSingleFullValueLength, rmwInfo.FullValueLength);
                    Assert.AreEqual(expectedUsedValueLength, rmwInfo.UsedValueLength);
                    Assert.GreaterOrEqual(rmwInfo.Address, store.hlog.ReadOnlyAddress);
                }
                return base.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);
            }

            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                AssertInfoValid(ref rmwInfo);
                Assert.AreEqual(expectedInputLength, input.Length);
                Assert.AreEqual(expectedConcurrentDestLength, value.Length);
                Assert.AreEqual(expectedConcurrentFullValueLength, rmwInfo.FullValueLength);

                VerifyKeyAndValue(ref key, ref value);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();
                Assert.AreEqual(expectedUsedValueLength, rmwInfo.UsedValueLength);

                Assert.GreaterOrEqual(rmwInfo.Address, store.hlog.ReadOnlyAddress);

                return base.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
            }

            // Override the default SpanByteFunctions impelementation; for these tests, we always want the input length.
            public override int GetRMWModifiedValueLength(ref SpanByte value, ref SpanByte input) => input.TotalSize;

            public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            {
                AssertInfoValid(ref deleteInfo);
                Assert.AreEqual(expectedSingleDestLength, value.Length);
                Assert.AreEqual(expectedSingleFullValueLength, deleteInfo.FullValueLength);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();
                Assert.AreEqual(expectedUsedValueLength, deleteInfo.UsedValueLength);

                Assert.GreaterOrEqual(deleteInfo.Address, store.hlog.ReadOnlyAddress);

                return base.SingleDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);
            }

            public override bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            {
                AssertInfoValid(ref deleteInfo);
                Assert.AreEqual(expectedConcurrentDestLength, value.Length);
                Assert.AreEqual(expectedConcurrentFullValueLength, deleteInfo.FullValueLength);

                var expectedUsedValueLength = expectedUsedValueLengths.Dequeue();
                Assert.AreEqual(expectedUsedValueLength, deleteInfo.UsedValueLength);

                Assert.GreaterOrEqual(deleteInfo.Address, store.hlog.ReadOnlyAddress);

                return base.ConcurrentDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);
            }

            public override void PostCopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                base.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            }

            public override void PostInitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo)
            {
                AssertInfoValid(ref rmwInfo);
                base.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            }

            public override void PostSingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason writeReason)
            {
                AssertInfoValid(ref upsertInfo);
                base.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, writeReason);
            }

            public override void PostSingleDeleter(ref SpanByte key, ref DeleteInfo deleteInfo)
            {
                AssertInfoValid(ref deleteInfo);
                base.PostSingleDeleter(ref key, ref deleteInfo);
            }

            public override void ReadCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                readCcCalled = true;
                base.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);
            }

            public override void RMWCompletionCallback(ref SpanByte key, ref SpanByte input, ref SpanByteAndMemory output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                rmwCcCalled = true;
                base.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);
            }
        }

        static int RoundUpSpanByteFullValueLength(SpanByte input) => RoundupTotalSizeFullValue(input.TotalSize);

        static int RoundUpSpanByteFullValueLength(int dataLength) => RoundupTotalSizeFullValue(sizeof(int) + dataLength);

        internal static int RoundupTotalSizeFullValue(int length) => (length + SpanByteAllocator.kRecordAlignment - 1) & (~(SpanByteAllocator.kRecordAlignment - 1));

        static int RoundUpSpanByteUsedLength(int dataLength) => RoundUp(SpanByteTotalSize(dataLength), sizeof(int));

        static int SpanByteTotalSize(int dataLength) => sizeof(int) + dataLength;

        const int numRecords = 200;

        RevivificationSpanByteFunctions functions;
        RevivificationSpanByteComparer comparer;

        private TsavoriteKV<SpanByte, SpanByte> store;
        private ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            CollisionRange collisionRange = CollisionRange.None;
            LogSettings logSettings = new() { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 17, MemorySizeBits = 20 };
            var concurrencyControlMode = ConcurrencyControlMode.LockTable;
            var revivificationSettings = RevivificationSettings.PowerOf2Bins;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is CollisionRange cr)
                {
                    collisionRange = cr;
                    continue;
                }
                if (arg is ConcurrencyControlMode ccm)
                {
                    concurrencyControlMode = ccm;
                    continue;
                }
                if (arg is PendingOp)
                {
                    logSettings.ReadCopyOptions = new(ReadCopyFrom.Device, ReadCopyTo.MainLog);
                    continue;
                }
                if (arg is RevivificationEnabled revivEnabled)
                {
                    if (revivEnabled == RevivificationEnabled.NoReviv)
                        revivificationSettings = default;
                    continue;
                }
            }

            comparer = new RevivificationSpanByteComparer(collisionRange);
            store = new TsavoriteKV<SpanByte, SpanByte>(1L << 16, logSettings, comparer: comparer, concurrencyControlMode: concurrencyControlMode, revivificationSettings: revivificationSettings);

            functions = new RevivificationSpanByteFunctions(store);
            session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationSpanByteFunctions>(functions);
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

        void Populate() => Populate(0, numRecords);

        void Populate(int from, int to)
        {
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);

            SpanByteAndMemory output = new();

            for (int ii = from; ii < to; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);
                functions.expectedUsedValueLengths.Enqueue(input.TotalSize);
                var status = session.Upsert(ref key, ref input, ref input, ref output);
                Assert.IsTrue(status.Record.Created, status.ToString());
                Assert.IsEmpty(functions.expectedUsedValueLengths);
            }
        }

        public enum Growth { None, Grow, Shrink };

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SpanByteNoRevivLengthTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] Growth growth,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromFixedSpan(keyVec);

            // Do NOT delete; this is a no-reviv test of lengths

            functions.expectedInputLength = growth switch
            {
                Growth.None => InitialLength,
                Growth.Grow => GrowLength,
                Growth.Shrink => ShrinkLength,
                _ => -1
            };

            functions.expectedSingleDestLength = functions.expectedInputLength;
            functions.expectedConcurrentDestLength = InitialLength; // This is from the initial Populate()
            functions.expectedSingleFullValueLength = RoundUpSpanByteFullValueLength(functions.expectedInputLength);
            functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            Span<byte> inputVec = stackalloc byte[functions.expectedInputLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(fillByte);

            // For Grow, we won't be able to satisfy the request with a revivification, and the new value length will be GrowLength
            functions.expectedUsedValueLengths.Enqueue(sizeof(int) + InitialLength);
            if (growth == Growth.Grow)
                functions.expectedUsedValueLengths.Enqueue(sizeof(int) + GrowLength);

            SpanByteAndMemory output = new();

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.IsEmpty(functions.expectedUsedValueLengths);

            if (growth == Growth.Shrink)
            {
                // What's there now will be what is passed to ConcurrentWriter/IPU (if Shrink, we kept the same value we allocated initially)
                functions.expectedConcurrentFullValueLength = growth == Growth.Shrink ? RoundUpSpanByteFullValueLength(InitialLength) : functions.expectedSingleFullValueLength;

                // Now let's see if we have the correct expected extra length in the destination.
                inputVec = stackalloc byte[InitialLength / 2];  // Grow this from ShrinkLength to InitialLength
                input = SpanByte.FromFixedSpan(inputVec);
                inputVec.Fill(fillByte);

                functions.expectedInputLength = InitialLength / 2;
                functions.expectedConcurrentDestLength = InitialLength / 2;
                functions.expectedSingleFullValueLength = RoundUpSpanByteFullValueLength(functions.expectedInputLength);
                functions.expectedUsedValueLengths.Enqueue(input.TotalSize);

                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
                Assert.IsEmpty(functions.expectedUsedValueLengths);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SpanByteSimpleTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromFixedSpan(keyVec);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            var status = session.Delete(ref key);
            Assert.IsTrue(status.Found, status.ToString());

            Assert.AreEqual(tailAddress, store.Log.TailAddress);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

            RevivificationTestUtils.WaitForRecords(store, want: true);

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.AreEqual(tailAddress, store.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SpanByteIPUGrowAndRevivifyTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[GrowLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = GrowLength;
            functions.expectedSingleDestLength = GrowLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = RoundUpSpanByteFullValueLength(GrowLength);
            functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(GrowLength));

            // Get a free record from a failed IPU.
            if (updateOp == UpdateOp.Upsert)
            {
                var status = session.Upsert(ref key, ref input, ref input, ref output);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }
            else if (updateOp == UpdateOp.RMW)
            {
                var status = session.RMW(ref key, ref input);
                Assert.IsTrue(status.Record.CopyUpdated, status.ToString());
            }

            Assert.Less(tailAddress, store.Log.TailAddress);
            Assert.AreEqual(1, RevivificationTestUtils.GetFreeRecordCount(store));
            output.Memory?.Dispose();
            output.Memory = null;
            tailAddress = store.Log.TailAddress;

            RevivificationTestUtils.WaitForRecords(store, want: true);

            // Get a new key and shrink the requested length so we revivify the free record from the failed IPU.
            keyVec.Fill(numRecords + 1);
            input = SpanByte.FromFixedSpan(inputVec.Slice(0, InitialLength));

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

            if (updateOp == UpdateOp.Upsert)
            {
                var status = session.Upsert(ref key, ref input, ref input, ref output);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }
            else if (updateOp == UpdateOp.RMW)
            {
                var status = session.RMW(ref key, ref input);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }

            Assert.AreEqual(tailAddress, store.Log.TailAddress);
            Assert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store));
            output.Memory?.Dispose();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SpanByteReadOnlyMinAddressTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            var tailAddress = store.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            byte fillByte = 42;
            keyVec.Fill(fillByte);
            var key = SpanByte.FromFixedSpan(keyVec);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            var status = session.Delete(ref key);
            Assert.IsTrue(status.Found, status.ToString());

            Assert.AreEqual(tailAddress, store.Log.TailAddress);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(fillByte);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);
            Assert.Greater(store.Log.TailAddress, tailAddress);
        }

        public enum UpdateKey { Unfound, DeletedAboveRO, DeletedBelowRO, CopiedBelowRO };

        const byte unfound = numRecords + 2;
        const byte delBelowRO = numRecords / 2 - 4;
        const byte copiedBelowRO = numRecords / 2 - 5;

        private long PrepareDeletes(bool stayInChain, byte delAboveRO, FlushMode flushMode, CollisionRange collisionRange)
        {
            Populate(0, numRecords / 2);

            FreeRecordPool<SpanByte, SpanByte> pool = default;
            if (stayInChain)
                pool = RevivificationTestUtils.SwapFreeRecordPool(store, pool);

            // Delete key below (what will be) the readonly line. This is for a target for the test; the record should not be revivified.
            Span<byte> keyVecDelBelowRO = stackalloc byte[KeyLength];
            keyVecDelBelowRO.Fill(delBelowRO);
            var delKeyBelowRO = SpanByte.FromFixedSpan(keyVecDelBelowRO);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            var status = session.Delete(ref delKeyBelowRO);
            Assert.IsTrue(status.Found, status.ToString());

            if (flushMode == FlushMode.ReadOnly)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            else if (flushMode == FlushMode.OnDisk)
                store.Log.FlushAndEvict(wait: true);

            Populate(numRecords / 2 + 1, numRecords);

            var tailAddress = store.Log.TailAddress;

            // Delete key above the readonly line. This is the record that will be revivified.
            // If not stayInChain, this also puts two elements in the free list; one should be skipped over on Take() as it is below readonly.
            Span<byte> keyVecDelAboveRO = stackalloc byte[KeyLength];
            keyVecDelAboveRO.Fill(delAboveRO);
            var delKeyAboveRO = SpanByte.FromFixedSpan(keyVecDelAboveRO);

            if (!stayInChain && collisionRange == CollisionRange.None)  // CollisionRange.Ten has a valid .PreviousAddress so won't be moved to FreeList
                RevivificationTestUtils.AssertElidable(store, ref delKeyAboveRO);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            status = session.Delete(ref delKeyAboveRO);
            Assert.IsTrue(status.Found, status.ToString());

            if (stayInChain)
            {
                Assert.IsFalse(RevivificationTestUtils.HasRecords(pool), "Expected empty pool");
                pool = RevivificationTestUtils.SwapFreeRecordPool(store, pool);
            }
            else if (collisionRange == CollisionRange.None)     // CollisionRange.Ten has a valid .PreviousAddress so won't be moved to FreeList
            {
                RevivificationTestUtils.WaitForRecords(store, want: true);
            }

            Assert.AreEqual(tailAddress, store.Log.TailAddress);

            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            return tailAddress;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        //[Repeat(300)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SpanByteUpdateRevivifyTest([Values] DeleteDest deleteDest, [Values] UpdateKey updateKey,
                                          [Values] CollisionRange collisionRange, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            bool stayInChain = deleteDest == DeleteDest.InChain || collisionRange != CollisionRange.None;   // Collisions make the key inelidable

            byte delAboveRO = (byte)(numRecords - (stayInChain
                ? (int)CollisionRange.Ten + 3       // Will remain in chain
                : 2));                              // Will be sent to free list

            long tailAddress = PrepareDeletes(stayInChain, delAboveRO, FlushMode.ReadOnly, collisionRange);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            Span<byte> keyVecToTest = stackalloc byte[KeyLength];
            var keyToTest = SpanByte.FromFixedSpan(keyVecToTest);

            bool expectReviv;
            if (updateKey == UpdateKey.Unfound || updateKey == UpdateKey.CopiedBelowRO)
            {
                // Unfound key should be satisfied from the freelist if !stayInChain, else will allocate a new record as it does not match the key chain.
                // CopiedBelowRO should be satisfied from the freelist if !stayInChain, else will allocate a new record as it does not match the key chain
                //      (but exercises a different code path than Unfound).
                // CollisionRange.Ten has a valid PreviousAddress so it is not elided from the cache.
                byte fillByte = updateKey == UpdateKey.Unfound ? unfound : copiedBelowRO;
                keyVecToTest.Fill(fillByte);
                inputVec.Fill(fillByte);
                expectReviv = !stayInChain && collisionRange != CollisionRange.Ten;
            }
            else if (updateKey == UpdateKey.DeletedBelowRO)
            {
                // DeletedBelowRO will not match the key for the in-chain above-RO slot, and we cannot reviv below RO or retrieve below-RO from the
                // freelist, so we will always allocate a new record unless we're using the freelist.
                byte fillByte = delBelowRO;
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
            functions.expectedSingleDestLength = InitialLength;

            if (!expectReviv)
                functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(input);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref keyToTest, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref keyToTest, ref input);

            if (expectReviv)
                Assert.AreEqual(tailAddress, store.Log.TailAddress);
            else
                Assert.Greater(store.Log.TailAddress, tailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SimpleRevivifyTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;
            if (stayInChain)
                _ = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            // This freed record stays in the hash chain.
            byte chainKey = numRecords / 2 - 1;
            Span<byte> keyVec = stackalloc byte[KeyLength];
            keyVec.Fill(chainKey);
            var key = SpanByte.FromFixedSpan(keyVec);
            if (!stayInChain)
                RevivificationTestUtils.AssertElidable(store, ref key);

            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            var status = session.Delete(ref key);
            Assert.IsTrue(status.Found, status.ToString());

            var tailAddress = store.Log.TailAddress;

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(chainKey);

            SpanByteAndMemory output = new();

            if (!stayInChain)
                RevivificationTestUtils.WaitForRecords(store, want: true);

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            Assert.AreEqual(tailAddress, store.Log.TailAddress);
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void DeleteEntireChainAndRevivifyTest([Values(CollisionRange.Ten)] CollisionRange collisionRange,
                                                     [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                                                     [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            // These freed records stay in the hash chain; we even skip the first one to ensure nothing goes into the free list.
            byte chainKey = 5;
            Span<byte> keyVec = stackalloc byte[KeyLength];
            keyVec.Fill(chainKey);
            var key = SpanByte.FromFixedSpan(keyVec);
            var hash = comparer.GetHashCode64(ref key);

            List<byte> deletedSlots = new();
            for (int ii = chainKey + 1; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                if (comparer.GetHashCode64(ref key) != hash)
                    continue;

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, status.ToString());
                if (ii > RevivificationTestUtils.GetMinRevivifiableKey(store, numRecords))
                    deletedSlots.Add((byte)ii);
            }

            // For this test we're still limiting to byte repetition
            Assert.Greater(255 - numRecords, deletedSlots.Count);
            RevivificationTestUtils.WaitForRecords(store, want: false);
            Assert.IsFalse(RevivificationTestUtils.HasRecords(store), "Expected empty pool");
            Assert.Greater(deletedSlots.Count, 5);    // should be about Ten
            var tailAddress = store.Log.TailAddress;

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);
            inputVec.Fill(chainKey);

            SpanByteAndMemory output = new();

            // Revivify in the chain. Because this stays in the chain, the expectedFullValueLength is roundup(InitialLength)
            for (int ii = 0; ii < deletedSlots.Count; ++ii)
            {
                keyVec.Fill(deletedSlots[ii]);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
                Assert.AreEqual(tailAddress, store.Log.TailAddress);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void DeleteAllRecordsAndRevivifyTest([Values(CollisionRange.None)] CollisionRange collisionRange,
                                                    [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                                                    [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            long tailAddress = store.Log.TailAddress;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            // "sizeof(int) +" because SpanByte has an int length prefix
            var recordSize = RecordInfo.GetLength() + RoundUp(sizeof(int) + keyVec.Length, 8) + RoundUp(sizeof(int) + InitialLength, 8);

            // Delete
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, status.ToString());
            }
            Assert.AreEqual(tailAddress, store.Log.TailAddress);
            Assert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, numRecords), RevivificationTestUtils.GetFreeRecordCount(store), $"Expected numRecords ({numRecords}) free records");

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;

            // These come from the existing initial allocation so keep the full length
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            // Revivify
            var revivifiableKeyCount = RevivificationTestUtils.GetRevivifiableRecordCount(store, numRecords);
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
                if (ii < revivifiableKeyCount)
                    Assert.AreEqual(tailAddress, store.Log.TailAddress, $"unexpected new record for key {ii}");
                else
                    Assert.Less(tailAddress, store.Log.TailAddress, $"unexpected revivified record for key {ii}");

                var status = session.Read(ref key, ref output);
                Assert.IsTrue(status.Found, $"Expected to find key {ii}; status == {status}");
            }

            Assert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store), "expected no free records remaining");
            RevivificationTestUtils.WaitForRecords(store, want: false);

            // Confirm
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                var status = session.Read(ref key, ref output);
                Assert.IsTrue(status.Found, $"Expected to find key {ii}; status == {status}");
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void DeleteAllRecordsAndTakeSnapshotTest([Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            // Delete
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, status.ToString());
            }
            Assert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, numRecords), RevivificationTestUtils.GetFreeRecordCount(store), $"Expected numRecords ({numRecords}) free records");

            _ = store.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot).GetAwaiter().GetResult();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void DeleteAllRecordsAndIterateTest([Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            // Delete
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);

                RevivificationTestUtils.AssertElidable(store, ref key);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, status.ToString());
            }
            Assert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, numRecords), RevivificationTestUtils.GetFreeRecordCount(store), $"Expected numRecords ({numRecords}) free records");

            using var iterator = session.Iterate();
            while (iterator.GetNext(out _))
                ;
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void BinSelectionTest([Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            FreeRecordPool<SpanByte, SpanByte> pool = store.RevivificationManager.FreeRecordPool;
            int expectedBin = 0, recordSize = RevivificationTestUtils.GetMaxRecordSize(pool, expectedBin);
            while (true)
            {
                Assert.IsTrue(pool.GetBinIndex(recordSize - 1, out int actualBin));
                Assert.AreEqual(expectedBin, actualBin);
                Assert.IsTrue(pool.GetBinIndex(recordSize, out actualBin));
                Assert.AreEqual(expectedBin, actualBin);

                if (++expectedBin == RevivificationTestUtils.GetBinCount(pool))
                {
                    Assert.IsFalse(pool.GetBinIndex(recordSize + 1, out actualBin));
                    Assert.AreEqual(-1, actualBin);
                    break;
                }
                Assert.IsTrue(pool.GetBinIndex(recordSize + 1, out actualBin));
                Assert.AreEqual(expectedBin, actualBin);
                recordSize = RevivificationTestUtils.GetMaxRecordSize(pool, expectedBin);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        //[Repeat(30)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public unsafe void ArtificialBinWrappingTest([Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            FreeRecordPool<SpanByte, SpanByte> pool = store.RevivificationManager.FreeRecordPool;

            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            Populate();

            const int recordSize = 42;

            Assert.IsTrue(pool.GetBinIndex(recordSize, out int binIndex));
            Assert.AreEqual(2, binIndex);

            const int minAddress = 1_000;
            int logicalAddress = 1_000_000;

            RevivificationStats revivStats = new();

            // Fill the bin, including wrapping around at the end.
            var recordCount = RevivificationTestUtils.GetRecordCount(pool, binIndex);
            for (var ii = 0; ii < recordCount; ++ii)
                Assert.IsTrue(store.RevivificationManager.TryAdd(logicalAddress + ii, recordSize, ref revivStats), "ArtificialBinWrappingTest: Failed to Add free record, pt 1");

            // Try to add to a full bin; this should fail.
            revivStats.Reset();
            Assert.IsFalse(store.RevivificationManager.TryAdd(logicalAddress + recordCount, recordSize, ref revivStats), "ArtificialBinWrappingTest: Expected to fail Adding free record");

            RevivificationTestUtils.WaitForRecords(store, want: true);

            for (var ii = 0; ii < recordCount; ++ii)
                Assert.IsTrue(RevivificationTestUtils.IsSet(pool, binIndex, ii), "expected bin to be set at ii == {ii}");

            // Take() one to open up a space in the bin, then add one
            revivStats.Reset();
            Assert.IsTrue(RevivificationTestUtils.TryTakeFromBin(pool, binIndex, recordSize, minAddress, store, out _, ref revivStats));
            revivStats.Reset();
            Assert.IsTrue(store.RevivificationManager.TryAdd(logicalAddress + recordCount + 1, recordSize, ref revivStats), "ArtificialBinWrappingTest: Failed to Add free record, pt 2");

            // Take() all records in the bin.
            revivStats.Reset();
            for (var ii = 0; ii < recordCount; ++ii)
                Assert.IsTrue(RevivificationTestUtils.TryTakeFromBin(pool, binIndex, recordSize, minAddress, store, out _, ref revivStats), $"ArtificialBinWrappingTest: failed to Take at ii == {ii}");
            var statsString = revivStats.Dump();
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        //[Repeat(3000)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public unsafe void LiveBinWrappingTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values] WaitMode waitMode, [Values] DeleteDest deleteDest,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            Populate();

            // Note: this test assumes no collisions (every delete goes to the FreeList)

            FreeRecordPool<SpanByte, SpanByte> pool = store.RevivificationManager.FreeRecordPool;

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            // "sizeof(int) +" because SpanByte has an int length prefix.
            var recordSize = RecordInfo.GetLength() + RoundUp(sizeof(int) + keyVec.Length, 8) + RoundUp(sizeof(int) + InitialLength, 8);
            Assert.IsTrue(pool.GetBinIndex(recordSize, out int binIndex));
            Assert.AreEqual(3, binIndex);

            // We should have a recordSize > min size record in the bin, to test wrapping.
            Assert.AreNotEqual(0, RevivificationTestUtils.GetSegmentStart(pool, binIndex, recordSize), "SegmentStart should not be 0, to test wrapping");

            // Delete 
            functions.expectedInputLength = InitialLength;
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                var status = session.Delete(ref key);
                Assert.IsTrue(status.Found, $"{status} for key {ii}");
                //Assert.AreEqual(ii + 1, RevivificationTestUtils.GetFreeRecordCount(store), $"mismatched free record count for key {ii}, pt 1");
            }

            if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait)
            {
                var actualNumRecords = RevivificationTestUtils.GetFreeRecordCount(store);
                Assert.AreEqual(RevivificationTestUtils.GetRevivifiableRecordCount(store, numRecords), actualNumRecords, $"mismatched free record count");
            }

            // Revivify
            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            for (var ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                long tailAddress = store.Log.TailAddress;

                SpanByteAndMemory output = new();
                if (updateOp == UpdateOp.Upsert)
                    session.Upsert(ref key, ref input, ref input, ref output);
                else if (updateOp == UpdateOp.RMW)
                    session.RMW(ref key, ref input);
                output.Memory?.Dispose();

                if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait && tailAddress != store.Log.TailAddress)
                {
                    var expectedReviv = ii < RevivificationTestUtils.GetRevivifiableRecordCount(store, numRecords);
                    if (expectedReviv != (tailAddress == store.Log.TailAddress))
                    {
                        var freeRecs = RevivificationTestUtils.GetFreeRecordCount(store);
                        if (expectedReviv)
                            Assert.AreEqual(tailAddress, store.Log.TailAddress, $"failed to revivify record for key {ii}, freeRecs {freeRecs}");
                        else
                            Assert.Less(tailAddress, store.Log.TailAddress, $"Unexpectedly revivified record for key {ii}, freeRecs {freeRecs}");
                    }
                }
            }

            if (deleteDest == DeleteDest.FreeList && waitMode == WaitMode.Wait)
            {
                Assert.AreEqual(0, RevivificationTestUtils.GetFreeRecordCount(store), "expected no free records remaining");
                RevivificationTestUtils.WaitForRecords(store, want: false);
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void LiveBinWrappingNoRevivTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp, [Values(RevivificationEnabled.NoReviv)] RevivificationEnabled revivEnabled,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            // For a comparison to the reviv version above.
            Populate();

            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            for (var iter = 0; iter < 100; ++iter)
            {
                // Delete 
                functions.expectedInputLength = InitialLength;
                for (var ii = 0; ii < numRecords; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    inputVec.Fill((byte)ii);

                    functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(iter == 0 ? InitialLength : InitialLength));
                    var status = session.Delete(ref key);
                    Assert.IsTrue(status.Found, $"{status} for key {ii}, iter {iter}");
                }

                for (var ii = 0; ii < numRecords; ++ii)
                {
                    keyVec.Fill((byte)ii);
                    inputVec.Fill((byte)ii);

                    functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

                    SpanByteAndMemory output = new();
                    if (updateOp == UpdateOp.Upsert)
                        session.Upsert(ref key, ref input, ref input, ref output);
                    else if (updateOp == UpdateOp.RMW)
                        session.RMW(ref key, ref input);
                    output.Memory?.Dispose();
                }
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SimpleOversizeRevivifyTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            bool stayInChain = deleteDest == DeleteDest.InChain;

            // Both in and out of chain revivification of oversize should have the same lengths.
            if (stayInChain)
                _ = RevivificationTestUtils.SwapFreeRecordPool(store, default);

            byte chainKey = numRecords + 1;
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[OversizeLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            keyVec.Fill(chainKey);
            inputVec.Fill(chainKey);

            // Oversize records in this test do not go to "next higher" bin (there is no next-higher bin in the default PowersOf2 bins we use)
            functions.expectedInputLength = OversizeLength;
            functions.expectedSingleDestLength = OversizeLength;
            functions.expectedConcurrentDestLength = OversizeLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(OversizeLength);
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(OversizeLength));

            // Initial insert of the oversize record
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            // Delete it
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(OversizeLength));
            var status = session.Delete(ref key);
            Assert.IsTrue(status.Found, status.ToString());
            if (!stayInChain)
                RevivificationTestUtils.WaitForRecords(store, want: true);

            var tailAddress = store.Log.TailAddress;

            // Revivify in the chain. Because this is oversize, the expectedFullValueLength remains the same
            functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(OversizeLength));
            if (updateOp == UpdateOp.Upsert)
                session.Upsert(ref key, ref input, ref input, ref output);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(ref key, ref input);

            Assert.AreEqual(tailAddress, store.Log.TailAddress);
        }

        public enum PendingOp { Read, RMW };

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SimplePendingOpsRevivifyTest([Values(CollisionRange.None)] CollisionRange collisionRange, [Values] PendingOp pendingOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            byte delAboveRO = numRecords - 2;   // Will be sent to free list
            byte targetRO = numRecords / 2 - 15;

            long tailAddress = PrepareDeletes(stayInChain: false, delAboveRO, FlushMode.OnDisk, collisionRange);

            // We always want freelist for this test.
            FreeRecordPool<SpanByte, SpanByte> pool = store.RevivificationManager.FreeRecordPool;
            Assert.IsTrue(RevivificationTestUtils.HasRecords(pool));

            SpanByteAndMemory output = new();

            functions.expectedInputLength = InitialLength;
            functions.expectedSingleDestLength = InitialLength;
            functions.expectedConcurrentDestLength = InitialLength;
            functions.expectedSingleFullValueLength = functions.expectedConcurrentFullValueLength = RoundUpSpanByteFullValueLength(InitialLength);

            // Use a different key below RO than we deleted; this will go pending to retrieve it
            Span<byte> keyVec = stackalloc byte[KeyLength];
            var key = SpanByte.FromFixedSpan(keyVec);

            if (pendingOp == PendingOp.Read)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                keyVec.Fill(targetRO);
                inputVec.Fill(targetRO);

                functions.expectedInputLength = InitialLength;
                functions.expectedSingleDestLength = InitialLength;
                functions.expectedConcurrentDestLength = InitialLength;

                var spanSlice = inputVec[..InitialLength];
                var inputSlice = SpanByte.FromFixedSpan(spanSlice);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));
                var status = session.Read(ref key, ref inputSlice, ref output);
                Assert.IsTrue(status.IsPending, status.ToString());
                session.CompletePending(wait: true);
                Assert.IsTrue(functions.readCcCalled);
            }
            else if (pendingOp == PendingOp.RMW)
            {
                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                keyVec.Fill(targetRO);
                inputVec.Fill(targetRO);

                functions.expectedUsedValueLengths.Enqueue(SpanByteTotalSize(InitialLength));

                session.RMW(ref key, ref input);
                session.CompletePending(wait: true);
                Assert.IsTrue(functions.rmwCcCalled);
            }
            Assert.AreEqual(tailAddress, store.Log.TailAddress);
        }
    }

    [TestFixture]
    class RevivificationObjectTests
    {
        const int numRecords = 1000;
        internal const int valueMult = 1_000_000;

        private MyFunctions functions;
        private TsavoriteKV<MyKey, MyValue> store;
        private ClientSession<MyKey, MyValue, MyInput, MyOutput, Empty, MyFunctions> session;
        private IDevice log;
        private IDevice objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.obj.log"), deleteOnClose: true);

            var concurrencyControlMode = ConcurrencyControlMode.LockTable;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ConcurrencyControlMode ccm)
                {
                    concurrencyControlMode = ccm;
                    continue;
                }
            }

            store = new TsavoriteKV<MyKey, MyValue>
                (128,
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 22, PageSizeBits = 12 },
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() },
                concurrencyControlMode: concurrencyControlMode, revivificationSettings: RevivificationSettings.DefaultFixedLength);

            functions = new MyFunctions();
            session = store.NewSession<MyInput, MyOutput, Empty, MyFunctions>(functions);
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
            for (int key = 0; key < numRecords; key++)
            {
                var keyObj = new MyKey { key = key };
                var valueObj = new MyValue { value = key + valueMult };
                var status = session.Upsert(keyObj, valueObj);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        [Test]
        [Category(RevivificationCategory)]
        [Category(SmokeTestCategory)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void SimpleObjectTest([Values] DeleteDest deleteDest, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            Populate();

            var deleteKey = RevivificationTestUtils.GetMinRevivifiableKey(store, numRecords);
            var tailAddress = store.Log.TailAddress;
            session.Delete(new MyKey { key = deleteKey });
            Assert.AreEqual(tailAddress, store.Log.TailAddress);

            var updateKey = deleteDest == DeleteDest.InChain ? deleteKey : numRecords + 1;

            var key = new MyKey { key = updateKey };
            var value = new MyValue { value = key.key + valueMult };
            var input = new MyInput { value = value.value };

            RevivificationTestUtils.WaitForRecords(store, want: true);
            Assert.IsTrue(RevivificationTestUtils.HasRecords(store.RevivificationManager.FreeRecordPool), "Expected a free record after delete and WaitForRecords");

            if (updateOp == UpdateOp.Upsert)
                session.Upsert(key, value);
            else if (updateOp == UpdateOp.RMW)
                session.RMW(key, input);

            RevivificationTestUtils.WaitForRecords(store, want: false);
            Assert.AreEqual(tailAddress, store.Log.TailAddress, "Expected tail address not to grow (record was revivified)");
        }
    }

    [TestFixture]
    class RevivificationSpanByteStressTests
    {
        const int KeyLength = 10;
        const int InitialLength = 50;

        internal class RevivificationStressFunctions : SpanByteFunctions<Empty>
        {
            internal ITsavoriteEqualityComparer<SpanByte> keyComparer;     // non-null if we are doing key comparisons (and thus expectedKey is non-default)
            internal SpanByte expectedKey = default;                    // Set for each operation by the calling thread
            internal bool isFirstLap = true;                            // For first 

            internal RevivificationStressFunctions(ITsavoriteEqualityComparer<SpanByte> keyComparer) => this.keyComparer = keyComparer;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void VerifyKey(ref SpanByte functionsKey)
            {
                if (keyComparer is not null)
                    Assert.IsTrue(keyComparer.Equals(ref expectedKey, ref functionsKey));
            }

            private void VerifyKeyAndValue(ref SpanByte functionsKey, ref SpanByte functionsValue)
            {
                if (keyComparer is not null)
                    Assert.IsTrue(keyComparer.Equals(ref expectedKey, ref functionsKey), "functionsKey does not equal expectedKey");

                // Even in CompletePending(), we can verify internal consistency of key/value
                int valueOffset = 0, valueLengthRemaining = functionsValue.Length;
                Assert.Less(functionsKey.Length, valueLengthRemaining);
                while (valueLengthRemaining > 0)
                {
                    var compareLength = Math.Min(functionsKey.Length, valueLengthRemaining);
                    Span<byte> valueSpan = functionsValue.AsSpan().Slice(valueOffset, compareLength);
                    Span<byte> keySpan = functionsKey.AsSpan()[..compareLength];
                    Assert.IsTrue(valueSpan.SequenceEqual(keySpan), $"functionsValue (offset {valueOffset}, len {compareLength}: {SpanByte.FromFixedSpan(valueSpan)}) does not match functionsKey ({SpanByte.FromFixedSpan(keySpan)})");
                    valueOffset += compareLength;
                    valueLengthRemaining -= compareLength;
                }
            }

            public override bool SingleWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                VerifyKey(ref key);
                return base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);
            }

            public override bool ConcurrentWriter(ref SpanByte key, ref SpanByte input, ref SpanByte src, ref SpanByte dst, ref SpanByteAndMemory output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                VerifyKeyAndValue(ref key, ref dst);
                return base.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo);
            }

            public override bool InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                VerifyKey(ref key);
                return base.InitialUpdater(ref key, ref input, ref newValue, ref output, ref rmwInfo, ref recordInfo);
            }

            public override bool CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                VerifyKeyAndValue(ref key, ref oldValue);
                return base.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);
            }

            public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value, ref SpanByteAndMemory output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                VerifyKeyAndValue(ref key, ref value);
                return base.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
            }

            public override bool SingleDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
                => base.SingleDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);

            public override unsafe bool ConcurrentDeleter(ref SpanByte key, ref SpanByte value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
                => base.ConcurrentDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);
        }

        const int numRecords = 200;
        const int DefaultMaxRecsPerBin = 1024;

        RevivificationStressFunctions functions;
        RevivificationSpanByteComparer comparer;

        private TsavoriteKV<SpanByte, SpanByte> store;
        private ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions> session;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Combine(MethodTestDir, "test.log"), deleteOnClose: true);

            CollisionRange collisionRange = CollisionRange.None;
            LogSettings logSettings = new() { LogDevice = log, ObjectLogDevice = null, PageSizeBits = 17, MemorySizeBits = 20 };
            var concurrencyControlMode = ConcurrencyControlMode.LockTable;
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is CollisionRange cr)
                {
                    collisionRange = cr;
                    continue;
                }
                if (arg is ConcurrencyControlMode ccm)
                {
                    concurrencyControlMode = ccm;
                    continue;
                }
            }

            comparer = new RevivificationSpanByteComparer(collisionRange);
            store = new TsavoriteKV<SpanByte, SpanByte>(1L << 16, logSettings, comparer: comparer, concurrencyControlMode: concurrencyControlMode, revivificationSettings: RevivificationSettings.PowerOf2Bins);

            functions = new RevivificationStressFunctions(keyComparer: null);
            session = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(functions);
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
            var key = SpanByte.FromFixedSpan(keyVec);

            Span<byte> inputVec = stackalloc byte[InitialLength];
            var input = SpanByte.FromFixedSpan(inputVec);

            SpanByteAndMemory output = new();

            for (int ii = 0; ii < numRecords; ++ii)
            {
                keyVec.Fill((byte)ii);
                inputVec.Fill((byte)ii);

                var status = session.Upsert(ref key, ref input, ref input, ref output);
                Assert.IsTrue(status.Record.Created, status.ToString());
            }
        }

        const int AddressIncrement = 1_000_000; // must be > ReadOnlyAddress

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
            List<int> strayFlags = new(0), strayRecords = new();

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
                Assert.IsTrue(strayFlags.Count == 0 && strayRecords.Count == 0 && maxRecords == totalTaken,
                              $"maxRec/taken {maxRecords}/{totalTaken}, strayflags {strayFlags.Count}, strayRecords {strayRecords.Count}, iteration {iteration}");
            }

            void runAddThread(int tid)
            {
                RevivificationStats revivStats = new();
                for (var ii = 0; ii < numRecordsPerThread; ++ii)
                {
                    var addressBase = ii + tid * numRecordsPerThread;
                    var flag = flags[addressBase];
                    Assert.AreEqual(Unadded, flag, $"Invalid flag {flag} trying to add addressBase {addressBase}, tid {tid}, iteration {iteration}");
                    flags[addressBase] = 1;
                    Assert.IsTrue(freeRecordPool.TryAdd(addressBase + AddressIncrement, recordSize, ref revivStats), $"Failed to add addressBase {addressBase}, tid {tid}, iteration {iteration}");
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
                        Assert.AreEqual(1, prevFlag, $"Take() found unexpected addressBase {addressBase} (flag {prevFlag}), tid {tid}, iteration {iteration}");
                        Interlocked.Increment(ref totalTaken);
                    }
                }
            }

            // Task rather than Thread for propagation of exception.
            List<Task> tasks = new();

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
                    Assert.IsTrue(Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(timeoutSec)), $"Task timeout at {timeoutSec} sec, maxRec/taken {maxRecords}/{totalTaken}, iteration {iteration}");
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
            Assert.IsTrue(freeRecordPool.TryAdd(AddressIncrement + 1, TakeSize, ref revivStats));
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress: AddressIncrement, out var address, ref revivStats));

            Assert.AreEqual(AddressIncrement + 1, address, "out address");
            Assert.AreEqual(1, revivStats.successfulAdds, "Successful Adds");
            Assert.AreEqual(1, revivStats.successfulTakes, "Successful Takes");
            var statsString = revivStats.Dump();
        }

        public enum WrapMode { Wrap, NoWrap };
        const int TakeSize = 40;

        private FreeRecordPool<SpanByte, SpanByte> CreateBestFitTestPool(int scanLimit, WrapMode wrapMode, ref RevivificationStats revivStats)
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
                    Assert.IsTrue(freeRecordPool.TryAdd(minAddress + ii + 1, smallSize, ref revivStats));

                // Now take out the four at the beginning.
                for (var ii = 0; ii < 4; ++ii, ++expectedTakes)
                    Assert.IsTrue(freeRecordPool.TryTake(smallSize, minAddress, out _, ref revivStats));
            }

            long address = AddressIncrement;
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize + 1, ref revivStats));
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize + 2, ref revivStats));
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize + 3, ref revivStats));
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize, ref revivStats));    // 4
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize, ref revivStats));    // 5 
            Assert.IsTrue(freeRecordPool.TryAdd(++address, TakeSize, ref revivStats));
            expectedAdds += 6;

            Assert.AreEqual(expectedAdds, revivStats.successfulAdds, "Successful Adds");
            Assert.AreEqual(expectedTakes, revivStats.successfulTakes, "Successful Takes");

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
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out var address, ref revivStats));
            Assert.AreEqual(4, address -= AddressIncrement);
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            Assert.AreEqual(5, address -= AddressIncrement);
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            Assert.AreEqual(1, address -= AddressIncrement);

            // Now that we've taken the first item, the new first-fit will be moved up one, which brings the last exact-fit into scanLimit range.
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            Assert.AreEqual(6, address -= AddressIncrement);

            // Now Take will return them in order until we have no more
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            Assert.AreEqual(2, address -= AddressIncrement);
            Assert.IsTrue(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            Assert.AreEqual(3, address -= AddressIncrement);
            Assert.IsFalse(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));
            expectedTakes += 6; // Plus one failure

            Assert.AreEqual(expectedTakes, revivStats.successfulTakes, "Successful Takes");
            Assert.AreEqual(1, revivStats.failedTakes, "Failed Takes");
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
                Assert.AreEqual(ii + 1, address -= AddressIncrement, $"address comparison failed at ii {ii}");
            }
            Assert.IsFalse(freeRecordPool.TryTake(TakeSize, minAddress, out address, ref revivStats));

            Assert.AreEqual(expectedSuccessfulTakes, revivStats.successfulTakes, "Successful Takes");
            Assert.AreEqual(1, revivStats.failedTakes, "Failed Takes");
            if (wrapMode == WrapMode.NoWrap)
                Assert.AreEqual(1, revivStats.takeEmptyBins, "Empty Bins");
            else
                Assert.AreEqual(1, revivStats.takeRecordSizeFailures, "Record Size");
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
                        Assert.IsTrue(freeRecordPool.TryAdd(TestAddress, size, ref revivStats), $"Failed TryAdd on iter {iteration}");
                        ++counter;
                    }
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < 8; t++)
            {
                var tid = t + 1;
                tasks.Add(Task.Factory.StartNew(() => runThread(tid)));
            }
            Task.WaitAll(tasks.ToArray());

            Assert.IsTrue(counter == 0);
        }

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(3000)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void LiveThreadContentionOnOneRecordTest([Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
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

                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numDeleteThreads)
                    {
                        var kk = rng.Next(keyRange);
                        keyVec.Fill((byte)kk);
                        localSession.Delete(key);
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                Random rng = new(tid * 101);

                RevivificationStressFunctions localFunctions = new(keyComparer: store.comparer);
                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(localFunctions);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numUpdateThreads)
                    {
                        var kk = rng.Next(keyRange);
                        keyVec.Fill((byte)kk);
                        inputVec.Fill((byte)kk);

                        localSession.functions.expectedKey = key;
                        if (updateOp == UpdateOp.Upsert)
                            localSession.Upsert(key, input);
                        else
                            localSession.RMW(key, input);
                        localSession.functions.expectedKey = default;
                    }

                    // Clear keyComparer so it does not try to validate during CompletePending (when it doesn't have an expectedKey)
                    localFunctions.keyComparer = null;
                    localSession.CompletePending(wait: true);
                    localFunctions.keyComparer = store.comparer;
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
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
            Task.WaitAll(tasks.ToArray());
        }

        public enum ThreadingPattern { SameKeys, RandomKeys };

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(3000)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void LiveFreeListThreadStressTest([Values] CollisionRange collisionRange,
                                             [Values] ThreadingPattern threadingPattern, [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                                             [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
        {
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            int numIterations = 100;
            const int numDeleteThreads = 5, numUpdateThreads = 5;

            unsafe void runDeleteThread(int tid)
            {
                Random rng = new(tid * 101);

                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(new RevivificationStressFunctions(keyComparer: null));

                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numDeleteThreads)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(numRecords) : ii;
                        keyVec.Fill((byte)kk);
                        localSession.Delete(key);
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                Random rng = new(tid * 101);

                RevivificationStressFunctions localFunctions = new(keyComparer: store.comparer);
                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(localFunctions);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numUpdateThreads)
                    {
                        var kk = threadingPattern == ThreadingPattern.RandomKeys ? rng.Next(numRecords) : ii;
                        keyVec.Fill((byte)kk);
                        inputVec.Fill((byte)kk);

                        localSession.functions.expectedKey = key;
                        if (updateOp == UpdateOp.Upsert)
                            localSession.Upsert(key, input);
                        else
                            localSession.RMW(key, input);
                        localSession.functions.expectedKey = default;
                    }

                    // Clear keyComparer so it does not try to validate during CompletePending (when it doesn't have an expectedKey)
                    localFunctions.keyComparer = null;
                    localSession.CompletePending(wait: true);
                    localFunctions.keyComparer = store.comparer;
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
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
            Task.WaitAll(tasks.ToArray());
        }

        [Test]
        [Category(RevivificationCategory)]
        //[Repeat(30)]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Style", "IDE0060:Remove unused parameter", Justification = "concurrencyControlMode is used by Setup")]
        public void LiveInChainThreadStressTest([Values(CollisionRange.Ten)] CollisionRange collisionRange,
                                                [Values(UpdateOp.Upsert, UpdateOp.RMW)] UpdateOp updateOp,
                                                [Values(ConcurrencyControlMode.LockTable, ConcurrencyControlMode.RecordIsolation)] ConcurrencyControlMode concurrencyControlMode)
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

                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numDeleteThreads)
                    {
                        keyVec.Fill((byte)ii);
                        localSession.Delete(key);
                    }
                }
            }

            unsafe void runUpdateThread(int tid)
            {
                Span<byte> keyVec = stackalloc byte[KeyLength];
                var key = SpanByte.FromFixedSpan(keyVec);

                Span<byte> inputVec = stackalloc byte[InitialLength];
                var input = SpanByte.FromFixedSpan(inputVec);

                RevivificationStressFunctions localFunctions = new RevivificationStressFunctions(keyComparer: null);
                using var localSession = store.NewSession<SpanByte, SpanByteAndMemory, Empty, RevivificationStressFunctions>(localFunctions);

                for (var iteration = 0; iteration < numIterations; ++iteration)
                {
                    for (var ii = tid; ii < numRecords; ii += numUpdateThreads)
                    {
                        keyVec.Fill((byte)ii);
                        inputVec.Fill((byte)ii);

                        localSession.functions.expectedKey = key;
                        if (updateOp == UpdateOp.Upsert)
                            localSession.Upsert(key, input);
                        else
                            localSession.RMW(key, input);
                        localSession.functions.expectedKey = default;
                    }

                    // Clear keyComparer so it does not try to validate during CompletePending (when it doesn't have an expectedKey)
                    localFunctions.keyComparer = null;
                    localSession.CompletePending(wait: true);
                    localFunctions.keyComparer = store.comparer;
                }
            }

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
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
            Task.WaitAll(tasks.ToArray());
        }
    }
}