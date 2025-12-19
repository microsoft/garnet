// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LockTests
{
    using StructStoreFunctions = StoreFunctions<LongKeyComparerModulo, SpanByteRecordDisposer>;

    [TestFixture]
    public class BasicLockTests
    {
        internal class Functions : SimpleLongSimpleFunctions
        {
            internal bool throwOnInitialUpdater;
            internal long initialUpdaterThrowAddress;

            static bool Increment(Span<byte> field)
            {
                ++field.AsRef<long>();
                return true;
            }

            public override bool InPlaceWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref long input, ReadOnlySpan<byte> srcValue, ref long output, ref UpsertInfo upsertInfo)
            {
                return Increment(logRecord.ValueSpan);
            }

            public override bool InPlaceUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
            {
                return Increment(logRecord.ValueSpan);
            }

            public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = rmwInfo.Address;
                    throw new TsavoriteException(nameof(throwOnInitialUpdater));
                }
                return base.InitialUpdater(ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
            }

            public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref long input, ReadOnlySpan<byte> srcValue, ref long output, ref UpsertInfo upsertInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = upsertInfo.Address;
                    throw new TsavoriteException(nameof(throwOnInitialUpdater));
                }
                return base.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }

            public override bool InitialDeleter(ref LogRecord logRecord, ref DeleteInfo deleteInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = deleteInfo.Address;
                    throw new TsavoriteException(nameof(throwOnInitialUpdater));
                }
                return base.InitialDeleter(ref logRecord, ref deleteInfo);
            }
        }

        private TsavoriteKV<StructStoreFunctions, SpanByteAllocator<StructStoreFunctions>> store;
        private ClientSession<long, long, Empty, Functions, StructStoreFunctions, SpanByteAllocator<StructStoreFunctions>> session;
        private BasicContext<long, long, Empty, Functions, StructStoreFunctions, SpanByteAllocator<StructStoreFunctions>> bContext;
        private IDevice log;
        private LongKeyComparerModulo keyComparer = new(NumRecords);

        const int NumRecords = 100;
        const int ValueMult = 1000000;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectStringTests.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log
            }, StoreFunctions.Create(keyComparer, SpanByteRecordDisposer.Instance)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<long, long, Empty, Functions>(new Functions());
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

        [Test]
        [Category("TsavoriteKV")]
        public void FunctionsLockTest([Values(1, 20)] int numThreads)
        {
            // Populate
            for (long key = 0; key < NumRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                long valueNum = key * ValueMult;
                ClassicAssert.IsFalse(bContext.Upsert(SpanByte.FromPinnedVariable(ref key), SpanByte.FromPinnedVariable(ref valueNum)).IsPending);
            }

            // Update
            const int numIters = 500;
            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(() => UpdateFunc((ii & 1) == 0, NumRecords, numIters))).ToArray();
            Task.WaitAll(tasks);

            // Verify
            for (long key = 0; key < NumRecords; key++)
            {
                var expectedOutput = key * ValueMult + numThreads * numIters;
                long output = 0;
                ClassicAssert.IsFalse(bContext.Read(SpanByte.FromPinnedVariable(ref key), ref output).IsPending);
                ClassicAssert.AreEqual(expectedOutput, output);
            }
        }

        void UpdateFunc(bool useRMW, int numRecords, int numIters)
        {
            for (long keyNum = 0; keyNum < numRecords; ++keyNum)
            {
                var key = SpanByte.FromPinnedVariable(ref keyNum);
                for (var iter = 0; iter < numIters; iter++)
                {
                    if ((iter & 7) == 7)
                        ClassicAssert.IsFalse(bContext.Read(key).status.IsPending);

                    // These will both just increment the stored value, ignoring the input argument.
                    long input = default;
                    _ = useRMW ? bContext.RMW(key, ref input) : bContext.Upsert(key, SpanByte.FromPinnedVariable(ref input));
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public unsafe void CollidingDeletedRecordTest([Values(UpdateOp.RMW, UpdateOp.Upsert)] UpdateOp updateOp, [Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode flushMode)
        {
            // Populate
            long keyNum = 0, valueNum = 0;
            for (keyNum = 0; keyNum < NumRecords; keyNum++)
            {
                // For this test we should be in-memory, so no pending
                valueNum = keyNum * ValueMult;
                ClassicAssert.IsFalse(bContext.Upsert(SpanByte.FromPinnedVariable(ref keyNum), SpanByte.FromPinnedVariable(ref valueNum)).IsPending);
            }

            // Insert a colliding key so we don't elide the deleted key from the hash chain.
            long deleteKeyNum = NumRecords / 2;
            long collidingKeyNum = deleteKeyNum + NumRecords;
            keyNum = collidingKeyNum;
            valueNum = collidingKeyNum * ValueMult;
            Span<byte> key = SpanByte.FromPinnedVariable(ref keyNum), value = SpanByte.FromPinnedVariable(ref valueNum), deleteKey = SpanByte.FromPinnedVariable(ref deleteKeyNum);
            ClassicAssert.IsFalse(bContext.Upsert(key, value).IsPending);

            // Now make sure we did collide
            HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(deleteKey));
            ClassicAssert.IsTrue(store.FindTag(ref hei), "Cannot find deleteKey entry");
            ClassicAssert.Greater(hei.Address, LogAddress.kInvalidAddress, "Couldn't find deleteKey Address");
            var physicalAddress = store.hlogBase.GetPhysicalAddress(hei.Address);
            var lookupKey = LogRecord.GetInlineKey(physicalAddress);
            ClassicAssert.AreEqual(collidingKeyNum, lookupKey.AsRef<long>(), "Expected collidingKey");

            // Backtrace to deleteKey
            physicalAddress = store.hlogBase.GetPhysicalAddress(LogRecord.GetInfo(physicalAddress).PreviousAddress);
            lookupKey = LogRecord.GetInlineKey(physicalAddress);
            ClassicAssert.AreEqual(deleteKey.AsRef<long>(), lookupKey.AsRef<long>(), "Expected deleteKey");
            ClassicAssert.IsFalse(LogRecord.GetInfo(physicalAddress).Tombstone, "Tombstone should be false");

            // In-place delete.
            ClassicAssert.IsFalse(bContext.Delete(deleteKey).IsPending);
            ClassicAssert.IsTrue(LogRecord.GetInfo(physicalAddress).Tombstone, "Tombstone should be true after Delete");

            if (flushMode == FlushMode.ReadOnly)
                _ = store.hlogBase.ShiftReadOnlyAddress(store.Log.TailAddress);

            var status = updateOp switch
            {
                UpdateOp.RMW => bContext.RMW(deleteKey, ref valueNum),
                UpdateOp.Upsert => bContext.Upsert(deleteKey, value),
                UpdateOp.Delete => throw new InvalidOperationException("UpdateOp.Delete not expected in this test"),
                _ => throw new InvalidOperationException($"Unknown updateOp {updateOp}")
            };
            ClassicAssert.IsFalse(status.IsPending);

            ClassicAssert.IsTrue(LogRecord.GetInfo(physicalAddress).Tombstone, "Tombstone should be true after Update");
        }

        [Test]
        [Category("TsavoriteKV")]
        public unsafe void SetInvalidOnException([Values] UpdateOp updateOp)
        {
            // Don't modulo the hash codes.
            keyComparer.mod = int.MaxValue;

            // Populate
            for (long keyNum = 0; keyNum < NumRecords; keyNum++)
            {
                // For this test we should be in-memory, so no pending
                long valueNum = keyNum * ValueMult;
                ClassicAssert.IsFalse(bContext.Upsert(SpanByte.FromPinnedVariable(ref keyNum), SpanByte.FromPinnedVariable(ref valueNum)).IsPending);
            }

            var expectedThrowAddress = store.Log.TailAddress;
            session.functions.throwOnInitialUpdater = true;

            // Delete must try with an existing key; Upsert and Delete should insert a new key
            long deleteKeyNum = NumRecords / 2;
            long insertKeyNum = NumRecords + 1;

            // Make sure everything will create a new record.
            store.Log.FlushAndEvict(wait: true);

            var threw = false;
            try
            {
                long input = 0;
                var status = updateOp switch
                {
                    UpdateOp.RMW => bContext.RMW(SpanByte.FromPinnedVariable(ref insertKeyNum), ref input),
                    UpdateOp.Upsert => bContext.Upsert(SpanByte.FromPinnedVariable(ref insertKeyNum), SpanByte.FromPinnedVariable(ref input)),
                    UpdateOp.Delete => bContext.Delete(SpanByte.FromPinnedVariable(ref deleteKeyNum)),
                    _ => throw new InvalidOperationException($"Unknown updateOp {updateOp}")
                };
                ClassicAssert.IsFalse(status.IsPending);
            }
            catch (TsavoriteException ex)
            {
                ClassicAssert.AreEqual(nameof(session.functions.throwOnInitialUpdater), ex.Message);
                threw = true;
            }

            ClassicAssert.IsTrue(threw, "Test should have thrown");
            ClassicAssert.AreEqual(expectedThrowAddress, session.functions.initialUpdaterThrowAddress, "Unexpected throw address");

            var physicalAddress = store.hlogBase.GetPhysicalAddress(expectedThrowAddress);
            var recordInfo = LogRecord.GetInfo(physicalAddress);
            ClassicAssert.IsTrue(recordInfo.Invalid, "Expected Invalid record");
        }
    }
}