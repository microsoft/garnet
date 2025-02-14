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
    using StructStoreFunctions = StoreFunctions<SpanByte, LongKeyComparerModulo, SpanByteRecordDisposer>;

    [TestFixture]
    public class BasicLockTests
    {
        internal class Functions : SimpleLongSimpleFunctions
        {
            internal bool throwOnInitialUpdater;
            internal long initialUpdaterThrowAddress;

            static bool Increment(SpanByte field)
            {
                ++field.AsRef<long>();
                return true;
            }

            public override bool ConcurrentWriter(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref long input, SpanByte srcValue, ref long output, ref UpsertInfo upsertInfo)
            {
                return Increment(logRecord.ValueSpan);
            }

            public override bool InPlaceUpdater(ref LogRecord<SpanByte> logRecord, ref RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
            {
                return Increment(logRecord.ValueSpan);
            }

            public override bool InitialUpdater(ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref long input, ref long output, ref RMWInfo rmwInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = rmwInfo.Address;
                    throw new TsavoriteException(nameof(throwOnInitialUpdater));
                }
                return base.InitialUpdater(ref dstLogRecord, ref sizeInfo, ref input, ref output, ref rmwInfo);
            }

            public override bool SingleWriter(ref LogRecord<SpanByte> dstLogRecord, ref RecordSizeInfo sizeInfo, ref long input, SpanByte srcValue, ref long output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = upsertInfo.Address;
                    throw new TsavoriteException(nameof(throwOnInitialUpdater));
                }
                return base.SingleWriter(ref dstLogRecord, ref sizeInfo, ref input, srcValue, ref output, ref upsertInfo, reason);
            }

            public override bool SingleDeleter(ref LogRecord<SpanByte> logRecord, ref DeleteInfo deleteInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = deleteInfo.Address;
                    throw new TsavoriteException(nameof(throwOnInitialUpdater));
                }
                return base.SingleDeleter(ref logRecord, ref deleteInfo);
            }
        }

        private TsavoriteKV<SpanByte, StructStoreFunctions, SpanByteAllocator<StructStoreFunctions>> store;
        private ClientSession<SpanByte, long, long, Empty, Functions, StructStoreFunctions, SpanByteAllocator<StructStoreFunctions>> session;
        private BasicContext<SpanByte, long, long, Empty, Functions, StructStoreFunctions, SpanByteAllocator<StructStoreFunctions>> bContext;
        private IDevice log;
        private LongKeyComparerModulo keyComparer = new(NumRecords);

        const int NumRecords = 100;
        const int ValueMult = 1000000;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericStringTests.log"), deleteOnClose: true);
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log
            }, StoreFunctions<SpanByte>.Create(keyComparer, SpanByteRecordDisposer.Instance)
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
                ClassicAssert.IsFalse(bContext.Upsert(SpanByteFrom(ref key), SpanByteFrom(ref valueNum)).IsPending);
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
                ClassicAssert.IsFalse(bContext.Read(SpanByteFrom(ref key), ref output).IsPending);
                ClassicAssert.AreEqual(expectedOutput, output);
            }
        }

        void UpdateFunc(bool useRMW, int numRecords, int numIters)
        {
            for (var keyNum = 0; keyNum < numRecords; ++keyNum)
            {
                var key = SpanByteFrom(ref keyNum);
                for (var iter = 0; iter < numIters; iter++)
                {
                    if ((iter & 7) == 7)
                        ClassicAssert.IsFalse(bContext.Read(key).status.IsPending);

                    // These will both just increment the stored value, ignoring the input argument.
                    _ = useRMW ? bContext.RMW(key, default) : bContext.Upsert(key, default);
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
                ClassicAssert.IsFalse(bContext.Upsert(SpanByteFrom(ref keyNum), SpanByteFrom(ref valueNum)).IsPending);
            }

            // Insert a colliding key so we don't elide the deleted key from the hash chain.
            long deleteKeyNum = NumRecords / 2;
            long collidingKey = deleteKeyNum + NumRecords;
            keyNum = collidingKey;
            valueNum = collidingKey * ValueMult;
            SpanByte key = SpanByteFrom(ref keyNum), value = SpanByteFrom(ref valueNum), deleteKey = SpanByteFrom(ref deleteKeyNum);
            ClassicAssert.IsFalse(bContext.Upsert(key, value).IsPending);

            // Now make sure we did collide
            HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(deleteKey));
            ClassicAssert.IsTrue(store.FindTag(ref hei), "Cannot find deleteKey entry");
            ClassicAssert.Greater(hei.Address, Constants.kInvalidAddress, "Couldn't find deleteKey Address");
            var physicalAddress = store.hlog.GetPhysicalAddress(hei.Address);
            var recordInfo = LogRecord.GetInfo(physicalAddress);
            var lookupKey = LogRecord.GetKey(physicalAddress);
            ClassicAssert.AreEqual(collidingKey, lookupKey, "Expected collidingKey");

            // Backtrace to deleteKey
            physicalAddress = store.hlog.GetPhysicalAddress(recordInfo.PreviousAddress);
            recordInfo = LogRecord.GetInfo(physicalAddress);
            lookupKey = LogRecord.GetKey(physicalAddress);
            ClassicAssert.AreEqual(deleteKey, lookupKey, "Expected deleteKey");
            ClassicAssert.IsFalse(recordInfo.Tombstone, "Tombstone should be false");

            // In-place delete.
            ClassicAssert.IsFalse(bContext.Delete(deleteKey).IsPending);
            ClassicAssert.IsTrue(recordInfo.Tombstone, "Tombstone should be true after Delete");

            if (flushMode == FlushMode.ReadOnly)
                _ = store.hlogBase.ShiftReadOnlyAddress(store.Log.TailAddress);

            var status = updateOp switch
            {
                UpdateOp.RMW => bContext.RMW(deleteKey, default),
                UpdateOp.Upsert => bContext.Upsert(deleteKey, default),
                UpdateOp.Delete => throw new InvalidOperationException("UpdateOp.Delete not expected in this test"),
                _ => throw new InvalidOperationException($"Unknown updateOp {updateOp}")
            };
            ClassicAssert.IsFalse(status.IsPending);

            ClassicAssert.IsTrue(recordInfo.Tombstone, "Tombstone should be true after Update");
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
                ClassicAssert.IsFalse(bContext.Upsert(SpanByteFrom(ref keyNum), SpanByteFrom(ref valueNum)).IsPending);
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
                var status = updateOp switch
                {
                    UpdateOp.RMW => bContext.RMW(SpanByteFrom(ref insertKeyNum), default),
                    UpdateOp.Upsert => bContext.Upsert(SpanByteFrom(ref insertKeyNum), default),
                    UpdateOp.Delete => bContext.Delete(SpanByteFrom(ref deleteKeyNum)),
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

            var physicalAddress = store.hlog.GetPhysicalAddress(expectedThrowAddress);
            var recordInfo = LogRecord.GetInfo(physicalAddress);
            ClassicAssert.IsTrue(recordInfo.Invalid, "Expected Invalid record");
        }
    }
}