// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LockTests
{
    // Must be in a separate block so the "using StructStoreFunctions" is the first line in its namespace declaration.
    internal sealed class LocalIntKeyComparer : IKeyComparer<int>
    {
        internal int mod;

        internal LocalIntKeyComparer(int mod) => this.mod = mod;

        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        public long GetHashCode64(ref int k) => Utility.GetHashCode(k % mod);
    }
}

namespace Tsavorite.test.LockTests
{
    using StructStoreFunctions = StoreFunctions<int, int, LocalIntKeyComparer, DefaultRecordDisposer<int, int>>;

    [AllureNUnit]
    [TestFixture]
    public class BasicLockTests : AllureTestBase
    {
        internal class Functions : SimpleSimpleFunctions<int, int>
        {
            internal bool throwOnInitialUpdater;
            internal long initialUpdaterThrowAddress;

            static bool Increment(ref int dst)
            {
                ++dst;
                return true;
            }

            public override bool ConcurrentWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo) => Increment(ref dst);

            public override bool InPlaceUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => Increment(ref value);

            public override bool InitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = rmwInfo.Address;
                    throw new TsavoriteException(nameof(throwOnInitialUpdater));
                }
                return base.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
            }

            public override bool SingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = upsertInfo.Address;
                    throw new TsavoriteException(nameof(throwOnInitialUpdater));
                }
                return base.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);
            }

            public override bool SingleDeleter(ref int key, ref int value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            {
                if (throwOnInitialUpdater)
                {
                    initialUpdaterThrowAddress = deleteInfo.Address;
                    throw new TsavoriteException(nameof(throwOnInitialUpdater));
                }
                return base.SingleDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);
            }
        }

        private TsavoriteKV<int, int, StructStoreFunctions, BlittableAllocator<int, int, StructStoreFunctions>> store;
        private ClientSession<int, int, int, int, Empty, Functions, StructStoreFunctions, BlittableAllocator<int, int, StructStoreFunctions>> session;
        private BasicContext<int, int, int, int, Empty, Functions, StructStoreFunctions, BlittableAllocator<int, int, StructStoreFunctions>> bContext;
        private IDevice log;
        private LocalIntKeyComparer keyComparer = new(NumRecords);

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
            }, StoreFunctions<int, int>.Create(keyComparer)
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions)
            );
            session = store.NewSession<int, int, Empty, Functions>(new Functions());
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

            OnTearDown();
        }

        [Test]
        [Category("TsavoriteKV")]
        public void FunctionsLockTest([Values(1, 20)] int numThreads)
        {
            // Populate
            for (var key = 0; key < NumRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                ClassicAssert.IsFalse(bContext.Upsert(key, key * ValueMult).IsPending);
            }

            // Update
            const int numIters = 500;
            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(() => UpdateFunc((ii & 1) == 0, NumRecords, numIters))).ToArray();
            Task.WaitAll(tasks);

            // Verify
            for (var key = 0; key < NumRecords; key++)
            {
                var expectedValue = key * ValueMult + numThreads * numIters;
                ClassicAssert.IsFalse(bContext.Read(key, out var value).IsPending);
                ClassicAssert.AreEqual(expectedValue, value);
            }
        }

        void UpdateFunc(bool useRMW, int numRecords, int numIters)
        {
            for (var key = 0; key < numRecords; ++key)
            {
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
            for (var key = 0; key < NumRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                ClassicAssert.IsFalse(bContext.Upsert(key, key * ValueMult).IsPending);
            }

            // Insert a colliding key so we don't elide the deleted key from the hash chain.
            var deleteKey = NumRecords / 2;
            var collidingKey = deleteKey + NumRecords;
            ClassicAssert.IsFalse(bContext.Upsert(collidingKey, collidingKey * ValueMult).IsPending);

            // Now make sure we did collide
            HashEntryInfo hei = new(store.storeFunctions.GetKeyHashCode64(ref deleteKey));
            ClassicAssert.IsTrue(store.FindTag(ref hei), "Cannot find deleteKey entry");
            ClassicAssert.Greater(hei.Address, Constants.kInvalidAddress, "Couldn't find deleteKey Address");
            var physicalAddress = store.hlog.GetPhysicalAddress(hei.Address);
            ref var recordInfo = ref store.hlog.GetInfo(physicalAddress);
            ref var lookupKey = ref store.hlog.GetKey(physicalAddress);
            ClassicAssert.AreEqual(collidingKey, lookupKey, "Expected collidingKey");

            // Backtrace to deleteKey
            physicalAddress = store.hlog.GetPhysicalAddress(recordInfo.PreviousAddress);
            recordInfo = ref store.hlog.GetInfo(physicalAddress);
            lookupKey = ref store.hlog.GetKey(physicalAddress);
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
            for (var key = 0; key < NumRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                ClassicAssert.IsFalse(bContext.Upsert(key, key * ValueMult).IsPending);
            }

            var expectedThrowAddress = store.Log.TailAddress;
            session.functions.throwOnInitialUpdater = true;

            // Delete must try with an existing key; Upsert and Delete should insert a new key
            var deleteKey = NumRecords / 2;
            var insertKey = NumRecords + 1;

            // Make sure everything will create a new record.
            store.Log.FlushAndEvict(wait: true);

            var threw = false;
            try
            {
                var status = updateOp switch
                {
                    UpdateOp.RMW => bContext.RMW(insertKey, default),
                    UpdateOp.Upsert => bContext.Upsert(insertKey, default),
                    UpdateOp.Delete => bContext.Delete(deleteKey),
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
            ref var recordInfo = ref store.hlog.GetInfo(physicalAddress);
            ClassicAssert.IsTrue(recordInfo.Invalid, "Expected Invalid record");
        }
    }
}