// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.LockTests
{
    [TestFixture]
    public class BasicLockTests
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

        internal class LocalComparer : ITsavoriteEqualityComparer<int>
        {
            internal int mod = numRecords;

            public bool Equals(ref int k1, ref int k2) => k1 == k2;

            public long GetHashCode64(ref int k) => Utility.GetHashCode(k % mod);
        }

        private TsavoriteKV<int, int> store;
        private ClientSession<int, int, int, int, Empty, Functions> session;
        private BasicContext<int, int, int, int, Empty, Functions> bContext;
        private IDevice log;

        const int numRecords = 100;
        const int valueMult = 1000000;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "GenericStringTests.log"), deleteOnClose: true);
            store = new TsavoriteKV<int, int>(1L << 20, new LogSettings { LogDevice = log, ObjectLogDevice = null }, comparer: new LocalComparer());
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

            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void FunctionsLockTest([Values(1, 20)] int numThreads)
        {
            // Populate
            for (int key = 0; key < numRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                Assert.IsFalse(bContext.Upsert(key, key * valueMult).IsPending);
            }

            // Update
            const int numIters = 500;
            var tasks = Enumerable.Range(0, numThreads).Select(ii => Task.Factory.StartNew(() => UpdateFunc((ii & 1) == 0, numRecords, numIters))).ToArray();
            Task.WaitAll(tasks);

            // Verify
            for (int key = 0; key < numRecords; key++)
            {
                var expectedValue = key * valueMult + numThreads * numIters;
                Assert.IsFalse(bContext.Read(key, out int value).IsPending);
                Assert.AreEqual(expectedValue, value);
            }
        }

        void UpdateFunc(bool useRMW, int numRecords, int numIters)
        {
            for (var key = 0; key < numRecords; ++key)
            {
                for (int iter = 0; iter < numIters; iter++)
                {
                    if ((iter & 7) == 7)
                        Assert.IsFalse(bContext.Read(key).status.IsPending);

                    // These will both just increment the stored value, ignoring the input argument.
                    if (useRMW)
                        bContext.RMW(key, default);
                    else
                        bContext.Upsert(key, default);
                }
            }
        }

        [Test]
        [Category("TsavoriteKV")]
        public unsafe void CollidingDeletedRecordTest([Values(UpdateOp.RMW, UpdateOp.Upsert)] UpdateOp updateOp, [Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode flushMode)
        {
            // Populate
            for (int key = 0; key < numRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                Assert.IsFalse(bContext.Upsert(key, key * valueMult).IsPending);
            }

            // Insert a colliding key so we don't elide the deleted key from the hash chain.
            int deleteKey = numRecords / 2;
            int collidingKey = deleteKey + numRecords;
            Assert.IsFalse(bContext.Upsert(collidingKey, collidingKey * valueMult).IsPending);

            // Now make sure we did collide
            HashEntryInfo hei = new(store.comparer.GetHashCode64(ref deleteKey));
            Assert.IsTrue(store.FindTag(ref hei), "Cannot find deleteKey entry");
            Assert.Greater(hei.Address, Constants.kInvalidAddress, "Couldn't find deleteKey Address");
            long physicalAddress = store.hlog.GetPhysicalAddress(hei.Address);
            ref var recordInfo = ref store.hlog.GetInfo(physicalAddress);
            ref var lookupKey = ref store.hlog.GetKey(physicalAddress);
            Assert.AreEqual(collidingKey, lookupKey, "Expected collidingKey");

            // Backtrace to deleteKey
            physicalAddress = store.hlog.GetPhysicalAddress(recordInfo.PreviousAddress);
            recordInfo = ref store.hlog.GetInfo(physicalAddress);
            lookupKey = ref store.hlog.GetKey(physicalAddress);
            Assert.AreEqual(deleteKey, lookupKey, "Expected deleteKey");
            Assert.IsFalse(recordInfo.Tombstone, "Tombstone should be false");

            // In-place delete.
            Assert.IsFalse(bContext.Delete(deleteKey).IsPending);
            Assert.IsTrue(recordInfo.Tombstone, "Tombstone should be true after Delete");

            if (flushMode == FlushMode.ReadOnly)
                store.hlog.ShiftReadOnlyAddress(store.Log.TailAddress);

            var status = updateOp switch
            {
                UpdateOp.RMW => bContext.RMW(deleteKey, default),
                UpdateOp.Upsert => bContext.Upsert(deleteKey, default),
                UpdateOp.Delete => throw new InvalidOperationException("UpdateOp.Delete not expected in this test"),
                _ => throw new InvalidOperationException($"Unknown updateOp {updateOp}")
            };
            Assert.IsFalse(status.IsPending);

            Assert.IsTrue(recordInfo.Tombstone, "Tombstone should be true after Update");
        }

        [Test]
        [Category("TsavoriteKV")]
        public unsafe void SetInvalidOnException([Values] UpdateOp updateOp)
        {
            // Don't modulo the hash codes.
            (store.comparer as LocalComparer).mod = int.MaxValue;

            // Populate
            for (int key = 0; key < numRecords; key++)
            {
                // For this test we should be in-memory, so no pending
                Assert.IsFalse(bContext.Upsert(key, key * valueMult).IsPending);
            }

            long expectedThrowAddress = store.Log.TailAddress;
            session.functions.throwOnInitialUpdater = true;

            // Delete must try with an existing key; Upsert and Delete should insert a new key
            int deleteKey = numRecords / 2;
            var insertKey = numRecords + 1;

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
                Assert.IsFalse(status.IsPending);
            }
            catch (TsavoriteException ex)
            {
                Assert.AreEqual(nameof(session.functions.throwOnInitialUpdater), ex.Message);
                threw = true;
            }

            Assert.IsTrue(threw, "Test should have thrown");
            Assert.AreEqual(expectedThrowAddress, session.functions.initialUpdaterThrowAddress, "Unexpected throw address");

            long physicalAddress = store.hlog.GetPhysicalAddress(expectedThrowAddress);
            ref var recordInfo = ref store.hlog.GetInfo(physicalAddress);
            Assert.IsTrue(recordInfo.Invalid, "Expected Invalid record");
        }
    }
}