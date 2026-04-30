// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Threading;
using Allure.NUnit;
using Garnet.test;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ObjTrackingAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, ObjectDeleteDisposeTests.ObjTrackingRecordTriggers>>;
    using ObjTrackingStoreFunctions = StoreFunctions<TestObjectKey.Comparer, ObjectDeleteDisposeTests.ObjTrackingRecordTriggers>;

    using TrackingAllocator = SpanByteAllocator<StoreFunctions<IntKeyComparer, DeleteDisposeTests.TrackingRecordTriggers>>;
    using TrackingStoreFunctions = StoreFunctions<IntKeyComparer, DeleteDisposeTests.TrackingRecordTriggers>;

    [AllureNUnit]
    [TestFixture]
    internal class DeleteDisposeTests : AllureTestBase
    {
        internal struct TrackingRecordTriggers : IRecordTriggers
        {
            internal readonly DisposeTracker tracker;
            public TrackingRecordTriggers(DisposeTracker tracker) => this.tracker = tracker;
            public readonly bool CallOnEvict => false;
            public readonly bool CallOnFlush => false;
            public readonly bool CallOnDiskRead => false;
            public readonly void OnDispose(ref LogRecord logRecord, DisposeReason reason) => tracker?.RecordDispose(reason);
            public readonly void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason) { }
        }

        internal class DisposeTracker
        {
            private int _deletedCount;
            public int DeletedCount => _deletedCount;
            public void RecordDispose(DisposeReason reason) { if (reason == DisposeReason.Deleted) Interlocked.Increment(ref _deletedCount); }
            public void Reset() => Interlocked.Exchange(ref _deletedCount, 0);
        }

        internal class ExpirableFunctions : SimpleIntSimpleFunctions
        {
            internal RMWAction expireAction = RMWAction.Default;
            internal bool requestLargerReinit;

            public override bool InPlaceUpdater(ref LogRecord logRecord, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                if (expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                return base.InPlaceUpdater(ref logRecord, ref input, ref output, ref rmwInfo);
            }

            public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                if (expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                return true;
            }

            public override unsafe RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref int input)
            {
                var info = base.GetRMWInitialFieldInfo(key, ref input);
                // Request a larger value so TryReinitializeValueLength fails on the existing record
                if (requestLargerReinit)
                    info.ValueSize = sizeof(int) * 4;
                return info;
            }

            public override unsafe bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                if (requestLargerReinit)
                {
                    // Write input into the first int of the larger value span
                    dstLogRecord.ValueSpan.Clear();
                    dstLogRecord.ValueSpan.AsRef<int>() = input;
                    return true;
                }
                return base.InitialUpdater(ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
            }
        }

        private TsavoriteKV<TrackingStoreFunctions, TrackingAllocator> store;
        private IDevice log;
        private DisposeTracker tracker;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "DeleteDisposeTests.log"), deleteOnClose: true);
            tracker = new DisposeTracker();
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 10
            }, StoreFunctions.Create(IntKeyComparer.Instance, new TrackingRecordTriggers(tracker))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            OnTearDown();
        }

        private void UpsertKey(int key, int value)
        {
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, SimpleIntSimpleFunctions>(new SimpleIntSimpleFunctions());
            _ = s.BasicContext.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref value));
        }

        private static TestSpanByteKey Key(ref int k) => TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref k));

        #region Delete

        [Test]
        [Category("TsavoriteKV")]
        public void DisposeOnMutableDeleteTest()
        {
            UpsertKey(1, 100);
            tracker.Reset();
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, SimpleIntSimpleFunctions>(new SimpleIntSimpleFunctions());
            var k = 1;
            _ = s.BasicContext.Delete(Key(ref k));
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "Mutable delete: expected exactly 1");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void DisposeOnImmutableDeleteTest()
        {
            UpsertKey(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, SimpleIntSimpleFunctions>(new SimpleIntSimpleFunctions());
            var k = 1;
            _ = s.BasicContext.Delete(Key(ref k));
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "Immutable delete: expected exactly 1");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void DisposeOnImmutableDeleteMultipleTest()
        {
            const int n = 10;
            for (int i = 0; i < n; i++) UpsertKey(i, i * 10);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, SimpleIntSimpleFunctions>(new SimpleIntSimpleFunctions());
            for (int i = 0; i < n; i++) { var k = i; _ = s.BasicContext.Delete(Key(ref k)); }
            ClassicAssert.AreEqual(n, tracker.DeletedCount, $"Immutable delete multiple: expected exactly {n}");
        }

        #endregion

        #region ExpireAndStop

        [Test]
        [Category("TsavoriteKV")]
        public void DisposeOnMutableExpireAndStopTest()
        {
            UpsertKey(1, 100);
            tracker.Reset();
            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndStop };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpirableFunctions>(fn);
            var k = 1; var input = 0;
            _ = s.BasicContext.RMW(Key(ref k), ref input);
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "Mutable ExpireAndStop: expected exactly 1");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void DisposeOnImmutableExpireAndStopTest()
        {
            UpsertKey(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndStop };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpirableFunctions>(fn);
            var k = 1; var input = 0;
            _ = s.BasicContext.RMW(Key(ref k), ref input);
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "Immutable ExpireAndStop: expected exactly 1");
        }

        #endregion

        #region ExpireAndResume

        [Test]
        [Category("TsavoriteKV")]
        public void DisposeOnMutableExpireAndResumeFitsTest()
        {
            UpsertKey(1, 100);
            tracker.Reset();
            // Same-sized value: ReinitializeExpiredRecord succeeds in-place
            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndResume };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpirableFunctions>(fn);
            var k = 1; var input = 50;
            _ = s.BasicContext.RMW(Key(ref k), ref input);
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "Mutable ExpireAndResume (fits): expected exactly 1");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void DisposeOnMutableExpireAndResumeDoesNotFitTest()
        {
            UpsertKey(1, 100);
            tracker.Reset();
            // Request larger reinit (HasETag) so TryReinitializeValueLength fails,
            // forcing fallback to CreateNewRecordRMW → InitialUpdater
            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndResume, requestLargerReinit = true };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpirableFunctions>(fn);
            var k = 1; var input = 50;
            _ = s.BasicContext.RMW(Key(ref k), ref input);
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "Mutable ExpireAndResume (does not fit): expected exactly 1");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void DisposeOnImmutableExpireAndResumeTest()
        {
            UpsertKey(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndResume };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpirableFunctions>(fn);
            var k = 1; var input = 50;
            _ = s.BasicContext.RMW(Key(ref k), ref input);
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "Immutable ExpireAndResume: expected exactly 1");
        }

        #endregion
    }

    /// <summary>
    /// Tests that <see cref="IRecordTriggers.OnDispose"/> is called exactly once for IHeapObject records through all delete and expiration paths.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    internal class ObjectDeleteDisposeTests : AllureTestBase
    {
        internal struct ObjTrackingRecordTriggers : IRecordTriggers
        {
            internal readonly ObjDisposeTracker tracker;
            public ObjTrackingRecordTriggers(ObjDisposeTracker tracker) => this.tracker = tracker;
            public readonly bool CallOnEvict => false;
            public readonly bool CallOnFlush => false;
            public readonly bool CallOnDiskRead => false;

            public readonly void OnDispose(ref LogRecord logRecord, DisposeReason reason)
                => tracker?.RecordDispose(reason);

            public readonly void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason) { }
        }

        internal class ObjDisposeTracker
        {
            private int _disposeRecordDeletedCount;

            public int DisposeRecordDeletedCount => _disposeRecordDeletedCount;

            public void RecordDispose(DisposeReason reason)
            {
                if (reason == DisposeReason.Deleted)
                    Interlocked.Increment(ref _disposeRecordDeletedCount);
            }

            public void Reset()
            {
                Interlocked.Exchange(ref _disposeRecordDeletedCount, 0);
            }
        }

        internal class ObjExpirableFunctions : TestObjectFunctionsDelete
        {
            internal RMWAction expireAction = RMWAction.Default;
            internal bool failIPU;
            internal bool expireOnlyInCU; // If true, only CopyUpdater returns the expire action (not IPU/NCU)

            public override bool InPlaceUpdater(ref LogRecord logRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                if (!expireOnlyInCU && expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                if (failIPU) return false;
                return base.InPlaceUpdater(ref logRecord, ref input, ref output, ref rmwInfo);
            }

            public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                if (!expireOnlyInCU && expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                return true;
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                if (expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                return base.CopyUpdater(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
            }
        }

        private TsavoriteKV<ObjTrackingStoreFunctions, ObjTrackingAllocator> store;
        private IDevice log, objlog;
        private ObjDisposeTracker tracker;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjDeleteDisposeTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjDeleteDisposeTests.obj.log"), deleteOnClose: true);
            tracker = new ObjDisposeTracker();
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 10
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(),
                    new ObjTrackingRecordTriggers(tracker))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        private void UpsertObj(int key, int value)
        {
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            _ = s.BasicContext.Upsert(new TestObjectKey { key = key }, new TestObjectValue { value = value }, 0);
        }

        #region Delete

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnMutableDeleteTest()
        {
            UpsertObj(1, 100);
            tracker.Reset();
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnImmutableDeleteTest()
        {
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnImmutableDeleteMultipleTest()
        {
            const int n = 10;
            for (int i = 0; i < n; i++) UpsertObj(i, i * 10);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            for (int i = 0; i < n; i++) _ = s.BasicContext.Delete(new TestObjectKey { key = i });
            ClassicAssert.AreEqual(n, tracker.DisposeRecordDeletedCount, $"OnDispose(Deleted) should be called exactly {n} times");
        }

        #endregion

        #region ExpireAndStop

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnMutableExpireAndStopTest()
        {
            UpsertObj(1, 100);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { expireAction = RMWAction.ExpireAndStop };
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
            var input = new TestObjectInput { value = 0 };
            _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnImmutableExpireAndStopTest()
        {
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { expireAction = RMWAction.ExpireAndStop };
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
            var input = new TestObjectInput { value = 0 };
            _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        #endregion

        #region ExpireAndResume

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnMutableExpireAndResumeTest()
        {
            UpsertObj(1, 100);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { expireAction = RMWAction.ExpireAndResume };
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
            var input = new TestObjectInput { value = 50 };
            _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnImmutableExpireAndResumeTest()
        {
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { expireAction = RMWAction.ExpireAndResume };
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
            var input = new TestObjectInput { value = 50 };
            _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        #endregion

        #region IPU fails → CopyUpdater expires (simulates insufficient space forcing CU path)

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnIPUFailThenNCUExpireAndStopTest()
        {
            UpsertObj(1, 100);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { failIPU = true, expireAction = RMWAction.ExpireAndStop };
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
            var input = new TestObjectInput { value = 0 };
            _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnIPUFailThenNCUExpireAndResumeTest()
        {
            UpsertObj(1, 100);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { failIPU = true, expireAction = RMWAction.ExpireAndResume };
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
            var input = new TestObjectInput { value = 50 };
            _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnIPUFailThenCUExpireAndStopTest()
        {
            // IPU fails → NCU passes → CopyUpdater returns ExpireAndStop → source disposed
            UpsertObj(1, 100);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { failIPU = true, expireOnlyInCU = true, expireAction = RMWAction.ExpireAndStop };
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
            var input = new TestObjectInput { value = 0 };
            _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        #endregion
    }
}