// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
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
    [TestFixture]
    internal class DeleteDisposeTests : TestBase
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

            // Explicit defaults/no-ops (rather than relying on the IRecordTriggers defaults) so calls
            // through the TRecordTriggers generic constraint resolve here instead of boxing to dispatch the DIMs.
            public readonly bool CallPostCopyToTail => false;
            public readonly bool CallOnTruncate => false;
            public readonly void OnFlush(ref LogRecord logRecord, long logicalAddress) { }
            public readonly void OnEvict(ref LogRecord logRecord, EvictionSource source) { }
            public readonly void OnDiskRead(ref LogRecord logRecord) { }
            public readonly void OnRecovery(System.Guid checkpointToken) { }
            public readonly void OnRecoverySnapshotRead(ref LogRecord logRecord) { }
            public readonly void OnCheckpoint(CheckpointTrigger trigger, System.Guid checkpointToken) { }
            public readonly void PostCopyToTail<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, long srcLogicalAddress,
                                                                  ref LogRecord dstLogRecord, long dstLogicalAddress)
                where TSourceLogRecord : ISourceLogRecord, allows ref struct
            { }
            public readonly void OnTruncate(long newBeginAddress) { }
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
            internal bool expireOnlyInCU; // If true, only CopyUpdater returns the expire action (not IPU/NCU)

            public override bool InPlaceUpdater(ref LogRecord logRecord, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                if (!expireOnlyInCU && expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                return base.InPlaceUpdater(ref logRecord, ref input, ref output, ref rmwInfo);
            }

            public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                if (!expireOnlyInCU && expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                return true;
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                if (expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                return base.CopyUpdater(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
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

        internal class ExpiredReinitWithExpirationFunctions : SimpleIntSimpleFunctions
        {
            internal const long ExpirationTicks = 123456789L;

            internal int newValueLength;
            internal int readValueLength;
            internal long readExpiration;
            internal byte seedRecordType;
            internal byte readRecordType;

            public override bool InPlaceUpdater(ref LogRecord logRecord, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                rmwInfo.Action = RMWAction.ExpireAndResume;
                return false;
            }

            public override RecordFieldInfo GetRMWInitialFieldInfo<TKey>(TKey key, ref int input)
                => new() { KeySize = key.KeyBytes.Length, ValueSize = newValueLength, HasExpiration = true, ExtendedNamespaceSize = RecordNamespace.GetExtendedNamespaceSize(in key) };

            public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref int input)
                => new() { KeySize = key.KeyBytes.Length, ValueSize = value.Length, RecordType = seedRecordType, ExtendedNamespaceSize = RecordNamespace.GetExtendedNamespaceSize(in key) };

            public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ReadOnlySpan<byte> srcValue, ref int output, ref UpsertInfo upsertInfo)
                => dstLogRecord.TrySetValueSpanAndPrepareOptionals(srcValue, in sizeInfo);

            public override bool InitialUpdater(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                var value = new byte[sizeInfo.FieldInfo.ValueSize];
                value[0] = (byte)input;
                if (!dstLogRecord.TrySetValueSpanAndPrepareOptionals(value, in sizeInfo) || !dstLogRecord.TrySetExpiration(ExpirationTicks))
                    return false;
                output = input;
                return true;
            }

            public override bool Reader<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref int input, ref int output, ref ReadInfo readInfo)
            {
                readValueLength = srcLogRecord.ValueSpan.Length;
                readExpiration = srcLogRecord.Expiration;
                readRecordType = srcLogRecord.RecordType;
                output = srcLogRecord.ValueSpan[0];
                return true;
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
                PageSize = MinKvLogPageSize
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

        [Test]
        [Category("TsavoriteKV")]
        public void ExpireAndResumeReinitDoesNotKeepStaleRecordTypeTest([Values(1, 2)] byte seedRecordType)
        {
            // The seed record is large enough that the new value would fit it in place; a differing RecordType must still force a new record
            var fn = new ExpiredReinitWithExpirationFunctions { newValueLength = sizeof(int), seedRecordType = seedRecordType };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpiredReinitWithExpirationFunctions>(fn);
            var key = 1;
            _ = s.BasicContext.Upsert(Key(ref key), new byte[64]);

            var input = 50;
            var output = 0;
            _ = s.BasicContext.RMW(Key(ref key), ref input, ref output);

            output = 0;
            var status = s.BasicContext.Read(Key(ref key), ref input, ref output);
            ClassicAssert.IsTrue(status.Found);
            ClassicAssert.AreEqual(50, output);
            ClassicAssert.AreEqual(0, fn.readRecordType, "Reinitialized record must not retain the expired record's RecordType");
            ClassicAssert.AreEqual(ExpiredReinitWithExpirationFunctions.ExpirationTicks, fn.readExpiration);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void DeleteReportsExpiredRecordTest([Values] bool immutable, [Values] bool expiring)
        {
            // InitialUpdater of ExpiredReinitWithExpirationFunctions stores a (long past) expiration
            var fn = new ExpiredReinitWithExpirationFunctions { newValueLength = sizeof(int) };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpiredReinitWithExpirationFunctions>(fn);
            var key = 1;
            if (expiring)
            {
                var input = 50;
                _ = s.BasicContext.RMW(Key(ref key), ref input);
            }
            else
                UpsertKey(key, 50);

            if (immutable)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();

            var status = s.BasicContext.Delete(Key(ref key));
            ClassicAssert.AreEqual(expiring, status.IsExpired, $"Delete expired status (immutable {immutable}, expiring {expiring})");
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "Expected the record to be disposed even if expired");
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

        [Test]
        [Category("TsavoriteKV")]
        public void ExpireAndResumeReinitWithExpirationStoresValueTest([Values(1, 4, 5, 8)] int seedLength, [Values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 16, 64)] int valueGrowth)
        {
            // The new value may fit the expired record without the expiration but not with it; the write must never be lost.
            var fn = new ExpiredReinitWithExpirationFunctions { newValueLength = seedLength + valueGrowth };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpiredReinitWithExpirationFunctions>(fn);
            var key = 1;
            var seed = new byte[seedLength];
            _ = s.BasicContext.Upsert(Key(ref key), seed);

            var input = 50;
            var output = 0;
            _ = s.BasicContext.RMW(Key(ref key), ref input, ref output);

            output = 0;
            var status = s.BasicContext.Read(Key(ref key), ref input, ref output);
            ClassicAssert.IsTrue(status.Found, $"Key not found after RMW (seed {seedLength}, growth {valueGrowth})");
            ClassicAssert.AreEqual(50, output);
            ClassicAssert.AreEqual(seedLength + valueGrowth, fn.readValueLength);
            ClassicAssert.AreEqual(ExpiredReinitWithExpirationFunctions.ExpirationTicks, fn.readExpiration);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void DisposeOnImmutableCUExpireAndResumeTest()
        {
            UpsertKey(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndResume, expireOnlyInCU = true };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpirableFunctions>(fn);
            var k = 1; var input = 50;
            var status = s.BasicContext.RMW(Key(ref k), ref input);
            ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "CopyUpdater ExpireAndResume: only the expired source is disposed");

            var output = 0;
            status = s.BasicContext.Read(Key(ref k), ref output);
            ClassicAssert.IsTrue(status.Found, status.ToString());
            ClassicAssert.AreEqual(50, output);
        }

        #endregion
    }

    /// <summary>
    /// Tests that <see cref="IRecordTriggers.OnDispose"/> is called exactly once for IHeapObject records through all delete and expiration paths.
    /// </summary>
    [TestFixture]
    internal class ObjectDeleteDisposeTests : TestBase
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

            // Explicit defaults/no-ops (rather than relying on the IRecordTriggers defaults) so calls
            // through the TRecordTriggers generic constraint resolve here instead of boxing to dispatch the DIMs.
            public readonly bool CallPostCopyToTail => false;
            public readonly bool CallOnTruncate => false;
            public readonly void OnFlush(ref LogRecord logRecord, long logicalAddress) { }
            public readonly void OnEvict(ref LogRecord logRecord, EvictionSource source) { }
            public readonly void OnDiskRead(ref LogRecord logRecord) { }
            public readonly void OnRecovery(System.Guid checkpointToken) { }
            public readonly void OnRecoverySnapshotRead(ref LogRecord logRecord) { }
            public readonly void OnCheckpoint(CheckpointTrigger trigger, System.Guid checkpointToken) { }
            public readonly void PostCopyToTail<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, long srcLogicalAddress,
                                                                  ref LogRecord dstLogRecord, long dstLogicalAddress)
                where TSourceLogRecord : ISourceLogRecord, allows ref struct
            { }
            public readonly void OnTruncate(long newBeginAddress) { }
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
                PageSize = MinKvLogPageSize
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

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnIPUFailThenCUExpireAndResumeTest()
        {
            // IPU fails → NCU passes → CopyUpdater returns ExpireAndResume → the new record is reinitialized by InitialUpdater
            UpsertObj(1, 100);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { failIPU = true, expireOnlyInCU = true, expireAction = RMWAction.ExpireAndResume };
            AssertCUExpireAndResume(fn);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnImmutableCUExpireAndResumeTest()
        {
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { expireOnlyInCU = true, expireAction = RMWAction.ExpireAndResume };
            AssertCUExpireAndResume(fn);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnDiskCUExpireAndResumeTest()
        {
            UpsertObj(1, 100);
            store.Log.FlushAndEvict(wait: true);
            tracker.Reset();
            var fn = new ObjExpirableFunctions { expireOnlyInCU = true, expireAction = RMWAction.ExpireAndResume };
            AssertCUExpireAndResume(fn, expectedDisposes: 0);
        }

        /// <summary>RMWs key 1 with <paramref name="fn"/>, whose CopyUpdater expires the source, and verifies the source alone was disposed and the record now holds
        /// the initial value. A source on disk has no in-memory record to dispose.</summary>
        private void AssertCUExpireAndResume(ObjExpirableFunctions fn, int expectedDisposes = 1)
        {
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
            var input = new TestObjectInput { value = 50 };
            // Context 1 makes RMWCompletionCallback assert !Found: the expired source counts as not found.
            var status = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 1);
            if (status.IsPending)
            {
                _ = s.BasicContext.CompletePendingWithOutputs(out var outputs, wait: true);
                using (outputs)
                {
                    ClassicAssert.IsTrue(outputs.Next());
                    status = outputs.Current.Status;
                }
            }
            ClassicAssert.IsTrue(status.Record.Created, status.ToString());
            ClassicAssert.AreEqual(expectedDisposes, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called only for the expired source");

            TestObjectOutput output = default;
            status = s.BasicContext.Read(new TestObjectKey { key = 1 }, ref output, 0);
            ClassicAssert.IsTrue(status.Found, status.ToString());
            ClassicAssert.AreEqual(50, output.value.value);
        }

        #endregion
    }
}