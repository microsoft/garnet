// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.core.Utility;
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
                where TSourceLogRecord : ISourceLogRecord
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
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
                where TSourceLogRecord : ISourceLogRecord
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
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
            internal bool expireInPostCopyUpdater; // If true, PostCopyUpdater returns ExpireAndStop
            internal bool declineInitialUpdate; // If true, NeedInitialUpdate declines to create a record
            internal Action onNeedCopyUpdate; // Called first in NeedCopyUpdate, after the RMW has chosen whether to elide its source
            internal Action onCopyUpdater; // Called first in CopyUpdater, after the RMW has allocated its new record

            public override bool InPlaceUpdater(ref LogRecord logRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                if (!expireOnlyInCU && expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                if (failIPU) return false;
                return base.InPlaceUpdater(ref logRecord, ref input, ref output, ref rmwInfo);
            }

            public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                onNeedCopyUpdate?.Invoke();
                if (!expireOnlyInCU && expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                return true;
            }

            public override bool NeedInitialUpdate<TKey>(TKey key, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
                => !declineInitialUpdate && base.NeedInitialUpdate(key, ref input, ref output, ref rmwInfo);

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                onCopyUpdater?.Invoke();
                if (expireAction != RMWAction.Default) { rmwInfo.Action = expireAction; return false; }
                return base.CopyUpdater(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
            }

            public override bool PostCopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                if (expireInPostCopyUpdater) { rmwInfo.Action = RMWAction.ExpireAndStop; return false; }
                return base.PostCopyUpdater(in srcLogRecord, ref dstLogRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
            }
        }

        /// <summary>Calls a hook from InitialDeleter or InitialWriter, after the Delete or Upsert has chosen whether to elide its source and
        /// allocated its new record, and before the new record's compare-and-swap.</summary>
        internal class ObjHookFunctions : TestObjectFunctionsDelete
        {
            internal Action onInitialDeleter;
            internal Action onInitialWriter;
            internal bool failInPlaceWriter; // If true, an Upsert of a mutable record writes a new record

            public override bool InitialDeleter(ref LogRecord dstLogRecord, ref DeleteInfo deleteInfo)
            {
                onInitialDeleter?.Invoke();
                return base.InitialDeleter(ref dstLogRecord, ref deleteInfo);
            }

            public override bool InPlaceWriter(ref LogRecord logRecord, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
                => !failInPlaceWriter && base.InPlaceWriter(ref logRecord, ref input, srcValue, ref output, ref upsertInfo);

            public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
            {
                onInitialWriter?.Invoke();
                return base.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo);
            }
        }

        private TsavoriteKV<ObjTrackingStoreFunctions, ObjTrackingAllocator> store;
        private IDevice log, objlog;
        private GatedCompletionDevice gatedLog;
        private ObjDisposeTracker tracker;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjDeleteDisposeTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjDeleteDisposeTests.obj.log"), deleteOnClose: true);

            // Only the flush-window tests need to hold a flush in flight; everywhere else the gate would just add indirection.
            if (TestContext.CurrentContext.Test.MethodName.Contains("FrozenForFlush")
                || TestContext.CurrentContext.Test.MethodName.Contains("PartialSector"))
                gatedLog = new GatedCompletionDevice(log);

            tracker = new ObjDisposeTracker();
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = (IDevice)gatedLog ?? log,
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
            gatedLog?.Dispose();
            gatedLog = null;
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

        /// <summary>
        /// Disposing a superseded source frees its value object and returns its ObjectIdMap slot for reuse. The object allocator
        /// flushes from the live page, so doing that while the record's flush is issued but not yet durable can persist a Valid
        /// record whose ObjectLogPosition refers to a freed - or recycled, hence unrelated - object. Disposal must be deferred to
        /// eviction for the duration of that window.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeDeferredWhileFrozenForFlushTest()
        {
            var recordAddress = store.Log.TailAddress;
            UpsertObj(1, 100);

            // Hold the flush's device write so SafeReadOnlyAddress passes the record while FlushedUntilAddress stays behind it.
            gatedLog.Gate = true;
            store.hlogBase.ShiftReadOnlyAddressWithWait(store.Log.TailAddress, wait: false);
            gatedLog.WaitForPending(1, TimeSpan.FromSeconds(30));
            tracker.Reset();

            Assert.That(store.hlogBase.SafeReadOnlyAddress, Is.GreaterThan(recordAddress), "record should be immutable");
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.LessThanOrEqualTo(recordAddress), "record's flush should still be in flight");
            Assert.That(store.hlog.IsFrozenForFlush(recordAddress), Is.True);

            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete()))
                _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });

            ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) must be deferred while the record's flush is in flight");

            // Let the flush complete so teardown does not block on it.
            gatedLog.Release(0);
            gatedLog.WaitForCompletion(0, TimeSpan.FromSeconds(30));
        }

        /// <summary>
        /// A partial flush starts its device write on a sector boundary, so it writes again the records between that boundary and its
        /// own start address, which a previous flush already made durable. It takes those bytes from the image the previous flush kept,
        /// not the live page, so such a record is not frozen and its disposal runs at once, even while that write is in flight.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeImmediateWhileNextFlushWritesItsPartialSectorTest()
        {
            var recordAddress = store.Log.TailAddress;
            UpsertObj(1, 100);

            // First flush: make the record durable, ending on a non-sector-aligned boundary.
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            var sectorSize = (int)log.SectorSize;
            var flushedUntil = store.hlogBase.FlushedUntilAddress;
            Assert.That(flushedUntil, Is.GreaterThan(recordAddress), "record should be durable");
            Assert.That(flushedUntil % sectorSize, Is.Not.EqualTo(0), "the flush must end mid-sector for the next one to write the sector again");
            Assert.That(recordAddress, Is.GreaterThanOrEqualTo(RoundDown(flushedUntil, sectorSize)),
                "record must sit in the sector that the next flush writes again");

            // Second flush: hold its device writes. It starts at the first flush's end and writes that sector on its own, followed by
            // the rest of its span if the span reaches past that sector.
            UpsertObj(2, 200);
            var heldWrites = RoundUp(store.Log.TailAddress, sectorSize) - RoundDown(flushedUntil, sectorSize) > sectorSize ? 2 : 1;
            gatedLog.Gate = true;
            store.hlogBase.ShiftReadOnlyAddressWithWait(store.Log.TailAddress, wait: false);
            gatedLog.WaitForPending(heldWrites, TimeSpan.FromSeconds(30));
            tracker.Reset();

            Assert.That(store.hlog.IsFrozenForFlush(recordAddress), Is.False,
                "a durable record must not be frozen while the next flush writes its sector");

            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete()))
                _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });

            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) must run at once for a durable record");

            // Let the flush complete so teardown does not block on it.
            gatedLog.Gate = false;
            for (var i = 0; i < heldWrites; i++)
            {
                gatedLog.Release(i);
                gatedLog.WaitForCompletion(i, TimeSpan.FromSeconds(30));
            }
        }

        /// <summary>
        /// A record below FlushedUntilAddress is durable and not frozen, even in the sector a flush ended in. The next flush starts in
        /// that sector and writes it again, but takes the bytes below its start from the image the previous flush kept, so disposing the
        /// record cannot reach its durable image and runs at once.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeImmediateInCompletedPartialSectorTest()
        {
            var recordAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            var sectorSize = (int)log.SectorSize;
            var flushedUntil = store.hlogBase.FlushedUntilAddress;
            Assert.That(flushedUntil % sectorSize, Is.Not.EqualTo(0), "the flush must end mid-sector for the next one to write the sector again");
            Assert.That(recordAddress, Is.GreaterThanOrEqualTo(RoundDown(flushedUntil, sectorSize)),
                "record must sit in the sector that the next flush writes again");
            Assert.That(store.hlog.IsFrozenForFlush(recordAddress), Is.False,
                "a durable record in the sector the next flush writes again must not be frozen");
            tracker.Reset();

            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete()))
                _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });

            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) must run at once for a durable record");
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

        #region Expirations that complete after the new record is in the chain

        /// <summary>
        /// A PostCopyUpdater that expires the key turns the CopyUpdate's new record into a tombstone. The source is disposed as deleted,
        /// which runs the delete trigger, and the tombstone keeps no value. Otherwise both would be held until eviction, and the
        /// tombstone's object would be serialized when its page flushes.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjDisposeOnImmutablePostCopyUpdaterExpireAndStopTest()
        {
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            var tombstoneAddress = store.Log.TailAddress;
            var fn = new ObjExpirableFunctions { expireInPostCopyUpdater = true };
            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn))
            {
                var input = new TestObjectInput { value = 0 };
                _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            }

            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "The expired source must be disposed as deleted exactly once");
            var tombstone = store.hlogBase._wrapper.CreateLogRecord(tombstoneAddress);
            Assert.That(tombstone.Info.Tombstone, Is.True, "The CopyUpdate's new record should be the tombstone");
            Assert.That(tombstone.DataHeader.ValueIsInline, Is.True, "The tombstone must not keep the value the CopyUpdater gave it");
        }

        /// <summary>
        /// An expiration releases its source only once a new record is in the chain. If NeedInitialUpdate then declines to create one, the
        /// expired source stays as it was. Releasing the source at the expiration decision, before NeedInitialUpdate, would leave the key's
        /// live record with its value and its expiration cleared, so the expired key would come back with no expiration.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjExpireAndResumeWithoutInitialUpdateLeavesTheSourceIntactTest()
        {
            var sourceAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            var tailBefore = store.Log.TailAddress;
            var fn = new ObjExpirableFunctions { expireAction = RMWAction.ExpireAndResume, declineInitialUpdate = true };
            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn))
            {
                var input = new TestObjectInput { value = 0 };
                _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            }

            Assert.That(store.Log.TailAddress, Is.EqualTo(tailBefore), "NeedInitialUpdate declined, so no record should have been created");
            ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "Nothing replaced the expired source, so it must not be released");
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).DataHeader.ValueIsObject, Is.True, "The expired source must keep its value");
        }

        /// <summary>
        /// An expiration of a record whose flush is in flight defers the source's release, as a Delete does. Once the flush completes, the
        /// drain disposes the source as deleted, which runs the delete trigger. Such a source lies below SafeReadOnlyAddress, so it is not
        /// sealed: the drain decides the kind of release by the mark, not by whether the record is sealed.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjExpireAndStopDeferredWhileFrozenForFlushRunsTheDeleteTriggerTest()
        {
            var recordAddress = store.Log.TailAddress;
            UpsertObj(1, 100);

            // Hold the flush's device write so SafeReadOnlyAddress passes the record while FlushedUntilAddress stays behind it.
            gatedLog.Gate = true;
            store.hlogBase.ShiftReadOnlyAddressWithWait(store.Log.TailAddress, wait: false);
            gatedLog.WaitForPending(1, TimeSpan.FromSeconds(30));
            tracker.Reset();
            Assert.That(store.hlog.IsFrozenForFlush(recordAddress), Is.True);

            var fn = new ObjExpirableFunctions { expireAction = RMWAction.ExpireAndStop };
            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn))
            {
                var input = new TestObjectInput { value = 0 };
                _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            }

            ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "The expired source's release must wait for its flush");
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(recordAddress).Info.IsSealed, Is.False,
                "Precondition: an expired source below SafeReadOnlyAddress is not sealed");

            // The drain runs on the flush completion, after the wait is released, so poll for it.
            gatedLog.Release(0);
            gatedLog.WaitForCompletion(0, TimeSpan.FromSeconds(30));
            Assert.That(SpinWait.SpinUntil(() => tracker.DisposeRecordDeletedCount > 0, TimeSpan.FromSeconds(30)), Is.True,
                "The drain must dispose the expired source as deleted once its flush completes");
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        #endregion

        #region A freeze that begins after the operation chose to elide its source

        /// <summary>
        /// An operation chooses to elide its source before it allocates its new record, and a freeze can begin after that choice. Here a
        /// ReadOnly flush of the source starts while the CopyUpdater runs: the epoch bump that makes the source read-only runs the flush's start
        /// on this thread, as an allocation's epoch refresh can. The CopyUpdater then expires the key, so the source's disposal is marked to
        /// wait for the flush. The source must stay valid: an elided source is invalid, and the drain skips invalid records, so its delete
        /// trigger would never run.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjCopyUpdaterExpireAndStopFrozenForFlushDuringTheOperationRunsTheDeleteTriggerTest()
        {
            var sourceAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            var newRecordAddress = store.Log.TailAddress;

            // Hold the flush's device write, so FlushedUntilAddress stays behind the source once the flush starts.
            gatedLog.Gate = true;
            tracker.Reset();

            // The source is mutable, so it is not frozen when the RMW chooses to elide it.
            var fn = new ObjExpirableFunctions
            {
                failIPU = true,
                expireOnlyInCU = true,
                expireAction = RMWAction.ExpireAndStop,
                onCopyUpdater = () => store.hlogBase.ShiftReadOnlyAddressWithWait(newRecordAddress, wait: false)
            };
            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn))
            {
                var input = new TestObjectInput { value = 0 };
                _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            }

            Assert.That(store.hlogBase.SafeReadOnlyAddress, Is.GreaterThan(sourceAddress), "Precondition: the source's flush should have started during the RMW");
            Assert.That(store.hlogBase.FlushedUntilAddress, Is.LessThanOrEqualTo(sourceAddress), "Precondition: the source's flush should still be in flight");
            ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "The expired source's release must wait for its flush");
            var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
            Assert.That(source.Info.Valid, Is.True, "A source whose disposal is marked must not be elided");
            Assert.That(source.Info.IsSealed, Is.True, "The bypassed source should be sealed");
            Assert.That(source.Info.DeferredDispose, Is.True, "The source's disposal should be marked");

            // The drain runs on the flush completion, after the wait is released, so poll for it.
            gatedLog.Release(0);
            gatedLog.WaitForCompletion(0, TimeSpan.FromSeconds(30));
            Assert.That(SpinWait.SpinUntil(() => tracker.DisposeRecordDeletedCount > 0, TimeSpan.FromSeconds(30)), Is.True,
                "The drain must run the expired source's delete trigger once its flush completes");
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        /// <summary>
        /// A streaming snapshot's scan can publish its hold while a Delete runs, after the Delete chose to elide its source. The scan may then
        /// push the source, value and all, so the Delete marks the source's disposal and leaves the source valid. Eviction runs the mark once
        /// the scan has ended.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjDeleteHeldForStreamingScanDuringTheOperationDefersTheDeleteTriggerTest()
        {
            var sourceAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();

            var fn = new ObjHookFunctions { onInitialDeleter = () => store.hlogBase.BeginStreamingScanHold(sourceAddress) };
            try
            {
                using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjHookFunctions>(fn))
                    _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });

                ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "The scan may still push the source, so its disposal must wait");
                var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
                Assert.That(source.Info.Valid, Is.True, "A source the scan may push must not be elided");
                Assert.That(source.Info.IsSealed, Is.True, "The bypassed source should be sealed");
                Assert.That(source.Info.DeferredDispose, Is.True, "The source's disposal should be marked");
                Assert.That(source.ValueObject, Is.Not.Null, "The source must keep its value while the scan may push it");
            }
            finally
            {
                store.hlogBase.EndStreamingScanHold();
            }

            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "Eviction must run the deletion exactly once");
        }

        /// <summary>
        /// NeedCopyUpdate expiring a key whose only record is elidable elides that record in place, with no new record. A streaming snapshot's
        /// hold that begins after the RMW chose to elide sends it down the non-elidable path instead: a tombstone record, with the source's
        /// disposal marked until the scan ends. Eliding the source would invalidate the mark, so the delete trigger would never run.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjNeedCopyUpdateExpireAndStopHeldForStreamingScanDuringTheOperationAddsATombstoneTest()
        {
            var sourceAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            var tombstoneAddress = store.Log.TailAddress;
            tracker.Reset();

            var fn = new ObjExpirableFunctions
            {
                expireAction = RMWAction.ExpireAndStop,
                onNeedCopyUpdate = () => store.hlogBase.BeginStreamingScanHold(sourceAddress)
            };
            try
            {
                using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn))
                {
                    var input = new TestObjectInput { value = 0 };
                    _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
                }

                ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "The scan may still push the source, so its disposal must wait");
                Assert.That(store.Log.TailAddress, Is.GreaterThan(tombstoneAddress), "A held source should get a tombstone record");
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(tombstoneAddress).Info.Tombstone, Is.True, "The new record should be a tombstone");
                var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
                Assert.That(source.Info.Valid, Is.True, "A source the scan may push must not be elided");
                Assert.That(source.Info.Tombstone, Is.False, "A held source must not be tombstoned in place");
                Assert.That(source.Info.DeferredDispose, Is.True, "The source's disposal should be marked");
                Assert.That(source.ValueObject, Is.Not.Null, "The source must keep its value while the scan may push it");
            }
            finally
            {
                store.hlogBase.EndStreamingScanHold();
            }

            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "Eviction must run the deletion exactly once");
        }

        /// <summary>
        /// An Upsert that chose to elide its source must not elide one that a streaming snapshot's scan began holding since: the scan may push
        /// the source, value and all. Upsert finds only a mutable source, and elides it when it cannot write the source in place.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjUpsertHeldForStreamingScanDuringTheOperationKeepsTheSourceTest()
        {
            var sourceAddress = store.Log.TailAddress;
            UpsertObj(1, 100);

            var fn = new ObjHookFunctions { failInPlaceWriter = true, onInitialWriter = () => store.hlogBase.BeginStreamingScanHold(sourceAddress) };
            try
            {
                var tailBefore = store.Log.TailAddress;
                using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjHookFunctions>(fn))
                    _ = s.BasicContext.Upsert(new TestObjectKey { key = 1 }, new TestObjectValue { value = 200 }, 0);

                Assert.That(store.Log.TailAddress, Is.GreaterThan(tailBefore), "Precondition: the Upsert should have written a new record");
                var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
                Assert.That(source.Info.Valid, Is.True, "A source the scan may push must not be elided");
                Assert.That(source.Info.IsSealed, Is.True, "The bypassed source should be sealed");
                Assert.That(((TestObjectValue)source.ValueObject).value, Is.EqualTo(100), "The source must keep its value while the scan may push it");
            }
            finally
            {
                store.hlogBase.EndStreamingScanHold();
            }
        }

        /// <summary>
        /// A CopyUpdate that chose to elide its source must not elide one that a streaming snapshot's scan began holding since. The source's
        /// value is released at once, as for any CopyUpdate, which changes none of its bytes, but the source stays valid and sealed.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjCopyUpdateHeldForStreamingScanDuringTheOperationKeepsTheSourceValidTest()
        {
            var sourceAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            var fn = new ObjExpirableFunctions { onCopyUpdater = () => store.hlogBase.BeginStreamingScanHold(sourceAddress) };
            try
            {
                using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn))
                {
                    var input = new TestObjectInput { value = 1 };
                    _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
                }

                var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
                Assert.That(source.Info.Valid, Is.True, "A source the scan may push must not be elided");
                Assert.That(source.Info.IsSealed, Is.True, "The bypassed source should be sealed");
            }
            finally
            {
                store.hlogBase.EndStreamingScanHold();
            }
        }

        #endregion
    }
}