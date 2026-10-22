// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
            internal bool declineInitialUpdate; // If true, NeedInitialUpdate declines, so nothing replaces an expired record
            internal bool expireOnlyWithExpiration; // If true, only a record that has an expiration expires, as when records expire by time
            internal int needCopyUpdateCalls;

            private bool Expires<TSourceLogRecord>(in TSourceLogRecord srcLogRecord)
                where TSourceLogRecord : ISourceLogRecord, allows ref struct
                => expireAction != RMWAction.Default && (!expireOnlyWithExpiration || srcLogRecord.Expiration != 0);

            public override bool InPlaceUpdater(ref LogRecord logRecord, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                if (!expireOnlyInCU && Expires(in logRecord)) { rmwInfo.Action = expireAction; return false; }
                return base.InPlaceUpdater(ref logRecord, ref input, ref output, ref rmwInfo);
            }

            public override bool NeedInitialUpdate<TKey>(TKey key, ref int input, ref int output, ref RMWInfo rmwInfo)
                => !declineInitialUpdate;

            public override bool NeedCopyUpdate<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                _ = Interlocked.Increment(ref needCopyUpdateCalls);
                if (!expireOnlyInCU && Expires(in srcLogRecord)) { rmwInfo.Action = expireAction; return false; }
                return true;
            }

            public override bool CopyUpdater<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ref int output, ref RMWInfo rmwInfo)
            {
                if (Expires(in srcLogRecord)) { rmwInfo.Action = expireAction; return false; }
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

        /// <summary>Writes records with empty values, which are the size of a tombstone for the same key.</summary>
        internal class EmptyValueFunctions : SessionFunctionsBase<int, int, Empty> { }

        /// <summary>Writes records that carry an expiration.</summary>
        internal class ExpiringUpsertFunctions : SimpleIntSimpleFunctions
        {
            public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, ReadOnlySpan<byte> value, ref int input)
            {
                var info = base.GetUpsertFieldInfo(key, value, ref input);
                info.HasExpiration = true;
                return info;
            }

            public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref int input, ReadOnlySpan<byte> srcValue, ref int output, ref UpsertInfo upsertInfo)
                => base.InitialWriter(ref dstLogRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo) && dstLogRecord.TrySetExpiration(1);
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

        private void UpsertExpiringKey(int key, int value)
        {
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpiringUpsertFunctions>(new ExpiringUpsertFunctions());
            _ = s.BasicContext.Upsert(TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref key)), SpanByte.FromPinnedVariable(ref value));
        }

        private static TestSpanByteKey Key(ref int k) => TestSpanByteKey.FromPinnedSpan(SpanByte.FromPinnedVariable(ref k));

        private Status ReadKey(int key, out int value)
        {
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, SimpleIntSimpleFunctions>(new SimpleIntSimpleFunctions());
            int input = 0, output = 0;
            var status = s.BasicContext.Read(Key(ref key), ref input, ref output);
            if (status.IsPending)
            {
                _ = s.BasicContext.CompletePendingWithOutputs(out var outputs, wait: true);
                using (outputs)
                {
                    ClassicAssert.IsTrue(outputs.Next());
                    status = outputs.Current.Status;
                    output = outputs.Current.Output;
                }
            }
            value = output;
            return status;
        }

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

        /// <summary>
        /// Runs an RMW of key 1 with input 50 whose new record must wait for a flush to be allocated, so the RMW retries. The key's live record
        /// is in memory, read-only, and cannot be elided, because the key's previous record is flushed.
        /// </summary>
        /// <param name="abandonedRecordSize">The size of a record the RMW allocates and abandons before the one that must wait, if any</param>
        private Status RMWWhoseAllocationWaitsForAFlush(ExpirableFunctions fn, bool liveRecordExpires, int abandonedRecordSize = 0)
        {
            // Rebuild the store over a device whose writes the test completes, so the new record's allocation waits for a flush.
            store.Dispose();
            log.Dispose();
            var gated = new GatedCompletionDevice(Devices.CreateLogDevice(Path.Join(MethodTestDir, "gated.log"), deleteOnClose: true));
            log = gated;
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(IntKeyComparer.Instance, new TrackingRecordTriggers(tracker))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
            var hlogBase = store.hlogBase;

            // The key's first record is flushed, so its second record links to it and cannot be elided.
            UpsertKey(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            var flushedPage = hlogBase.GetPage(hlogBase.FlushedUntilAddress);

            gated.Gate = true;
            var liveAddress = store.Log.TailAddress;
            if (liveRecordExpires)
                UpsertExpiringKey(1, 200);
            else
                UpsertKey(1, 200);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: false);

            // Fill memory through the last page that does not wait for a flush, leaving room on it for the abandoned record but not then for a
            // tombstone, the smallest record for the key.
            var k = 1;
            var tombstoneSize = store.hlog.GetDeleteRecordSize(Key(ref k)).AllocatedInlineRecordSize;
            var lastPage = flushedPage + hlogBase.BufferSize - 1;
            long NextAllocationPage(int size)
            {
                var tail = hlogBase.GetTailAddress();
                var offset = hlogBase.GetOffsetOnPage(tail);
                return hlogBase.GetPage(tail) + (offset != 0 && offset + size > hlogBase.PageSize ? 1 : 0);
            }
            using (var filler = store.NewSession<TestSpanByteKey, int, int, Empty, EmptyValueFunctions>(new EmptyValueFunctions()))
            {
                for (var fk = 1000; NextAllocationPage(abandonedRecordSize + tombstoneSize) <= lastPage; fk++)
                {
                    var before = hlogBase.GetTailAddress();
                    _ = filler.BasicContext.Upsert(Key(ref fk), []);
                    if (fk == 1000)
                        ClassicAssert.AreEqual(tombstoneSize, hlogBase.GetTailAddress() - before, "A filler record must take exactly the tombstone's space");
                }
            }
            if (abandonedRecordSize > 0)
                ClassicAssert.AreEqual(lastPage, NextAllocationPage(abandonedRecordSize), "The abandoned record must fit without waiting for a flush");
            ClassicAssert.AreEqual(flushedPage, hlogBase.GetPage(hlogBase.FlushedUntilAddress), "No flush may complete while the gate is closed");
            Assert.That(liveAddress, Is.GreaterThanOrEqualTo(hlogBase.HeadAddress).And.LessThan(hlogBase.ReadOnlyAddress),
                "The live record must be in memory and read-only, so the RMW replaces it with a new record");

            tracker.Reset();
            var rmw = Task.Factory.StartNew(() =>
            {
                using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpirableFunctions>(fn);
                var key = 1; var input = 50;
                var status = s.BasicContext.RMW(Key(ref key), ref input);
                if (status.IsPending)
                {
                    _ = s.BasicContext.CompletePendingWithOutputs(out var outputs, wait: true);
                    using (outputs)
                    {
                        ClassicAssert.IsTrue(outputs.Next());
                        status = outputs.Current.Status;
                    }
                }
                return status;
            }, TaskCreationOptions.LongRunning);

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (Volatile.Read(ref fn.needCopyUpdateCalls) == 0)
            {
                Assert.That(rmw.IsCompleted, Is.False, "The RMW finished without reaching the live record");
                Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "The RMW never reached NeedCopyUpdate");
                _ = Thread.Yield();
            }

            // Past NeedCopyUpdate the RMW holds the epoch until its allocation has failed, so this action runs only after that failure.
            using var allocationFailed = new ManualResetEventSlim();
            store.epoch.Resume();
            store.epoch.BumpCurrentEpoch(allocationFailed.Set);
            store.epoch.Suspend();
            while (!allocationFailed.IsSet)
            {
                Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "The RMW never released the epoch");
                store.epoch.Resume();
                store.epoch.ProtectAndDrain();
                store.epoch.Suspend();
                _ = Thread.Yield();
            }
            Assert.That(rmw.IsCompleted, Is.False, "The RMW's allocation must wait for the flush");

            // Complete every captured write, including any captured as the gate opens, until the retried RMW finishes.
            gated.Gate = false;
            var released = 0;
            while (!rmw.Wait(10))
            {
                Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "The RMW never completed");
                for (var count = gated.PendingCount; released < count; released++)
                    gated.Release(released);
            }
            for (var count = gated.PendingCount; released < count; released++)
                gated.Release(released);
            return rmw.Result;
        }

        /// <summary>
        /// ExpireAndStop of a live record that cannot be elided appends a tombstone record. When that allocation has to wait for a flush the RMW
        /// retries, and the retry must still find the record live: the delete succeeds and the key stays absent, in memory and on disk.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ExpireAndStopOfALiveRecordSurvivesAWaitForFlush()
        {
            var status = RMWWhoseAllocationWaitsForAFlush(new ExpirableFunctions { expireAction = RMWAction.ExpireAndStop, declineInitialUpdate = true }, liveRecordExpires: false);
            ClassicAssert.IsTrue(status.Found && status.IsExpired, $"Deleting the live record must succeed, but was {status}");

            ClassicAssert.IsTrue(ReadKey(1, out _).NotFound, "The deleted key must be absent in memory");
            store.Log.FlushAndEvict(wait: true);
            ClassicAssert.IsTrue(ReadKey(1, out _).NotFound, "The deleted key must be absent once evicted");
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

        [Test]
        [Category("TsavoriteKV")]
        public void ImmutableExpireAndResumeWithoutInitialUpdateLeavesKeyAbsentTest()
        {
            UpsertKey(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();
            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndResume, declineInitialUpdate = true };
            using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpirableFunctions>(fn);
            var k = 1; var input = 50;
            var status = s.BasicContext.RMW(Key(ref k), ref input);
            ClassicAssert.IsFalse(status.Found, status.ToString());
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "The expired source is disposed once");

            // The disposed source must not be found again, by a Read or by a later RMW
            var output = 0;
            status = s.BasicContext.Read(Key(ref k), ref output);
            ClassicAssert.IsFalse(status.Found, status.ToString());
            status = s.BasicContext.RMW(Key(ref k), ref input);
            ClassicAssert.IsFalse(status.Found, status.ToString());
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "The expired source is not disposed again");
        }

        /// <summary>
        /// NeedCopyUpdate expires a read-only record whose replacement must wait for a flush to be allocated, so the RMW retries. Disposal has
        /// cleared the record's expiration, yet the retry must still find the key absent and create it afresh, not update the expired value.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ExpiredRecordStaysAbsentWhileItsReplacementWaitsForAFlush()
        {
            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndResume, expireOnlyWithExpiration = true };
            var status = RMWWhoseAllocationWaitsForAFlush(fn, liveRecordExpires: true);
            ClassicAssert.IsTrue(!status.Found && status.Record.Created, $"The expired key must be created afresh, but was {status}");
            ClassicAssert.AreEqual(1, fn.needCopyUpdateCalls, "Only the first attempt finds the expired record");
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "The expired record is disposed once");

            store.Log.FlushAndEvict(wait: true);
            ClassicAssert.IsTrue(ReadKey(1, out var value).Found);
            ClassicAssert.AreEqual(50, value);
        }

        /// <summary>
        /// CopyUpdater expires a read-only record, and the initial value does not fit the record allocated for the copy, so the RMW abandons
        /// that record for an initial update, whose allocation must wait for a flush, so the RMW retries. Disposal has cleared the record's
        /// expiration, yet the retry must still find the key absent and create it afresh, not copy-update the expired value.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void CopyUpdaterExpirationStaysAbsentWhileTheReplacementWaitsForAFlush()
        {
            // SimpleIntSimpleFunctions sizes a copy as it sizes an initial record: the key and an int value
            var k = 1; var input = 50;
            var copySize = store.hlog.GetRMWInitialRecordSize(Key(ref k), ref input, new SimpleIntSimpleFunctions()).AllocatedInlineRecordSize;

            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndResume, expireOnlyInCU = true, expireOnlyWithExpiration = true, requestLargerReinit = true };
            var status = RMWWhoseAllocationWaitsForAFlush(fn, liveRecordExpires: true, abandonedRecordSize: copySize);
            ClassicAssert.IsTrue(!status.Found && status.Record.Created, $"The expired key must be created afresh, but was {status}");
            ClassicAssert.AreEqual(1, fn.needCopyUpdateCalls, "Only the first attempt finds the expired record");
            ClassicAssert.AreEqual(1, tracker.DeletedCount, "The expired record is disposed once");

            store.Log.FlushAndEvict(wait: true);
            ClassicAssert.IsTrue(ReadKey(1, out var value).Found);
            ClassicAssert.AreEqual(50, value);
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

            public readonly void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason)
            {
                if (logRecord.IsSet)
                    tracker?.RecordDiskDispose(reason);
            }

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
            private int _diskDisposeCount;

            public int DisposeRecordDeletedCount => _disposeRecordDeletedCount;
            public int DiskDisposeCount => _diskDisposeCount;

            public void RecordDispose(DisposeReason reason)
            {
                if (reason == DisposeReason.Deleted)
                    Interlocked.Increment(ref _disposeRecordDeletedCount);
            }

            public void Reset()
            {
                Interlocked.Exchange(ref _disposeRecordDeletedCount, 0);
                Interlocked.Exchange(ref _diskDisposeCount, 0);
            }

            public void RecordDiskDispose(DisposeReason reason)
            {
                if (reason == DisposeReason.DeserializedFromDisk)
                    Interlocked.Increment(ref _diskDisposeCount);
            }
        }

        internal class ObjExpirableFunctions : TestObjectFunctionsDelete
        {
            internal RMWAction expireAction = RMWAction.Default;
            internal bool failIPU;
            internal bool expireOnlyInCU; // If true, only CopyUpdater returns the expire action (not IPU/NCU)
            internal bool checkHeapSize;

            public override bool InitialUpdater(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, ref TestObjectOutput output, ref RMWInfo rmwInfo)
            {
                if (checkHeapSize)
                {
                    Assert.That(logRecord.ValueObjectIsSet, Is.False);
                    Assert.That(logRecord.GetValueHeapMemorySize(), Is.Zero, "An unset object slot owns no value heap");
                }

                var success = base.InitialUpdater(ref logRecord, in sizeInfo, ref input, ref output, ref rmwInfo);
                if (checkHeapSize)
                    Assert.That(logRecord.GetValueHeapMemorySize(), Is.EqualTo(sizeof(int)), "A populated slot reports its object's heap size");
                return success;
            }

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
        public void ObjValueHeapSizeBeforeAndAfterInitializationTest()
        {
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(
                new ObjExpirableFunctions { checkHeapSize = true });
            var input = new TestObjectInput { value = 50 };
            var status = session.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 1);
            Assert.That(status.Record.Created, Is.True);
        }

        [Test]
        [Category("TsavoriteKV")]
        public void ObjExpireAndResumeFromCopyUpdaterTest([Values] bool fromDisk)
        {
            UpsertObj(1, 100);
            if (fromDisk)
                store.Log.FlushAndEvict(wait: true);
            else
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();

            var fn = new ObjExpirableFunctions { expireOnlyInCU = true, expireAction = RMWAction.ExpireAndResume };
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
            var context = session.BasicContext;
            var key = new TestObjectKey { key = 1 };
            var input = new TestObjectInput { value = 50 };
            var status = context.RMW(key, ref input, 1);
            Assert.That(status.IsPending, Is.EqualTo(fromDisk));
            if (fromDisk)
            {
                Assert.That(context.CompletePendingWithOutputs(out var outputs, wait: true), Is.True);
                (status, _) = GetSinglePendingResult(outputs);
            }

            Assert.That(status.IsExpired, Is.True);
            Assert.That(status.Record.Created, Is.True);
            Assert.That(tracker.DiskDisposeCount, Is.EqualTo(fromDisk ? 1 : 0));

            var output = new TestObjectOutput();
            status = context.Read(key, ref input, ref output);
            Assert.That(status.Found, Is.True);
            Assert.That(output.value.value, Is.EqualTo(50), "Expiration must initialize a fresh value rather than update the old one");
        }

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