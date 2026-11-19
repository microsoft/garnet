// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
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
        /// <param name="wentPending">Whether the RMW completed as a pending operation: the key's live record was evicted while the RMW waited,
        /// so the RMW completed over the record's disk image</param>
        /// <param name="abandonedRecordSize">The size of a record the RMW allocates and abandons before the one that must wait, if any</param>
        private Status RMWWhoseAllocationWaitsForAFlush(ExpirableFunctions fn, bool liveRecordExpires, out bool wentPending, int abandonedRecordSize = 0)
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
            var rmwWentPending = false;
            var rmw = Task.Factory.StartNew(() =>
            {
                using var s = store.NewSession<TestSpanByteKey, int, int, Empty, ExpirableFunctions>(fn);
                var key = 1; var input = 50;
                var status = s.BasicContext.RMW(Key(ref key), ref input);
                if (status.IsPending)
                {
                    rmwWentPending = true;
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
            var result = rmw.Result;
            wentPending = rmwWentPending;
            return result;
        }

        /// <summary>
        /// ExpireAndStop of a live record that cannot be elided appends a tombstone record. When that allocation has to wait for a flush the RMW
        /// retries, and the retry must still find the record live: the delete succeeds and the key stays absent, in memory and on disk.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ExpireAndStopOfALiveRecordSurvivesAWaitForFlush()
        {
            var status = RMWWhoseAllocationWaitsForAFlush(new ExpirableFunctions { expireAction = RMWAction.ExpireAndStop, declineInitialUpdate = true }, liveRecordExpires: false, out _);
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
        /// NeedCopyUpdate expires a read-only record whose replacement must wait for a flush to be allocated, so the RMW retries. The expired
        /// record is released only once its replacement is in the chain, so the retry finds it intact, still expired, and expires it again. The
        /// RMW must create the key afresh, not update the expired value. See <see cref="AssertExpiredSourceReleasedOnce"/> for the release.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ExpiredRecordStaysAbsentWhileItsReplacementWaitsForAFlush()
        {
            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndResume, expireOnlyWithExpiration = true };
            var status = RMWWhoseAllocationWaitsForAFlush(fn, liveRecordExpires: true, out var wentPending);
            ClassicAssert.IsTrue(!status.Found && status.Record.Created, $"The expired key must be created afresh, but was {status}");
            Assert.That(fn.needCopyUpdateCalls, Is.GreaterThanOrEqualTo(2), "The retry must find the expired record intact");
            AssertExpiredSourceReleasedOnce(wentPending);
        }

        /// <summary>
        /// CopyUpdater expires a read-only record, and the initial value does not fit the record allocated for the copy, so the RMW abandons
        /// that record for an initial update, whose allocation must wait for a flush, so the RMW retries. The expired record is released only
        /// once its replacement is in the chain, so the retry finds it intact and expires it again. The RMW must create the key afresh, not
        /// copy-update the expired value. See <see cref="AssertExpiredSourceReleasedOnce"/> for the release.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void CopyUpdaterExpirationStaysAbsentWhileTheReplacementWaitsForAFlush()
        {
            // SimpleIntSimpleFunctions sizes a copy as it sizes an initial record: the key and an int value
            var k = 1; var input = 50;
            var copySize = store.hlog.GetRMWInitialRecordSize(Key(ref k), ref input, new SimpleIntSimpleFunctions()).AllocatedInlineRecordSize;

            var fn = new ExpirableFunctions { expireAction = RMWAction.ExpireAndResume, expireOnlyInCU = true, expireOnlyWithExpiration = true, requestLargerReinit = true };
            var status = RMWWhoseAllocationWaitsForAFlush(fn, liveRecordExpires: true, out var wentPending, abandonedRecordSize: copySize);
            ClassicAssert.IsTrue(!status.Found && status.Record.Created, $"The expired key must be created afresh, but was {status}");
            Assert.That(fn.needCopyUpdateCalls, Is.GreaterThanOrEqualTo(2), "The retry must find the expired record intact");
            AssertExpiredSourceReleasedOnce(wentPending);
        }

        /// <summary>
        /// Check the release of the expired record that an RMW from <see cref="RMWWhoseAllocationWaitsForAFlush"/> replaced, and that the key
        /// then holds the RMW's initial value.
        /// </summary>
        /// <remarks>
        /// The expired record is released as deleted once its replacement is in the chain, so its delete trigger runs once if it is still in
        /// memory then. Its own flush may still be in flight, which defers the release until the flush completes; eviction runs a deferred
        /// release that the drain has not. The record may instead be evicted while the RMW waits, as a live record, like any other, and the RMW
        /// then completes over its disk image, which holds nothing to release: no delete trigger runs.
        /// </remarks>
        private void AssertExpiredSourceReleasedOnce(bool wentPending)
        {
            Assert.That(tracker.DeletedCount, Is.LessThanOrEqualTo(1), "The expired record is disposed at most once");
            store.Log.FlushAndEvict(wait: true);
            if (wentPending)
                ClassicAssert.AreEqual(0, tracker.DeletedCount, "The expired record was evicted live before its replacement was in the chain");
            else
                ClassicAssert.AreEqual(1, tracker.DeletedCount, "The expired record is disposed once");

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
            internal bool expireInPostCopyUpdater; // If true, PostCopyUpdater returns ExpireAndStop
            internal bool wrongTypeInPostCopyUpdater; // If true, PostCopyUpdater returns WrongType
            internal bool declineInitialUpdate; // If true, NeedInitialUpdate declines to create a record
            internal Action onNeedCopyUpdate; // Called first in NeedCopyUpdate, after the RMW has chosen whether to elide its source
            internal Action onCopyUpdater; // Called first in CopyUpdater, after the RMW has allocated its new record
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
                if (wrongTypeInPostCopyUpdater) { rmwInfo.Action = RMWAction.WrongType; return false; }
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

        /// <summary>Writes an expiration on every Upsert, so that a release that cleared the record's optionals would change its bytes.</summary>
        internal class ObjExpiringUpsertFunctions : TestObjectFunctionsDelete
        {
            /// <summary>The expiration every Upsert writes; Tsavorite does not interpret it.</summary>
            internal const long Expiration = 1L << 40;

            public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref TestObjectInput input)
            {
                var fieldInfo = base.GetUpsertFieldInfo(key, value, ref input);
                fieldInfo.HasExpiration = true;
                return fieldInfo;
            }

            public override bool InitialWriter(ref LogRecord logRecord, in RecordSizeInfo sizeInfo, ref TestObjectInput input, IHeapObject srcValue, ref TestObjectOutput output, ref UpsertInfo upsertInfo)
                => base.InitialWriter(ref logRecord, in sizeInfo, ref input, srcValue, ref output, ref upsertInfo) && logRecord.TrySetExpiration(Expiration);
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
            CreateStore();
        }

        /// <summary>Create the store on the fixture's devices, with every key out of line if <paramref name="overflowKeys"/>, and with
        /// <paramref name="revivificationSettings"/>, which are off by default.</summary>
        private void CreateStore(bool overflowKeys = false, RevivificationSettings revivificationSettings = null)
            => store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = (IDevice)gatedLog ?? log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize,
                CheckpointDir = MethodTestDir,
                MaxInlineKeySize = overflowKeys ? 0 : KVSettings.DefaultMaxInlineKeySize,
                RevivificationSettings = revivificationSettings
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(),
                    new ObjTrackingRecordTriggers(tracker))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

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

        /// <summary>Upsert <paramref name="key"/> with a new value and an expiration, and return a weak reference to the value, which only the
        /// store then holds.</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private WeakReference UpsertExpiring(int key, int value)
        {
            var valueObject = new TestObjectValue { value = value };
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpiringUpsertFunctions>(new ObjExpiringUpsertFunctions());
            _ = s.BasicContext.Upsert(new TestObjectKey { key = key }, valueObject, 0);
            return new WeakReference(valueObject);
        }

        /// <summary>The address of the record the next upsert writes: the tail, or the first page's first valid address on an empty log.</summary>
        private long NextRecordAddress()
        {
            var address = store.Log.TailAddress;
            var firstValidAddress = store.hlogBase.GetFirstValidLogicalAddressOnPage(store.hlogBase.GetPage(address));
            return address < firstValidAddress ? firstValidAddress : address;
        }

        /// <summary>A copy of the bytes of the record at <paramref name="logicalAddress"/> after its <see cref="RecordInfo"/>, which a deletion's
        /// seal and deferred-disposal mark change.</summary>
        private unsafe byte[] BytesAfterRecordInfo(long logicalAddress)
        {
            var logRecord = store.hlogBase._wrapper.CreateLogRecord(logicalAddress);
            return new ReadOnlySpan<byte>((byte*)logRecord.physicalAddress + RecordInfo.Size, logRecord.AllocatedSize - RecordInfo.Size).ToArray();
        }

        /// <summary>The index of the <see cref="ObjectIdMap"/> slot that the value of the record at <paramref name="logicalAddress"/> refers to.</summary>
        private unsafe int ValueSlot(long logicalAddress)
        {
            var logRecord = store.hlogBase._wrapper.CreateLogRecord(logicalAddress);
            Assert.That(logRecord.DataHeader.ValueIsObject, Is.True, $"The record at {logicalAddress} has no object value");
            return ObjectIdMap.GetIndex(*(int*)logRecord.DataHeader.GetValueFieldInfo(logRecord.physicalAddress).valueAddress);
        }

        /// <summary>Copy the record at <paramref name="logicalAddress"/> into pinned memory, as a scan does before it resolves the copy's object
        /// ids.</summary>
        private unsafe SectorAlignedMemory CopyRecord(long logicalAddress)
        {
            var logRecord = store.hlogBase._wrapper.CreateLogRecord(logicalAddress);
            var copy = store.hlogBase.bufferPool.Get(logRecord.AllocatedSize);
            Buffer.MemoryCopy((void*)logRecord.physicalAddress, copy.GetValidPointer(), logRecord.AllocatedSize, logRecord.AllocatedSize);
            return copy;
        }

        /// <summary>Resolve the object ids of <paramref name="copy"/>, a copy of the record at <paramref name="logicalAddress"/>, as a scan does,
        /// and return the copy's value object.</summary>
        private unsafe IHeapObject ResolveCopiedValue(long logicalAddress, SectorAlignedMemory copy)
        {
            Assert.That(store.hlogBase._wrapper.TryCreateRemappedLogRecordOverPinnedTransientMemory(logicalAddress, (long)copy.GetValidPointer(), out var remapped),
                Is.True, "The copy's key could not be resolved");
            try
            {
                return remapped.ValueObject;
            }
            finally
            {
                remapped.Dispose();
            }
        }

        /// <summary>The number of out-of-line fields (overflow keys, and object or overflow values) of the records on <paramref name="page"/>
        /// below the tail.</summary>
        private int CountOutOfLineFields(int page)
        {
            var count = 0;
            var endAddress = Math.Min(store.Log.TailAddress, store.hlogBase.GetLogicalAddressOfStartOfPage(page + 1));
            for (var address = store.hlogBase.GetFirstValidLogicalAddressOnPage(page); address < endAddress;)
            {
                var logRecord = store.hlogBase._wrapper.CreateLogRecord(address);
                if (logRecord.DataHeader.KeyIsOverflow)
                    count++;
                if (!logRecord.DataHeader.ValueIsInline)
                    count++;
                address += logRecord.AllocatedSize;
            }
            return count;
        }

        /// <summary>Assert that the value of the record at <paramref name="logicalAddress"/> was released in place: the record is still an object
        /// record, and its value's slot is allocated and holds the released placeholder.</summary>
        private void AssertReleasedInPlace(long logicalAddress)
        {
            var logRecord = store.hlogBase._wrapper.CreateLogRecord(logicalAddress);
            Assert.That(logRecord.DataHeader.ValueIsObject, Is.True, "The release converted the record's value");
            Assert.That(logRecord.TryGetOutOfLineComponents(out _, out _, out var slotObject), Is.True, "The release freed the value's slot");
            Assert.That(slotObject, Is.SameAs(SupersededObjectValue.Released), "The value's slot does not hold the released placeholder");
            Assert.That(logRecord.ValueObject, Is.Null, "A released value must read as no value");
        }

        /// <summary>Assert that nothing holds the objects that <paramref name="values"/> refer to.</summary>
        private static void AssertCollectable(params WeakReference[] values)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            foreach (var value in values)
                Assert.That(value.IsAlive, Is.False, "Something still holds a released value");
        }

        /// <summary>Stop holding writes, and let every write the gate captured complete.</summary>
        private void ReleaseGatedWrites()
        {
            gatedLog.Gate = false;
            for (var i = 0; i < gatedLog.PendingCount; i++)
            {
                gatedLog.Release(i);
                gatedLog.WaitForCompletion(i, TimeSpan.FromSeconds(30));
            }
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
        /// An expiration releases its source only once a new record is in the chain. If NeedInitialUpdate then declines to create one, nothing
        /// will replace the expired source, so it is deleted in place and elided: its delete trigger runs once, its value is released, and the
        /// key stays absent.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjExpireAndResumeWithoutInitialUpdateDeletesTheSourceTest()
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
                var status = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
                ClassicAssert.IsFalse(status.Found, status.ToString());

                TestObjectOutput output = default;
                status = s.BasicContext.Read(new TestObjectKey { key = 1 }, ref output, 0);
                ClassicAssert.IsFalse(status.Found, $"The expired key must stay absent; {status}");
            }

            Assert.That(store.Log.TailAddress, Is.EqualTo(tailBefore), "NeedInitialUpdate declined, so no record should have been created");
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "The expired source must be deleted exactly once");
            var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
            Assert.That(source.Info.Tombstone, Is.True, "The expired source must be a tombstone");
            Assert.That(source.ValueObject, Is.Null, "The expired source's value must be released");
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

            // The source is durable, so the drain at the end of the hold runs the deletion.
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "The end of the hold must run the deletion");
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "The deletion must run exactly once");
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

            // The source is durable, so the drain at the end of the hold runs the deletion.
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "The end of the hold must run the deletion");
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "The deletion must run exactly once");
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

        /// <summary>
        /// A drain whose range starts below a streaming snapshot's hold, on the page where the hold starts, reaches held marks in that page.
        /// It must give a held mark back, so the drain at the end of the hold runs the deletion; claiming it would dispose the record while
        /// the scan may push it, and dropping it would leave the deletion to eviction.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjDrainFromBelowAMidPageHoldLeavesTheHeldMarkTest()
        {
            var firstAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            var heldAddress = store.Log.TailAddress;
            UpsertObj(2, 200);
            Assert.That(store.hlogBase.GetPage(heldAddress), Is.EqualTo(store.hlogBase.GetPage(firstAddress)), "Precondition: both records are on one page");
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();

            store.hlogBase.BeginStreamingScanHold(heldAddress);
            try
            {
                using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete()))
                    _ = s.BasicContext.Delete(new TestObjectKey { key = 2 });
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(heldAddress).Info.DeferredDispose, Is.True, "Precondition: the held deletion is marked");

                store.hlog.DrainDeferredDisposes(firstAddress, store.Log.FlushedUntilAddress);
                ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "The drain ran a deletion the hold covers");
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(heldAddress).Info.DeferredDispose, Is.True, "The drain must give the held mark back");
            }
            finally
            {
                store.hlogBase.EndStreamingScanHold();
            }
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "The end of the hold must run the deletion");
        }

        #endregion

        #region The flush freeze's upper bound

        /// <summary>
        /// A read-only shift publishes ReadOnlyAddress and then bumps the epoch with the action that advances SafeReadOnlyAddress and starts
        /// the flush's walk. That action waits only for threads that held the epoch at the bump, so an operation that resumed after it does not
        /// hold the flush back. A record between SafeReadOnlyAddress and ReadOnlyAddress is therefore frozen: a Delete marks its disposal and
        /// seals it rather than eliding it, and the drain runs the deletion once the flush completes. Here another thread holds the epoch so the
        /// action stays pending, which leaves the record in that range.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjDeleteOfARecordWhoseReadOnlyFlushHasNotStartedIsDeferredTest()
        {
            var recordAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            tracker.Reset();

            var epoch = store.hlogBase.epoch;
            using var holding = new ManualResetEventSlim(false);
            using var release = new ManualResetEventSlim(false);
            var holder = new Thread(() =>
            {
                epoch.Resume();
                try
                {
                    holding.Set();
                    _ = release.Wait(TimeSpan.FromSeconds(30));
                }
                finally
                {
                    epoch.Suspend();
                }
            });
            holder.Start();
            try
            {
                Assert.That(holding.Wait(TimeSpan.FromSeconds(30)), Is.True, "The epoch holder did not start");
                store.hlogBase.ShiftReadOnlyAddressWithWait(store.Log.TailAddress, wait: false);
                Assert.That(store.hlogBase.ReadOnlyAddress, Is.GreaterThan(recordAddress), "Precondition: the record is below ReadOnlyAddress");
                Assert.That(store.hlogBase.SafeReadOnlyAddress, Is.LessThanOrEqualTo(recordAddress),
                    "Precondition: the action that starts the record's flush is still pending");

                using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete()))
                    _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });

                ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "The flush may start at any moment, so the deletion must wait for it");
                var source = store.hlogBase._wrapper.CreateLogRecord(recordAddress);
                Assert.That(source.Info.Valid, Is.True, "A record whose flush may start must not be elided");
                Assert.That(source.Info.IsSealed, Is.True, "The bypassed source should be sealed");
                Assert.That(source.Info.DeferredDispose, Is.True, "The source's disposal should be marked");
            }
            finally
            {
                release.Set();
                holder.Join();
            }

            // The drain runs on the flush completion, after the wait is released, so poll for it.
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            Assert.That(SpinWait.SpinUntil(() => tracker.DisposeRecordDeletedCount > 0, TimeSpan.FromSeconds(30)), Is.True,
                "The drain must run the deletion once the flush completes");
            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "OnDispose(Deleted) should be called exactly once");
        }

        #endregion

        #region A new record that bypasses an elidable source detaches it

        /// <summary>
        /// An RMW that finds an expired source and writes a new record with InitialUpdater, after NeedCopyUpdate or CopyUpdater returned
        /// <see cref="RMWAction.ExpireAndResume"/>, bypasses an elidable source in the chain. The source is released as deleted and then elided,
        /// as a CopyUpdate's source is; left valid and unsealed, it would hold its record until eviction and a scan would report it.
        /// <see cref="ObjDisposeOnImmutableExpireAndResumeTest"/> checks the release.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjExpireAndResumeElidesTheExpiredSourceTest([Values] bool expireInCopyUpdater)
        {
            var sourceAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            var fn = new ObjExpirableFunctions { expireAction = RMWAction.ExpireAndResume, expireOnlyInCU = expireInCopyUpdater };
            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn))
            {
                var input = new TestObjectInput { value = 5 };
                var status = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
                Assert.That(status.Record.Created, Is.True, "Precondition: the RMW wrote a new record");
            }

            Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).Info.Valid, Is.False, "The bypassed expired source should be elided");
        }

        /// <summary>
        /// An RMW that finds a tombstone writes a new record with InitialUpdater, which bypasses the tombstone in the chain when it is elidable.
        /// The tombstone is then elided, as a CopyUpdate's source is.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjInitialUpdateOverAnElidableTombstoneElidesItTest()
        {
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            // Deleting the immutable record writes a tombstone at the tail that takes the record's place, so the tombstone is the key's only record.
            var tombstoneAddress = store.Log.TailAddress;
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });
            var tombstone = store.hlogBase._wrapper.CreateLogRecord(tombstoneAddress);
            Assert.That(tombstone.Info.Valid && tombstone.Info.Tombstone, Is.True, $"Precondition: the Delete wrote a tombstone: {tombstone.Info}");

            var input = new TestObjectInput { value = 5 };
            var status = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            Assert.That(status.Record.Created, Is.True, "Precondition: the RMW wrote a new record");
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(tombstoneAddress).Info.Valid, Is.False, "The bypassed tombstone should be elided");
        }

        /// <summary>
        /// A mutable tombstone with an earlier record of its key in the chain is not elidable, so an InitialUpdater record does not bypass it.
        /// The tombstone stays in the chain below the new record, sealed, as a CopyUpdate's non-elidable source in the mutable region is.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjInitialUpdateOverANonElidableTombstoneSealsItTest()
        {
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            // A blind Upsert writes a mutable record whose PreviousAddress is the immutable one, and a Delete then tombstones it in place.
            // A filler first keeps it above SafeReadOnlyAddress, where a superseded record is sealed.
            UpsertObj(2, 0);
            var tombstoneAddress = store.Log.TailAddress;
            UpsertObj(1, 200);
            using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
            _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });
            var tombstone = store.hlogBase._wrapper.CreateLogRecord(tombstoneAddress);
            Assert.That(tombstone.Info.Valid && tombstone.Info.Tombstone && !tombstone.Info.IsSealed, Is.True,
                $"Precondition: the Delete left an unsealed tombstone in the chain: {tombstone.Info}");

            var input = new TestObjectInput { value = 5 };
            var status = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            Assert.That(status.Record.Created, Is.True, "Precondition: the RMW wrote a new record");
            tombstone = store.hlogBase._wrapper.CreateLogRecord(tombstoneAddress);
            Assert.That(tombstone.Info.Valid && tombstone.Info.IsSealed, Is.True, $"The superseded tombstone should stay in the chain, sealed: {tombstone.Info}");
        }

        /// <summary>
        /// A PostCopyUpdater that returns <see cref="RMWAction.WrongType"/> does so after the new record is in the chain, so the source is
        /// superseded and, when elidable, bypassed. It is elided, as on success, but its value is not released as a CopyUpdate's is.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjPostCopyUpdaterWrongTypeDetachesTheSourceTest()
        {
            var sourceAddress = store.Log.TailAddress;
            UpsertObj(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();

            var fn = new ObjExpirableFunctions { wrongTypeInPostCopyUpdater = true };
            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn))
            {
                var input = new TestObjectInput { value = 5 };
                _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            }

            ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "Nothing was deleted");
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).Info.Valid, Is.False, "The bypassed source should be elided");
        }

        #endregion

        #region A deletion is released in place

        /// <summary>
        /// A Delete of a durable source that nothing freezes releases the source at once, in place: the delete trigger runs, the value's slot
        /// keeps its id and gets the released placeholder, and none of the record's bytes change but its <see cref="RecordInfo"/>, which the
        /// Delete seals; the record's expiration stays. A scan may be copying the record without a lock meanwhile.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjDeleteReleasesADurableSourceWithoutChangingItsBytesTest()
        {
            // An earlier record of the key keeps the source in a chain, so the Delete seals the source rather than eliding it.
            _ = UpsertExpiring(1, 50);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            var sourceAddress = NextRecordAddress();
            var sourceValue = UpsertExpiring(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            Assert.That(store.hlog.IsFrozenForFlush(sourceAddress), Is.False, "Precondition: the source is durable");
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).Expiration, Is.EqualTo(ObjExpiringUpsertFunctions.Expiration),
                "Precondition: the source has an expiration");
            var bytesBefore = BytesAfterRecordInfo(sourceAddress);
            tracker.Reset();

            using (var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete()))
                _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });

            ClassicAssert.AreEqual(1, tracker.DisposeRecordDeletedCount, "The Delete must release a durable source at once");
            var source = store.hlogBase._wrapper.CreateLogRecord(sourceAddress);
            Assert.That(source.Info.Valid && source.Info.IsSealed, Is.True, $"Precondition: the Delete sealed the source: {source.Info}");
            Assert.That(BytesAfterRecordInfo(sourceAddress), Is.EqualTo(bytesBefore), "The release changed the record's bytes");
            AssertReleasedInPlace(sourceAddress);
            AssertCollectable(sourceValue);
        }

        /// <summary>
        /// The drain releases a deletion that a flush froze once the flush completes, and takes no lock to do so, so a scan may have copied the
        /// record, under the key's lock, and not yet resolved the copy's object ids. The flush leaves the records' page straddling
        /// ReadOnlyAddress, as a FoldOver checkpoint's does, and new records then allocate from the page's ObjectIdMap. The release changes none
        /// of the record's bytes but its <see cref="RecordInfo"/>, and keeps the value's slot, holding the released placeholder: so no new record
        /// is given the slot, and the copy's value resolves to no value rather than to a new record's. The retained slots never grow the page's
        /// map beyond one entry per out-of-line field of its records, and hold none of the released values.
        /// </summary>
        [Test]
        [Category("TsavoriteKV"), Category("CheckpointRestore")]
        public void ObjDeletionsFrozenForFlushKeepTheirSlotsWhenTheDrainReleasesThemTest([Values] bool foldOverCheckpoint)
        {
            const int SourceCount = 4;
            var sourceAddresses = new long[SourceCount];
            var sourceValues = new WeakReference[SourceCount];
            for (var i = 0; i < SourceCount; i++)
            {
                sourceAddresses[i] = NextRecordAddress();
                sourceValues[i] = UpsertExpiring(i, i * 10);
            }
            var page = store.hlogBase.GetPage(sourceAddresses[0]);

            // Create the session before the checkpoint, so that it takes part from PREPARE onward and sees the checkpoint in progress.
            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());

            // Hold the flush's device write, so FlushedUntilAddress stays below the records once the flush has walked them. A FoldOver
            // checkpoint's flush, like the read-only shift, moves ReadOnlyAddress to the tail, which is mid-page.
            gatedLog.Gate = true;
            if (foldOverCheckpoint)
                Assert.That(store.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver), Is.True);
            else
                store.hlogBase.ShiftReadOnlyAddressWithWait(store.Log.TailAddress, wait: false);

            byte[][] bytesBefore;
            SectorAlignedMemory copy = null;
            try
            {
                gatedLog.WaitForPending(1, TimeSpan.FromSeconds(30));
                tracker.Reset();
                for (var i = 0; i < SourceCount; i++)
                {
                    Assert.That(store.hlog.IsFrozenForFlush(sourceAddresses[i]), Is.True, "Precondition: the flush is writing the record");
                    _ = session.BasicContext.Delete(new TestObjectKey { key = i });
                    Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddresses[i]).Info.DeferredDispose, Is.True, "Precondition: the deletion is marked");
                }
                ClassicAssert.AreEqual(0, tracker.DisposeRecordDeletedCount, "Precondition: the deletions wait for the flush");
                bytesBefore = sourceAddresses.Select(BytesAfterRecordInfo).ToArray();

                // The scan's copy of the first record, whose object ids it has not resolved yet.
                copy = CopyRecord(sourceAddresses[0]);
            }
            finally
            {
                ReleaseGatedWrites();
            }

            try
            {
                Assert.That(SpinWait.SpinUntil(() => tracker.DisposeRecordDeletedCount == SourceCount, TimeSpan.FromSeconds(30)), Is.True,
                    "The drain did not release the deletions once the flush completed");
                if (foldOverCheckpoint)
                    store.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

                // New records on the same page allocate from its map.
                var newAddresses = new long[SourceCount];
                for (var i = 0; i < SourceCount; i++)
                {
                    newAddresses[i] = NextRecordAddress();
                    UpsertObj(SourceCount + i, 100 + i);
                }
                Assert.That(store.hlogBase.GetPage(store.Log.TailAddress - 1), Is.EqualTo(page), "Precondition: the new records are on the deleted records' page");

                Assert.That(ResolveCopiedValue(sourceAddresses[0], copy), Is.Null, "The scan's copy resolved to another record's value");
                var sourceSlots = sourceAddresses.Select(ValueSlot).ToArray();
                foreach (var newAddress in newAddresses)
                    Assert.That(sourceSlots, Has.No.Member(ValueSlot(newAddress)), "A new record was given a slot that a released record holds");

                for (var i = 0; i < SourceCount; i++)
                {
                    Assert.That(BytesAfterRecordInfo(sourceAddresses[i]), Is.EqualTo(bytesBefore[i]), $"The release of record {i} changed its bytes");
                    AssertReleasedInPlace(sourceAddresses[i]);
                }

                var objectIdMap = store.hlogBase._wrapper.CreateLogRecord(sourceAddresses[0]).ObjectIdMap;
                Assert.That(objectIdMap.Count, Is.EqualTo(CountOutOfLineFields(page)), "The page's map must hold one entry per out-of-line field of its records");
                AssertCollectable(sourceValues);
            }
            finally
            {
                copy.Return();
            }
        }

        #endregion

        #region An elided record keeps its key

        /// <summary>How a record alone in its hash chain, with an out-of-line key, is elided.</summary>
        public enum ElisionPath
        {
            /// <summary>An in-place Delete of the mutable record.</summary>
            InPlaceDelete,

            /// <summary>An in-place expiration of the mutable record: InPlaceUpdater returns <see cref="RMWAction.ExpireAndStop"/>.</summary>
            InPlaceExpiration,

            /// <summary>An expiration of the durable record with no new record: NeedCopyUpdate returns <see cref="RMWAction.ExpireAndStop"/>.</summary>
            ImmutableExpiration,

            /// <summary>A Delete of the durable record, whose tombstone bypasses it.</summary>
            BypassedByDelete,

            /// <summary>An in-place Delete with the free-record pool, of a mutable record below the revivifiable range, which the pool does not
            /// take and does not restore to the chain.</summary>
            InPlaceDeleteBelowTheRevivifiableRange,
        }

        /// <summary>
        /// An elided record keeps its overflow key: a scan, or a lookup that read the record's address before the elision, still reads the key.
        /// The key was charged to the size tracker when the record was inserted, and the eviction walk skips invalid records, so each elision
        /// credits the key; the tracker returns to zero once every record is evicted.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        public void ObjElidedRecordKeepsItsOverflowKeyAndCreditsItTest([Values] ElisionPath path)
        {
            RevivificationSettings revivificationSettings = null;
            if (path == ElisionPath.InPlaceDeleteBelowTheRevivifiableRange)
            {
                // Only the top of the mutable region is revivifiable, and the pool does not put back a record it does not take.
                revivificationSettings = RevivificationSettings.PowerOf2Bins.Clone();
                revivificationSettings.RevivifiableFraction = 0.05;
                revivificationSettings.RestoreDeletedRecordsIfBinIsFull = false;
            }
            store.Dispose();
            CreateStore(overflowKeys: true, revivificationSettings);
            const long TargetSize = 1L << 20;
            var sizeTracker = new LogSizeTracker<ObjTrackingStoreFunctions, ObjTrackingAllocator>(store.Log, TargetSize, TargetSize / 8, TargetSize / 16, logger: null);
            store.Log.SetLogSizeTracker(sizeTracker);

            var address = NextRecordAddress();
            UpsertObj(1, 100);
            Assert.That(store.hlogBase._wrapper.CreateLogRecord(address).DataHeader.KeyIsOverflow, Is.True, "Precondition: the key is out of line");
            if (path is ElisionPath.ImmutableExpiration or ElisionPath.BypassedByDelete)
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            if (path == ElisionPath.InPlaceDeleteBelowTheRevivifiableRange)
                Assert.That(address, Is.LessThan(store.GetMinRevivifiableAddress()), "Precondition: the record is below the revivifiable range");

            if (path is ElisionPath.InPlaceExpiration or ElisionPath.ImmutableExpiration)
            {
                var fn = new ObjExpirableFunctions { expireAction = RMWAction.ExpireAndStop };
                using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, ObjExpirableFunctions>(fn);
                var input = new TestObjectInput { value = 0 };
                _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, 0);
            }
            else
            {
                using var s = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());
                _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });
            }

            var record = store.hlogBase._wrapper.CreateLogRecord(address);
            Assert.That(record.Info.Valid, Is.False, $"Precondition: the record was elided: {record.Info}");
            Assert.That(record.DataHeader.KeyIsOverflow, Is.True, "The elision released the record's key");
            Assert.That(BitConverter.ToInt32(record.Key), Is.EqualTo(1), "The elided record's key does not resolve to its key");

            store.Log.FlushAndEvict(wait: true);
            Assert.That(sizeTracker.LogHeapSizeBytes, Is.Zero, "Eviction must leave nothing counted: the elision must credit the key it keeps");
        }

        #endregion
    }
}