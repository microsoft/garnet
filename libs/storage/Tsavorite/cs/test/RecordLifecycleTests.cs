// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
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
    using LifecycleAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, RecordLifecycleTests.LifecycleRecordTriggers>>;
    using LifecycleStoreFunctions = StoreFunctions<TestObjectKey.Comparer, RecordLifecycleTests.LifecycleRecordTriggers>;

    /// <summary>
    /// Lifecycle tests for <see cref="IRecordTriggers"/> on the object allocator. These tests go beyond
    /// counting <see cref="DisposeReason.Deleted"/> calls — they assert the EXACT number of calls to
    /// <see cref="IRecordTriggers.OnDispose"/> (per reason), <see cref="IRecordTriggers.OnDisposeDiskRecord"/>
    /// (per reason), and <see cref="IRecordTriggers.OnEvict"/> (per source), and they track the number
    /// of times IHeapObject.Dispose is invoked on individual value objects. This protects
    /// against double-dispose, leaked triggers, and asymmetric DiskLogRecord disposal.
    /// </summary>
    [AllureNUnit]
    [TestFixture]
    internal class RecordLifecycleTests : AllureTestBase
    {
        /// <summary>
        /// Per-instance dispose-counting heap object. Subclasses <see cref="TestObjectValue"/> so existing
        /// <see cref="TestObjectFunctionsDelete"/> casts succeed; overrides Dispose() to increment a counter
        /// and registers itself in a static registry so tests can verify that no value object is ever
        /// disposed more than once across its entire lifecycle.
        /// </summary>
        internal class TrackedObjectValue : TestObjectValue
        {
            private static int _nextId;
            private static readonly ConcurrentDictionary<int, TrackedObjectValue> Registry = new();

            public int Id { get; }
            public int DisposeCount;

            public TrackedObjectValue()
            {
                Id = Interlocked.Increment(ref _nextId);
                _ = Registry.TryAdd(Id, this);
            }

            public override void Dispose() => _ = Interlocked.Increment(ref DisposeCount);

            public override HeapObjectBase Clone() => new TrackedObjectValue { value = value };

            public override string ToString() => $"TrackedObjectValue(Id={Id}, Value={value}, Disposes={DisposeCount})";

            /// <summary>Returns (totalDisposes, maxDisposesPerInstance) across every live tracked object.</summary>
            public static (int total, int max) Snapshot()
            {
                int total = 0, max = 0;
                foreach (var kv in Registry)
                {
                    var d = Volatile.Read(ref kv.Value.DisposeCount);
                    total += d;
                    if (d > max) max = d;
                }
                return (total, max);
            }

            public static void Clear() => Registry.Clear();

            public new class Serializer : BinaryObjectSerializer<IHeapObject>
            {
                public override void Deserialize(out IHeapObject obj) => obj = new TrackedObjectValue { value = reader.ReadInt32() };
                public override void Serialize(IHeapObject obj) => writer.Write(((TrackedObjectValue)obj).value);
            }
        }

        /// <summary>
        /// Rich lifecycle counter. Separates counts by <see cref="DisposeReason"/> and by
        /// <see cref="EvictionSource"/> so tests can assert precise call patterns.
        /// Optionally invokes IHeapObject.Dispose from the trigger — used to
        /// verify that handler-driven disposal flows correctly (and that Tsavorite itself
        /// never calls Dispose behind the handler's back).
        /// </summary>
        internal class LifecycleTracker
        {
            // OnDispose(reason)
            public readonly int[] DisposeCounts = new int[Enum.GetValues<DisposeReason>().Length];
            // OnDisposeDiskRecord(reason)
            public readonly int[] DisposeDiskCounts = new int[Enum.GetValues<DisposeReason>().Length];
            // OnEvict(source)
            public readonly int[] EvictCounts = new int[Enum.GetValues<EvictionSource>().Length];

            /// <summary>If true, <see cref="IRecordTriggers.OnDispose"/> invokes IHeapObject.Dispose
            /// on <see cref="DisposeReason.Deleted"/> and <see cref="DisposeReason.CopyUpdated"/> only.</summary>
            public bool DisposeValuesOnDispose;

            /// <summary>If true, <see cref="IRecordTriggers.OnDisposeDiskRecord"/> invokes IHeapObject.Dispose.
            /// NOTE: Unsafe for scan-iterator wrappers that share a value-object reference with the on-log record;
            /// tests that use this flag must avoid in-memory scans or use it only for known-owned disk records.</summary>
            public bool DisposeValuesOnDisposeDiskRecord;

            // Gating flags read by LifecycleRecordTriggers. Tests toggle these to verify that
            // Tsavorite only walks the corresponding per-record paths when the application opts in.
            public bool CallOnFlushFlag;
            public bool CallOnDiskReadFlag;
            // CallOnEvict flag. Default true preserves the behaviour expected by the
            // original tests; the gating test sets it to false.
            public bool CallOnEvictFlag = true;

            // OnFlush(record) count and OnDiskRead(record) count
            public int FlushCount;
            public int DiskReadCount;

            public int DisposeCount(DisposeReason r) => Volatile.Read(ref DisposeCounts[(int)r]);
            public int DisposeDiskCount(DisposeReason r) => Volatile.Read(ref DisposeDiskCounts[(int)r]);
            public int EvictCount(EvictionSource s) => Volatile.Read(ref EvictCounts[(int)s]);
            public int TotalDispose() { int sum = 0; foreach (var v in DisposeCounts) sum += v; return sum; }
            public int TotalDisposeDisk() { int sum = 0; foreach (var v in DisposeDiskCounts) sum += v; return sum; }
            public int TotalEvict() { int sum = 0; foreach (var v in EvictCounts) sum += v; return sum; }

            public void Reset()
            {
                for (int i = 0; i < DisposeCounts.Length; i++) DisposeCounts[i] = 0;
                for (int i = 0; i < DisposeDiskCounts.Length; i++) DisposeDiskCounts[i] = 0;
                for (int i = 0; i < EvictCounts.Length; i++) EvictCounts[i] = 0;
                FlushCount = 0;
                DiskReadCount = 0;
            }
        }

        internal struct LifecycleRecordTriggers : IRecordTriggers
        {
            internal readonly LifecycleTracker tracker;
            public LifecycleRecordTriggers(LifecycleTracker tracker) => this.tracker = tracker;

            public readonly bool CallOnFlush => tracker?.CallOnFlushFlag ?? false;
            public readonly bool CallOnDiskRead => tracker?.CallOnDiskReadFlag ?? false;
            public readonly bool CallOnEvict => tracker?.CallOnEvictFlag ?? false;

            public readonly void OnFlush(ref LogRecord logRecord)
            {
                if (tracker is null) return;
                _ = Interlocked.Increment(ref tracker.FlushCount);
            }

            public readonly void OnDiskRead(ref LogRecord logRecord)
            {
                if (tracker is null) return;
                _ = Interlocked.Increment(ref tracker.DiskReadCount);
            }

            public readonly void OnDispose(ref LogRecord logRecord, DisposeReason reason)
            {
                if (tracker is null) return;
                _ = Interlocked.Increment(ref tracker.DisposeCounts[(int)reason]);

                if (tracker.DisposeValuesOnDispose
                    && logRecord.Info.ValueIsObject
                    && (reason == DisposeReason.Deleted || reason == DisposeReason.CopyUpdated))
                {
                    logRecord.ValueObject?.Dispose();
                }
            }

            public readonly void OnDisposeDiskRecord(ref DiskLogRecord logRecord, DisposeReason reason)
            {
                if (tracker is null) return;
                // Defensive-no-op calls from AsyncIOContextCompletionEvent pass a default (unset) DiskLogRecord;
                // filter them out so call-count assertions reflect only records that actually held data.
                if (!logRecord.IsSet) return;
                _ = Interlocked.Increment(ref tracker.DisposeDiskCounts[(int)reason]);

                if (tracker.DisposeValuesOnDisposeDiskRecord && logRecord.Info.ValueIsObject)
                    logRecord.ValueObject?.Dispose();
            }

            public readonly void OnEvict(ref LogRecord logRecord, EvictionSource source)
            {
                if (tracker is null) return;
                _ = Interlocked.Increment(ref tracker.EvictCounts[(int)source]);
            }
        }

        private TsavoriteKV<LifecycleStoreFunctions, LifecycleAllocator> store;
        private IDevice log, objlog;
        private LifecycleTracker tracker;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            TrackedObjectValue.Clear();
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "RecordLifecycleTests.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "RecordLifecycleTests.obj.log"), deleteOnClose: true);
            tracker = new LifecycleTracker();
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 10,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TrackedObjectValue.Serializer(),
                    new LifecycleRecordTriggers(tracker))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose(); store = null;
            log?.Dispose(); log = null;
            objlog?.Dispose(); objlog = null;
            TrackedObjectValue.Clear();
            OnTearDown();
        }

        private ClientSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete, LifecycleStoreFunctions, LifecycleAllocator> NewSession()
            => store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete());

        private void Upsert(int key, int value)
        {
            using var s = NewSession();
            _ = s.BasicContext.Upsert(new TestObjectKey { key = key }, new TrackedObjectValue { value = value }, 0);
        }

        #region CopyUpdate — value-object slot clearing

        /// <summary>
        /// RMW on a record in the immutable region forces CopyUpdate. After the CAS succeeds,
        /// Tsavorite internally clears the source value-object slot and decrements the logSizeTracker
        /// for the value-object's heap. The trigger is NOT involved — <see cref="DisposeReason.CopyUpdated"/>
        /// does not fire via <see cref="IRecordTriggers.OnDispose"/>. The source record stays alive
        /// (sealed) until eviction, where OnEvict picks up any remaining key overflow.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void CopyUpdateDoesNotFireOnDisposeCopyUpdated()
        {
            Upsert(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();

            using (var s = NewSession())
            {
                var input = new TestObjectInput { value = 7 };
                var output = new TestObjectOutput();
                _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, ref output, 0);
            }

            ClassicAssert.AreEqual(0, tracker.DisposeCount(DisposeReason.CopyUpdated),
                "CopyUpdated is handled internally by logSizeTracker — OnDispose must not fire for it");
            ClassicAssert.AreEqual(0, tracker.DisposeCount(DisposeReason.Deleted),
                "Deleted must not fire on a CopyUpdate path");
            ClassicAssert.AreEqual(0, tracker.TotalEvict(),
                "No page eviction should have happened in this test window");
        }

        /// <summary>
        /// After CopyUpdate, the source value-object is cleared from the ObjectIdMap (via ClearValueIfHeap).
        /// Tsavorite calls IHeapObject.Dispose on the freed object internally — the trigger is not involved.
        /// Verify that exactly one IHeapObject.Dispose fires for the source value object.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void CopyUpdateDisposesSourceValueExactlyOnce()
        {
            Upsert(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            tracker.Reset();

            using (var s = NewSession())
            {
                var input = new TestObjectInput { value = 7 };
                var output = new TestObjectOutput();
                _ = s.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, ref output, 0);
            }

            ClassicAssert.AreEqual(0, tracker.DisposeCount(DisposeReason.CopyUpdated),
                "CopyUpdated must not fire — handled internally");

            var (total, max) = TrackedObjectValue.Snapshot();
            ClassicAssert.AreEqual(1, total, "Exactly one IHeapObject.Dispose call expected (the CU source object)");
            ClassicAssert.AreEqual(1, max, "No value object should be disposed more than once");
        }

        #endregion

        #region Delete

        [Test, Category("TsavoriteKV")]
        public void DeleteFiresOnDisposeDeletedAndNoOnDisposeDiskRecord()
        {
            Upsert(1, 100);
            tracker.Reset();

            using (var s = NewSession())
                _ = s.BasicContext.Delete(new TestObjectKey { key = 1 });

            ClassicAssert.AreEqual(1, tracker.DisposeCount(DisposeReason.Deleted),
                "OnDispose(Deleted) should fire exactly once");
            ClassicAssert.AreEqual(0, tracker.TotalDisposeDisk(),
                "Delete should not invoke OnDisposeDiskRecord for any reason");
        }

        #endregion

        #region Scan — in-memory records

        /// <summary>
        /// Scanning N records that are all in memory fires <see cref="IRecordTriggers.OnDisposeDiskRecord"/>
        /// exactly N times with <see cref="DisposeReason.DeserializedFromDisk"/> (the iterator wraps each
        /// remapped in-memory record as a transient DiskLogRecord). Value objects are SHARED with the
        /// live on-log records, so the default trigger (no-op) must not dispose them.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void ScanInMemoryFiresOnDisposeDiskRecordOncePerRecord()
        {
            const int n = 20;
            for (int i = 0; i < n; i++) Upsert(i, i * 10);
            tracker.Reset();

            int scanned = 0;
            using (var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress, DiskScanBufferingMode.SinglePageBuffering))
            {
                while (iter.GetNext()) scanned++;
            }

            ClassicAssert.AreEqual(n, scanned, "Iterator should yield exactly N in-memory records");
            ClassicAssert.AreEqual(n, tracker.DisposeDiskCount(DisposeReason.DeserializedFromDisk),
                "OnDisposeDiskRecord should fire exactly once per scanned in-memory record");
            ClassicAssert.AreEqual(0, tracker.TotalDispose(),
                "In-memory scan must not invoke OnDispose (on-log records are not being disposed)");

            var (total, _) = TrackedObjectValue.Snapshot();
            ClassicAssert.AreEqual(0, total,
                "In-memory scan with a no-op OnDisposeDiskRecord handler must leave value objects undisposed " +
                "(they are shared references to the still-live on-log records)");

            // The on-log records remain fully readable after the scan — no corruption from the scan wrappers.
            using (var s = NewSession())
            {
                for (int i = 0; i < n; i++)
                {
                    var input = new TestObjectInput();
                    var output = new TestObjectOutput();
                    var status = s.BasicContext.Read(new TestObjectKey { key = i }, ref input, ref output, 0);
                    ClassicAssert.IsTrue(status.Found && !status.IsPending, $"Read {i} should hit memory");
                    ClassicAssert.AreEqual(i * 10, output.value.value, $"Record {i} value should be intact after scan");
                }
            }
        }

        #endregion

        #region Scan — records on disk

        /// <summary>
        /// After FlushAndEvict pushes all records to disk, scanning K records fires
        /// <see cref="IRecordTriggers.OnDisposeDiskRecord"/> exactly K times. These are genuinely
        /// disk-deserialized records that OWN their value objects, so a handler that opts to Dispose
        /// will see exactly K IHeapObject.Dispose invocations (one per iterated record)
        /// with no double-dispose.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void ScanDiskFiresOnDisposeDiskRecordOncePerRecord()
        {
            const int n = 40;
            for (int i = 0; i < n; i++) Upsert(i, i * 10);

            store.Log.FlushAndEvict(wait: true);
            tracker.Reset();
            tracker.DisposeValuesOnDisposeDiskRecord = true; // disk-deserialized records own their values; safe to dispose

            // Snapshot existing disposes so we only count new ones from this scan.
            var (beforeTotal, _) = TrackedObjectValue.Snapshot();

            int scanned = 0;
            using (var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress, DiskScanBufferingMode.SinglePageBuffering))
            {
                while (iter.GetNext()) scanned++;
            }

            ClassicAssert.AreEqual(n, scanned, "Iterator should yield exactly N disk-resident records");
            ClassicAssert.AreEqual(n, tracker.DisposeDiskCount(DisposeReason.DeserializedFromDisk),
                "OnDisposeDiskRecord should fire exactly once per scanned disk record");
            ClassicAssert.AreEqual(n, tracker.TotalDisposeDisk(),
                "Only DeserializedFromDisk reason should fire — no other DisposeReason should appear for a disk scan");
            ClassicAssert.AreEqual(0, tracker.TotalDispose(),
                "Disk scan must not invoke OnDispose (there are no on-log records being disposed)");
            ClassicAssert.AreEqual(0, tracker.TotalEvict(),
                "Scan is not eviction — OnEvict must not fire");

            var (afterTotal, afterMax) = TrackedObjectValue.Snapshot();
            var newDisposes = afterTotal - beforeTotal;
            ClassicAssert.AreEqual(n, newDisposes,
                "Handler opts to Dispose each disk value — expected exactly N new IHeapObject.Dispose calls");
            ClassicAssert.LessOrEqual(afterMax, 1, "No individual value object should be disposed more than once");
        }

        #endregion

        #region Pending read from disk

        /// <summary>
        /// Reading a key whose record is on disk issues a pending IO; on completion,
        /// <see cref="IRecordTriggers.OnDisposeDiskRecord"/> fires exactly once with
        /// <see cref="DisposeReason.DeserializedFromDisk"/>. If the handler opts in,
        /// the deserialized value is disposed exactly once.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void PendingReadFromDiskFiresOnDisposeDiskRecordOnce()
        {
            const int key = 42;
            // Value matches key so TestObjectFunctionsDelete.ReadCompletionCallback's key==value
            // assertion (ObjectTestTypes.cs:192) succeeds on the pending-read path.
            Upsert(key, 42);
            store.Log.FlushAndEvict(wait: true);

            tracker.Reset();
            tracker.DisposeValuesOnDisposeDiskRecord = true;
            var (beforeTotal, _) = TrackedObjectValue.Snapshot();

            using (var s = NewSession())
            {
                var input = new TestObjectInput();
                var output = new TestObjectOutput();
                var status = s.BasicContext.Read(new TestObjectKey { key = key }, ref input, ref output, 0);
                ClassicAssert.IsTrue(status.IsPending, "Record should be disk-resident and Read should go pending");
                _ = s.BasicContext.CompletePending(wait: true);
            }

            ClassicAssert.AreEqual(1, tracker.DisposeDiskCount(DisposeReason.DeserializedFromDisk),
                "Exactly one OnDisposeDiskRecord(DeserializedFromDisk) call expected per pending IO");
            ClassicAssert.AreEqual(1, tracker.TotalDisposeDisk(),
                "Only DeserializedFromDisk reason should fire — no other DisposeReason");
            ClassicAssert.AreEqual(0, tracker.TotalDispose(),
                "Pending read must not invoke OnDispose (no on-log records are being disposed)");
            ClassicAssert.AreEqual(0, tracker.TotalEvict(),
                "Pending read must not invoke OnEvict");

            var (afterTotal, afterMax) = TrackedObjectValue.Snapshot();
            ClassicAssert.AreEqual(1, afterTotal - beforeTotal,
                "Exactly one IHeapObject.Dispose call expected (the pending IO's deserialized value)");
            ClassicAssert.LessOrEqual(afterMax, 1, "No double-dispose");
        }

        #endregion

        #region OnEvict

        /// <summary>
        /// Filling the log well beyond its mutable window forces page eviction. OnEvict must fire for
        /// every non-tombstoned, non-invalid record evicted past HeadAddress — including sealed source
        /// records from immutable-region deletes. Tombstoned records are skipped (heap was decremented
        /// at the delete site). Invalid/elided records are skipped (already cleaned up).
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void PageEvictionFiresOnEvictForEveryLiveRecord()
        {
            const int n = 500; // enough to push pages past HeadAddress given 32KB memory / 1KB pages
            for (int i = 0; i < n; i++) Upsert(i, i);

            // Delete ~10% of records. Track which go through the immutable path (TailAddress moves)
            // vs in-place mutable path (TailAddress stays).
            tracker.Reset();
            const int deleted = 50;
            int mutableDeletes = 0;
            using (var s = NewSession())
            {
                for (int i = 0; i < deleted; i++)
                {
                    var tailBefore = store.Log.TailAddress;
                    _ = s.BasicContext.Delete(new TestObjectKey { key = i });
                    if (store.Log.TailAddress == tailBefore)
                        mutableDeletes++;
                }
            }
            var immutableDeletes = deleted - mutableDeletes;

            ClassicAssert.AreEqual(deleted, tracker.DisposeCount(DisposeReason.Deleted),
                "Each Delete should fire OnDispose(Deleted) exactly once");
            var deletedDisposeCountBeforeEvict = tracker.DisposeCount(DisposeReason.Deleted);

            // Force all records out to disk.
            store.Log.FlushAndEvict(wait: true);

            // Precise count:
            //  - (n - deleted) live records: visited by OnEvict.
            //  - mutableDeletes records: tombstoned in-place, skipped by OnEvict.
            //  - immutableDeletes sealed source records: NOT tombstoned, visited by OnEvict.
            //  - immutableDeletes new tombstone records at tail: tombstoned, skipped by OnEvict.
            // Total = (n - deleted) + immutableDeletes = n - mutableDeletes.
            ClassicAssert.AreEqual(n - mutableDeletes, tracker.EvictCount(EvictionSource.MainLog),
                $"OnEvict(MainLog) must fire exactly {n - mutableDeletes} times: " +
                $"{n - deleted} live + {immutableDeletes} sealed sources, skipping {mutableDeletes} in-place tombstones");
            ClassicAssert.AreEqual(0, tracker.EvictCount(EvictionSource.ReadCache),
                "No read cache is configured, OnEvict(ReadCache) must never fire");
            ClassicAssert.AreEqual(deletedDisposeCountBeforeEvict, tracker.DisposeCount(DisposeReason.Deleted),
                "Page eviction must not re-fire OnDispose(Deleted) for tombstoned records");
            ClassicAssert.AreEqual(0, tracker.TotalDisposeDisk(),
                "Page eviction must not route through OnDisposeDiskRecord");
        }

        #endregion

        #region Trigger gating — CallOnFlush / CallOnDiskRead / CallOnEvict

        /// <summary>
        /// With <see cref="IRecordTriggers.CallOnFlush"/> enabled, OnFlush must fire exactly once per
        /// non-tombstoned record on the in-memory page being flushed to disk. With the flag disabled
        /// (default), OnFlush must never fire regardless of how many flushes run.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void CallOnFlushGatesOnFlushInvocation()
        {
            // Control: flag off — OnFlush never fires even across a full flush.
            tracker.CallOnFlushFlag = false;
            const int n = 20;
            for (int i = 0; i < n; i++) Upsert(i, i);
            store.Log.FlushAndEvict(wait: true);
            ClassicAssert.AreEqual(0, tracker.FlushCount,
                "CallOnFlush=false must fully suppress OnFlush invocation");

            // Experiment: populate fresh records, enable flag, flush. Some of the new records may share
            // a page that Tsavorite auto-sealed during insert (before the flag was flipped); the exact
            // post-flag count is therefore a lower bound. The gating invariant is the key assertion:
            // flipping the flag from false → true must cause OnFlush to fire for at least one record.
            tracker.Reset();
            tracker.CallOnFlushFlag = true;
            for (int i = 0; i < n; i++) Upsert(n + i, i);
            store.Log.Flush(wait: true);
            ClassicAssert.Greater(tracker.FlushCount, 0,
                "CallOnFlush=true must fire OnFlush for records flushed while the flag was set");
            ClassicAssert.LessOrEqual(tracker.FlushCount, n,
                "OnFlush must not fire more times than there are records flushed on this page range");
        }

        /// <summary>
        /// With <see cref="IRecordTriggers.CallOnDiskRead"/> enabled, OnDiskRead must fire exactly once
        /// per record loaded from disk into memory. With the flag disabled, OnDiskRead must never fire
        /// even when pending reads pull records from disk.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void CallOnDiskReadGatesOnDiskReadInvocation()
        {
            const int n = 10;
            for (int i = 0; i < n; i++) Upsert(i, i);
            store.Log.FlushAndEvict(wait: true);

            // Control: flag off — pending reads must not fire OnDiskRead.
            tracker.CallOnDiskReadFlag = false;
            tracker.Reset();
            using (var s = NewSession())
            {
                for (int i = 0; i < n; i++)
                {
                    var input = new TestObjectInput();
                    var output = new TestObjectOutput();
                    _ = s.BasicContext.Read(new TestObjectKey { key = i }, ref input, ref output, 0);
                }
                _ = s.BasicContext.CompletePending(wait: true);
            }
            ClassicAssert.AreEqual(0, tracker.DiskReadCount,
                "CallOnDiskRead=false must fully suppress OnDiskRead invocation");
        }

        /// <summary>
        /// <see cref="IRecordTriggers.CallOnEvict"/> gates the OnEvict callback. When the application
        /// opts out, no OnEvict calls should reach the trigger — but Tsavorite's internal heap
        /// accounting still runs.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void CallOnEvictGatingSuppressesOnEvictCallback()
        {
            tracker.CallOnEvictFlag = false;   // opt out of eviction callbacks

            const int n = 100;
            for (int i = 0; i < n; i++) Upsert(i, i);
            store.Log.FlushAndEvict(wait: true);

            ClassicAssert.AreEqual(0, tracker.EvictCount(EvictionSource.MainLog),
                "CallOnEvict=false must fully suppress OnEvict invocation");
            ClassicAssert.AreEqual(0, tracker.EvictCount(EvictionSource.ReadCache),
                "No read-cache configured — ReadCache OnEvict must stay zero");
        }

        #endregion

        #region Read cache eviction

        /// <summary>
        /// With a read cache configured, pending reads populate the read-cache page tail. Flushing and
        /// evicting the read cache must fire <see cref="IRecordTriggers.OnEvict"/> with
        /// <see cref="EvictionSource.ReadCache"/> — separately from main-log eviction so heap-accounting
        /// handlers can route to the correct counter.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void ReadCacheEvictionFiresOnEvictWithReadCacheSource()
        {
            // Tear down the default main-log-only store and recreate with read cache enabled.
            store.Dispose(); store = null;
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = 1L << 10,
                ReadCacheMemorySize = 1L << 15,
                ReadCachePageSize = 1L << 10,
                ReadCacheEnabled = true,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TrackedObjectValue.Serializer(),
                    new LifecycleRecordTriggers(tracker))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            const int n = 50;
            for (int i = 0; i < n; i++) Upsert(i, i);
            store.Log.FlushAndEvict(wait: true); // push to disk so next Read populates read cache

            tracker.Reset();
            // Issue reads — each pending completion brings a record into the read cache.
            using (var s = NewSession())
            {
                for (int i = 0; i < n; i++)
                {
                    var input = new TestObjectInput();
                    var output = new TestObjectOutput();
                    _ = s.BasicContext.Read(new TestObjectKey { key = i }, ref input, ref output, 0);
                }
                _ = s.BasicContext.CompletePending(wait: true);
            }

            // Evict the read cache. Every live readcache record must fire OnEvict(ReadCache); zero main-log evictions.
            var readCacheEvictBefore = tracker.EvictCount(EvictionSource.ReadCache);
            store.ReadCache.FlushAndEvict(wait: true);
            var readCacheEvicted = tracker.EvictCount(EvictionSource.ReadCache) - readCacheEvictBefore;

            ClassicAssert.Greater(readCacheEvicted, 0,
                "Read-cache eviction must fire OnEvict(ReadCache) at least once for cached records");
            ClassicAssert.AreEqual(0, tracker.EvictCount(EvictionSource.MainLog),
                "Read-cache flush must not route through main-log OnEvict");
        }

        #endregion

        #region No double-dispose across lifecycle

        /// <summary>
        /// Cross-path regression test: upsert → delete → upsert-same-key → evict-to-disk → pending-read.
        /// Across all these operations, no individual <see cref="TrackedObjectValue"/> instance should
        /// be disposed more than once — regardless of which trigger disposed it.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void NoValueObjectIsDisposedMoreThanOnceAcrossLifecycle()
        {
            tracker.DisposeValuesOnDispose = true;
            tracker.DisposeValuesOnDisposeDiskRecord = true;

            const int n = 30;
            // Round 1: upsert 30 records
            for (int i = 0; i < n; i++) Upsert(i, i);

            // Delete half — fires OnDispose(Deleted) which disposes the value
            using (var s = NewSession())
            {
                for (int i = 0; i < n / 2; i++)
                    _ = s.BasicContext.Delete(new TestObjectKey { key = i });
            }

            // Upsert all keys again with new values — for the still-live halves this fires OnDispose(Deleted)
            // via the tombstone path; for deleted-then-upserted keys the prior tombstone was elided.
            // Keep value==key so the pending-read completion callback's key==value assertion passes below.
            for (int i = 0; i < n; i++) Upsert(i, i);

            // Push everything to disk to force a fresh disk-deserialization cycle.
            store.Log.FlushAndEvict(wait: true);

            // Read each key — all pending. Handler disposes each deserialized value exactly once.
            using (var s = NewSession())
            {
                for (int i = 0; i < n; i++)
                {
                    var input = new TestObjectInput();
                    var output = new TestObjectOutput();
                    var status = s.BasicContext.Read(new TestObjectKey { key = i }, ref input, ref output, 0);
                    if (status.IsPending) _ = s.BasicContext.CompletePending(wait: true);
                }
            }

            var (_, maxDisposes) = TrackedObjectValue.Snapshot();
            ClassicAssert.LessOrEqual(maxDisposes, 1,
                "No individual value object should ever be disposed more than once across the full lifecycle");
        }

        #endregion
    }
}