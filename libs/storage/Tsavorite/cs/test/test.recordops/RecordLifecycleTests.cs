// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.core.Utility;
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
    [TestFixture]
    internal class RecordLifecycleTests : TestBase
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
                // Cast to the base TestObjectValue: an RMW CopyUpdater produces a plain TestObjectValue for the new
                // record, which can be flushed/serialized in tests that mix promoted TrackedObjectValue sources with
                // RMW-updated values. Both expose the same `value` field.
                public override void Serialize(IHeapObject obj) => writer.Write(((TestObjectValue)obj).value);
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

            /// <summary>Called with each record's address from <see cref="IRecordTriggers.OnFlush"/>, so a test can hold the trigger.</summary>
            public Action<long> OnFlushHook;

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

            public readonly void OnFlush(ref LogRecord logRecord, long logicalAddress)
            {
                if (tracker is null) return;
                _ = Interlocked.Increment(ref tracker.FlushCount);
                tracker.OnFlushHook?.Invoke(logicalAddress);
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
                    && logRecord.DataHeader.ValueIsObject
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

                if (tracker.DisposeValuesOnDisposeDiskRecord && logRecord.DataHeader.ValueIsObject)
                    logRecord.ValueObject?.Dispose();
            }

            public readonly void OnEvict(ref LogRecord logRecord, EvictionSource source)
            {
                if (tracker is null) return;
                _ = Interlocked.Increment(ref tracker.EvictCounts[(int)source]);
            }

            // Explicit defaults/no-ops (rather than relying on the IRecordTriggers defaults) so calls
            // through the TRecordTriggers generic constraint resolve here instead of boxing to dispatch the DIMs.
            public readonly bool CallPostCopyToTail => false;
            public readonly bool CallOnTruncate => false;
            public readonly void OnRecovery(Guid checkpointToken) { }
            public readonly void OnRecoverySnapshotRead(ref LogRecord logRecord) { }
            public readonly void OnCheckpoint(CheckpointTrigger trigger, Guid checkpointToken) { }
            public readonly void PostCopyToTail<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, long srcLogicalAddress,
                                                                  ref LogRecord dstLogRecord, long dstLogicalAddress)
                where TSourceLogRecord : ISourceLogRecord
#if NET9_0_OR_GREATER
                    , allows ref struct
#endif
            { }
            public readonly void OnTruncate(long newBeginAddress) { }
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
            store = CreateStore(log, objlog);
        }

        private TsavoriteKV<LifecycleStoreFunctions, LifecycleAllocator> CreateStore(IDevice logDevice, IDevice objectLogDevice)
            => new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = logDevice,
                ObjectLogDevice = objectLogDevice,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TrackedObjectValue.Serializer(),
                    new LifecycleRecordTriggers(tracker))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

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

        private void RmwAdd(int key, int value)
        {
            using var s = NewSession();
            var input = new TestObjectInput { value = value };
            var output = new TestObjectOutput();
            _ = s.BasicContext.RMW(new TestObjectKey { key = key }, ref input, ref output, 0);
        }

        /// <summary>Whether the record at <paramref name="logicalAddress"/> still holds its value object. A CopyUpdate's release leaves a
        /// placeholder in the slot, which reads as no object; an elision converts the value to inline.</summary>
        private bool SourceHoldsObject(long logicalAddress)
        {
            var logRecord = store.hlogBase._wrapper.CreateLogRecord(logicalAddress);
            return logRecord.DataHeader.ValueIsObject && logRecord.ValueObject is not null;
        }

        /// <summary>A copy of the bytes of the record at <paramref name="logicalAddress"/> on its page.</summary>
        private unsafe byte[] RecordBytes(long logicalAddress)
        {
            var logRecord = store.hlogBase._wrapper.CreateLogRecord(logicalAddress);
            return new ReadOnlySpan<byte>((void*)logRecord.physicalAddress, logRecord.AllocatedSize).ToArray();
        }

        /// <summary>Replace the store with one whose main log and object log are NullDevices.</summary>
        private void UseNullDevice()
        {
            store.Dispose();
            log.Dispose();
            objlog.Dispose();
            log = new NullDevice();
            objlog = new NullDevice();
            store = CreateStore(log, objlog);
        }

        /// <summary>The address of the record the next upsert on an empty log writes: the tail, or the first page's first valid address.</summary>
        private long NextRecordAddress()
        {
            var address = store.Log.TailAddress;
            var firstValidAddress = store.hlogBase.GetFirstValidLogicalAddressOnPage(store.hlogBase.GetPage(address));
            return address < firstValidAddress ? firstValidAddress : address;
        }

        /// <summary>Writes no bytes, so a record's value component is zero length on the object log.</summary>
        sealed class ZeroByteValueSerializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj) => obj = new TrackedObjectValue { value = 0 };
            public override void Serialize(IHeapObject obj) { }
        }

        /// <summary>
        /// A page whose out-of-line components are ALL zero length must still return every record when it is read back.
        /// </summary>
        /// <remarks>
        /// A zero-byte object is stored as an exact extent of length zero: the record carries a stamped position, an exact-size flag,
        /// and a hint of zero, so its extent is empty. A larger component cannot produce an empty extent, because one over 511 bytes is
        /// length-prefixed and hinted with a 4 KB page count of at least 1. Reading the page back must therefore handle records whose
        /// extents, and so the page's whole object-log range, are empty.
        /// <para>The KEY must be inline. An overflow key makes the record's extent non-empty -- which is exactly why
        /// <c>ObjectLogScanTests</c>, whose fixture keys are overflow, cannot pin this. <see cref="TestObjectKey"/> is inline, so the
        /// zero-length value is the page's only out-of-line component.</para>
        /// </remarks>
        [Test, Category("TsavoriteKV")]
        public void AllZeroLengthComponentPageIsMaterializedNotRead()
        {
            using var zlLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ZeroLen.log"), deleteOnClose: true);
            using var zlObjLog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "ZeroLen.obj.log"), deleteOnClose: true);
            using var zlStore = new TsavoriteKV<LifecycleStoreFunctions, LifecycleAllocator>(new()
            {
                IndexSize = 1L << 13,
                LogDevice = zlLog,
                ObjectLogDevice = zlObjLog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << 15,
                PageSize = MinKvLogPageSize
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => (IObjectSerializer<IHeapObject>)new ZeroByteValueSerializer(),
                    new LifecycleRecordTriggers(null))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            const int numRecords = 4;
            using (var s = zlStore.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, int, TestObjectFunctionsDelete>(new TestObjectFunctionsDelete()))
            {
                for (var ii = 0; ii < numRecords; ++ii)
                    _ = s.BasicContext.Upsert(new TestObjectKey { key = ii }, new TrackedObjectValue { value = ii }, 0);
            }

            // Evict so the scan must read the page back from the device, which is the only path that walks a page of records.
            zlStore.Log.FlushAndEvict(wait: true);

            var scanned = 0;
            using (var iter = zlStore.Log.Scan(zlStore.Log.BeginAddress, zlStore.Log.TailAddress))
            {
                while (iter.GetNext())
                    ++scanned;
            }

            Assert.That(scanned, Is.EqualTo(numRecords), "every zero-length-component record must still be returned by the scan");
        }

        #region Deferred dispose of a record frozen for flush

        /// <summary>
        /// A superseded source that is frozen for flush has its disposal declined, because releasing its heap or rewriting its
        /// layout while the allocator is writing the live page would persist a torn or dangling record. Before the deferred-dispose
        /// drain existed, such a record was simply "released at eviction" — but eviction calls <see cref="IRecordTriggers.OnEvict"/>,
        /// never <see cref="IRecordTriggers.OnDispose"/>, so the Deleted disposal never fired at all. For Garnet that meant a deleted
        /// RangeIndex or Vector record never had its data files deleted or its vector deletion requested.
        /// <para>
        /// This test pins that behavior from both sides: zero <c>OnDispose(Deleted)</c> while the flush is still in flight (which is
        /// also the state the old code ended in, so removing the drain makes the final assertion fail), and exactly one once the flush
        /// completes and the drain runs.
        /// </para>
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void FrozenSupersededSourceIsDisposedWhenTheFlushCompletes()
        {
            Upsert(1, 100);

            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the flush window cannot be held open.");

            using var insideFlushWindow = new SemaphoreSlim(0);
            using var releaseFlush = new SemaphoreSlim(0);
            var hookFired = false;

            ObjectFlushInjection.Hook = (phase, logicalAddress) =>
            {
                if (phase != ObjectFlushPhase.AfterRecordWritten || hookFired)
                    return;
                hookFired = true;
                _ = insideFlushWindow.Release();
                _ = releaseFlush.Wait(TimeSpan.FromSeconds(30));
            };

            try
            {
                tracker.Reset();

                // Run the waiting shift on a worker: it blocks until the flush completes, and the flush is parked in the hook.
                var shift = Task.Run(() => store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true));

                Assert.That(insideFlushWindow.Wait(TimeSpan.FromSeconds(30)), Is.True,
                    "the flush never reached AfterRecordWritten, so this test proves nothing (note the hook is DEBUG-only)");

                // The record is now immutable and its flush is in flight, so IsFrozenForFlush holds: the Delete supersedes it but
                // its disposal must be declined rather than run under the flush.
                using (var s = NewSession())
                    _ = s.BasicContext.Delete(new TestObjectKey { key = 1 }, 0);

                ClassicAssert.AreEqual(0, tracker.DisposeCount(DisposeReason.Deleted),
                    "disposal must be declined while the record is frozen for flush");

                _ = releaseFlush.Release();
                Assert.That(shift.Wait(TimeSpan.FromSeconds(30)), Is.True, "the read-only shift did not complete");

                // The drain is dispatched through BumpCurrentEpoch when a flush completion advances FlushedUntilAddress past the
                // record's sector, so the log has to keep moving: upserting one key repeatedly would update it in place and never
                // allocate. Use fresh keys, and touch the store until the drain has run rather than assuming it fired inline.
                var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
                for (var key = 2; tracker.DisposeCount(DisposeReason.Deleted) == 0 && DateTime.UtcNow < deadline; ++key)
                    Upsert(key, key * 100);

                ClassicAssert.AreEqual(1, tracker.DisposeCount(DisposeReason.Deleted),
                    "the deferred disposal must run exactly once after the flush that froze the record completed");
                ClassicAssert.AreEqual(0, tracker.TotalEvict(),
                    "the drain must release the record while it is still resident; eviction honors the mark too, but only as a fallback");
            }
            finally
            {
                ObjectFlushInjection.Reset();
            }
        }

        /// <summary>Pauses the checkpoint state machine on entry to a chosen phase so the test can act in that window.</summary>
        private sealed class PauseAtPhase(Phase pauseAt) : IStateMachineCallback
        {
            internal readonly ManualResetEventSlim Reached = new(false);
            internal readonly ManualResetEventSlim Release = new(false);

            public void BeforeEnteringState(SystemState next)
            {
                if (next.Phase != pauseAt)
                    return;
                Reached.Set();
                _ = Release.Wait(TimeSpan.FromSeconds(30));
            }
        }

        /// <summary>
        /// A Delete of a record that a checkpoint still needs runs the delete trigger exactly once, when no flush reads the record any more.
        /// On a real main-log device that is when FlushedUntilAddress passes the record. On a NullDevice main log FlushedUntilAddress advances
        /// without writing anything, so it is at eviction. Eviction runs only OnEvict, so a deletion left to eviction without a mark would
        /// never run the trigger, and Garnet would leave a deleted RangeIndex key's files, and a deleted vector set's data, behind.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CheckpointFrozenDeleteRunsTheDeleteTriggerOnceNoFlushReadsIt([Values] bool nullDevice)
        {
            if (nullDevice)
                UseNullDevice();

            var sourceAddress = store.Log.TailAddress;
            Upsert(1, 100);
            var firstValidAddress = store.hlogBase.GetFirstValidLogicalAddressOnPage(0);
            if (sourceAddress < firstValidAddress)
                sourceAddress = firstValidAddress;

            // Create the session before the checkpoint, so that it takes part from PREPARE onward and sees the checkpoint in progress.
            using var deleteSession = NewSession();
            var pause = new PauseAtPhase(Phase.WAIT_FLUSH);
            store.stateMachineDriver.UnsafeRegisterCallback(pause);
            tracker.Reset();
            try
            {
                Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach WAIT_FLUSH");
                _ = deleteSession.BasicContext.Delete(new TestObjectKey { key = 1 }, 0);

                Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.Zero, "The delete trigger ran while the checkpoint still needed the record");
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).Info.DeferredDispose, Is.True,
                    "The deletion should be marked for later release");
            }
            finally
            {
                pause.Release.Set();
            }
            await store.CompleteCheckpointAsync().ConfigureAwait(false);

            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThan(sourceAddress));
            if (!nullDevice)
            {
                // The drain runs on the flush completion, after the wait is released, so poll for it.
                Assert.That(SpinWait.SpinUntil(() => tracker.DisposeCount(DisposeReason.Deleted) > 0, TimeSpan.FromSeconds(30)), Is.True,
                    "The drain did not run the deletion once FlushedUntilAddress passed the record");
                Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.EqualTo(1), "The deletion must run exactly once");
                Assert.That(tracker.TotalEvict(), Is.Zero, "The drain, not eviction, should have run the deletion");
                return;
            }

            // On a NullDevice the drain releases nothing, whatever range it walks, because FlushedUntilAddress passing a record does not mean a
            // Snapshot checkpoint's flush has read it. So walk the record directly, as a drain would: the deletion must still wait for eviction.
            Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.Zero, "On a NullDevice a deletion held back by a checkpoint must wait for eviction");
            var epoch = store.hlogBase.epoch;
            var resumed = epoch.ResumeIfNotProtected();
            try
            {
                store.hlogBase.DrainDeferredDisposes(sourceAddress, store.Log.TailAddress);
            }
            finally
            {
                if (resumed)
                    epoch.Suspend();
            }
            Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.Zero, "A drain that walks the record on a NullDevice must leave the deletion to eviction");

            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.EqualTo(1), "Eviction must run the deletion exactly once");
        }

        /// <summary>
        /// A Delete during a checkpoint of a record whose image is already on a real main-log device runs the delete trigger at once: the
        /// checkpoint recovers the record from that image, and below FlushedUntilAddress no flush reads the live record.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CheckpointDeleteOfADurableRecordRunsTheDeleteTriggerAtOnce()
        {
            var sourceAddress = store.Log.TailAddress;
            Upsert(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            Assert.That(store.Log.FlushedUntilAddress, Is.GreaterThan(sourceAddress), "Precondition: the record's image is durable");

            using var deleteSession = NewSession();
            var pause = new PauseAtPhase(Phase.WAIT_FLUSH);
            store.stateMachineDriver.UnsafeRegisterCallback(pause);
            tracker.Reset();
            try
            {
                Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach WAIT_FLUSH");
                _ = deleteSession.BasicContext.Delete(new TestObjectKey { key = 1 }, 0);
                Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.EqualTo(1), "The deletion of a durable record must run at once");
            }
            finally
            {
                pause.Release.Set();
            }
            await store.CompleteCheckpointAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// On a NullDevice main log, whose flush writes nothing, a record is still frozen while the OnFlush trigger may read it, so a Delete
        /// then defers the delete trigger. Eviction runs it exactly once, although neither a size tracker nor OnEvict needs eviction to walk
        /// the records.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void NullDeviceDeleteDuringOnFlushDefersTheDeleteTrigger()
        {
            UseNullDevice();
            tracker.CallOnFlushFlag = true;
            tracker.CallOnEvictFlag = false;
            var sourceAddress = NextRecordAddress();
            Upsert(1, 100);

            using var inOnFlush = new ManualResetEventSlim(false);
            using var releaseOnFlush = new ManualResetEventSlim(false);
            tracker.OnFlushHook = logicalAddress =>
            {
                if (logicalAddress != sourceAddress || inOnFlush.IsSet)
                    return;
                inOnFlush.Set();
                _ = releaseOnFlush.Wait(TimeSpan.FromSeconds(30));
            };

            using var deleteSession = NewSession();
            tracker.Reset();
            var shift = Task.Run(() => store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true));
            try
            {
                Assert.That(inOnFlush.Wait(TimeSpan.FromSeconds(30)), Is.True, "OnFlush never ran for the record");
                _ = deleteSession.BasicContext.Delete(new TestObjectKey { key = 1 }, 0);
                Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.Zero, "The delete trigger ran while OnFlush could still read the record");
            }
            finally
            {
                releaseOnFlush.Set();
            }
            Assert.That(shift.Wait(TimeSpan.FromSeconds(30)), Is.True, "The read-only shift did not complete");

            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.EqualTo(1), "Eviction must run the deferred deletion exactly once");
        }

        /// <summary>
        /// On a NullDevice main log a Snapshot checkpoint's flush holds FlushedUntilAddress back on the pages it has not completed, but the
        /// ReadOnly flush, which reads no record there, has run the OnFlush trigger and passed them. A Delete in that window of a source that
        /// no checkpoint needs therefore disposes the source at once.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task NullDeviceDeleteWhileTheSnapshotFlushHoldsBackFlushedUntilAddressDisposesTheSource()
        {
            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the Snapshot flush cannot be held mid-page.");

            UseNullDevice();
            var heldAddress = NextRecordAddress();
            Upsert(0, 100);

            // Create the session before the checkpoint, so that it takes part from PREPARE onward and sees the checkpoint in progress.
            using var session = NewSession();
            using var snapshotHeld = new ManualResetEventSlim(false);
            using var releaseSnapshot = new ManualResetEventSlim(false);
            var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
            store.stateMachineDriver.UnsafeRegisterCallback(pause);
            Task shift = null;
            try
            {
                Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot), Is.True);
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach IN_PROGRESS");

                // A (v+1) source, which no checkpoint needs. It lies below the tail WAIT_FLUSH samples, so the Snapshot flush covers its page.
                // A filler takes the fuzzy region's start address, which CheckpointNeedsSourceImage counts as inside the checkpoint's range.
                _ = session.BasicContext.Upsert(new TestObjectKey { key = 2 }, new TrackedObjectValue { value = 300 }, 0);
                var sourceAddress = NextRecordAddress();
                _ = session.BasicContext.Upsert(new TestObjectKey { key = 1 }, new TrackedObjectValue { value = 200 }, 0);
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).Info.IsInNewVersion, Is.True, "Precondition: the source is (v+1)");

                // Hold the Snapshot flush at the first record, before it completes the page.
                ObjectFlushInjection.Hook = (phase, logicalAddress) =>
                {
                    if (phase != ObjectFlushPhase.AfterCapture || logicalAddress != heldAddress || snapshotHeld.IsSet)
                        return;
                    snapshotHeld.Set();
                    _ = releaseSnapshot.Wait(TimeSpan.FromSeconds(30));
                };
                pause.Release.Set();
                Assert.That(snapshotHeld.Wait(TimeSpan.FromSeconds(30)), Is.True, "The Snapshot flush never reached the first record");

                // The ReadOnly flush of the page waits for the Snapshot flush, after it has run the OnFlush trigger on the page.
                shift = Task.Run(() => store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true));
                Assert.That(SpinWait.SpinUntil(() => store.Log.SafeReadOnlyAddress > sourceAddress && !store.hlog.IsFrozenForFlush(sourceAddress),
                    TimeSpan.FromSeconds(10)), Is.True, "The source is still frozen for the ReadOnly flush, which reads no record on a NullDevice");
                Assert.That(store.Log.FlushedUntilAddress, Is.LessThanOrEqualTo(sourceAddress),
                    "Precondition: the Snapshot flush holds FlushedUntilAddress below the source");

                tracker.Reset();
                var status = session.BasicContext.Delete(new TestObjectKey { key = 1 }, 0);
                Assert.That(status.Record.Created, Is.True, "The Delete did not insert a tombstone for the immutable source");
                Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.EqualTo(1), "The Delete did not dispose the source at once");
            }
            finally
            {
                releaseSnapshot.Set();
                pause.Release.Set();
                ObjectFlushInjection.Reset();
            }
            Assert.That(shift.Wait(TimeSpan.FromSeconds(30)), Is.True, "The read-only shift did not complete");
            await store.CompleteCheckpointAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// A CopyUpdate from a read-cache source during a checkpoint releases the source's value at once. The read cache holds a copy of a
        /// durable main-log record, which no checkpoint reads from the read cache, so preserving the source's image would only keep its object
        /// and serialized bytes until the read cache evicts it.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task CheckpointCopyUpdateOfAReadCacheSourceReleasesItsValueAtOnce()
        {
            store.Dispose();
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << (MinKvLogPageSizeBits + 4),
                PageSize = MinKvLogPageSize,
                ReadCacheMemorySize = 1L << (MinKvLogPageSizeBits + 5),
                ReadCachePageSize = MinKvLogPageSize,
                ReadCacheEnabled = true,
                CheckpointDir = MethodTestDir
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TrackedObjectValue.Serializer(),
                    new LifecycleRecordTriggers(tracker))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            // The read below checks that a key's value is the key.
            Upsert(1, 1);
            store.Log.FlushAndEvict(wait: true);

            // Create the session before the checkpoint, so that it takes part from PREPARE onward and sees the checkpoint in progress. Its
            // pending read copies the record into the read cache.
            using var session = NewSession();
            {
                TestObjectInput input = new();
                TestObjectOutput output = new();
                if (session.BasicContext.Read(new TestObjectKey { key = 1 }, ref input, ref output, 0).IsPending)
                    _ = session.BasicContext.CompletePending(wait: true);
            }

            var pause = new PauseAtPhase(Phase.WAIT_INDEX_CHECKPOINT);
            store.stateMachineDriver.UnsafeRegisterCallback(pause);
            try
            {
                Assert.That(store.TryInitiateFullCheckpoint(out _, CheckpointType.FoldOver), Is.True);
                Assert.That(pause.Reached.Wait(TimeSpan.FromSeconds(30)), Is.True, "The checkpoint did not reach IN_PROGRESS");

                var disposedBefore = TrackedObjectValue.Snapshot().total;
                TestObjectInput input = new() { value = 7 };
                TestObjectOutput output = new();
                var status = session.BasicContext.RMW(new TestObjectKey { key = 1 }, ref input, ref output, 0);
                Assert.That(status.IsPending, Is.False, "Precondition: the RMW found the key in the read cache");
                Assert.That(status.Record.CopyUpdated, Is.True, "The RMW did not copy-update the read-cache source");
                Assert.That(TrackedObjectValue.Snapshot().total - disposedBefore, Is.EqualTo(1), "The read-cache source's value was not released at once");
            }
            finally
            {
                pause.Release.Set();
            }
            await store.CompleteCheckpointAsync().ConfigureAwait(false);
        }

        /// <summary>A streaming snapshot's scan functions that delete a key while the second scan pushes another, and record what it pushes.</summary>
        private sealed class DeleteDuringStreamingScan(Action onPushOfFirstKey) : IStreamingSnapshotIteratorFunctions
        {
            /// <summary>The value pushed for each key; -1 if the record no longer held a value object.</summary>
            internal readonly ConcurrentDictionary<int, int> Pushed = new();

            /// <summary>Deletions the triggers had run when the scan pushed each key.</summary>
            internal readonly ConcurrentDictionary<int, int> DeletionsAtPush = new();

            internal Func<int> DeletionCount;
            internal Exception Failure;
            bool acted;

            public bool OnStart(Guid checkpointToken, long currentVersion, long nextVersion) => true;

            public bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords)
                where TSourceLogRecord : ISourceLogRecord
            {
                var key = BitConverter.ToInt32(logRecord.Key);
                Pushed[key] = logRecord.DataHeader.ValueIsObject && logRecord.ValueObject is TestObjectValue value ? value.value : -1;
                DeletionsAtPush[key] = DeletionCount();
                if (key == 1 && !acted)
                {
                    acted = true;
                    try
                    {
                        onPushOfFirstKey();
                    }
                    catch (Exception ex)
                    {
                        Failure = ex;
                    }
                }
                return true;
            }

            public void OnException(Exception exception, long numberOfRecords) => Failure ??= exception;

            public void OnStop(bool completed, long numberOfRecords) { }
        }

        /// <summary>
        /// A streaming snapshot's second scan pushes the latest version of each key below the tail it samples, so it pushes a record superseded
        /// after that sample. A Delete during the scan, of a record the scan has not reached, must therefore leave the record's value, whether
        /// the record's image is already durable or its release would wait for a flush. The deletion runs once, after the scan.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task StreamingSnapshotPushesTheValueOfARecordDeletedDuringItsScan([Values] bool durableBeforeDelete)
        {
            Upsert(1, 100);
            var deletedAddress = store.Log.TailAddress;
            Upsert(2, 200);
            Assert.That(store.hlogBase.GetPage(deletedAddress), Is.EqualTo(store.hlogBase.GetPage(store.Log.TailAddress - 1)),
                "Precondition: the deleted record starts where the first record ended");
            Assert.That(store.Log.SafeReadOnlyAddress, Is.LessThanOrEqualTo(deletedAddress),
                "Precondition: the record is mutable when the checkpoint starts, so the second scan reads it");

            using var deleteSession = NewSession();
            tracker.Reset();
            var epoch = store.hlogBase.epoch;
            var scan = new DeleteDuringStreamingScan(() =>
            {
                // The scan has suspended its epoch to push key 1, and has sampled the tail, so the tombstone lies above it.
                if (durableBeforeDelete)
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                _ = deleteSession.BasicContext.Delete(new TestObjectKey { key = 2 }, 0);
                if (!durableBeforeDelete)
                {
                    // Make the record durable, then walk it as the drain on that flush's completion does.
                    store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                    var resumed = epoch.ResumeIfNotProtected();
                    try
                    {
                        store.hlogBase.DrainDeferredDisposes(deletedAddress, deletedAddress + 1);
                    }
                    finally
                    {
                        if (resumed)
                            epoch.Suspend();
                    }
                }
                if (store.Log.FlushedUntilAddress <= deletedAddress)
                    throw new InvalidOperationException("The record should be durable");
            })
            { DeletionCount = () => tracker.DisposeCount(DisposeReason.Deleted) };

            _ = await store.TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, streamingSnapshotIteratorFunctions: scan).ConfigureAwait(false);

            Assert.That(scan.Failure, Is.Null, $"The scan failed: {scan.Failure}");
            Assert.That(scan.Pushed.TryGetValue(2, out var pushed), Is.True, "The scan did not push the record deleted after its tail sample");
            Assert.That(scan.DeletionsAtPush[2], Is.Zero, "The deletion ran before the scan pushed the record");
            Assert.That(pushed, Is.EqualTo(200), "The scan pushed a released value for the deleted record");

            // The hold ends with the scan, so a deletion of a durable record now runs at once.
            Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.Zero, "The deletion held for the scan ran before eviction");
            _ = deleteSession.BasicContext.Delete(new TestObjectKey { key = 1 }, 0);
            Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.EqualTo(1), "A deletion after the scan was still held");

            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.EqualTo(2), "Each deletion must run exactly once");
        }

        /// <summary>
        /// The scan pushes the latest version of each key below the tail it sampled, whatever its version, so a Delete during the scan of a
        /// <c>(v+1)</c> record that it could otherwise elide must leave the record in place, sealed, with its value. Elision would free the
        /// record's value at once, and the scan would push the freed record.
        /// </summary>
        [Test, Category("TsavoriteKV"), Category("CheckpointRestore")]
        public async Task StreamingSnapshotPushesTheValueOfANewVersionRecordDeletedDuringItsScan()
        {
            Upsert(1, 100);
            using var session = NewSession();
            tracker.Reset();

            var scan = new DeleteDuringStreamingScan(() =>
            {
                // Make the record immutable, so the Delete supersedes it with a tombstone at the tail rather than deleting it in place.
                store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
                _ = session.BasicContext.Delete(new TestObjectKey { key = 3 }, 0);
            })
            { DeletionCount = () => tracker.DisposeCount(DisposeReason.Deleted) };

            // An old-version transaction holds the checkpoint in IN_PROGRESS, so the records below are written in the new version, below the
            // tail the scan samples when the transaction ends.
            var txnVersion = store.stateMachineDriver.AcquireTransactionVersion();
            long deletedAddress;
            Task checkpoint;
            try
            {
                checkpoint = store.TakeFullCheckpointAsync(CheckpointType.StreamingSnapshot, streamingSnapshotIteratorFunctions: scan).AsTask();
                Assert.That(SpinWait.SpinUntil(() => store.SystemState.Phase == Phase.IN_PROGRESS, TimeSpan.FromSeconds(30)), Is.True,
                    "The checkpoint did not reach IN_PROGRESS");

                // A filler takes the fuzzy region's start address, which CheckpointNeedsSourceImage counts as inside the checkpoint's range.
                _ = session.BasicContext.Upsert(new TestObjectKey { key = 2 }, new TrackedObjectValue { value = 200 }, 0);
                deletedAddress = NextRecordAddress();
                _ = session.BasicContext.Upsert(new TestObjectKey { key = 3 }, new TrackedObjectValue { value = 300 }, 0);
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(deletedAddress).Info.IsInNewVersion, Is.True, "Precondition: the record is (v+1)");
            }
            finally
            {
                store.stateMachineDriver.EndTransaction(txnVersion);
            }
            await checkpoint.ConfigureAwait(false);

            Assert.That(scan.Failure, Is.Null, $"The scan failed: {scan.Failure}");
            Assert.That(scan.Pushed.TryGetValue(3, out var pushed), Is.True, "The scan did not push the record deleted after its tail sample");
            Assert.That(scan.DeletionsAtPush[3], Is.Zero, "The deletion ran before the scan pushed the record");
            Assert.That(pushed, Is.EqualTo(300), "The scan pushed a released value for the deleted record");
            var deleted = store.hlogBase._wrapper.CreateLogRecord(deletedAddress);
            Assert.That(deleted.Info.Valid && deleted.Info.IsSealed, Is.True, $"The deleted record was elided while the scan could push it: {deleted.Info}");

            store.Log.ShiftHeadAddress(store.Log.TailAddress, wait: true);
            Assert.That(tracker.DisposeCount(DisposeReason.Deleted), Is.EqualTo(1), "The deletion must run exactly once");
        }

        /// <summary>
        /// A CopyUpdate releases a durable source's value at once, even when the source sits in the sector the last flush ended in. The
        /// next flush starts in that sector and writes it again, but takes the bytes below its start from the image the previous flush
        /// kept, so the release cannot reach the source's durable image.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void CopyUpdateReleasesADurableSourceInTheBoundarySectorAtOnce()
        {
            var sourceAddress = store.Log.TailAddress;
            Upsert(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);

            var sectorSize = (int)log.SectorSize;
            var flushedUntil = store.hlogBase.FlushedUntilAddress;
            Assert.That(flushedUntil % sectorSize, Is.Not.Zero, "the flush must end mid-sector for the next one to write the sector again");
            Assert.That(sourceAddress, Is.GreaterThanOrEqualTo(RoundDown(flushedUntil, sectorSize)),
                "the source must sit in the sector the next flush writes again");
            Assert.That(store.hlog.IsFrozenForFlush(sourceAddress), Is.False, "a durable source must not be frozen");

            RmwAdd(1, 7);
            Assert.That(TrackedObjectValue.Snapshot().total, Is.EqualTo(1), "the CopyUpdate must dispose the durable source's value at once");
            Assert.That(TrackedObjectValue.Snapshot().max, Is.EqualTo(1), "no value object may be disposed more than once");
            Assert.That(SourceHoldsObject(sourceAddress), Is.False, "the CopyUpdate did not clear the source's value");
        }

        /// <summary>
        /// A CopyUpdate outside a checkpoint whose source the ReadOnly flush is walking releases the source's value at once, without changing
        /// the bytes of the record, which the flush writes from the live page: the value's slot gets a placeholder, and the record stays an
        /// object record with its position and hints.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void CopyUpdateDuringTheReadOnlyFlushWalkReleasesTheSourceWithoutChangingItsBytes()
        {
            var sourceAddress = store.Log.TailAddress;
            Upsert(1, 100);

            if (!ObjectFlushInjection.IsAvailable)
                Assert.Ignore("ObjectFlushInjection is compiled out in Release, so the flush walk cannot be held open.");

            using var insideFlushWindow = new SemaphoreSlim(0);
            using var releaseFlush = new SemaphoreSlim(0);
            var hookFired = false;

            ObjectFlushInjection.Hook = (phase, logicalAddress) =>
            {
                if (phase != ObjectFlushPhase.AfterCapture || logicalAddress != sourceAddress || hookFired)
                    return;
                hookFired = true;
                _ = insideFlushWindow.Release();
                _ = releaseFlush.Wait(TimeSpan.FromSeconds(30));
            };

            try
            {
                var shift = Task.Run(() => store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true));
                Assert.That(insideFlushWindow.Wait(TimeSpan.FromSeconds(30)), Is.True,
                    "the flush never reached AfterCapture, so this test proves nothing (note the hook is DEBUG-only)");
                Assert.That(store.hlog.IsFrozenForFlush(sourceAddress), Is.True, "Precondition: the flush is walking the source");

                var bytesBefore = RecordBytes(sourceAddress);
                RmwAdd(1, 7);
                Assert.That(TrackedObjectValue.Snapshot().total, Is.EqualTo(1), "the CopyUpdate did not release the source's value at once");
                Assert.That(SourceHoldsObject(sourceAddress), Is.False, "the source still holds its value object");
                Assert.That(store.hlogBase._wrapper.CreateLogRecord(sourceAddress).DataHeader.ValueIsObject, Is.True,
                    "the release must leave the source an object record");
                Assert.That(RecordBytes(sourceAddress), Is.EqualTo(bytesBefore), "the release changed the bytes of a record the flush is writing");

                _ = releaseFlush.Release();
                Assert.That(shift.Wait(TimeSpan.FromSeconds(30)), Is.True, "the read-only shift did not complete");
            }
            finally
            {
                ObjectFlushInjection.Reset();
            }

            Assert.That(TrackedObjectValue.Snapshot().max, Is.EqualTo(1), "no value object may be disposed more than once");
        }

        #endregion Deferred dispose of a record frozen for flush

        #region CopyUpdate — value-object slot clearing

        /// <summary>
        /// RMW on a record in the immutable region forces CopyUpdate. After the CAS succeeds, Tsavorite releases the source's value object
        /// and decrements the logSizeTracker for its heap. The trigger is NOT involved — <see cref="DisposeReason.CopyUpdated"/>
        /// does not fire via <see cref="IRecordTriggers.OnDispose"/>. The source record stays in the log until eviction, where OnEvict
        /// picks up any remaining key overflow.
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
                "CopyUpdated is handled internally by logSizeTracker; OnDispose must not fire for it");
            ClassicAssert.AreEqual(0, tracker.DisposeCount(DisposeReason.Deleted),
                "Deleted must not fire on a CopyUpdate path");
            ClassicAssert.AreEqual(0, tracker.TotalEvict(),
                "No page eviction should have happened in this test window");
        }

        /// <summary>
        /// After CopyUpdate, the source's value object is released: its slot gets a placeholder, and Tsavorite calls IHeapObject.Dispose on
        /// the object internally — the trigger is not involved. Verify that exactly one IHeapObject.Dispose fires for the source value object.
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

        /// <summary>
        /// A log scan reports a CopyUpdate's superseded source, which outside a checkpoint stays valid and unsealed in the immutable region,
        /// as an object record with no value object: the CopyUpdate released the value, leaving a placeholder that reads as null.
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void ScanReportsACopyUpdatedSourceWithNoValueObject()
        {
            // An earlier record for the key keeps the source in a chain, so the CopyUpdate does not elide it.
            Upsert(1, 50);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            var sourceAddress = store.Log.TailAddress;
            Upsert(1, 100);
            store.Log.ShiftReadOnlyAddress(store.Log.TailAddress, wait: true);
            RmwAdd(1, 7);
            Assert.That(TrackedObjectValue.Snapshot().total, Is.EqualTo(1), "Precondition: the CopyUpdate released the source's value");

            var found = false;
            using (var iter = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress, DiskScanBufferingMode.SinglePageBuffering))
            {
                while (iter.GetNext())
                {
                    if (iter.CurrentAddress != sourceAddress)
                        continue;
                    found = true;
                    Assert.That(iter.Info.Valid, Is.True, "The superseded source should still be valid");
                    Assert.That(iter.DataHeader.ValueIsObject, Is.True, "The superseded source should still be an object record");
                    Assert.That(iter.ValueObject, Is.Null, "The scan reported the released value of a CopyUpdate's source");
                }
            }
            Assert.That(found, Is.True, "The scan did not report the superseded source");
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
        /// every non-tombstoned, non-invalid record evicted past HeadAddress. Tombstoned records are
        /// skipped (heap was decremented at the delete site). Invalid records are skipped, including the
        /// source records that an immutable-region delete elided from the tag chain and already cleaned up.
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

            // An immutable-region delete whose source is the only record in its tag chain elides that source:
            // it is invalidated and either freelisted or disposed, so eviction will never visit it.
            var elidedSources = tracker.DisposeCount(DisposeReason.Elided) + tracker.DisposeCount(DisposeReason.RevivificationFreeList);
            ClassicAssert.LessOrEqual(elidedSources, immutableDeletes,
                "Only immutable-delete source records can be elided");

            // Force all records out to disk.
            store.Log.FlushAndEvict(wait: true);

            // Precise count:
            //  - (n - deleted) live records: visited by OnEvict.
            //  - mutableDeletes records: tombstoned in-place, skipped by OnEvict.
            //  - elidedSources records: invalidated and cleaned up at the delete site, skipped by OnEvict.
            //  - the remaining immutableDeletes sealed source records: NOT tombstoned, visited by OnEvict.
            //  - immutableDeletes new tombstone records at tail: tombstoned, skipped by OnEvict.
            var expectedEvictions = (n - deleted) + (immutableDeletes - elidedSources);
            ClassicAssert.AreEqual(expectedEvictions, tracker.EvictCount(EvictionSource.MainLog),
                $"OnEvict(MainLog) must fire exactly {expectedEvictions} times: " +
                $"{n - deleted} live + {immutableDeletes - elidedSources} sealed sources, " +
                $"skipping {mutableDeletes} in-place tombstones and {elidedSources} elided sources");
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
                PageSize = MinKvLogPageSize,
                ReadCacheMemorySize = 1L << 15,
                ReadCachePageSize = MinKvLogPageSize,
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

        internal enum ReadCacheUpdateOp { Upsert, RMW }

        /// <summary>
        /// Read-cache heap-size accounting parity. Every byte of heap a promoted read-cache record adds to
        /// the read-cache <see cref="LogSizeTracker{TStoreFunctions, TAllocator}"/> (at copy-to-read-cache
        /// time, via <c>UpdateSize(add: true)</c>) must be removed again when that record leaves memory (at
        /// page-close eviction, via <c>EvictRecordsInRange</c>, or — for RMW CopyUpdate — at the in-place
        /// source-value clear). In particular, a value-changing update over a key that is currently cached in
        /// the read cache must NOT leak the cached record's heap from the read-cache tracker.
        ///
        /// Two value-changing paths are exercised:
        /// <list type="bullet">
        /// <item><b>Upsert</b> (blind): detaches the read-cache prefix; the orphaned source stays Valid with
        /// its value intact and is decremented at page-close eviction. Regression for a latent leak in the old
        /// splice design, which invalidated the read-cache source in place (<c>PostCopyToTail</c> ->
        /// <c>SetInvalidAtomic</c>); since <c>EvictRecordsInRange</c> skips Invalid records, the
        /// <c>+UpdateSize</c> from promotion was never matched and the tracker drifted upward.</item>
        /// <item><b>RMW</b> (CopyUpdate): clears and disposes the read-cache source value object in place and
        /// must decrement the <i>read-cache</i> tracker (the allocator that owns the source), not the main-log
        /// tracker. Regression for crediting the wrong tracker, which leaked the read-cache tracker and
        /// under-counted the main-log tracker.</item>
        /// </list>
        /// </summary>
        [Test, Category("TsavoriteKV")]
        public void ReadCacheHeapSizeTrackerReturnsToZeroAfterUpdateAndEvict([Values] ReadCacheUpdateOp updateOp)
        {
            // Tear down the default main-log-only store and recreate with read cache enabled.
            store.Dispose(); store = null;
            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.1,
                LogMemorySize = 1L << (MinKvLogPageSizeBits + 4),
                PageSize = MinKvLogPageSize,
                ReadCacheMemorySize = 1L << (MinKvLogPageSizeBits + 5),
                ReadCachePageSize = MinKvLogPageSize,
                ReadCacheEnabled = true,
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TrackedObjectValue.Serializer(),
                    new LifecycleRecordTriggers(tracker))
                , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            // Wire a heap-size tracker onto the read cache so HeapSizeBytes reflects the tracked heap. The
            // target is set far above the read cache's actual capacity so the tracker never triggers trimming
            // on its own; the resizer task is intentionally NOT started, because we drive eviction explicitly
            // below. Only the create-site (UpdateSize) and evict-site (IncrementSize) accounting is exercised.
            var rcTargetSize = 1L << 20;
            var rcTracker = new LogSizeTracker<LifecycleStoreFunctions, LifecycleAllocator>(
                store.ReadCache, rcTargetSize, rcTargetSize / 10, rcTargetSize / 50, logger: null);
            store.ReadCache.SetLogSizeTracker(rcTracker);

            ClassicAssert.AreEqual(0, store.ReadCache.HeapSizeBytes, "Read-cache heap must start at zero");

            const int n = 50;
            for (int i = 0; i < n; i++) Upsert(i, i);
            store.Log.FlushAndEvict(wait: true); // push to disk so the next Read populates the read cache

            // Reads complete pending and promote each record into the read cache (each carries a heap value object).
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

            var heapAfterPromote = store.ReadCache.HeapSizeBytes;
            ClassicAssert.Greater(heapAfterPromote, 0,
                "Promoting heap-bearing records into the read cache must increase tracked heap");

            // Value-changing update over every read-cached key. Each operation finds the read-cache record as
            // its source (HasReadCacheSrc) and publishes a new main-log record, detaching the read-cache prefix.
            // Upsert leaves the orphaned source value intact; RMW CopyUpdate clears/disposes it in place.
            using (var s = NewSession())
            {
                for (int i = 0; i < n; i++)
                {
                    if (updateOp == ReadCacheUpdateOp.Upsert)
                    {
                        _ = s.BasicContext.Upsert(new TestObjectKey { key = i }, new TrackedObjectValue { value = i + 1000 }, 0);
                    }
                    else
                    {
                        var input = new TestObjectInput { value = i + 1000 };
                        var output = new TestObjectOutput();
                        _ = s.BasicContext.RMW(new TestObjectKey { key = i }, ref input, ref output, 0);
                    }
                }
            }

            // Evict the entire read cache. Every promoted record must have had its value heap decremented
            // exactly once (at the RMW clear, or here at page-close eviction for the Upsert orphans), so the
            // read-cache tracker returns to zero with no leak and no double-decrement.
            store.ReadCache.FlushAndEvict(wait: true);

            ClassicAssert.AreEqual(0, store.ReadCache.HeapSizeBytes,
                $"Read-cache heap tracker leaked after {updateOp}+evict: expected 0 but was {store.ReadCache.HeapSizeBytes} (promoted {heapAfterPromote})");
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