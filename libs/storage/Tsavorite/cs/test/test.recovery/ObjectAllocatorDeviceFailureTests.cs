// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test
{
    using ClassAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ClassStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    [TestFixture]
    internal class ObjectAllocatorDeviceFailureTests : TestBase
    {
        private TsavoriteKV<ClassStoreFunctions, ClassAllocator> store;
        private ControlledReadFailureDevice logDevice;
        private ControlledReadFailureDevice objectLogDevice;
        private ManualResetEventSlim flushFailureEvent;
        private CommitInfo flushFailure;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            flushFailure = default;
            flushFailureEvent = new(false);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            logDevice?.Dispose();
            logDevice = null;
            objectLogDevice?.Dispose();
            objectLogDevice = null;
            flushFailureEvent?.Dispose();
            flushFailureEvent = null;
            OnTearDown();
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ObjectIdMapCategory)]
        public void PartialSectorFlushSkipsReadBack()
        {
            CreateStore(useLargeObjects: false, captureFlushFailures: true);

            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var context = session.BasicContext;

            _ = context.Upsert(new TestObjectKey { key = 1 }, new TestObjectValue { value = 1 }, Empty.Default);
            store.Log.Flush(wait: true);

            var lastSuccessfulFlush = store.Log.FlushedUntilAddress;
            Assert.That(lastSuccessfulFlush, Is.EqualTo(store.Log.TailAddress));
            Assert.That(lastSuccessfulFlush % logDevice.SectorSize, Is.Not.Zero,
                "The first flush must end mid-sector so the next flush rewrites an aligned live-page prefix.");

            var writeCountBeforeFlush = logDevice.WriteCount;
            _ = context.Upsert(new TestObjectKey { key = 2 }, new TestObjectValue { value = 2 }, Empty.Default);
            var expectedFlushUntilAddress = store.Log.TailAddress;

            logDevice.FailNextRead(InjectedReadFailure.DeviceError);
            store.Log.Flush(wait: true);

            Assert.That(logDevice.FailedReadCount, Is.Zero, "Front-partial flushing must not read back the already-durable sector prefix.");
            Assert.That(logDevice.WriteCount, Is.GreaterThan(writeCountBeforeFlush));
            Assert.That(store.Log.FlushedUntilAddress, Is.EqualTo(expectedFlushUntilAddress));
            Assert.That(flushFailureEvent.IsSet, Is.False);
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ObjectIdMapCategory)]
        public void EveryRecordTheFlushWritesIsFrozenWhileItIsWritten()
        {
            // The deferred-cleanup design rests on this: while the object-log flush is writing a record, that record's address must
            // report IsFrozenForFlush, because that is what makes a concurrent dispose decline and defer its release to eviction.
            // The injection hook lets this be asserted at the exact moment of the write instead of inferred.
            CreateStore(useLargeObjects: true, captureFlushFailures: false);

            var observed = 0;
            var notFrozen = new List<long>();
            ObjectFlushInjection.Hook = (phase, logicalAddress) =>
            {
                if (phase != ObjectFlushPhase.AfterRecordWritten)
                    return;
                _ = Interlocked.Increment(ref observed);
                if (!store.hlog.IsFrozenForFlush(logicalAddress))
                {
                    lock (notFrozen)
                        notFrozen.Add(logicalAddress);
                }
            };

            try
            {
                using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
                var context = session.BasicContext;

                for (var key = 0; key < 16; ++key)
                    _ = context.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue(1024), Empty.Default);

                store.Log.Flush(wait: true);
            }
            finally
            {
                ObjectFlushInjection.Reset();
            }

            Assert.That(observed, Is.GreaterThan(0), "the injection hook must fire for records the flush writes, or this test proves nothing");
            Assert.That(notFrozen, Is.Empty, $"records written by the flush must be frozen at that moment; unfrozen addresses: {string.Join(", ", notFrozen)}");
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ObjectIdMapCategory)]
        public void WriteFailureIsSurfaced([Values] bool synchronous)
        {
            // A device write can fail two ways: rejected at submission, so the completion callback runs INLINE on the submitting
            // thread before WriteAsync returns (LocalMemoryDevice's ENOENT/EINVAL paths, ShardedStorageDevice's inline countdown);
            // or accepted and then failed, so the completion is posted from another thread. Both must surface the error rather than
            // swallow it, which would let FlushedUntilAddress advance past bytes that never reached storage.
            //
            // NOTE: the flush is issued with wait: false deliberately. A failed write never advances FlushedUntilAddress, and
            // Flush(wait: true) waits on exactly that, so it does not return after a failed write in EITHER mode -- the failure is
            // observable only through the commit callback. Waiting here would hang the run rather than test anything.
            CreateStore(useLargeObjects: true, captureFlushFailures: true);

            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var context = session.BasicContext;

            for (var key = 0; key < 8; ++key)
                _ = context.Upsert(new TestObjectKey { key = key }, new TestLargeObjectValue(1024), Empty.Default);

            objectLogDevice.FailWriteNumber(objectLogDevice.WriteCount + 1, synchronous);

            try
            {
                store.Log.Flush(wait: false);
            }
            catch (TsavoriteException)
            {
                // Propagating the failure to the caller is an acceptable surfacing.
            }

            var surfaced = flushFailureEvent.Wait(TimeSpan.FromSeconds(30));

            Assert.That(objectLogDevice.FailedWriteCount, Is.EqualTo(1), "the injected write failure must have been taken");
            Assert.That(surfaced, Is.True, "the write failure must be surfaced, not swallowed");
            Assert.That(flushFailure.ErrorCode, Is.Not.Zero, "the surfaced failure must carry the device error code");
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ObjectIdMapCategory)]
        public void SnapshotPartialSectorSkipsReadBackOnFreshDevice()
        {
            CreateStore(useLargeObjects: false, captureFlushFailures: false);

            using var session = store.NewSession<TestObjectKey, TestObjectInput, TestObjectOutput, Empty, TestObjectFunctions>(new TestObjectFunctions());
            var context = session.BasicContext;

            _ = context.Upsert(new TestObjectKey { key = 1 }, new TestObjectValue { value = 1 }, Empty.Default);
            var snapshotStartAddress = store.Log.TailAddress;
            _ = context.Upsert(new TestObjectKey { key = 2 }, new TestObjectValue { value = 2 }, Empty.Default);
            var snapshotEndAddress = store.Log.TailAddress;

            using var snapshotLogDevice = new ControlledReadFailureDevice(
                Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectAllocatorDeviceFailureTests.snapshot.log"), deleteOnClose: true));
            using var snapshotObjectLogDevice = new ControlledReadFailureDevice(
                Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectAllocatorDeviceFailureTests.snapshot.obj.log"), deleteOnClose: true));
            using var flushBuffers = store.hlogBase.CreateCircularFlushBuffers(snapshotObjectLogDevice, logger: null);

            Assert.That(snapshotStartAddress % snapshotLogDevice.SectorSize, Is.Not.Zero,
                "The snapshot must start mid-sector to exercise the read-back path.");

            var startPage = store.hlogBase.GetPage(snapshotStartAddress);
            var endPage = store.hlogBase.GetPage(snapshotEndAddress) + 1;
            using var coordination = new SnapshotFlushCoordination();
            coordination.BeginCutoffCapture(startPage);
            coordination.PublishReadOnlyFlushCutoff(0);
            coordination.BeginFlushing();
            store.hlogBase.AsyncFlushPagesForSnapshot(flushBuffers, startPage, endPage, snapshotStartAddress, snapshotEndAddress,
                long.MaxValue, snapshotLogDevice, snapshotObjectLogDevice, coordination, out var completedTask, throttleCheckpointFlushDelayMs: -1);

            Assert.DoesNotThrowAsync(async () => await completedTask);
            Assert.That(snapshotLogDevice.ReadCount, Is.Zero, "A fresh snapshot sector has no existing prefix to preserve.");
            Assert.That(snapshotLogDevice.WriteCount, Is.EqualTo(1));
            Assert.That(logDevice.ReadCount, Is.Zero, "Snapshot read-back must not read from the allocator's main-log device.");
        }

        [Test]
        [Category(TsavoriteKVTestCategory)]
        [Category(ObjectIdMapCategory)]
        public void HeaderReadFailureDefersPairedTruncation([Values] InjectedReadFailure failureMode)
        {
            const int pageSize = MinKvLogPageSize;
            CreateStore(useLargeObjects: true, captureFlushFailures: false, segmentSize: pageSize * 2, objectLogSegmentSize: 1L << 22);

            using var session = store.NewSession<TestObjectKey, TestLargeObjectInput, TestLargeObjectOutput, Empty, TestLargeObjectFunctions>(new TestLargeObjectFunctions());
            var context = session.BasicContext;
            var input = new TestLargeObjectInput();
            var output = new TestLargeObjectOutput();
            long truncateAddress = 0;

            for (var key = 0; key < 400; key++)
            {
                var recordAddress = store.Log.TailAddress;
                if (truncateAddress == 0
                    && recordAddress > (2 * pageSize) + PageHeader.Size
                    && store.hlogBase.GetOffsetOnPage(recordAddress) > PageHeader.Size)
                    truncateAddress = recordAddress;

                _ = context.Upsert(new TestObjectKey { key = key }, ref input, new TestLargeObjectValue(1 << 15), ref output);
            }

            Assert.That(truncateAddress, Is.GreaterThan(0), "The test did not create a record on the third main-log page.");
            store.Log.Flush(wait: true);
            store.Log.ShiftBeginAddress(truncateAddress, truncateLog: false);
            Assert.That(store.Log.BeginAddress, Is.EqualTo(truncateAddress));

            var truncateMethod = store.hlogBase.GetType().GetMethod("TruncateUntilAddressBlocking", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.That(truncateMethod, Is.Not.Null);

            logDevice.FailNextRead(failureMode);
            var invocationException = Assert.Throws<TargetInvocationException>(() => truncateMethod.Invoke(store.hlogBase, [truncateAddress]));
            Assert.That(invocationException.InnerException, Is.TypeOf<TsavoriteIOException>());

            Assert.That(logDevice.FailedReadCount, Is.EqualTo(1));
            Assert.That(logDevice.LastFailedReadAddress, Is.EqualTo((ulong)store.hlogBase.GetAddressOfStartOfPageOfAddress(truncateAddress)));
            Assert.That(logDevice.TruncateUntilAddressCount, Is.Zero, "The main log must not truncate when its page-header read fails.");
            Assert.That(objectLogDevice.RemoveSegmentCount, Is.Zero, "The object log must not truncate when the paired main-log header read fails.");

            store.Log.Truncate();

            Assert.That(logDevice.TruncateCompleted.Wait(TimeSpan.FromSeconds(10)), Is.True, "The retried main-log truncation did not complete.");
            Assert.That(objectLogDevice.SegmentRemovalCompleted.Wait(TimeSpan.FromSeconds(10)), Is.True, "The retried object-log truncation did not complete.");
            Assert.That(logDevice.TruncateUntilAddressCount, Is.EqualTo(1));
            Assert.That(objectLogDevice.RemoveSegmentCount, Is.GreaterThan(0));
        }

        private void CreateStore(bool useLargeObjects, bool captureFlushFailures, long segmentSize = 1L << 30, long objectLogSegmentSize = 1L << 30)
        {
            logDevice = new(Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectAllocatorDeviceFailureTests.log"), deleteOnClose: true));
            objectLogDevice = new(Devices.CreateLogDevice(Path.Join(MethodTestDir, "ObjectAllocatorDeviceFailureTests.obj.log"), deleteOnClose: true));

            var storeFunctions = useLargeObjects
                ? StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestLargeObjectValue.Serializer(), DefaultRecordTriggers.Instance)
                : StoreFunctions.Create(new TestObjectKey.Comparer(), () => new TestObjectValue.Serializer(), DefaultRecordTriggers.Instance);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = logDevice,
                ObjectLogDevice = objectLogDevice,
                MutableFraction = 0.1,
                LogMemorySize = 1L << (MinKvLogPageSizeBits + 5),
                PageSize = MinKvLogPageSize,
                SegmentSize = segmentSize,
                ObjectLogSegmentSize = objectLogSegmentSize,
                loggerFactory = TestLoggerFactory
            }, storeFunctions, (allocatorSettings, functions) =>
            {
                if (captureFlushFailures)
                {
                    allocatorSettings.flushCallback = info =>
                    {
                        if (info.ErrorCode == 0)
                            return;
                        flushFailure = info;
                        flushFailureEvent.Set();
                    };
                }

                return new(allocatorSettings, functions);
            });
        }
    }

    internal enum InjectedReadFailure
    {
        DeviceError,
        ShortRead
    }

    internal sealed class ControlledReadFailureDevice : StorageDeviceBase
    {
        internal const uint InjectedErrorCode = 38;

        private readonly IDevice underlying;
        private int failNextRead;
        private int failWriteNumber;
        private int failWriteSynchronously;
        private int failedWriteCount;
        private int readCount;
        private int writeCount;
        private int failedReadCount;
        private int truncateUntilAddressCount;
        private int removeSegmentCount;
        private int completedRemoveSegmentCount;
        private InjectedReadFailure failureMode;

        internal readonly ManualResetEventSlim TruncateCompleted = new(false);
        internal readonly ManualResetEventSlim SegmentRemovalCompleted = new(false);

        internal int ReadCount => Volatile.Read(ref readCount);
        internal int WriteCount => Volatile.Read(ref writeCount);
        internal int FailedReadCount => Volatile.Read(ref failedReadCount);
        internal int FailedWriteCount => Volatile.Read(ref failedWriteCount);
        internal int TruncateUntilAddressCount => Volatile.Read(ref truncateUntilAddressCount);
        internal int RemoveSegmentCount => Volatile.Read(ref removeSegmentCount);
        internal ulong LastFailedReadAddress { get; private set; }
        internal uint LastFailedReadLength { get; private set; }
        internal ulong LastFailedWriteAddress { get; private set; }

        internal ControlledReadFailureDevice(IDevice underlying)
            : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
        {
            this.underlying = underlying;
        }

        internal void FailNextRead(InjectedReadFailure mode)
        {
            failureMode = mode;
            _ = Interlocked.Exchange(ref failNextRead, 1);
        }

        /// <summary>Fail the <paramref name="writeNumber"/>'th write (1-based). When <paramref name="synchronous"/> the completion callback
        /// is invoked with an error inline on the submitting thread, which is the shape a device uses when it rejects a request outright
        /// (see LocalMemoryDevice's ENOENT/EINVAL paths and ShardedStorageDevice's inline countdown completion). Otherwise the error is
        /// posted to the thread pool, matching a request that was accepted and then failed asynchronously. Neither case touches the
        /// underlying device, so no bytes for the failed write reach storage.</summary>
        internal void FailWriteNumber(int writeNumber, bool synchronous = true)
        {
            Volatile.Write(ref failWriteSynchronously, synchronous ? 1 : 0);
            Volatile.Write(ref failWriteNumber, writeNumber);
        }

        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            _ = Interlocked.Increment(ref removeSegmentCount);
            underlying.RemoveSegmentAsync(segment, removeResult =>
            {
                callback?.Invoke(removeResult);
                if (Interlocked.Increment(ref completedRemoveSegmentCount) >= StartSegment)
                    SegmentRemovalCompleted.Set();
            }, result);
        }

        public override void TruncateUntilAddress(long toAddress)
        {
            _ = Interlocked.Increment(ref truncateUntilAddressCount);
            try
            {
                base.TruncateUntilAddress(toAddress);
            }
            finally
            {
                TruncateCompleted.Set();
            }
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            var thisWrite = Interlocked.Increment(ref writeCount);
            if (Volatile.Read(ref failWriteNumber) == thisWrite)
            {
                _ = Interlocked.Increment(ref failedWriteCount);
                LastFailedWriteAddress = destinationAddress;

                if (Volatile.Read(ref failWriteSynchronously) == 1)
                {
                    // Submission failure: complete inline on the caller's thread, before WriteAsync returns.
                    callback(InjectedErrorCode, 0, context, ioException: null);
                }
                else
                {
                    // Accepted-then-failed: complete from a different thread, as a posted IO completion would.
                    _ = ThreadPool.UnsafeQueueUserWorkItem(_ => callback(InjectedErrorCode, 0, context, ioException: null), null);
                }
                return;
            }
            underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            _ = Interlocked.Increment(ref readCount);
            if (Interlocked.Exchange(ref failNextRead, 0) == 1)
            {
                _ = Interlocked.Increment(ref failedReadCount);
                LastFailedReadAddress = SegmentSize > 0 ? (ulong)(segmentId * SegmentSize) + sourceAddress : sourceAddress;
                LastFailedReadLength = readLength;

                if (failureMode == InjectedReadFailure.DeviceError)
                    callback(InjectedErrorCode, 0, context, ioException: null);
                else
                    callback(0, readLength / 2, context, ioException: null);
                return;
            }

            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        public override bool TryComplete() => underlying.TryComplete();

        public override bool Throttle() => underlying.Throttle();

        public override long GetFileSize(int segment) => underlying.GetFileSize(segment);

        public override void Reset() => underlying.Reset();

        public override void Dispose()
        {
            TruncateCompleted.Dispose();
            SegmentRemovalCompleted.Dispose();
            underlying.Dispose();
        }
    }
}