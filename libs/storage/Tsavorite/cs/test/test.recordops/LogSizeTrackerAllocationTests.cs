// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    using static TestUtils;

    using ObjAllocator = ObjectAllocator<StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>>;
    using ObjStoreFunctions = StoreFunctions<TestObjectKey.Comparer, DefaultRecordTriggers>;

    /// <summary>
    /// A heap object whose reported <see cref="IHeapObject.HeapMemorySize"/> is set by the caller, so a test can drive
    /// <see cref="LogSizeTracker{TStoreFunctions, TAllocator}"/> over its budget from the heap alone while the log's
    /// page count stays well under <c>MaxAllocatedPageCount</c>.
    /// </summary>
    internal sealed class SizedHeapObject : HeapObjectBase
    {
        internal SizedHeapObject(long heapMemorySize) => HeapMemorySize = heapMemorySize;

        public override HeapObjectBase Clone() => new SizedHeapObject(HeapMemorySize);
        public override void Dispose() { }
        public override void DoSerialize(BinaryWriter writer) => writer.Write(HeapMemorySize);
        public override void WriteType(BinaryWriter writer, bool isNull) => writer.Write(isNull);

        internal sealed class Serializer : BinaryObjectSerializer<IHeapObject>
        {
            public override void Deserialize(out IHeapObject obj)
            {
                _ = reader.ReadBoolean();
                obj = new SizedHeapObject(reader.ReadInt64());
            }

            public override void Serialize(IHeapObject obj) => obj.Serialize(writer);
        }
    }

    internal sealed class SizedHeapObjectFunctions : SessionFunctionsBase<Empty, Empty, Empty>
    {
        public override bool InitialWriter(ref LogRecord dstLogRecord, in RecordSizeInfo sizeInfo, ref Empty input, IHeapObject srcValue, ref Empty output, ref UpsertInfo upsertInfo)
            => dstLogRecord.TrySetValueObject(srcValue);

        public override bool InPlaceWriter(ref LogRecord logRecord, ref Empty input, IHeapObject srcValue, ref Empty output, ref UpsertInfo upsertInfo)
            => logRecord.TrySetValueObject(srcValue);

        public override RecordFieldInfo GetUpsertFieldInfo<TKey>(TKey key, IHeapObject value, ref Empty input)
            => new() { KeySize = key.KeyBytes.Length, ValueSize = ObjectIdMap.ObjectIdSize, ValueIsObject = true };
    }

    /// <summary>
    /// The allocation path must never wait on an eviction that cannot happen. <see cref="LogSizeTracker"/> backpressure is
    /// relieved only by its background resizer, which is not running before it is started (checkpoint recovery runs the log
    /// without it), while it is started but not yet dispatched by the thread pool, or once it has been stopped for shutdown.
    /// If the heap alone is over budget while the page count is still below <c>MaxAllocatedPageCount</c>, the page-turn path
    /// used to signal the absent resizer and return RETRY_NOW forever, so the allocating thread spun at low CPU and never
    /// completed. Regression for issue #2174.
    /// </summary>
    [TestFixture]
    internal class LogSizeTrackerAllocationTests : TestBase
    {
        const int PageSize = 1 << MinKvLogPageSizeBits;             // 4 KB
        const long LogMemorySize = PageSize << 4;                   // MaxAllocatedPageCount == BufferSize == 16
        const long TargetSize = LogMemorySize;                      // >= LogSizeTracker.MinTargetPageCount pages
        const int ObjectHeapSize = 1024;                            // ~126 records/page => the heap blows the budget after one page

        // More than BufferSize pages' worth of records, so the test turns many pages past the point where the heap is over budget.
        const int NumRecords = 4000;

        // Generous relative to the ~1s this takes when the allocation path is not livelocked.
        static readonly TimeSpan CompletionTimeout = TimeSpan.FromSeconds(60);

        TsavoriteKV<ObjStoreFunctions, ObjAllocator> store;
        LogSizeTracker<ObjStoreFunctions, ObjAllocator> tracker;
        IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LogSizeTrackerAllocation.log"), deleteOnClose: true);
            objlog = Devices.CreateLogDevice(Path.Join(MethodTestDir, "LogSizeTrackerAllocation.obj.log"), deleteOnClose: true);

            store = new(new()
            {
                IndexSize = 1L << 13,
                LogDevice = log,
                ObjectLogDevice = objlog,
                MutableFraction = 0.9,
                LogMemorySize = LogMemorySize,
                PageSize = PageSize
            }, StoreFunctions.Create(new TestObjectKey.Comparer(), () => new SizedHeapObject.Serializer(), DefaultRecordTriggers.Instance)
             , (allocatorSettings, storeFunctions) => new(allocatorSettings, storeFunctions));

            tracker = new LogSizeTracker<ObjStoreFunctions, ObjAllocator>(store.Log, TargetSize, TargetSize / 8, TargetSize / 16, logger: null);
            store.Log.SetLogSizeTracker(tracker);
        }

        [TearDown]
        public void TearDown()
        {
            store?.Dispose();
            store = null;
            tracker = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            OnTearDown();
        }

        /// <param name="startAndStopResizer">When false the resizer was never started, as while checkpoint recovery owns the
        ///     log. When true it is started and then stopped, as during shutdown. Both leave nobody to act on the tracker's signal.</param>
        [Test, Category(TsavoriteKVTestCategory), Category(SmokeTestCategory), Category(ObjectIdMapCategory)]
        public void UpsertCompletesWhenHeapIsOverBudgetAndResizerIsNotRunning([Values] bool startAndStopResizer)
        {
            if (startAndStopResizer)
            {
                tracker.Start(CancellationToken.None);
                tracker.Stop(wait: true);
                Assert.That(tracker.IsStopped, Is.True);
            }
            Assert.That(tracker.IsRunning, Is.False);

            // Run on a worker so a regression fails the test instead of hanging the test host forever.
            var upserts = Task.Run(() =>
            {
                using var session = store.NewSession<TestObjectKey, Empty, Empty, Empty, SizedHeapObjectFunctions>(new SizedHeapObjectFunctions());
                var bContext = session.BasicContext;
                for (var key = 0; key < NumRecords; key++)
                    _ = bContext.Upsert(new TestObjectKey { key = key }, new SizedHeapObject(ObjectHeapSize));
            });

            Assert.That(upserts.Wait(CompletionTimeout), Is.True,
                $"Upserts did not complete within {CompletionTimeout}; the allocation path is waiting on an eviction that the stopped resizer cannot perform. Tracker: {tracker}");
            upserts.GetAwaiter().GetResult();   // surface any exception

            // The heap drove us over budget, so eviction must have happened: the page count is capped and HeadAddress advanced.
            Assert.That(tracker.IsOverBudget, Is.True, $"Test did not exceed the tracker budget. Tracker: {tracker}");
            Assert.That(store.Log.AllocatedPageCount, Is.LessThanOrEqualTo(store.Log.MaxAllocatedPageCount));
            Assert.That(store.hlogBase.HeadAddress, Is.GreaterThan(store.hlogBase.BeginAddress),
                "HeadAddress must have advanced, evicting pages to stay within MaxAllocatedPageCount");
        }
    }
}