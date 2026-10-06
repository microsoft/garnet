// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;
using static Tsavorite.test.TestUtils;

namespace Tsavorite.test.scancursor
{
    using SpanByteStoreFunctions = StoreFunctions<SpanByteComparer, SpanByteRecordTriggers>;

    /// <summary>
    /// Device that holds each full-page read's completion briefly and records the peak number of full-page reads in
    /// flight at once, so read-ahead is observable as a peak above one.
    /// </summary>
    /// <remarks>
    /// Read-ahead is otherwise invisible from outside the iterator: it changes when a read is issued, not how many are
    /// issued or what they return. Holding each completion widens the window in which a second read can be observed
    /// overlapping the first, which is the only externally visible difference between the two buffering modes.
    /// </remarks>
    internal sealed class PageReadConcurrencyDevice : StorageDeviceBase
    {
        /// <summary>The real device; this class only instruments it and forwards every operation unchanged.</summary>
        private readonly IDevice underlying;

        /// <summary>Log page size, used to tell frame loads (a full page) from the smaller record-level reads.</summary>
        private readonly int pageSize;

        /// <summary>How long each full-page read's completion is held before being delivered.</summary>
        private readonly int holdMs;

        /// <summary>Full-page reads submitted but whose completion has not yet been delivered.</summary>
        private int inFlight;

        /// <summary>Maximum value <see cref="inFlight"/> has reached; 2 means a read-ahead overlapped a read.</summary>
        private int peak;

        /// <summary>Total full-page reads seen, so a test can confirm it exercised enough pages to be meaningful.</summary>
        private int fullPageReads;

        /// <summary>Peak number of full-page reads in flight at once over the run.</summary>
        public int PeakConcurrentPageReads => Volatile.Read(ref peak);

        /// <summary>Total number of full-page reads issued over the run.</summary>
        public int FullPageReadCount => Volatile.Read(ref fullPageReads);

        /// <summary>Wraps <paramref name="underlying"/>, instrumenting its full-page reads.</summary>
        /// <param name="underlying">Device to forward all operations to.</param>
        /// <param name="pageSize">Log page size; reads shorter than this are forwarded uninstrumented.</param>
        /// <param name="holdMs">How long to hold each full-page read's completion.</param>
        public PageReadConcurrencyDevice(IDevice underlying, int pageSize, int holdMs)
            : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
        {
            this.underlying = underlying;
            this.pageSize = pageSize;
            this.holdMs = holdMs;
        }

        /// <inheritdoc/>
        /// <remarks>Initializes this instance and the underlying device, which the base class does not know about.</remarks>
        public override void Initialize(long segmentSize, LightEpoch epoch = null, bool omitSegmentIdFromFilename = false)
        {
            base.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
            underlying.Initialize(segmentSize, epoch, omitSegmentIdFromFilename);
        }

        /// <inheritdoc/>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
            => underlying.RemoveSegmentAsync(segment, callback, result);

        /// <inheritdoc/>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, object context)
            => underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);

        /// <inheritdoc/>
        /// <remarks>
        /// Counts the read, records the resulting concurrency, then delays its completion so an overlapping read-ahead
        /// has time to arrive and be counted alongside it.
        /// </remarks>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
                DeviceIOCompletionCallback callback, object context)
        {
            // Frame loads read a whole page; the liveness lookup's record reads are smaller and would otherwise be
            // counted as scan IO and inflate the concurrency this measures.
            if (readLength < pageSize)
            {
                underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
                return;
            }

            _ = Interlocked.Increment(ref fullPageReads);
            var now = Interlocked.Increment(ref inFlight);

            // Raise the high-water mark to this read's concurrency. CAS in a loop because another read may be doing the
            // same concurrently, and the peak must never move backwards.
            int observed;
            while (now > (observed = Volatile.Read(ref peak)) && Interlocked.CompareExchange(ref peak, now, observed) != observed)
            { }

            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, (errorCode, numBytes, ctx, ioException) =>
            {
                // Hand the delay to the thread pool rather than sleeping here. A local device can invoke this callback
                // inline on the thread that called ReadAsync, so sleeping inline would stall the scanning thread before
                // it reaches the read-ahead -- making the absence of overlap an artifact of this device rather than a
                // property of the iterator, and letting the test pass against a build with no read-ahead at all.
                _ = ThreadPool.UnsafeQueueUserWorkItem(_ =>
                {
                    Thread.Sleep(holdMs);

                    // Drop out of the in-flight count before delivering the completion, so the scan cannot observe the
                    // read as finished while it is still counted as outstanding.
                    _ = Interlocked.Decrement(ref inFlight);
                    callback(errorCode, numBytes, ctx, ioException);
                }, null);
            }, context);
        }

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }

    /// <summary>
    /// Covers <c>ScanCursor</c>'s disk read-ahead policy. An unbounded <c>count</c> iterates the whole log in one call,
    /// so the next page is read while the current one is processed; a bounded <c>count</c> creates a new iterator per
    /// call and usually consumes only part of one page, so reading ahead would be wasted IO.
    /// </summary>
    /// <remarks>
    /// The record-level assertions here also hold under either buffering mode, so they alone would not catch the policy
    /// silently reverting; the peak-concurrency assertions are what pin it.
    /// </remarks>
    [TestFixture]
    internal class ScanCursorReadAheadTests : TestBase
    {
        /// <summary>Store under test; its log is fully evicted before each scan so every record is read from disk.</summary>
        private TsavoriteKV<SpanByteStoreFunctions, SpanByteAllocator<SpanByteStoreFunctions>> store;

        /// <summary>The store's log device, wrapped so the scan's page reads can be counted.</summary>
        private PageReadConcurrencyDevice log;

        /// <summary>Page size bits. Small enough that <see cref="TotalRecords"/> spans several pages.</summary>
        private const int PageSizeBits = 15;

        /// <summary>Page size in bytes; also the threshold the device uses to recognize a frame load.</summary>
        private const int PageSize = 1 << PageSizeBits;

        /// <summary>Record count, chosen to fill enough pages that read-ahead has somewhere to run.</summary>
        private const int TotalRecords = 3000;

        /// <summary>How long the device holds each page-read completion, widening the overlap window.</summary>
        private const int PageReadHoldMs = 25;

        /// <summary>Fewest page reads a run must make for its peak-concurrency result to mean anything.</summary>
        private const int MinPageReadsForMeaningfulResult = 4;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            log = new PageReadConcurrencyDevice(Devices.CreateLogDevice(Path.Join(MethodTestDir, "test.log"), deleteOnClose: true), PageSize, PageReadHoldMs);
            store = new(new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                LogMemorySize = 1L << 25,
                PageSize = PageSize
            }, StoreFunctions.Create(SpanByteComparer.Instance, SpanByteRecordTriggers.Instance)
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

        /// <summary>
        /// Fills the log with <see cref="TotalRecords"/> distinct keys and evicts it, so the scan reads every record
        /// from disk across several pages rather than finding them in memory.
        /// </summary>
        private unsafe void PopulateAndEvict()
        {
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());
            var bContext = session.BasicContext;

            // Pad the values so the records fill pages quickly; the contents are never checked.
            var valueFill = new string('x', 100);
            for (var i = 0; i < TotalRecords; i++)
            {
                var key = MemoryMarshal.Cast<char, byte>($"key_{i}".AsSpan());
                var value = MemoryMarshal.Cast<char, byte>($"v{valueFill}_{i}".AsSpan());
                fixed (byte* keyPtr = key)
                    _ = bContext.Upsert(TestSpanByteKey.FromPointer(keyPtr, key.Length), value);
            }

            // Push the whole log below HeadAddress, so the scan's records come through the frame-load path.
            store.Log.FlushAndEvict(wait: true);
        }

        /// <summary>Records the address of every record the scan pushes, so none is lost or duplicated.</summary>
        private struct CollectingFuncs : IScanIteratorFunctions
        {
            /// <summary>
            /// Addresses pushed, in order. Must be a reference type: the struct is boxed into
            /// <c>ScanCursorState.functions</c>, so mutations land on the boxed copy rather than the caller's struct,
            /// and only a shared reference is visible to both.
            /// </summary>
            public List<long> addresses;

            public readonly bool Reader<TSourceLogRecord>(in TSourceLogRecord logRecord, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult)
                where TSourceLogRecord : ISourceLogRecord
            {
                cursorRecordResult = CursorRecordResult.Accept;
                addresses.Add(recordMetadata.Address);
                return true;
            }

            public readonly bool OnStart(long beginAddress, long endAddress) => true;
            public readonly void OnException(Exception exception, long numberOfRecords) { }
            public readonly void OnStop(bool completed, long numberOfRecords) { }
        }

        /// <summary>
        /// A full-log scan (<c>count</c> of <see cref="long.MaxValue"/>) reads the next page while processing the
        /// current one, so two page reads are in flight at the peak.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void FullLogScanCursorReadsAhead()
        {
            PopulateAndEvict();
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());

            var fns = new CollectingFuncs { addresses = [] };
            long cursor = 0;
            _ = session.ScanCursor(ref cursor, count: long.MaxValue, fns, endAddress: long.MaxValue);

            ClassicAssert.AreEqual(TotalRecords, fns.addresses.Count, "All records must still be returned");
            ClassicAssert.GreaterOrEqual(log.FullPageReadCount, MinPageReadsForMeaningfulResult, "Need several evicted pages for read-ahead to be observable");
            ClassicAssert.AreEqual(2, log.PeakConcurrentPageReads,
                $"Full-log ScanCursor must keep two page reads in flight; saw peak {log.PeakConcurrentPageReads} over {log.FullPageReadCount} page reads");
        }

        /// <summary>
        /// A bounded <c>count</c> (the SCAN path) builds a new iterator per call and usually consumes only part of one
        /// page, so it must not read ahead: the prefetched page would mostly be discarded unread.
        /// </summary>
        [Test]
        [Category("TsavoriteKV")]
        [Category("Smoke")]
        public void BoundedCountScanCursorDoesNotReadAhead()
        {
            PopulateAndEvict();
            using var session = store.NewSession<TestSpanByteKey, PinnedSpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(new SpanByteFunctions<Empty>());

            var fns = new CollectingFuncs { addresses = [] };
            long cursor = 0;

            // Drive the cursor to exhaustion the way SCAN does, so this covers the same page count as the full-log case.
            while (session.ScanCursor(ref cursor, count: 10, fns, endAddress: long.MaxValue))
                ;

            ClassicAssert.AreEqual(TotalRecords, fns.addresses.Count, "All records must still be returned");
            ClassicAssert.GreaterOrEqual(log.FullPageReadCount, MinPageReadsForMeaningfulResult, "Need several evicted pages for read-ahead to be observable");
            ClassicAssert.AreEqual(1, log.PeakConcurrentPageReads,
                $"Bounded-count ScanCursor must not read ahead; saw peak {log.PeakConcurrentPageReads} over {log.FullPageReadCount} page reads");
        }
    }
}