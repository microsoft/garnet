// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.Objects
{
    using static TestUtils;

    /// <summary>
    /// Covers <see cref="ObjectLog"/>, the log-structured substrate that replaces the hand-managed object-log file: allocation
    /// (including the page-bounded partial form), reading from memory and from the device, durability, truncation, recovery
    /// positioning, and the address ceiling.
    /// </summary>
    [TestFixture]
    internal class ObjectLogTests : TestBase
    {
        // Small geometry so page turns, eviction and segment truncation all happen within a few hundred KB.
        const int PageSizeBits = 12;                    // 4 KB
        const int PageSize = 1 << PageSizeBits;
        const int PageCount = ObjectLog.MinPageCount;   // 4
        const int SegmentSizeBits = 14;                 // 16 KB == 4 pages

        IDevice device;
        ObjectLog objectLog;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);
            device = CreateDevice();
        }

        [TearDown]
        public void TearDown()
        {
            objectLog?.Dispose();
            objectLog = null;
            device?.Dispose();
            device = null;
            DeleteDirectory(MethodTestDir);
            OnTearDown();
        }

        static IDevice CreateDevice(string name = "objectlog.obj.log")
            => Devices.CreateLogDevice(Path.Join(MethodTestDir, name), deleteOnClose: false);

        static ObjectLogSettings DefaultSettings(IDevice device) => new()
        {
            LogDevice = device,
            PageSizeBits = PageSizeBits,
            PageCount = PageCount,
            SegmentSizeBits = SegmentSizeBits
        };

        ObjectLog CreateLog(Action<ObjectLogSettings> configure = null)
        {
            var settings = DefaultSettings(device);
            configure?.Invoke(settings);
            return objectLog = new ObjectLog(settings);
        }

        /// <summary>Allocate and fill <paramref name="length"/> bytes with a pattern derived from <paramref name="seed"/>.</summary>
        static long Write(ObjectLog log, int length, byte seed)
        {
            var scope = log.BeginScope();
            try
            {
                var logicalAddress = log.Allocate(length);
                Fill(log.AsSpan(logicalAddress, length), seed);
                return logicalAddress;
            }
            finally
            {
                log.EndScope(scope);
            }
        }

        static void Fill(Span<byte> span, byte seed)
        {
            for (var ii = 0; ii < span.Length; ++ii)
                span[ii] = (byte)(seed + ii);
        }

        static void VerifyPattern(ReadOnlySpan<byte> span, byte seed, string what)
        {
            for (var ii = 0; ii < span.Length; ++ii)
            {
                if (span[ii] != (byte)(seed + ii))
                    Assert.Fail($"{what}: byte {ii} of {span.Length} is {span[ii]}, expected {(byte)(seed + ii)}");
            }
        }

        static void VerifyRead(ObjectLog log, long logicalAddress, int length, byte seed, string what)
        {
            var buffer = new byte[length];
            log.Read(logicalAddress, length, buffer);
            VerifyPattern(buffer, seed, $"{what} at address {logicalAddress}");
        }

        #region Construction
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public void ConstructorRequiresADevice()
        {
            var settings = DefaultSettings(device);
            settings.LogDevice = null;
            _ = Assert.Throws<TsavoriteException>(() => _ = new ObjectLog(settings));
        }

        [Test, Category(ObjectLogCategory)]
        public void ConstructorRejectsOutOfRangeGeometry(
            [Values(LogSettings.kMinPageSizeBits - 1, LogSettings.kMaxPageSizeBits + 1)] int pageSizeBits)
        {
            var settings = DefaultSettings(device);
            settings.PageSizeBits = pageSizeBits;
            _ = Assert.Throws<TsavoriteException>(() => _ = new ObjectLog(settings));
        }

        [Test, Category(ObjectLogCategory)]
        public void ConstructorRejectsTooFewPages()
        {
            var settings = DefaultSettings(device);
            settings.PageCount = ObjectLog.MinPageCount - 1;
            _ = Assert.Throws<TsavoriteException>(() => _ = new ObjectLog(settings));
        }

        /// <summary>A segment is one or more pages, so a segment smaller than a page is a configuration error rather than
        /// something the device quietly absorbs.</summary>
        [Test, Category(ObjectLogCategory)]
        public void ConstructorRejectsSegmentSmallerThanPage()
        {
            var settings = DefaultSettings(device);
            settings.SegmentSizeBits = PageSizeBits - 1;
            _ = Assert.Throws<TsavoriteException>(() => _ = new ObjectLog(settings));
        }

        [Test, Category(ObjectLogCategory)]
        public void DefaultPageSizeIsTwiceTheStreamBufferSize()
            => Assert.That(1 << ObjectLog.DefaultPageSizeBits, Is.EqualTo(IStreamBuffer.BufferSize * 2),
                "The default page must be twice the buffer so a full buffer fits a page with room to spare");
        #endregion

        #region Allocation and in-memory reads
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public void AllocateAndReadFromMemory([Values(1, 17, 512, PageSize / 2)] int length)
        {
            var log = CreateLog();
            var logicalAddress = Write(log, length, seed: 7);

            Assert.That(log.TailAddress, Is.EqualTo(logicalAddress + length));
            Assert.That(logicalAddress, Is.GreaterThanOrEqualTo(log.HeadAddress), "The record just written must still be resident");
            VerifyRead(log, logicalAddress, length, seed: 7, "in-memory read");
        }

        [Test, Category(ObjectLogCategory)]
        public void AllocateRejectsSizesOutsideAPage([Values(0, -1, PageSize + 1)] int length)
        {
            var log = CreateLog();
            var scope = log.BeginScope();
            try
            {
                _ = Assert.Throws<TsavoriteException>(() => _ = log.Allocate(length));
            }
            finally
            {
                log.EndScope(scope);
            }
        }

        /// <summary>
        /// A read translates to one device extent, which cannot cross a page, so no allocation may straddle a page boundary. This is
        /// the invariant the directory format relies on when it records each chunk's address and length.
        /// </summary>
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public void AllocationsNeverSpanAPage()
        {
            var log = CreateLog();
            var random = new Random(1234);

            var scope = log.BeginScope();
            try
            {
                for (var ii = 0; ii < 2000; ++ii)
                {
                    var length = random.Next(1, PageSize / 3);
                    var logicalAddress = log.Allocate(length);
                    Assert.That(log.GetPage(logicalAddress), Is.EqualTo(log.GetPage(logicalAddress + length - 1)),
                        $"Allocation of {length} bytes at {logicalAddress} spans a page boundary");
                    Fill(log.AsSpan(logicalAddress, length), (byte)ii);
                }
            }
            finally
            {
                log.EndScope(scope);
            }
        }

        /// <summary>
        /// When the remainder of the page and the leftover are both at least <c>minPartialBytes</c>, the allocation is satisfied by
        /// the page remainder and the caller is told how much it actually got. This is how a chunk is cut at a page end.
        /// </summary>
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public void PartialAllocationIsCutAtThePageEnd()
        {
            var log = CreateLog();
            const int MinPartial = 64;
            const int Remaining = 500;

            var scope = log.BeginScope();
            try
            {
                // Fill the page up to exactly Remaining bytes before its end.
                var filler = log.RemainingOnPage(log.TailAddress) - Remaining;
                Assert.That(filler, Is.GreaterThan(0));
                _ = log.Allocate(filler);
                Assert.That(log.RemainingOnPage(log.TailAddress), Is.EqualTo(Remaining));

                var startPage = log.GetPage(log.TailAddress);
                var logicalAddress = log.Allocate(Remaining + MinPartial, MinPartial, out var allocatedLength);

                Assert.That(allocatedLength, Is.EqualTo(Remaining), "The allocation must be cut at the page end");
                Assert.That(log.GetPage(logicalAddress), Is.EqualTo(startPage));
                Assert.That(log.GetOffsetOnPage(logicalAddress + allocatedLength), Is.EqualTo(0).Or.EqualTo(PageSize),
                    "A cut allocation must end exactly at the page end");

                // The remainder is allocated separately and lands on the next page.
                var secondAddress = log.Allocate(MinPartial, MinPartial, out var secondLength);
                Assert.That(secondLength, Is.EqualTo(MinPartial));
                Assert.That(log.GetPage(secondAddress), Is.EqualTo(startPage + 1));
            }
            finally
            {
                log.EndScope(scope);
            }
        }

        /// <summary>
        /// A cut is only worth taking when both halves are useful, so when either side would fall below <c>minPartialBytes</c> the
        /// whole allocation moves to the next page and the page tail is wasted.
        /// </summary>
        [Test, Category(ObjectLogCategory)]
        public void PartialAllocationIsDeclinedWhenEitherSideWouldBeTooSmall([Values] bool remainderTooSmall)
        {
            var log = CreateLog();
            const int MinPartial = 256;

            // remainderTooSmall: the page tail is below MinPartial. Otherwise the tail is ample but what is left over is not.
            var remaining = remainderTooSmall ? MinPartial - 1 : MinPartial * 2;
            var requested = remainderTooSmall ? MinPartial * 2 : remaining + MinPartial - 1;

            var scope = log.BeginScope();
            try
            {
                var filler = log.RemainingOnPage(log.TailAddress) - remaining;
                _ = log.Allocate(filler);
                var startPage = log.GetPage(log.TailAddress);

                var logicalAddress = log.Allocate(requested, MinPartial, out var allocatedLength);

                Assert.That(allocatedLength, Is.EqualTo(requested), "The allocation must not be cut");
                Assert.That(log.GetPage(logicalAddress), Is.EqualTo(startPage + 1), "The whole allocation must move to the next page");
            }
            finally
            {
                log.EndScope(scope);
            }
        }
        #endregion

        #region Device reads
        /// <summary>
        /// Once a record falls below HeadAddress its page has been flushed and freed, so the read must come from the device. This is
        /// the common case in production: the object log is deliberately given far less memory than the data it holds.
        /// </summary>
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public void ReadFallsBackToTheDeviceBelowHeadAddress()
        {
            var log = CreateLog();
            const int RecordLength = 300;
            const int NumRecords = 200;     // far more than the 4-page ring holds

            var addresses = new List<long>(NumRecords);
            for (var ii = 0; ii < NumRecords; ++ii)
                addresses.Add(Write(log, RecordLength, (byte)ii));

            Assert.That(log.HeadAddress, Is.GreaterThan(addresses[0]),
                "The ring must have rolled far enough to evict the first records; otherwise this test is not reading from the device");

            // Read every record, oldest first, so the early ones exercise the device path and the last ones memory.
            var sawDeviceRead = false;
            for (var ii = 0; ii < NumRecords; ++ii)
            {
                sawDeviceRead |= addresses[ii] < log.HeadAddress;
                VerifyRead(log, addresses[ii], RecordLength, (byte)ii, $"record {ii}");
            }
            Assert.That(sawDeviceRead, Is.True);
        }

        /// <summary>Reads below HeadAddress but above BeginAddress stay readable after a flush, and the flush is observable.</summary>
        [Test, Category(ObjectLogCategory)]
        public void FlushTailMakesEverythingWrittenDurable()
        {
            var log = CreateLog();
            var addresses = new List<long>();
            for (var ii = 0; ii < 20; ++ii)
                addresses.Add(Write(log, 200, (byte)ii));

            var flushedThrough = log.FlushTail(wait: true);
            Assert.That(log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(flushedThrough));

            for (var ii = 0; ii < addresses.Count; ++ii)
                VerifyRead(log, addresses[ii], 200, (byte)ii, $"record {ii} after flush");
        }

        /// <summary>
        /// A flush can be requested without waiting and awaited later, which is what a durability deadline later than "now" needs: the
        /// writer issues the flush and something else drains it before the deadline.
        /// </summary>
        [Test, Category(ObjectLogCategory)]
        public void WaitForFlushDrainsAFlushThatWasNotWaitedOn()
        {
            var log = CreateLog();
            var addresses = new List<long>();
            for (var ii = 0; ii < 20; ++ii)
                addresses.Add(Write(log, 200, (byte)ii));

            var flushedThrough = log.FlushTail(wait: false);
            log.WaitForFlush(flushedThrough);

            Assert.That(log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(flushedThrough));
            for (var ii = 0; ii < addresses.Count; ++ii)
                VerifyRead(log, addresses[ii], 200, (byte)ii, $"record {ii} after drained flush");
        }

        /// <summary>Records each <see cref="ObjectLog.OnFlushed"/> callback invocation.</summary>
        sealed class FlushNotification
        {
            internal readonly ManualResetEventSlim Invoked = new(false);
            internal int Count;
            internal uint ErrorCode;
            internal Exception Exception;
            internal int ThreadId;

            internal static void Callback(uint errorCode, uint numBytes, object context, Exception exception)
            {
                var notification = (FlushNotification)context;
                notification.ErrorCode = errorCode;
                notification.Exception = exception;
                notification.ThreadId = Environment.CurrentManagedThreadId;
                _ = Interlocked.Increment(ref notification.Count);
                notification.Invoked.Set();
            }
        }

        static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(30);

        /// <summary>An address that is already durable completes the notification before <see cref="ObjectLog.OnFlushed"/> returns.</summary>
        [Test, Category(ObjectLogCategory)]
        public void OnFlushedCompletesAtOnceWhenAlreadyDurable()
        {
            var log = CreateLog();
            for (var ii = 0; ii < 20; ++ii)
                _ = Write(log, 200, (byte)ii);
            var flushedThrough = log.FlushTail(wait: true);

            var notification = new FlushNotification();
            log.OnFlushed(flushedThrough, FlushNotification.Callback, notification);

            Assert.That(notification.Count, Is.EqualTo(1), "an already-durable address must complete the notification at once");
            Assert.That(notification.ErrorCode, Is.Zero);
            Assert.That(notification.ThreadId, Is.EqualTo(Environment.CurrentManagedThreadId));
        }

        /// <summary>A notification registered while its flush is in flight completes, exactly once, when that flush reaches the device.</summary>
        [Test, Category(ObjectLogCategory)]
        public void OnFlushedCompletesWhenTheFlushReachesTheDevice()
        {
            var gated = new GatedCompletionDevice(device);
            var log = CreateLog(settings => settings.LogDevice = gated);
            for (var ii = 0; ii < 20; ++ii)
                _ = Write(log, 200, (byte)ii);

            gated.Gate = true;
            var flushedThrough = log.FlushTail(wait: false);
            gated.WaitForPending(1, FlushTimeout);

            var notification = new FlushNotification();
            log.OnFlushed(flushedThrough, FlushNotification.Callback, notification);
            Assert.That(notification.Count, Is.Zero, "the notification must wait for the flush");

            gated.Gate = false;
            for (var ii = 0; ii < gated.CapturedCount; ++ii)
                gated.Release(ii);

            Assert.That(notification.Invoked.Wait(FlushTimeout), Is.True, "the notification must complete once the flush is durable");
            Assert.That(notification.ErrorCode, Is.Zero);
            Assert.That(log.FlushedUntilAddress, Is.GreaterThanOrEqualTo(flushedThrough));
            Assert.That(notification.Count, Is.EqualTo(1));
        }

        /// <summary>A failed flush below the address fails the notification, and a later notification past the failure fails at once.</summary>
        [Test, Category(ObjectLogCategory)]
        public void OnFlushedReportsAFailedFlush()
        {
            const uint IoError = 5;
            var gated = new GatedCompletionDevice(device);
            var log = CreateLog(settings => settings.LogDevice = gated);
            for (var ii = 0; ii < 20; ++ii)
                _ = Write(log, 200, (byte)ii);

            gated.Gate = true;
            var flushedThrough = log.FlushTail(wait: false);
            gated.WaitForPending(1, FlushTimeout);

            var pending = new FlushNotification();
            log.OnFlushed(flushedThrough, FlushNotification.Callback, pending);

            gated.Gate = false;
            for (var ii = 0; ii < gated.CapturedCount; ++ii)
                gated.Release(ii, IoError);

            Assert.That(pending.Invoked.Wait(FlushTimeout), Is.True, "a failed flush must fail the notification rather than strand it");
            Assert.That(pending.ErrorCode, Is.EqualTo(IoError));
            Assert.That(pending.Exception, Is.InstanceOf<TsavoriteException>());
            Assert.That(pending.Count, Is.EqualTo(1));

            var late = new FlushNotification();
            log.OnFlushed(flushedThrough, FlushNotification.Callback, late);
            Assert.That(late.Count, Is.EqualTo(1), "a notification past a known failure must fail at once");
            Assert.That(late.ErrorCode, Is.EqualTo(IoError));
        }

        /// <summary>Disposing the log fails a notification whose flush can no longer complete, rather than stranding its caller.</summary>
        [Test, Category(ObjectLogCategory)]
        public void DisposeFailsPendingOnFlushedNotifications()
        {
            var log = CreateLog();
            for (var ii = 0; ii < 20; ++ii)
                _ = Write(log, 200, (byte)ii);
            var tailAddress = log.TailAddress;

            // No flush covers the tail, so only disposal can complete this notification.
            var notification = new FlushNotification();
            log.OnFlushed(tailAddress, FlushNotification.Callback, notification);
            Assert.That(notification.Count, Is.Zero);

            log.Dispose();

            Assert.That(notification.Count, Is.EqualTo(1));
            Assert.That(notification.ErrorCode, Is.Not.Zero);
            Assert.That(notification.Exception, Is.InstanceOf<ObjectDisposedException>());

            var late = new FlushNotification();
            log.OnFlushed(tailAddress, FlushNotification.Callback, late);
            Assert.That(late.Count, Is.EqualTo(1), "a notification on a disposed log must fail at once");
            Assert.That(late.Exception, Is.InstanceOf<ObjectDisposedException>());
        }

        [Test, Category(ObjectLogCategory)]
        public void ReadRejectsAnInvalidRange()
        {
            var log = CreateLog();
            var logicalAddress = Write(log, 100, seed: 3);
            var buffer = new byte[200];

            // Zero length, and a length longer than the destination.
            _ = Assert.Throws<TsavoriteException>(() => log.Read(logicalAddress, 0, buffer));
            _ = Assert.Throws<TsavoriteException>(() => log.Read(logicalAddress, buffer.Length + 1, buffer));

            // Past the tail.
            _ = Assert.Throws<TsavoriteException>(() => log.Read(log.TailAddress, 8, buffer));

            // Below BeginAddress.
            _ = Assert.Throws<TsavoriteException>(() => log.Read(log.BeginAddress - 1, 8, buffer));
        }

        /// <summary>A read is one device extent, so it cannot cross a page even though the addresses on both sides are valid.</summary>
        [Test, Category(ObjectLogCategory)]
        public void ReadRejectsARangeThatSpansAPage()
        {
            var log = CreateLog();
            var scope = log.BeginScope();
            long nearPageEnd;
            try
            {
                _ = log.Allocate(log.RemainingOnPage(log.TailAddress) - 16);
                nearPageEnd = log.Allocate(16);         // ends exactly at the page end
                _ = log.Allocate(16);                   // first bytes of the next page
            }
            finally
            {
                log.EndScope(scope);
            }

            var buffer = new byte[32];
            _ = Assert.Throws<TsavoriteException>(() => log.Read(nearPageEnd, 32, buffer));
        }
        #endregion

        #region Truncation and recovery
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public void TruncateRetiresEarlierAddresses()
        {
            var log = CreateLog();
            const int RecordLength = 256;
            var addresses = new List<long>();
            for (var ii = 0; ii < 100; ++ii)
                addresses.Add(Write(log, RecordLength, (byte)ii));
            _ = log.FlushTail(wait: true);

            // Truncate to a segment boundary well inside what we wrote.
            var newBeginAddress = 1L << SegmentSizeBits;
            Assert.That(newBeginAddress, Is.LessThan(log.TailAddress));
            log.Truncate(newBeginAddress);

            Assert.That(log.BeginAddress, Is.GreaterThanOrEqualTo(newBeginAddress));

            var buffer = new byte[RecordLength];
            _ = Assert.Throws<TsavoriteException>(() => log.Read(0, RecordLength, buffer));

            // Everything at or above the new begin address is still readable.
            for (var ii = 0; ii < addresses.Count; ++ii)
            {
                if (addresses[ii] >= log.BeginAddress)
                    VerifyRead(log, addresses[ii], RecordLength, (byte)ii, $"record {ii} after truncate");
            }
        }

        /// <summary>
        /// There is no commit protocol: the tail comes from checkpoint metadata, and <see cref="ObjectLog.RestoreTail"/> positions a
        /// fresh log there so appends resume after the recovered content rather than overwriting it.
        /// </summary>
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public void RestoreTailResumesAppendsAfterRecoveredContent()
        {
            const int RecordLength = 300;
            var addresses = new List<long>();
            long recoveredTail, recoveredBegin;

            var log = CreateLog();
            for (var ii = 0; ii < 40; ++ii)
                addresses.Add(Write(log, RecordLength, (byte)ii));
            recoveredTail = log.FlushTail(wait: true);
            recoveredBegin = log.BeginAddress;
            Assert.That(recoveredTail, Is.EqualTo(log.TailAddress));

            log.Dispose();
            objectLog = null;
            device.Dispose();

            // Reopen over the same file and position at the recorded tail.
            device = CreateDevice();
            log = CreateLog();
            log.RestoreTail(recoveredBegin, recoveredTail);
            Assert.That(log.TailAddress, Is.EqualTo(recoveredTail));

            // Pre-existing content reads back, and new content appends above it.
            for (var ii = 0; ii < addresses.Count; ++ii)
                VerifyRead(log, addresses[ii], RecordLength, (byte)ii, $"recovered record {ii}");

            var appended = Write(log, RecordLength, seed: 99);
            Assert.That(appended, Is.GreaterThanOrEqualTo(recoveredTail));
            VerifyRead(log, appended, RecordLength, seed: 99, "record appended after recovery");
        }

        /// <summary>
        /// Appending after recovery and then flushing must not destroy the recovered bytes sharing the tail page.
        /// </summary>
        /// <remarks>
        /// A flush writes whole sectors. If recovery leaves the partial tail page unloaded, the page buffer holds zeros below the
        /// recovered tail, and the first flush after recovery writes those zeros over the recovered bytes in the same sector. The
        /// records must therefore still read back correctly <i>after</i> a post-recovery flush, not merely before one.
        /// </remarks>
        [Test, Category(ObjectLogCategory), Category(SmokeTestCategory)]
        public void RecoveredContentSurvivesTheFirstFlushAfterRecovery()
        {
            const int RecordLength = 300;
            var addresses = new List<long>();

            var log = CreateLog();
            for (var ii = 0; ii < 40; ++ii)
                addresses.Add(Write(log, RecordLength, (byte)ii));
            var recoveredTail = log.FlushTail(wait: true);
            var recoveredBegin = log.BeginAddress;

            // The tail must land mid-page for this to be meaningful: the hazard is the partially filled page.
            Assert.That(log.GetOffsetOnPage(recoveredTail), Is.Not.EqualTo(0), "Test geometry must leave a partially filled tail page");

            log.Dispose();
            objectLog = null;
            device.Dispose();

            device = CreateDevice();
            log = CreateLog();
            log.RestoreTail(recoveredBegin, recoveredTail);

            // Append and flush, which rewrites the sector the recovered tail shares.
            _ = Write(log, RecordLength, seed: 99);
            _ = log.FlushTail(wait: true);

            // Every recovered record must still be intact.
            for (var ii = 0; ii < addresses.Count; ++ii)
                VerifyRead(log, addresses[ii], RecordLength, (byte)ii, $"recovered record {ii} after a post-recovery flush");
        }
        #endregion

        #region Address ceiling
        /// <summary>
        /// The real ceiling is billions of pages away, so the enforcement path is exercised through the test-only override. What is
        /// verified is the boundary: an allocation ending exactly at the ceiling is allowed, one byte past it is not.
        /// </summary>
        [Test, Category(ObjectLogCategory)]
        public void AllocationIsRejectedAtTheAddressCeiling()
        {
            const int Length = 64;
            var ceiling = (3L << PageSizeBits) + Length;    // some way into page 3, so nothing has to flush to reach it

            var log = CreateLog(settings => settings.MaxExclusiveAddressOverride = ceiling);
            Assert.That(log.MaxExclusiveAddress, Is.EqualTo(ceiling));

            var scope = log.BeginScope();
            try
            {
                // Walk up to the ceiling; the allocation that ends exactly on it must succeed.
                while (log.TailAddress + Length < ceiling)
                    _ = log.Allocate(Length);
                Assert.That(log.TailAddress + Length, Is.EqualTo(ceiling), "Test geometry must land exactly on the ceiling");
                _ = log.Allocate(Length);
                Assert.That(log.TailAddress, Is.EqualTo(ceiling));

                var ex = Assert.Throws<TsavoriteException>(() => _ = log.Allocate(1));
                Assert.That(ex.Message, Does.Contain("address space exhausted"));
            }
            finally
            {
                log.EndScope(scope);
            }
        }

        /// <summary>
        /// Two ceilings bound an address, and the allocator's 32-bit page number is the lower one at every page size this log allows.
        /// The 60-bit position ceiling would bind only at a 2^29 page, above <see cref="LogSettings.kMaxPageSizeBits"/>.
        /// </summary>
        [Test, Category(ObjectLogCategory)]
        public void TheAllocatorPageNumberIsAlwaysTheBindingCeiling(
            [Values(LogSettings.kMinPageSizeBits, ObjectLog.DefaultPageSizeBits, LogSettings.kMaxPageSizeBits)] int pageSizeBits)
        {
            var positionCeiling = (long)ObjectLogPositionInfo.AddressMask;
            var ceiling = ObjectLog.ComputeMaxExclusiveAddress(pageSizeBits, ObjectLog.DefaultPageCount);

            Assert.That(ceiling, Is.LessThan(positionCeiling), "The page-number ceiling must bind before the position-word ceiling");
        }

        /// <summary>
        /// The ceiling must leave headroom above the highest allocatable page. The allocator computes <c>pageIndex + 1</c> and
        /// <c>BufferSize + GetPage(...)</c> on signed 32-bit page numbers while deciding whether a page can be reused; near
        /// <see cref="int.MaxValue"/> those additions overflow and misclassify a page, inside the allocator where the ceiling check
        /// cannot observe it.
        /// </summary>
        [Test, Category(ObjectLogCategory)]
        public void TheCeilingReservesHeadroomForTheAllocatorsPageArithmetic(
            [Values(LogSettings.kMinPageSizeBits, ObjectLog.DefaultPageSizeBits, LogSettings.kMaxPageSizeBits)] int pageSizeBits,
            [Values(ObjectLog.MinPageCount, ObjectLog.DefaultPageCount, 1024)] int pageCount)
        {
            var ceiling = ObjectLog.ComputeMaxExclusiveAddress(pageSizeBits, pageCount);
            var lastAllowedPage = (ceiling - 1) >> pageSizeBits;
            var bufferSize = (long)Utility.NextPowerOf2(pageCount);

            Assert.That(lastAllowedPage + bufferSize + 1, Is.LessThan(int.MaxValue),
                "page + BufferSize and pageIndex + 1 must both stay within a signed 32-bit page number");
        }

        /// <summary>
        /// A rejected allocation must not leave the tail past the ceiling. The allocator advances its tail before returning an
        /// address, so a check that only ran afterwards would fail the call yet still corrupt the tail that a later checkpoint
        /// records as the object-log tail.
        /// </summary>
        [Test, Category(ObjectLogCategory)]
        public void ARejectedAllocationLeavesTheTailAtOrBelowTheCeiling()
        {
            const int Length = 64;
            var ceiling = (3L << PageSizeBits) + Length;

            var log = CreateLog(settings => settings.MaxExclusiveAddressOverride = ceiling);
            var scope = log.BeginScope();
            try
            {
                while (log.TailAddress + Length < ceiling)
                    _ = log.Allocate(Length);
                _ = log.Allocate(Length);
                Assert.That(log.TailAddress, Is.EqualTo(ceiling));

                _ = Assert.Throws<TsavoriteException>(() => _ = log.Allocate(1));
                Assert.That(log.TailAddress, Is.LessThanOrEqualTo(ceiling),
                    "A rejected allocation must not advance the tail past the ceiling");
            }
            finally
            {
                log.EndScope(scope);
            }
        }
        #endregion
    }
}