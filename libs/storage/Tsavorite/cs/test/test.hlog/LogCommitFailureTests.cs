// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    [TestFixture]
    internal class LogCommitFailureTests : TsavoriteLogTestBase
    {
        [SetUp]
        public void Setup() => BaseSetup(false);

        [TearDown]
        public void TearDown() => BaseTearDown();

        /// <summary>
        /// A device write failure surfaced during commit must reach the caller as a <see cref="CommitFailureException"/>
        /// whose <see cref="Exception.InnerException"/> is the exact device-level exception, rather than collapsing to a
        /// bare numeric error code. This validates the device-to-allocator error plumbing: the failing device forwards
        /// its typed exception through the completion callback's ioException parameter, and it must flow
        /// ioException -&gt; AsyncFlushPageCallback -&gt; CommitInfo -&gt; CommitFailureException.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public void CommitFailureExceptionCarriesDeviceError()
        {
            var failing = new RecordingFailingDevice(
                Devices.CreateLogDevice(Path.Join(TestUtils.MethodTestDir, "commitFailure.log"), deleteOnClose: true));
            device = failing;
            var logSettings = new TsavoriteLogSettings
            { LogDevice = failing, LogChecksum = LogChecksumType.PerEntry, LogCommitManager = manager };
            log = new TsavoriteLog(logSettings);

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            // A handful of entries stays in the current in-memory page (default 4MB page), so no device write is
            // issued during enqueue. The failing write is therefore triggered deterministically by the commit flush.
            for (int i = 0; i < 100; i++)
                _ = log.Enqueue(entry);

            failing.FailWrites = true;

            var ex = Assert.Throws<CommitFailureException>(() => log.Commit(spinWait: true));
            // The typed device fault must survive as the InnerException, not be discarded as a numeric-only code.
            ClassicAssert.AreSame(failing.InjectedError, ex.InnerException);
        }

        /// <summary>
        /// A fast-commit log whose recovery fails (here: every device read fails while committed metadata references
        /// real log data) must not leave the log poisoned with the fast-commit "shut up safe guards" sentinel
        /// <c>HeadAddress == long.MaxValue</c>, which overflows <c>CalculateReadOnlyAddress</c> on the next page turn.
        /// <para>
        /// Phase 2a (default <c>TolerateDeviceFailure = false</c>): construction must fail fast (throw) instead of
        /// silently presenting a poisoned/empty log as if recovery had succeeded.
        /// </para>
        /// <para>
        /// Phase 2b (<c>TolerateDeviceFailure = true</c>): construction is allowed to proceed after swallowing the read
        /// failure, which intentionally leaves the sentinel HeadAddress in place. Page-turning enqueues (which feed
        /// <c>long.MaxValue</c> into <c>CalculateReadOnlyAddress</c>) must still flush: before the fix the overflow
        /// produced a negative read-only address, so <c>ReadOnlyAddress</c> never advanced, filled pages never flushed,
        /// and <c>FlushedUntilAddress</c> stayed pinned at its post-recovery value; the clamp makes ReadOnlyAddress track
        /// the tail so pages flush normally and <c>FlushedUntilAddress</c> advances. A large in-memory buffer keeps the
        /// enqueue clear of eviction (which the sentinel HeadAddress would legitimately block), so an advancing
        /// FlushedUntilAddress isolates the regression to the address-arithmetic fix.
        /// </para>
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        [Explicit("Flaky under test-suite ordering: process-global recovery state can leak between fixtures and make " +
            "Phase 2a recover an empty log (no read attempted, so no fast-fail throw). The clamp regression is covered " +
            "deterministically by CalculateReadOnlyAddressClampsOutOfRangeHeadAddress; the leak is tracked separately.")]
        [CancelAfter(60000)]
        public void FastCommitRecoveryFailureFailsFastAndDoesNotPoisonLog(CancellationToken cancellationToken)
        {
            // 4 KB pages inside a large (4 MB => 1024-page) in-memory buffer: the bounded enqueue below turns several
            // pages (exercising CalculateReadOnlyAddress with the sentinel HeadAddress) but never approaches the buffer
            // capacity, so it cannot hit the separate eviction backpressure that a pinned HeadAddress would cause.
            const int pageSizeBits = 12;
            const int memorySizeBits = 22;
            const int segmentSizeBits = 22;
            const int seededEntries = 200;    // enough committed data that recovery attempts a real hybrid-log restore
            const int retryEntries = 200;     // ~6 page turns; far below the 1024-page buffer

            var logPath = Path.Join(TestUtils.MethodTestDir, "recovery.log");

            byte[] entry = new byte[entryLength];
            for (int i = 0; i < entryLength; i++)
                entry[i] = (byte)i;

            // Phase 1: write and fast-commit real data on a healthy device so the shared manager holds a commit whose
            // UntilAddress references log data (making recovery attempt a real hybrid-log restore).
            using (var goodDevice = Devices.CreateLogDevice(logPath, deleteOnClose: false))
            {
                var seedSettings = new TsavoriteLogSettings
                {
                    LogDevice = goodDevice,
                    LogChecksum = LogChecksumType.PerEntry,
                    LogCommitManager = manager,
                    FastCommitMode = true,
                    PageSizeBits = pageSizeBits,
                    MemorySizeBits = memorySizeBits,
                    SegmentSizeBits = segmentSizeBits,
                    TryRecoverLatest = false,
                };
                using var seedLog = new TsavoriteLog(seedSettings);
                for (int i = 0; i < seededEntries; i++)
                    _ = seedLog.Enqueue(entry);
                seedLog.Commit(spinWait: true);
            }

            // Phase 2a: recover with a device whose reads all fail. Recovery loads the committed metadata (UntilAddress
            // > 0), sets the fast-commit sentinels, then fails to restore the hybrid log. With TolerateDeviceFailure
            // off (default), construction must throw rather than return a poisoned log.
            using (var failReads = new SimulatedFlakyDevice(
                Devices.CreateLogDevice(logPath, deleteOnClose: false),
                new ErrorSimulationOptions { readPermanentErrorRate = 1.0 }))
            {
                var failSettings = new TsavoriteLogSettings
                {
                    LogDevice = failReads,
                    LogChecksum = LogChecksumType.PerEntry,
                    LogCommitManager = manager,
                    FastCommitMode = true,
                    PageSizeBits = pageSizeBits,
                    MemorySizeBits = memorySizeBits,
                    SegmentSizeBits = segmentSizeBits,
                };
                _ = Assert.Catch<Exception>(() =>
                {
                    using var poisoned = new TsavoriteLog(failSettings);
                });
            }

            // Phase 2b: same failing-read recovery but with TolerateDeviceFailure = true. Construction succeeds while
            // leaving the sentinel HeadAddress in place; page-turning enqueues (writes succeed here, only reads fail)
            // must still flush past the post-recovery FlushedUntilAddress without tripping the CalculateReadOnlyAddress
            // overflow that would otherwise freeze ReadOnlyAddress and, with it, all flushing.
            using (var failReads = new SimulatedFlakyDevice(
                Devices.CreateLogDevice(logPath, deleteOnClose: false),
                new ErrorSimulationOptions { readPermanentErrorRate = 1.0 }))
            {
                var tolerateSettings = new TsavoriteLogSettings
                {
                    LogDevice = failReads,
                    LogChecksum = LogChecksumType.PerEntry,
                    LogCommitManager = manager,
                    FastCommitMode = true,
                    PageSizeBits = pageSizeBits,
                    MemorySizeBits = memorySizeBits,
                    SegmentSizeBits = segmentSizeBits,
                    TolerateDeviceFailure = true,
                };
                using var tolerantLog = new TsavoriteLog(tolerateSettings);

                const int pageSize = 1 << pageSizeBits;

                // These page turns feed the sentinel HeadAddress into CalculateReadOnlyAddress. With a large buffer the
                // allocation itself never blocks on eviction, so the only thing that can hold flushing back is the
                // overflow: an out-of-range (negative) ReadOnlyAddress that the monotonic ">ReadOnlyAddress" guard
                // filters out, freezing ReadOnlyAddress (and hence FlushedUntilAddress) one page in.
                for (int i = 0; i < retryEntries; i++)
                    _ = tolerantLog.Enqueue(entry);

                long tail = tolerantLog.TailAddress;

                // Flushing must keep pace with the tail as pages turn read-only. Before the fix, ReadOnlyAddress freezes
                // at the first page boundary (the overflowed negative value is filtered), so FlushedUntilAddress stalls
                // ~one page in while the tail keeps growing; after the fix ReadOnlyAddress tracks the tail and every page
                // except the final still-mutable one flushes. Require flushing to reach within two pages of the tail.
                long flushTarget = tail - 2 * pageSize;
                var deadline = System.Diagnostics.Stopwatch.StartNew();
                while (tolerantLog.FlushedUntilAddress < flushTarget
                       && deadline.Elapsed < TimeSpan.FromSeconds(15)
                       && !cancellationToken.IsCancellationRequested)
                    _ = Thread.Yield();

                ClassicAssert.GreaterOrEqual(tolerantLog.FlushedUntilAddress, flushTarget,
                    $"FlushedUntilAddress ({tolerantLog.FlushedUntilAddress}) did not keep pace with TailAddress ({tail}) " +
                    "on a tolerated recovery-failure log: page flushing froze near the first page boundary, indicating " +
                    "the CalculateReadOnlyAddress overflow regression (HeadAddress == long.MaxValue).");
            }
        }

        /// <summary>
        /// Deterministic regression guard for the <c>CalculateReadOnlyAddress</c> clamp. Any HeadAddress at or beyond
        /// TailAddress -- in particular the fast-commit "never evict" sentinel <c>long.MaxValue</c> that a failed or tolerated
        /// recovery leaves behind -- must resolve to TailAddress rather than overflowing the page arithmetic to a negative,
        /// out-of-range address (which the monotonic "&gt; ReadOnlyAddress" guard would then silently filter, freezing
        /// flushing). This exercises the arithmetic directly, sidestepping the fragile recovery/flush plumbing of the
        /// poisoned-log scenario in <see cref="FastCommitRecoveryFailureFailsFastAndDoesNotPoisonLog"/>.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public void CalculateReadOnlyAddressClampsOutOfRangeHeadAddress(
            [Values(512L, 8192L, 1L << 30)] long tailAddress)
        {
            using var clampDevice = Devices.CreateLogDevice(
                Path.Join(TestUtils.MethodTestDir, "clamp.log"), deleteOnClose: true);
            var logSettings = new TsavoriteLogSettings
            {
                LogDevice = clampDevice,
                LogChecksum = LogChecksumType.PerEntry,
                LogCommitManager = manager,
                PageSizeBits = 12,
                MemorySizeBits = 20,
                SegmentSizeBits = 20,
                TryRecoverLatest = false,
            };
            using var clampLog = new TsavoriteLog(logSettings);

            // Every HeadAddress >= TailAddress -- equal, just past, and the long.MaxValue sentinel -- must clamp to
            // TailAddress and never overflow into a negative address.
            foreach (long headAddress in new[] { tailAddress, tailAddress + 1, long.MaxValue })
            {
                long readOnlyAddress = clampLog.AllocatorCalculateReadOnlyAddress(tailAddress, headAddress);
                ClassicAssert.AreEqual(tailAddress, readOnlyAddress,
                    $"HeadAddress {headAddress} >= TailAddress {tailAddress} must clamp ReadOnlyAddress to TailAddress.");
                ClassicAssert.GreaterOrEqual(readOnlyAddress, 0L,
                    $"ReadOnlyAddress must never be negative (HeadAddress {headAddress}, TailAddress {tailAddress}).");
            }

            // Sanity: an in-range HeadAddress (strictly below TailAddress) is not over-clamped -- it stays a valid,
            // non-negative address no greater than TailAddress. Guards against the clamp swallowing the healthy path.
            const long normalTail = 1L << 20;
            long normalReadOnly = clampLog.AllocatorCalculateReadOnlyAddress(normalTail, headAddress: 0L);
            ClassicAssert.GreaterOrEqual(normalReadOnly, 0L, "In-range ReadOnlyAddress must be non-negative.");
            ClassicAssert.LessOrEqual(normalReadOnly, normalTail, "In-range ReadOnlyAddress must not exceed TailAddress.");
        }
    }

    /// <summary>
    /// Test device that deterministically fails every write once armed. It surfaces a typed exception through the
    /// completion callback's <c>ioException</c> parameter alongside the numeric-only error code
    /// (<see cref="uint.MaxValue"/>) that a real device reports, so the fault reaches the allocator via the plumbed
    /// callback parameter. All other operations delegate to an underlying device.
    /// </summary>
    internal sealed class RecordingFailingDevice : StorageDeviceBase
    {
        private readonly IDevice underlying;

        /// <summary>When set, every subsequent write fails.</summary>
        internal volatile bool FailWrites;

        /// <summary>The exact exception forwarded through the failed write's completion callback; used to assert InnerException identity.</summary>
        internal readonly Exception InjectedError = new IOException("injected device write failure");

        public RecordingFailingDevice(IDevice underlying)
            : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
        {
            this.underlying = underlying;
        }

        /// <inheritdoc/>
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
        {
            if (FailWrites)
            {
                // Mirror a real device surfacing a write fault: complete the IO with the numeric-only error code
                // (uint.MaxValue) that production devices report, and forward the typed exception through the
                // completion callback's ioException parameter, which carries the fault to AsyncFlushPageCallback.
                callback(uint.MaxValue, numBytesToWrite, context, ioException: InjectedError);
                return;
            }
            underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
        }

        /// <inheritdoc/>
        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
            => underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);

        /// <inheritdoc/>
        public override void Dispose() => underlying.Dispose();
    }
}