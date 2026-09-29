// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// Covers <c>BufferAndLoad</c>'s frame claim when issuing the page read fails. The claim is published to
    /// <c>pendingDrainCallbacks</c> before the read is deferred onto the epoch's drain list, so every failure path
    /// must release it exactly once and leave the frame reusable.
    /// </summary>
    /// <remarks>
    /// <see cref="LightEpoch.BumpCurrentEpoch(Action)"/> runs other threads' drain actions, so one of them throwing can
    /// surface either before or after the page-read action is registered, and the caller cannot tell which. Which
    /// epoch call observes the throw is timing dependent, so these tests assert the invariants that hold in every
    /// ordering rather than a particular one.
    /// </remarks>
    [TestFixture]
    [NonParallelizable]
    internal class ScanIteratorEpochFailureTests
    {
        private const int LogPageSizeBits = 12;
        private const int MaxDrainAttempts = 8;
        private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(60);

        /// <summary>
        /// How long a re-claim must stay blocked to show it is waiting on the prior read-ahead. Without the wait the
        /// re-claim issues its read immediately, so this only has to exceed scheduling noise.
        /// </summary>
        private static readonly TimeSpan ReclaimObservationWindow = TimeSpan.FromMilliseconds(500);

        /// <summary>Minimal <see cref="IAllocator"/> for the address arithmetic the iterator performs.</summary>
        private struct StubAllocator : IAllocator
        {
            public readonly int OverflowPageCount => 0;
            public readonly void PopulateRecordSizeInfo(ref RecordSizeInfo sizeInfo) => throw new NotSupportedException();
            public readonly void AllocatePage(int pageIndex) => throw new NotSupportedException();
            public readonly void FreePage(long pageIndex) => throw new NotSupportedException();
            public readonly long GetPageOfAddress(long logicalAddress, int logPageSizeBits) => logicalAddress >> logPageSizeBits;
        }

        /// <summary>Iterator that records page-read issuance and exposes the members the tests drive.</summary>
        private sealed class StubScanIterator : ScanIteratorBase<StubAllocator>, IDisposable
        {
            private int readCallCount;
            private readonly ConcurrentDictionary<long, CountdownEvent> heldCompletions = new();
            private readonly HashSet<CountdownEvent> distinctCompletionEvents = [];

            /// <summary>When set, issuing the page read throws, as a device that fails before the read is submitted does.</summary>
            public Exception ThrowOnRead;

            /// <summary>When set, a page read is reported as still in flight until <see cref="CompleteRead"/> is called
            /// for it, as a real device does between submission and its completion callback.</summary>
            public bool HoldCompletions;

            public StubScanIterator(LightEpoch epoch, DiskScanBufferingMode scanBufferingMode = DiskScanBufferingMode.SinglePageBuffering)
                : base(beginAddress: 0, endAddress: long.MaxValue, scanBufferingMode,
                       InMemoryScanBufferingMode.NoBuffering, includeClosedRecords: false, epoch, LogPageSizeBits, new StubAllocator())
            { }

            public int PendingDrainCallbacks => Volatile.Read(ref pendingDrainCallbacks);

            public int ReadCallCount => Volatile.Read(ref readCallCount);

            public int FrameSize => frameSize;

            /// <summary>Number of distinct completion event instances handed to page reads, by reference.</summary>
            public int DistinctCompletionEventCount
            {
                get
                {
                    lock (distinctCompletionEvents)
                        return distinctCompletionEvents.Count;
                }
            }

            public bool ClaimFrameAndIssueRead(long page)
                => BufferAndLoad(currentIterationAddress: page << LogPageSizeBits, currentPage: page, currentFrame: page % frameSize,
                                 headAddress: long.MaxValue, endIterationAddress: long.MaxValue);

            /// <summary>Delivers the completion of a held page read, as the device's callback would.</summary>
            public void CompleteRead(long readPage)
            {
                if (!heldCompletions.TryRemove(readPage, out var completed))
                    return;
                if (!completed.IsSet)
                    _ = completed.Signal();
                _ = Interlocked.Decrement(ref pendingDrainCallbacks);
            }

            /// <summary>Delivers every outstanding held completion, so Dispose is not left waiting on them.</summary>
            public void CompleteAllReads()
            {
                foreach (var readPage in heldCompletions.Keys)
                    CompleteRead(readPage);
            }

            /// <summary>Blocks until <paramref name="count"/> page reads have been issued.</summary>
            public void WaitForReadCount(int count, TimeSpan timeout)
            {
                var deadline = DateTime.UtcNow + timeout;
                var spinWait = new SpinWait();
                while (ReadCallCount < count)
                {
                    if (DateTime.UtcNow >= deadline)
                        throw new TimeoutException($"Only {ReadCallCount} of {count} page reads were issued");
                    spinWait.SpinOnce();
                }
            }

            internal override void AsyncReadPageFromDeviceToFrame<TContext>(CircularDiskReadBuffer readBuffers, long readPage, long untilAddress, TContext context,
                    ref CountdownEvent completed, long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            {
                if (ThrowOnRead is not null)
                {
                    _ = Interlocked.Increment(ref readCallCount);
                    throw ThrowOnRead;
                }

                // Reuse the frame's event exactly as the allocator does, so the tests exercise that lifetime too.
                if (completed is null)
                    completed = new CountdownEvent(1);
                else
                    completed.Reset();

                lock (distinctCompletionEvents)
                    _ = distinctCompletionEvents.Add(completed);

                if (HoldCompletions)
                {
                    // Publish the event before the read is counted, so a test that waits on the count can always
                    // resolve the read it just observed.
                    heldCompletions[readPage] = completed;
                    _ = Interlocked.Increment(ref readCallCount);
                    return;
                }

                // Report the load as already complete so the caller does not wait on a device that does not exist.
                _ = Interlocked.Increment(ref readCallCount);
                _ = completed.Signal();
                _ = Interlocked.Decrement(ref pendingDrainCallbacks);
            }
        }

        /// <summary>
        /// Runs <paramref name="body"/> on a dedicated thread and fails the test if it does not finish, so a frame left
        /// claimed surfaces as a failure rather than stalling the run.
        /// </summary>
        private static void RunBounded(Action body)
        {
            Exception failure = null;
            var thread = new Thread(() =>
            {
                try
                {
                    body();
                }
                catch (Exception ex)
                {
                    failure = ex;
                }
            })
            { IsBackground = true };

            thread.Start();
            if (!thread.Join(TestTimeout))
                Assert.Fail($"test body did not complete within {TestTimeout.TotalSeconds} seconds; a frame was left claimed");
            if (failure is not null)
                ExceptionDispatchInfo.Capture(failure).Throw();
        }

        /// <summary>
        /// Runs whatever the epoch still has queued, tolerating a queued action that throws. A throwing action aborts
        /// the rest of the pass, so drain until a pass completes.
        /// </summary>
        private static void DrainIgnoringQueuedFailures(LightEpoch epoch)
        {
            for (var attempt = 0; attempt < MaxDrainAttempts; attempt++)
            {
                try
                {
                    epoch.ProtectAndDrain();
                    return;
                }
                catch (InvalidOperationException)
                {
                }
            }
        }

        /// <summary>
        /// Runs whatever the epoch still has queued, then disposes the iterator and the epoch. The iterator is
        /// disposed while the epoch is still held, so it can drain the actions holding its claims.
        /// </summary>
        private static void DrainAndDispose(LightEpoch epoch, StubScanIterator iterator)
        {
            DrainIgnoringQueuedFailures(epoch);
            iterator.Dispose();
            epoch.Suspend();
            epoch.Dispose();
        }

        /// <summary>
        /// Registers <paramref name="poison"/> as a drain action from a thread that then leaves, so it stays queued
        /// until some later drain reclaims its epoch.
        /// </summary>
        private static void QueuePoisonFromOtherThread(LightEpoch epoch, Exception poison)
        {
            var thread = new Thread(() =>
            {
                epoch.Resume();
                try
                {
                    epoch.BumpCurrentEpoch(() => throw poison);
                }
                catch (InvalidOperationException)
                {
                    // The bump drained its own action. Which drain observes the poison is timing dependent.
                }
                epoch.Suspend();
            })
            { IsBackground = true };

            thread.Start();
            ClassicAssert.IsTrue(thread.Join(TestTimeout), "helper thread did not finish");
        }

        /// <summary>
        /// A drain action that throws while the epoch is being bumped for a page read must leave the frame's claim
        /// released exactly once, whether it throws before or after the page-read action is registered.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public void FrameClaimIsReleasedWhenEpochBumpThrows(
                [Values(DiskScanBufferingMode.SinglePageBuffering, DiskScanBufferingMode.DoublePageBuffering)] DiskScanBufferingMode scanBufferingMode)
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();

                // Construct while unprotected; the iterator only adopts the epoch when the constructing thread is not
                // already holding it.
                var iterator = new StubScanIterator(epoch, scanBufferingMode);

                var poison = new InvalidOperationException("poison drain action");

                epoch.Resume();
                try
                {
                    QueuePoisonFromOtherThread(epoch, poison);

                    // Which drain observes the poison is timing dependent, so tolerate it surfacing elsewhere. What
                    // must hold in every ordering is the accounting checked below.
                    Exception thrown = null;
                    try
                    {
                        _ = iterator.ClaimFrameAndIssueRead(page: 0);
                    }
                    catch (Exception ex)
                    {
                        thrown = ex;
                    }

                    if (thrown is not null)
                        ClassicAssert.AreSame(poison, thrown, "the drain failure must reach the scanning thread unchanged");

                    // The page-read action may still be queued, so drain before checking the claim. It must be
                    // released once: never twice, which drives the count negative, and never left outstanding, which
                    // stalls Dispose.
                    DrainIgnoringQueuedFailures(epoch);
                    ClassicAssert.AreEqual(0, iterator.PendingDrainCallbacks, "the frame's claim must be released exactly once");
                    ClassicAssert.LessOrEqual(iterator.ReadCallCount, iterator.FrameSize, "the abandoned read must not be issued twice");

                    // Draining again must not repeat the release.
                    DrainIgnoringQueuedFailures(epoch);
                    ClassicAssert.AreEqual(0, iterator.PendingDrainCallbacks);

                    // The frame is reusable rather than claimed forever, so a later pass over the same page completes
                    // instead of spinning.
                    try
                    {
                        _ = iterator.ClaimFrameAndIssueRead(page: 0);
                    }
                    catch (Exception ex) when (ex is OperationCanceledException or TsavoriteException or InvalidOperationException)
                    {
                    }
                    ClassicAssert.AreEqual(0, iterator.PendingDrainCallbacks);
                }
                finally
                {
                    DrainAndDispose(epoch, iterator);
                }
            });
        }

        /// <summary>
        /// Drives the ordering in which the poison throws before the page-read action is registered, which is the
        /// ordering that leaves nothing queued to release the frame's claim. <see cref="LightEpoch.BumpCurrentEpoch()"/>
        /// drains before the caller reaches the drain-list slot scan, so the claim survives only if the scanning
        /// thread repairs the frame itself.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public void FrameClaimIsReleasedWhenEpochBumpThrowsBeforeRegistration(
                [Values(DiskScanBufferingMode.SinglePageBuffering, DiskScanBufferingMode.DoublePageBuffering)] DiskScanBufferingMode scanBufferingMode)
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();
                var iterator = new StubScanIterator(epoch, scanBufferingMode);
                var poison = new InvalidOperationException("poison drain action");

                // A second protected thread keeps SafeToReclaimEpoch below the poison's epoch while it is queued, so
                // the queueing thread cannot drain it.
                using var holderProtected = new ManualResetEventSlim();
                using var releaseHolder = new ManualResetEventSlim();
                var holder = new Thread(() =>
                {
                    epoch.Resume();
                    holderProtected.Set();
                    releaseHolder.Wait();
                    epoch.Suspend();
                })
                { IsBackground = true };

                holder.Start();
                ClassicAssert.IsTrue(holderProtected.Wait(TestTimeout), "holder thread did not acquire the epoch");

                epoch.Resume();
                try
                {
                    QueuePoisonFromOtherThread(epoch, poison);
                    ClassicAssert.AreEqual(0, iterator.ReadCallCount, "queueing the poison must not have run a page read");

                    // Advance past the poison's epoch while the holder still pins SafeToReclaimEpoch, so this drain
                    // leaves the poison queued.
                    epoch.ProtectAndDrain();

                    releaseHolder.Set();
                    ClassicAssert.IsTrue(holder.Join(TestTimeout), "holder thread did not release the epoch");

                    // This thread is now the only protected one and is past the poison's epoch, so the bump taken for
                    // the page read drains the poison before registering its own action.
                    var thrown = Assert.Catch(() => iterator.ClaimFrameAndIssueRead(page: 0));
                    ClassicAssert.AreSame(poison, thrown, "the drain failure must reach the scanning thread unchanged");

                    Assume.That(iterator.ReadCallCount, Is.Zero, "the poison did not surface before the page-read action was registered");
                    ClassicAssert.AreEqual(0, iterator.PendingDrainCallbacks, "the frame's claim must be released by the scanning thread");
                }
                finally
                {
                    releaseHolder.Set();
                    DrainAndDispose(epoch, iterator);
                }
            });
        }

        /// <summary>
        /// A device that throws while the page read is being issued fails inside the deferred drain action, where
        /// nothing may escape. The claim must still be released and the frame left reusable.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public void FrameClaimIsReleasedWhenIssuingTheReadThrows(
                [Values(DiskScanBufferingMode.SinglePageBuffering, DiskScanBufferingMode.DoublePageBuffering)] DiskScanBufferingMode scanBufferingMode)
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();
                var iterator = new StubScanIterator(epoch, scanBufferingMode) { ThrowOnRead = new InvalidOperationException("device failed to issue read") };

                epoch.Resume();
                try
                {
                    // The read is skipped rather than retried, so the claim is released and the wait is cancelled.
                    _ = Assert.Catch(() => iterator.ClaimFrameAndIssueRead(page: 0));

                    ClassicAssert.AreEqual(iterator.FrameSize, iterator.ReadCallCount, "every frame's read must have been attempted");
                    ClassicAssert.AreEqual(0, iterator.PendingDrainCallbacks, "the frame's claim must be released exactly once");

                    epoch.ProtectAndDrain();
                    ClassicAssert.AreEqual(0, iterator.PendingDrainCallbacks);
                }
                finally
                {
                    DrainAndDispose(epoch, iterator);
                }
            });
        }

        /// <summary>
        /// The failure handling must not disturb a page read that is issued normally.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public void FrameLoadSucceedsWhenEpochBumpDoesNotThrow(
                [Values(DiskScanBufferingMode.SinglePageBuffering, DiskScanBufferingMode.DoublePageBuffering)] DiskScanBufferingMode scanBufferingMode)
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();
                var iterator = new StubScanIterator(epoch, scanBufferingMode);

                epoch.Resume();
                try
                {
                    _ = iterator.ClaimFrameAndIssueRead(page: 0);
                    epoch.ProtectAndDrain();

                    ClassicAssert.AreEqual(iterator.FrameSize, iterator.ReadCallCount);
                    ClassicAssert.AreEqual(0, iterator.PendingDrainCallbacks);
                }
                finally
                {
                    DrainAndDispose(epoch, iterator);
                }
            });
        }

        /// <summary>
        /// Each frame keeps one completion event for its lifetime, reset per page read, so the number of events a scan
        /// creates is bounded by the frame count rather than the number of pages it reads. Dispose disposes each
        /// frame's event, so this also bounds the kernel wait handles the scan holds.
        /// </summary>
        [Test]
        [Category("TsavoriteLog")]
        public void FrameLoadCompletionEventIsReusedAcrossPageLoads(
                [Values(DiskScanBufferingMode.SinglePageBuffering, DiskScanBufferingMode.DoublePageBuffering)] DiskScanBufferingMode scanBufferingMode)
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();
                var iterator = new StubScanIterator(epoch, scanBufferingMode);

                const int pageCount = 6;

                epoch.Resume();
                try
                {
                    for (var page = 0; page < pageCount; page++)
                        _ = iterator.ClaimFrameAndIssueRead(page);
                    epoch.ProtectAndDrain();

                    ClassicAssert.GreaterOrEqual(iterator.ReadCallCount, pageCount, "every page must have been read");
                    ClassicAssert.AreEqual(iterator.FrameSize, iterator.DistinctCompletionEventCount,
                        $"{iterator.ReadCallCount} page reads must share {iterator.FrameSize} completion event(s), one per frame");
                    ClassicAssert.AreEqual(0, iterator.PendingDrainCallbacks);
                }
                finally
                {
                    DrainAndDispose(epoch, iterator);
                }
            });
        }

        /// <summary>
        /// Only currentFrame is awaited by <c>BufferAndLoad</c>, so a read-ahead issued into the other frame can still
        /// be in flight when the scan next maps a page back to it — which happens when the scan skips past the
        /// prefetched page, as advancing BeginAddress makes it do. Re-issuing then would put two device reads on one
        /// buffer and orphan the completion event being replaced, so the claim must wait for the prior load.
        /// </summary>
        /// <remarks>
        /// Unreachable while frameSize is 1: nextFrame is then always currentFrame, which is awaited before every
        /// return, so this is specific to read-ahead.
        /// </remarks>
        [Test]
        [Category("TsavoriteLog")]
        public void FrameReclaimWaitsForAnInFlightReadAhead()
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();
                var iterator = new StubScanIterator(epoch, DiskScanBufferingMode.DoublePageBuffering) { HoldCompletions = true };
                ClassicAssert.AreEqual(2, iterator.FrameSize, "this test covers read-ahead");

                // The claims block on their own frame, so they run off-thread while this thread delivers completions.
                // Each claim thread holds the epoch across the call, as a scanning thread does.
                Exception claimFailure = null;
                Thread runClaim(long page) => new(() =>
                {
                    epoch.Resume();
                    try
                    {
                        _ = iterator.ClaimFrameAndIssueRead(page);
                    }
                    catch (Exception ex)
                    {
                        claimFailure = ex;
                    }
                    finally
                    {
                        epoch.Suspend();
                    }
                })
                { IsBackground = true };

                Thread first = null, second = null;
                try
                {
                    // Pages 0 and 1 are claimed into frames 0 and 1; only frame 0 is awaited, so page 1's read-ahead
                    // is still in flight once this returns.
                    first = runClaim(0);
                    first.Start();
                    iterator.WaitForReadCount(2, TestTimeout);
                    iterator.CompleteRead(0);
                    ClassicAssert.IsTrue(first.Join(TestTimeout), "the first claim did not complete");

                    // Page 5 maps back to frame 1, whose page-1 read-ahead has not completed.
                    second = runClaim(5);
                    second.Start();

                    ClassicAssert.IsFalse(second.Join(ReclaimObservationWindow),
                        "re-claiming a frame must not return while its prior read-ahead is in flight");
                    ClassicAssert.AreEqual(2, iterator.ReadCallCount,
                        "no read may be issued into a frame whose prior read-ahead is still in flight");

                    // Releasing the read-ahead lets the re-claim proceed and issue its own reads.
                    iterator.CompleteRead(1);
                    iterator.WaitForReadCount(4, TestTimeout);
                    iterator.CompleteRead(5);
                    ClassicAssert.IsTrue(second.Join(TestTimeout), "the re-claim did not complete after its prior load finished");
                    ClassicAssert.IsNull(claimFailure, $"claim failed: {claimFailure}");
                }
                finally
                {
                    // Release every held read and let the claim threads exit before the epoch is disposed under them.
                    iterator.CompleteAllReads();
                    _ = first?.Join(TestTimeout);
                    _ = second?.Join(TestTimeout);
                    epoch.Resume();
                    DrainAndDispose(epoch, iterator);
                }
            });
        }
    }
}