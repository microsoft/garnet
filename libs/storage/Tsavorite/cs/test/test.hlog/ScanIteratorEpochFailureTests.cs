// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
            public int ReadCallCount;

            /// <summary>When set, issuing the page read throws, as a device that fails before the read is submitted does.</summary>
            public Exception ThrowOnRead;

            public StubScanIterator(LightEpoch epoch)
                : base(beginAddress: 0, endAddress: long.MaxValue, DiskScanBufferingMode.SinglePageBuffering,
                       InMemoryScanBufferingMode.NoBuffering, includeClosedRecords: false, epoch, LogPageSizeBits, new StubAllocator())
            { }

            public int PendingDrainCallbacks => Volatile.Read(ref pendingDrainCallbacks);

            public bool ClaimFrameAndIssueRead(long page)
                => BufferAndLoad(currentIterationAddress: page << LogPageSizeBits, currentPage: page, currentFrame: 0,
                                 headAddress: long.MaxValue, endIterationAddress: long.MaxValue);

            internal override void AsyncReadPageFromDeviceToFrame<TContext>(CircularDiskReadBuffer readBuffers, long readPage, long untilAddress, TContext context,
                    out CountdownEvent completed, long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            {
                _ = Interlocked.Increment(ref ReadCallCount);

                if (ThrowOnRead is not null)
                    throw ThrowOnRead;

                // Report the load as already complete so the caller does not wait on a device that does not exist.
                completed = new CountdownEvent(1);
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
        public void FrameClaimIsReleasedWhenEpochBumpThrows()
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();

                // Construct while unprotected; the iterator only adopts the epoch when the constructing thread is not
                // already holding it.
                var iterator = new StubScanIterator(epoch);

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
                    ClassicAssert.LessOrEqual(iterator.ReadCallCount, 1, "the abandoned read must not be issued twice");

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
        public void FrameClaimIsReleasedWhenEpochBumpThrowsBeforeRegistration()
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();
                var iterator = new StubScanIterator(epoch);
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
        public void FrameClaimIsReleasedWhenIssuingTheReadThrows()
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();
                var iterator = new StubScanIterator(epoch) { ThrowOnRead = new InvalidOperationException("device failed to issue read") };

                epoch.Resume();
                try
                {
                    // The read is skipped rather than retried, so the claim is released and the wait is cancelled.
                    _ = Assert.Catch(() => iterator.ClaimFrameAndIssueRead(page: 0));

                    ClassicAssert.AreEqual(1, iterator.ReadCallCount, "the read must have been attempted");
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
        public void FrameLoadSucceedsWhenEpochBumpDoesNotThrow()
        {
            RunBounded(() =>
            {
                var epoch = new LightEpoch();
                var iterator = new StubScanIterator(epoch);

                epoch.Resume();
                try
                {
                    _ = iterator.ClaimFrameAndIssueRead(page: 0);
                    epoch.ProtectAndDrain();

                    ClassicAssert.AreEqual(1, iterator.ReadCallCount);
                    ClassicAssert.AreEqual(0, iterator.PendingDrainCallbacks);
                }
                finally
                {
                    DrainAndDispose(epoch, iterator);
                }
            });
        }
    }
}