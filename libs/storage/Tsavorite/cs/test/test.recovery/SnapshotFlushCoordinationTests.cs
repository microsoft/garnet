// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Tsavorite.core;

namespace Tsavorite.test.recovery
{
    [TestFixture]
    public class SnapshotFlushCoordinationTests
    {
        static void BeginFlushing(SnapshotFlushCoordination coordination, long firstPage, long readOnlyFlushCutoffAddress = 0)
        {
            coordination.BeginCutoffCapture(firstPage);
            coordination.PublishReadOnlyFlushCutoff(readOnlyFlushCutoffAddress);
            coordination.BeginFlushing();
        }

        [Test]
        public void ReadOnlyWaitsOnlyForSameSnapshotPage()
        {
            using var coordination = new SnapshotFlushCoordination();
            BeginFlushing(coordination, 10);
            coordination.WaitToIssuePage(10);

            coordination.WaitUntilReadOnlyMayFlush(9);

            var started = new ManualResetEventSlim();
            var waiter = Task.Run(() =>
            {
                started.Set();
                coordination.WaitUntilReadOnlyMayFlush(10);
            });

            ClassicAssert.IsTrue(started.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.IsFalse(waiter.Wait(TimeSpan.FromMilliseconds(100)));

            coordination.CompletePage(10);
            ClassicAssert.IsTrue(waiter.Wait(TimeSpan.FromSeconds(1)));
        }

        [Test]
        public void StableSnapshotStartAdvancesProvisionalLimit()
        {
            using var coordination = new SnapshotFlushCoordination();
            coordination.BeginCutoffCapture(10);
            coordination.PublishReadOnlyFlushCutoff(0);
            coordination.AdvanceReadOnlyFlushPageLimit(12);
            coordination.BeginFlushing();

            ClassicAssert.AreEqual(12, coordination.ReadOnlyFlushPageLimit);
            // This is the normal no-wait fast path: the requested page is already below the monotonic watermark.
            coordination.WaitUntilReadOnlyMayFlush(11);
        }

        [Test]
        public void ClosedCoordinationReleasesFinalPage()
        {
            using var coordination = new SnapshotFlushCoordination();
            BeginFlushing(coordination, 20);
            var waiter = Task.Run(() => coordination.WaitUntilReadOnlyMayFlush(20));

            ClassicAssert.IsFalse(waiter.Wait(TimeSpan.FromMilliseconds(100)));
            coordination.CloseSuccessfully(21);
            ClassicAssert.IsTrue(waiter.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.AreEqual(21, coordination.ReadOnlyFlushPageLimit);
        }

        [Test]
        public void LatePageCompletionCannotRegressWatermark()
        {
            using var coordination = new SnapshotFlushCoordination();
            BeginFlushing(coordination, 10);
            coordination.WaitToIssuePage(10);
            coordination.WaitToIssuePage(11);

            coordination.CompletePage(11);
            ClassicAssert.AreEqual(10, coordination.ReadOnlyFlushPageLimit);
            coordination.CompletePage(10);

            ClassicAssert.AreEqual(12, coordination.ReadOnlyFlushPageLimit);
        }

        [Test]
        public void CompletionWindowBoundsOutstandingPages()
        {
            using var coordination = new SnapshotFlushCoordination(completionWindowSize: 3);
            BeginFlushing(coordination, 10);

            coordination.WaitToIssuePage(10);
            coordination.WaitToIssuePage(11);
            coordination.WaitToIssuePage(12);

            var started = new ManualResetEventSlim();
            var issuer = Task.Run(() =>
            {
                started.Set();
                coordination.WaitToIssuePage(13);
            });

            ClassicAssert.IsTrue(started.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.IsFalse(issuer.Wait(TimeSpan.FromMilliseconds(100)));

            coordination.CompletePage(11);
            ClassicAssert.IsFalse(issuer.Wait(TimeSpan.FromMilliseconds(100)));
            coordination.CompletePage(10);

            ClassicAssert.IsTrue(issuer.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.AreEqual(12, coordination.ReadOnlyFlushPageLimit);
        }

        [Test]
        public void WaitForAllPagesRequiresContiguousCompletion()
        {
            using var coordination = new SnapshotFlushCoordination(completionWindowSize: 3);
            BeginFlushing(coordination, 20);
            coordination.WaitToIssuePage(20);
            coordination.WaitToIssuePage(21);
            coordination.WaitToIssuePage(22);

            var waiter = Task.Run(() => coordination.WaitForAllPages(23));
            coordination.CompletePage(22);
            coordination.CompletePage(20);
            ClassicAssert.IsFalse(waiter.Wait(TimeSpan.FromMilliseconds(100)));

            coordination.CompletePage(21);
            ClassicAssert.IsTrue(waiter.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.AreEqual(23, coordination.ReadOnlyFlushPageLimit);
        }

        [Test]
        public void CompletionWindowReusesWrappedSlotOnlyAfterFrontierAdvances()
        {
            using var coordination = new SnapshotFlushCoordination(completionWindowSize: 2);
            BeginFlushing(coordination, 10);

            coordination.WaitToIssuePage(10);
            coordination.WaitToIssuePage(11);
            coordination.CompletePage(10);
            coordination.WaitToIssuePage(12);

            coordination.CompletePage(11);
            ClassicAssert.AreEqual(12, coordination.ReadOnlyFlushPageLimit,
                "the stale completion formerly in the wrapped slot must not complete its replacement");

            coordination.CompletePage(12);
            ClassicAssert.AreEqual(13, coordination.ReadOnlyFlushPageLimit);
        }

        [Test]
        public void FailedPageReleasesWindowAndRethrowsOriginalFailure()
        {
            using var coordination = new SnapshotFlushCoordination(completionWindowSize: 2);
            BeginFlushing(coordination, 10);
            coordination.WaitToIssuePage(10);
            coordination.WaitToIssuePage(11);
            var expected = new TsavoriteException("injected Snapshot page failure");

            coordination.FailPage(10, expected);

            var actual = Assert.Throws<TsavoriteException>(() => coordination.WaitForAllPages(12));
            Assert.That(actual, Is.SameAs(expected));
            var drain = Task.Run(coordination.WaitForInFlightPages);
            ClassicAssert.IsFalse(drain.Wait(TimeSpan.FromMilliseconds(100)));
            coordination.FailPage(11, expected);
            ClassicAssert.IsTrue(drain.Wait(TimeSpan.FromSeconds(1)));
        }

        [Test]
        public void PrepareTrackingDoesNotBlockReadOnly()
        {
            using var coordination = new SnapshotFlushCoordination();

            coordination.WaitUntilReadOnlyMayFlush(10);

            Assert.That(coordination.State, Is.EqualTo(SnapshotFlushState.Open));
        }

        [Test]
        public void FailureKeepsReadOnlyBlockedUntilCoordinationCloses()
        {
            using var coordination = new SnapshotFlushCoordination();
            BeginFlushing(coordination, 30);
            var waiter = Task.Run(() => coordination.WaitUntilReadOnlyMayFlush(30));

            ClassicAssert.IsFalse(waiter.Wait(TimeSpan.FromMilliseconds(100)));
            coordination.RecordFailure(new TsavoriteException("injected Snapshot write failure"));
            ClassicAssert.IsFalse(waiter.Wait(TimeSpan.FromMilliseconds(100)));
            coordination.Close();
            ClassicAssert.IsTrue(waiter.Wait(TimeSpan.FromSeconds(1)));
        }

        [Test]
        public void CapturedReadOnlyRangeProceedsDuringCutoffDrain()
        {
            using var coordination = new SnapshotFlushCoordination();
            coordination.BeginCutoffCapture(10);

            var classified = Task.Run(() => coordination.WaitForReadOnlyRangeDisposition(100));
            ClassicAssert.IsFalse(classified.Wait(TimeSpan.FromMilliseconds(100)));

            coordination.PublishReadOnlyFlushCutoff(100);

            ClassicAssert.IsTrue(classified.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.IsFalse(classified.Result);
            Assert.That(coordination.State, Is.EqualTo(SnapshotFlushState.DrainingCutoff));
        }

        [Test]
        public void PostCutoffReadOnlyRangeWaitsForFlushingState()
        {
            using var coordination = new SnapshotFlushCoordination();
            coordination.BeginCutoffCapture(10);
            coordination.PublishReadOnlyFlushCutoff(100);

            var classified = Task.Run(() => coordination.WaitForReadOnlyRangeDisposition(101));
            ClassicAssert.IsFalse(classified.Wait(TimeSpan.FromMilliseconds(100)));

            coordination.BeginFlushing();

            ClassicAssert.IsTrue(classified.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.IsTrue(classified.Result);
        }

        [Test]
        public void CloseReleasesReadOnlyRangeWaitingForCutoff()
        {
            using var coordination = new SnapshotFlushCoordination();
            coordination.BeginCutoffCapture(10);

            var classified = Task.Run(() => coordination.WaitForReadOnlyRangeDisposition(100));
            ClassicAssert.IsFalse(classified.Wait(TimeSpan.FromMilliseconds(100)));

            coordination.Close();

            ClassicAssert.IsTrue(classified.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.IsFalse(classified.Result);
        }

        [Test]
        public void DisposeReleasesPostCutoffRangeWaitingForFlushing()
        {
            using var coordination = new SnapshotFlushCoordination();
            coordination.BeginCutoffCapture(10);
            coordination.PublishReadOnlyFlushCutoff(100);

            var classified = Task.Run(() => coordination.WaitForReadOnlyRangeDisposition(101));
            ClassicAssert.IsFalse(classified.Wait(TimeSpan.FromMilliseconds(100)));

            coordination.Dispose();

            ClassicAssert.IsTrue(classified.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.IsFalse(classified.Result);
        }

        [Test]
        public void PageWriteBatchRetainsEarlierSpanFailure()
        {
            var result = new PageAsyncFlushResult<Empty> { count = 2 };

            ClassicAssert.AreEqual(17, result.RecordError(17));
            ClassicAssert.AreEqual(17, result.RecordError(0));
            ClassicAssert.AreEqual(17, result.RecordError(23));
        }
    }
}