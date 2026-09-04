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
        [Test]
        public void ReadOnlyWaitsOnlyForSameSnapshotPage()
        {
            using var coordination = new SnapshotFlushCoordination(10);
            coordination.Arm(10);
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
            using var coordination = new SnapshotFlushCoordination(10);
            coordination.Arm(10);

            coordination.AdvanceInitialPage(12);

            ClassicAssert.AreEqual(12, coordination.LastCompletedSnapshotPage);
            // This is the normal no-wait fast path: the requested page is already below the monotonic watermark.
            coordination.WaitUntilReadOnlyMayFlush(11);
        }

        [Test]
        public void TerminalWatermarkReleasesFinalPage()
        {
            using var coordination = new SnapshotFlushCoordination(20);
            coordination.Arm(20);
            var waiter = Task.Run(() => coordination.WaitUntilReadOnlyMayFlush(20));

            ClassicAssert.IsFalse(waiter.Wait(TimeSpan.FromMilliseconds(100)));
            coordination.Complete(21);
            ClassicAssert.IsTrue(waiter.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.AreEqual(21, coordination.LastCompletedSnapshotPage);
        }

        [Test]
        public void LatePageCompletionCannotRegressWatermark()
        {
            using var coordination = new SnapshotFlushCoordination(10);
            coordination.Arm(10);
            coordination.WaitToIssuePage(10);
            coordination.WaitToIssuePage(11);

            coordination.CompletePage(11);
            ClassicAssert.AreEqual(10, coordination.LastCompletedSnapshotPage);
            coordination.CompletePage(10);

            ClassicAssert.AreEqual(12, coordination.LastCompletedSnapshotPage);
        }

        [Test]
        public void CompletionWindowBoundsOutstandingPages()
        {
            using var coordination = new SnapshotFlushCoordination(10, completionWindowSize: 3);
            coordination.Arm(10);

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
            ClassicAssert.AreEqual(12, coordination.LastCompletedSnapshotPage);
        }

        [Test]
        public void WaitForAllPagesRequiresContiguousCompletion()
        {
            using var coordination = new SnapshotFlushCoordination(20, completionWindowSize: 3);
            coordination.Arm(20);
            coordination.WaitToIssuePage(20);
            coordination.WaitToIssuePage(21);
            coordination.WaitToIssuePage(22);

            var waiter = Task.Run(() => coordination.WaitForAllPages(23));
            coordination.CompletePage(22);
            coordination.CompletePage(20);
            ClassicAssert.IsFalse(waiter.Wait(TimeSpan.FromMilliseconds(100)));

            coordination.CompletePage(21);
            ClassicAssert.IsTrue(waiter.Wait(TimeSpan.FromSeconds(1)));
            ClassicAssert.AreEqual(23, coordination.LastCompletedSnapshotPage);
        }

        [Test]
        public void CompletionWindowReusesWrappedSlotOnlyAfterFrontierAdvances()
        {
            using var coordination = new SnapshotFlushCoordination(10, completionWindowSize: 2);
            coordination.Arm(10);

            coordination.WaitToIssuePage(10);
            coordination.WaitToIssuePage(11);
            coordination.CompletePage(10);
            coordination.WaitToIssuePage(12);

            coordination.CompletePage(11);
            ClassicAssert.AreEqual(12, coordination.LastCompletedSnapshotPage,
                "the stale completion formerly in the wrapped slot must not complete its replacement");

            coordination.CompletePage(12);
            ClassicAssert.AreEqual(13, coordination.LastCompletedSnapshotPage);
        }

        [Test]
        public void FailedPageReleasesWindowAndRethrowsOriginalFailure()
        {
            using var coordination = new SnapshotFlushCoordination(10, completionWindowSize: 2);
            coordination.Arm(10);
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
            using var coordination = new SnapshotFlushCoordination(10);

            coordination.WaitUntilReadOnlyMayFlush(10);

            ClassicAssert.IsFalse(coordination.IsArmed);
        }

        [Test]
        public void FailureWakesReadOnlyWaiter()
        {
            using var coordination = new SnapshotFlushCoordination(30);
            coordination.Arm(30);
            var waiter = Task.Run(() => coordination.WaitUntilReadOnlyMayFlush(30));

            ClassicAssert.IsFalse(waiter.Wait(TimeSpan.FromMilliseconds(100)));
            coordination.Fail(new TsavoriteException("injected Snapshot write failure"));
            ClassicAssert.IsTrue(waiter.Wait(TimeSpan.FromSeconds(1)));
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