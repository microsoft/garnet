// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Stage 1 correctness tests for <c>DuplexOperationChannel</c> exercised in
    /// isolation (no socket, no server) through <see cref="RingTestHarness"/>. Covers the inline / out-of-line
    /// size matrix, single- and multi-chunk framing, concurrent mixed ingestion under page wrap and
    /// back-pressure, response-expecting flow with a reply-advancing reader, flush-lane back-pressure under
    /// deferred transport completions, and the empty-slot stride recovered during teardown.
    /// </summary>
    [TestFixture]
    public class DuplexOperationRingTests : TestBase
    {
        static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(60);

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        /// <summary>1a — single producer, full inline/out-of-line size matrix, single-chunk and multi-chunk.</summary>
        [Test]
        public async Task SizeMatrix_SingleProducer([Values(1 << 20, 64)] int maxChunkSize)
        {
            const int pageSize = 4096;
            using var h = new RingTestHarness(pageSize, pageCount: 8, completionCapacity: 64, maxChunkSize: maxChunkSize);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));

            var maxInline = h.MaxInlinePayloadSize;
            var sizes = new[]
            {
                RingPayload.HeaderSize,      // smallest possible, inline
                64,                          // inline
                1000,                        // inline
                maxInline - 1,               // inline, near boundary
                maxInline,                   // inline, exactly one page
                maxInline + 1,               // forced out-of-line
                2 * pageSize,                // multi-page out-of-line
                (5 * pageSize) + 123,        // multi-page, unaligned tail
            };

            var expectedIds = new List<long>();
            for (var i = 0; i < sizes.Length; i++)
            {
                long id = i + 1;
                expectedIds.Add(id);
                await h.EnqueueAsync(RingPayload.Create(id, sizes[i]), expectCompletion: false, cts.Token).ConfigureAwait(false);
            }

            await h.DrainUntilAsync(expectedIds.Count, DrainTimeout, cts.Token).ConfigureAwait(false);

            h.AssertReceived(expectedIds);
            h.AssertNoBufferLeaks();
        }

        /// <summary>Logical page wrap preserves the published out-of-line descriptor address.</summary>
        [Test]
        public async Task OutOfLineDescriptorAddressWrapsWithPageCounter()
        {
            var operationCount = (1 << PageOffset.kPageBits) + 1;
            using var h = new RingTestHarness(pageSize: 8, pageCount: 4, completionCapacity: 64, maxChunkSize: 8);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            var expectedIds = new List<long>(operationCount);
            for (var i = 0; i < operationCount; i++)
            {
                long id = i + 1;
                expectedIds.Add(id);
                await h.EnqueueAsync(RingPayload.Create(id, RingPayload.HeaderSize), expectCompletion: false, cts.Token).ConfigureAwait(false);
            }

            await h.DrainUntilAsync(operationCount, DrainTimeout, cts.Token).ConfigureAwait(false);

            h.AssertReceived(expectedIds);
            h.AssertNoBufferLeaks();
            ClassicAssert.IsEmpty(h.FlushErrors);
        }

        /// <summary>1b — many concurrent producers, mixed inline/out-of-line, small pages to force wrap + back-pressure.</summary>
        [Test]
        public async Task ConcurrentMixedIngestion([Values(2, 8)] int producers)
        {
            const int perProducer = 500;
            // Small pages (MaxInline == 248) so random sizes straddle the inline/out-of-line boundary, and few
            // pages so page reuse must wait for the ring's own flush (request-lane back-pressure).
            using var h = new RingTestHarness(pageSize: 256, pageCount: 4, completionCapacity: 64, maxChunkSize: 128);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));

            var expectedIds = new List<long>(producers * perProducer);
            for (var p = 0; p < producers; p++)
            {
                for (var i = 0; i < perProducer; i++)
                    expectedIds.Add(((long)(p + 1) * 1_000_000) + i);
            }

            var tasks = new Task[producers];
            for (var p = 0; p < producers; p++)
            {
                var producer = p;
                tasks[p] = Task.Run(async () =>
                {
                    var rng = new Random(1000 + producer);
                    for (var i = 0; i < perProducer; i++)
                    {
                        var id = ((long)(producer + 1) * 1_000_000) + i;
                        var size = RingPayload.HeaderSize + rng.Next(0, 1024);
                        await h.EnqueueAsync(RingPayload.Create(id, size), expectCompletion: false, cts.Token).ConfigureAwait(false);
                    }
                }, cts.Token);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            await h.DrainUntilAsync(expectedIds.Count, DrainTimeout, cts.Token).ConfigureAwait(false);

            h.AssertReceived(expectedIds);
            h.AssertNoBufferLeaks();
        }

        /// <summary>1b (response lane) — response-expecting claims with a small completion lane and a reply-advancing reader.</summary>
        [Test]
        public async Task ResponseExpecting_WithAdvancingReader()
        {
            const int producers = 4;
            const int perProducer = 300;
            const int total = producers * perProducer;

            // Completion capacity deliberately far smaller than the outstanding count, so producers block on
            // completion-lane back-pressure until the reader advances the reply watermark.
            using var h = new RingTestHarness(pageSize: 512, pageCount: 8, completionCapacity: 8, maxChunkSize: 256);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));

            var readerDone = new CancellationTokenSource();
            var reader = Task.Run(async () =>
            {
                var advanced = 0;
                while (advanced < total && !readerDone.IsCancellationRequested)
                {
                    var issued = h.CompletionTail;
                    if (issued > advanced)
                    {
                        h.AdvanceCompletions(issued - advanced);
                        advanced = issued;
                    }
                    else
                    {
                        await Task.Delay(1, cts.Token).ConfigureAwait(false);
                    }
                }
            }, cts.Token);

            var expectedIds = new List<long>(total);
            var tasks = new Task[producers];
            for (var p = 0; p < producers; p++)
            {
                var producer = p;
                tasks[p] = Task.Run(async () =>
                {
                    var rng = new Random(2000 + producer);
                    for (var i = 0; i < perProducer; i++)
                    {
                        var id = ((long)(producer + 1) * 1_000_000) + i;
                        var size = RingPayload.HeaderSize + rng.Next(0, 700);
                        await h.EnqueueAsync(RingPayload.Create(id, size), expectCompletion: true, cts.Token).ConfigureAwait(false);
                    }
                }, cts.Token);
            }

            for (var p = 0; p < producers; p++)
            {
                for (var i = 0; i < perProducer; i++)
                    expectedIds.Add(((long)(p + 1) * 1_000_000) + i);
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            await h.DrainUntilAsync(total, DrainTimeout, cts.Token).ConfigureAwait(false);

            // Keep the reader advancing until every ticket issued during the run is drained, then stop it.
            while (h.CompletionTail > 0 && !reader.IsCompleted)
            {
                h.AdvanceCompletions(0);
                if (reader.Wait(50))
                    break;
            }
            readerDone.Cancel();
            try { await reader.ConfigureAwait(false); } catch (OperationCanceledException) { }

            h.AssertReceived(expectedIds);
            h.AssertNoBufferLeaks();
        }

        /// <summary>
        /// A teardown drain can observe an allocated completion ticket before its producer publishes the
        /// completion. The producer must still be able to publish and claim that completion afterward so the
        /// caller is faulted instead of stranded.
        /// </summary>
        [Test]
        public void CompletionPublishedAfterTeardownScanCanBeClaimed()
        {
            using var h = new RingTestHarness(pageSize: 256, pageCount: 2, completionCapacity: 1, maxChunkSize: 256);

            h.epoch.Resume();
            DuplexOperationReservation reservation;
            try
            {
                ClassicAssert.IsTrue(h.Ring.TryScheduleOperation(
                    DuplexRingRecordFormat.HeaderSize,
                    expectsCompletion: true,
                    out reservation,
                    out _));
            }
            finally
            {
                h.epoch.Suspend();
            }

            h.Close();

            // The receive-side teardown reached this ticket before its producer published the completion.
            ClassicAssert.IsFalse(h.Ring.TryClaimCompletionTicket(reservation.completionTicket, out _));
            h.AdvanceCompletions(1);

            // The producer publishes after the teardown scan and performs the fallback fault claim.
            const int completion = 42;
            h.Ring.RegisterCompletion(reservation.completionTicket, completion);
            ClassicAssert.IsTrue(h.Ring.TryClaimCompletionTicket(reservation.completionTicket, out var claimed));
            ClassicAssert.AreEqual(completion, claimed);
            ClassicAssert.IsFalse(h.Ring.TryClaimCompletionTicket(reservation.completionTicket, out _));
            ClassicAssert.IsFalse(h.Ring.TryReadCompletion(reservation.completionTicket, out _));
        }

        /// <summary>
        /// When teardown and the producer fault path target the same published completion concurrently, exactly
        /// one path claims it for delivery.
        /// </summary>
        [Test]
        public void ConcurrentCompletionFaultClaimsDeliverOnce()
        {
            using var h = new RingTestHarness(pageSize: 256, pageCount: 2, completionCapacity: 1, maxChunkSize: 256);

            h.epoch.Resume();
            DuplexOperationReservation reservation;
            try
            {
                ClassicAssert.IsTrue(h.Ring.TryScheduleOperation(
                    DuplexRingRecordFormat.HeaderSize,
                    expectsCompletion: true,
                    out reservation,
                    out _));
            }
            finally
            {
                h.epoch.Suspend();
            }

            const int completion = 42;
            h.Ring.RegisterCompletion(reservation.completionTicket, completion);

            var claimCount = 0;
            var claimedValue = 0;
            Parallel.Invoke(ClaimCompletion, ClaimCompletion);

            ClassicAssert.AreEqual(1, claimCount);
            ClassicAssert.AreEqual(completion, claimedValue);
            ClassicAssert.IsFalse(h.Ring.TryReadCompletion(reservation.completionTicket, out _));

            void ClaimCompletion()
            {
                if (!h.Ring.TryClaimCompletionTicket(reservation.completionTicket, out var claimed))
                    return;

                Interlocked.Increment(ref claimCount);
                Interlocked.Add(ref claimedValue, claimed);
            }
        }

        /// <summary>
        /// 1c (flush lane) — with transport completions deferred out of band, producers must park on request-lane
        /// back-pressure and then make forward progress only as completions are released one at a time, with no
        /// lost wakeups, full and intact delivery, and exactly-once disposal.
        /// </summary>
        [Test]
        public async Task DelayedCompletions_FlushBackpressure_ReleasesWithoutLostWakeups([Values(2, 4)] int producers)
        {
            const int perProducer = 150;
            var total = producers * perProducer;

            // Small ring and small chunks: the request lane fills after only a handful of records (forcing
            // producers to park), and multi-chunk requests exercise partial completion while completions are held.
            using var h = new RingTestHarness(pageSize: 256, pageCount: 4, completionCapacity: 64, maxChunkSize: 64);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));

            // Hold every transport send-completion in a side queue instead of finalizing it inline. Nothing the
            // ring flushes can advance flushedUntilAddress until the test releases it below.
            h.DeferCompletions();

            var expectedIds = new List<long>(total);
            for (var p = 0; p < producers; p++)
            {
                for (var i = 0; i < perProducer; i++)
                    expectedIds.Add(((long)(p + 1) * 1_000_000) + i);
            }

            var tasks = new Task[producers];
            for (var p = 0; p < producers; p++)
            {
                var producer = p;
                tasks[p] = Task.Run(async () =>
                {
                    var rng = new Random(4000 + producer);
                    for (var i = 0; i < perProducer; i++)
                    {
                        var id = ((long)(producer + 1) * 1_000_000) + i;
                        var size = RingPayload.HeaderSize + rng.Next(0, 1024);
                        await h.EnqueueAsync(RingPayload.Create(id, size), expectCompletion: false, cts.Token).ConfigureAwait(false);
                    }
                }, cts.Token);
            }

            var all = Task.WhenAll(tasks);

            // Let producers fill the ring and park. Pumping drives flushes (so records are sent and their
            // completions queued), but with completions withheld flushedUntilAddress cannot advance, so no payload
            // can finalize and the whole workload cannot drain.
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromMilliseconds(250))
            {
                h.Pump();
                await Task.Delay(5, cts.Token).ConfigureAwait(false);
            }
            ClassicAssert.AreEqual(0, h.CompletedCount, "No payload may finalize while all transport completions are deferred.");
            ClassicAssert.IsFalse(all.IsCompleted, "Producers should be parked on request-lane back-pressure while completions are withheld.");

            // Release completions one at a time, pumping to keep flushing freshly unblocked records. Each release
            // advances the flushed watermark and must wake a parked producer; a lost wakeup would stall this loop
            // until the timeout fires.
            var drainTimeout = TimeSpan.FromSeconds(90);
            sw.Restart();
            while (h.CompletedCount < total)
            {
                cts.Token.ThrowIfCancellationRequested();
                var released = h.CompleteOneDeferredChunk();
                h.Pump();
                if (!released)
                    await Task.Delay(1, cts.Token).ConfigureAwait(false);
                if (sw.Elapsed > drainTimeout)
                    throw new TimeoutException($"Delayed-completion drain stalled: {h.CompletedCount}/{total} finalized, {h.DeferredCompletionCount} completions still deferred (possible lost wakeup).");
            }

            await all.ConfigureAwait(false);

            // Every deferred completion was released, every payload arrived intact exactly once, and every
            // out-of-line buffer was disposed exactly once.
            ClassicAssert.AreEqual(0, h.DeferredCompletionCount, "All deferred completions should have been released.");
            h.AssertReceived(expectedIds);
            h.AssertNoBufferLeaks();
        }

        /// <summary>
        /// Reconnect disposes the old request channel. A producer parked because that channel is awaiting local
        /// send completion must wake and fault rather than remain blocked on its retired capacity event.
        /// </summary>
        [Test]
        public async Task DisposeWakesOperationWaitingOnRequestCapacity()
        {
            using var h = new RingTestHarness(pageSize: 256, pageCount: 2, completionCapacity: 16, maxChunkSize: 64);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            h.DeferCompletions();

            var tasks = new Task[128];
            for (var i = 0; i < tasks.Length; i++)
                tasks[i] = h.EnqueueAsync(RingPayload.Create(i + 1, 512), expectCompletion: false, cts.Token);

            ClassicAssert.IsTrue(
                SpinWait.SpinUntil(
                    () => h.DeferredCompletionCount > 0 && tasks.Any(task => !task.IsCompleted),
                    TimeSpan.FromSeconds(5)),
                "No producer waited for request-ring capacity.");

            h.Close();

            var allTasks = Task.WhenAll(tasks);
            var completed = await Task.WhenAny(allTasks, Task.Delay(TimeSpan.FromSeconds(5), cts.Token)).ConfigureAwait(false);
            ClassicAssert.AreSame(allTasks, completed, "A request-capacity waiter remained blocked after disposal.");
            ClassicAssert.IsTrue(tasks.Any(task => task.IsFaulted), "At least one parked producer should observe channel disposal.");
            foreach (var task in tasks.Where(task => task.IsFaulted))
                ClassicAssert.IsInstanceOf<ObjectDisposedException>(task.Exception?.GetBaseException());
        }

        /// <summary>1d — allocated-but-unpublished descriptors decode as empty and stride by one descriptor on teardown.</summary>
        [Test]
        public void UnpublishedDescriptors_StrideCorrectlyOnDispose()
        {
            const int count = 20;
            var h = new RingTestHarness(pageSize: 4096, pageCount: 4, completionCapacity: 64, maxChunkSize: 4096);
            try
            {
                var nextId = 1L;

                h.epoch.Resume();
                try
                {
                    for (var i = 0; i < count; i++)
                    {
                        var reserved = h.Ring.TryScheduleOperation(
                            sizeof(long),
                            expectsCompletion: false,
                            out var reservation,
                            out _);
                        ClassicAssert.IsTrue(reserved);
                        var address = reservation.requestAddress;

                        // Publish only even slots; odd slots stay at the 0xFF page fill (Uninitialized), which the
                        // teardown walk must stride over by a single descriptor without misreading the next record.
                        if ((i % 2) == 0)
                        {
                            var id = nextId++;
                            h.TrackOutOfLine(id);
                            var payload = RingPayload.Create(id, 200);
                            h.Ring.RegisterOfflineRecord(address, new TestRequest(payload, payload.Length, id, h.DisposeCounts));
                        }
                    }
                }
                finally
                {
                    h.epoch.Suspend();
                }

                // Dispose reclaims every published (still-unflushed) out-of-line buffer exactly once and strides
                // cleanly over the unpublished slots.
                h.Ring.Dispose();
                h.AssertNoBufferLeaks();
            }
            finally
            {
                h.epoch.Dispose();
            }
        }
    }
}