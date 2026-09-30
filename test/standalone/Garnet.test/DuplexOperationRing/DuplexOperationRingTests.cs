// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Stage 1 correctness tests for <c>DuplexOperationRing</c> exercised in
    /// isolation (no socket, no server) through <see cref="RingTestHarness"/>. Covers the inline / out-of-line
    /// size matrix, single- and multi-chunk framing, concurrent mixed ingestion under page wrap and
    /// back-pressure, response-expecting flow with a reply-advancing reader, and the empty-slot stride recovered
    /// during teardown.
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
                        var address = reservation.RequestAddress;

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