// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Stage 1 soak test for <c>DuplexOperationChannel</c>. Runs many randomized
    /// rounds — varying page geometry, producer counts, payload sizes, response expectation and occasional
    /// transport failures — over a bounded wall-clock budget, re-checking the core invariants (no hang, no memory
    /// corruption, exactly-once buffer disposal, and full in-order-agnostic delivery on clean rounds) on each
    /// round. It is tagged <c>Soak</c> so the default CI gate skips it; run it explicitly with
    /// <c>--filter "TestCategory=Soak"</c>. The seed and duration are overridable via the
    /// <c>GARNET_RING_SOAK_SEED</c> and <c>GARNET_RING_SOAK_SECONDS</c> environment variables for reproduction.
    /// </summary>
    [TestFixture]
    public class DuplexOperationRingSoakTests : TestBase
    {
        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        [Test]
        [Category("Soak")]
        public async Task RandomizedSoak()
        {
            var seed = EnvInt("GARNET_RING_SOAK_SEED", Environment.TickCount);
            var seconds = EnvInt("GARNET_RING_SOAK_SECONDS", 15);
            TestContext.Progress.WriteLine($"Ring soak: seed={seed}, budget={seconds}s");

            var rng = new Random(seed);
            var budget = TimeSpan.FromSeconds(seconds);
            var sw = Stopwatch.StartNew();
            var round = 0;

            while (sw.Elapsed < budget)
            {
                round++;
                var pageSizeBytes = 128 << rng.Next(0, 5);      // 128 .. 2048
                var pageCount = 1 << rng.Next(1, 4);            // 2 .. 8 (power of two)
                var completionCapacity = 1 << rng.Next(3, 7);  // 8 .. 64 (power of two)
                var maxChunkSize = 32 << rng.Next(0, 5);       // 32 .. 512
                var producers = rng.Next(1, 9);
                var perProducer = rng.Next(50, 300);
                var expectsResponse = rng.Next(0, 2) == 0;
                var injectFailure = rng.Next(0, 5) == 0;       // ~20% of rounds fault the transport

                await RunRoundAsync(seed, round, pageSizeBytes, pageCount, completionCapacity, maxChunkSize,
                    producers, perProducer, expectsResponse, injectFailure).ConfigureAwait(false);
            }

            TestContext.Progress.WriteLine($"Ring soak complete: {round} rounds in {sw.Elapsed}.");
        }

        static async Task RunRoundAsync(int seed, int round, int pageSizeBytes, int pageCount, int completionCapacity,
            int maxChunkSize, int producers, int perProducer, bool expectsResponse, bool injectFailure)
        {
            var total = producers * perProducer;
            var label = $"seed={seed} round={round} page={pageSizeBytes}x{pageCount} cap={completionCapacity} " +
                        $"chunk={maxChunkSize} producers={producers} perProducer={perProducer} resp={expectsResponse} " +
                        $"fail={injectFailure}";

            var h = new RingTestHarness(
                pageSize: pageSizeBytes,
                pageCount: pageCount,
                maxOutstandingRequests: pageSizeBytes * pageCount / DuplexRingRecordFormat.HeaderSize,
                completionCapacity: completionCapacity,
                maxChunkSize: maxChunkSize);
            using var producerCts = new CancellationTokenSource();
            using var guardCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var readerDone = new CancellationTokenSource();
            Task reader = null;

            try
            {
                if (injectFailure)
                {
                    var rng = new Random((seed * 131) + round);
                    var everyN = rng.Next(2, 9);
                    h.SetFailurePredicate(i => (i % everyN) == (everyN - 1));
                }

                if (expectsResponse)
                {
                    // Drain the completion lane so response-expecting producers never wedge on completion
                    // back-pressure.
                    reader = Task.Run(async () =>
                    {
                        var advanced = 0;
                        while (!readerDone.IsCancellationRequested)
                        {
                            var issued = h.CompletionTail;
                            if (issued > advanced)
                            {
                                h.AdvanceCompletions(issued - advanced);
                                advanced = issued;
                            }
                            else
                            {
                                try { await Task.Delay(1, readerDone.Token).ConfigureAwait(false); }
                                catch (OperationCanceledException) { break; }
                            }
                        }
                    });
                }

                var expectedIds = new List<long>(total);
                for (var p = 0; p < producers; p++)
                {
                    for (var i = 0; i < perProducer; i++)
                        expectedIds.Add(((long)(p + 1) * 10_000_000) + i);
                }

                var tasks = new Task[producers];
                for (var p = 0; p < producers; p++)
                {
                    var producer = p;
                    tasks[p] = Task.Run(async () =>
                    {
                        var prng = new Random((seed * 977) + (round * 31) + producer);
                        for (var i = 0; i < perProducer; i++)
                        {
                            var id = ((long)(producer + 1) * 10_000_000) + i;
                            var size = RingPayload.HeaderSize + prng.Next(0, pageSizeBytes * 2);
                            await h.EnqueueAsync(RingPayload.Create(id, size), expectsResponse, producerCts.Token).ConfigureAwait(false);
                        }
                    });
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);

                if (!injectFailure)
                {
                    // Clean round: every payload must be delivered intact, exactly once.
                    await h.DrainUntilAsync(total, TimeSpan.FromSeconds(60), guardCts.Token).ConfigureAwait(false);
                    h.AssertReceived(expectedIds);
                }
                else
                {
                    // Faulted round: just require that the ring quiesced without hanging.
                    await h.PumpUntilAsync(() => h.FlushErrors.Count > 0, TimeSpan.FromSeconds(60), guardCts.Token).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Ring soak round failed ({label}).", ex);
            }
            finally
            {
                readerDone.Cancel();
                if (reader != null)
                {
                    try { await reader.ConfigureAwait(false); } catch (OperationCanceledException) { }
                }
                readerDone.Dispose();
                h.Dispose();
            }

            h.AssertNoBufferLeaks();
        }

        static int EnvInt(string name, int fallback)
        {
            var raw = Environment.GetEnvironmentVariable(name);
            return int.TryParse(raw, out var value) ? value : fallback;
        }
    }
}