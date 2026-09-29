// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    /// <summary>
    /// Stage 1 fault-injection tests for <see cref="DuplexBackpressureRing{TRequest, TCompletion}"/>. These assert
    /// the ring's liveness and accounting invariants under adverse conditions — a throwing transport, teardown
    /// concurrent with in-flight producers, and racing single-delivery of completions — rather than full payload
    /// delivery. In every case the ring must never hang, never corrupt memory, and must dispose each out-of-line
    /// request buffer exactly once (no leak, no double-free).
    /// </summary>
    [TestFixture]
    public class DuplexBackpressureRingFailureTests : TestBase
    {
        static readonly TimeSpan HangTimeout = TimeSpan.FromSeconds(30);

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        /// <summary>1c — a transport that throws must surface via onFlushError, not hang, and leak no buffers.</summary>
        [Test]
        public async Task SendThrows_ReportsErrorNoHangNoLeak()
        {
            var h = new RingTestHarness(pageSize: 256, pageCount: 4, completionCapacity: 32, maxChunkSize: 128);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            try
            {
                // Fail a subset of send calls; the ring must set its flush-failed state, invoke onFlushError, and
                // still make forward progress (disposing the records it can no longer send) without hanging.
                h.SetFailurePredicate(i => (i % 3) == 2);

                for (var i = 0; i < 64; i++)
                {
                    // Out-of-line payloads (larger than one page) so every record owns a pooled buffer whose
                    // exactly-once disposal we can verify.
                    await h.EnqueueAsync(RingPayload.Create(i + 1, 600), expectsResponse: false, cts.Token).ConfigureAwait(false);
                }

                await h.PumpUntilAsync(() => h.FlushErrors.Count > 0, HangTimeout, cts.Token).ConfigureAwait(false);
                ClassicAssert.Greater(h.FlushErrors.Count, 0, "Expected at least one flush error to be reported.");
            }
            finally
            {
                h.Dispose();
            }

            // After teardown, every out-of-line buffer was disposed exactly once — whether it was sent, dropped on
            // the failed flush, or reclaimed by the disposal walk.
            h.AssertNoBufferLeaks();
        }

        /// <summary>1c — disposing the ring while producers are mid-flight must not hang, AV, or leak.</summary>
        [Test]
        public async Task DisposeMidFlight_NoHangNoLeak([Values(2, 6)] int producers)
        {
            var h = new RingTestHarness(pageSize: 256, pageCount: 4, completionCapacity: 32, maxChunkSize: 64);
            using var producerCts = new CancellationTokenSource();
            using var guardCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

            var tasks = new Task[producers];
            try
            {
                for (var p = 0; p < producers; p++)
                {
                    var producer = p;
                    tasks[p] = Task.Run(async () =>
                    {
                        var rng = new Random(3000 + producer);
                        var i = 0;
                        while (!producerCts.IsCancellationRequested)
                        {
                            var id = ((long)(producer + 1) * 1_000_000) + i++;
                            var size = RingPayload.HeaderSize + rng.Next(0, 800);
                            try
                            {
                                await h.EnqueueAsync(RingPayload.Create(id, size), expectsResponse: false, producerCts.Token).ConfigureAwait(false);
                            }
                            catch (ObjectDisposedException)
                            {
                                // The ring was disposed under us: the reserve/register throws once teardown
                                // begins. Expected — stop producing.
                                break;
                            }
                            catch (OperationCanceledException)
                            {
                                break;
                            }
                        }
                    });
                }

                // Let producers get in-flight, then tear the ring down concurrently. Cancel the producers so any
                // thread parked on request-lane back-pressure (whose wait event was disposed by teardown) unwinds
                // instead of spinning.
                await Task.Delay(40, guardCts.Token).ConfigureAwait(false);
                h.Ring.Dispose();
                producerCts.Cancel();

                var completed = Task.WhenAll(tasks);
                var winner = await Task.WhenAny(completed, Task.Delay(HangTimeout, guardCts.Token)).ConfigureAwait(false);
                ClassicAssert.AreSame(completed, winner, "Producers did not unwind after dispose (possible hang).");
                await completed.ConfigureAwait(false);
            }
            finally
            {
                h.Dispose();
            }

            h.AssertNoBufferLeaks();
        }

        /// <summary>1c — a published completion is delivered to exactly one racing claimant; later reads see nothing.</summary>
        [Test]
        public void CompletionSingleDelivery_ExactlyOnceUnderRace()
        {
            const int count = 256;          // == completion capacity, so ticket->slot is a bijection here
            const int claimers = 8;
            var h = new RingTestHarness(pageSize: 64, pageCount: 2, completionCapacity: count, maxChunkSize: 64);
            try
            {
                for (var t = 0; t < count; t++)
                    h.Ring.RegisterCompletion(t, (t * 7) + 1);

                var wins = new int[count];
                var claimedValue = new int[count];
                using var start = new ManualResetEventSlim(false);

                var threads = new Task[claimers];
                for (var c = 0; c < claimers; c++)
                {
                    threads[c] = Task.Run(() =>
                    {
                        start.Wait();
                        for (var t = 0; t < count; t++)
                        {
                            if (h.Ring.TryClaimCompletionTicket(t, out var value))
                            {
                                Interlocked.Increment(ref wins[t]);
                                Volatile.Write(ref claimedValue[t], value);
                            }
                        }
                    });
                }

                start.Set();
                Task.WaitAll(threads);

                for (var t = 0; t < count; t++)
                {
                    ClassicAssert.AreEqual(1, wins[t], $"Ticket {t} was delivered {wins[t]} times (expected exactly one).");
                    ClassicAssert.AreEqual((t * 7) + 1, claimedValue[t], $"Ticket {t} delivered the wrong completion value.");
                    ClassicAssert.IsFalse(h.Ring.TryReadCompletion(t, out _), $"Ticket {t} still reads as published after being claimed.");
                }
            }
            finally
            {
                h.Dispose();
            }
        }
    }
}