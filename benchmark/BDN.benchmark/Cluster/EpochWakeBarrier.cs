// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BenchmarkDotNet.Attributes;
using Garnet.cluster;

namespace BDN.benchmark.Cluster
{
    /// <summary>
    /// Measures the per-session overhead of the wake-based epoch barrier's waker path. A data session
    /// brackets each batch with acquire/release of its observed epoch; release invokes
    /// <c>GarnetEpoch.NotifyEpochReleased</c>. This benchmark runs that acquire/release cycle on the
    /// measured thread while a configurable number of background session threads churn the same epoch
    /// and, optionally, a background thread periodically bumps it.
    ///
    /// Invariant under test (barrier design goal #1): a running session that releases its epoch pays a
    /// single volatile read and allocates nothing, whether or not a bump is in flight. Expect the
    /// <c>BackgroundBumps = true</c> rows to match the <c>false</c> rows on both time and allocated
    /// bytes. Only the rare bump thread allocates (a TCS on re-arm plus the async wait); paced well
    /// below real-world bump frequency, its cost amortizes to a negligible per-session-op figure.
    ///
    /// The harness reaches only the three existing internal entry points
    /// (<c>GetCurrentEpoch</c>, <c>NotifyEpochReleased</c>, <c>BumpAndWaitForEpochTransitionWakeAsync</c>)
    /// and models sessions with a benchmark-local observer, so it needs no additions to the barrier API.
    /// </summary>
    [MemoryDiagnoser]
    public class EpochWakeBarrier
    {
        /// <summary>Background-bump and concurrency configuration for a run.</summary>
        [ParamsSource(nameof(ParamsProvider))]
        public EpochBenchParams Params { get; set; }

        /// <summary>The with/without-background-bumps matrix, each at no and moderate session concurrency.</summary>
        public IEnumerable<EpochBenchParams> ParamsProvider()
        {
            yield return new(backgroundBumps: false, backgroundSessions: 0);
            yield return new(backgroundBumps: false, backgroundSessions: 7);
            yield return new(backgroundBumps: true, backgroundSessions: 0);
            yield return new(backgroundBumps: true, backgroundSessions: 7);
        }

        /// <summary>Acquire/release cycles performed per measured invocation.</summary>
        const int SessionCycles = 1024;

        /// <summary>Pacing between background bumps; keeps bumps "background" and far rarer than reality's seconds-apart cadence.</summary>
        const int BumpIntervalMs = 1;

        /// <summary>
        /// Benchmark-local observer over a flat array of per-session observed epochs (slot 0 is the
        /// measured session). Mirrors <c>ServerEpochObserverSource</c>'s predicate; a readonly struct so
        /// <see cref="GarnetEpoch{TEpochObserver}"/> specializes and the scan inlines.
        /// </summary>
        readonly struct ArrayObserverSource : IEpochObserver
        {
            readonly long[] epochs;

            public ArrayObserverSource(long[] epochs) => this.epochs = epochs;

            public bool AllSessionsQuiesced(long targetEpoch)
            {
                for (var i = 0; i < epochs.Length; i++)
                {
                    var entryEpoch = Volatile.Read(ref epochs[i]);
                    if (entryEpoch != 0 && entryEpoch < targetEpoch)
                        return false;
                }
                return true;
            }
        }

        GarnetEpoch<ArrayObserverSource> epoch;
        long[] sessionEpochs;
        CancellationTokenSource cts;
        Thread[] backgroundThreads;
        Thread bumpThread;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var backgroundSessions = Params.backgroundSessions;

            // Slot 0 is the measured session; the remaining slots are background sessions.
            sessionEpochs = new long[1 + backgroundSessions];
            epoch = new GarnetEpoch<ArrayObserverSource>(new ArrayObserverSource(sessionEpochs));
            cts = new CancellationTokenSource();

            backgroundThreads = new Thread[backgroundSessions];
            for (var i = 0; i < backgroundSessions; i++)
            {
                var slot = i + 1;
                backgroundThreads[i] = new Thread(() => ChurnSession(slot, cts.Token))
                {
                    IsBackground = true,
                    Name = $"epoch-bench-session-{slot}"
                };
                backgroundThreads[i].Start();
            }

            if (Params.backgroundBumps)
            {
                bumpThread = new Thread(() => BumpLoop(cts.Token))
                {
                    IsBackground = true,
                    Name = "epoch-bench-bumper"
                };
                bumpThread.Start();
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            cts.Cancel();
            bumpThread?.Join();
            foreach (var t in backgroundThreads)
                t.Join();
            cts.Dispose();
        }

        /// <summary>
        /// Measured path: acquire then release the observed epoch in a tight loop, invoking the waker
        /// hook on each release exactly as <c>ClusterSession.ReleaseCurrentEpoch</c> will once wired.
        /// </summary>
        [Benchmark(OperationsPerInvoke = SessionCycles)]
        public void SessionEpochCycle()
        {
            for (var i = 0; i < SessionCycles; i++)
            {
                Volatile.Write(ref sessionEpochs[0], epoch.GetCurrentEpoch());
                Volatile.Write(ref sessionEpochs[0], 0);
                epoch.NotifyEpochReleased();
            }
        }

        void ChurnSession(int slot, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                Volatile.Write(ref sessionEpochs[slot], epoch.GetCurrentEpoch());
                Volatile.Write(ref sessionEpochs[slot], 0);
                epoch.NotifyEpochReleased();

                // Idle briefly at quiescence so bumps can observe a drained window, as real sessions do between batches.
                Thread.SpinWait(1);
            }
        }

        void BumpLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                _ = epoch.BumpAndWaitForEpochTransitionAsync(token: token).GetAwaiter().GetResult();
                Thread.Sleep(BumpIntervalMs);
            }
        }
    }
}