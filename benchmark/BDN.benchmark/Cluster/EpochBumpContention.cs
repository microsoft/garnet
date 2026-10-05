// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BDN.benchmark.Diagnostics;
using BenchmarkDotNet.Attributes;
using Garnet.cluster;

namespace BDN.benchmark.Cluster
{
    /// <summary>
    /// Which bump-wait strategy the measured thread exercises.
    /// </summary>
    public enum BumpWaitMode
    {
        /// <summary>Legacy cooperative busy-spin (<c>BumpAndSpinWaitForEpochTransitionAsync</c>).</summary>
        Spin,

        /// <summary>Wake-based park (<c>BumpAndWaitForEpochTransitionAsync</c>).</summary>
        Wake
    }

    /// <summary>
    /// Contended bump benchmark: a configurable number of background threads repeatedly acquire and
    /// release their observed epoch while the single measured thread bumps the epoch and waits for the
    /// barrier to converge. It contrasts the legacy busy-spin waiter against the wake-based waiter under
    /// identical contention; pair it with <see cref="CpuDiagnoserAttribute"/> so the spin path's CPU
    /// burn (largely kernel-mode scheduler churn from <c>Task.Yield</c>) shows up against the wake
    /// path's near-idle wait, which the wall-clock Mean column alone cannot distinguish.
    ///
    /// Every thread paces itself with a <em>pre-computed</em> random delay: the per-thread delay
    /// sequences are generated once in <see cref="GlobalSetup"/> and merely indexed during measurement,
    /// so no random number is drawn on the hot path and the RNG adds no CPU to the figures under test.
    /// Acquiring threads hold their epoch for a low-CPU <c>Thread.Sleep</c> (not a spin), modeling a
    /// session busy on a long operation such as a migration scan. Sleeping keeps the acquirers' own CPU
    /// out of the diagnoser's whole-process measurement, so Total CPU reflects the bumper's wait strategy
    /// alone. Holds dominate the (negligible) idle, so simultaneous quiescence is rare and the bump
    /// thread must genuinely wait: the spin variant then burns a hot core for the wait while the wake
    /// variant parks at ~0 CPU, and the two converge in comparable wall-clock because the releasing
    /// session both clears its slot and signals the parked waiter.
    /// </summary>
    [CpuDiagnoser]
    [MemoryDiagnoser]
    [Config(typeof(WaiterPenaltyConfig))]
    public class EpochBumpContention
    {
        /// <summary>Number of background threads concurrently acquiring/releasing their epoch.</summary>
        [Params(2, 4, 8)]
        public int AcquiringThreads { get; set; }

        /// <summary>Bump-wait strategy exercised by the measured thread.</summary>
        [Params(BumpWaitMode.Spin, BumpWaitMode.Wake)]
        public BumpWaitMode WaitMode { get; set; }

        /// <summary>Bumps performed per measured invocation.</summary>
        const int BumpsPerInvoke = 32;

        /// <summary>Length of each pre-computed delay ring; a power of two so indexing masks cheaply.</summary>
        const int DelayRingLength = 1024;

        /// <summary>Inclusive lower bound (ms) on how long an acquiring thread holds its epoch.</summary>
        const int MinHoldMs = 2;

        /// <summary>Exclusive upper bound (ms) on the epoch hold; holds are uniform in [2, 8] ms.</summary>
        const int MaxHoldMsExclusive = 9;

        /// <summary>Exclusive upper bound (ms) on the bump thread's inter-bump pacing; uniform in [0, 2] ms.</summary>
        const int MaxBumpPaceMsExclusive = 3;

        /// <summary>Fixed seed so delay sequences are reproducible across runs and variants.</summary>
        const int RandomSeed = 92821;

        /// <summary>
        /// Benchmark-local observer over a flat array of per-session observed epochs. Mirrors
        /// <c>ServerEpochObserverSource</c>'s predicate; a readonly struct so
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
        Thread[] acquirers;
        int[][] acquireDelays;
        int[] bumpDelays;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var threads = AcquiringThreads;
            var rng = new Random(RandomSeed);

            // One observed slot per acquiring thread; the measured thread only bumps and is not observed.
            sessionEpochs = new long[threads];
            epoch = new GarnetEpoch<ArrayObserverSource>(new ArrayObserverSource(sessionEpochs));
            cts = new CancellationTokenSource();

            // Pre-compute every delay sequence up front so the hot path never touches the RNG.
            bumpDelays = BuildDelays(rng, 0, MaxBumpPaceMsExclusive);
            acquireDelays = new int[threads][];
            for (var i = 0; i < threads; i++)
                acquireDelays[i] = BuildDelays(rng, MinHoldMs, MaxHoldMsExclusive);

            acquirers = new Thread[threads];
            for (var i = 0; i < threads; i++)
            {
                var slot = i;
                var delays = acquireDelays[i];
                acquirers[i] = new Thread(() => AcquireLoop(slot, delays, cts.Token))
                {
                    IsBackground = true,
                    Name = $"epoch-contention-acquirer-{slot}"
                };
                acquirers[i].Start();
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            cts.Cancel();
            foreach (var t in acquirers)
                t.Join();
            cts.Dispose();
        }

        /// <summary>
        /// Measured path: bump the epoch and wait for every acquiring thread to converge, repeated
        /// <see cref="BumpsPerInvoke"/> times, pacing between bumps with the pre-computed delay ring.
        /// </summary>
        [Benchmark(OperationsPerInvoke = BumpsPerInvoke)]
        public void BumpUnderContention()
        {
            var delays = bumpDelays;
            var mask = delays.Length - 1;
            var token = cts.Token;

            for (var i = 0; i < BumpsPerInvoke; i++)
            {
                if (WaitMode == BumpWaitMode.Spin)
                    _ = epoch.BumpAndSpinWaitForEpochTransitionAsync().GetAwaiter().GetResult();
                else
                    _ = epoch.BumpAndWaitForEpochTransitionAsync(token: token).GetAwaiter().GetResult();

                Thread.Sleep(delays[i & mask]);
            }
        }

        /// <summary>
        /// Background session: acquire the observed epoch, hold it for a pre-computed low-CPU sleep
        /// (modeling a session busy on a long operation), release it (invoking the waker hook exactly as
        /// <c>ClusterSession.ReleaseCurrentEpoch</c> will once wired), then yield briefly before the next
        /// cycle. Sleeping rather than spinning keeps the acquirer's CPU out of the whole-process figure.
        /// </summary>
        void AcquireLoop(int slot, int[] holdMs, CancellationToken token)
        {
            var mask = holdMs.Length - 1;
            var i = 0;
            while (!token.IsCancellationRequested)
            {
                Volatile.Write(ref sessionEpochs[slot], epoch.GetCurrentEpoch());
                Thread.Sleep(holdMs[i & mask]);

                Volatile.Write(ref sessionEpochs[slot], 0);
                epoch.NotifyEpochReleased();

                // Negligible idle so holds dominate and simultaneous quiescence stays rare.
                Thread.Yield();

                i++;
            }
        }

        static int[] BuildDelays(Random rng, int minInclusive, int maxExclusive)
        {
            var delays = new int[DelayRingLength];
            for (var i = 0; i < delays.Length; i++)
                delays[i] = rng.Next(minInclusive, maxExclusive);
            return delays;
        }
    }
}