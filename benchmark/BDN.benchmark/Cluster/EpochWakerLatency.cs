// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using BDN.benchmark.Diagnostics;
using BenchmarkDotNet.Attributes;
using Garnet.cluster;

namespace BDN.benchmark.Cluster
{
    /// <summary>
    /// Background bump load applied to the sessions while their per-cycle latency is measured.
    /// </summary>
    public enum BumpLoad
    {
        /// <summary>No background bumps; the sessions' baseline cost with the barrier idle.</summary>
        None,

        /// <summary>A background thread bumps with the legacy busy-spin (<c>BumpAndSpinWaitForEpochTransitionAsync</c>).</summary>
        Spin,

        /// <summary>A background thread bumps with the wake-based park (<c>BumpAndWaitForEpochTransitionAsync</c>).</summary>
        Wake
    }

    /// <summary>
    /// Waker-side companion to <see cref="EpochBumpContention"/>. Where that benchmark measures the
    /// <em>waiter's</em> penalty (the bumping thread's wait), this one measures the <em>waker's</em>
    /// penalty: the per-cycle latency a running session pays to acquire, do a fixed unit of CPU work,
    /// and release its observed epoch — while a background thread drives the epoch under the chosen
    /// <see cref="BumpLoad"/>. The Mean column is relabeled "Waker penalty" via
    /// <see cref="WakerPenaltyConfig"/>.
    ///
    /// The barrier never blocks a session (acquire/release are lock-free volatile writes plus an
    /// allocation-free signal), so the bump does not slow the session <em>directly</em>. The penalty it
    /// can inflict is <em>indirect</em>: a busy-spin bump keeps cores hot, and on a loaded box that
    /// stolen CPU is throughput the sessions no longer get. To expose that, one background "long-holder"
    /// session pins a stale epoch for the entire run — modeling a session stuck in a long operation such
    /// as a migration scan — so the barrier can never converge. A <see cref="BumpLoad.Spin"/> bumper
    /// therefore busy-spins <em>continuously</em> (its <c>Task.Yield</c> loop churning the thread pool
    /// across several cores, exactly as in the production incident), while a <see cref="BumpLoad.Wake"/>
    /// bumper parks. The remaining sessions (including the measured one) run CPU-bound work on roughly
    /// half the cores, so None and Wake leave them headroom and Spin's churn pushes the box into
    /// oversubscription — raising the measured session's per-cycle latency. The effect scales with how
    /// much CPU the spin steals; in the production migration incident it pinned dozens of logical
    /// processors, starving the whole server.
    ///
    /// <see cref="BumpLoad.Wake"/> inflicts a subtler, mostly <em>indirect</em> penalty under the same
    /// long hold. There is a single waiter (the bump thread); with it parked, every release takes
    /// <c>NotifyEpochReleased</c>'s real branch (<c>parkedWaiters &gt; 0</c> ⇒ <c>Signal.Set</c>) rather
    /// than a cheap no-op. That <c>Set</c> is the only <em>direct</em> waker cost and is small. The heavy
    /// work is the <em>waiter's</em>: each wake makes it re-scan quiescence, re-arm a fresh signal (an
    /// allocation), and re-park — a self-sustaining wake-storm driven by the release traffic. The single
    /// waiter's CPU churn and per-re-arm allocations (Gen0 GC) steal cycles and inject pauses that land
    /// on the measured session, so the waker's latency measures highest and most variable here even
    /// though the waker itself does almost no extra work. This single-waiter wake-storm is the scenario
    /// the zero-allocation / coalescing follow-ups target; it is distinct from the multi-waiter
    /// thundering herd, which this benchmark does not exercise.
    ///
    /// Only the measured session's cycle is timed; the <see cref="CpuDiagnoserAttribute"/> still reports
    /// whole-process CPU, so the spin bumper's continuous burn is visible there as well.
    /// </summary>
    [CpuDiagnoser]
    [MemoryDiagnoser]
    [Config(typeof(WakerPenaltyConfig))]
    public class EpochWakerLatency
    {
        /// <summary>Background bump strategy competing with the sessions during measurement.</summary>
        [Params(BumpLoad.None, BumpLoad.Spin, BumpLoad.Wake)]
        public BumpLoad Load { get; set; }

        /// <summary>Acquire/work/release cycles performed per measured invocation.</summary>
        const int SessionCycles = 2048;

        /// <summary>Fixed CPU work (spin iterations) a session or load thread performs per unit of work.</summary>
        const int WorkSpins = 512;

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
        Thread[] loadThreads;
        Thread holderThread;
        Thread bumpThread;

        [GlobalSetup]
        public void GlobalSetup()
        {
            // Roughly half the logical cores run CPU-bound load so the None/Wake baseline keeps headroom,
            // while a Spin bumper's thread-pool churn pushes the box into oversubscription. The measured
            // session and the long-holder are two more runnable threads on top of this load.
            var loadCount = Math.Max(1, Environment.ProcessorCount / 2);

            // Slot 0 is the measured session; slot 1 is the long-holder. The CPU-load threads below are
            // pure load and do not participate in the barrier, so the only releases that wake a parked
            // Wake bumper during measurement come from the measured session itself.
            sessionEpochs = new long[2];
            epoch = new GarnetEpoch<ArrayObserverSource>(new ArrayObserverSource(sessionEpochs));
            cts = new CancellationTokenSource();

            // Pin the long-holder to the pre-bump epoch BEFORE the bumper starts, so the first bump can
            // never converge: it models a session stuck in a long operation (e.g. a migration scan) that
            // holds its epoch for the whole run. With convergence impossible, a Spin bumper busy-spins
            // continuously (burning CPU the sessions need) while a Wake bumper parks.
            Volatile.Write(ref sessionEpochs[1], epoch.GetCurrentEpoch());
            holderThread = new Thread(() => LongHolderLoop(cts.Token))
            {
                IsBackground = true,
                Name = "epoch-waker-holder"
            };
            holderThread.Start();

            loadThreads = new Thread[loadCount];
            for (var i = 0; i < loadCount; i++)
            {
                loadThreads[i] = new Thread(() => CpuLoadLoop(cts.Token))
                {
                    IsBackground = true,
                    Name = $"epoch-waker-load-{i}"
                };
                loadThreads[i].Start();
            }

            if (Load != BumpLoad.None)
            {
                bumpThread = new Thread(() => BumpLoop(Load, cts.Token))
                {
                    IsBackground = true,
                    Name = "epoch-waker-bumper"
                };
                bumpThread.Start();
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            // Cancel first so the long-holder releases its stale epoch; only then can a pending Spin bump
            // converge and its thread exit, so order the holder release ahead of the bump-thread join.
            cts.Cancel();
            bumpThread?.Join();
            holderThread.Join();
            foreach (var t in loadThreads)
                t.Join();
            cts.Dispose();
        }

        /// <summary>
        /// Measured path: acquire the observed epoch, perform a fixed unit of CPU work while holding it,
        /// then release it (invoking the waker hook exactly as <c>ClusterSession.ReleaseCurrentEpoch</c>
        /// will once wired). The session itself never blocks on the barrier; its per-cycle latency rises
        /// only when a background bump load steals the CPU this work needs.
        /// </summary>
        [Benchmark(OperationsPerInvoke = SessionCycles)]
        public void SessionCycleUnderBump()
        {
            for (var i = 0; i < SessionCycles; i++)
            {
                Volatile.Write(ref sessionEpochs[0], epoch.GetCurrentEpoch());
                Thread.SpinWait(WorkSpins);

                Volatile.Write(ref sessionEpochs[0], 0);
                epoch.NotifyEpochReleased();
            }
        }

        /// <summary>
        /// Pure CPU-bound load: a tight work loop that keeps its core busy to set the saturation
        /// baseline. It does not touch the barrier, so it adds no wake traffic of its own.
        /// </summary>
        void CpuLoadLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
                Thread.SpinWait(WorkSpins);
        }

        /// <summary>
        /// Long-holder: pins its observed epoch at the pre-bump value for the whole run, modeling a
        /// session stuck in a long operation that never lets the barrier converge. It spins (tight work
        /// loop) rather than sleeping, so it both pins the epoch and burns a core like the production
        /// CPU-bound migration scan; it releases on cancellation so the pending bump can finally
        /// converge and shut down cleanly.
        /// </summary>
        void LongHolderLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
                Thread.SpinWait(WorkSpins);

            Volatile.Write(ref sessionEpochs[1], 0);
            epoch.NotifyEpochReleased();
        }

        /// <summary>
        /// Drives the epoch under the chosen strategy. Because the long-holder never lets the barrier
        /// converge during measurement, a single Spin call busy-spins for the whole run and a single
        /// Wake call parks for it; the loop re-issues only after cancellation lets a call return.
        /// </summary>
        void BumpLoop(BumpLoad load, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                if (load == BumpLoad.Spin)
                    _ = epoch.BumpAndSpinWaitForEpochTransitionAsync().GetAwaiter().GetResult();
                else
                    _ = epoch.BumpAndWaitForEpochTransitionAsync(token: token).GetAwaiter().GetResult();
            }
        }
    }
}