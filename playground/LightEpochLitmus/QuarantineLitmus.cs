// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.epoch.litmus
{
    /// <summary>Outcome of a <see cref="QuarantineLitmus{TEpoch}"/> run.</summary>
    internal sealed class QuarantineLitmusResult
    {
        /// <summary>Rounds in which the reader dereferenced a page it had been poisoned under.</summary>
        internal long Violations { get; init; }

        /// <summary>
        /// Rounds in which the reader captured a live page pointer. If this is 0 the race
        /// window was never sampled and a clean result says nothing.
        /// </summary>
        internal long SampledRounds { get; init; }

        /// <summary>Rounds the reclaimer completed.</summary>
        internal long Rounds { get; init; }

        /// <summary>Pages retired via <see cref="IEpochUnderTest.BumpCurrentEpoch(Action)"/>.</summary>
        internal long Drains { get; init; }

        /// <summary>Pages the epoch actually decided were safe to recycle.</summary>
        internal long Quarantines { get; init; }

        internal TimeSpan Elapsed { get; init; }

        public override string ToString()
            => $"violations={Violations:N0} sampledRounds={SampledRounds:N0} rounds={Rounds:N0} "
             + $"drains={Drains:N0} quarantined={Quarantines:N0} elapsed={Elapsed.TotalSeconds:F1}s";
    }

    /// <summary>Isolates a struct from false sharing: lines are 64 bytes, doubled because x86 prefetches them in adjacent pairs.</summary>
    internal static class Padding
    {
        internal const int Size = 128;
    }

    /// <summary>Written only by the reader.</summary>
    [StructLayout(LayoutKind.Sequential, Size = Padding.Size)]
    internal struct ReaderCounters
    {
        internal long ObservedPages;
        internal long Sink;
        internal long Violations;
    }

    /// <summary>The page pointer the reclaimer publishes and the reader loads every round.</summary>
    [StructLayout(LayoutKind.Sequential, Size = Padding.Size)]
    internal struct PageHandoff
    {
        internal long CurPage;
    }

    /// <summary>Written only by the reclaimer.</summary>
    [StructLayout(LayoutKind.Sequential, Size = Padding.Size)]
    internal struct ReclaimerCounters
    {
        internal long Drains;
        internal long Quarantines;
    }

    /// <summary>Written only by the disturbers, once each on exit.</summary>
    [StructLayout(LayoutKind.Sequential, Size = Padding.Size)]
    internal struct DisturberCounters
    {
        internal long Sink;
    }

    /// <summary>Shared counters, one cache line per writer so no two threads share a line.</summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct Counters
    {
        internal ReaderCounters Reader;
        internal PageHandoff Handoff;
        internal ReclaimerCounters Reclaimer;
        internal DisturberCounters Disturber;
    }

    /// <summary>
    /// Store-buffer litmus over one epoch instance, detecting a use-after-free logically rather
    /// than by hardware fault. <typeparamref name="TEpoch"/> selects the epoch under test:
    /// <see cref="FixedEpoch"/> is expected to pass, <see cref="BuggyEpoch"/> to fail.
    ///
    /// The reader announces its epoch then loads a shared page pointer; the reclaimer unlinks that
    /// pointer and retires the page. If the reclaimer's scan misses the announce, the epoch
    /// authorises the free while the reader is inside the page. "Freeing" stamps a poison sentinel,
    /// so a reader that observes poison in a page it was protecting is a use-after-free.
    ///
    /// Pages are pooled and never unmapped, so no round allocates or enters the kernel.
    /// </summary>
    internal sealed unsafe class QuarantineLitmus<TEpoch> where TEpoch : struct, IEpochUnderTest
    {
        const nuint PageSize = 4096;

        /// <summary>
        /// Sized so the pool is far larger than L2: a cache-cold page makes the reader's
        /// dereference slow enough that it is still inside the page when the poison lands.
        /// Load-bearing, not just an allocation pool -- against the unfixed epoch, 15s runs
        /// reported 12 and 491 violations at 1024 pages but 0, 1 and 0 at 8 and 64.
        /// </summary>
        const int PoolPages = 1024;

        const long Poison = unchecked((long)0xDEAD_BEEF_DEAD_BEEFUL);
        const int JoinTimeoutMs = 5000;

        private readonly TEpoch epoch;
        private readonly TwoThreadBarrier barrier = new();
        private readonly TimeSpan duration;
        private readonly int deref;
        private readonly CoreLayout cores;
        private readonly bool selfTest;

        // BumpCurrentEpoch defers the callback, so it must stay bound to the page that was retired.
        // Building them once keeps that binding without allocating in the race loop.
        private readonly Action[] drainCallbacks = new Action[PoolPages];

        private PagePool pool;
        private Counters* counters;
        private int wordIndexMask;

        private ref long ObservedPages => ref counters->Reader.ObservedPages;
        private ref long Sink => ref counters->Reader.Sink;
        private ref long Violations => ref counters->Reader.Violations;
        private ref long CurPage => ref counters->Handoff.CurPage;
        private ref long Drains => ref counters->Reclaimer.Drains;
        private ref long Quarantines => ref counters->Reclaimer.Quarantines;
        private ref long DisturberSink => ref counters->Disturber.Sink;

        internal QuarantineLitmus(TEpoch epoch, TimeSpan duration, int deref, CoreLayout cores, bool selfTest = false)
        {
            this.epoch = epoch;
            this.duration = duration;
            this.deref = deref;
            this.cores = cores;
            this.selfTest = selfTest;
        }

        internal QuarantineLitmusResult Run()
        {
            pool = new PagePool(PageSize, PoolPages);
            wordIndexMask = pool.WordIndexMask;

            // Its own mapping, so the counters cannot land in a page the reclaimer poisons.
            counters = (Counters*)Platform.MapPage(PageSize);
            var threadsExited = false;
            try
            {
                for (var slot = 0; slot < PoolPages; slot++)
                {
                    var page = (long)pool.Page(slot);
                    drainCallbacks[slot] = () => Quarantine(page);
                }

                var reader = new Thread(ReaderLoop) { IsBackground = true, Name = "litmus-reader", Priority = ThreadPriority.Highest };
                reader.Start();

                // Disturbers only read the epoch table, so they cannot influence any epoch decision.
                // They keep its cache lines shared, so an announce must first take the line
                // exclusive -- and since x86 store buffers commit in order, that pins the announce
                // in the buffer long enough for a missing StoreLoad fence to show.
                var disturbers = new Thread[cores.DisturberCores.Length];
                for (var i = 0; i < disturbers.Length; i++)
                {
                    var core = cores.DisturberCores[i];
                    disturbers[i] = new Thread(() => DisturberLoop(core)) { IsBackground = true, Name = $"litmus-disturber{core}" };
                    disturbers[i].Start();
                }

                Pin(cores.ReclaimerCore);

                var stopwatch = Stopwatch.StartNew();
                var rounds = ReclaimerLoop();
                stopwatch.Stop();

                threadsExited = reader.Join(JoinTimeoutMs);
                foreach (var disturber in disturbers)
                    threadsExited &= disturber.Join(JoinTimeoutMs);

                return new QuarantineLitmusResult
                {
                    Violations = Volatile.Read(ref Violations),
                    SampledRounds = Volatile.Read(ref ObservedPages),
                    Rounds = rounds,
                    Drains = Volatile.Read(ref Drains),
                    Quarantines = Volatile.Read(ref Quarantines),
                    Elapsed = stopwatch.Elapsed
                };
            }
            finally
            {
                // A thread that outlived the join is still dereferencing the pool and the epoch, so
                // tearing either down would turn a hang into an access violation. Leak instead.
                if (threadsExited)
                {
                    pool.Dispose();
                    Platform.Unmap((byte*)counters, PageSize);
                    epoch.Dispose();
                }
                else
                {
                    Console.Error.WriteLine($"warning: a harness thread did not exit within {JoinTimeoutMs}ms; leaving the page pool mapped rather than pulling it out from under a live thread");
                }
            }
        }

        /// <summary>Pin the calling thread. The core layout is what creates the race, so a failure to pin invalidates the run outright.</summary>
        void Pin(int core)
        {
            if (!Platform.TryPin(core))
                Environment.FailFast($"could not pin a harness thread to processor {core}; the litmus depends on the core layout, so the run would be meaningless");
        }

        void DisturberLoop(int core)
        {
            Pin(core);

            long local = 0;
            while (!barrier.Stop)
            {
                for (var i = 1; i <= epoch.EntryCount; i++)
                    local += epoch.TestHookAnnouncedEpochAt(i);
            }

            _ = Interlocked.Add(ref DisturberSink, local);
        }

        void ReaderLoop()
        {
            Pin(cores.ReaderCore);

            while (true)
            {
                barrier.WaitAtStart();

                // Nothing may sit between the barrier and Resume(). The window is a few instructions
                // wide, and arriving late drains the announce out of the store buffer before the
                // reclaimer scans. Hence the shutdown check lives after WaitAtEnd.
                //
                // Resume-then-ProtectAndDrain mirrors ClientSession.UnsafeResumeThread, which calls
                // Resume and then InternalRefresh.
                epoch.Resume();
                epoch.ProtectAndDrain();

                ReadAndCheck();

                epoch.Suspend();
                barrier.WaitAtEnd();

                // After WaitAtEnd so it stays out of the window above. Depart() because the
                // reclaimer's Shutdown may already be waiting in a pass this thread will not enter.
                if (barrier.Stop)
                {
                    barrier.Depart();
                    return;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ReadAndCheck()
        {
            var pageAddress = CurPage;
            if (pageAddress == 0)
                return;

            ObservedPages++;

            var page = (long*)pageAddress;
            long accumulator = 0;
            var poisoned = false;
            for (var index = 0; index < deref; index++)
            {
                var value = page[index & wordIndexMask];
                poisoned |= value == Poison;
                accumulator += value;
            }

            Sink += accumulator;

            if (poisoned)
                _ = Interlocked.Increment(ref Violations);
        }

        /// <summary>
        /// BumpCurrentEpoch asserts ThisInstanceProtected(), so the retiring thread holds and
        /// refreshes an epoch every round, the way Tsavorite drives it in production.
        /// </summary>
        long ReclaimerLoop()
        {
            epoch.Resume();

            var deadline = Environment.TickCount64 + (long)duration.TotalMilliseconds;
            long round = 0;
            while (Environment.TickCount64 < deadline)
            {
                var page = pool.PageForRound(round);
                pool.Fill(page);
                Volatile.Write(ref CurPage, (long)page);

                barrier.WaitAtStart();

                CurPage = 0;

                // Poison unconditionally, as if the epoch had wrongly cleared the page every round.
                if (selfTest)
                    Quarantine((long)page);

                epoch.BumpCurrentEpoch(drainCallbacks[round % PoolPages]);
                epoch.ProtectAndDrain();
                Drains++;
                barrier.WaitAtEnd();
                round++;
            }

            barrier.Shutdown();
            epoch.Suspend();
            return round;
        }

        /// <summary>Stands in for the unmap: stamping the page destroys any value a still-protected reader could legitimately see.</summary>
        void Quarantine(long page)
        {
            _ = Interlocked.Increment(ref Quarantines);
            pool.Stamp(page, Poison);
        }
    }
}