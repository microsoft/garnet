// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.epoch.litmus
{
    /// <summary>How the toy suspender clears its slot before reading drainCount.</summary>
    internal enum ToyRelease
    {
        /// <summary>Volatile.Write: a plain store on x64, which can still sit in the store buffer when drainCount is read.</summary>
        Broken,

        /// <summary>Interlocked.Exchange: a locked instruction that drains the store buffer first.</summary>
        FixedExchange,

        /// <summary>Volatile.Write followed by Thread.MemoryBarrier: a full fence between the store and the load.</summary>
        FixedBarrier,
    }

    /// <summary>Outcome of one <see cref="ToyLitmus"/> run.</summary>
    internal readonly struct ToyLitmusResult
    {
        internal ToyRelease Release { get; init; }

        internal long Rounds { get; init; }

        /// <summary>Drains executed. Each round registers exactly one, so a correct release executes one per round.</summary>
        internal long Executed { get; init; }

        /// <summary>Drains that never ran: <see cref="Rounds"/> minus <see cref="Executed"/>.</summary>
        internal long Missed => Rounds - Executed;

        /// <summary>Drains run by the suspender, after it saw the bumper's increment.</summary>
        internal long DrainedBySuspender { get; init; }

        /// <summary>Drains run by the bumper, after it saw the suspender's cleared slot.</summary>
        internal long DrainedByBumper { get; init; }

        internal TimeSpan Elapsed { get; init; }

        public override string ToString()
            => $"{Release,-13} missed={Missed:N0} executed={Executed:N0} rounds={Rounds:N0} drainedBySuspender={DrainedBySuspender:N0} "
             + $"drainedByBumper={DrainedByBumper:N0} elapsed={Elapsed.TotalSeconds:F1}s";
    }

    /// <summary>
    /// The drain race reduced to its two memory operations per side, without LightEpoch.
    ///
    /// The suspender clears its slot, then reads drainCount (LightEpoch's Release, then the Suspend check).
    /// The bumper increments drainCount, then reads the slot (BumpCurrentEpoch, then the SuspendDrain scan).
    /// Whichever side sees the other's store tries to run the drain; both may, so the drain is claimed first
    /// and runs at most once. If neither side sees the other's store, it never runs. That requires the
    /// suspender's store to be reordered after its load, which only the store buffer can do, and only when
    /// the store is not fenced. A correct release therefore executes exactly one drain per round.
    /// </summary>
    internal sealed class ToyLitmus
    {
        /// <summary>Each field on its own cache line, so the two sides only share the lines the race needs.</summary>
        [StructLayout(LayoutKind.Explicit, Size = 6 * Padding.Size)]
        struct Shared
        {
            [FieldOffset(0 * Padding.Size)] internal int Slot;
            [FieldOffset(1 * Padding.Size)] internal int DrainCount;
            [FieldOffset(2 * Padding.Size)] internal int Pending;
            [FieldOffset(3 * Padding.Size)] internal long Executed;
            [FieldOffset(4 * Padding.Size)] internal long DrainedBySuspender;
            [FieldOffset(5 * Padding.Size)] internal long DrainedByBumper;
        }

        /// <summary>Delay range, in loop iterations, swept to slide the suspender's store across the bumper's increment.</summary>
        const int JitterMask = 63;
        const int JoinTimeoutMs = 5000;

        private readonly ToyRelease release;
        private readonly RunLimit limit;
        private readonly CoreLayout cores;
        private readonly PairBarrier start = new();
        private readonly PairBarrier end = new();
        private Shared shared;
        private volatile bool stop;
        private long delaySink;

        internal ToyLitmus(ToyRelease release, RunLimit limit, CoreLayout cores)
        {
            this.release = release;
            this.limit = limit;
            this.cores = cores;
        }

        internal ToyLitmusResult Run()
        {
            var suspender = new Thread(SuspenderDriver) { IsBackground = true, Name = "toy-suspender", Priority = ThreadPriority.Highest };
            suspender.Start();

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var rounds = BumperDriver();
            stopwatch.Stop();

            if (!suspender.Join(JoinTimeoutMs))
                throw new InvalidOperationException($"The toy suspender did not exit within {JoinTimeoutMs}ms; the run is invalid.");

            return new ToyLitmusResult
            {
                Release = release,
                Rounds = rounds,
                Executed = Volatile.Read(ref shared.Executed),
                DrainedBySuspender = Volatile.Read(ref shared.DrainedBySuspender),
                DrainedByBumper = Volatile.Read(ref shared.DrainedByBumper),
                Elapsed = stopwatch.Elapsed
            };
        }

        // Suspender side: clear the slot, then check for a pending drain.

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Foo_Broken()
        {
            Volatile.Write(ref shared.Slot, 0);
            if (Volatile.Read(ref shared.DrainCount) > 0)
                TryDrain(ref shared.DrainedBySuspender);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Foo_Fixed_Exchange()
        {
            _ = Interlocked.Exchange(ref shared.Slot, 0);
            if (Volatile.Read(ref shared.DrainCount) > 0)
                TryDrain(ref shared.DrainedBySuspender);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Foo_Fixed_Barrier()
        {
            Volatile.Write(ref shared.Slot, 0);
            Thread.MemoryBarrier();
            if (Volatile.Read(ref shared.DrainCount) > 0)
                TryDrain(ref shared.DrainedBySuspender);
        }

        // Bumper side: register the drain, then check whether the suspender has left.

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Bar()
        {
            // Pending is published before drainCount; the locked increment orders the two stores.
            shared.Pending = 1;
            _ = Interlocked.Increment(ref shared.DrainCount);
            if (Volatile.Read(ref shared.Slot) == 0)
                TryDrain(ref shared.DrainedByBumper);
        }

        /// <summary>
        /// Claims the drain before running it, as LightEpoch claims a drain-list entry, so a round in which both
        /// sides see the other's store still executes it once. Each counter is written by one thread only.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void TryDrain(ref long drainedBy)
        {
            if (Interlocked.CompareExchange(ref shared.Pending, 0, 1) == 1)
            {
                _ = Interlocked.Increment(ref shared.Executed);
                drainedBy++;
            }
        }

        void SuspenderDriver()
        {
            Pin(cores.ReaderCore);

            // Spread consecutive rounds across the delay range rather than ramping through it.
            const int stride = 23;
            var jitter = 0;

            while (true)
            {
                start.Wait();
                if (stop)
                    return;

                Delay(jitter);
                jitter = (jitter + stride) & JitterMask;

                switch (release)
                {
                    case ToyRelease.Broken: Foo_Broken(); break;
                    case ToyRelease.FixedExchange: Foo_Fixed_Exchange(); break;
                    default: Foo_Fixed_Barrier(); break;
                }

                end.Wait();
            }
        }

        long BumperDriver()
        {
            Pin(cores.ReclaimerCore);

            var deadline = limit.StartDeadline();
            long rounds = 0;

            while (true)
            {
                // The suspender is waiting at start, so this reset is published by the barrier's interlocked arrival.
                shared.Slot = 1;
                shared.DrainCount = 0;
                shared.Pending = 0;

                if (limit.IsReached(rounds, deadline))
                {
                    stop = true;
                    start.Wait();
                    return rounds;
                }

                start.Wait();
                Bar();
                end.Wait();
                rounds++;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Delay(int iterations)
        {
            long local = 0;
            for (var i = 0; i < iterations; i++)
                local += Volatile.Read(ref delaySink);
            if (local == long.MinValue)
                delaySink = local;
        }

        static void Pin(int core)
        {
            if (!Platform.TryPin(core))
                Environment.FailFast($"could not pin a toy thread to processor {core}; the litmus depends on the core layout, so the run would be meaningless");
        }
    }
}