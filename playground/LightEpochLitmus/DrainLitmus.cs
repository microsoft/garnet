// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.epoch.litmus
{
    internal static class Padding
    {
        internal const int Size = 128;
    }

    /// <summary>Outcome of a <see cref="DrainLitmus{TEpoch}"/> run.</summary>
    internal sealed class DrainLitmusResult
    {
        /// <summary>Rounds that ended with both threads suspended and the drain action still pending.</summary>
        internal long Stranded { get; init; }

        /// <summary>Rounds completed.</summary>
        internal long Rounds { get; init; }

        /// <summary>
        /// Actions run by the suspender's Suspend(), meaning it read drainCount after the bump. Together with
        /// <see cref="DrainedByBumper"/> this shows the jitter sweep crossed the two orderings; if either is 0
        /// the race window was never straddled and a clean result says nothing.
        /// </summary>
        internal long DrainedBySuspender { get; init; }

        /// <summary>Actions run by the bumper, from its own Suspend() or a later Resume().</summary>
        internal long DrainedByBumper { get; init; }

        internal TimeSpan Elapsed { get; init; }

        public override string ToString()
            => $"stranded={Stranded:N0} rounds={Rounds:N0} drainedBySuspender={DrainedBySuspender:N0} "
             + $"drainedByBumper={DrainedByBumper:N0} elapsed={Elapsed.TotalSeconds:F1}s";
    }

    /// <summary>Shared counters, one padded line per writer.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 4 * Padding.Size)]
    internal struct DrainCounters
    {
        [FieldOffset(0 * Padding.Size)] internal long Executed;
        [FieldOffset(1 * Padding.Size)] internal long DrainedBySuspender;
        [FieldOffset(2 * Padding.Size)] internal long DisturberSink;
        [FieldOffset(3 * Padding.Size)] internal long JitterSink;
    }

    /// <summary>Sense-reversing rendezvous for exactly two threads; <see cref="Abandon"/> releases a waiter for good.</summary>
    internal sealed class PairBarrier
    {
        private int count;
        private int sense;
        private volatile bool abandoned;

        internal void Abandon() => abandoned = true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Wait()
        {
            var local = Volatile.Read(ref sense);
            if (Interlocked.Increment(ref count) == 2)
            {
                count = 0;
                Volatile.Write(ref sense, local ^ 1);
                return;
            }

            // The rendezvous is the slow path: yield progressively until the other thread arrives.
            var spinner = new SpinWait();
            while (Volatile.Read(ref sense) == local && !abandoned)
                spinner.SpinOnce(-1);
        }
    }

    /// <summary>
    /// Store-buffer litmus for the Release side of <see cref="IEpochUnderTest.Suspend"/>: a lost drain.
    ///
    /// Both threads hold the epoch. The suspender leaves (slot clear, then drainCount read) while the bumper
    /// registers an action (drainCount increment, then a scan of the slots on its own Suspend). Each side
    /// stores then loads what the other stores, so unless the slot clear is fenced, both loads can miss:
    /// the suspender sees no pending drain and the bumper still sees the suspender's slot occupied. Both
    /// then return from Suspend() with the action pending and nobody left in the epoch to run it -- in
    /// GarnetClient, a read-only range whose requests are never written to the socket.
    ///
    /// After both Suspend() calls return, the action must have run. A round that ends with it pending is a
    /// violation. The next round's Resume() runs it, so each round starts clean.
    /// </summary>
    internal sealed class DrainLitmus<TEpoch> where TEpoch : struct, IEpochUnderTest
    {
        /// <summary>Jitter range, in loop iterations, swept to slide the suspender's Suspend() across the bumper's increment.</summary>
        const int JitterMask = 1023;
        const int JoinTimeoutMs = 5000;

        private readonly TEpoch epoch;
        private readonly TimeSpan duration;
        private readonly CoreLayout cores;
        private readonly PairBarrier start = new();
        private readonly PairBarrier end = new();
        private readonly PairBarrier checkpoint = new();
        private readonly Action onDrain;

        private DrainCounters counters;
        private volatile bool stop;
        private volatile bool disturbersStop;
        private int suspenderThreadId;

        internal DrainLitmus(TEpoch epoch, TimeSpan duration, CoreLayout cores)
        {
            this.epoch = epoch;
            this.duration = duration;
            this.cores = cores;
            onDrain = OnDrain;
        }

        internal DrainLitmusResult Run()
        {
            var suspender = new Thread(SuspenderLoop) { IsBackground = true, Name = "litmus-suspender", Priority = ThreadPriority.Highest };
            suspender.Start();

            // Disturbers only read the epoch table. Holding its lines shared makes the suspender's slot clear
            // wait for ownership of the line, which keeps it in the store buffer long enough to matter.
            var disturbers = new Thread[cores.DisturberCores.Length];
            for (var i = 0; i < disturbers.Length; i++)
            {
                var core = cores.DisturberCores[i];
                disturbers[i] = new Thread(() => DisturberLoop(core)) { IsBackground = true, Name = $"litmus-disturber{core}" };
                disturbers[i].Start();
            }

            Pin(cores.ReclaimerCore);

            var stopwatch = Stopwatch.StartNew();
            var (rounds, stranded) = BumperLoop();
            stopwatch.Stop();

            disturbersStop = true;
            var threadsExited = suspender.Join(JoinTimeoutMs);
            foreach (var disturber in disturbers)
                threadsExited &= disturber.Join(JoinTimeoutMs);

            if (!threadsExited)
                throw new InvalidOperationException($"A harness thread did not exit within {JoinTimeoutMs}ms; the run is invalid and the epoch cannot be disposed.");

            epoch.Dispose();

            var executed = Volatile.Read(ref counters.Executed);
            var bySuspender = Volatile.Read(ref counters.DrainedBySuspender);
            return new DrainLitmusResult
            {
                Stranded = stranded,
                Rounds = rounds,
                DrainedBySuspender = bySuspender,
                DrainedByBumper = executed - bySuspender,
                Elapsed = stopwatch.Elapsed
            };
        }

        static void Pin(int core)
        {
            if (!Platform.TryPin(core))
                Environment.FailFast($"could not pin a harness thread to processor {core}; the litmus depends on the core layout, so the run would be meaningless");
        }

        void OnDrain()
        {
            if (Environment.CurrentManagedThreadId == suspenderThreadId)
                _ = Interlocked.Increment(ref counters.DrainedBySuspender);
            _ = Interlocked.Increment(ref counters.Executed);
        }

        void DisturberLoop(int core)
        {
            Pin(core);

            long local = 0;
            while (!disturbersStop)
            {
                for (var i = 1; i <= epoch.EntryCount; i++)
                    local += epoch.TestHookAnnouncedEpochAt(i);
            }

            _ = Interlocked.Add(ref counters.DisturberSink, local);
        }

        void SuspenderLoop()
        {
            Pin(cores.ReaderCore);
            suspenderThreadId = Environment.CurrentManagedThreadId;

            // Spread consecutive rounds across the jitter range rather than ramping through it.
            const int stride = 389;
            var jitter = 0;

            while (true)
            {
                epoch.Resume();
                start.Wait();

                Delay(jitter);
                jitter = (jitter + stride) & JitterMask;

                // Release() clears the slot, then reads drainCount. That clear-then-read is the window.
                epoch.Suspend();

                end.Wait();
                checkpoint.Wait();

                if (stop)
                {
                    start.Abandon();
                    end.Abandon();
                    return;
                }
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void Delay(int iterations)
        {
            long local = 0;
            for (var i = 0; i < iterations; i++)
                local += Volatile.Read(ref counters.JitterSink);
            if (local == long.MinValue)
                counters.JitterSink = local;
        }

        (long Rounds, long Stranded) BumperLoop()
        {
            var deadline = Environment.TickCount64 + (long)duration.TotalMilliseconds;
            long rounds = 0, stranded = 0, bumps = 0;

            while (true)
            {
                // A drain stranded last round runs here, inside Acquire(), before this round's bump.
                epoch.Resume();
                start.Wait();

                // Interlocked increment of drainCount, then (in Suspend) a scan of every slot.
                epoch.BumpCurrentEpoch(onDrain);
                bumps++;
                epoch.Suspend();

                end.Wait();

                // Both threads have returned from Suspend() and nobody holds the epoch.
                if (Volatile.Read(ref counters.Executed) != bumps)
                    stranded++;

                rounds++;
                if (Environment.TickCount64 >= deadline)
                    stop = true;

                checkpoint.Wait();

                if (stop)
                    break;
            }

            // Run a final stranded action so the counts balance.
            epoch.Resume();
            epoch.Suspend();
            return (rounds, stranded);
        }
    }
}