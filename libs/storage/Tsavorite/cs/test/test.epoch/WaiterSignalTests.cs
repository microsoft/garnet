// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.epoch
{
    /// <summary>
    /// The waiter semaphore that parks threads when the epoch table has no free slot. A thread that
    /// releases a slot wakes a waiter, and the signals it issues must stay bounded by the number of
    /// waiters: signalling once per release for as long as any waiter exists grows the semaphore's
    /// count without bound until <see cref="System.Threading.SemaphoreSlim.Release()"/> throws
    /// <see cref="SemaphoreFullException"/>, which then escapes every epoch release process-wide.
    /// </summary>
    [TestFixture]
    public class WaiterSignalTests : EpochTestBase
    {
        /// <summary>Longest any of these runs should ever take; exceeded only by a lost wakeup.</summary>
        static readonly TimeSpan JoinTimeout = TimeSpan.FromSeconds(120);

        /// <summary>What a single oversubscribed run observed.</summary>
        readonly struct RunResult
        {
            /// <summary>Total refresh cycles completed across all threads.</summary>
            internal long Cycles { get; init; }

            /// <summary>Highest unconsumed signal count seen while the threads ran.</summary>
            internal int MaxOutstandingSignals { get; init; }

            /// <summary>Highest signal reservation count seen while the threads ran.</summary>
            internal int MaxPendingSignals { get; init; }

            /// <summary>Highest number of threads seen blocked waiting for a slot.</summary>
            internal int MaxWaiters { get; init; }
        }

        [Test]
        public void SignalCountStaysBoundedWhenTheTableIsOversubscribed()
        {
            var threadCount = LightEpoch.TestHookTableSize + 8;
            var result = RunOversubscribed(threadCount, cyclesPerThread: 2_000);

            Assert.Multiple(() =>
            {
                Assert.That(result.MaxWaiters, Is.GreaterThan(0),
                    "the table was never actually oversubscribed, so nothing was exercised");
                Assert.That(result.Cycles, Is.GreaterThan(10_000),
                    "too few epoch releases for the bound to mean anything");
                Assert.That(result.MaxOutstandingSignals, Is.LessThanOrEqualTo(threadCount),
                    "unconsumed signals outgrew the waiters that can consume them, so the count is unbounded");
                Assert.That(result.MaxPendingSignals, Is.LessThanOrEqualTo(threadCount),
                    "signal reservations outgrew the waiters they were taken for");
                Assert.That(result.MaxOutstandingSignals, Is.LessThanOrEqualTo(result.MaxPendingSignals),
                    "every signal must be covered by a reservation taken before it was issued");
            });
        }

        /// <summary>
        /// Suppressing a signal must never strand a waiter: with far more threads than slots, most of
        /// them are parked at any moment and nearly every release is a candidate for suppression.
        /// </summary>
        [Test]
        public void EveryThreadCompletesWhenTheTableIsOversubscribed()
        {
            var threadCount = LightEpoch.TestHookTableSize + 32;
            var result = RunOversubscribed(threadCount, cyclesPerThread: 500);

            Assert.Multiple(() =>
            {
                Assert.That(result.MaxWaiters, Is.GreaterThan(0),
                    "the table was never actually oversubscribed, so nothing was exercised");
                Assert.That(result.Cycles, Is.EqualTo((long)threadCount * 500),
                    "every thread must complete all of its cycles");
                Assert.That(epoch.TestHookWaiterCount, Is.Zero, "a thread was left waiting for a slot");
            });
        }

        /// <summary>
        /// Signals left over from one contention burst must not carry into the next. Accumulating across
        /// bursts is what takes a long-lived server to the overflow.
        /// </summary>
        [Test]
        public void SignalsDrainAfterContentionEnds()
        {
            var threadCount = LightEpoch.TestHookTableSize + 8;

            for (var burst = 1; burst <= 3; burst++)
            {
                var result = RunOversubscribed(threadCount, cyclesPerThread: 500);

                Assert.That(result.MaxWaiters, Is.GreaterThan(0),
                    $"burst {burst} never oversubscribed the table, so nothing was exercised");
                Assert.That(epoch.TestHookPendingWaiterSignals, Is.LessThanOrEqualTo(threadCount),
                    $"signal reservations accumulated across bursts (after burst {burst})");
                Assert.That(epoch.TestHookOutstandingWaiterSignals, Is.LessThanOrEqualTo(threadCount),
                    $"unconsumed signals accumulated across bursts (after burst {burst})");
            }
        }

        /// <summary>
        /// Run <paramref name="threadCount"/> threads, more than the table has slots, each completing
        /// <paramref name="cyclesPerThread"/> refreshes. Slots are filled first and the surplus threads
        /// are confirmed parked before any of them is allowed to churn, so the run always starts from a
        /// genuinely oversubscribed table. <see cref="LightEpoch.ProtectAndDrain"/> then hops through
        /// <c>SuspendResume</c> for as long as a waiter exists, so every cycle releases a slot while the
        /// threads that lost the race for one are parked on the semaphore. A sampler thread watches the
        /// signal counters throughout.
        /// </summary>
        RunResult RunOversubscribed(int threadCount, int cyclesPerThread)
        {
            var slotCount = LightEpoch.TestHookTableSize;
            Assert.That(threadCount, Is.GreaterThan(slotCount), "the run must ask for more threads than the table has slots");

            var contenderCount = threadCount - slotCount;
            var slotsFilled = new CountdownEvent(slotCount);
            var go = new ManualResetEventSlim();
            var running = true;
            long cycles = 0;
            var maxOutstandingSignals = 0;
            var maxPendingSignals = 0;
            var maxWaiters = 0;

            void Churn()
            {
                try
                {
                    for (var cycle = 0; cycle < cyclesPerThread; cycle++)
                        epoch.ProtectAndDrain();
                }
                finally
                {
                    epoch.Suspend();
                }

                _ = Interlocked.Add(ref cycles, cyclesPerThread);
            }

            var sampler = new Thread(() =>
            {
                while (Volatile.Read(ref running))
                {
                    RecordMax(ref maxOutstandingSignals, epoch.TestHookOutstandingWaiterSignals);
                    RecordMax(ref maxPendingSignals, epoch.TestHookPendingWaiterSignals);
                    RecordMax(ref maxWaiters, epoch.TestHookWaiterCount);
                    Thread.Yield();
                }
            })
            { IsBackground = true, Name = $"{nameof(RunOversubscribed)}-sampler" };

            var workers = new Thread[threadCount];

            // Holders take every slot and park, so the table is provably full before anyone else arrives.
            for (var i = 0; i < slotCount; i++)
            {
                workers[i] = new Thread(() =>
                {
                    epoch.Resume();
                    slotsFilled.Signal();
                    go.Wait();
                    Churn();
                })
                { IsBackground = true, Name = $"{nameof(RunOversubscribed)}-holder-{i}" };
            }

            // Contenders find no slot and block in ReserveEntryWait until a holder releases one.
            for (var i = slotCount; i < threadCount; i++)
            {
                workers[i] = new Thread(() =>
                {
                    epoch.Resume();
                    Churn();
                })
                { IsBackground = true, Name = $"{nameof(RunOversubscribed)}-contender-{i}" };
            }

            try
            {
                sampler.Start();

                for (var i = 0; i < slotCount; i++)
                    workers[i].Start();
                Assert.That(slotsFilled.Wait(JoinTimeout), Is.True, "the holders never filled the epoch table");

                for (var i = slotCount; i < threadCount; i++)
                    workers[i].Start();
                WaitForWaiters(contenderCount);

                go.Set();

                foreach (var worker in workers)
                    Assert.That(worker.Join(JoinTimeout), Is.True, $"{worker.Name} never finished; a waiter was likely stranded");
            }
            finally
            {
                // Unblock the holders even if an assertion above fired, so no thread is left parked.
                go.Set();
                Volatile.Write(ref running, false);
                _ = sampler.Join(JoinTimeout);

                slotsFilled.Dispose();
                go.Dispose();
            }

            return new RunResult
            {
                Cycles = Volatile.Read(ref cycles),
                MaxOutstandingSignals = Volatile.Read(ref maxOutstandingSignals),
                MaxPendingSignals = Volatile.Read(ref maxPendingSignals),
                MaxWaiters = Volatile.Read(ref maxWaiters)
            };
        }

        /// <summary>Block until <paramref name="expected"/> threads are parked waiting for a slot.</summary>
        void WaitForWaiters(int expected)
        {
            var deadline = DateTime.UtcNow + JoinTimeout;
            while (epoch.TestHookWaiterCount < expected)
            {
                Assert.That(DateTime.UtcNow, Is.LessThan(deadline),
                    $"only {epoch.TestHookWaiterCount} of {expected} threads parked waiting for a slot");
                Thread.Yield();
            }
        }

        static void RecordMax(ref int target, int observed)
        {
            var max = Volatile.Read(ref target);
            while (observed > max)
            {
                var prev = Interlocked.CompareExchange(ref target, observed, max);
                if (prev == max)
                    return;
                max = prev;
            }
        }
    }
}