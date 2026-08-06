// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test.epoch
{
    /// <summary>
    /// The drain list: deferred actions that must run once the epoch they were registered against
    /// becomes safe to reclaim, and not one moment sooner.
    /// </summary>
    [TestFixture]
    public class DrainTests : EpochTestBase
    {
        [Test]
        public void ActionRunsImmediatelyWhenNobodyElseIsProtected()
        {
            var ran = 0;

            using (epoch.ProtectedScope())
            {
                epoch.BumpCurrentEpoch(() => Interlocked.Increment(ref ran));
                Assert.That(Volatile.Read(ref ran), Is.EqualTo(1), "with no other thread protected the action's epoch is immediately reclaimable");
            }
        }

        /// <summary>
        /// <see cref="LightEpoch.Suspend"/> drains on the way out when it is the last thread to leave,
        /// so an action registered while others were protected still runs without anyone calling
        /// <see cref="LightEpoch.ProtectAndDrain"/> afterwards.
        /// </summary>
        [Test]
        public void TheLastThreadToSuspendRunsPendingActions()
        {
            var drained = 0;
            using var reader = new ParkedReaderThread(epoch);

            using (epoch.ProtectedScope())
            {
                epoch.BumpCurrentEpoch(() => Interlocked.Increment(ref drained));
            }

            Assert.That(Volatile.Read(ref drained), Is.Zero, "the action ran while a reader was still protected");

            reader.LeaveAndJoin();

            Assert.That(Volatile.Read(ref drained), Is.EqualTo(1), "the last thread to suspend must drain the pending action itself");
        }

        [Test]
        public void EveryActionRunsExactlyOnceWhenTheDrainListFills()
        {
            var capacity = LightEpoch.TestHookDrainListCapacity;
            var counts = new int[capacity];

            using var reader = new ParkedReaderThread(epoch);

            using (epoch.ProtectedScope())
            {
                for (var i = 0; i < capacity; i++)
                {
                    var index = i;
                    epoch.BumpCurrentEpoch(() => Interlocked.Increment(ref counts[index]));
                }
            }

            Assert.That(counts, Is.All.Zero, "actions ran while a reader was still protected");

            reader.LeaveAndJoin();

            Assert.That(counts, Is.All.EqualTo(1), $"every registered action must run exactly once; got [{string.Join(",", counts)}]");
        }

        /// <summary>
        /// The drain list is finite. When it is full and nothing can be reclaimed, registering another
        /// action must block rather than drop it, and must complete once the blocker leaves.
        /// </summary>
        [Test]
        public void RegisteringBlocksWhileTheDrainListIsFullAndCompletesAfterwards()
        {
            var capacity = LightEpoch.TestHookDrainListCapacity;
            var counts = new int[capacity];
            var extraRan = 0;

            using var reader = new ParkedReaderThread(epoch);

            using (epoch.ProtectedScope())
            {
                for (var i = 0; i < capacity; i++)
                {
                    var index = i;
                    epoch.BumpCurrentEpoch(() => Interlocked.Increment(ref counts[index]));
                }

                using var registered = new ManualResetEventSlim();
                var latecomer = new Thread(() =>
                {
                    using (epoch.ProtectedScope())
                    {
                        epoch.BumpCurrentEpoch(() => Interlocked.Increment(ref extraRan));
                        registered.Set();
                    }
                })
                { IsBackground = true };
                latecomer.Start();

                Assert.That(registered.Wait(TimeSpan.FromMilliseconds(100)), Is.False, "registered an action into a full drain list while nothing was reclaimable");

                reader.LeaveAndJoin();

                registered.Wait();
                latecomer.Join();

                Assert.That(counts, Is.All.EqualTo(1), "the backlog did not drain exactly once each");
            }
        }

        [Test]
        public void ActionsRunInEpochOrder()
        {
            const int ActionCount = 8;
            var order = new ConcurrentQueue<int>();

            using var reader = new ParkedReaderThread(epoch);

            using (epoch.ProtectedScope())
            {
                for (var i = 0; i < ActionCount; i++)
                {
                    var index = i;
                    epoch.BumpCurrentEpoch(() => order.Enqueue(index));
                }
            }

            Assert.That(order, Is.Empty);

            reader.LeaveAndJoin();

            Assert.That(order.ToArray(), Is.EqualTo(Enumerable.Range(0, ActionCount).ToArray()), "actions registered against increasing epochs must drain in that order");
        }

        [Test]
        public void ManyThreadsRegisteringActionsAllRunExactlyOnce()
        {
            const int ThreadCount = 8;
            const int PerThread = 200;

            var counts = new int[ThreadCount * PerThread];
            var threads = new Thread[ThreadCount];
            using var start = new ManualResetEventSlim();

            for (var t = 0; t < ThreadCount; t++)
            {
                var threadIndex = t;
                threads[t] = new Thread(() =>
                {
                    start.Wait();
                    for (var i = 0; i < PerThread; i++)
                    {
                        var index = (threadIndex * PerThread) + i;
                        using (epoch.ProtectedScope())
                        {
                            epoch.BumpCurrentEpoch(() => Interlocked.Increment(ref counts[index]));
                        }
                    }
                })
                { IsBackground = true };
                threads[t].Start();
            }

            start.Set();
            JoinAll(threads);

            Assert.That(counts, Is.All.EqualTo(1), $"{counts.Count(c => c == 0)} actions never ran and {counts.Count(c => c > 1)} ran more than once");
        }

        [Test]
        public void ActionDoesNotRunWhileAnotherThreadIsProtected()
        {
            using var protectedThreadEntered = new ManualResetEventSlim();
            using var releaseProtectedThread = new ManualResetEventSlim();

            var drained = 0;

            var reader = new Thread(() =>
            {
                epoch.Resume();
                protectedThreadEntered.Set();
                releaseProtectedThread.Wait();
                epoch.Suspend();
            })
            { IsBackground = true };
            reader.Start();
            protectedThreadEntered.Wait();

            epoch.Resume();
            epoch.BumpCurrentEpoch(() => Interlocked.Increment(ref drained));
            epoch.Suspend();

            Assert.That(Volatile.Read(ref drained), Is.Zero, "action drained while a thread was still protected");

            releaseProtectedThread.Set();
            reader.Join();

            Assert.That(Volatile.Read(ref drained), Is.EqualTo(1), "the last thread to suspend did not drain the action on its way out");
        }
    }
}