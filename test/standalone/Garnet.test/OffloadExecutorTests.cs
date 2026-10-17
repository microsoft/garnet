// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class OffloadExecutorTests : TestBase
    {
        static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

        sealed class WorkItem(Action action) : IThreadPoolWorkItem
        {
            public void Execute() => action();
        }

        /// <summary>
        /// With the one thread busy, an item posted with <c>mustRun</c> starts a thread of its own and runs ahead of an
        /// item queued before it. The busy item here waits for the one that must run, as a command waiting for a key a
        /// transaction has locked waits for that transaction.
        /// </summary>
        [Test]
        public void AnItemThatMustRunGetsAThreadPastTheLimitAheadOfTheQueue()
        {
            using var executor = new OffloadExecutor(maxThreads: 1, TimeSpan.FromSeconds(30));
            using var blockerStarted = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            using var queuedRan = new ManualResetEventSlim();
            using var mustRunRan = new ManualResetEventSlim();
            var queuedRanAfterMustRun = false;

            try
            {
                executor.Post(new WorkItem(() => { blockerStarted.Set(); release.Wait(); }), mustRun: false);
                ClassicAssert.IsTrue(blockerStarted.Wait(Deadline));

                executor.Post(new WorkItem(() => { queuedRanAfterMustRun = mustRunRan.IsSet; queuedRan.Set(); }), mustRun: false);
                executor.Post(new WorkItem(() => { mustRunRan.Set(); release.Set(); }), mustRun: true);

                ClassicAssert.IsTrue(mustRunRan.Wait(Deadline), "An item that must run waited for a thread at the limit.");
                ClassicAssert.IsTrue(queuedRan.Wait(Deadline), "An item queued at the limit never ran.");
                ClassicAssert.IsTrue(queuedRanAfterMustRun, "An item that must run did not run ahead of the queue.");
            }
            finally
            {
                release.Set();
            }
        }

        [Test]
        public void IdleThreadsRetireAndAreStartedAgainForLaterItems()
        {
            using var executor = new OffloadExecutor(maxThreads: 4, TimeSpan.FromMilliseconds(50));
            using var ran = new SemaphoreSlim(0);

            executor.Post(new WorkItem(() => ran.Release()), mustRun: false);
            ClassicAssert.IsTrue(ran.Wait(Deadline));
            ClassicAssert.IsTrue(SpinWait.SpinUntil(() => executor.ThreadCount == 0, Deadline), "An idle thread did not retire.");

            executor.Post(new WorkItem(() => ran.Release()), mustRun: false);
            ClassicAssert.IsTrue(ran.Wait(Deadline), "An item posted after every thread retired never ran.");
        }

        /// <summary>
        /// Items posted from many threads at once, with threads retiring between bursts, each run exactly once and on an
        /// executor thread. A post that races a thread going idle or retiring must still be picked up.
        /// </summary>
        [Test]
        public void ItemsPostedConcurrentlyWhileThreadsRetireEachRunOnceOnAnExecutorThread()
        {
            const int Producers = 8;
            const int PerProducer = 20_000;

            using var executor = new OffloadExecutor(maxThreads: 2, TimeSpan.FromMilliseconds(1));
            var runs = new int[Producers * PerProducer];
            var remaining = runs.Length;
            var offExecutor = 0;
            using var done = new ManualResetEventSlim();

            var producers = new Task[Producers];
            for (var p = 0; p < Producers; p++)
            {
                var first = p * PerProducer;
                producers[p] = Task.Factory.StartNew(() =>
                {
                    for (var i = first; i < first + PerProducer; i++)
                    {
                        var id = i;
                        executor.Post(new WorkItem(() =>
                        {
                            if (!OffloadExecutor.OnExecutorThread)
                                _ = Interlocked.Increment(ref offExecutor);
                            _ = Interlocked.Increment(ref runs[id]);
                            if (Interlocked.Decrement(ref remaining) == 0)
                                done.Set();
                        }), mustRun: id % 64 == 0);

                        // Lets the threads go idle and retire between bursts
                        if (id % 1000 == 0)
                            Thread.Sleep(2);
                    }
                }, TaskCreationOptions.LongRunning);
            }

            ClassicAssert.IsTrue(Task.WaitAll(producers, Deadline));
            ClassicAssert.IsTrue(done.Wait(Deadline), $"{Volatile.Read(ref remaining)} posted items never ran.");
            ClassicAssert.AreEqual(0, offExecutor, "An item ran on a thread other than an executor thread.");
            for (var i = 0; i < runs.Length; i++)
                ClassicAssert.AreEqual(1, runs[i], $"Item {i} ran {runs[i]} times.");
            ClassicAssert.IsFalse(OffloadExecutor.OnExecutorThread);
        }

        /// <summary>
        /// Disposing hands queued items to the thread pool, and items posted afterwards run there too: an item is a
        /// parked session's resume, which nothing else would run.
        /// </summary>
        [Test]
        public void ItemsQueuedAtDisposeOrPostedAfterItStillRun()
        {
            var executor = new OffloadExecutor(maxThreads: 1, TimeSpan.FromSeconds(30));
            using var blockerStarted = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            using var queuedRan = new ManualResetEventSlim();
            using var postedRan = new ManualResetEventSlim();

            try
            {
                executor.Post(new WorkItem(() => { blockerStarted.Set(); release.Wait(); }), mustRun: false);
                ClassicAssert.IsTrue(blockerStarted.Wait(Deadline));

                executor.Post(new WorkItem(queuedRan.Set), mustRun: false);
                executor.Dispose();
                ClassicAssert.IsTrue(queuedRan.Wait(Deadline), "An item queued when the executor was disposed never ran.");

                executor.Post(new WorkItem(postedRan.Set), mustRun: false);
                ClassicAssert.IsTrue(postedRan.Wait(Deadline), "An item posted after the executor was disposed never ran.");
            }
            finally
            {
                release.Set();
                executor.Dispose();
            }
        }
    }
}