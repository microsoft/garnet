// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [TestFixture]
    public class CountingEventSlimTests : TestBase
    {
        [Test]
        public void Basic()
        {
            using var evt = CountingEventSlim.Create();

            // Starts signalled
            ClassicAssert.IsTrue(evt.Wait());

            // Signalled can be wait'd multiple times
            ClassicAssert.IsTrue(evt.Wait());

            // Once incremented blocks
            evt.Increment();
            ClassicAssert.IsFalse(evt.Wait(0));

            // Decrementing unblocks
            evt.Decrement();

            ClassicAssert.IsTrue(evt.Wait());
        }

        [Test]
        public async Task WaitAsyncCompletesOnceTheCountReachesZero()
        {
            var evt = CountingEventSlim.Create();
            try
            {
                ClassicAssert.IsTrue(evt.WaitAsync().IsCompletedSuccessfully, "A wait on a count of 0 did not complete at once.");

                evt.Increment();
                evt.Increment();
                var wait = evt.WaitAsync().AsTask();
                evt.Decrement();
                ClassicAssert.IsFalse(wait.IsCompleted, "A wait completed while the count was above 0.");

                evt.Decrement();
                await wait.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            }
            finally
            {
                evt.Dispose();
            }
        }

        [Test]
        public void WaitAsyncIsNotCompletedByAZeroBeforeIt()
        {
            var evt = CountingEventSlim.Create();
            try
            {
                evt.Increment();
                var first = evt.WaitAsync().AsTask();
                evt.Decrement();
                ClassicAssert.IsTrue(first.Wait(TimeSpan.FromSeconds(10)), "The first wait was not completed when the count reached 0.");

                evt.Increment();
                var second = evt.WaitAsync().AsTask();
                ClassicAssert.IsFalse(second.IsCompleted, "A wait was completed by the count reaching 0 before it.");

                evt.Decrement();
                ClassicAssert.IsTrue(second.Wait(TimeSpan.FromSeconds(10)), "The second wait was not completed when the count reached 0.");
            }
            finally
            {
                evt.Dispose();
            }
        }

        [Test]
        public async Task Concurrent([Values] bool waitAsync)
        {
            // Spawn a number of tasks and fill them with work from a channel
            //
            // Periodically waits for CountingEventSlim to be 0, and confirms that all work is complete when the signal is received

            const int Iters = 1_000;
            const int ItemsPerIter = 1_000;

            for (var iter = 0; iter < Iters; iter++)
            {
                using var evt = CountingEventSlim.Create();

                var channel = Channel.CreateUnbounded<int>(new() { SingleReader = false, SingleWriter = true, AllowSynchronousContinuations = false });

                var working = new ConcurrentDictionary<int, bool>();

                var tasks = new Task[Math.Max(Environment.ProcessorCount, 4)];

                for (var i = 0; i < tasks.Length; i++)
                {
                    tasks[i] =
                        Task.Run(
                            async () =>
                            {
                                await foreach (var item in channel.Reader.ReadAllAsync())
                                {
                                    ClassicAssert.IsTrue(working.TryRemove(item, out _));
                                    evt.Decrement();
                                }
                            }
                        );
                }

                var ix = 0;
                while (ix < ItemsPerIter)
                {
                    var toAdd = Random.Shared.Next(ItemsPerIter - ix) + 1;

                    for (var i = 0; i < toAdd; i++)
                    {
                        ClassicAssert.IsTrue(working.TryAdd(ix, true));

                        evt.Increment();
                        ClassicAssert.IsTrue(channel.Writer.TryWrite(ix));

                        ix++;
                    }

                    if (waitAsync)
                        await evt.WaitAsync().ConfigureAwait(false);
                    else
                        _ = evt.Wait();
                    ClassicAssert.IsTrue(working.IsEmpty);
                }

                channel.Writer.Complete();

                Task.WaitAll(tasks);
            }
        }
    }
}