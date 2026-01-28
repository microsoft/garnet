// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks;
using Allure.NUnit;
using Garnet.common;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Garnet.test
{
    [AllureNUnit]
    [TestFixture]
    public class CountingEventSlimTests
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
        public void Concurrent()
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

                    _ = evt.Wait();
                    ClassicAssert.IsTrue(working.IsEmpty);
                }

                channel.Writer.Complete();

                Task.WaitAll(tasks);
            }
        }
    }
}