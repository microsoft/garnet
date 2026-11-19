// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using NUnit.Framework;

namespace Garnet.test
{
    [TestFixture]
    public class LimitedFixedBufferPoolTests : TestBase
    {
        static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        [TearDown]
        public void TearDown()
        {
            TestUtils.OnTearDown();
        }

        static LimitedFixedBufferPool CreatePopulatedPool()
        {
            var pool = new LimitedFixedBufferPool(64);
            var entries = new List<PoolEntry>();
            for (var size = 64; size <= 512; size *= 2)
                for (var i = 0; i < 16; i++)
                    entries.Add(pool.Get(size));
            foreach (var entry in entries)
                entry.Dispose();
            return pool;
        }

        [TestCase("Dispose")]
        [TestCase("Purge")]
        [TestCase("GetStats")]
        public void OperationsCanRaceDisposal(string operation)
        {
            using var ready = new Barrier(2);
            for (var attempt = 0; attempt < 3000; attempt++)
            {
                using var pool = CreatePopulatedPool();
                Parallel.Invoke(
                    () =>
                    {
                        Assert.That(ready.SignalAndWait(Timeout), Is.True, "Concurrent operation did not start.");
                        switch (operation)
                        {
                            case "Dispose":
                                pool.Dispose();
                                break;
                            case "Purge":
                                for (var i = 0; i < 16; i++)
                                    pool.Purge();
                                break;
                            case "GetStats":
                                for (var i = 0; i < 16; i++)
                                    _ = pool.GetStats();
                                break;
                        }
                    },
                    () =>
                    {
                        Assert.That(ready.SignalAndWait(Timeout), Is.True, "Disposer did not start.");
                        pool.Dispose();
                    });

                Assert.That(pool.PooledBytes, Is.Zero);
                Assert.That(pool.LiveBytes, Is.Zero);
                Assert.That(pool.GetStats(), Does.Contain("totalBufferCount=0"));
                pool.Purge();
            }
        }

        [Test]
        public async Task ConcurrentDisposersWaitForOutstandingBuffers()
        {
            using var pool = CreatePopulatedPool();
            var borrowed = pool.Get(64);
            using var ready = new CountdownEvent(4);
            var disposers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
            {
                ready.Signal();
                pool.Dispose();
                Assert.That(pool.PooledBytes, Is.Zero, "Dispose returned before teardown completed.");
                Assert.That(pool.LiveBytes, Is.Zero, "Dispose returned while a buffer was still checked out.");
            })).ToArray();

            try
            {
                Assert.That(ready.Wait(Timeout), Is.True, "Concurrent disposers did not start.");
                Assert.That(disposers.All(task => !task.IsCompleted), Is.True,
                    "Dispose must wait for the outstanding buffer.");
            }
            finally
            {
                borrowed.Dispose();
                await Task.WhenAll(disposers).WaitAsync(Timeout);
            }

            pool.Dispose();
            Assert.That(pool.GetStats(), Does.Contain("totalBufferCount=0"));
        }
    }
}