// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using NUnit.Framework;

namespace Garnet.test
{
    /// <summary>
    /// Limits the worker pool to a few threads for the life of the scope, so that a thread a request holds is a thread
    /// the others cannot use. Uncapped, a held thread would go unnoticed on a large host: the pool injects a thread for
    /// each one that is held.
    /// </summary>
    internal sealed class ThreadPoolCap : IDisposable
    {
        readonly int minWorkers, minIo, maxWorkers, maxIo;
        bool restored;

        ThreadPoolCap(int minWorkers, int minIo, int maxWorkers, int maxIo)
            => (this.minWorkers, this.minIo, this.maxWorkers, this.maxIo) = (minWorkers, minIo, maxWorkers, maxIo);

        /// <summary>
        /// Caps the worker pool at <paramref name="workers"/> threads, or ignores the test if the pool cannot be capped.
        /// </summary>
        /// <param name="workers">Most worker threads the pool may run</param>
        internal static ThreadPoolCap Apply(int workers)
        {
            ThreadPool.GetMinThreads(out var minWorkers, out var minIo);
            ThreadPool.GetMaxThreads(out var maxWorkers, out var maxIo);
            var cap = new ThreadPoolCap(minWorkers, minIo, maxWorkers, maxIo);

            if (!ThreadPool.SetMinThreads(Math.Min(minWorkers, workers), minIo) || !ThreadPool.SetMaxThreads(workers, maxIo))
            {
                cap.Dispose();
                Assert.Ignore("Could not cap the thread pool, so a held thread cannot be made to matter.");
            }

            return cap;
        }

        /// <summary>
        /// Caps the worker pool so that <paramref name="freeWorkers"/> threads are left beyond those the test host keeps
        /// busy for the whole run, or ignores the test if that cannot be arranged. The host's own count is not fixed, so
        /// it is found by raising the cap until a queued item runs.
        /// </summary>
        /// <param name="freeWorkers">Worker threads to leave for the test</param>
        internal static ThreadPoolCap ApplyFree(int freeWorkers)
        {
            const int MaxHeld = 16;

            var cap = Apply(1);
            for (var held = 0; held <= MaxHeld; held++)
            {
                if (held > 0 && !cap.TrySet(held + 1))
                    break;

                // Not disposed: a probe that timed out still runs once the cap is raised
                var ran = new ManualResetEventSlim();
                ThreadPool.UnsafeQueueUserWorkItem(static done => done.Set(), ran, preferLocal: false);
                if (ran.Wait(TimeSpan.FromSeconds(1)))
                {
                    if (cap.TrySet(held + freeWorkers))
                        return cap;
                    break;
                }
            }

            cap.Dispose();
            Assert.Ignore($"Could not leave the thread pool {freeWorkers} free worker(s), so a held thread cannot be made to matter.");
            return null;
        }

        bool TrySet(int workers)
            => ThreadPool.SetMaxThreads(workers, maxIo) && ThreadPool.SetMinThreads(workers, minIo);

        /// <summary>
        /// Restores the pool's limits. Idempotent.
        /// </summary>
        public void Dispose()
        {
            if (restored)
                return;
            restored = true;

            _ = ThreadPool.SetMaxThreads(maxWorkers, maxIo);
            _ = ThreadPool.SetMinThreads(minWorkers, minIo);
        }
    }
}