// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    /// <summary>
    /// Dedicated threads for commands that may hold their thread on synchronous IO, such as a read-modify-write of a
    /// record on disk or a scan of the log, so that such a command never runs on a network or thread-pool thread.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A session hands itself over here by parking before the command runs; see
    /// <see cref="RespServerSession.MustOffload"/>. The item posted is the session's resume, which runs the command
    /// and the rest of its batch on an executor thread and then returns the session to its receive loop on the
    /// thread pool.
    /// </para>
    /// <para>
    /// Threads are started on demand, up to a limit, and retire once idle for a while. Past the limit, items wait
    /// in FIFO order for a thread to finish. That wait is safe only if no executor thread is waiting on an item
    /// still queued, and the one such dependency is a transaction: its commands hold key locks that other commands
    /// may be waiting on, on executor threads. A command of a running transaction is therefore posted with
    /// <c>mustRun</c>, which queues it ahead of the others and gets it a thread whatever the limit.
    /// </para>
    /// <para>
    /// After <see cref="Dispose"/>, items are run on the thread pool instead. A resume that is never run leaves its
    /// session parked, with its receive loop waiting on it, so none may be dropped.
    /// </para>
    /// </remarks>
    internal sealed class OffloadExecutor : IDisposable
    {
        [ThreadStatic]
        static bool onExecutorThread;

        /// <summary>Whether the calling thread is an executor thread.</summary>
        internal static bool OnExecutorThread => onExecutorThread;

        readonly ConcurrentQueue<IThreadPoolWorkItem> queue = new();
        readonly ConcurrentQueue<IThreadPoolWorkItem> mustRunQueue = new();
        readonly SemaphoreSlim wake = new(0);
        readonly int maxThreads;
        readonly TimeSpan idleTimeout;
        readonly ILogger logger;

        /// <summary>Threads started and not yet retired.</summary>
        int threadCount;

        /// <summary>
        /// Threads waiting for work that no post has claimed yet. A post claims one by decrementing this and
        /// releasing <see cref="wake"/>; a waiting thread claims itself the same way when it finds work, or retires.
        /// </summary>
        int idleCount;

        int disposed;

        long posts;

        /// <summary>
        /// Creates an executor.
        /// </summary>
        /// <param name="maxThreads">Most threads to run items on, except items posted with <c>mustRun</c>.</param>
        /// <param name="idleTimeout">How long a thread waits for work before it retires.</param>
        /// <param name="logger">Logger.</param>
        internal OffloadExecutor(int maxThreads, TimeSpan idleTimeout, ILogger logger = null)
        {
            this.maxThreads = Math.Max(1, maxThreads);
            this.idleTimeout = idleTimeout;
            this.logger = logger;
        }

        /// <summary>
        /// Default for the most threads, used when the configured limit is 0.
        /// </summary>
        internal static int DefaultMaxThreads => Math.Max(64, 2 * Environment.ProcessorCount);

        /// <summary>Threads started and not yet retired.</summary>
        internal int ThreadCount => Volatile.Read(ref threadCount);

        /// <summary>Items posted so far.</summary>
        internal long Posts => Volatile.Read(ref posts);

        /// <summary>
        /// Whether a command on the calling thread runs where it is rather than being posted here: the thread is an
        /// executor thread, or the executor is disposed and would only hand the command back to the thread pool.
        /// </summary>
        internal bool RunsInPlace => onExecutorThread || Volatile.Read(ref disposed) != 0;

        /// <summary>
        /// Runs <paramref name="item"/> on an executor thread.
        /// </summary>
        /// <param name="item">Item to run.</param>
        /// <param name="mustRun">
        /// Whether the item must not wait behind items already queued: it is run ahead of them, and a thread is
        /// started for it if none is free, even past the limit.
        /// </param>
        internal void Post(IThreadPoolWorkItem item, bool mustRun)
        {
            _ = Interlocked.Increment(ref posts);
            (mustRun ? mustRunQueue : queue).Enqueue(item);

            // Paired with the store to disposed and the drain that follows it in Dispose: one of the two sees the
            // other, so an item enqueued as the executor is disposed is not left behind.
            if (Volatile.Read(ref disposed) != 0)
            {
                DrainToThreadPool();
                return;
            }

            if (TryClaimIdleThread())
            {
                _ = wake.Release();
                return;
            }

            if (mustRun)
            {
                _ = Interlocked.Increment(ref threadCount);
                StartThread();
            }
            else if (TryReserveThread())
            {
                StartThread();
            }

            // Otherwise every thread is busy and the limit is reached: a thread takes the item when it finishes, and
            // one that is about to retire re-checks for it after giving up its place.
        }

        bool TryClaimIdleThread()
        {
            while (true)
            {
                var idle = Volatile.Read(ref idleCount);
                if (idle == 0)
                    return false;
                if (Interlocked.CompareExchange(ref idleCount, idle - 1, idle) == idle)
                    return true;
            }
        }

        bool TryReserveThread()
        {
            while (true)
            {
                var count = Volatile.Read(ref threadCount);
                if (count >= maxThreads)
                    return false;
                if (Interlocked.CompareExchange(ref threadCount, count + 1, count) == count)
                    return true;
            }
        }

        void StartThread()
        {
            try
            {
                var thread = new Thread(WorkerLoop) { IsBackground = true, Name = "Garnet offload" };
                thread.UnsafeStart();
            }
            catch (Exception ex)
            {
                // Without the thread the item still runs, on a thread that is busy now or the next one started
                _ = Interlocked.Decrement(ref threadCount);
                logger?.LogError(ex, "Could not start an offload thread");
            }
        }

        bool HasWork => !mustRunQueue.IsEmpty || !queue.IsEmpty;

        bool TryDequeue(out IThreadPoolWorkItem item)
            => mustRunQueue.TryDequeue(out item) || queue.TryDequeue(out item);

        void WorkerLoop()
        {
            onExecutorThread = true;

            while (true)
            {
                while (TryDequeue(out var item))
                    Execute(item);

                if (Volatile.Read(ref disposed) != 0)
                    break;

                _ = Interlocked.Increment(ref idleCount);

                // A post that found no idle thread and could not start one left its item for a busy thread, which
                // this one now is not. Paired with that post's read of idleCount: one of the two sees the other. The
                // same pairing with Dispose's exchange of idleCount ensures this thread is either woken or exits.
                if ((HasWork || Volatile.Read(ref disposed) != 0) && TryClaimIdleThread())
                    continue;

                if (wake.Wait(idleTimeout))
                    continue;

                // Timed out. A post may have claimed this thread since, in which case its wake is on the way.
                if (!TryClaimIdleThread())
                {
                    wake.Wait();
                    continue;
                }

                if (Volatile.Read(ref disposed) != 0)
                    break;

                _ = Interlocked.Decrement(ref threadCount);

                // A post that found the limit reached before this thread gave up its place left its item for a
                // busy thread. Paired with that post's read of threadCount: one of the two sees the other.
                if (HasWork && TryReserveThread())
                    continue;

                return;
            }

            _ = Interlocked.Decrement(ref threadCount);
        }

        void Execute(IThreadPoolWorkItem item)
        {
            try
            {
                item.Execute();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "An offloaded command escaped its session's handling");
            }
        }

        void DrainToThreadPool()
        {
            while (TryDequeue(out var item))
                ThreadPool.UnsafeQueueUserWorkItem(item, preferLocal: false);
        }

        /// <summary>
        /// Stops the executor. Threads finish the item they are running and exit, and items still queued, or
        /// posted from now on, run on the thread pool.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            DrainToThreadPool();

            var idle = Interlocked.Exchange(ref idleCount, 0);
            if (idle > 0)
                _ = wake.Release(idle);
        }
    }
}