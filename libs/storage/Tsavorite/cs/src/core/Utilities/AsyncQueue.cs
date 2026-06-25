// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Async queue
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <remarks>
    /// Polling-friendly: callers that drain via <see cref="TryDequeue(out T)"/> (the
    /// steady-state pending-IO completion path in <c>InternalCompletePendingRequests</c>)
    /// never touch the semaphore. Only <see cref="WaitForEntry"/> /
    /// <see cref="WaitForEntryAsync"/> consume it, and <see cref="Enqueue(T)"/> only
    /// releases the semaphore when at least one such waiter is in flight. This avoids
    /// a per-Enqueue <see cref="SemaphoreSlim.Release()"/> (and its internal
    /// <see cref="Monitor"/> acquire) that otherwise dominates the completion-thread
    /// hot path on disk-bound workloads.
    /// </remarks>
    public sealed class AsyncQueue<T>
    {
        private readonly SemaphoreSlim semaphore;
        private readonly ConcurrentQueue<T> queue;

        /// <summary>
        /// Number of callers currently blocked in <see cref="WaitForEntry"/> /
        /// <see cref="WaitForEntryAsync"/>. Producers check this with <c>Volatile.Read</c>
        /// and skip <see cref="SemaphoreSlim.Release()"/> when zero. Race with arriving
        /// waiters is handled by waiters re-checking <see cref="ConcurrentQueue{T}.Count"/>
        /// AFTER incrementing this field.
        /// </summary>
        private int waiterCount;

        /// <summary>
        /// Queue count
        /// </summary>
        public int Count => queue.Count;

        /// <summary>
        /// Constructor
        /// </summary>
        public AsyncQueue()
        {
            semaphore = new SemaphoreSlim(0);
            queue = new ConcurrentQueue<T>();
        }

        /// <summary>
        /// Enqueue item
        /// </summary>
        /// <param name="item"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(T item)
        {
            queue.Enqueue(item);
            // Full StoreLoad fence between the enqueue and the waiterCount load.
            // ConcurrentQueue.Enqueue publishes the slot sequence number via a
            // release-store AFTER its full-fence Tail CAS. Without an explicit
            // barrier here, an acquire-only Volatile.Read of waiterCount can be
            // reordered ahead of that release-store on weak memory models (ARM):
            // a producer would then skip Release while a concurrent consumer in
            // DequeueAsync, having already incremented waiterCount, fails its
            // TryDequeue recheck (which reads the slot sequence number) and
            // sleeps on the semaphore forever. The full fence guarantees either
            // (a) the producer sees the consumer's waiterCount increment and
            // wakes it, or (b) the consumer's TryDequeue sees the published
            // slot. WaitForEntry/WaitForEntryAsync rechecks queue.Count, which
            // is bumped by the same full-fence Tail CAS and so is safe with or
            // without this barrier; we add it once for all consumers.
            Interlocked.MemoryBarrier();
            // Skip the semaphore release when nobody is waiting (the common polling-path
            // case). The check is racy by design: a waiter that increments waiterCount
            // after this read still re-checks the queue Count on its own side and returns
            // without blocking when an entry is present.
            if (Volatile.Read(ref waiterCount) > 0)
                semaphore.Release();
        }

        /// <summary>
        /// Async dequeue
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<T> DequeueAsync(CancellationToken cancellationToken = default)
        {
            for (; ; )
            {
                if (queue.TryDequeue(out T fastItem))
                    return fastItem;

                _ = Interlocked.Increment(ref waiterCount);
                try
                {
                    // Re-check after increment closes the race against a producer that
                    // observed waiterCount==0 just before we bumped it.
                    if (queue.TryDequeue(out T raceItem))
                        return raceItem;
                    await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _ = Interlocked.Decrement(ref waiterCount);
                }
            }
        }

        /// <summary>
        /// Wait for queue to have at least one entry
        /// </summary>
        /// <returns></returns>
        public void WaitForEntry()
        {
            // Fast path: queue already has an entry, skip the semaphore entirely.
            if (queue.Count > 0)
                return;

            _ = Interlocked.Increment(ref waiterCount);
            try
            {
                // Re-check after increment: a producer that saw waiterCount==0 between
                // our initial Count check and the Increment will have skipped Release.
                // If it already enqueued, we see it here and return immediately.
                if (queue.Count > 0)
                    return;
                semaphore.Wait();
            }
            finally
            {
                _ = Interlocked.Decrement(ref waiterCount);
            }
        }

        /// <summary>
        /// Wait for queue to have at least one entry
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public ValueTask WaitForEntryAsync(CancellationToken token = default)
        {
            if (queue.Count > 0)
                return ValueTask.CompletedTask;

            return WaitForEntryAsyncSlow(token);
        }

        private async ValueTask WaitForEntryAsyncSlow(CancellationToken token)
        {
            _ = Interlocked.Increment(ref waiterCount);
            try
            {
                if (queue.Count > 0)
                    return;
                await semaphore.WaitAsync(token).ConfigureAwait(false);
            }
            finally
            {
                _ = Interlocked.Decrement(ref waiterCount);
            }
        }

        /// <summary>
        /// Try dequeue (if item exists)
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryDequeue(out T item)
        {
            return queue.TryDequeue(out item);
        }
    }
}