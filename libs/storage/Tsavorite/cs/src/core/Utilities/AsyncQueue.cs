// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace Tsavorite.core
{
    /// <summary>
    /// Async queue
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <remarks>
    /// Polling-friendly: callers that drain via <see cref="TryDequeue(out T)"/> (the
    /// steady-state pending-IO completion path in <c>InternalCompletePendingRequests</c>)
    /// never touch either wait primitive. Only <see cref="WaitForEntry"/>,
    /// <see cref="DequeueAsync"/> and <see cref="WaitForEntryAsync"/> do, and
    /// <see cref="Enqueue(T)"/> only signals when at least one such waiter is in flight.
    /// This avoids a per-Enqueue <see cref="SemaphoreSlim.Release()"/> (and its internal
    /// <see cref="Monitor"/> acquire) that otherwise dominates the completion-thread
    /// hot path on disk-bound workloads.
    /// </remarks>
    public sealed class AsyncQueue<T>
    {
        private readonly SemaphoreSlim semaphore;
        private readonly ConcurrentQueue<T> queue;
        private readonly WaitGate gate;

        /// <summary>
        /// Number of callers currently blocked on the semaphore, i.e. in <see cref="WaitForEntry"/>,
        /// <see cref="DequeueAsync"/>, or the cancellable <see cref="WaitForEntryAsync"/> path.
        /// Producers check this with <c>Volatile.Read</c> and skip <see cref="SemaphoreSlim.Release()"/>
        /// when zero. Race with arriving waiters is handled by waiters re-checking
        /// <see cref="ConcurrentQueue{T}.Count"/> AFTER incrementing this field.
        /// </summary>
        private int waiterCount;

        /// <summary>
        /// Non-zero while the single <see cref="WaitForEntryAsync"/> waiter is armed on
        /// <see cref="gate"/>. Separate from <see cref="waiterCount"/> so a producer signals
        /// only the primitive that actually has a waiter on it.
        /// </summary>
        private int gateWaiterCount;

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
            gate = new WaitGate(this);
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
            // Skip signalling when nobody is waiting (the common polling-path case). Both checks
            // are racy by design: a waiter that arms after this read still re-checks the queue
            // Count on its own side and completes without parking when an entry is present.
            if (Volatile.Read(ref gateWaiterCount) > 0)
                gate.Signal();
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
        /// <remarks>
        /// Allocation-free when <paramref name="token"/> cannot be cancelled, which is the
        /// pending-I/O park taken by a session on every read that misses memory. There is exactly
        /// one such waiter per queue - <c>readyResponses</c> is owned by a single
        /// <c>TsavoriteExecutionContext</c> - so the wait is served by a reusable
        /// <see cref="IValueTaskSource"/> rather than by a fresh <see cref="SemaphoreSlim"/>
        /// waiter node plus an async state machine. A cancellable token falls back to the
        /// semaphore, which is what observes the token.
        /// </remarks>
        public ValueTask WaitForEntryAsync(CancellationToken token = default)
        {
            if (queue.Count > 0)
                return ValueTask.CompletedTask;

            if (token.CanBeCanceled)
                return WaitForEntryAsyncSlow(token);

            var version = gate.Arm();
            _ = Interlocked.Increment(ref gateWaiterCount);
            // Pairs with the fence in Enqueue: either the producer sees our armed gate and
            // signals it, or we see its entry here and signal ourselves. Signal() is a CAS,
            // so exactly one of the two completes the gate.
            Interlocked.MemoryBarrier();
            if (queue.Count > 0)
                gate.Signal();
            return new ValueTask(gate, version);
        }

        /// <remarks>
        /// Pools its state machine: this serves the cancellable callers, which are rare relative to
        /// the park on <see cref="WaitForEntryAsync"/>, and the returned <see cref="ValueTask"/> is
        /// awaited exactly once by its only caller.
        /// </remarks>
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
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
        /// Reusable single-waiter completion source backing <see cref="WaitForEntryAsync"/>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The <see cref="state"/> field is what makes reuse safe. <see cref="core"/> may only be
        /// reset while no producer can touch it, and a producer may only touch it after winning the
        /// <see cref="Waiting"/> -&gt; <see cref="Signalled"/> transition. <see cref="Arm"/> resets
        /// before publishing <see cref="Waiting"/>, so the two never overlap. A producer that is
        /// late - the waiter it observed has already completed - simply loses the CAS and does
        /// nothing, and a producer that signals a round the waiter has already satisfied itself
        /// causes a spurious wakeup, which callers absorb by re-checking the queue.
        /// </para>
        /// <para>
        /// Continuations are scheduled, never inlined. <see cref="Signal"/> runs on a device
        /// completion thread; resuming a session inline there would let that session's next pending
        /// read wait on the very thread that would have to complete it.
        /// </para>
        /// </remarks>
        private sealed class WaitGate : IValueTaskSource
        {
            private const int Idle = 0;
            private const int Waiting = 1;
            private const int Signalled = 2;

            private readonly AsyncQueue<T> owner;
            private ManualResetValueTaskSourceCore<bool> core = new() { RunContinuationsAsynchronously = true };
            private int state;

            internal WaitGate(AsyncQueue<T> owner) => this.owner = owner;

            /// <summary>
            /// Readies the gate for one wait and returns the token identifying it. Called only by the
            /// single owning waiter, and only after the previous wait has run <see cref="GetResult"/>.
            /// </summary>
            internal short Arm()
            {
                core.Reset();
                var version = core.Version;
                Volatile.Write(ref state, Waiting);
                return version;
            }

            /// <summary>
            /// Completes the current wait, if one is armed and has not already been completed.
            /// </summary>
            internal void Signal()
            {
                if (Interlocked.CompareExchange(ref state, Signalled, Waiting) == Waiting)
                    core.SetResult(true);
            }

            public void GetResult(short token)
            {
                try
                {
                    core.GetResult(token);
                }
                finally
                {
                    Volatile.Write(ref state, Idle);
                    _ = Interlocked.Decrement(ref owner.gateWaiterCount);
                }
            }

            public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

            public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
                => core.OnCompleted(continuation, state, token, flags);
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