// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Centralized two-phase cyclic rendezvous barrier (the classic "double turnstile" construction) for
    /// one leader plus N-1 workers that jointly process a shared unit of work (an AOF replay "page") one
    /// at a time. It replaces the previous design of N per-worker two-semaphore handshakes with a single
    /// shared instance backed by just two bounded semaphores. The two turnstiles (ready and completed)
    /// gate every participant at the start and end of each page; the second turnstile resets the count to
    /// zero so the barrier is safely reusable for the next page.
    ///
    /// <para>
    /// <b>Participants.</b> The leader is a participant like everyone else: <c>participantCount</c> equals
    /// the number of replay workers <b>plus one</b> for the leader. Every participant (leader and workers)
    /// calls <see cref="SignalWorkReadyWait"/>/<see cref="SignalWorkReadyWaitAsync"/> to meet at the start
    /// of a page, and <see cref="SignalWorkCompletedWait"/>/<see cref="SignalWorkCompletedWaitAsync"/> to meet at
    /// the end of a page. The leader writes the shared page context <b>before</b> arriving at the ready
    /// barrier and advances the replication/replay offset <b>after</b> returning from the completion
    /// barrier, so the completion rendezvous guarantees no worker is still reading the page when the
    /// leader recycles the buffer.
    /// </para>
    ///
    /// <para>
    /// <b>Why this is steal-proof.</b> The earlier shared, cumulative-count barrier lost worker identity:
    /// a fast worker could consume a permit leaked from a previous cycle, round-trip, and process a page
    /// twice while another worker starved — silently double-applying or missing entries while the offset
    /// still advanced. A rendezvous barrier removes that race by a different mechanism than per-worker
    /// identity: <b>no participant can advance to the next page until the whole cohort has passed both
    /// barriers of the current page</b>, so no participant can lap another. The internal counter is
    /// provably drained to zero before the next phase's arrivals begin (the single last arriver skips its
    /// own wait and releases exactly the <c>participantCount - 1</c> parked participants, leaving no stale
    /// permit to be stolen next cycle).
    /// </para>
    ///
    /// <para>
    /// <b>Cycle</b> (repeated per page): every participant calls
    /// <see cref="SignalWorkReadyWait"/> (or the async form) — the last arriver releases the rest, then all
    /// proceed. Workers apply the page; the leader waits. Every participant then calls
    /// <see cref="SignalWorkCompletedWait"/> — again the last arriver releases the rest. The counter returns to
    /// zero, so there is nothing to reset between cycles.
    /// </para>
    ///
    /// <para>
    /// <b>Cancellation / timeout are terminal.</b> A timeout throws <see cref="Garnet.common.GarnetException"/>
    /// and a cancelled token throws <see cref="OperationCanceledException"/> out of the waiting call. Either
    /// leaves the internal count in a partially-advanced state <b>by design</b>: any failure to complete a
    /// rendezvous is unrecoverable for replay, so the whole driver is torn down and the barrier is never
    /// reused. Every participant must pass the same <see cref="CancellationToken"/> so that a fault on one
    /// participant (which cancels the token) unblocks the entire cohort instead of deadlocking it.
    /// </para>
    /// </summary>
    public sealed class DoubleTurnstileBarrier
    {
        readonly int participantCount;
        int workerCount = 0;

        readonly SemaphoreSlim workReady;
        readonly SemaphoreSlim workComplete;

        static TimeSpan ProcessTimeSpan(TimeSpan timeout)
            => timeout == default ? Timeout.InfiniteTimeSpan : timeout;

        /// <summary>
        /// Creates a barrier for <paramref name="participantCount"/> participants (the number of replay
        /// workers plus one for the leader).
        /// </summary>
        /// <param name="participantCount">Total participants, including the leader. Must be at least 1.</param>
        public DoubleTurnstileBarrier(int participantCount)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(participantCount, 1);
            this.participantCount = participantCount;
            workReady = new(0, participantCount);
            workComplete = new(0, participantCount);
        }

        /// <summary>
        /// Arrive at the ready barrier (synchronous). Blocks until every participant has arrived, at which
        /// point all proceed to process the current page.
        /// </summary>
        /// <param name="timeout">Maximum time to wait for the cohort; <c>default</c> waits indefinitely.</param>
        /// <param name="cancellationToken">Token observed while waiting; cancellation throws.</param>
        public void SignalWorkReadyWait(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            if (SignalWorkReadyInternal() && !workReady.Wait(ProcessTimeSpan(timeout), cancellationToken))
                ExceptionUtils.ThrowException(new GarnetException($"Failed on {nameof(SignalWorkReadyWait)}"));
        }

        /// <summary>
        /// Arrive at the ready barrier (asynchronous). Preferred for workers so an idle worker releases its
        /// thread-pool thread while parked. Blocks until every participant has arrived.
        /// </summary>
        /// <param name="timeout">Maximum time to wait for the cohort; <c>default</c> waits indefinitely.</param>
        /// <param name="cancellationToken">Token observed while waiting; cancellation throws.</param>
        public async Task SignalWorkReadyWaitAsync(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            if (SignalWorkReadyInternal() && !await workReady.WaitAsync(ProcessTimeSpan(timeout), cancellationToken).ConfigureAwait(false))
                ExceptionUtils.ThrowException(new GarnetException($"Failed on {nameof(SignalWorkReadyWaitAsync)}"));
        }

        private bool SignalWorkReadyInternal()
        {
            var newValue = Interlocked.Increment(ref workerCount);
            if (newValue == participantCount)
            {
                // The last arriver skips its own Wait, so only participantCount - 1 participants are
                // parked. Releasing exactly that many drains the semaphore back to zero every cycle;
                // releasing participantCount would leak one stale permit that a fast participant could
                // consume next cycle without the cohort assembling (a rendezvous bypass / steal).
                if (participantCount > 1)
                    _ = workReady.Release(participantCount - 1);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Arrive at the completion barrier (synchronous). Blocks until every participant has finished the
        /// current page.
        /// </summary>
        /// <param name="timeout">Maximum time to wait for the cohort; <c>default</c> waits indefinitely.</param>
        /// <param name="cancellationToken">Token observed while waiting; cancellation throws.</param>
        public void SignalWorkCompletedWait(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            if (SignalWorkCompletedInternal() && !workComplete.Wait(ProcessTimeSpan(timeout), cancellationToken))
                ExceptionUtils.ThrowException(new GarnetException($"Failed on {nameof(SignalWorkCompletedWait)}"));
        }

        /// <summary>
        /// Arrive at the completion barrier (asynchronous). Preferred for workers. Blocks until every
        /// participant has finished the current page.
        /// </summary>
        /// <param name="timeout">Maximum time to wait for the cohort; <c>default</c> waits indefinitely.</param>
        /// <param name="cancellationToken">Token observed while waiting; cancellation throws.</param>
        public async Task SignalWorkCompletedWaitAsync(TimeSpan timeout = default, CancellationToken cancellationToken = default)
        {
            if (SignalWorkCompletedInternal() && !await workComplete.WaitAsync(ProcessTimeSpan(timeout), cancellationToken).ConfigureAwait(false))
                ExceptionUtils.ThrowException(new GarnetException($"Failed on {nameof(SignalWorkCompletedWaitAsync)}"));
        }

        private bool SignalWorkCompletedInternal()
        {
            var newValue = Interlocked.Decrement(ref workerCount);
            if (newValue == 0)
            {
                // Symmetric to the ready phase: only participantCount - 1 participants are parked on
                // workComplete (the last completer skips its Wait), so release exactly that many to
                // avoid leaking a stale permit across cycles.
                if (participantCount > 1)
                    _ = workComplete.Release(participantCount - 1);
                return false;
            }

            return true;
        }
    }
}