// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Single-leader / single-worker two-phase handshake used to coordinate one parallel replay task
    /// with the replay leader for exactly one page (unit of work) at a time.
    ///
    /// It wraps two <b>bounded</b> (maxCount == 1) semaphores so each is strictly binary:
    /// <list type="bullet">
    /// <item><c>workReady</c>: leader -&gt; worker ("a page is ready for you to process").</item>
    /// <item><c>workCompleted</c>: worker -&gt; leader ("I finished applying the page you handed me").</item>
    /// </list>
    ///
    /// Because each instance is dedicated to a single worker and each permit is bounded to a single slot,
    /// a permit can never accumulate or be stolen by another worker across cycle boundaries. That is the
    /// exact race the earlier shared, cumulative-count barrier allowed: a fast worker could consume a
    /// permit leaked from the previous cycle, round-trip, and process the next page twice while another
    /// worker was starved — silently double-applying or missing entries while the replication offset
    /// still advanced. Here a stray second <see cref="SignalWorkReady"/> or <see cref="SignalCompleted"/>
    /// throws <see cref="SemaphoreFullException"/>, turning a coordination bug into an immediate, loud
    /// failure instead of a silent one.
    ///
    /// Cycle (repeated per page):
    /// <code>
    ///   leader:  SignalWorkReady()  ->  WaitCompleted()
    ///   worker:  WaitReadyWorkAsync()  ->  (apply page)  ->  SignalCompleted()
    /// </code>
    /// Both semaphores return to zero at the end of every cycle, so there is nothing to reset and nothing
    /// to leak.
    /// </summary>
    public sealed class WorkReadyComplete
    {
        readonly SemaphoreSlim workReady = new(0, 1);
        readonly SemaphoreSlim workCompleted = new(0, 1);

        static TimeSpan ProcessTimeSpan(TimeSpan timeout)
            => timeout == default ? Timeout.InfiniteTimeSpan : timeout;

        /// <summary>
        /// Leader: hand the worker the current page to process. Wakes the worker blocked in
        /// <see cref="WaitReadyWorkAsync"/>. Must be paired one-to-one with <see cref="WaitCompleted"/>;
        /// a second call before the worker consumes the first throws <see cref="SemaphoreFullException"/>.
        /// </summary>
        public void SignalWorkReady() => workReady.Release();

        /// <summary>
        /// Worker: wait for the leader to hand over the next page.
        /// </summary>
        public Task WaitReadyWorkAsync(CancellationToken cancellationToken = default)
            => workReady.WaitAsync(cancellationToken);

        /// <summary>
        /// Worker: signal that the handed-over page has been fully applied. Must be called at most once
        /// per <see cref="SignalWorkReady"/>; a second call before the leader consumes the first throws
        /// <see cref="SemaphoreFullException"/> by design.
        /// </summary>
        public void SignalCompleted() => workCompleted.Release();

        /// <summary>
        /// Leader: wait for this worker to finish applying the page it was handed.
        /// </summary>
        /// <returns><c>true</c> if the worker signalled completion; <c>false</c> if the wait timed out.</returns>
        public bool WaitCompleted(TimeSpan timeout = default, CancellationToken cancellationToken = default)
            => AsyncUtils.BlockingWait(workCompleted.WaitAsync(ProcessTimeSpan(timeout), cancellationToken));
    }
}