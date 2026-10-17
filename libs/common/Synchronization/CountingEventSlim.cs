// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// A <see cref="ManualResetEventSlim"/> that is triggered based on a count hitting or advancing above 0 rather than
    /// explicit Set and Reset calls.
    /// </summary>
    /// <remarks>
    /// Akin to a <see cref="CountdownEvent"/>, but allows for count to go back up after hitting 0.
    /// </remarks>
    public struct CountingEventSlim : IDisposable
    {
        private const int HoldForSetValue = int.MinValue / 2;

        private readonly ManualResetEventSlim resetEvent;
        private int count = 0;

        // Completed by the Decrement that brings the count to 0, for the callers of WaitAsync that found it above 0
        private TaskCompletionSource zeroed;

        private CountingEventSlim(ManualResetEventSlim resetEvent)
        {
            this.resetEvent = resetEvent;
        }

        /// <summary>
        /// Increment the internal count.
        /// 
        /// Any caller to <see cref="Wait"/> after this call returns but before a paired call to <see cref="Decrement"/> will block.
        /// </summary>
        public void Increment()
        {
            while (true)
            {
                var addRes = Interlocked.Increment(ref count);
                if (addRes <= 0)
                {
                    // Some thread is about to Set, undo and wait

                    _ = Interlocked.Decrement(ref count);
                    _ = Thread.Yield();
                }
                else
                {
                    resetEvent.Reset();
                    break;
                }
            }
        }

        /// <summary>
        /// Decrement the internal count.
        /// 
        /// If this is the last outstanding <see cref="Decrement"/> paired to a completed <see cref="Increment"/>, threads blocked in <see cref="Wait"/> will be unblocked.
        /// </summary>
        public void Decrement()
        {
            var decrRes = Interlocked.Decrement(ref count);
            Debug.Assert(decrRes >= 0, "Decrement fell below 0, implies unbalanced calls to Increment and Decrement");

            if (decrRes == 0 && Interlocked.CompareExchange(ref count, HoldForSetValue, 0) == 0)
            {
                resetEvent.Set();

                // Taken while the hold keeps Increment from resetting the event, so that the waiters woken are
                // only those that found the count above 0 before it reached 0 here
                var waiters = Interlocked.Exchange(ref zeroed, null);

                var unlockRes = Interlocked.Add(ref count, -HoldForSetValue);
                Debug.Assert(unlockRes >= 0, "Unlock resulted in incoherent count");

                waiters?.TrySetResult();
            }
        }

        /// <summary>
        /// Block until the internal count hits 0.
        /// 
        /// Returns true if wait was successful, false otherwise.
        /// </summary>
        /// <param name="millisecondsTimeout">Timeout for wait operation.  -1 (the default) waits indefinitely, 0 returns immediately.</param>
        public readonly bool Wait(int millisecondsTimeout = -1)
        => resetEvent.Wait(millisecondsTimeout);

        /// <summary>
        /// Completes when the internal count is 0, as <see cref="Wait"/> returns, without blocking a thread while it is
        /// above 0.
        /// </summary>
        /// <param name="cancellationToken">Token observed while waiting; cancelling it faults the returned task.</param>
        /// <returns>A completed task, without allocating, when the count is 0 on entry.</returns>
        /// <remarks>
        /// The waiter is published before the count is checked again, and a <see cref="Decrement"/> that brings the
        /// count to 0 sets the event before it takes the waiter. Both are full fences, so a waiter either sees the count
        /// at 0 or is woken by the next <see cref="Decrement"/> that brings it to 0, and is never woken by one that did
        /// so before the call. A waiter resumes on the thread pool, never inline on the thread that decremented.
        /// </remarks>
        public ValueTask WaitAsync(CancellationToken cancellationToken = default)
        {
            if (resetEvent.IsSet)
                return default;

            var waiter = Volatile.Read(ref zeroed);
            if (waiter == null)
            {
                var armed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                waiter = Interlocked.CompareExchange(ref zeroed, armed, null) ?? armed;
            }

            if (resetEvent.IsSet)
                return default;

            return new ValueTask(cancellationToken.CanBeCanceled ? waiter.Task.WaitAsync(cancellationToken) : waiter.Task);
        }

        /// <summary>
        /// Create a new <see cref="CountingEventSlim"/>.
        /// </summary>
        public static CountingEventSlim Create()
        => new(new(true));

        /// <inheritdoc/>
        public readonly void Dispose()
        => resetEvent.Dispose();
    }
}