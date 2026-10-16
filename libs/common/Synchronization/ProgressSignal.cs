// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Parks a single waiter until a condition that other threads change stops holding, without blocking a thread and
    /// without polling.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The threads that change the condition call <see cref="Pulse"/> after each change that can clear it, and the waiter
    /// calls <see cref="WaitWhileAsync"/>. The waiter publishes its wake-up before it evaluates the condition, and
    /// <see cref="Pulse"/> publishes the change before it takes the wake-up. Both publications are full fences, so a
    /// change that lands at any moment is either seen by the waiter's evaluation or wakes the waiter.
    /// </para>
    /// <para>
    /// There is at most one waiter at a time. A parked waiter resumes on the thread pool, never inline on the thread that
    /// pulsed or cancelled.
    /// </para>
    /// </remarks>
    public sealed class ProgressSignal
    {
        TaskCompletionSource armed;

        /// <summary>
        /// Wakes the waiter if one is parked. Call after every change that can clear the condition it waits on.
        /// </summary>
        /// <remarks>
        /// This is a single unconditional exchange. Reading <c>armed</c> first and exchanging only when it is set would
        /// leave no fence between the caller's change and that read, so the read could pass a waiter that arms just
        /// after it while the waiter's evaluation still misses the change.
        /// </remarks>
        public void Pulse() => Interlocked.Exchange(ref armed, null)?.TrySetResult();

        /// <summary>
        /// Completes once <paramref name="blocked"/> returns false, parking without a thread while it returns true.
        /// </summary>
        /// <param name="blocked">The condition to wait out. It is evaluated on the calling thread and on pool threads.</param>
        /// <param name="cancellationToken">Token observed while parked; cancelling it faults the returned task.</param>
        /// <returns>A completed task, without allocating, when the condition does not hold on entry.</returns>
        public ValueTask WaitWhileAsync(Func<bool> blocked, CancellationToken cancellationToken)
            => blocked() ? new ValueTask(ParkAsync(blocked, cancellationToken)) : default;

        async Task ParkAsync(Func<bool> blocked, CancellationToken cancellationToken)
        {
            do
            {
                var wake = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _ = Interlocked.Exchange(ref armed, wake);
                if (!blocked())
                    return;

                using var cancellation = cancellationToken.UnsafeRegister(static (state, token) => ((TaskCompletionSource)state).TrySetCanceled(token), wake);
                await wake.Task.ConfigureAwait(false);
            }
            while (blocked());
        }
    }
}
