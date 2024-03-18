// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Offers reactivity about when a counter reaches zero
    /// </summary>
    internal sealed class AsyncCountDown
    {
        int counter;
        TaskCompletionSource<int> tcs;
        TaskCompletionSource<int> nextTcs;

        public AsyncCountDown()
        {
            nextTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        /// <summary>
        /// Increments the counter by 1
        /// </summary>
        public void Add()
        {
            Interlocked.Increment(ref counter);
        }

        /// <summary>
        /// Decrements the counter by 1
        /// </summary>
        public void Remove()
        {
            if (Interlocked.Decrement(ref counter) == 0)
                TryCompleteAwaitingTask();
        }

        /// <summary>
        /// Check if countdown is empty
        /// </summary>
        public bool IsEmpty => counter == 0;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TryCompleteAwaitingTask()
        {
            // Complete the Task
            Volatile.Read(ref tcs)?.TrySetResult(0);

            // Reset TCS, so next awaiters produce a new one
            Interlocked.Exchange(ref tcs, null);
        }

        /// <summary>
        /// Provides a way to execute a continuation when the counter reaches zero
        /// </summary>
        /// <returns>A Task that completes when the counter reaches zero</returns>
        public async ValueTask WaitUntilEmptyAsync(CancellationToken cancellationToken)
        {
            if (counter == 0 || !GetOrCreateTaskCompletionSource(out var tcsOut))
                return;

            using var reg = cancellationToken.Register(() => tcsOut.TrySetCanceled());
            await tcsOut.Task.WithCancellationAsync(cancellationToken).ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool GetOrCreateTaskCompletionSource(out TaskCompletionSource<int> tcsOut)
        {
            // If tcs is not null, we'll get it in taskSource
            var taskSource = Interlocked.CompareExchange(ref tcs, nextTcs, null);

            if (taskSource == null)
            {
                // Tcs was null and nextTcs got assigned to it. 
                taskSource = nextTcs;

                // We need a new nextTcs
                nextTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            tcsOut = taskSource;
            return counter > 0;
        }
    }
}