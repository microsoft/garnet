// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 0162

using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    internal sealed class CountdownWrapper
    {
        // Separate event for sync code and tcs for async code: Do not block on async code.
        private readonly CountdownEvent syncEvent;
        private readonly TaskCompletionSource<int> asyncTcs;
        int remaining;

        internal CountdownWrapper(int count, bool isAsync)
        {
            if (isAsync)
            {
                asyncTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
                remaining = count;
                return;
            }
            syncEvent = new CountdownEvent(count);
        }

        internal bool IsCompleted => syncEvent is null ? remaining == 0 : syncEvent.IsSet;

        internal void Wait() => syncEvent.Wait();

        internal async ValueTask WaitAsync(CancellationToken cancellationToken)
        {
            // Task.WaitAsync abandons the wait without completing asyncTcs, so a cancelled wait leaves the countdown
            // usable: the IO that is still outstanding continues to decrement it, and DrainAsync can wait for it.
            // Completing asyncTcs here instead would strand those counts and make the real completion unobservable.
            await asyncTcs.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Wait for all counts without observing cancellation, for failure paths that must not release
        /// resources the outstanding IO is still using. A no-op for the sync variant and once already completed.</summary>
        internal ValueTask DrainAsync()
            => asyncTcs is null || IsCompleted ? default : new ValueTask(asyncTcs.Task);

        internal void Decrement()
        {
            if (asyncTcs is not null)
            {
                Debug.Assert(remaining > 0);
                if (Interlocked.Decrement(ref remaining) == 0)
                    asyncTcs.TrySetResult(0);
                return;
            }
            syncEvent.Signal();
        }
    }
}