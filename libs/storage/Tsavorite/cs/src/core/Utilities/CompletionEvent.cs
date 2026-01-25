// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    // This structure uses a SemaphoreSlim as if it were a ManualResetEventSlim, because MRES does not support async waiting.
    internal struct CompletionEvent : IDisposable
    {
        private SemaphoreSlim semaphore;

        internal void Initialize() => semaphore = new SemaphoreSlim(0);

        internal void Set()
        {
            // If we have an existing semaphore, replace with a new one (to which any subequent waits will apply) and signal any waits on the existing one.
            var newSemaphore = new SemaphoreSlim(0);
            while (true)
            {
                var tempSemaphore = semaphore;
                if (tempSemaphore == null)
                {
                    newSemaphore.Dispose();
                    break;
                }
                if (Interlocked.CompareExchange(ref semaphore, newSemaphore, tempSemaphore) == tempSemaphore)
                {
                    // Release all waiting threads
                    tempSemaphore.Release(int.MaxValue);
                    tempSemaphore.Dispose();
                    break;
                }
            }
        }

        internal bool IsDefault() => semaphore is null;

        internal void Wait(CancellationToken token = default) => semaphore.Wait(token);

        internal Task WaitAsync(CancellationToken token = default) => semaphore.WaitAsync(token);

        internal Task WaitAsync(TimeSpan timeSpan, CancellationToken cancellationToken = default) => semaphore.WaitAsync(timeSpan, cancellationToken);

        /// <inheritdoc/>
        public void Dispose()
        {
            semaphore?.Dispose();
            semaphore = null;
        }
    }
}