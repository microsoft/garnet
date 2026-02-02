// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.client
{
    // This structure uses a SemaphoreSlim as if it were an AutoResetEvent, because ARE does not support async waiting.
    internal struct CompletionEvent : IDisposable
    {
        private SemaphoreSlim semaphore;

        internal void Initialize() => this.semaphore = new SemaphoreSlim(0);

        internal void Set()
        {
            var newSemaphore = new SemaphoreSlim(0);
            while (true)
            {
                var tempSemaphore = this.semaphore;
                if (tempSemaphore == null)
                {
                    newSemaphore.Dispose();
                    break;
                }

                if (Interlocked.CompareExchange(ref this.semaphore, newSemaphore, tempSemaphore) == tempSemaphore)
                {
                    // Release all waiting threads
                    tempSemaphore.Release(int.MaxValue);
                    tempSemaphore.Dispose();
                    break;
                }
            }
        }

        internal bool IsDefault() => this.semaphore is null;

        internal void Wait(CancellationToken token = default) => this.semaphore.Wait(token);

        internal Task WaitAsync(CancellationToken token = default) => this.semaphore.WaitAsync(token);

        /// <inheritdoc/>
        public void Dispose()
        {
            while (true)
            {
                var tempSemaphore = semaphore;
                if (tempSemaphore == null)
                    break;

                if (Interlocked.CompareExchange(ref semaphore, null, tempSemaphore) == tempSemaphore)
                {
                    // Release all waiting threads
                    tempSemaphore.Release(int.MaxValue);
                    tempSemaphore.Dispose();
                    break;
                }
            }
        }
    }
}