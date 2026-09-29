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

        public override string ToString() => semaphore?.ToString();

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

        internal void Wait(CancellationToken token = default)
        {
            // A disposed event (null after Dispose, or disposed between the read and the wait) reports as
            // signaled: teardown must never block a waiter, and every caller re-checks its state after waking.
            var s = this.semaphore;
            if (s is null)
                return;
            try
            {
                s.Wait(token);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        internal Task WaitAsync(CancellationToken token = default)
        {
            var s = this.semaphore;
            if (s is null)
                return Task.CompletedTask;
            try
            {
                return s.WaitAsync(token);
            }
            catch (ObjectDisposedException)
            {
                return Task.CompletedTask;
            }
        }

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