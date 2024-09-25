// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Shared work queue with a single work processor task loop. Uses LIFO ordering of work.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class WorkQueueLIFO<T> : IDisposable
    {
        readonly ConcurrentStack<T> queue;
        readonly Action<T> work;
        readonly SingleWaiterAutoResetEvent onWork;
        readonly Task processQueue;
        private bool disposed;

        public WorkQueueLIFO(Action<T> work)
        {
            queue = new ConcurrentStack<T>();
            this.work = work;
            disposed = false;
            onWork = new()
            {
                RunContinuationsAsynchronously = true
            };
            processQueue = Task.Run(ProcessQueue);
        }

        public void Dispose()
        {
            disposed = true;
            onWork.Signal();
        }

        /// <summary>
        /// Enqueue work item
        /// </summary>
        /// <param name="workItem">Work to enqueue</param>
        /// <returns>Whether the enqueue is successful</returns>>
        public bool Enqueue(T workItem)
        {
            queue.Push(workItem);
            onWork.Signal();
            return true;
        }

        private async Task ProcessQueue()
        {
            // Process items in work queue
            while (!disposed)
            {
                while (queue.TryPop(out var workItem))
                {
                    try
                    {
                        work(workItem);
                    }
                    catch { }
                    if (disposed) return;
                }
                await onWork.WaitAsync().ConfigureAwait(false);
            }
        }
    }
}