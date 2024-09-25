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
        readonly ConcurrentStack<T> stack;
        readonly Action<T> work;
        readonly SingleWaiterAutoResetEvent onWork;
        readonly Task processQueue;
        private bool disposed;

        public WorkQueueLIFO(Action<T> work)
        {
            stack = new ConcurrentStack<T>();
            this.work = work;
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
        /// Add work item
        /// </summary>
        /// <param name="workItem">Work item</param>
        /// <returns>Whether the add is successful</returns>>
        public void AddWorkItem(T workItem)
        {
            // Add the work item
            stack.Push(workItem);
            // Signal the processing logic to check for work
            onWork.Signal();
        }

        private async Task ProcessQueue()
        {
            // Process items in work queue
            while (!disposed)
            {
                while (stack.TryPop(out var workItem))
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