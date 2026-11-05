// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;
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
        private int disposed;

        // Managed thread id of the thread running a work item, or 0
        private int workingThreadId;

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

        /// <summary>
        /// Stops processing work. A work item that is running finishes before this returns, unless this is called
        /// from that work item.
        /// </summary>
        public void Dispose()
        {
            _ = Interlocked.Exchange(ref disposed, 1);
            onWork.Signal();

            // A work item can run for as long as a device write, so the wait yields and sleeps rather than spinning.
            var spinner = new SpinWait();
            while (true)
            {
                var working = Volatile.Read(ref workingThreadId);
                if (working == 0 || working == Environment.CurrentManagedThreadId)
                    return;
                spinner.SpinOnce();
            }
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
            while (Volatile.Read(ref disposed) == 0)
            {
                while (stack.TryPop(out var workItem))
                {
                    // Publishing the thread before checking for dispose means either Dispose sees the item running and
                    // waits for it, or the item sees Dispose and does not run.
                    _ = Interlocked.Exchange(ref workingThreadId, Environment.CurrentManagedThreadId);
                    try
                    {
                        if (Volatile.Read(ref disposed) != 0)
                            return;
                        work(workItem);
                    }
                    catch { }
                    finally
                    {
                        Volatile.Write(ref workingThreadId, 0);
                    }
                }
                await onWork.WaitAsync().ConfigureAwait(false);
            }
        }
    }
}