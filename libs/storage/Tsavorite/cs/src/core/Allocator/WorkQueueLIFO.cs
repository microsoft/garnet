// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
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

        // Set by Dispose. Once set, no work item starts.
        const long DisposedBit = 1L << 32;

        // DisposedBit, plus in the low 32 bits the managed thread id of the thread running a work item, or 0
        long state;

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
            var working = (int)Interlocked.Or(ref state, DisposedBit);
            onWork.Signal();
            if (working == 0 || working == Environment.CurrentManagedThreadId)
                return;

            // No work item starts once DisposedBit is set, so the thread id can only clear. A work item can run for as long
            // as a device write, so the wait yields and sleeps rather than spinning.
            var spinner = new SpinWait();
            while ((int)Volatile.Read(ref state) != 0)
                spinner.SpinOnce();
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
            while ((Volatile.Read(ref state) & DisposedBit) == 0)
            {
                while (stack.TryPop(out var workItem))
                {
                    // Publish this thread as running a work item unless Dispose came first, so either Dispose sees the item
                    // running and waits for it, or the item sees Dispose and does not run.
                    var prior = Interlocked.CompareExchange(ref state, Environment.CurrentManagedThreadId, 0);
                    if (prior != 0)
                    {
                        Debug.Assert(prior == DisposedBit, "Only the queue's single processing loop runs work items");
                        return;
                    }

                    try
                    {
                        work(workItem);
                    }
                    catch { }
                    finally
                    {
                        // Clear the thread id, keeping DisposedBit if Dispose set it meanwhile
                        _ = Interlocked.And(ref state, DisposedBit);
                    }
                }
                await onWork.WaitAsync().ConfigureAwait(false);
            }
        }
    }
}