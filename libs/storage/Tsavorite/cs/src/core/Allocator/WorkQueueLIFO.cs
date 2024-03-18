// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Shared work queue that ensures one worker at any given time. Uses LIFO ordering of work.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    class WorkQueueLIFO<T> : IDisposable
    {
        const int kMaxQueueSize = 1 << 30;
        readonly ConcurrentStack<T> _queue;
        readonly Action<T> _work;
        private int _count;
        private bool _disposed;

        public WorkQueueLIFO(Action<T> work)
        {
            _queue = new ConcurrentStack<T>();
            _work = work;
            _count = 0;
            _disposed = false;
        }

        public void Dispose()
        {
            _disposed = true;
            // All future enqueue requests will no longer perform work after _disposed is set to true.
            while (_count != 0)
                Thread.Yield();
            // After this point, any previous work must have completed. Even if another enqueue request manipulates the
            // count field, they are guaranteed to see disposed and not enqueue any actual work.
        }

        /// <summary>
        /// Enqueue work item, take ownership of draining the work queue
        /// if needed
        /// </summary>
        /// <param name="work">Work to enqueue</param>
        /// <param name="asTask">Process work as separate task</param>
        /// <returns> whether the enqueue is successful. Enqueuing into a disposed WorkQueue will fail and the task will not be performed</returns>>
        public bool EnqueueAndTryWork(T work, bool asTask)
        {
            Interlocked.Increment(ref _count);
            if (_disposed)
            {
                // Remove self from count in case Dispose() is actively waiting for completion
                Interlocked.Decrement(ref _count);
                return false;
            }

            _queue.Push(work);

            // Try to take over work queue processing if needed
            while (true)
            {
                int count = _count;
                if (count >= kMaxQueueSize) return true;
                if (Interlocked.CompareExchange(ref _count, count + kMaxQueueSize, count) == count)
                    break;
            }

            if (asTask)
                _ = Task.Run(ProcessQueue);
            else
                ProcessQueue();
            return true;
        }

        private void ProcessQueue()
        {
            // Process items in work queue
            while (true)
            {
                while (_queue.TryPop(out var workItem))
                {
                    try
                    {
                        _work(workItem);
                    }
                    catch { }
                    Interlocked.Decrement(ref _count);
                }

                int count = _count;
                if (count != kMaxQueueSize) continue;
                if (Interlocked.CompareExchange(ref _count, 0, count) == count)
                    break;
            }
        }
    }
}