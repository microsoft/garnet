// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Garnet.common
{
    /// <summary>
    /// Asynchronous pool of fixed pre-filled capacity
    /// Supports sync get (TryGet) for fast path
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class AsyncPool<T> : IDisposable where T : IDisposable
    {
        readonly int size;
        readonly Func<T> creator;
        readonly SemaphoreSlim handleAvailable;
        readonly ConcurrentQueue<T> itemQueue;

        bool disposed = false;
        int totalAllocated = 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="size"></param>
        /// <param name="creator"></param>
        public AsyncPool(int size, Func<T> creator)
        {
            this.size = size;
            this.creator = creator;
            this.handleAvailable = new SemaphoreSlim(0);
            this.itemQueue = new ConcurrentQueue<T>();
        }

        /// <summary>
        /// Get item synchronously
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public T Get(CancellationToken token = default)
        {
            for (; ; )
            {
                if (disposed)
                    throw new Exception("Getting handle in disposed device");

                if (GetOrAdd(itemQueue, out T item))
                    return item;

                handleAvailable.Wait(token);
            }
        }

        /// <summary>
        /// Get item asynchronously
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask<T> GetAsync(CancellationToken token = default)
        {
            for (; ; )
            {
                if (disposed)
                    throw new Exception("Getting handle in disposed device");

                if (GetOrAdd(itemQueue, out T item))
                    return item;

                await handleAvailable.WaitAsync(token).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Try get item (fast path)
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryGet(out T item)
        {
            if (disposed)
            {
                item = default;
                return false;
            }
            return GetOrAdd(itemQueue, out item);
        }

        /// <summary>
        /// Return item to pool
        /// </summary>
        /// <param name="item"></param>
        public void Return(T item)
        {
            itemQueue.Enqueue(item);
            if (handleAvailable.CurrentCount < itemQueue.Count)
                handleAvailable.Release();
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            disposed = true;

            while (totalAllocated > 0)
            {
                while (itemQueue.TryDequeue(out var item))
                {
                    item.Dispose();
                    Interlocked.Decrement(ref totalAllocated);
                }
                if (totalAllocated > 0)
                    handleAvailable.Wait();
            }
        }

        /// <summary>
        /// Get item from queue, adding up to pool-size items if necessary
        /// </summary>
        private bool GetOrAdd(ConcurrentQueue<T> itemQueue, out T item)
        {
            if (itemQueue.TryDequeue(out item)) return true;

            var _totalAllocated = totalAllocated;
            while (_totalAllocated < size)
            {
                if (Interlocked.CompareExchange(ref totalAllocated, _totalAllocated + 1, _totalAllocated) == _totalAllocated)
                {
                    item = creator();
                    return true;
                }
                if (itemQueue.TryDequeue(out item)) return true;
                _totalAllocated = totalAllocated;
            }
            return false;
        }
    }
}