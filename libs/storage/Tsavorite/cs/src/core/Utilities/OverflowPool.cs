// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;

namespace Tsavorite.core
{
    /// <summary>
    /// Fixed size pool of overflow objects
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class OverflowPool<T> : IDisposable
    {
        readonly int size;
        readonly ConcurrentQueue<T> itemQueue;
        readonly Action<T> disposer;

        /// <summary>
        /// Number of pages in pool
        /// </summary>
        public int Count => itemQueue.Count;

        bool disposed = false;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="size"></param>
        /// <param name="disposer"></param>
        public OverflowPool(int size, Action<T> disposer = null)
        {
            this.size = size;
            itemQueue = new ConcurrentQueue<T>();
            this.disposer = disposer ?? (e => { });
        }

        /// <summary>
        /// Try get overflow item, if it exists
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryGet(out T item)
        {
            return itemQueue.TryDequeue(out item);
        }

        /// <summary>
        /// Try to add overflow item to pool
        /// </summary>
        /// <param name="item"></param>
        public bool TryAdd(T item)
        {
            if (itemQueue.Count < size && !disposed)
            {
                itemQueue.Enqueue(item);
                return true;
            }
            else
            {
                disposer(item);
                return false;
            }
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            disposed = true;
            while (itemQueue.TryDequeue(out var item))
                disposer(item);
        }
    }
}