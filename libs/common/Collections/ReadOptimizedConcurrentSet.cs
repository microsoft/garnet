// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.common
{
    /// <summary>
    /// Concurrent set optimized for read-mostly workloads.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ReadOptimizedConcurrentSet<T>
    {
        SingleWriterMultiReaderLock rwLock;
        readonly List<T> list;

        /// <summary>
        /// Constructor
        /// </summary>
        public ReadOptimizedConcurrentSet()
        {
            rwLock = new SingleWriterMultiReaderLock();
            list = [];
        }

        /// <summary>
        /// Number of items in the set
        /// </summary>
        public int Count
        {
            get
            {
                rwLock.ReadLock();
                var count = list.Count;
                rwLock.ReadUnlock();
                return count;
            }
        }

        /// <summary>
        /// Try to add an item to the set
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryAdd(T item)
        {
            var ret = false;
            rwLock.WriteLock();
            if (!list.Contains(item))
            {
                list.Add(item);
                ret = true;
            }
            rwLock.WriteUnlock();
            return ret;
        }

        /// <summary>
        /// Try to add an item to the set
        /// </summary>
        /// <param name="item">Item to be added</param>
        /// <param name="count">Count of items in set after the operation</param>
        /// <returns></returns>
        public bool TryAdd(T item, out int count)
        {
            var ret = false;
            rwLock.WriteLock();
            if (!list.Contains(item))
            {
                list.Add(item);
                ret = true;
            }
            count = list.Count;
            rwLock.WriteUnlock();
            return ret;
        }

        /// <summary>
        /// Try to add an item to the set and return the added (or existing) item
        /// </summary>
        /// <param name="item">Item to be added</param>
        /// <param name="addedItem">Added or existing item</param>
        /// <returns>Whether provided item was added</returns>
        public bool TryAddAndGet(T item, out T addedItem)
        {
            var ret = false;
            rwLock.WriteLock();
            var i = list.IndexOf(item);
            if (i == -1)
            {
                list.Add(item);
                addedItem = item;
                ret = true;
            }
            else
            {
                addedItem = list[i];
            }
            rwLock.WriteUnlock();
            return ret;
        }

        /// <summary>
        /// Try to remove an item from the set
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryRemove(T item)
        {
            rwLock.WriteLock();
            var ret = list.Remove(item);
            rwLock.WriteUnlock();
            return ret;
        }

        /// <summary>
        /// Iterator for the set
        /// </summary>
        /// <param name="index">Current index (start at 0)</param>
        /// <param name="item">Item</param>
        /// <returns>Whether iteration ended</returns>
        public bool Iterate(ref int index, out T item)
        {
            rwLock.ReadLock();
            if (index < list.Count)
            {
                item = list[index];
                index++;
                rwLock.ReadUnlock();
                return true;
            }
            rwLock.ReadUnlock();
            item = default;
            return false;
        }

        /// <summary>
        /// Clear the set
        /// </summary>
        public void Clear()
        {
            rwLock.WriteLock();
            list.Clear();
            rwLock.WriteUnlock();
        }
    }
}