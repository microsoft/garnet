// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Garnet.common;

namespace Garnet.server
{
    class ConcurrentList<T>
    {
        SingleWriterMultiReaderLock rwLock;
        readonly List<T> list;

        public ConcurrentList()
        {
            rwLock = new SingleWriterMultiReaderLock();
            list = new List<T>();
        }

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

        public void Add(T item)
        {
            rwLock.WriteLock();
            list.Add(item);
            rwLock.WriteUnlock();
        }

        public bool RemoveAll(T item)
        {
            var ret = false;
            rwLock.WriteLock();
            while (list.Remove(item))
                ret = true;
            rwLock.WriteUnlock();
            return ret;
        }

        public bool Get(ref int index, out T item)
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
    }
}