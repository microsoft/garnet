// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Tsavorite.core
{
    sealed class PendingFlushList
    {
        public readonly LinkedList<PageAsyncFlushResult<Empty>> list;

        public PendingFlushList()
        {
            list = new();
        }

        public void Add(PageAsyncFlushResult<Empty> t)
        {
            lock (list)
            {
                list.AddFirst(t);
            }
        }

        /// <summary>
        /// Remove item from flush list with from-address equal to the specified address
        /// </summary>
        public bool RemoveNextAdjacent(long address, out PageAsyncFlushResult<Empty> request)
        {
            lock (list)
            {
                for (var it = list.First; it != null;)
                {
                    request = it.Value;
                    if (request.fromAddress == address)
                    {
                        list.Remove(it);
                        return true;
                    }
                    it = it.Next;
                }
            }
            request = null;
            return false;
        }

        /// <summary>
        /// Remove item from flush list with until-address equal to the specified address
        /// </summary>
        public bool RemovePreviousAdjacent(long address, out PageAsyncFlushResult<Empty> request)
        {
            lock (list)
            {
                for (var it = list.First; it != null;)
                {
                    request = it.Value;
                    if (request.untilAddress == address)
                    {
                        list.Remove(it);
                        return true;
                    }
                    it = it.Next;
                }
            }
            request = null;
            return false;
        }
    }
}