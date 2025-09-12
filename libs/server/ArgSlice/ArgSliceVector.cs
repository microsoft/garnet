// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Vector of ArgSlices represented as PinnedSpanByte
    /// </summary>
    /// <param name="maxItemNum"></param>
    public unsafe class ArgSliceVector(int maxItemNum = 1 << 18) : IEnumerable<PinnedSpanByte>
    {
        ScratchBufferBuilder bufferManager = new();
        readonly int maxCount = maxItemNum;
        public int Count => items.Count;
        public bool IsEmpty => items.Count == 0;
        readonly List<PinnedSpanByte> items = [];

        /// <summary>
        /// Try to add ArgSlice
        /// </summary>
        /// <param name="item"></param>
        /// <returns>True if it succeeds to add ArgSlice, false if maxCount has been reached.</returns>
        public bool TryAddItem(ReadOnlySpan<byte> item)
        {
            if (Count + 1 >= maxCount)
                return false;

            items.Add(bufferManager.CreateArgSlice(item));
            return true;
        }

        /// <summary>
        /// Clear ArgSliceVector
        /// </summary>
        public void Clear()
        {
            items.Clear();
            bufferManager.Reset();
        }

        /// <inheritdoc/>
        public IEnumerator<PinnedSpanByte> GetEnumerator()
        {
            foreach (var item in items)
                yield return item;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}