// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Vector of ArgSlices
    /// </summary>
    /// <param name="maxItemNum"></param>
    public unsafe class ArgSliceVector(int maxItemNum = 1 << 18) : IEnumerable<PinnedSpanByte>
    {
        private bool enumerating;

        ScratchBufferBuilder bufferManager = new();
        readonly int maxCount = maxItemNum;
        public int Count => items.Count;
        public bool IsEmpty => items.Count == 0;
        readonly List<(int Offset, int Length)> items = [];

        /// <summary>
        /// Try to add ArgSlice
        /// </summary>
        /// <param name="item"></param>
        /// <returns>True if it succeeds to add ArgSlice, false if maxCount has been reached.</returns>
        public bool TryAddItem(ReadOnlySpan<byte> item)
        {
            Debug.Assert(!enumerating, "Cannot modify while enumerating");

            if (Count + 1 >= maxCount)
                return false;

            var entry = bufferManager.CreateArgSliceAsOffset(item);
            items.Add(entry);
            return true;
        }

        /// <summary>
        /// Clear ArgSliceVector
        /// </summary>
        public void Clear()
        {
            Debug.Assert(!enumerating, "Cannot modify while enumerating");

            items.Clear();
            bufferManager.Reset();
        }

        /// <inheritdoc/>
        public IEnumerator<PinnedSpanByte> GetEnumerator()
        {
            Debug.Assert(!enumerating, "Concurrent enumeration is not allwed");

            var full = bufferManager.ViewFullArgSlice();

            enumerating = true;
            try
            {
                foreach (var (offset, length) in items)
                {
                    var span = full.ReadOnlySpan.Slice(offset, length);
                    var ret = PinnedSpanByte.FromPinnedSpan(span);

                    yield return ret;
                }
            }
            finally
            {
                enumerating = false;
            }
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}