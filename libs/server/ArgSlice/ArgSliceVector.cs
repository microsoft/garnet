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
    public unsafe class ArgSliceVector(int maxItemNum = 1 << 18) : IEnumerable<SpanByte>
    {
        ScratchBufferBuilder bufferManager = new();
        readonly int maxCount = maxItemNum;
        public int Count => items.Count;
        public bool IsEmpty => items.Count == 0;
        readonly List<SpanByte> items = [];

        /// <summary>
        /// Try to add ArgSlice
        /// </summary>
        /// <param name="item"></param>
        /// <returns>True if it succeeds to add ArgSlice, false if maxCount has been reached.</returns>
        public bool TryAddItem(Span<byte> item)
        {
            if (Count + 1 >= maxCount)
                return false;

            var argSlice = bufferManager.CreateArgSlice(item);

            items.Add(argSlice.SpanByte);
            return true;
        }

        /// <summary>
        /// Try to add ArgSlice
        /// </summary>
        /// <param name="item"></param>
        /// <returns>True if it succeeds to add ArgSlice, false if maxCount has been reached.</returns>
        public bool TryAddItem(ulong ns, Span<byte> item)
        {
            Debug.Assert(ns <= byte.MaxValue, "Only byte-size namespaces supported currently");

            if (Count + 1 >= maxCount)
                return false;

            var argSlice = bufferManager.CreateArgSlice(item.Length + 1);
            var sb = argSlice.SpanByte;

            sb.MarkNamespace();
            sb.SetNamespaceInPayload((byte)ns);
            item.CopyTo(sb.AsSpan());

            items.Add(sb);
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

        public IEnumerator<SpanByte> GetEnumerator()
        {
            foreach (var item in items)
                yield return item;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}