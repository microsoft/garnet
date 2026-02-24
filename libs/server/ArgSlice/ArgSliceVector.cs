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
        private bool enumerating;

        ScratchBufferBuilder bufferManager = new();
        readonly int maxCount = maxItemNum;
        public int Count => items.Count;
        public bool IsEmpty => items.Count == 0;
        readonly List<(int Offset, int Length, bool HasNamespace)> items = [];

        /// <summary>
        /// Try to add ArgSlice
        /// </summary>
        /// <param name="item"></param>
        /// <returns>True if it succeeds to add ArgSlice, false if maxCount has been reached.</returns>
        public bool TryAddItem(Span<byte> item)
        {
            Debug.Assert(!enumerating, "Cannot modify while enumerating");

            if (Count + 1 >= maxCount)
                return false;

            var insertLoc = bufferManager.ScratchBufferOffset;

            var sb = bufferManager.CreateArgSlice(item);

            items.Add((insertLoc, sb.Length, HasNamespace: false));
            return true;
        }

        /// <summary>
        /// Try to add ArgSlice
        /// </summary>
        /// <param name="ns"></param>
        /// <param name="item"></param>
        /// <returns>True if it succeeds to add ArgSlice, false if maxCount has been reached.</returns>
        public bool TryAddItem(ulong ns, Span<byte> item)
        {
            Debug.Assert(!enumerating, "Cannot modify while enumerating");
            Debug.Assert(ns <= byte.MaxValue, "Only byte-size namespaces supported currently");

            if (Count + 1 >= maxCount)
                return false;

            var insertLoc = bufferManager.ScratchBufferOffset;

            var argSlice = bufferManager.CreateArgSlice(item.Length + 1);
            var sb = argSlice.SpanByte;

            sb.MarkNamespace();
            sb.SetNamespaceInPayload((byte)ns);
            item.CopyTo(sb.AsSpan());

            items.Add((insertLoc, sb.Length, HasNamespace: true));
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

        public IEnumerator<SpanByte> GetEnumerator()
        {
            Debug.Assert(!enumerating, "Concurrent enumeration is not allwed");

            var full = bufferManager.ViewFullArgSlice();

            enumerating = true;
            try
            {
                foreach (var (offset, length, hasNamespace) in items)
                {
                    var span = full.ReadOnlySpan.Slice(offset, length);
                    var ret = SpanByte.FromPinnedSpan(span);

                    if (hasNamespace)
                    {
                        ret.MarkNamespace();
                    }

                    yield return ret;
                }
            }
            finally
            {
                enumerating = false;
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}