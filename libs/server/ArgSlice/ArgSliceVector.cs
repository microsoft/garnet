// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
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
    public unsafe class ArgSliceVector(int maxItemNum = 1 << 18) : IEnumerable<(PinnedSpanByte NamespaceBytes, PinnedSpanByte KeyBytes, bool HasNamespace)>
    {
        private bool enumerating;

        ScratchBufferBuilder bufferManager = new();
        readonly int maxCount = maxItemNum;
        public int Count => items.Count;
        public bool IsEmpty => items.Count == 0;
        readonly List<((int Offset, int Length) Entry, bool HasNamespace)> items = [];

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

            items.Add((entry, false));
            return true;
        }

        /// <summary>
        /// Try to add ArgSlice
        /// </summary>
        /// <param name="namespaceBytes"></param>
        /// <param name="item"></param>
        /// <returns>True if it succeeds to add ArgSlice, false if maxCount has been reached.</returns>
        public bool TryAddItem(ReadOnlySpan<byte> namespaceBytes, ReadOnlySpan<byte> item)
        {
            Debug.Assert(!enumerating, "Cannot modify while enumerating");
            if (Count + 1 >= maxCount)
                return false;

            var insertLoc = bufferManager.ScratchBufferOffset;

            Span<byte> toWrite = stackalloc byte[sizeof(int) + namespaceBytes.Length + sizeof(int) + item.Length];
            BinaryPrimitives.WriteInt32LittleEndian(toWrite, namespaceBytes.Length);
            namespaceBytes.CopyTo(toWrite[sizeof(int)..]);
            BinaryPrimitives.WriteInt32LittleEndian(toWrite[(sizeof(int) + namespaceBytes.Length)..], item.Length);
            item.CopyTo(toWrite[(sizeof(int) + namespaceBytes.Length + sizeof(int))..]);

            var entry = bufferManager.CreateArgSliceAsOffset(toWrite);
            items.Add((entry, true));
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
        public IEnumerator<(PinnedSpanByte NamespaceBytes, PinnedSpanByte KeyBytes, bool HasNamespace)> GetEnumerator()
        {
            Debug.Assert(!enumerating, "Concurrent enumeration is not allwed");

            var full = bufferManager.ViewFullArgSlice();

            enumerating = true;
            try
            {
                foreach (var ((offset, length), hasNamespace) in items)
                {
                    if (!hasNamespace)
                    {
                        var span = full.ReadOnlySpan.Slice(offset, length);
                        var ret = PinnedSpanByte.FromPinnedSpan(span);

                        yield return (default, ret, false);
                    }
                    else
                    {
                        var span = full.ReadOnlySpan.Slice(offset, length);

                        var nsLen = BinaryPrimitives.ReadInt32LittleEndian(span);
                        var ns = PinnedSpanByte.FromPinnedSpan(span.Slice(sizeof(int), nsLen));

                        var keyLen = BinaryPrimitives.ReadInt32LittleEndian(span[(sizeof(int) + nsLen)..]);
                        var key = PinnedSpanByte.FromPinnedSpan(span.Slice(sizeof(int) + nsLen + sizeof(int), keyLen));

                        yield return (ns, key, true);
                    }
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