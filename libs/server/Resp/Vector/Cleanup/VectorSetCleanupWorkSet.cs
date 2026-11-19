// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Garnet.server
{
    /// <summary>
    /// A keyed set of outstanding Vector Set cleanup work.
    /// </summary>
    internal sealed class VectorSetCleanupWorkSet<TValue>
    {
        private readonly ConcurrentDictionary<byte[], TValue> entries;
        private readonly ConcurrentDictionary<byte[], TValue>.AlternateLookup<ReadOnlySpan<byte>> lookup;
        /// <summary>
        /// Are there any pending items for cleanup?
        /// </summary>
        public bool IsEmpty => entries.IsEmpty;

        public VectorSetCleanupWorkSet()
        {
            entries = new(ByteArrayComparer.Instance);
            lookup = entries.GetAlternateLookup<ReadOnlySpan<byte>>();
        }

        /// <summary>
        /// Whether work is still pending for <paramref name="key"/>.
        /// </summary>
        public bool Contains(ReadOnlySpan<byte> key)
        {
            return lookup.ContainsKey(key);
        }

        /// <summary>
        /// Block until no work is pending for <paramref name="key"/>. Entries are removed only once the work
        /// has been performed, so returning implies completion, not merely dequeue.
        ///
        /// Do not call this while holding any Vector Set related locks, we will deadlock.
        /// </summary>
        public void WaitForCompletion(ReadOnlySpan<byte> key)
        {
            while (Contains(key))
            {
                _ = Thread.Yield();
            }
        }

        /// <summary>
        /// Add work for <paramref name="key"/>. False if work is already pending for it.
        /// </summary>
        public bool TryAdd(byte[] key, TValue value) => entries.TryAdd(key, value);

        /// <summary>
        /// Remove the entry, marking its work done. False if it was not present.
        /// </summary>
        public bool TryComplete(byte[] key) => entries.TryRemove(key, out _);

        /// <summary>
        /// Iterate the pending work, so a consumer can service the whole backlog in one pass.
        /// </summary>
        public IEnumerator<KeyValuePair<byte[], TValue>> GetEnumerator() => entries.GetEnumerator();
    }
}