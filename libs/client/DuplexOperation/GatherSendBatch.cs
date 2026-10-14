// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using Garnet.common;

namespace Garnet.client
{
    /// <summary>
    /// A reusable, fixed-capacity holder for a contiguous run of single-chunk request payloads coalesced into
    /// one scatter/gather network send. It exposes its payload segments as the <see cref="IList{T}"/> consumed
    /// by <c>SocketAsyncEventArgs.BufferList</c> (only <see cref="Count"/> and the indexer are used) while also
    /// carrying the matching per-record send-completion contexts.
    /// <para>
    /// Capacity is the ring's request-slot count: records are allocated in fixed quanta, so a single flush can
    /// never contain more records than there are slots. Instances are pooled, so steady-state coalescing
    /// allocates nothing; a fresh instance is checked out per dispatched send so an in-flight send's completion
    /// can read it while the next batch accumulates into a different instance.
    /// </para>
    /// </summary>
    sealed class GatherSendBatch : IList<ArraySegment<byte>>, IDisposable
    {
        readonly ArraySegment<byte>[] segments;
        readonly object[] contexts;
        readonly SimpleObjectPool<GatherSendBatch> pool;
        int count;

        internal GatherSendBatch(int capacity, SimpleObjectPool<GatherSendBatch> pool)
        {
            segments = new ArraySegment<byte>[capacity];
            contexts = new object[capacity];
            this.pool = pool;
        }

        /// <summary>Number of records accumulated so far.</summary>
        internal int RecordCount => count;

        /// <summary>Send-completion context for the record at <paramref name="index"/>.</summary>
        internal object ContextAt(int index) => contexts[index];

        /// <summary>Append one single-chunk record's payload segment and its completion context.</summary>
        internal void Append(byte[] buffer, int offset, int length, object context)
        {
            segments[count] = new ArraySegment<byte>(buffer, offset, length);
            contexts[count] = context;
            count++;
        }

        /// <summary>Clear references and return this holder to its pool for reuse.</summary>
        internal void Release()
        {
            // Drop buffer/context references so a pooled holder pins no memory between uses.
            for (var i = 0; i < count; i++)
            {
                segments[i] = default;
                contexts[i] = null;
            }
            count = 0;
            pool.Return(this);
        }

        public void Dispose() { }

        // IList surface consumed by SocketAsyncEventArgs.BufferList: only Count and the indexer are exercised;
        // the remaining members are mutation/search operations the gather send never calls.
        public int Count => count;

        public bool IsReadOnly => true;

        public ArraySegment<byte> this[int index]
        {
            get => segments[index];
            set => throw new NotSupportedException();
        }

        public void CopyTo(ArraySegment<byte>[] array, int arrayIndex)
        {
            for (var i = 0; i < count; i++)
                array[arrayIndex + i] = segments[i];
        }

        public IEnumerator<ArraySegment<byte>> GetEnumerator()
        {
            for (var i = 0; i < count; i++)
                yield return segments[i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public int IndexOf(ArraySegment<byte> item) => throw new NotSupportedException();

        public void Insert(int index, ArraySegment<byte> item) => throw new NotSupportedException();

        public void RemoveAt(int index) => throw new NotSupportedException();

        public void Add(ArraySegment<byte> item) => throw new NotSupportedException();

        public void Clear() => throw new NotSupportedException();

        public bool Contains(ArraySegment<byte> item) => throw new NotSupportedException();

        public bool Remove(ArraySegment<byte> item) => throw new NotSupportedException();
    }
}