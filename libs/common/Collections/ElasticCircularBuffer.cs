// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace Garnet.common
{
    /// <summary>
    /// Circular buffer
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [DataContract]
    sealed class CircularBuffer<T>
    {
        public const int DefaultCapacity = 0xfff;
        [DataMember]
        public T[] Items = new T[DefaultCapacity + 1];
        [DataMember]
        public int head = 0;
        [DataMember]
        public int tail = 0;
        [DataMember]
        public bool Sealed = false;

        public CircularBuffer()
        {
        }

        public T PeekFirst()
        {
            return Items[head];
        }

        public T PeekLast()
        {
            return Items[(tail - 1) & DefaultCapacity];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(ref T value)
        {
            int next = (tail + 1) & DefaultCapacity;
            if (next == head)
            {
                throw new InvalidOperationException("The inner list is full!");
            }
            Items[tail] = value;
            tail = next;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Dequeue()
        {
            if (head == tail)
            {
                throw new InvalidOperationException("The list is empty!");
            }
            int oldhead = head;
            head = (head + 1) & DefaultCapacity;
            var ret = Items[oldhead];
            Items[oldhead] = default;
            return ret;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsFull() => (((tail + 1) & DefaultCapacity) == head);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsEmpty() => (head == tail);

        public IEnumerable<T> Iterate()
        {
            int i = head;
            while (i != tail)
            {
                yield return Items[i];
                i = (i + 1) & DefaultCapacity;
            }
        }
    }

    /// <summary>
    /// Elastic circular buffer
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [DataContract]
    public sealed class ElasticCircularBuffer<T> : IEnumerable<T>
    {
        private readonly LinkedList<CircularBuffer<T>> buffers;
        private LinkedListNode<CircularBuffer<T>> head;
        private LinkedListNode<CircularBuffer<T>> tail;

        /// <summary>
        /// Constructor
        /// </summary>
        public ElasticCircularBuffer()
        {
            buffers = new LinkedList<CircularBuffer<T>>();
            var node = new LinkedListNode<CircularBuffer<T>>(new CircularBuffer<T>());
            buffers.AddFirst(node);
            tail = head = node;
        }

        /// <summary>
        /// Enqueue
        /// </summary>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Enqueue(ref T value)
        {
            if (tail.Value.IsFull())
            {
                tail.Value.Sealed = true;
                var next = tail.Next;
                if (next == null) next = buffers.First;
                if (next.Value.Sealed)
                {
                    next = new LinkedListNode<CircularBuffer<T>>(new CircularBuffer<T>());
                    buffers.AddAfter(tail, next);
                }
                next.Value.Enqueue(ref value);
                tail = next;
            }
            else
            {
                if (tail.Value.Sealed)
                    throw new Exception("Unexpected sealed buffer found");
                tail.Value.Enqueue(ref value);
            }
        }

        /// <summary>
        /// Enqueue
        /// </summary>
        /// <param name="value"></param>
        public void Enqueue(T value)
        {
            Enqueue(ref value);
        }

        /// <summary>
        /// Dequeue
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Dequeue()
        {
            if (head.Value.IsEmpty())
            {
                if (head == tail)
                    throw new InvalidOperationException("The outer list is empty!");

                var temp = head;
                head = head.Next;
                if (head == null) head = buffers.First;
                temp.Value.Sealed = false;
            }
            return head.Value.Dequeue();
        }

        /// <summary>
        /// Peek at head
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T PeekFirst()
        {
            if (head.Value.head == head.Value.tail)
            {
                if (head == tail)
                    throw new InvalidOperationException("The list is empty!");

                var temp = head;
                head = head.Next;
                if (head == null) head = buffers.First;
                temp.Value.Sealed = false;
            }
            return head.Value.Items[head.Value.head];
        }

        /// <summary>
        /// Peek at tail
        /// </summary>
        /// <returns></returns>
        public T PeekLast()
        {
            if (tail.Value.IsEmpty())
                throw new InvalidOperationException("The list is empty!");
            return tail.Value.PeekLast();
        }

        /// <summary>
        /// Is empty
        /// </summary>
        /// <returns></returns>
        public bool IsEmpty() => (head.Value.IsEmpty() && (head == tail));

        IEnumerable<T> Iterate()
        {
            foreach (CircularBuffer<T> buffer in buffers)
            {
                foreach (T item in buffer.Iterate())
                {
                    yield return item;
                }
            }
        }

        /// <summary>
        /// Get enumerator
        /// </summary>
        /// <returns></returns>
        public IEnumerator<T> GetEnumerator() => Iterate().GetEnumerator();

        /// <summary>
        /// Get enumerator
        /// </summary>
        /// <returns></returns>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}