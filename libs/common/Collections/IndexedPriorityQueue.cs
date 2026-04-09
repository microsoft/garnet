// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Garnet.common.Collections
{
    /// <summary>
    /// In-place updatable min-heap. With methods to access priority in constant time.
    /// </summary>
    public class IndexedPriorityQueue<TElement, TPriority>
    {
        // element -> index in heap
        private readonly Dictionary<TElement, int> _index;

        private const int DefaultCapacity = 4;

        // binary heap
        private (TElement element, TPriority priority)[] _heap = [];
        private int _count;

        /// <summary>
        /// Number of elements in the priority queue.
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Creates an IndexedPriorityQueue using the default equality comparer for elements.
        /// </summary>
        public IndexedPriorityQueue() : this(null) { }

        /// <summary>
        /// Creates an IndexedPriorityQueue using the specified equality comparer for elements.
        /// </summary>
        public IndexedPriorityQueue(IEqualityComparer<TElement> comparer)
        {
            _index = new Dictionary<TElement, int>(comparer);
        }

        public bool Exists(TElement element) => _index.ContainsKey(element);

        /// <summary>
        /// O(log N) - Enqueue or update the priority of a key
        /// </summary>
        /// <param name="element"></param>
        /// <param name="value"></param>
        public void EnqueueOrUpdate(TElement element, TPriority value)
        {
            if (_index.TryGetValue(element, out int idxInHeap))
            {
                _index[element] = UpdateHeap(idxInHeap, value);
                return;
            }

            _index[element] = InsertIntoHeap(element, value);
        }

        /// <summary>
        /// O(log N) - Dequeue Key with Lowest Priority
        /// </summary>
        /// <returns>Element with lowest priority</returns>
        public TElement Dequeue()
        {
            if (_count == 0)
                throw new InvalidOperationException("The queue is empty.");

            return DequeueFromHeap();
        }

        /// 
        /// <summary>
        /// O(1) - Try to peek at the element with the lowest priority
        /// </summary>
        /// <param name="key">The element with the lowest priority</param>
        /// <param name="value">The priority of the element</param>
        /// <returns>True if the queue is not empty, otherwise false</returns>
        public bool TryPeek(out TElement key, out TPriority value)
        {
            if (_count == 0)
            {
                key = default!;
                value = default!;
                return false;
            }
            (key, value) = _heap[0];
            return true;
        }

        /// <summary>
        /// O(log N) - Change the priority of an element
        /// </summary>
        /// <param name="key">The element whose priority is to be changed</param>
        /// <param name="newValue">The new priority value</param>
        public void ChangePriority(TElement key, TPriority newValue) => _index[key] = UpdateHeap(_index[key], newValue);

        /// <summary>
        /// O(1) - Get the priority of an element
        /// </summary>
        /// <param name="key">The element whose priority is to be retrieved</param>
        /// <param name="value">The priority of the element</param>
        public void GetPriority(TElement key, out TPriority value) => value = _heap[_index[key]].priority;


        /// <summary>
        /// O(1) - Try to get the priority of an element. Returns false if the element is not in the queue.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool TryGetPriority(TElement key, out TPriority value)
        {
            if (_index.TryGetValue(key, out int idxInHeap))
            {
                value = _heap[idxInHeap].priority;
                return true;
            }
            value = default!;
            return false;
        }


        /// <summary>
        /// O(log N) - Try to remove an element from the queue. Returns false if the element is not in the queue.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool TryRemove(TElement key)
        {
            if (!_index.TryGetValue(key, out int idxInHeap))
            {
                return false;
            }

            _index.Remove(key);
            _count--;
            if (idxInHeap == _count)
            {
                // Removing the last element, no need to sift
                return true;
            }

            _heap[idxInHeap] = _heap[_count];
            _index[_heap[idxInHeap].element] = idxInHeap;

            // Try sifting down, if it doesn't move then try sifting up
            if (SiftDown(idxInHeap) == idxInHeap)
            {
                SiftUp(idxInHeap);
            }

            if (_heap.Length > DefaultCapacity && _count < _heap.Length / 2)
            {
                Shrink();
            }
            return true;
        }


        // helper - methods

        private int InsertIntoHeap(TElement key, TPriority value)
        {
            if (_count == _heap.Length)
            {
                Grow(_count + 1);
            }

            _heap[_count] = (key, value);
            _index[key] = _count;
            _count++;
            return SiftUp(_count - 1);
        }

        private TElement DequeueFromHeap()
        {
            TElement element = _heap[0].element;
            _index.Remove(element);
            _count--;

            if (_count > 0)
            {
                _heap[0] = _heap[_count];
                _index[_heap[0].element] = 0;
                SiftDown(0);
            }

            if (_heap.Length > DefaultCapacity && _count < _heap.Length / 2)
            {
                Shrink();
            }

            return element;
        }

        private int UpdateHeap(int idxInHeap, TPriority newValue)
        {
            TPriority oldValue = _heap[idxInHeap].priority;
            TElement element = _heap[idxInHeap].element;
            _heap[idxInHeap] = (element, newValue);

            int cmp = Comparer<TPriority>.Default.Compare(newValue, oldValue);
            if (cmp < 0)
            {
                // new priority is smaller – sift up
                return SiftUp(idxInHeap);
            }
            else if (cmp > 0)
            {
                // new priority is larger – sift down
                return SiftDown(idxInHeap);
            }

            return idxInHeap;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int SiftUp(int currIdx)
        {
            var entry = _heap[currIdx];
            while (currIdx > 0)
            {
                int parentIdx = GetParentIndex(currIdx);
                if (Comparer<TPriority>.Default.Compare(_heap[parentIdx].priority, entry.priority) <= 0)
                    break;

                _heap[currIdx] = _heap[parentIdx];
                _index[_heap[currIdx].element] = currIdx;
                currIdx = parentIdx;
            }
            _heap[currIdx] = entry;
            _index[entry.element] = currIdx;
            return currIdx;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int SiftDown(int currIdx)
        {
            var entry = _heap[currIdx];
            while (true)
            {
                int smallerChildIdx = GetLeftChildIndex(currIdx);
                if (smallerChildIdx >= _count)
                    break;

                int rightChildIdx = smallerChildIdx + 1;
                if (rightChildIdx < _count && Comparer<TPriority>.Default.Compare(_heap[rightChildIdx].priority, _heap[smallerChildIdx].priority) < 0)
                    smallerChildIdx = rightChildIdx;

                if (Comparer<TPriority>.Default.Compare(entry.priority, _heap[smallerChildIdx].priority) <= 0)
                    break;

                _heap[currIdx] = _heap[smallerChildIdx];
                _index[_heap[currIdx].element] = currIdx;
                currIdx = smallerChildIdx;
            }
            _heap[currIdx] = entry;
            _index[entry.element] = currIdx;
            return currIdx;
        }

        private int GetParentIndex(int i) => (i - 1) / 2;

        private int GetLeftChildIndex(int i) => (2 * i) + 1;

        /// <summary>
        /// Grows the priority queue to match the specified min capacity.
        /// </summary>
        private void Grow(int minCapacity)
        {
            Debug.Assert(_heap.Length < minCapacity);

            const int GrowFactor = 2;
            const int MinimumGrow = 4;

            int newcapacity = GrowFactor * _heap.Length;

            // Allow the queue to grow to maximum possible capacity (~2G elements) before encountering overflow.
            // Note that this check works even when _heap.Length overflowed thanks to the (uint) cast
            if ((uint)newcapacity > Array.MaxLength) newcapacity = Array.MaxLength;

            // Ensure minimum growth is respected.
            newcapacity = Math.Max(newcapacity, _heap.Length + MinimumGrow);

            // If the computed capacity is still less than specified, set to the original argument.
            // Capacities exceeding Array.MaxLength will be surfaced as OutOfMemoryException by Array.Resize.
            if (newcapacity < minCapacity) newcapacity = minCapacity;

            Array.Resize(ref _heap, newcapacity);
        }

        /// <summary>
        /// Shrinks the backing array when more than half the space is unoccupied.
        /// </summary>
        private void Shrink()
        {
            int newCapacity = _heap.Length / 2;
            newCapacity = Math.Max(newCapacity, DefaultCapacity);

            if (newCapacity < _heap.Length)
            {
                Array.Resize(ref _heap, newCapacity);
            }
        }
    }
}
