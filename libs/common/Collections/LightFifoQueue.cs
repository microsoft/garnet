// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Fixed-capacity, paged FIFO queue with lock-free multi-producer publication and serialized consumption.
    /// </summary>
    /// <typeparam name="T">Queue item type.</typeparam>
    internal sealed class LightFifoQueue<T> : IDisposable where T : class
    {
        enum SlotState
        {
            Empty,
            Published,
            Removed,
        }

        struct QueueSlot
        {
            internal T item;
            internal long address;
            internal int state;
        }

        struct RingPage
        {
            internal readonly QueueSlot[] slots;

            internal RingPage(int pageSize)
            {
                slots = new QueueSlot[pageSize];
            }
        }

        /// <summary>
        /// Identifies one logical queue entry across physical slot reuse.
        /// </summary>
        internal readonly struct EntryHandle
        {
            readonly long addressPlusOne;

            internal long Address => addressPlusOne - 1;
            internal bool IsValid => addressPlusOne != 0;

            internal EntryHandle(long address)
            {
                addressPlusOne = address + 1;
            }
        }

        readonly RingPage[] bufferPages;
        readonly int pageSizeBits;
        readonly int pageSizeMask;
        readonly int capacity;
        readonly Func<T> itemFactory;
        readonly Action<T> itemDisposer;
        readonly T[] itemPool;

        long headAddress;
        long tailAddress;
        int occupiedCount;
        int itemCount;
        int activePublishers;
        int addingCompleted;
        int disposed;
        int pooledItemCount;
        SpinLock itemPoolLock;

        /// <summary>
        /// Number of published entries that have not been dequeued or removed.
        /// </summary>
        internal int Count => Volatile.Read(ref itemCount);

        /// <summary>
        /// Maximum number of entries and removal tombstones retained by the queue.
        /// </summary>
        internal int Capacity => capacity;

        /// <summary>
        /// Creates a fixed-capacity paged FIFO queue.
        /// </summary>
        /// <param name="pageSize">Number of entries per page. Must be a power of two.</param>
        /// <param name="pageCount">Number of pages. Must be a power of two.</param>
        /// <param name="itemFactory">Optional factory used by <see cref="Rent"/>.</param>
        /// <param name="itemDisposer">Optional callback for items rejected by or remaining in the pool during disposal.</param>
        /// <param name="maxPooledItems">Maximum items retained for reuse.</param>
        internal LightFifoQueue(
            int pageSize,
            int pageCount,
            Func<T> itemFactory = null,
            Action<T> itemDisposer = null,
            int maxPooledItems = 0)
        {
            if (pageSize <= 0 || !BitOperations.IsPow2((uint)pageSize))
                throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be a positive power of two.");
            if (pageCount <= 0 || !BitOperations.IsPow2((uint)pageCount))
                throw new ArgumentOutOfRangeException(nameof(pageCount), "Page count must be a positive power of two.");
            ArgumentOutOfRangeException.ThrowIfNegative(maxPooledItems);
            if (maxPooledItems > 0 && itemFactory == null)
                throw new ArgumentNullException(nameof(itemFactory));

            pageSizeBits = BitOperations.Log2((uint)pageSize);
            pageSizeMask = pageSize - 1;
            capacity = checked(pageSize * pageCount);
            this.itemFactory = itemFactory;
            this.itemDisposer = itemDisposer;
            itemPool = new T[maxPooledItems];

            bufferPages = new RingPage[pageCount];
            for (var i = 0; i < bufferPages.Length; i++)
                bufferPages[i] = new RingPage(pageSize);
        }

        /// <summary>
        /// Rents an item from the queue-owned pool or creates one with the configured factory.
        /// </summary>
        internal T Rent()
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
            if (itemFactory == null)
                throw new InvalidOperationException("No item factory was configured.");

            var lockTaken = false;
            try
            {
                itemPoolLock.Enter(ref lockTaken);
                ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
                if (pooledItemCount != 0)
                {
                    var item = itemPool[--pooledItemCount];
                    itemPool[pooledItemCount] = null;
                    return item;
                }
            }
            finally
            {
                if (lockTaken)
                    itemPoolLock.Exit(useMemoryBarrier: false);
            }

            return itemFactory();
        }

        /// <summary>
        /// Returns an item to the queue-owned pool or disposes it when retention is unavailable.
        /// </summary>
        /// <param name="item">Item to return.</param>
        internal void Return(T item)
        {
            ArgumentNullException.ThrowIfNull(item);

            var retained = false;
            var lockTaken = false;
            try
            {
                itemPoolLock.Enter(ref lockTaken);
                if (Volatile.Read(ref disposed) == 0 && pooledItemCount < itemPool.Length)
                {
                    itemPool[pooledItemCount++] = item;
                    retained = true;
                }
            }
            finally
            {
                if (lockTaken)
                    itemPoolLock.Exit(useMemoryBarrier: false);
            }

            if (!retained)
                itemDisposer?.Invoke(item);
        }

        /// <summary>
        /// Attempts to append an item and assign its FIFO logical address.
        /// </summary>
        /// <param name="item">Item to append.</param>
        /// <param name="handle">Handle identifying the published entry.</param>
        /// <returns>False when the queue has no free slot.</returns>
        internal bool TryEnqueue(T item, out EntryHandle handle)
        {
            ArgumentNullException.ThrowIfNull(item);

            Interlocked.Increment(ref activePublishers);
            try
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref addingCompleted) != 0, this);

                if (Interlocked.Increment(ref occupiedCount) > capacity)
                {
                    Interlocked.Decrement(ref occupiedCount);
                    handle = default;
                    return false;
                }

                var address = Interlocked.Increment(ref tailAddress) - 1;
                ref var slot = ref GetSlot(address);
                slot.item = item;
                slot.address = address;
                Interlocked.Increment(ref itemCount);
                Volatile.Write(ref slot.state, (int)SlotState.Published);
                handle = new EntryHandle(address);
                return true;
            }
            finally
            {
                Interlocked.Decrement(ref activePublishers);
            }
        }

        /// <summary>
        /// Reads the published FIFO head without removing it. Removal tombstones are reclaimed automatically.
        /// </summary>
        /// <param name="handle">Handle identifying the head entry.</param>
        /// <param name="item">Published head item.</param>
        /// <returns>False when the queue is empty or its logical head has not yet been published.</returns>
        /// <remarks>This method must be called by a serialized consumer.</remarks>
        internal bool TryPeek(out EntryHandle handle, out T item)
        {
            while (headAddress != Volatile.Read(ref tailAddress))
            {
                ref var slot = ref GetSlot(headAddress);
                var state = Volatile.Read(ref slot.state);
                if (state == (int)SlotState.Empty)
                    break;

                if (state == (int)SlotState.Removed)
                {
                    AdvanceHead();
                    continue;
                }

                handle = new EntryHandle(headAddress);
                item = slot.item;
                return true;
            }

            handle = default;
            item = default;
            return false;
        }

        /// <summary>
        /// Removes the published head when it still matches the supplied handle.
        /// </summary>
        /// <param name="handle">Handle returned by <see cref="TryPeek"/>.</param>
        /// <param name="item">Removed item.</param>
        /// <returns>True when the matching head was removed.</returns>
        /// <remarks>This method must be called by the same serialized consumer as <see cref="TryPeek"/>.</remarks>
        internal bool TryDequeue(EntryHandle handle, out T item)
        {
            if (!handle.IsValid || headAddress != handle.Address)
            {
                item = default;
                return false;
            }

            ref var slot = ref GetSlot(handle.Address);
            if (Volatile.Read(ref slot.state) != (int)SlotState.Published ||
                Volatile.Read(ref slot.address) != handle.Address ||
                Interlocked.CompareExchange(ref slot.state, (int)SlotState.Empty, (int)SlotState.Published) != (int)SlotState.Published)
            {
                item = default;
                return false;
            }

            item = slot.item;
            Interlocked.Decrement(ref itemCount);
            ClearAndAdvanceHead(ref slot);
            return true;
        }

        /// <summary>
        /// Marks a published entry as removed. The serialized consumer reclaims its slot in FIFO order.
        /// </summary>
        /// <param name="handle">Handle returned by <see cref="TryEnqueue"/>.</param>
        /// <param name="item">Removed item.</param>
        /// <returns>True when the matching published entry was marked as removed.</returns>
        internal bool TryRemove(EntryHandle handle, out T item)
        {
            if (!handle.IsValid)
            {
                item = default;
                return false;
            }

            ref var slot = ref GetSlot(handle.Address);
            if (Volatile.Read(ref slot.state) != (int)SlotState.Published ||
                Volatile.Read(ref slot.address) != handle.Address ||
                Interlocked.CompareExchange(ref slot.state, (int)SlotState.Removed, (int)SlotState.Published) != (int)SlotState.Published)
            {
                item = default;
                return false;
            }

            item = slot.item;
            Interlocked.Decrement(ref itemCount);
            return true;
        }

        /// <summary>
        /// Prevents new producers and waits for producers that already entered publication to finish.
        /// Existing entries remain available to the consumer.
        /// </summary>
        internal void CompleteAdding()
        {
            Interlocked.Exchange(ref addingCompleted, 1);

            var spinner = new SpinWait();
            while (Volatile.Read(ref activePublishers) != 0)
                spinner.SpinOnce();
        }

        ref QueueSlot GetSlot(long address)
        {
            var pageIndex = (int)((address >> pageSizeBits) & (bufferPages.Length - 1));
            return ref bufferPages[pageIndex].slots[(int)(address & pageSizeMask)];
        }

        void AdvanceHead()
        {
            ref var slot = ref GetSlot(headAddress);
            ClearAndAdvanceHead(ref slot);
        }

        void ClearAndAdvanceHead(ref QueueSlot slot)
        {
            slot = default;
            Volatile.Write(ref headAddress, headAddress + 1);
            Interlocked.Decrement(ref occupiedCount);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            CompleteAdding();
            while (TryPeek(out var handle, out _))
                TryDequeue(handle, out _);

            T[] pooledItems = null;
            var lockTaken = false;
            try
            {
                itemPoolLock.Enter(ref lockTaken);
                if (pooledItemCount != 0)
                {
                    pooledItems = new T[pooledItemCount];
                    Array.Copy(itemPool, pooledItems, pooledItemCount);
                    Array.Clear(itemPool, 0, pooledItemCount);
                    pooledItemCount = 0;
                }
            }
            finally
            {
                if (lockTaken)
                    itemPoolLock.Exit(useMemoryBarrier: false);
            }

            if (itemDisposer != null && pooledItems != null)
            {
                foreach (var item in pooledItems)
                    itemDisposer(item);
            }
        }
    }
}