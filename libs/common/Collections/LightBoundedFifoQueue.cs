// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace Garnet.common
{
    /// <summary>
    /// Fixed-capacity, paged FIFO queue with lock-free multi-producer publication and serialized consumption.
    /// </summary>
    /// <typeparam name="T">Queue item type.</typeparam>
    internal sealed class LightBoundedFifoQueue<T> : IDisposable where T : class
    {
        enum SlotState : byte
        {
            Empty,
            Published,
        }

        struct QueueSlot
        {
            internal T item;
            internal byte slotState;
        }

        readonly RingBoundedBuffer<QueueSlot> buffer;
        readonly ActiveWorkerMonitor publisherMonitor = new();
        readonly int capacity;
        readonly int maxSpinCount;

        long headAddress;
        long tailAddress;
        int disposed;

        /// <summary>
        /// Number of reserved entries that have not been dequeued.
        /// </summary>
        internal int Count
        {
            get
            {
                var head = Volatile.Read(ref headAddress);
                return (int)(Volatile.Read(ref tailAddress) - head);
            }
        }

        /// <summary>
        /// Maximum number of entries retained by the queue.
        /// </summary>
        internal int Capacity => capacity;

        /// <summary>
        /// Creates a fixed-capacity paged FIFO queue.
        /// </summary>
        /// <param name="pageSize">Number of entries per page. Must be a power of two.</param>
        /// <param name="pageCount">Number of pages. Must be a power of two.</param>
        /// <param name="maxSpinCount">Maximum spin iterations after the initial bounded-tail reservation attempt.</param>
        internal LightBoundedFifoQueue(
            int pageSize,
            int pageCount,
            int maxSpinCount)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(maxSpinCount);

            buffer = new RingBoundedBuffer<QueueSlot>(pageSize, pageCount);
            capacity = buffer.Capacity;
            this.maxSpinCount = maxSpinCount;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
                return;

            CompleteAdding();
            while (TryDequeue(out _))
            {
            }
        }

        /// <summary>
        /// Attempts to append an item.
        /// </summary>
        /// <param name="item">Item to append.</param>
        /// <returns>False when bounded slot reservation does not succeed within the configured spin limit.</returns>
        internal bool TryEnqueue(T item)
        {
            ArgumentNullException.ThrowIfNull(item);
            ObjectDisposedException.ThrowIf(!publisherMonitor.TryEnter(), this);

            try
            {
                long address;
                var spinner = new SpinWait();
                while (true)
                {
                    var tail = Volatile.Read(ref tailAddress);
                    if (tail - Volatile.Read(ref headAddress) < capacity &&
                        Interlocked.CompareExchange(ref tailAddress, tail + 1, tail) == tail)
                    {
                        address = tail;
                        break;
                    }

                    if (spinner.Count >= maxSpinCount)
                        return false;

                    spinner.SpinOnce();
                }

                ref var slot = ref buffer[address];
                slot.item = item;
                Volatile.Write(ref slot.slotState, (byte)SlotState.Published);
                return true;
            }
            finally
            {
                _ = publisherMonitor.Exit();
            }
        }

        /// <summary>
        /// Reads the published FIFO head without removing it.
        /// </summary>
        /// <param name="item">Published head item.</param>
        /// <returns>False when the queue is empty or its logical head has not yet been published.</returns>
        /// <remarks>This method must be called by a serialized consumer.</remarks>
        internal bool TryPeek(out T item)
        {
            if (headAddress != Volatile.Read(ref tailAddress))
            {
                ref var slot = ref buffer[headAddress];
                if (Volatile.Read(ref slot.slotState) == (byte)SlotState.Published)
                {
                    item = slot.item;
                    return true;
                }
            }

            item = default;
            return false;
        }

        /// <summary>
        /// Removes the published FIFO head.
        /// </summary>
        /// <param name="item">Removed item.</param>
        /// <returns>True when the published head was removed.</returns>
        /// <remarks>This method must be called by the same serialized consumer as <see cref="TryPeek"/>.</remarks>
        internal bool TryDequeue(out T item)
        {
            if (headAddress == Volatile.Read(ref tailAddress))
            {
                item = default;
                return false;
            }

            ref var slot = ref buffer[headAddress];
            if (Volatile.Read(ref slot.slotState) != (byte)SlotState.Published)
            {
                item = default;
                return false;
            }

            item = slot.item;
            ClearAndAdvanceHead(ref slot);
            return true;

            void ClearAndAdvanceHead(ref QueueSlot slot)
            {
                slot = default;
                Volatile.Write(ref headAddress, headAddress + 1);
            }
        }

        /// <summary>
        /// Prevents new producers and waits for producers that already entered publication to finish.
        /// Existing entries remain available to the consumer.
        /// </summary>
        internal void CompleteAdding() => publisherMonitor.Dispose();
    }
}