// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;

namespace Garnet.client
{
    /// <summary>
    /// A request-lane payload for the <see cref="DuplexOperationChannel{TRequest, TCompletion, TTransport}"/>
    /// (and its <see cref="LightNetworkWriter"/> realization). It is a pure request descriptor: it
    /// carries only the rented buffer and its length. The response completion no longer travels with
    /// the payload — in the duplex ring the completion lives in a separate, reply-gated lane keyed by
    /// the completion ticket that the combined allocator hands out alongside the request address.
    /// The request lane is freed on flush (send), independently of when the reply arrives.
    /// </summary>
    struct LightRequestContext : IRequestContext
    {
        /// <summary>
        /// Rented buffer holding the serialized command bytes.
        /// </summary>
        internal PoolEntry poolEntry;

        int length;
        WaiterQueue<MemoryThrottle, int> outOfLineRentedBytesWaiters;
        int reservedBytes;

        /// <inheritdoc />
        public byte[] Buffer => poolEntry.entry;

        /// <inheritdoc />
        public readonly int Length => length;

        internal LightRequestContext(
            PoolEntry poolEntry,
            int length,
            WaiterQueue<MemoryThrottle, int> outOfLineRentedBytesWaiters,
            int reservedBytes)
        {
            this.poolEntry = poolEntry;
            this.length = length;
            this.outOfLineRentedBytesWaiters = outOfLineRentedBytesWaiters;
            this.reservedBytes = reservedBytes;
        }

        internal static int GetRequestBufferAllocationSize(LimitedFixedBufferPool pool, int length)
        {
            var allocationSize = length;
            if (length <= pool.MaxAllocationSize)
            {
                var minimumSize = Math.Max(length, pool.MinAllocationSize);
                var roundedSize = System.Numerics.BitOperations.RoundUpToPowerOf2((uint)minimumSize);
                if (roundedSize <= (uint)pool.MaxAllocationSize)
                    allocationSize = (int)roundedSize;
            }

            return allocationSize;
        }

        internal static LightRequestContext RentRequestBuffer(
            LimitedFixedBufferPool pool,
            int length,
            WaiterQueue<MemoryThrottle, int> outOfLineRentedBytesWaiters = null,
            int reservedBytes = 0)
        {
            var allocationSize = GetRequestBufferAllocationSize(pool, length);
            if ((outOfLineRentedBytesWaiters == null) != (reservedBytes == 0))
                throw new InvalidOperationException("Out-of-line rental admission and reserved bytes must be provided together.");
            if (reservedBytes != 0 && reservedBytes != allocationSize)
                throw new InvalidOperationException($"Reserved {reservedBytes} bytes for a {allocationSize}-byte request allocation.");

            var entry = pool.Get(allocationSize, PoolEntryBufferType.OutOfLinePayload);
            ObjectDisposedException.ThrowIf(entry is null, pool);
            return new LightRequestContext(entry, length, outOfLineRentedBytesWaiters, reservedBytes);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (poolEntry == null)
                return;

            var entry = poolEntry;
            var waiters = outOfLineRentedBytesWaiters;
            var bytes = reservedBytes;
            poolEntry = null;
            outOfLineRentedBytesWaiters = null;
            reservedBytes = 0;

            entry.Dispose();
            waiters?.Release(bytes);
        }
    }

}