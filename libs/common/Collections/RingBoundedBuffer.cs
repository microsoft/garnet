// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;

namespace Garnet.common
{
    /// <summary>
    /// Fixed-capacity ring buffer addressed by monotonically increasing logical positions.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    internal sealed class RingBoundedBuffer<T>
    {
        readonly T[] buffer;
        readonly int capacityMask;

        /// <summary>
        /// Number of logical positions retained by the ring.
        /// </summary>
        internal int Capacity { get; }

        /// <summary>
        /// Creates a fixed-capacity ring buffer. Capacity is <paramref name="pageSize"/> * <paramref name="pageCount"/>;
        /// both are required to be powers of two so the capacity is a power of two and logical positions wrap with a mask.
        /// </summary>
        /// <param name="pageSize">Number of elements per page. Must be a power of two.</param>
        /// <param name="pageCount">Number of pages. Must be a power of two.</param>
        internal RingBoundedBuffer(int pageSize, int pageCount)
        {
            if (pageSize <= 0 || !BitOperations.IsPow2((uint)pageSize))
                throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be a positive power of two.");
            if (pageCount <= 0 || !BitOperations.IsPow2((uint)pageCount))
                throw new ArgumentOutOfRangeException(nameof(pageCount), "Page count must be a positive power of two.");

            Capacity = checked(pageSize * pageCount);
            capacityMask = Capacity - 1;
            buffer = new T[Capacity];
        }

        /// <summary>
        /// Gets the physical element mapped to a logical position.
        /// </summary>
        /// <param name="address">Monotonically increasing logical position.</param>
        internal ref T this[long address] => ref buffer[(int)(address & capacityMask)];
    }
}