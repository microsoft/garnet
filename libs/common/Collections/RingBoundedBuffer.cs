// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Numerics;

namespace Garnet.common
{
    /// <summary>
    /// Fixed-capacity paged buffer addressed by monotonically increasing logical positions.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    internal sealed class RingBoundedBuffer<T>
    {
        readonly T[][] bufferPages;
        readonly int pageSizeBits;
        readonly int pageSizeMask;

        /// <summary>
        /// Number of logical positions retained by the ring.
        /// </summary>
        internal int Capacity { get; }

        /// <summary>
        /// Creates a fixed-capacity paged ring buffer.
        /// </summary>
        /// <param name="pageSize">Number of elements per page. Must be a power of two.</param>
        /// <param name="pageCount">Number of pages. Must be a power of two.</param>
        internal RingBoundedBuffer(int pageSize, int pageCount)
        {
            if (pageSize <= 0 || !BitOperations.IsPow2((uint)pageSize))
                throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be a positive power of two.");
            if (pageCount <= 0 || !BitOperations.IsPow2((uint)pageCount))
                throw new ArgumentOutOfRangeException(nameof(pageCount), "Page count must be a positive power of two.");

            pageSizeBits = BitOperations.Log2((uint)pageSize);
            pageSizeMask = pageSize - 1;
            Capacity = checked(pageSize * pageCount);

            bufferPages = new T[pageCount][];
            for (var i = 0; i < bufferPages.Length; i++)
                bufferPages[i] = new T[pageSize];
        }

        /// <summary>
        /// Gets the physical element mapped to a logical position.
        /// </summary>
        /// <param name="address">Monotonically increasing logical position.</param>
        internal ref T this[long address]
        {
            get
            {
                var pageIndex = (int)((address >> pageSizeBits) & (bufferPages.Length - 1));
                return ref bufferPages[pageIndex][(int)(address & pageSizeMask)];
            }
        }
    }
}