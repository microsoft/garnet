// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;

namespace Garnet.common
{
    /// <summary>
    /// Wrapper struct for memory result from shared pool, of particular actual length
    /// </summary>
    /// <typeparam name="T">Type of memory elements</typeparam>
    public struct MemoryResult<T> : IDisposable
        where T : unmanaged
    {
        /// <summary>
        /// Create memory result
        /// </summary>
        public MemoryResult(IMemoryOwner<T> memoryOwner)
        {
            MemoryOwner = memoryOwner;
            Length = memoryOwner.Memory.Length;
        }

        /// <summary>
        /// Create memory result
        /// </summary>
        public MemoryResult(IMemoryOwner<T> memoryOwner, int actualLength)
        {
            MemoryOwner = memoryOwner;
            Length = actualLength;
        }

        /// <summary>
        /// Create memory result
        /// </summary>
        public static MemoryResult<T> Create(MemoryPool<T> pool, int actualLength)
            => new(pool.Rent(actualLength), actualLength);

        /// <summary>
        /// Memory owner
        /// </summary>
        public IMemoryOwner<T> MemoryOwner;

        /// <summary>
        /// Actual length of result
        /// </summary>
        public int Length;

        /// <summary>
        /// Get Span of actual length
        /// </summary>
        public Span<T> Span => MemoryOwner.Memory.Span[..Length];

        /// <summary>
        /// Get Memory of actual length
        /// </summary>
        public Memory<T> Memory => MemoryOwner.Memory[..Length];

        /// <inheritdoc />
        public void Dispose() => MemoryOwner?.Dispose();
    }
}