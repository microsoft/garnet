// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;

namespace Tsavorite.core
{
    /// <summary>
    /// Lightweight <see cref="IMemoryOwner{T}"/> wrapper around an externally-owned <see cref="System.Memory{T}"/>.
    /// </summary>
    /// <remarks>
    /// Use this when you need to expose a region of memory that lives elsewhere (e.g. inside an
    /// <see cref="OverflowByteArray"/> or another long-lived allocation) through APIs that require an
    /// <see cref="IMemoryOwner{T}"/> (e.g. <see cref="SpanByteAndMemory.Memory"/>) without copying.
    /// <see cref="Dispose"/> is a no-op because this type does not own the underlying allocation.
    /// </remarks>
    public sealed class BorrowedMemoryOwner : IMemoryOwner<byte>
    {
        /// <inheritdoc/>
        public Memory<byte> Memory { get; }

        public BorrowedMemoryOwner(Memory<byte> memory)
        {
            Memory = memory;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            // No-op: the underlying memory is owned by the producer (e.g. Tsavorite log/overflow allocator).
        }
    }
}