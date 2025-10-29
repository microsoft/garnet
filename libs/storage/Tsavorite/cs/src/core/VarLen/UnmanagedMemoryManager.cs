// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;

namespace Tsavorite.core
{
    /// <summary>
    /// A MemoryManager over a raw pointer
    /// </summary>
    /// <remarks>The pointer is assumed to be fully unmanaged, or externally pinned - no attempt will be made to pin this data</remarks>
    public sealed unsafe class UnmanagedMemoryManager<T> : MemoryManager<T>
        where T : unmanaged
    {
        private T* _pointer;
        private int _length;

        /// <summary>
        /// Create a new UnmanagedMemoryManager instance at the given pointer and size
        /// </summary>
        public UnmanagedMemoryManager(T* pointer, int length)
        {
            _pointer = pointer;
            _length = length;
        }

        public UnmanagedMemoryManager()
        {
            _pointer = null;
            _length = 0;
        }

        /// <summary>
        /// Set destination for this object
        /// </summary>
        /// <param name="pointer"></param>
        /// <param name="length"></param>
        public void SetDestination(T* pointer, int length)
        {
            _pointer = pointer;
            _length = length;
        }

        /// <summary>
        /// Obtains a span that represents the region
        /// </summary>
        public override Span<T> GetSpan() => new Span<T>(_pointer, _length);

        /// <summary>
        /// Provides access to a pointer that represents the data (note: no actual pin occurs)
        /// </summary>
        public override MemoryHandle Pin(int elementIndex = 0)
        {
            if (elementIndex < 0 || elementIndex >= _length)
                throw new ArgumentOutOfRangeException(nameof(elementIndex));
            return new MemoryHandle(_pointer + elementIndex);
        }
        /// <summary>
        /// Has no effect
        /// </summary>
        public override void Unpin() { }

        /// <summary>
        /// Releases all resources associated with this object
        /// </summary>
        protected override void Dispose(bool disposing) { }
    }
}