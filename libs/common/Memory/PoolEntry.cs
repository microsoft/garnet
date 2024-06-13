// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Garnet.common
{
    /// <summary>
    /// Pool entry
    /// </summary>
    public unsafe class PoolEntry : IDisposable
    {
        /// <summary>
        /// Entry
        /// </summary>
        public readonly byte[] entry;

        /// <summary>
        /// Entry pointer
        /// </summary>
        public byte* entryPtr;

        GCHandle handle;
        readonly LimitedFixedBufferPool pool;
        bool disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        public PoolEntry(int size, LimitedFixedBufferPool pool)
        {
            Debug.Assert(pool != null);
            this.pool = pool;
            this.disposed = false;
            entry = GC.AllocateArray<byte>(size, pinned: true);
            entryPtr = (byte*)Unsafe.AsPointer(ref entry[0]);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Debug.Assert(!disposed);
            disposed = true;
            pool.Return(this);
        }

        /// <summary>
        /// Reuse
        /// </summary>
        public void Reuse()
        {
            Debug.Assert(disposed);
            disposed = false;
        }
    }
}