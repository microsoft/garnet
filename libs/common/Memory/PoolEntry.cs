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

        readonly bool useHandlesForPin;
        GCHandle handle;
        readonly LimitedFixedBufferPool pool;
        bool disposed;

        /// <summary>
        /// Constructor
        /// </summary>
        public PoolEntry(int size, LimitedFixedBufferPool pool, bool useHandlesForPin = false)
        {
            Debug.Assert(pool != null);
            this.pool = pool;
            this.useHandlesForPin = useHandlesForPin;
            this.disposed = false;
            if (useHandlesForPin)
            {
                entry = new byte[size];
                Pin();
            }
            else
            {
                entry = GC.AllocateArray<byte>(size, pinned: true);
                entryPtr = (byte*)Unsafe.AsPointer(ref entry[0]);
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Debug.Assert(!disposed);
            disposed = true;
            Unpin();
            pool.Return(this);
        }

        /// <summary>
        /// Reuse
        /// </summary>
        public void Reuse()
        {
            Debug.Assert(disposed);
            disposed = false;
            Pin();
        }

        /// <summary>
        /// Pin
        /// </summary>
        void Pin()
        {
            if (useHandlesForPin)
            {
                handle = GCHandle.Alloc(entry, GCHandleType.Pinned);
                entryPtr = (byte*)handle.AddrOfPinnedObject();
            }
        }

        /// <summary>
        /// Unpin
        /// </summary>
        void Unpin()
        {
            if (useHandlesForPin) handle.Free();
        }
    }
}