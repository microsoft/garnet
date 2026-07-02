// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using Tsavorite.core;

namespace Garnet.server.Objects.Stream.BTree
{
    class BlockAllocator : IDisposable
    {
        private readonly IDevice m_device;
        private bool disposedValue;
        private long m_freeListStackHead;


        public BlockAllocator(IDevice device)
        {
            m_device = device;
        }
        


        public IntPtr AllocateBlock()
        {

        }

        public void FreeBlock()
        {

        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                    m_mm.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~NodeAllocator()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
