// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Tsavorite.core;

namespace Device.benchmark
{
    internal static class DeviceUtils
    {
        static readonly SectorAlignedBufferPool pool = new SectorAlignedBufferPool(1, 512);

        /// <summary>
        /// Note: pads the bytes with zeros to achieve sector alignment
        /// </summary>
        /// <param name="device"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        public static unsafe void WriteInto(IDevice device, ulong address, byte[] buffer, int size = 0)
        {
            if (size == 0) size = buffer.Length;

            long numBytesToWrite = size;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = pool.Get((int)numBytesToWrite);
            fixed (byte* bufferRaw = buffer)
            {
                Buffer.MemoryCopy(bufferRaw, pbuffer.aligned_pointer, size, size);
            }
            using var semaphore = new SemaphoreSlim(0);

            try
            {
                device.WriteAsync((IntPtr)pbuffer.aligned_pointer, address, (uint)numBytesToWrite, IOCallback, semaphore);
                semaphore.Wait();
            }
            finally
            {
                pbuffer.Return();
            }
        }

        /// <summary>
        /// Note: will read potentially more data (based on sector alignment)
        /// </summary>
        /// <param name="device"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        public static unsafe void ReadInto(IDevice device, ulong address, int size, out byte[] buffer)
        {
            long numBytesToRead = size;
            numBytesToRead = ((numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = pool.Get((int)numBytesToRead);

            using var semaphore = new SemaphoreSlim(0);

            try
            {
                device.ReadAsync(address, (IntPtr)pbuffer.aligned_pointer, (uint)numBytesToRead, IOCallback, semaphore);
                semaphore.Wait();

                buffer = new byte[size];
                fixed (byte* bufferRaw = buffer)
                    Buffer.MemoryCopy(pbuffer.aligned_pointer, bufferRaw, size, size);
            }
            finally
            {
                pbuffer.Return();
            }
        }

        private static void IOCallback(uint errorCode, uint numBytes, object context)
        {
            ((SemaphoreSlim)context).Release();
        }
    }
}