// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers.Binary;
using System.ComponentModel;
using System.Threading;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal static class ClusterUtils
    {
        public static byte[] ReadDevice(IDevice device, SectorAlignedMemoryPool pool, ILogger logger = null)
        {
            ReadInto(device, pool, 0, out byte[] writePad, sizeof(int), logger);
            int size = BitConverter.ToInt32(writePad, 0);
            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, pool, 0, out body, size + sizeof(int), logger);
            return body.AsSpan(sizeof(int)).ToArray();
        }

        /// <summary>
        /// Note: pads the bytes with zeros to achieve sector alignment
        /// </summary>
        /// <param name="device"></param>
        /// <param name="pool"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        /// <param name="logger"></param>
        public static unsafe void WriteInto(IDevice device, SectorAlignedMemoryPool pool, ulong address, byte[] buffer, int size = 0, ILogger logger = null)
        {
            if (size == 0) size = buffer.Length;

            var lengthPrefixedSize = size + sizeof(int);
            var sectorAlignedBuffer = pool.Get(lengthPrefixedSize);
            var sectorAlignedBufferSpan = sectorAlignedBuffer.AsSpan();

            BinaryPrimitives.WriteInt32LittleEndian(sectorAlignedBufferSpan, size);
            buffer.AsSpan().CopyTo(sectorAlignedBufferSpan.Slice(sizeof(int)));

            using var semaphore = new SemaphoreSlim(0);
            device.WriteAsync((IntPtr)sectorAlignedBuffer.Pointer, address, (uint)sectorAlignedBuffer.Length, logger == null ? IOCallback : logger.IOCallback, semaphore);
            semaphore.Wait();

            sectorAlignedBuffer.Return();
        }


        /// <summary>
        /// Note: will read potentially more data (based on sector alignment)
        /// </summary>
        /// <param name="device"></param>
        /// <param name="pool"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        /// <param name="logger"></param>
        private static unsafe void ReadInto(IDevice device, SectorAlignedMemoryPool pool, ulong address, out byte[] buffer, int size, ILogger logger = null)
        {
            var sectorAlignedBuffer = pool.Get((int)size);

            using var semaphore = new SemaphoreSlim(0);
            device.ReadAsync(address, (IntPtr)sectorAlignedBuffer.Pointer, (uint)sectorAlignedBuffer.Length, logger == null ? IOCallback : logger.IOCallback, semaphore);
            semaphore.Wait();

            buffer = new byte[sectorAlignedBuffer.Length];
            sectorAlignedBuffer.AsSpan().CopyTo(buffer);
            sectorAlignedBuffer.Return();
        }

        private static void IOCallback(uint errorCode, uint numBytes, object context)
        {
            ((SemaphoreSlim)context).Release();
        }
    }

    static class LoggerExtensions
    {
        public static void IOCallback(this ILogger logger, uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                var errorMessage = new Win32Exception((int)errorCode).Message;
                logger.LogError("[ClusterUtils] OverlappedStream GetQueuedCompletionStatus error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
            }
            ((SemaphoreSlim)context).Release();
        }
    }
}