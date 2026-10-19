// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal static class ClusterUtils
    {
        public static byte[] ReadDevice(IDevice device, SectorAlignedBufferPool pool, ILogger logger = null)
        {
            ReadInto(device, pool, 0, out byte[] writePad, sizeof(int), logger);
            int size = BitConverter.ToInt32(writePad, 0);
            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, pool, 0, out body, size + sizeof(int), logger);
            return new Span<byte>(body)[sizeof(int)..].ToArray();
        }

        /// <summary>
        /// Copies <paramref name="buffer"/>, after its length, into a buffer rented from <paramref name="pool"/>,
        /// zero-padded to a whole number of sectors.
        /// </summary>
        internal static unsafe SectorAlignedMemory PrepareWrite(IDevice device, SectorAlignedBufferPool pool, byte[] buffer, out uint numBytesToWrite)
        {
            var size = buffer.Length;
            var _buffer = new byte[size + sizeof(int)];
            var len = BitConverter.GetBytes(size);
            Array.Copy(len, _buffer, len.Length);
            Array.Copy(buffer, 0, _buffer, len.Length, size);
            size += len.Length;

            long alignedSize = size;
            alignedSize = ((alignedSize + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = pool.Get((int)alignedSize);
            fixed (byte* bufferRaw = _buffer)
            {
                Buffer.MemoryCopy(bufferRaw, pbuffer.aligned_pointer, size, size);
            }
            numBytesToWrite = (uint)alignedSize;
            return pbuffer;
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
        private static unsafe void ReadInto(IDevice device, SectorAlignedBufferPool pool, ulong address, out byte[] buffer, int size, ILogger logger = null)
        {
            using var semaphore = new SemaphoreSlim(0);
            long numBytesToRead = size;
            numBytesToRead = ((numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = pool.Get((int)numBytesToRead);

            try
            {
                device.ReadAsync(address, (IntPtr)pbuffer.aligned_pointer,
                    (uint)numBytesToRead, logger == null ? IOCallback : logger.IOCallback, semaphore);
                semaphore.Wait();

                buffer = new byte[numBytesToRead];
                fixed (byte* bufferRaw = buffer)
                    Buffer.MemoryCopy(pbuffer.aligned_pointer, bufferRaw, numBytesToRead, numBytesToRead);
            }
            finally
            {
                pbuffer.Return();
            }
        }

        private static void IOCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            ((SemaphoreSlim)context).Release();
        }
    }

    static class LoggerExtensions
    {
        public static void IOCallback(this ILogger logger, uint errorCode, uint numBytes, object context, Exception ioException)
        {
            logger.LogIOError(errorCode, numBytes, context);
            ((SemaphoreSlim)context).Release();
        }

        internal static void LogIOError(this ILogger logger, uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                var errorMessage = Utility.GetCallbackErrorMessage(errorCode, numBytes, context);
                logger?.LogError("[ClusterUtils] OverlappedStream GetQueuedCompletionStatus error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
            }
        }

        [DllImport("libc")]
        private static extern IntPtr strerror(int errnum);
    }
}