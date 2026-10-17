// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
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
        /// Note: pads the bytes with zeros to achieve sector alignment
        /// </summary>
        /// <param name="device"></param>
        /// <param name="pool"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        /// <param name="logger"></param>
        public static unsafe void WriteInto(IDevice device, SectorAlignedBufferPool pool, ulong address, byte[] buffer, int size = 0, ILogger logger = null)
        {
            var pbuffer = PrepareWrite(device, pool, buffer, size, out var numBytesToWrite);
            using var semaphore = new SemaphoreSlim(0);
            try
            {
                device.WriteAsync((IntPtr)pbuffer.aligned_pointer, address, numBytesToWrite, logger == null ? IOCallback : logger.IOCallback, semaphore);
                semaphore.Wait();
            }
            finally
            {
                pbuffer.Return();
            }
        }

        /// <summary>
        /// Writes <paramref name="buffer"/> as <see cref="WriteInto"/> does, but completes when the device does rather
        /// than holding the calling thread until then.
        /// </summary>
        /// <param name="device">The device to write to.</param>
        /// <param name="pool">The pool to rent the sector-aligned write buffer from.</param>
        /// <param name="address">The device address to write at.</param>
        /// <param name="buffer">The bytes to write, after their length.</param>
        /// <param name="logger">Logs a failed write.</param>
        public static async ValueTask WriteIntoAsync(IDevice device, SectorAlignedBufferPool pool, ulong address, byte[] buffer, ILogger logger = null)
        {
            var pbuffer = PrepareWrite(device, pool, buffer, 0, out var numBytesToWrite);
            try
            {
                var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                IssueWrite(device, pbuffer, address, numBytesToWrite, logger == null ? AsyncIOCallback : logger.AsyncIOCallback, completion);
                await completion.Task.ConfigureAwait(false);
            }
            finally
            {
                pbuffer.Return();
            }
        }

        /// <summary>
        /// Copies the first <paramref name="size"/> bytes of <paramref name="buffer"/> (all of them if 0), after their
        /// length, into a buffer rented from <paramref name="pool"/>, zero-padded to a whole number of sectors.
        /// </summary>
        static unsafe SectorAlignedMemory PrepareWrite(IDevice device, SectorAlignedBufferPool pool, byte[] buffer, int size, out uint numBytesToWrite)
        {
            if (size == 0) size = buffer.Length;
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

        static unsafe void IssueWrite(IDevice device, SectorAlignedMemory pbuffer, ulong address, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
            => device.WriteAsync((IntPtr)pbuffer.aligned_pointer, address, numBytesToWrite, callback, context);

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

        private static void AsyncIOCallback(uint errorCode, uint numBytes, object context, Exception ioException)
            => ((TaskCompletionSource)context).SetResult();
    }

    static class LoggerExtensions
    {
        public static void IOCallback(this ILogger logger, uint errorCode, uint numBytes, object context, Exception ioException)
        {
            logger.LogIOError(errorCode, numBytes, context);
            ((SemaphoreSlim)context).Release();
        }

        public static void AsyncIOCallback(this ILogger logger, uint errorCode, uint numBytes, object context, Exception ioException)
        {
            logger.LogIOError(errorCode, numBytes, context);
            ((TaskCompletionSource)context).SetResult();
        }

        static void LogIOError(this ILogger logger, uint errorCode, uint numBytes, object context)
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