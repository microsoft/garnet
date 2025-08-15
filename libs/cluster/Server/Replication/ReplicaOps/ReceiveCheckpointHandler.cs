// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
#if DEBUG
using Garnet.common;
#endif
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe class ReceiveCheckpointHandler
    {
        readonly ClusterProvider clusterProvider;
        readonly CancellationTokenSource cts;
        IDevice writeIntoCkptDevice = null;
        private SemaphoreSlim writeCheckpointSemaphore = null;
        private SectorAlignedBufferPool writeCheckpointBufferPool = null;

        readonly ILogger logger;

        public ReceiveCheckpointHandler(ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.clusterProvider = clusterProvider;
            this.logger = logger;
            cts = new();
        }

        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
            writeCheckpointSemaphore?.Dispose();
            writeCheckpointBufferPool?.Free();
            writeCheckpointBufferPool = null;
            CloseDevice();
        }

        public void CloseDevice()
        {
            writeIntoCkptDevice?.Dispose();
            writeIntoCkptDevice = null;
        }

        /// <summary>
        /// Process file segments send from primary
        /// </summary>
        /// <param name="token"></param>
        /// <param name="type"></param>
        /// <param name="startAddress"></param>
        /// <param name="data"></param>
        /// <param name="segmentId"></param>
        public void ProcessFileSegments(int segmentId, Guid token, CheckpointFileType type, long startAddress, ReadOnlySpan<byte> data)
        {
            clusterProvider.replicationManager.UpdateLastPrimarySyncTime();
            if (writeIntoCkptDevice == null)
            {
                Debug.Assert(writeIntoCkptDevice == null);
                writeIntoCkptDevice = clusterProvider.replicationManager.GetInitializedSegmentFileDevice(token, type);
            }

            if (data.Length == 0)
            {
                Debug.Assert(writeIntoCkptDevice != null);
                CloseDevice();
                return;
            }

            Debug.Assert(writeIntoCkptDevice != null);
            WriteInto(writeIntoCkptDevice, (ulong)startAddress, data, data.Length, segmentId);

#if DEBUG
            ExceptionInjectionHelper.WaitOnClearAsync(ExceptionInjectionType.Replication_Timeout_On_Receive_Checkpoint).ConfigureAwait(false).GetAwaiter().GetResult();
#endif
        }

        /// <summary>
        /// Note: pads the bytes with zeros to achieve sector alignment
        /// </summary>
        /// <param name="device"></param>
        /// <param name="segmentId"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        private unsafe void WriteInto(IDevice device, ulong address, ReadOnlySpan<byte> buffer, int size, int segmentId = -1)
        {
            writeCheckpointBufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);

            long numBytesToWrite = size;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = writeCheckpointBufferPool.Get((int)numBytesToWrite);
            try
            {
                fixed (byte* bufferRaw = buffer)
                {
                    Buffer.MemoryCopy(bufferRaw, pbuffer.aligned_pointer, size, size);
                }

                writeCheckpointSemaphore ??= new(0);

                if (segmentId == -1)
                    device.WriteAsync((IntPtr)pbuffer.aligned_pointer, address, (uint)numBytesToWrite, IOCallback, null);
                else
                    device.WriteAsync((IntPtr)pbuffer.aligned_pointer, segmentId, address, (uint)numBytesToWrite, IOCallback, null);

                _ = writeCheckpointSemaphore.Wait(clusterProvider.serverOptions.ReplicaSyncTimeout, cts.Token);
            }
            finally
            {
                pbuffer.Return();
            }
        }

        private unsafe void IOCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                var errorMessage = Utility.GetCallbackErrorMessage(errorCode, numBytes, context);
                logger?.LogError("[ReceiveCheckpointHandler] OverlappedStream GetQueuedCompletionStatus error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
            }

            try
            {
                _ = writeCheckpointSemaphore.Release();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(ReceiveCheckpointHandler)}.IOCallback");
            }
        }

        [DllImport("libc")]
        private static extern IntPtr strerror(int errnum);
    }
}