// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class CheckpointReadContext(GarnetClientSession gcs, TimeSpan timeout, ILogger logger)
    {
        const int batchSize = 1 << 17;
        readonly GarnetClientSession gcs = gcs;
        private SectorAlignedBufferPool bufferPool;
        private SectorAlignedMemory buffer;
        readonly SemaphoreSlim signalCompletion = new(0);
        readonly CancellationTokenSource cts = new();
        readonly TimeSpan timeout = timeout;
        readonly ILogger logger = logger;

        public void Dispose()
        {
            signalCompletion.Release();
            try
            {
                signalCompletion.Wait(timeout);
                cts.Cancel();
            }
            finally
            {
                signalCompletion.Dispose();
                cts.Dispose();
                bufferPool?.Free();
            }
        }

        public async Task SendSegments(CheckpointDataSource cds)
        {
            var fileTokenBytes = cds.token.ToByteArray();
            string resp;
            while (cds.HasNextChunk)
            {
                var readBytes = await GetNextChunk(cds).ConfigureAwait(false);

                resp = await gcs.ExecuteClusterSendCheckpointFileSegment(
                    fileTokenBytes,
                    (int)cds.type,
                    startAddress: cds.currOffset - readBytes,
                    buffer.GetSlice(readBytes)).WaitAsync(timeout, cts.Token).ConfigureAwait(false);

                if (!resp.Equals("OK"))
                    ExceptionUtils.ThrowException(new GarnetException($"Primary error at SendFileSegments {cds.type} {resp} [{cds.startOffset},{cds.currOffset},{cds.endOffset}]"));

                buffer.Return();
            }

            // Send last empty package to indicate end of transmission and let replica dispose IDevice
            resp = await gcs.ExecuteClusterSendCheckpointFileSegment(fileTokenBytes, (int)cds.type, cds.currOffset, []).
                WaitAsync(timeout, cts.Token).ConfigureAwait(false);

            if (!resp.Equals("OK"))
                ExceptionUtils.ThrowException(new GarnetException($"Primary error at SendFileSegments Completion {cds.type} {resp}"));
        }

        private async Task<int> GetNextChunk(CheckpointDataSource cds)
        {
            var readBytes = await ReadInto(cds).ConfigureAwait(false);
            cds.currOffset += readBytes;
            return readBytes;
        }

        /// <summary>
        /// Note: will read potentially more data (based on sector alignment)
        /// </summary>
        private async Task<int> ReadInto(CheckpointDataSource cds)
        {
            var device = cds.device;
            var address = (ulong)cds.currOffset;
            var size = (int)cds.GetNextChunkBytes(Math.Min(batchSize, TsavoriteCheckpointReader.GetBatchSize(cds.type, clusterProvider.serverOptions)));
            bufferPool ??= new SectorAlignedBufferPool(1, (int)device.SectorSize);

            long numBytesToRead = size;
            numBytesToRead = (numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1);

            buffer = bufferPool.Get((int)numBytesToRead);
            unsafe
            {
                device.ReadAsync(address, (IntPtr)buffer.aligned_pointer, (uint)numBytesToRead, IOCallback, null);
            }
            _ = await signalCompletion.WaitAsync(timeout, cts.Token).ConfigureAwait(false);
            return (int)numBytesToRead;

            void IOCallback(uint errorCode, uint numBytes, object context)
            {
                if (errorCode != 0)
                {
                    var errorMessage = Tsavorite.core.Utility.GetCallbackErrorMessage(errorCode, numBytes, context);
                    logger?.LogError("[ReplicaSyncSession] OverlappedStream GetQueuedCompletionStatus error: {errorCode} msg: {errorMessage}", errorCode, errorMessage);
                }
                _ = signalCompletion.Release();
            }
        }
    }
}