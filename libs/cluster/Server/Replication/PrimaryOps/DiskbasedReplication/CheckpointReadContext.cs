// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Context for sending checkpoint segments over the network using GarnetClientSession.
    /// This class handles only the network transmission logic — reading is delegated to ICheckpointDataSource.
    /// </summary>
    internal sealed class CheckpointReadContext : IDisposable
    {
        readonly GarnetClientSession gcs;
        readonly TimeSpan timeout;
        readonly CancellationTokenSource cts;
        readonly ILogger logger;

        public CheckpointReadContext(GarnetClientSession gcs, TimeSpan timeout, ILogger logger = null)
        {
            this.gcs = gcs;
            this.timeout = timeout;
            this.logger = logger;
            cts = new CancellationTokenSource();
        }

        public void Dispose()
        {
            cts.Cancel();
            cts.Dispose();
        }

        /// <summary>
        /// Sends all segments from the given data source over the network.
        /// </summary>
        public async Task SendSegmentsAsync(ICheckpointDataSource dataSource, CancellationToken cancellationToken = default)
        {
            var fileTokenBytes = dataSource.Token.ToByteArray();
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken);

            while (dataSource.HasNextChunk)
            {
                var result = await dataSource.ReadNextChunkAsync(linkedCts.Token).ConfigureAwait(false);
                try
                {
                    var resp = await gcs.ExecuteClusterSendCheckpointFileSegment(
                        fileTokenBytes,
                        (int)dataSource.Type,
                        startAddress: result.ChunkStartAddress,
                        result.Buffer.GetSlice(result.BytesRead)).WaitAsync(timeout, linkedCts.Token).ConfigureAwait(false);

                    if (!resp.Equals("OK"))
                        ExceptionUtils.ThrowException(new GarnetException(
                            $"Primary error at SendFileSegments {dataSource.Type} {resp} [{dataSource.StartOffset},{dataSource.CurrentOffset},{dataSource.EndOffset}]"));
                }
                finally
                {
                    result.Buffer.Return();
                }
            }

            // Send empty package to indicate end of transmission
            var endResp = await gcs.ExecuteClusterSendCheckpointFileSegment(
                fileTokenBytes, (int)dataSource.Type, dataSource.CurrentOffset, [])
                .WaitAsync(timeout, linkedCts.Token).ConfigureAwait(false);

            if (!endResp.Equals("OK"))
                ExceptionUtils.ThrowException(new GarnetException(
                    $"Primary error at SendFileSegments Completion {dataSource.Type} {endResp}"));
        }
    }
}