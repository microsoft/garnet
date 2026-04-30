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
    /// Transmits checkpoint file segments over the network using chunked reads from an <see cref="ISnapshotDataSource"/>.
    /// Sends each chunk via <see cref="GarnetClientSession.ExecuteClusterSendCheckpointFileSegment"/>
    /// followed by an empty end-of-transmission packet.
    /// </summary>
    internal sealed class FileTransmitSource : ISnapshotTransmitSource
    {
        readonly ILogger logger;

        public FileTransmitSource(ILogger logger = null)
        {
            this.logger = logger;
        }

        /// <inheritdoc/>
        public async Task TransmitAsync(GarnetClientSession gcs, ISnapshotDataSource dataSource, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            var fileTokenBytes = dataSource.Token.ToByteArray();

            while (dataSource.HasNextChunk)
            {
                var result = await dataSource.ReadNextChunkAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    var resp = await gcs.ExecuteClusterSendCheckpointFileSegment(
                        fileTokenBytes,
                        (int)dataSource.Type,
                        startAddress: result.ChunkStartAddress,
                        result.Buffer.GetSlice(result.BytesRead)).WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

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
                .WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

            if (!endResp.Equals("OK"))
                ExceptionUtils.ThrowException(new GarnetException(
                    $"Primary error at SendFileSegments Completion {dataSource.Type} {endResp}"));
        }
    }
}