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
    /// Transmits checkpoint metadata over the network.
    /// Reads the metadata bytes from the owned data source and sends them via
    /// <see cref="GarnetClientSession.ExecuteClusterSendCheckpointMetadata"/>.
    /// Includes retry logic for transient failures.
    /// </summary>
    internal sealed class TsavoriteMetadataTransmitSource : ISnapshotTransmitSource
    {
        const int MaxRetryCount = 10;
        readonly ILogger logger;

        public ISnapshotDataSource DataSource { get; }

        public TsavoriteMetadataTransmitSource(ISnapshotDataSource dataSource, ILogger logger = null)
        {
            DataSource = dataSource;
            this.logger = logger;
        }

        /// <inheritdoc/>
        public async Task TransmitAsync(GarnetClientSession gcs, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            var retryCount = MaxRetryCount;

            while (true)
            {
                try
                {
                    logger?.LogInformation("<Begin sending checkpoint metadata {token} {type}", DataSource.Token, DataSource.Type);

                    byte[] checkpointMetadata = [];
                    if (DataSource.HasNextChunk)
                    {
                        var result = await DataSource.ReadNextChunkAsync(cancellationToken).ConfigureAwait(false);
                        checkpointMetadata = result.Data;
                    }

                    // A startAddress of -1 signals the receiver that this is a single-message payload
                    // (the entire content fits in one message, no streaming or end-of-stream marker needed).
                    var resp = await gcs.ExecuteClusterSnapshotData(
                        DataSource.Token.ToByteArray(), (int)DataSource.Type, -1, checkpointMetadata)
                        .WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

                    if (!resp.Equals("OK"))
                    {
                        logger?.LogError("Primary error at SendCheckpointMetadata {resp}", resp);
                        throw new GarnetException($"Primary error at SendCheckpointMetadata {resp}");
                    }

                    logger?.LogInformation("<Complete sending checkpoint metadata {token} {type}", DataSource.Token, DataSource.Type);
                    break;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger?.LogError("SendCheckpointMetadata Error: {msg}", ex.Message);
                    if (retryCount-- <= 0)
                        throw new GarnetException("Max retry attempts reached for checkpoint metadata sending.");
                }
                await Task.Yield();
            }
        }

        public void Dispose()
        {
            DataSource?.Dispose();
        }
    }
}