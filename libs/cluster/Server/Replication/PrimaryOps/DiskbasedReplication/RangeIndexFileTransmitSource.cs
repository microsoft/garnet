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
    /// Transmits a RangeIndex file over the network. Sends a metadata header (with
    /// <c>startAddress = -1</c>) containing the serialized key hash and address needed to
    /// reconstruct the target path, followed by chunked file content, followed by an empty
    /// end-of-transmission marker.
    ///
    /// <para>Metadata payload layout:</para>
    /// <list type="bullet">
    /// <item><b>STORE_RANGEINDEX_FLUSH</b>: keyHash (32 bytes ASCII) + address (8 bytes little-endian) = 40 bytes</item>
    /// <item><b>STORE_RANGEINDEX_SNAPSHOT</b>: keyHash (32 bytes ASCII) = 32 bytes</item>
    /// </list>
    /// </summary>
    internal sealed class RangeIndexFileTransmitSource : ISnapshotTransmitSource
    {
        private readonly ILogger logger;

        public ISnapshotDataSource DataSource { get; }

        public RangeIndexFileTransmitSource(ISnapshotDataSource dataSource, ILogger logger = null)
        {
            DataSource = dataSource;
            this.logger = logger;
        }

        /// <inheritdoc/>
        public async Task TransmitAsync(GarnetClientSession gcs, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            var riDataSource = (RangeIndexFileDataSource)DataSource;
            var fileTokenBytes = DataSource.Token.ToByteArray();

            // Get metadata from data source: keyHash + address (flush only)
            var metadata = riDataSource.GetMetadata();

            // Send header with startAddress = -1 to indicate single-message control payload.
            var headerResp = await gcs.ExecuteClusterSnapshotData(
                fileTokenBytes, (int)DataSource.Type, -1, metadata)
                .WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

            if (!headerResp.Equals("OK"))
                ExceptionUtils.ThrowException(new GarnetException(
                    $"Primary error at RangeIndex header {DataSource.Type} {headerResp} keyHash={riDataSource.KeyHash}"));

            // Stream file content in chunks
            while (DataSource.HasNextChunk)
            {
                var result = await DataSource.ReadNextChunkAsync(cancellationToken).ConfigureAwait(false);

                var resp = await gcs.ExecuteClusterSnapshotData(
                    fileTokenBytes,
                    (int)DataSource.Type,
                    startAddress: result.ChunkStartAddress,
                    new Span<byte>(result.Data, 0, result.BytesRead)).WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

                if (!resp.Equals("OK"))
                    ExceptionUtils.ThrowException(new GarnetException(
                        $"Primary error at RangeIndex TransmitAsync {DataSource.Type} {resp} [{DataSource.StartOffset},{DataSource.CurrentOffset},{DataSource.EndOffset}]"));
            }

            // Send empty package to indicate end of transmission
            var endResp = await gcs.ExecuteClusterSnapshotData(
                fileTokenBytes, (int)DataSource.Type, DataSource.CurrentOffset, [])
                .WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

            if (!endResp.Equals("OK"))
                ExceptionUtils.ThrowException(new GarnetException(
                    $"Primary error at RangeIndex TransmitAsync Completion {DataSource.Type} {endResp}"));
        }

        public void Dispose()
        {
            DataSource?.Dispose();
        }
    }
}