// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Garnet.common;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Transmits a RangeIndex BfTree snapshot file over the network.
    /// For each file, sends:
    /// 1. A header message (startAddress = -1) containing the key hash directory name
    /// 2. File data chunks via <see cref="FileTransmitSource"/> (sector-aligned, zero-copy)
    /// 3. An empty end-of-transmission packet (sent by <see cref="FileTransmitSource"/>)
    /// </summary>
    internal sealed class RangeIndexFileTransmitSource : ISnapshotTransmitSource
    {
        readonly FileTransmitSource fileTransmitSource;

        public ISnapshotDataSource DataSource => fileTransmitSource.DataSource;

        /// <summary>
        /// The key hash directory name identifying which tree this snapshot belongs to.
        /// </summary>
        public string KeyHashDir { get; }

        /// <summary>
        /// Creates a new RangeIndexFileTransmitSource.
        /// </summary>
        /// <param name="dataSource">The <see cref="FileDataSource"/> backed by an <see cref="Tsavorite.core.IDevice"/> for the BfTree file.</param>
        /// <param name="keyHashDir">The key hash directory name (32-char hex) identifying which tree this snapshot belongs to.</param>
        /// <param name="logger">Optional logger.</param>
        public RangeIndexFileTransmitSource(FileDataSource dataSource, string keyHashDir, ILogger logger = null)
        {
            this.fileTransmitSource = new FileTransmitSource(dataSource, logger);
            KeyHashDir = keyHashDir;
        }

        /// <inheritdoc/>
        public async Task TransmitAsync(GarnetClientSession gcs, TimeSpan timeout, CancellationToken cancellationToken = default)
        {
            var fileTokenBytes = DataSource.Token.ToByteArray();

            // Send header with key hash directory name so the replica knows which tree this file belongs to.
            // Uses startAddress = -1 to indicate a single-message control payload.
            var keyHashDirBytes = Encoding.ASCII.GetBytes(KeyHashDir);
            var headerResp = await gcs.ExecuteClusterSnapshotData(
                fileTokenBytes, (int)DataSource.Type, -1, keyHashDirBytes)
                .WaitAsync(timeout, cancellationToken).ConfigureAwait(false);

            if (!headerResp.Equals("OK"))
                ExceptionUtils.ThrowException(new GarnetException(
                    $"Primary error at SendRangeIndexHeader {DataSource.Type} {headerResp}"));

            // Delegate chunk sending + EOT to FileTransmitSource (uses IDevice + SectorAlignedMemory)
            await fileTransmitSource.TransmitAsync(gcs, timeout, cancellationToken).ConfigureAwait(false);
        }

        public void Dispose()
        {
            fileTransmitSource?.Dispose();
        }
    }
}