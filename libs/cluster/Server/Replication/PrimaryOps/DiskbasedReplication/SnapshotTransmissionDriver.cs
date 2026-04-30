// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    /// <summary>
    /// Drives checkpoint transmission by iterating over checkpoint readers and sending segments.
    /// </summary>
    internal sealed class SnapshotTransmissionDriver : IDisposable
    {
        readonly ISnapshotReader[] checkpointReaders;
        readonly GarnetClientSession gcs;
        readonly TimeSpan timeout;
        readonly ILogger logger;

        public SnapshotTransmissionDriver(ISnapshotReader[] checkpointReaders, GarnetClientSession gcs, TimeSpan timeout, ILogger logger = null)
        {
            this.checkpointReaders = checkpointReaders;
            this.gcs = gcs;
            this.timeout = timeout;
            this.logger = logger;
        }

        public void Dispose()
        {
        }

        /// <summary>
        /// Sends all checkpoint data by iterating transmit sources from each reader.
        /// For each source, delegates transmission to the <see cref="ISnapshotTransmitSource"/>.
        /// </summary>
        public async Task SendCheckpointAsync(CancellationToken cancellationToken = default)
        {
            foreach (var checkpointReader in checkpointReaders)
            {
                foreach (var transmitSource in checkpointReader.GetTransmitSources())
                {
                    try
                    {
                        logger?.LogInformation("<Begin sending checkpoint data {token} {type} {startAddress} {endAddress}",
                            transmitSource.DataSource.Token, transmitSource.DataSource.Type, transmitSource.DataSource.StartOffset, transmitSource.DataSource.EndOffset);

                        await transmitSource.TransmitAsync(gcs, timeout, cancellationToken).ConfigureAwait(false);

                        logger?.LogInformation("<Complete sending checkpoint data {token} {type} {startAddress} {endAddress}",
                            transmitSource.DataSource.Token, transmitSource.DataSource.Type, transmitSource.DataSource.StartOffset, transmitSource.DataSource.EndOffset);
                    }
                    finally
                    {
                        transmitSource.Dispose();
                    }
                }
            }
        }
    }
}