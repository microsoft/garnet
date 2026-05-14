// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
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
        readonly List<ISnapshotReader> checkpointReaders = [];
        readonly GarnetClientSession gcs;
        readonly TimeSpan timeout;
        readonly ILogger logger;

        public SnapshotTransmissionDriver(GarnetClientSession gcs, TimeSpan timeout, ILogger logger = null)
        {
            this.gcs = gcs;
            this.timeout = timeout;
            this.logger = logger;
        }

        /// <summary>
        /// Adds a checkpoint reader whose transmit sources will be sent during <see cref="SendCheckpointAsync"/>.
        /// The driver takes ownership and will dispose the reader in <see cref="Dispose"/>.
        /// </summary>
        public void AddReader(ISnapshotReader reader) => checkpointReaders.Add(reader);

        public void Dispose()
        {
            foreach (var reader in checkpointReaders)
            {
                try { reader.Dispose(); }
                catch (Exception ex) { logger?.LogError(ex, "Error disposing checkpoint reader"); }
            }
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