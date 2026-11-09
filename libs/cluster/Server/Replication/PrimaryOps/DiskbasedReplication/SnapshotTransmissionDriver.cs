// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        readonly ReplicaBenchmarkTiming benchmarkTiming;

        /// <summary>Creates a driver for one shipment, borrowing the connection and optional manager recorder.</summary>
        /// <param name="gcs">Replica connection used by each source; ownership remains with the sync session.</param>
        /// <param name="timeout">Timeout passed to individual source transmissions.</param>
        /// <param name="logger">Optional shipment/error logger.</param>
        /// <param name="benchmarkTiming">Startup-enabled manager recorder, or null to skip clocks and accounting.</param>
        public SnapshotTransmissionDriver(GarnetClientSession gcs, TimeSpan timeout, ILogger logger = null, ReplicaBenchmarkTiming benchmarkTiming = null)
        {
            this.gcs = gcs;
            this.timeout = timeout;
            this.logger = logger;
            this.benchmarkTiming = benchmarkTiming;
        }

        /// <summary>
        /// Adds a checkpoint reader whose transmit sources will be sent during <see cref="SendCheckpointAsync"/>.
        /// The driver takes ownership and will dispose the reader in <see cref="Dispose"/>.
        /// </summary>
        /// <param name="reader">Reader providing owned transmit sources for the current checkpoint.</param>
        public void AddReader(ISnapshotReader reader) => checkpointReaders.Add(reader);

        /// <summary>Releases owned readers while logging disposal failures; the borrowed connection stays open.</summary>
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
        /// <param name="cancellationToken">Cancellation spanning every file and EOF completion in this shipment.</param>
        public async Task SendCheckpointAsync(CancellationToken cancellationToken = default)
        {
            // Disabled timing takes no timestamps; enabled timing spans all readers and file completions.
            var started = benchmarkTiming == null ? 0 : Stopwatch.GetTimestamp();
            long bytes = 0;
            foreach (var checkpointReader in checkpointReaders)
            {
                foreach (var transmitSource in checkpointReader.GetTransmitSources())
                {
                    try
                    {
                        logger?.LogInformation("<Begin sending checkpoint data {token} {type} {startAddress} {endAddress}",
                            transmitSource.DataSource.Token, transmitSource.DataSource.Type, transmitSource.DataSource.StartOffset, transmitSource.DataSource.EndOffset);

                        await transmitSource.TransmitAsync(gcs, timeout, cancellationToken).ConfigureAwait(false);

                        // Account only at completed-file boundaries, using actual offsets that include padding.
                        // TransmitAsync includes the receiver's EOF barrier; chunk acceptance alone is not completion.
                        if (benchmarkTiming != null)
                            bytes = checked(bytes + transmitSource.DataSource.CurrentOffset - transmitSource.DataSource.StartOffset);

                        logger?.LogInformation("<Complete sending checkpoint data {token} {type} {startAddress} {endAddress}",
                            transmitSource.DataSource.Token, transmitSource.DataSource.Type, transmitSource.DataSource.StartOffset, transmitSource.DataSource.EndOffset);
                    }
                    finally
                    {
                        transmitSource.Dispose();
                    }
                }
            }

            // A failed/cancelled shipment never overwrites the recorder's last successful sample.
            benchmarkTiming?.RecordShipment(started, bytes);
        }
    }
}