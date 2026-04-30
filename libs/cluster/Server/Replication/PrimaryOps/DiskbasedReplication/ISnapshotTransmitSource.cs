// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.client;

namespace Garnet.cluster
{
    /// <summary>
    /// Interface for transmitting checkpoint data over the network.
    /// Implementations own an <see cref="ISnapshotDataSource"/> internally
    /// and define how to read and ship the data to a replica.
    /// </summary>
    internal interface ISnapshotTransmitSource : IDisposable
    {
        /// <summary>
        /// The underlying data source owned by this transmitter.
        /// </summary>
        ISnapshotDataSource DataSource { get; }

        /// <summary>
        /// Transmits data from the owned data source to the replica via the provided client session.
        /// </summary>
        /// <param name="gcs">The client session connected to the replica.</param>
        /// <param name="timeout">Timeout for network operations.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task TransmitAsync(GarnetClientSession gcs, TimeSpan timeout, CancellationToken cancellationToken = default);
    }
}