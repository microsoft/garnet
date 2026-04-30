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
    /// Implementations define how data from an <see cref="ISnapshotDataSource"/> is shipped to a replica.
    /// </summary>
    internal interface ISnapshotTransmitSource
    {
        /// <summary>
        /// Transmits data from the given data source to the replica via the provided client session.
        /// </summary>
        /// <param name="gcs">The client session connected to the replica.</param>
        /// <param name="dataSource">The data source to read from.</param>
        /// <param name="timeout">Timeout for network operations.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task TransmitAsync(GarnetClientSession gcs, ISnapshotDataSource dataSource, TimeSpan timeout, CancellationToken cancellationToken = default);
    }
}