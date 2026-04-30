// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace Garnet.cluster
{
    /// <summary>
    /// Interface for a checkpoint reader that provides an enumeration of data source pairs.
    /// </summary>
    internal interface ISnapshotReader
    {
        /// <summary>
        /// Returns an enumeration of (data source, transmitter) pairs with initialized data sources.
        /// The caller is responsible for disposing each data source after use.
        /// </summary>
        IEnumerable<(ISnapshotDataSource dataSource, ISnapshotTransmitSource transmitter)> GetDataSources();
    }
}