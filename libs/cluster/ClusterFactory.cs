﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    /// <summary>
    /// Cluster factory
    /// </summary>
    public class ClusterFactory : IClusterFactory
    {
        /// <inheritdoc />
        public DeviceLogCommitCheckpointManager CreateCheckpointManager(INamedDeviceFactoryCreator deviceFactoryCreator, ICheckpointNamingScheme checkpointNamingScheme, bool isMainStore, ILogger logger = default)
            => new ReplicationLogCheckpointManager(deviceFactoryCreator, checkpointNamingScheme, isMainStore, logger: logger);

        /// <inheritdoc />
        public IClusterProvider CreateClusterProvider(StoreWrapper store)
            => new ClusterProvider(store);
    }
}