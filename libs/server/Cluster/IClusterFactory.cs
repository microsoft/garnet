// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Cluster factory
    /// </summary>
    public interface IClusterFactory
    {
        /// <summary>
        /// Create checkpoint manager
        /// </summary>
        DeviceLogCommitCheckpointManager CreateCheckpointManager(int aofSublogCount, INamedDeviceFactoryCreator deviceFactoryCreator, ICheckpointNamingScheme checkpointNamingScheme, bool isMainStore, ILogger logger = default);

        /// <summary>
        /// Create cluster provider
        /// </summary>
        IClusterProvider CreateClusterProvider(StoreWrapper store);
    }
}