// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed class MigrationManager
    {
        const int initialReceiveBufferSize = 1 << 12;
        readonly ILogger logger;
        readonly ClusterProvider clusterProvider;
        readonly MigrateSessionTaskStore migrationTaskStore;

        /// <summary>
        /// NetworkBufferSettings for MigrateSession instances
        /// </summary>
        readonly NetworkBufferSettings networkBufferSettings;

        /// <summary>
        /// NetworkPool instance created according to spec
        /// </summary>
        readonly LimitedFixedBufferPool networkPool;

        /// <summary>
        /// Get NetworkBuffers object
        /// </summary>
        public NetworkBufferSettings GetNetworkBufferSettings => networkBufferSettings;

        /// <summary>
        /// Get NetworkPool instance
        /// </summary>
        public LimitedFixedBufferPool GetNetworkPool => networkPool;

        public MigrationManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.logger = logger;
            this.migrationTaskStore = new MigrateSessionTaskStore(logger);
            this.clusterProvider = clusterProvider;
            var sendBufferSize = 1 << clusterProvider.serverOptions.PageSizeBits();
            this.networkBufferSettings = new NetworkBufferSettings(sendBufferSize, initialReceiveBufferSize);
            this.networkPool = networkBufferSettings.CreateBufferPool(logger: logger);

            logger?.LogInformation("NetworkBufferSettings.sendBufferSize:{sendBufferSize}", networkBufferSettings.sendBufferSize);
            logger?.LogInformation("NetworkBufferSettings.initialReceiveBufferSize:{initialReceiveBufferSize}", networkBufferSettings.initialReceiveBufferSize);
            logger?.LogInformation("NetworkBufferSettings.maxReceiveBufferSize:{maxReceiveBufferSize}", networkBufferSettings.maxReceiveBufferSize);
            logger?.LogInformation("ParallelMigrateTasks:{ParallelMigrateTasks}", clusterProvider.serverOptions.ParallelMigrateTaskCount);
            logger?.LogInformation("FastMigrate:{FastMigrate}", clusterProvider.serverOptions.FastMigrate ? "Enabled" : "Disabled");
            logger?.LogInformation("ServerOptions.LoggingFrequency:{LoggingFrequency}", clusterProvider.serverOptions.LoggingFrequency);
            logger?.LogInformation("StoreWrapper.LoggingFrequency:{LoggingFrequency}", clusterProvider.storeWrapper.loggingFrequency);
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            migrationTaskStore?.Dispose();
            networkPool?.Dispose();
        }

        /// <summary>
        /// Used to free up buffer pool
        /// </summary>
        public void Purge() => networkPool.Purge();

        public string GetBufferPoolStats() => networkPool.GetStats();

        /// <summary>
        /// Get number of active migrate sessions
        /// </summary>
        /// <returns></returns>
        public int GetMigrationTaskCount()
            => migrationTaskStore.GetNumSessions();

        /// <summary>
        ///  Add a new migration task in response to an associated request.
        /// </summary>
        /// <param name="clusterSession"></param>
        /// <param name="sourceNodeId"></param>
        /// <param name="targetAddress"></param>
        /// <param name="targetPort"></param>
        /// <param name="targetNodeId"></param>
        /// <param name="username"></param>
        /// <param name="passwd"></param>
        /// <param name="copyOption"></param>
        /// <param name="replaceOption"></param>
        /// <param name="timeout"></param>
        /// <param name="slots"></param>
        /// <param name="sketch"></param>
        /// <param name="transferOption"></param>
        /// <param name="mSession"></param>
        /// <returns></returns>
        public bool TryAddMigrationTask(
            ClusterSession clusterSession,
            string sourceNodeId,
            string targetAddress,
            int targetPort,
            string targetNodeId,
            string username,
            string passwd,
            bool copyOption,
            bool replaceOption,
            int timeout,
            HashSet<int> slots,
            Sketch sketch,
            TransferOption transferOption,
            out MigrateSession mSession) => migrationTaskStore.TryAddMigrateSession(
                clusterSession,
                clusterProvider,
                sourceNodeId,
                targetAddress,
                targetPort,
                targetNodeId,
                username,
                passwd,
                copyOption,
                replaceOption,
                timeout,
                slots,
                sketch,
                transferOption,
                out mSession);

        /// <summary>
        /// Remove provided migration task
        /// </summary>
        /// <param name="mSession"></param>
        /// <returns></returns>
        public bool TryRemoveMigrationTask(MigrateSession mSession)
            => migrationTaskStore.TryRemove(mSession);

        /// <summary>
        /// Remove migration task associated with provided target nodeId 
        /// </summary>
        /// <param name="targetNodeId"></param>
        /// <returns></returns>
        public bool TryRemoveMigrationTask(string targetNodeId)
            => migrationTaskStore.TryRemove(targetNodeId);

        /// <summary>
        /// Check if provided key can be operated on.
        /// </summary>
        /// <param name="slot"></param>
        /// <param name="key"></param>
        /// <param name="readOnly"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CanAccessKey(PinnedSpanByte key, int slot, bool readOnly)
            => migrationTaskStore.CanAccessKey(key, slot, readOnly);
    }
}