// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class MigrationManager
    {
        readonly ILogger logger;
        readonly ClusterProvider clusterProvider;
        readonly MigrateSessionTaskStore migrationTaskStore;

        /// <summary>
        /// Used to initialize buffers for client connected to target node for active migrate sessions
        /// </summary>
        public NetworkBuffers networkBuffers;

        public MigrationManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            this.logger = logger;
            this.migrationTaskStore = new MigrateSessionTaskStore(logger);
            this.clusterProvider = clusterProvider;
            AllocatePool();
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            migrationTaskStore?.Dispose();
            networkBuffers.Dispose();
        }

        /// <summary>
        /// Allocate shared network buffer pool
        /// </summary>
        internal void AllocatePool()
        {
            var bufferSize = 1 << clusterProvider.serverOptions.PageSizeBits();
            this.networkBuffers = new NetworkBuffers(bufferSize, 1 << 12).Allocate(logger: logger);
        }

        /// <summary>
        /// Used to free up buffer pool
        /// </summary>
        public void Collect()
        {
            networkBuffers.Dispose();
            GC.Collect(GC.MaxGeneration);
            AllocatePool();
        }

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
        /// <param name="keys"></param>
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
            MigratingKeysWorkingSet keys,
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
                keys,
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
        public bool CanAccessKey(ref ArgSlice key, int slot, bool readOnly)
            => migrationTaskStore.CanAccessKey(ref key, slot, readOnly);
    }
}