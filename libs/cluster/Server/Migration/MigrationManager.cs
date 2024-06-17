// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Garnet.server;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal sealed class MigrationManager
    {
        readonly ClusterProvider clusterProvider;
        readonly MigrateSessionTaskStore migrationTaskStore;

        public MigrationManager(ClusterProvider clusterProvider, ILogger logger = null)
        {
            migrationTaskStore = new MigrateSessionTaskStore(logger);
            this.clusterProvider = clusterProvider;
        }

        public void Dispose()
        {
            migrationTaskStore.Dispose();
        }

        public int GetMigrationTaskCount()
            => migrationTaskStore.GetNumSession();

        /// <summary>
        ///  Add a new migration task in response to an associated request.
        /// </summary>
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
            Dictionary<ArgSlice, KeyMigrateState> keys,
            TransferOption transferOption,
            out MigrateSession mSession) => migrationTaskStore.TryAddMigrateSession(
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
    }
}