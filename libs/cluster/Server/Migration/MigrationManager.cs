// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Garnet.cluster
{
    internal class MigrationManager
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

        public bool TryAddMigrationTask(string sourceNodeId, string targetAddress, int targetPort, string targetNodeId, string username, string passwd, bool copyOption, bool replaceOption, int timeout, HashSet<int> slots, List<(long, long)> keysWithSize, out MigrateSession mSession)
            => migrationTaskStore.TryAddMigrateSession(clusterProvider, sourceNodeId, targetAddress, targetPort, targetNodeId, username, passwd, copyOption, replaceOption, timeout, slots, keysWithSize, out mSession);

        public bool TryRemoveMigrationTask(MigrateSession mSession)
            => migrationTaskStore.TryRemove(mSession);
    }
}