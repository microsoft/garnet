// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.IO;
using System.Linq;

namespace Garnet.server
{
    /// <summary>
    /// Factory class for creating new instances of IDatabaseManager
    /// </summary>
    public class DatabaseManagerFactory
    {
        /// <summary>
        /// Create a new instance of IDatabaseManager
        /// </summary>
        /// <param name="serverOptions">Garnet server options</param>
        /// <param name="createDatabaseDelegate">Delegate for creating a new logical database</param>
        /// <param name="storeWrapper">Store wrapper instance</param>
        /// <param name="createDefaultDatabase">True if database manager should create a default database instance (default: true)</param>
        /// <returns></returns>
        public static IDatabaseManager CreateDatabaseManager(GarnetServerOptions serverOptions,
            StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate, StoreWrapper storeWrapper, bool createDefaultDatabase = true)
        {
            return ShouldCreateMultipleDatabaseManager(serverOptions, createDatabaseDelegate) ?
                new MultiDatabaseManager(createDatabaseDelegate, storeWrapper, createDefaultDatabase) :
                new SingleDatabaseManager(createDatabaseDelegate, storeWrapper, createDefaultDatabase);
        }

        private static bool ShouldCreateMultipleDatabaseManager(GarnetServerOptions serverOptions,
            StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate)
        {
            // If multiple databases are not allowed or recovery is disabled, create a single database manager
            if (!serverOptions.AllowMultiDb || !serverOptions.Recover)
                return false;

            // If there are multiple databases to recover, create a multi database manager, otherwise create a single database manager.
            using (createDatabaseDelegate(0, out var checkpointDir, out var aofDir))
            {
                // Check if there are multiple databases to recover from checkpoint
                var checkpointDirInfo = new DirectoryInfo(checkpointDir);
                var checkpointDirBaseName = checkpointDirInfo.Name;
                var checkpointParentDir = checkpointDirInfo.Parent!.FullName;

                if (MultiDatabaseManager.TryGetSavedDatabaseIds(checkpointParentDir, checkpointDirBaseName,
                        out var dbIds) && dbIds.Any(id => id != 0))
                    return true;

                // Check if there are multiple databases to recover from AOF
                if (aofDir != null)
                {
                    var aofDirInfo = new DirectoryInfo(aofDir);
                    var aofDirBaseName = aofDirInfo.Name;
                    var aofParentDir = aofDirInfo.Parent!.FullName;

                    if (MultiDatabaseManager.TryGetSavedDatabaseIds(aofParentDir, aofDirBaseName,
                            out dbIds) && dbIds.Any(id => id != 0))
                        return true;
                }

                return false;
            }
        }
    }
}