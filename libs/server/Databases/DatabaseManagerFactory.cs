// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using static Garnet.server.StoreWrapper;

namespace Garnet.server.Databases
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
            DatabaseCreatorDelegate createDatabaseDelegate, StoreWrapper storeWrapper, bool createDefaultDatabase = true)
        {
            return serverOptions.EnableCluster || serverOptions.MaxDatabases == 1
                ? new SingleDatabaseManager(createDatabaseDelegate, storeWrapper, createDefaultDatabase)
                : new MultiDatabaseManager(createDatabaseDelegate, storeWrapper, createDefaultDatabase);
        }
    }
}