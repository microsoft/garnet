// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Linq;
using Microsoft.Extensions.Logging;

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
            var logger = storeWrapper.loggerFactory?.CreateLogger(nameof(DatabaseManagerFactory));

            return ShouldCreateMultipleDatabaseManager(serverOptions, createDatabaseDelegate, logger, out var storageSlots) ?
                new MultiDatabaseManager(createDatabaseDelegate, storeWrapper, createDefaultDatabase, storageSlots) :
                new SingleDatabaseManager(createDatabaseDelegate, storeWrapper, createDefaultDatabase);
        }

        /// <summary>
        /// Decide whether recovery needs a multi-database manager, and hand back the storage slots the
        /// decision was made from so that recovery does not scan the checkpoint directories a second time.
        /// </summary>
        /// <param name="serverOptions">Garnet server options</param>
        /// <param name="createDatabaseDelegate">Delegate for creating a new logical database</param>
        /// <param name="logger">Logger</param>
        /// <param name="storageSlots">Storage slots found under the checkpoint directory, or null if that directory does not exist</param>
        /// <returns>True if a multi-database manager is required</returns>
        private static bool ShouldCreateMultipleDatabaseManager(GarnetServerOptions serverOptions,
            StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate, ILogger logger, out int[] storageSlots)
        {
            storageSlots = null;

            // If multiple databases are not allowed or recovery is disabled, create a single database manager
            if (!serverOptions.AllowMultiDb || !serverOptions.Recover)
                return false;

            // If there are multiple databases to recover, create a multi database manager, otherwise create a single database manager.
            using (var db = createDatabaseDelegate(0))
            {
                // Check if there are multiple databases to recover from checkpoint. The directory names give
                // the storage slots, which match the logical database ids until a swap relabels a database.
                var checkpointParentDir = serverOptions.StoreCheckpointBaseDirectory;
                var checkpointDirBaseName = GarnetServerOptions.GetCheckpointDirectoryName(0);

                if (MultiDatabaseManager.TryGetSavedDatabaseIds(checkpointParentDir, checkpointDirBaseName,
                        out var checkpointSlots))
                {
                    storageSlots = checkpointSlots;

                    if (checkpointSlots.Any(slot => slot != 0))
                        return true;

                    // A lone storage slot can still hold a database other than 0, because a swap relabels a
                    // database without moving its files and the checkpoint records that mapping. Only the
                    // multi-database manager can place a store under an id that differs from its slot, so
                    // recovering one needs it even though a single database is involved. This reads the
                    // metadata of that one database, which recovery goes on to read in any case.
                    if (HasRelabellingMapping(db, logger))
                        return true;
                }

                // Check if there are multiple databases to recover from AOF
                if (serverOptions.EnableAOF)
                {
                    var aofParentDir = serverOptions.AppendOnlyFileBaseDirectory;
                    var aofDirBaseName = GarnetServerOptions.GetAppendOnlyFileDirectoryName(0);

                    if (MultiDatabaseManager.TryGetSavedDatabaseIds(aofParentDir, aofDirBaseName,
                            out var aofSlots) && aofSlots.Any(slot => slot != 0))
                        return true;
                }

                return false;
            }
        }

        /// <summary>
        /// Whether the latest checkpoint of storage slot 0 records a mapping that gives any slot a logical
        /// database id other than its own.
        /// </summary>
        /// <param name="db">Database bound to storage slot 0</param>
        /// <param name="logger">Logger</param>
        private static bool HasRelabellingMapping(GarnetDatabase db, ILogger logger)
        {
            try
            {
                return db.Store.TryGetLatestCheckpointDatabaseMapping(out var mapping, out _) &&
                    mapping.Where(static (dbId, storageSlot) => dbId != storageSlot).Any();
            }
            catch (Exception ex)
            {
                // Recovery reads this same checkpoint and reports an unreadable one where FailOnRecoveryError
                // is honored, so all this has to do is not take the server down before recovery gets there.
                logger?.LogWarning(ex,
                    "Unable to read the database mapping from the latest checkpoint of storage slot 0; recovering each database under the id matching its storage slot");
                return false;
            }
        }
    }
}