// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    internal class MultiDatabaseManager : DatabaseManagerBase
    {
        /// <inheritdoc/>
        public override ref GarnetDatabase DefaultDatabase => ref databases.Map[0];

        /// <inheritdoc/>
        public override int DatabaseCount => activeDbIdsLength;

        /// <summary>
        /// Delegate for creating a new logical database
        /// </summary>
        readonly StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate;

        readonly CancellationTokenSource cts = new();

        readonly int maxDatabases;

        // Map of databases by database ID (by default: of size 1, contains only DB 0)
        ExpandableMap<GarnetDatabase> databases;

        SingleWriterMultiReaderLock databasesLock;

        // Array containing active database IDs
        int[] activeDbIds;

        // Total number of current active database IDs
        int activeDbIdsLength;

        // Last DB ID activated
        int lastActivatedDbId = -1;

        // Reusable task array for tracking checkpointing of multiple DBs
        // Used by recurring checkpointing task if multiple DBs exist
        Task[] checkpointTasks;

        // Reusable task array for tracking aof commits of multiple DBs
        // Used by recurring aof commits task if multiple DBs exist
        Task[] aofTasks;
        readonly object activeDbIdsLock = new();

        // Path of serialization for the DB IDs file used when committing / recovering to / from AOF
        readonly string aofParentDir;

        readonly string aofDirBaseName;

        // Path of serialization for the DB IDs file used when committing / recovering to / from a checkpoint
        readonly string checkpointParentDir;

        readonly string checkpointDirBaseName;

        public MultiDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createsDatabaseDelegate, int maxDatabases, bool createDefaultDatabase = true)
        {
            this.createDatabaseDelegate = createsDatabaseDelegate;
            this.maxDatabases = maxDatabases;

            // Create default databases map of size 1
            databases = new ExpandableMap<GarnetDatabase>(1, 0, this.maxDatabases - 1);

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                var db = createDatabaseDelegate(0, out var storeCheckpointDir, out var aofDir);

                var checkpointDirInfo = new DirectoryInfo(storeCheckpointDir);
                this.checkpointDirBaseName = checkpointDirInfo.Name;
                this.checkpointParentDir = checkpointDirInfo.Parent!.FullName;

                if (aofDir != null)
                {
                    var aofDirInfo = new DirectoryInfo(aofDir);
                    this.aofDirBaseName = aofDirInfo.Name;
                    this.aofParentDir = aofDirInfo.Parent!.FullName;
                }

                // Set new database in map
                if (!this.TryAddDatabase(0, ref db))
                    throw new GarnetException("Failed to set initial database in databases map");
            }
        }

        public override void RecoverCheckpoint(bool failOnRecoveryError, bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null)
        {
            if (replicaRecover)
                throw new GarnetException(
                    $"Unexpected call to {nameof(MultiDatabaseManager)}.{nameof(RecoverCheckpoint)} with {nameof(replicaRecover)} == true.");

            try
            {
                if (!TryGetSavedDatabaseIds(checkpointParentDir, checkpointDirBaseName, out var dbIdsToRecover))
                    return;

                foreach (var dbId in dbIdsToRecover)
                {
                    if (!TryGetOrAddDatabase(dbId, out var db))
                        throw new GarnetException($"Failed create initial database for recovery (DB ID = {dbId}).");

                    RecoverDatabaseCheckpoint(ref db);
                }
            }
            catch (TsavoriteNoHybridLogException ex)
            {
                // No hybrid log being found is not the same as an error in recovery. e.g. fresh start
                logger?.LogInformation(ex, "No Hybrid Log found for recovery; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
            }
            catch (Exception ex)
            {
                logger?.LogInformation(ex, "Error during recovery of store; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
                if (failOnRecoveryError)
                    throw;
            }
        }

        public override FunctionsState CreateFunctionsState(CustomCommandManager customCommandManager, GarnetObjectSerializer garnetObjectSerializer, int dbId = 0)
        {
            if (!this.TryGetOrAddDatabase(dbId, out var db))
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return new(db.AppendOnlyFile, db.VersionMap, customCommandManager, null, db.ObjectStoreSizeTracker,
                garnetObjectSerializer);
        }

        /// <inheritdoc/>
        public override bool TryGetOrAddDatabase(int dbId, out GarnetDatabase db)
        {
            db = default;

            if (!databases.TryGetOrSet(dbId, () => createDatabaseDelegate(dbId, out _, out _), out db, out var added))
                return false;

            if (added)
                HandleDatabaseAdded(dbId);

            return true;
        }

        /// <summary>
        /// Try to add a new database
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <param name="db">Database</param>
        /// <returns></returns>
        private bool TryAddDatabase(int dbId, ref GarnetDatabase db)
        {
            if (!databases.TrySetValue(dbId, ref db))
                return false;

            HandleDatabaseAdded(dbId);
            return true;
        }

        /// <summary>
        /// Handle a new database added
        /// </summary>
        /// <param name="dbId">ID of database added</param>
        private void HandleDatabaseAdded(int dbId)
        {
            // If size tracker exists and is stopped, start it (only if DB 0 size tracker is started as well)
            var db = databases.Map[dbId];
            if (dbId != 0 && ObjectStoreSizeTracker != null && !ObjectStoreSizeTracker.Stopped &&
                db.ObjectStoreSizeTracker != null && db.ObjectStoreSizeTracker.Stopped)
                db.ObjectStoreSizeTracker.Start(cts.Token);

            var dbIdIdx = Interlocked.Increment(ref lastActivatedDbId);

            // If there is no size increase needed for activeDbIds, set the added ID in the array
            if (activeDbIds != null && dbIdIdx < activeDbIds.Length)
            {
                activeDbIds[dbIdIdx] = dbId;
                Interlocked.Increment(ref activeDbIdsLength);
                return;
            }

            lock (activeDbIdsLock)
            {
                // Select the next size of activeDbIds (as multiple of 2 from the existing size)
                var newSize = activeDbIds?.Length ?? 1;
                while (dbIdIdx + 1 > newSize)
                {
                    newSize = Math.Min(maxDatabases, newSize * 2);
                }

                // Set an updated instance of activeDbIds
                var activeDbIdsSnapshot = activeDbIds;
                var activeDbIdsUpdated = new int[newSize];

                if (activeDbIdsSnapshot != null)
                {
                    Array.Copy(activeDbIdsSnapshot, activeDbIdsUpdated, dbIdIdx);
                }

                // Set the last added ID
                activeDbIdsUpdated[dbIdIdx] = dbId;

                activeDbIds = activeDbIdsUpdated;
                activeDbIdsLength = dbIdIdx + 1;
                checkpointTasks = new Task[activeDbIdsLength];
                aofTasks = new Task[activeDbIdsLength];
            }
        }

        /// <summary>
        /// Retrieves saved database IDs from parent checkpoint / AOF path
        /// e.g. if path contains directories: baseName, baseName_1, baseName_2, baseName_10
        /// DB IDs 0,1,2,10 will be returned
        /// </summary>
        /// <param name="path">Parent path</param>
        /// <param name="baseName">Base name of directories containing database-specific checkpoints / AOFs</param>
        /// <param name="dbIds">DB IDs extracted from parent path</param>
        /// <returns>True if successful</returns>
        private bool TryGetSavedDatabaseIds(string path, string baseName, out int[] dbIds)
        {
            dbIds = default;
            if (!Directory.Exists(path)) return false;

            var dirs = Directory.GetDirectories(path, $"{baseName}*", SearchOption.TopDirectoryOnly);
            dbIds = new int[dirs.Length];
            for (var i = 0; i < dirs.Length; i++)
            {
                var dirName = new DirectoryInfo(dirs[i]).Name;
                var sepIdx = dirName.IndexOf('_');
                var dbId = 0;

                if (sepIdx != -1 && !int.TryParse(dirName.AsSpan(sepIdx + 1), out dbId))
                    continue;

                dbIds[i] = dbId;
            }

            return true;
        }

        public override void Dispose()
        {
            if (Disposed) return;

            cts.Cancel();

            // Disable changes to databases map and dispose all databases
            databases.mapLock.WriteLock();
            foreach (var db in databases.Map)
                db.Dispose();

            cts.Dispose();

            Disposed = true;
        }
    }
}
