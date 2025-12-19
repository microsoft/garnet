// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.Metrics;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Multiple logical database management
    /// </summary>
    internal class MultiDatabaseManager : DatabaseManagerBase
    {
        /// <inheritdoc/>
        public override GarnetDatabase DefaultDatabase => databases.Map[0];

        /// <inheritdoc/>
        public override int DatabaseCount => activeDbIds.ActualSize;

        /// <inheritdoc/>
        public override int MaxDatabaseId => databases.ActualSize - 1;

        // Map of databases by database ID (by default: of size 1, contains only DB 0)
        ExpandableMap<GarnetDatabase> databases;

        // Map containing active database IDs
        ExpandableMap<int> activeDbIds;

        // Reader-Writer lock for thread-safety during a swap-db operation
        // The swap-db operation should take a write lock and any operation that should be swap-db-safe should take a read lock.
        SingleWriterMultiReaderLock databasesContentLock;

        // Lock for synchronizing checkpointing of all active DBs (if more than one)
        SingleWriterMultiReaderLock multiDbCheckpointingLock;

        // Reusable task array for tracking checkpointing of multiple DBs
        // Used by recurring checkpointing task if multiple DBs exist
        Task[] checkpointTasks;

        // Reusable array for storing database IDs for checkpointing
        int[] dbIdsToCheckpoint;

        // True if StartObjectSizeTrackers was previously called
        bool sizeTrackersStarted;

        public MultiDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate,
            StoreWrapper storeWrapper, bool createDefaultDatabase = true) : base(createDatabaseDelegate, storeWrapper)
        {
            Logger = storeWrapper.loggerFactory?.CreateLogger(nameof(MultiDatabaseManager));

            var maxDatabases = storeWrapper.serverOptions.MaxDatabases;

            // Create default databases map of size 1
            databases = new ExpandableMap<GarnetDatabase>(1, 0, maxDatabases - 1);

            // Create default active database ids map of size 1
            activeDbIds = new ExpandableMap<int>(1, 0, maxDatabases - 1);

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                var db = createDatabaseDelegate(0);

                // Set new database in map
                if (!TryAddDatabase(0, db))
                    throw new GarnetException("Failed to set initial database in databases map");
            }
        }

        public MultiDatabaseManager(MultiDatabaseManager src, bool enableAof) : this(src.CreateDatabaseDelegate,
            src.StoreWrapper, createDefaultDatabase: false)
        {
            CopyDatabases(src, enableAof);
        }

        public MultiDatabaseManager(SingleDatabaseManager src) :
            this(src.CreateDatabaseDelegate, src.StoreWrapper, false)
        {
            CopyDatabases(src, src.StoreWrapper.serverOptions.EnableAOF);
        }

        /// <inheritdoc/>
        public override void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null)
        {
            if (replicaRecover)
                throw new GarnetException(
                    $"Unexpected call to {nameof(MultiDatabaseManager)}.{nameof(RecoverCheckpoint)} with {nameof(replicaRecover)} == true.");

            var checkpointParentDir = StoreWrapper.serverOptions.MainStoreCheckpointBaseDirectory;
            var checkpointDirBaseName = StoreWrapper.serverOptions.GetCheckpointDirectoryName(0);

            int[] dbIdsToRecover;
            try
            {
                if (!TryGetSavedDatabaseIds(checkpointParentDir, checkpointDirBaseName, out dbIdsToRecover))
                    return;
            }
            catch (Exception ex)
            {
                Logger?.LogInformation(ex,
                    "Error during recovery of database ids; checkpointParentDir = {checkpointParentDir}; checkpointDirBaseName = {checkpointDirBaseName}",
                    checkpointParentDir, checkpointDirBaseName);
                if (StoreWrapper.serverOptions.FailOnRecoveryError)
                    throw;
                return;
            }

            long storeVersion = 0, objectStoreVersion = 0;

            foreach (var dbId in dbIdsToRecover)
            {
                var db = TryGetOrAddDatabase(dbId, out var success, out _);
                if (!success)
                    throw new GarnetException($"Failed to retrieve or create database for checkpoint recovery (DB ID = {dbId}).");

                try
                {
                    RecoverDatabaseCheckpoint(db, out storeVersion, out objectStoreVersion);
                }
                catch (TsavoriteNoHybridLogException ex)
                {
                    // No hybrid log being found is not the same as an error in recovery. e.g. fresh start
                    Logger?.LogInformation(ex,
                        "No Hybrid Log found for recovery; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}",
                        storeVersion, objectStoreVersion);
                }
                catch (Exception ex)
                {
                    Logger?.LogInformation(ex,
                        "Error during recovery of store; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}",
                        storeVersion, objectStoreVersion);
                    if (StoreWrapper.serverOptions.FailOnRecoveryError)
                        throw;
                }

                // After recovery, we check if store versions match
                if (db.ObjectStore != null && storeVersion != objectStoreVersion)
                {
                    Logger?.LogInformation("Main store and object store checkpoint versions do not match; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
                    if (StoreWrapper.serverOptions.FailOnRecoveryError)
                        throw new GarnetException("Main store and object store checkpoint versions do not match");
                }

                // Once everything is setup, initialize the VectorManager
                db.VectorManager.Initialize();
            }
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, ILogger logger = null, CancellationToken token = default)
        {
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return false;

            var checkpointTask = Task.Run(async () =>
            {
                var checkpointLockTaken = false;

                try
                {
                    var activeDbIdsMapSize = activeDbIds.ActualSize;

                    if (activeDbIdsMapSize > 1)
                    {
                        if (!multiDbCheckpointingLock.TryWriteLock())
                            return false;

                        checkpointLockTaken = true;
                    }

                    var activeDbIdsMapSnapshot = activeDbIds.Map;
                    Array.Copy(activeDbIdsMapSnapshot, dbIdsToCheckpoint, activeDbIdsMapSize);

                    return await TakeDatabasesCheckpointAsync(activeDbIdsMapSize, logger: logger, token: token);
                }
                finally
                {
                    if (checkpointLockTaken)
                        multiDbCheckpointingLock.WriteUnlock();

                    databasesContentLock.ReadUnlock();
                }
            }, token).GetAwaiter();

            if (background)
                return true;

            return checkpointTask.GetResult();
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, int dbId, ILogger logger = null, CancellationToken token = default)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && databasesMapSnapshot[dbId] != null);

            // Check if checkpoint already in progress
            if (!TryPauseCheckpoints(dbId))
                return false;

            var checkpointTask = TakeCheckpointAsync(databasesMapSnapshot[dbId], logger: logger, token: token).ContinueWith(
                t =>
                {
                    try
                    {
                        if (t.IsCompletedSuccessfully)
                        {
                            var storeTailAddress = t.Result.Item1;
                            var objectStoreTailAddress = t.Result.Item2;
                            UpdateLastSaveData(dbId, storeTailAddress, objectStoreTailAddress);
                        }
                    }
                    finally
                    {
                        ResumeCheckpoints(dbId);
                    }
                }, TaskContinuationOptions.ExecuteSynchronously).GetAwaiter();

            if (background)
                return true;

            checkpointTask.GetResult();
            return true;
        }

        /// <inheritdoc/>
        public override async Task TakeOnDemandCheckpointAsync(DateTimeOffset entryTime, int dbId = 0)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && databasesMapSnapshot[dbId] != null);

            var db = databasesMapSnapshot[dbId];

            // Check if checkpoint already in progress
            var checkpointsPaused = TryPauseCheckpoints(dbId);

            try
            {
                // If another checkpoint is in progress or a checkpoint was taken beyond the provided entryTime - return
                if (!checkpointsPaused || db.LastSaveTime > entryTime)
                    return;

                // Necessary to take a checkpoint because the latest checkpoint is before entryTime
                var result = await TakeCheckpointAsync(db, logger: Logger);

                var storeTailAddress = result.Item1;
                var objectStoreTailAddress = result.Item2;
                UpdateLastSaveData(dbId, storeTailAddress, objectStoreTailAddress);
            }
            finally
            {
                ResumeCheckpoints(dbId);
            }
        }

        /// <inheritdoc/>
        public override async Task TaskCheckpointBasedOnAofSizeLimitAsync(long aofSizeLimit, CancellationToken token = default,
            ILogger logger = null)
        {
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            var activeDbIdsMapSize = activeDbIds.ActualSize;

            var checkpointLockTaken = false;

            try
            {
                if (activeDbIdsMapSize > 1)
                {
                    if (!multiDbCheckpointingLock.TryWriteLock())
                        return;

                    checkpointLockTaken = true;
                }

                var databasesMapSnapshot = databases.Map;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                var dbIdsIdx = 0;
                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    var dbAofSize = db.AppendOnlyFile.TailAddress - db.AppendOnlyFile.BeginAddress;
                    if (dbAofSize > aofSizeLimit)
                    {
                        logger?.LogInformation("Enforcing AOF size limit currentAofSize: {dbAofSize} > AofSizeLimit: {aofSizeLimit} (Database ID: {dbId})",
                            dbAofSize, aofSizeLimit, dbId);
                        dbIdsToCheckpoint[dbIdsIdx++] = dbId;
                        break;
                    }
                }

                if (dbIdsIdx == 0) return;

                await TakeDatabasesCheckpointAsync(dbIdsIdx, logger: logger, token: token);
            }
            finally
            {
                if (checkpointLockTaken)
                    multiDbCheckpointingLock.WriteUnlock();

                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override async Task CommitToAofAsync(CancellationToken token = default, ILogger logger = null)
        {
            // Take a read lock to make sure that swap-db operation is not in progress
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;
                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                var aofTasks = new Task<(long, long)>[activeDbIdsMapSize];

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    aofTasks[i] = db.AppendOnlyFile.CommitAsync(token: token).AsTask().ContinueWith(_ => (db.AppendOnlyFile.TailAddress, db.AppendOnlyFile.CommittedUntilAddress), token);
                }

                var exThrown = false;
                try
                {
                    await Task.WhenAll(aofTasks);
                }
                catch (Exception)
                {
                    // Only first exception is caught here, if any. 
                    // Proper handling of this and consequent exceptions in the next loop. 
                    exThrown = true;
                }

                foreach (var t in aofTasks)
                {
                    if (!t.IsFaulted || t.Exception == null) continue;

                    logger?.LogError(t.Exception,
                        "Exception raised while committing to AOF. AOF tail address = {tailAddress}; AOF committed until address = {commitAddress}; ",
                        t.Result.Item1, t.Result.Item2);
                }

                if (exThrown)
                    throw new GarnetException($"Error occurred while committing to AOF in {nameof(MultiDatabaseManager)}. Refer to previous log messages for more details.");
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override async Task CommitToAofAsync(int dbId, CancellationToken token = default, ILogger logger = null)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && databasesMapSnapshot[dbId] != null);

            await databasesMapSnapshot[dbId].AppendOnlyFile.CommitAsync(token: token);
        }

        /// <inheritdoc/>
        public override async Task WaitForCommitToAofAsync(CancellationToken token = default, ILogger logger = null)
        {
            // Take a read lock to make sure that swap-db operation is not in progress
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;
                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                var aofTasks = new Task[activeDbIdsMapSize];

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    aofTasks[i] = db.AppendOnlyFile.WaitForCommitAsync(token: token).AsTask();
                }

                await Task.WhenAll(aofTasks);
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override void RecoverAOF()
        {
            var aofParentDir = StoreWrapper.serverOptions.AppendOnlyFileBaseDirectory;
            var aofDirBaseName = StoreWrapper.serverOptions.GetAppendOnlyFileDirectoryName(0);

            int[] dbIdsToRecover;
            try
            {
                if (!TryGetSavedDatabaseIds(aofParentDir, aofDirBaseName, out dbIdsToRecover))
                    return;

            }
            catch (Exception ex)
            {
                Logger?.LogInformation(ex,
                    "Error during recovery of database ids; aofParentDir = {aofParentDir}; aofDirBaseName = {aofDirBaseName}",
                    aofParentDir, aofDirBaseName);
                return;
            }

            foreach (var dbId in dbIdsToRecover)
            {
                var db = TryGetOrAddDatabase(dbId, out var success, out _);
                if (!success)
                    throw new GarnetException($"Failed to retrieve or create database for AOF recovery (DB ID = {dbId}).");

                RecoverDatabaseAOF(db);
            }
        }

        /// <inheritdoc/>
        public override long ReplayAOF(long untilAddress = -1)
        {
            if (!StoreWrapper.serverOptions.EnableAOF)
                return -1;

            // When replaying AOF we do not want to write record again to AOF.
            // So initialize local AofProcessor with recordToAof: false.
            var aofProcessor = new AofProcessor(StoreWrapper, recordToAof: false, logger: Logger);

            long replicationOffset = 0;
            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var offset = ReplayDatabaseAOF(aofProcessor, databasesMapSnapshot[dbId], dbId == 0 ? untilAddress : -1);
                    if (dbId == 0) replicationOffset = offset;
                }
            }
            finally
            {
                aofProcessor.Dispose();
            }

            return replicationOffset;
        }

        /// <inheritdoc/>
        public override void DoCompaction(CancellationToken token = default, ILogger logger = null)
        {
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                var exThrown = false;
                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    try
                    {
                        DoCompaction(db);
                    }
                    catch (Exception)
                    {
                        exThrown = true;
                    }
                }

                if (exThrown)
                    throw new GarnetException($"Error occurred during compaction in {nameof(MultiDatabaseManager)}. Refer to previous log messages for more details.");
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override bool GrowIndexesIfNeeded(CancellationToken token = default)
        {
            var allIndexesMaxedOut = true;

            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return false;

            try
            {
                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                var databasesMapSnapshot = databases.Map;

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];

                    var indexesMaxedOut = GrowIndexesIfNeeded(databasesMapSnapshot[dbId]);
                    if (allIndexesMaxedOut && !indexesMaxedOut)
                        allIndexesMaxedOut = false;
                }

                return allIndexesMaxedOut;
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override void ExecuteObjectCollection()
        {
            var databasesMapSnapshot = databases.Map;

            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                ExecuteObjectCollection(databasesMapSnapshot[dbId], Logger);
            }
        }

        /// <inheritdoc/>
        public override void ExpiredKeyDeletionScan()
        {
            var databasesMapSnapshot = databases.Map;

            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                ExpiredKeyDeletionScan(databasesMapSnapshot[dbId]);
            }
        }

        /// <inheritdoc/>
        public override void StartObjectSizeTrackers(CancellationToken token = default)
        {
            sizeTrackersStarted = true;

            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(db != null);

                    db.ObjectStoreSizeTracker?.Start(token);
                }
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override void Reset(int dbId = 0)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            ResetDatabase(db);
        }

        /// <inheritdoc/>
        public override void ResetRevivificationStats()
        {
            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;
            var databaseMapSnapshot = databases.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                databaseMapSnapshot[dbId].MainStore.ResetRevivificationStats();
                databaseMapSnapshot[dbId].ObjectStore?.ResetRevivificationStats();
            }
        }

        public override void EnqueueCommit(AofEntryType entryType, long version, int dbId = 0)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            EnqueueDatabaseCommit(db, entryType, version);
        }

        /// <inheritdoc/>
        public override GarnetDatabase[] GetDatabasesSnapshot()
        {
            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;
            var databaseMapSnapshot = databases.Map;
            var databasesSnapshot = new GarnetDatabase[activeDbIdsMapSize];

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                databasesSnapshot[i] = databaseMapSnapshot[dbId];
            }

            return databasesSnapshot;
        }

        /// <inheritdoc/>
        public override bool TrySwapDatabases(int dbId1, int dbId2, CancellationToken token = default)
        {
            if (dbId1 == dbId2) return true;

            var db1 = TryGetOrAddDatabase(dbId1, out var success, out _);
            if (!success)
                return false;

            var db2 = TryGetOrAddDatabase(dbId2, out success, out _);
            if (!success)
                return false;

            if (!TryGetDatabasesContentWriteLock(token)) return false;

            try
            {
                var databaseMapSnapshot = databases.Map;
                var enableAof = StoreWrapper.serverOptions.EnableAOF;
                databaseMapSnapshot[dbId1] = new GarnetDatabase(dbId1, db2, enableAof, copyLastSaveData: true);
                databaseMapSnapshot[dbId2] = new GarnetDatabase(dbId2, db1, enableAof, copyLastSaveData: true);

                var activeSessions = 0;
                foreach (var server in StoreWrapper.Servers)
                {
                    if (server is not GarnetServerBase serverBase) continue;

                    foreach (var session in serverBase.ActiveConsumers())
                    {
                        if (session is not RespServerSession respServerSession) continue;
                        activeSessions++;

                        if (activeSessions > 1) return false;

                        respServerSession.TrySwapDatabaseSessions(dbId1, dbId2);
                    }
                }
            }
            finally
            {
                databasesContentLock.WriteUnlock();
            }

            return true;
        }

        /// <inheritdoc/>
        public override IDatabaseManager Clone(bool enableAof) => new MultiDatabaseManager(this, enableAof);

        /// <inheritdoc/>
        public override FunctionsState CreateFunctionsState(int dbId = 0, byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return new(db.AppendOnlyFile, db.VersionMap, StoreWrapper.customCommandManager, null, db.ObjectStoreSizeTracker,
                StoreWrapper.GarnetObjectSerializer, db.VectorManager, respProtocolVersion);
        }

        /// <inheritdoc/>
        public override GarnetDatabase TryGetOrAddDatabase(int dbId, out bool success, out bool added)
        {
            added = false;
            success = false;

            // Get a current snapshot of the databases
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            // If database exists in the map, return it
            if (dbId >= 0 && dbId < databasesMapSize && databasesMapSnapshot[dbId] != null)
            {
                success = true;
                return databasesMapSnapshot[dbId];
            }

            // Take the database map's write lock so that no new databases can be added with the same ID
            // Note that we don't call TrySetValue because that would only guarantee that the map instance does not change,
            // but the underlying values can still change.
            // So here we're calling TrySetValueUnsafe and handling the locks ourselves.
            databases.mapLock.WriteLock();

            try
            {
                // Check again if database exists in the map, if so return it
                if (dbId >= 0 && dbId < databasesMapSize && databasesMapSnapshot[dbId] != null)
                {
                    success = true;
                    return databasesMapSnapshot[dbId];
                }

                // Create the database and use TrySetValueUnsafe to add it to the map
                var db = CreateDatabaseDelegate(dbId);
                if (!databases.TrySetValueUnsafe(dbId, ref db, false))
                    return default;
            }
            finally
            {
                // Release the database map's lock
                databases.mapLock.WriteUnlock();
            }

            added = true;
            success = true;

            HandleDatabaseAdded(dbId);

            // Update the databases snapshot and return a reference to the added database
            databasesMapSnapshot = databases.Map;
            return databasesMapSnapshot[dbId];
        }

        /// <inheritdoc/>
        public override bool TryPauseCheckpoints(int dbId)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return TryPauseCheckpoints(db);
        }

        /// <inheritdoc/>
        public override void ResumeCheckpoints(int dbId)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && databasesMapSnapshot[dbId] != null);

            ResumeCheckpoints(databasesMapSnapshot[dbId]);
        }

        /// <inheritdoc/>
        public override GarnetDatabase TryGetDatabase(int dbId, out bool found)
        {
            found = false;

            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            if (dbId == 0)
            {
                Debug.Assert(databasesMapSnapshot[0] != null);
                found = true;
                return databasesMapSnapshot[0];
            }

            // Check if database already exists
            if (dbId < databasesMapSize)
            {
                if (databasesMapSnapshot[dbId] != null)
                {
                    found = true;
                    return databasesMapSnapshot[dbId];
                }
            }

            found = false;
            return default;
        }

        /// <inheritdoc/>
        public override void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            var db = TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            FlushDatabase(db, unsafeTruncateLog);
        }

        /// <inheritdoc/>
        public override void FlushAllDatabases(bool unsafeTruncateLog)
        {
            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;
            var databaseMapSnapshot = databases.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                FlushDatabase(databaseMapSnapshot[dbId], unsafeTruncateLog);
            }
        }

        /// <summary>
        /// Continuously try to take a databases content read lock
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if lock acquired</returns>
        public bool TryGetDatabasesContentReadLock(CancellationToken token = default)
        {
            var lockAcquired = databasesContentLock.TryReadLock();

            while (!lockAcquired && !token.IsCancellationRequested && !Disposed)
            {
                Thread.Yield();
                lockAcquired = databasesContentLock.TryReadLock();
            }

            return lockAcquired;
        }

        /// <summary>
        /// Continuously try to take a databases content write lock
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if lock acquired</returns>
        public bool TryGetDatabasesContentWriteLock(CancellationToken token = default)
        {
            var lockAcquired = databasesContentLock.TryWriteLock();

            while (!lockAcquired && !token.IsCancellationRequested && !Disposed)
            {
                Thread.Yield();
                lockAcquired = databasesContentLock.TryWriteLock();
            }

            return lockAcquired;
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
        internal static bool TryGetSavedDatabaseIds(string path, string baseName, out int[] dbIds)
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

        /// <summary>
        /// Try to add a new database
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <param name="db">Database</param>
        /// <returns></returns>
        private bool TryAddDatabase(int dbId, GarnetDatabase db)
        {
            if (!databases.TrySetValue(dbId, db))
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
            if (sizeTrackersStarted)
                db.ObjectStoreSizeTracker?.Start(StoreWrapper.ctsCommit.Token);

            activeDbIds.TryGetNextId(out var nextIdx);
            activeDbIds.TrySetValue(nextIdx, db.Id);

            activeDbIds.mapLock.ReadLock();
            try
            {
                checkpointTasks = new Task[activeDbIds.ActualSize];
                dbIdsToCheckpoint = new int[activeDbIds.ActualSize];
            }
            finally
            {
                activeDbIds.mapLock.ReadUnlock();
            }
        }

        /// <summary>
        /// Copy active databases from specified IDatabaseManager instance
        /// </summary>
        /// <param name="src">Source IDatabaseManager</param>
        /// <param name="enableAof">Enable AOF in copied databases</param>
        private void CopyDatabases(IDatabaseManager src, bool enableAof)
        {
            switch (src)
            {
                case SingleDatabaseManager sdbm:
                    var defaultDbCopy = new GarnetDatabase(0, sdbm.DefaultDatabase, enableAof);
                    TryAddDatabase(0, defaultDbCopy);
                    return;
                case MultiDatabaseManager mdbm:
                    var activeDbIdsMapSize = mdbm.activeDbIds.ActualSize;
                    var activeDbIdsMapSnapshot = mdbm.activeDbIds.Map;
                    var databasesMapSnapshot = mdbm.databases.Map;

                    for (var i = 0; i < activeDbIdsMapSize; i++)
                    {
                        var dbId = activeDbIdsMapSnapshot[i];
                        var dbCopy = new GarnetDatabase(dbId, databasesMapSnapshot[dbId], enableAof);
                        TryAddDatabase(dbId, dbCopy);
                    }

                    return;
                default:
                    throw new NotImplementedException();
            }
        }

        /// <summary>
        /// Asynchronously checkpoint multiple databases and wait for all to complete
        /// </summary>
        /// <param name="dbIdsCount">Number of databases to checkpoint (first dbIdsCount indexes from dbIdsToCheckpoint)</param>
        /// <param name="logger">Logger</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>False if checkpointing already in progress</returns>
        private async Task<bool> TakeDatabasesCheckpointAsync(int dbIdsCount, ILogger logger = null,
            CancellationToken token = default)
        {
            Debug.Assert(checkpointTasks != null);
            Debug.Assert(dbIdsCount <= dbIdsToCheckpoint.Length);

            for (var i = 0; i < checkpointTasks.Length; i++)
                checkpointTasks[i] = Task.CompletedTask;

            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return false;

            try
            {
                var databaseMapSnapshot = databases.Map;

                for (var currIdx = 0; currIdx < dbIdsCount; currIdx++)
                {
                    var dbId = dbIdsToCheckpoint[currIdx];

                    // If a checkpoint is already in progress for this database, skip it
                    if (!TryPauseCheckpoints(dbId))
                        continue;

                    checkpointTasks[currIdx] = TakeCheckpointAsync(databaseMapSnapshot[dbId], logger: logger, token: token).ContinueWith(
                        t =>
                        {
                            ResumeCheckpoints(dbId);

                            if (!t.IsCompletedSuccessfully)
                                return;

                            var storeTailAddress = t.Result.Item1;
                            var objectStoreTailAddress = t.Result.Item2;
                            UpdateLastSaveData(dbId, storeTailAddress, objectStoreTailAddress);
                        }, TaskContinuationOptions.ExecuteSynchronously);
                }

                await Task.WhenAll(checkpointTasks);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Checkpointing threw exception");
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }

            return true;
        }

        private void UpdateLastSaveData(int dbId, long? storeTailAddress, long? objectStoreTailAddress)
        {
            var databasesMapSnapshot = databases.Map;

            var db = databasesMapSnapshot[dbId];
            db.LastSaveTime = DateTimeOffset.UtcNow;

            if (storeTailAddress.HasValue)
            {
                db.LastSaveStoreTailAddress = storeTailAddress.Value;

                if (db.ObjectStore != null && objectStoreTailAddress.HasValue)
                    db.LastSaveObjectStoreTailAddress = objectStoreTailAddress.Value;
            }
        }

        /// <inheritdoc/>
        public override void RecoverVectorSets()
        {
            var databasesMapSnapshot = databases.Map;

            var activeDbIdsMapSize = activeDbIds.ActualSize;
            var activeDbIdsMapSnapshot = activeDbIds.Map;

            for (var i = 0; i < activeDbIdsMapSize; i++)
            {
                var dbId = activeDbIdsMapSnapshot[i];
                databasesMapSnapshot[dbId].VectorManager.ResumePostRecovery();
            }
        }

        public override void Dispose()
        {
            if (Disposed) return;

            Disposed = true;

            // Disable changes to databases map and dispose all databases
            databases.mapLock.CloseLock();
            foreach (var db in databases.Map)
                db?.Dispose();

            databasesContentLock.CloseLock();
            activeDbIds.mapLock.CloseLock();
        }

        public override (long numExpiredKeysFound, long totalRecordsScanned) ExpiredKeyDeletionScan(int dbId)
        {
            var (k1, t1) = MainStoreExpiredKeyDeletionScan(GetDbById(dbId));
            var (k2, t2) = StoreWrapper.serverOptions.DisableObjects ? (0, 0) : ObjectStoreExpiredKeyDeletionScan(GetDbById(dbId));
            return (k1 + k2, t1 + t2);
        }

        private GarnetDatabase GetDbById(int dbId)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && databasesMapSnapshot[dbId] != null);
            return databasesMapSnapshot[dbId];
        }

        public override (HybridLogScanMetrics mainStore, HybridLogScanMetrics objectStore)[] CollectHybridLogStats()
        {
            var databasesMapSnapshot = databases.Map;
            var result = new (HybridLogScanMetrics mainStore, HybridLogScanMetrics objectStore)[databasesMapSnapshot.Length];
            for (int i = 0; i < databasesMapSnapshot.Length; i++)
            {
                var db = databasesMapSnapshot[i];
                result[i] = CollectHybridLogStatsForDb(db);
            }
            return result;
        }
    }
}