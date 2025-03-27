﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
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
        public override ref GarnetDatabase DefaultDatabase => ref databases.Map[0];

        /// <inheritdoc/>
        public override int DatabaseCount => activeDbIds.ActualSize;

        // Map of databases by database ID (by default: of size 1, contains only DB 0)
        ExpandableMap<GarnetDatabase> databases;

        // Map containing active database IDs
        ExpandableMap<int> activeDbIds;

        // Reader-Writer lock for thread-safety during a swap-db operation
        // The swap-db operation should take a write lock and any operation that should be swap-db-safe should take a read lock.
        SingleWriterMultiReaderLock databasesContentLock;

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
                if (!TryAddDatabase(0, ref db))
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
                ref var db = ref TryGetOrAddDatabase(dbId, out var success, out _);
                if (!success)
                    throw new GarnetException($"Failed to retrieve or create database for checkpoint recovery (DB ID = {dbId}).");

                try
                {
                    RecoverDatabaseCheckpoint(ref db, out storeVersion, out objectStoreVersion);
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
            }
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, ILogger logger = null, CancellationToken token = default)
        {
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return false;

            try
            {
                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;
                Array.Copy(activeDbIdsMapSnapshot, dbIdsToCheckpoint, activeDbIdsMapSize);

                TakeDatabasesCheckpointAsync(activeDbIdsMapSize, logger: logger, token: token).GetAwaiter().GetResult();
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }

            return true;
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, int dbId, ILogger logger = null, CancellationToken token = default)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault());

            if (!TryPauseCheckpointsContinuousAsync(dbId, token).GetAwaiter().GetResult())
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
                            TryUpdateLastSaveData(dbId, storeTailAddress, objectStoreTailAddress, token);
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
            Debug.Assert(dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault());

            // Take lock to ensure no other task will be taking a checkpoint
            var checkpointsPaused = TryPauseCheckpoints(dbId);

            try
            {
                // If an external task has taken a checkpoint beyond the provided entryTime return
                if (!checkpointsPaused || databasesMapSnapshot[dbId].LastSaveTime > entryTime)
                    return;

                // Necessary to take a checkpoint because the latest checkpoint is before entryTime
                var result = await TakeCheckpointAsync(databasesMapSnapshot[dbId], logger: Logger);

                var storeTailAddress = result.Item1;
                var objectStoreTailAddress = result.Item2;
                TryUpdateLastSaveData(dbId, storeTailAddress, objectStoreTailAddress);
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

            try
            {
                var databasesMapSnapshot = databases.Map;
                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                var dbIdsIdx = 0;
                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(!db.IsDefault());

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
                    Debug.Assert(!db.IsDefault());

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
            Debug.Assert(dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault());

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
                    Debug.Assert(!db.IsDefault());

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
                ref var db = ref TryGetOrAddDatabase(dbId, out var success, out _);
                if (!success)
                    throw new GarnetException($"Failed to retrieve or create database for AOF recovery (DB ID = {dbId}).");

                RecoverDatabaseAOF(ref db);
            }
        }

        /// <inheritdoc/>
        public override long ReplayAOF(long untilAddress = -1)
        {
            if (!StoreWrapper.serverOptions.EnableAOF)
                return -1;

            // When replaying AOF we do not want to write record again to AOF.
            // So initialize local AofProcessor with recordToAof: false.
            var aofProcessor = new AofProcessor(StoreWrapper, recordToAof: false, Logger);

            long replicationOffset = 0;
            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsMapSize = activeDbIds.ActualSize;
                var activeDbIdsMapSnapshot = activeDbIds.Map;

                for (var i = 0; i < activeDbIdsMapSize; i++)
                {
                    var dbId = activeDbIdsMapSnapshot[i];
                    var offset = ReplayDatabaseAOF(aofProcessor, ref databasesMapSnapshot[dbId], dbId == 0 ? untilAddress : -1);
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
                    Debug.Assert(!db.IsDefault());

                    try
                    {
                        DoCompaction(ref db);
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

                    var indexesMaxedOut = GrowIndexesIfNeeded(ref databasesMapSnapshot[dbId]);
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
                    Debug.Assert(!db.IsDefault());

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
            ref var db = ref TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            ResetDatabase(ref db);
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
            ref var db = ref TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            EnqueueDatabaseCommit(ref db, entryType, version);
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

            ref var db1 = ref TryGetOrAddDatabase(dbId1, out var success, out _);
            if (!success)
                return false;

            ref var db2 = ref TryGetOrAddDatabase(dbId2, out success, out _);
            if (!success)
                return false;

            if (!TryGetDatabasesContentWriteLock(token)) return false;

            try
            {
                var databaseMapSnapshot = databases.Map;
                var tmp = db1;
                databaseMapSnapshot[dbId1] = db2;
                databaseMapSnapshot[dbId2] = tmp;

                databaseMapSnapshot[dbId1].Id = dbId1;
                databaseMapSnapshot[dbId2].Id = dbId2;

                var sessions = StoreWrapper.TcpServer?.ActiveConsumers().ToArray();
                if (sessions == null) return true;
                if (sessions.Length > 1) return false;

                foreach (var session in sessions)
                {
                    if (session is not RespServerSession respServerSession) continue;

                    respServerSession.TrySwapDatabaseSessions(dbId1, dbId2);
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
        public override FunctionsState CreateFunctionsState(int dbId = 0)
        {
            ref var db = ref TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return new(db.AppendOnlyFile, db.VersionMap, StoreWrapper.customCommandManager, null, db.ObjectStoreSizeTracker,
                StoreWrapper.GarnetObjectSerializer);
        }

        /// <inheritdoc/>
        public override ref GarnetDatabase TryGetOrAddDatabase(int dbId, out bool success, out bool added)
        {
            added = false;
            success = false;

            // Get a current snapshot of the databases
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            // If database exists in the map, return it
            if (dbId >= 0 && dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault())
            {
                success = true;
                return ref databasesMapSnapshot[dbId];
            }

            // Take the database map's write lock so that no new databases can be added with the same ID
            // Note that we don't call TrySetValue because that would only guarantee that the map instance does not change,
            // but the underlying values can still change.
            // So here we're calling TrySetValueUnsafe and handling the locks ourselves.
            databases.mapLock.WriteLock();

            try
            {
                // Check again if database exists in the map, if so return it
                if (dbId >= 0 && dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault())
                {
                    success = true;
                    return ref databasesMapSnapshot[dbId];
                }

                // Create the database and use TrySetValueUnsafe to add it to the map
                var db = CreateDatabaseDelegate(dbId);
                if (!databases.TrySetValueUnsafe(dbId, ref db, false))
                    return ref GarnetDatabase.Empty;
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
            return ref databasesMapSnapshot[dbId];
        }

        /// <inheritdoc/>
        public override bool TryPauseCheckpoints(int dbId)
        {
            ref var db = ref TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return TryPauseCheckpoints(ref db);
        }

        /// <inheritdoc/>
        public override void ResumeCheckpoints(int dbId)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault());

            ResumeCheckpoints(ref databasesMapSnapshot[dbId]);
        }

        /// <inheritdoc/>
        public override ref GarnetDatabase TryGetDatabase(int dbId, out bool found)
        {
            found = false;

            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            if (dbId == 0)
            {
                Debug.Assert(!databasesMapSnapshot[0].IsDefault());
                found = true;
                return ref databasesMapSnapshot[0];
            }

            // Check if database already exists
            if (dbId < databasesMapSize)
            {
                if (!databasesMapSnapshot[dbId].IsDefault())
                {
                    found = true;
                    return ref databasesMapSnapshot[dbId];
                }
            }

            found = false;
            return ref GarnetDatabase.Empty;
        }

        /// <inheritdoc/>
        public override void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            ref var db = ref TryGetOrAddDatabase(dbId, out var success, out _);
            if (!success)
                throw new GarnetException($"Database with ID {dbId} was not found.");

            FlushDatabase(ref db, unsafeTruncateLog);
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
                FlushDatabase(ref databaseMapSnapshot[dbId], unsafeTruncateLog);
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
            if (sizeTrackersStarted)
                db.ObjectStoreSizeTracker.Start(StoreWrapper.ctsCommit.Token);

            activeDbIds.TryGetNextId(out var nextIdx);
            activeDbIds.TrySetValue(nextIdx, ref db.Id);

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
                    var defaultDbCopy = new GarnetDatabase(ref sdbm.DefaultDatabase, enableAof);
                    TryAddDatabase(0, ref defaultDbCopy);
                    return;
                case MultiDatabaseManager mdbm:
                    var activeDbIdsMapSize = mdbm.activeDbIds.ActualSize;
                    var activeDbIdsMapSnapshot = mdbm.activeDbIds.Map;
                    var databasesMapSnapshot = mdbm.databases.Map;

                    for (var i = 0; i < activeDbIdsMapSize; i++)
                    {
                        var dbId = activeDbIdsMapSnapshot[i];
                        var dbCopy = new GarnetDatabase(ref databasesMapSnapshot[dbId], enableAof);
                        TryAddDatabase(dbId, ref dbCopy);
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
        private async Task TakeDatabasesCheckpointAsync(int dbIdsCount, ILogger logger = null,
            CancellationToken token = default)
        {
            Debug.Assert(checkpointTasks != null);
            Debug.Assert(dbIdsCount <= dbIdsToCheckpoint.Length);

            for (var i = 0; i < checkpointTasks.Length; i++)
                checkpointTasks[i] = Task.CompletedTask;

            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return;

            try
            {
                var databaseMapSnapshot = databases.Map;

                var currIdx = 0;
                while (currIdx < dbIdsCount)
                {
                    var dbId = dbIdsToCheckpoint[currIdx];

                    // Prevent parallel checkpoint
                    if (!await TryPauseCheckpointsContinuousAsync(dbId, token))
                        continue;

                    checkpointTasks[currIdx] = TakeCheckpointAsync(databaseMapSnapshot[dbId], logger: logger, token: token).ContinueWith(
                        t =>
                        {
                            try
                            {
                                if (t.IsCompletedSuccessfully)
                                {
                                    var storeTailAddress = t.Result.Item1;
                                    var objectStoreTailAddress = t.Result.Item2;
                                    TryUpdateLastSaveData(dbId, storeTailAddress, objectStoreTailAddress, token);
                                }
                            }
                            finally
                            {
                                ResumeCheckpoints(dbId);
                            }
                        }, TaskContinuationOptions.ExecuteSynchronously);

                    currIdx++;
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
        }

        private bool TryUpdateLastSaveData(int dbId, long? storeTailAddress, long? objectStoreTailAddress, CancellationToken token = default)
        {
            var lockAcquired = TryGetDatabasesContentReadLock(token);
            if (!lockAcquired) return false;

            var databasesMapSnapshot = databases.Map;

            try
            {
                databasesMapSnapshot[dbId].LastSaveTime = DateTimeOffset.UtcNow;
                if (storeTailAddress.HasValue)
                {
                    databasesMapSnapshot[dbId].LastSaveStoreTailAddress = storeTailAddress.Value;

                    if (databasesMapSnapshot[dbId].ObjectStore != null && objectStoreTailAddress.HasValue)
                        databasesMapSnapshot[dbId].LastSaveObjectStoreTailAddress = objectStoreTailAddress.Value;
                }
                return true;
            }
            finally
            {
                databasesContentLock.ReadUnlock();
            }
        }

        private async Task<bool> TryPauseCheckpointsContinuousAsync(int dbId, CancellationToken token = default)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault());

            var checkpointsPaused = TryPauseCheckpoints(ref databasesMapSnapshot[dbId]);

            while (!checkpointsPaused && !token.IsCancellationRequested && !Disposed)
            {
                await Task.Yield();
                checkpointsPaused = TryPauseCheckpoints(ref databasesMapSnapshot[dbId]);
            }

            return checkpointsPaused;
        }

        public override void Dispose()
        {
            if (Disposed) return;

            Disposed = true;

            // Disable changes to databases map and dispose all databases
            databases.mapLock.CloseLock();
            foreach (var db in databases.Map)
                db.Dispose();

            databasesContentLock.CloseLock();
            activeDbIds.mapLock.CloseLock();
        }
    }
}