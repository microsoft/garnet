// Copyright (c) Microsoft Corporation.
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
    internal class MultiDatabaseManager : DatabaseManagerBase
    {
        /// <inheritdoc/>
        public override ref GarnetDatabase DefaultDatabase => ref databases.Map[0];

        /// <inheritdoc/>
        public override int DatabaseCount => activeDbIdsLength;

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

        // Reusable array for storing database IDs for checkpointing
        int[] dbIdsToCheckpoint;

        public MultiDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate,
            StoreWrapper storeWrapper, bool createDefaultDatabase = true) : base(createDatabaseDelegate, storeWrapper)
        {
            maxDatabases = storeWrapper.serverOptions.MaxDatabases;
            Logger = storeWrapper.loggerFactory?.CreateLogger(nameof(MultiDatabaseManager));

            // Create default databases map of size 1
            databases = new ExpandableMap<GarnetDatabase>(1, 0, maxDatabases - 1);

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

            long storeVersion = -1, objectStoreVersion = -1;

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
            }
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, StoreType storeType = StoreType.All, ILogger logger = null,
            CancellationToken token = default)
        {
            var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
            if (!lockAcquired) return false;

            try
            {
                var activeDbIdsSize = activeDbIdsLength;
                Array.Copy(activeDbIds, dbIdsToCheckpoint, activeDbIdsSize);

                TakeDatabasesCheckpointAsync(storeType, activeDbIdsSize, logger: logger, token: token).GetAwaiter().GetResult();
            }
            finally
            {
                databasesLock.ReadUnlock();
            }

            return true;
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, int dbId, StoreType storeType = StoreType.All, ILogger logger = null,
            CancellationToken token = default)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault());

            if (!TryPauseCheckpointsContinuousAsync(dbId, token).GetAwaiter().GetResult())
                return false;

            var checkpointTask = TakeCheckpointAsync(databasesMapSnapshot[dbId], storeType, logger: logger, token: token).ContinueWith(
                _ =>
                {
                    TryUpdateLastSaveTimeAsync(dbId, token).GetAwaiter().GetResult();
                    ResumeCheckpoints(dbId);
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
                await TakeCheckpointAsync(databasesMapSnapshot[dbId], StoreType.All, logger: Logger);

                TryUpdateLastSaveTimeAsync(dbId).GetAwaiter().GetResult();
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
            var lockAcquired = await TryGetDatabasesReadLockAsync(token);
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;
                var activeDbIdsSize = activeDbIdsLength;
                var activeDbIdsSnapshot = activeDbIds;

                var dbIdsIdx = 0;
                for (var i = 0; i < activeDbIdsSize; i++)
                {
                    var dbId = activeDbIdsSnapshot[i];
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

                await TakeDatabasesCheckpointAsync(StoreType.All, dbIdsIdx, logger: logger, token: token);
            }
            finally
            {
                databasesLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override async Task CommitToAofAsync(CancellationToken token = default, ILogger logger = null)
        {
            var databasesMapSnapshot = databases.Map;

            var activeDbIdsSize = activeDbIdsLength;
            var activeDbIdsSnapshot = activeDbIds;

            // Take a read lock to make sure that swap-db operation is not in progress
            var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
            if (!lockAcquired) return;

            var aofTasks = new Task<(long, long)>[activeDbIdsSize];

            try
            {
                for (var i = 0; i < activeDbIdsSize; i++)
                {
                    var dbId = activeDbIdsSnapshot[i];
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
                databasesLock.ReadUnlock();
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
            var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsSize = activeDbIdsLength;
                var activeDbIdsSnapshot = activeDbIds;

                var aofTasks = new Task[activeDbIdsSize];

                for (var i = 0; i < activeDbIdsSize; i++)
                {
                    var dbId = activeDbIdsSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(!db.IsDefault());

                    aofTasks[i] = db.AppendOnlyFile.WaitForCommitAsync(token: token).AsTask();
                }

                await Task.WhenAll(aofTasks);
            }
            finally
            {
                databasesLock.ReadUnlock();
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

                var activeDbIdsSize = activeDbIdsLength;
                var activeDbIdsSnapshot = activeDbIds;

                for (var i = 0; i < activeDbIdsSize; i++)
                {
                    var dbId = activeDbIdsSnapshot[i];
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
            var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsSize = activeDbIdsLength;
                var activeDbIdsSnapshot = activeDbIds;

                var exThrown = false;
                for (var i = 0; i < activeDbIdsSize; i++)
                {
                    var dbId = activeDbIdsSnapshot[i];
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
                databasesLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override bool GrowIndexesIfNeeded(CancellationToken token = default)
        {
            var allIndexesMaxedOut = true;

            var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
            if (!lockAcquired) return false;

            try
            {
                var activeDbIdsSize = activeDbIdsLength;
                var activeDbIdsSnapshot = activeDbIds;

                var databasesMapSnapshot = databases.Map;

                for (var i = 0; i < activeDbIdsSize; i++)
                {
                    var dbId = activeDbIdsSnapshot[i];

                    var indexesMaxedOut = GrowIndexesIfNeeded(ref databasesMapSnapshot[dbId]);
                    if (allIndexesMaxedOut && !indexesMaxedOut)
                        allIndexesMaxedOut = false;
                }

                return allIndexesMaxedOut;
            }
            finally
            {
                databasesLock.ReadUnlock();
            }
        }

        /// <inheritdoc/>
        public override void StartObjectSizeTrackers(CancellationToken token = default)
        {
            var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
            if (!lockAcquired) return;

            try
            {
                var databasesMapSnapshot = databases.Map;

                var activeDbIdsSize = activeDbIdsLength;
                var activeDbIdsSnapshot = activeDbIds;

                for (var i = 0; i < activeDbIdsSize; i++)
                {
                    var dbId = activeDbIdsSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    Debug.Assert(!db.IsDefault());

                    db.ObjectStoreSizeTracker?.Start(token);
                }
            }
            finally
            {
                databasesLock.ReadUnlock();
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
            var activeDbIdsSize = activeDbIdsLength;
            var activeDbIdsSnapshot = activeDbIds;
            var databaseMapSnapshot = databases.Map;

            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
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
            var activeDbIdsSize = activeDbIdsLength;
            var activeDbIdsSnapshot = activeDbIds;
            var databaseMapSnapshot = databases.Map;
            var databasesSnapshot = new GarnetDatabase[activeDbIdsSize];

            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                databasesSnapshot[i] = databaseMapSnapshot[dbId];
            }

            return databasesSnapshot;
        }

        /// <inheritdoc/>
        public override bool TrySwapDatabases(int dbId1, int dbId2)
        {
            if (dbId1 == dbId2) return true;

            ref var db1 = ref TryGetOrAddDatabase(dbId1, out var success, out _);
            if (!success)
                return false;

            ref var db2 = ref TryGetOrAddDatabase(dbId2, out success, out _);
            if (!success)
                return false;

            databasesLock.WriteLock();
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
                databasesLock.WriteUnlock();
            }

            return true;
        }

        /// <inheritdoc/>
        public override IDatabaseManager Clone(bool enableAof) => new MultiDatabaseManager(this, enableAof);

        protected virtual ref GarnetDatabase GetDatabaseByRef(int dbId = 0)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            Debug.Assert(dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault());

            return ref databasesMapSnapshot[dbId];
        }

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

            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            if (dbId >= 0 && dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault())
            {
                success = true;
                return ref databasesMapSnapshot[dbId];
            }

            databases.mapLock.WriteLock();

            try
            {
                if (dbId >= 0 && dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault())
                {
                    success = true;
                    return ref databasesMapSnapshot[dbId];
                }

                var db = CreateDatabaseDelegate(dbId);
                if (!databases.TrySetValueUnsafe(dbId, ref db, false))
                    return ref GarnetDatabase.Empty;
            }
            finally
            {
                databases.mapLock.WriteUnlock();
            }

            added = true;
            success = true;

            HandleDatabaseAdded(dbId);

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
        public override async Task<bool> TryPauseCheckpointsContinuousAsync(int dbId, CancellationToken token = default)
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
            var activeDbIdsSize = activeDbIdsLength;
            var activeDbIdsSnapshot = activeDbIds;
            var databaseMapSnapshot = databases.Map;

            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                FlushDatabase(ref databaseMapSnapshot[dbId], unsafeTruncateLog);
            }
        }

        /// <summary>
        /// Continuously try to take a database read lock
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if lock acquired</returns>
        public async Task<bool> TryGetDatabasesReadLockAsync(CancellationToken token = default)
        {
            var lockAcquired = databasesLock.TryReadLock();

            while (!lockAcquired && !token.IsCancellationRequested && !Disposed)
            {
                await Task.Yield();
                lockAcquired = databasesLock.TryReadLock();
            }

            return lockAcquired;
        }

        /// <summary>
        /// Continuously try to take a database write lock
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>True if lock acquired</returns>
        public async Task<bool> TryGetDatabasesWriteLockAsync(CancellationToken token = default)
        {
            var lockAcquired = databasesLock.TryWriteLock();

            while (!lockAcquired && !token.IsCancellationRequested && !Disposed)
            {
                await Task.Yield();
                lockAcquired = databasesLock.TryWriteLock();
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

            if (!TryGetDatabasesWriteLockAsync().GetAwaiter().GetResult()) return;

            try
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
                dbIdsToCheckpoint = new int[activeDbIdsLength];
            }
            finally
            {
                databasesLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Copy active databases from specified IDatabaseManager instance
        /// </summary>
        /// <param name="src">Source IDatabaseManager</param>
        /// <param name="enableAof">True if should enable AOF in copied databases</param>
        private void CopyDatabases(IDatabaseManager src, bool enableAof)
        {
            switch (src)
            {
                case SingleDatabaseManager sdbm:
                    var defaultDbCopy = new GarnetDatabase(ref sdbm.DefaultDatabase, enableAof);
                    TryAddDatabase(0, ref defaultDbCopy);
                    return;
                case MultiDatabaseManager mdbm:
                    var activeDbIdsSize = mdbm.activeDbIdsLength;
                    var activeDbIdsSnapshot = mdbm.activeDbIds;
                    var databasesMapSnapshot = mdbm.databases.Map;

                    for (var i = 0; i < activeDbIdsSize; i++)
                    {
                        var dbId = activeDbIdsSnapshot[i];
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
        /// <param name="storeType">Store type to checkpoint</param>
        /// <param name="dbIdsCount">Number of databases to checkpoint (first dbIdsCount indexes from dbIdsToCheckpoint)</param>
        /// <param name="logger">Logger</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>False if checkpointing already in progress</returns>
        private async Task TakeDatabasesCheckpointAsync(StoreType storeType, int dbIdsCount, ILogger logger = null,
            CancellationToken token = default)
        {
            Debug.Assert(checkpointTasks != null);
            Debug.Assert(dbIdsCount <= dbIdsToCheckpoint.Length);

            for (var i = 0; i < checkpointTasks.Length; i++)
                checkpointTasks[i] = Task.CompletedTask;

            var lockAcquired = await TryGetDatabasesReadLockAsync(token);
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

                    checkpointTasks[currIdx] = TakeCheckpointAsync(databaseMapSnapshot[dbId], storeType, logger: logger, token: token).ContinueWith(
                        _ =>
                        {
                            TryUpdateLastSaveTimeAsync(dbId, token).GetAwaiter().GetResult();
                            ResumeCheckpoints(dbId);
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
                databasesLock.ReadUnlock();
            }
        }

        private async Task<bool> TryUpdateLastSaveTimeAsync(int dbId, CancellationToken token = default)
        {
            var lockAcquired = await TryGetDatabasesReadLockAsync(token);
            if (!lockAcquired) return false;

            var databasesMapSnapshot = databases.Map;

            try
            {
                databasesMapSnapshot[dbId].LastSaveTime = DateTimeOffset.UtcNow;
                return true;
            }
            finally
            {
                databasesLock.ReadUnlock();
            }
        }

        public override void Dispose()
        {
            if (Disposed) return;

            cts.Cancel();
            Disposed = true;

            // Disable changes to databases map and dispose all databases
            databases.mapLock.WriteLock();
            foreach (var db in databases.Map)
                db.Dispose();

            cts.Dispose();
        }
    }
}