// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        int[] dbIdsToCheckpoint = null;

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

        public MultiDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createsDatabaseDelegate,
            StoreWrapper storeWrapper, bool createDefaultDatabase = true) : base(createsDatabaseDelegate, storeWrapper)
        {
            this.maxDatabases = storeWrapper.serverOptions.MaxDatabases;
            this.Logger = storeWrapper.loggerFactory?.CreateLogger(nameof(MultiDatabaseManager));

            // Create default databases map of size 1
            databases = new ExpandableMap<GarnetDatabase>(1, 0, this.maxDatabases - 1);

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                var db = CreateDatabaseDelegate(0, out var storeCheckpointDir, out var aofDir);

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

        public MultiDatabaseManager(MultiDatabaseManager src, bool enableAof) : this(src.CreateDatabaseDelegate,
            src.StoreWrapper, createDefaultDatabase: false)
        {
            this.CopyDatabases(src, enableAof);
        }

        /// <inheritdoc/>
        public override void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null)
        {
            if (replicaRecover)
                throw new GarnetException(
                    $"Unexpected call to {nameof(MultiDatabaseManager)}.{nameof(RecoverCheckpoint)} with {nameof(replicaRecover)} == true.");

            int[] dbIdsToRecover;
            try
            {
                if (!TryGetSavedDatabaseIds(checkpointParentDir, checkpointDirBaseName, out dbIdsToRecover))
                    return;

            }
            catch (Exception ex)
            {
                Logger?.LogInformation(ex, $"Error during recovery of database ids; checkpointParentDir = {checkpointParentDir}; checkpointDirBaseName = {checkpointDirBaseName}");
                if (StoreWrapper.serverOptions.FailOnRecoveryError)
                    throw;
                return;
            }

            long storeVersion = -1, objectStoreVersion = -1;
            
            foreach (var dbId in dbIdsToRecover)
            {
                if (!TryGetOrAddDatabase(dbId, out var db))
                    throw new GarnetException($"Failed to retrieve or create database for checkpoint recovery (DB ID = {dbId}).");

                try
                {
                    RecoverDatabaseCheckpoint(ref db, out storeVersion, out objectStoreVersion);
                }
                catch (TsavoriteNoHybridLogException ex)
                {
                    // No hybrid log being found is not the same as an error in recovery. e.g. fresh start
                    Logger?.LogInformation(ex, $"No Hybrid Log found for recovery; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
                }
                catch (Exception ex)
                {
                    Logger?.LogInformation(ex, $"Error during recovery of store; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
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
                    databasesMapSnapshot[dbId].LastSaveTime = DateTimeOffset.UtcNow;
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

                databasesMapSnapshot[dbId].LastSaveTime = DateTimeOffset.UtcNow;
            }
            finally
            {
                ResumeCheckpoints(dbId);
            }
        }

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
                        logger?.LogInformation($"Enforcing AOF size limit currentAofSize: {dbAofSize} > AofSizeLimit: {aofSizeLimit} (Database ID: {dbId})");
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
        public override void RecoverAOF()
        {
            int[] dbIdsToRecover;
            try
            {
                if (!TryGetSavedDatabaseIds(aofParentDir, aofDirBaseName, out dbIdsToRecover))
                    return;

            }
            catch (Exception ex)
            {
                Logger?.LogInformation(ex, $"Error during recovery of database ids; aofParentDir = {aofParentDir}; aofDirBaseName = {aofDirBaseName}");
                return;
            }

            foreach (var dbId in dbIdsToRecover)
            {
                if (!TryGetOrAddDatabase(dbId, out var db))
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
        public override void Reset(int dbId = 0)
        {
            if (!this.TryGetOrAddDatabase(dbId, out var db))
                throw new GarnetException($"Database with ID {dbId} was not found.");

            ResetDatabase(ref db);
        }

        public override void EnqueueCommit(bool isMainStore, long version, int dbId = 0)
        {
            if (!this.TryGetOrAddDatabase(dbId, out var db))
                throw new GarnetException($"Database with ID {dbId} was not found.");

            EnqueueDatabaseCommit(ref db, isMainStore, version);
        }

        /// <inheritdoc/>
        public override bool TrySwapDatabases(int dbId1, int dbId2)
        {
            if (!this.TryGetOrAddDatabase(dbId1, out var db1) ||
                !this.TryGetOrAddDatabase(dbId2, out var db2))
                return false;

            databasesLock.WriteLock();
            try
            {
                var databaseMapSnapshot = this.databases.Map;
                databaseMapSnapshot[dbId2] = db1;
                databaseMapSnapshot[dbId1] = db2;
            }
            finally
            {
                databasesLock.WriteUnlock();
            }

            return true;
        }

        /// <inheritdoc/>
        public override IDatabaseManager Clone(bool enableAof) => new MultiDatabaseManager(this, enableAof);

        public override FunctionsState CreateFunctionsState(int dbId = 0)
        {
            if (!this.TryGetOrAddDatabase(dbId, out var db))
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return new(db.AppendOnlyFile, db.VersionMap, StoreWrapper.customCommandManager, null, db.ObjectStoreSizeTracker,
                StoreWrapper.GarnetObjectSerializer);
        }

        protected override ref GarnetDatabase GetDatabaseByRef(int dbId)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            Debug.Assert(dbId < databasesMapSize);
            return ref databasesMapSnapshot[dbId];
        }

        /// <inheritdoc/>
        public override bool TryGetOrAddDatabase(int dbId, out GarnetDatabase db)
        {
            if (!databases.TryGetOrSet(dbId, () => CreateDatabaseDelegate(dbId, out _, out _), out db, out var added))
                return false;

            if (added)
                HandleDatabaseAdded(dbId);

            return true;
        }

        /// <inheritdoc/>
        public override bool TryPauseCheckpoints(int dbId = 0)
        {
            if (!this.TryGetOrAddDatabase(dbId, out var db))
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return TryPauseCheckpoints(ref db);
        }

        /// <inheritdoc/>
        public override async Task<bool> TryPauseCheckpointsContinuousAsync(int dbId = 0,
            CancellationToken token = default)
        {
            if (!this.TryGetOrAddDatabase(dbId, out var db))
                throw new GarnetException($"Database with ID {dbId} was not found.");

            var checkpointsPaused = TryPauseCheckpoints(ref db);

            while (!checkpointsPaused && !token.IsCancellationRequested && !Disposed)
            {
                await Task.Yield();
                checkpointsPaused = TryPauseCheckpoints(ref db);
            }

            return checkpointsPaused;
        }

        /// <inheritdoc/>
        public override void ResumeCheckpoints(int dbId = 0)
        {
            if (!this.TryGetOrAddDatabase(dbId, out var db))
                throw new GarnetException($"Database with ID {dbId} was not found.");

            ResumeCheckpoints(ref db);
        }

        /// <inheritdoc/>
        public override bool TryGetDatabase(int dbId, out GarnetDatabase db)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            if (dbId == 0)
            {
                db = databasesMapSnapshot[0];
                Debug.Assert(!db.IsDefault());
                return true;
            }

            // Check if database already exists
            if (dbId < databasesMapSize)
            {
                db = databasesMapSnapshot[dbId];
                if (!db.IsDefault()) return true;
            }

            // Try to retrieve or add database
            return this.TryGetOrAddDatabase(dbId, out db);
        }

        /// <inheritdoc/>
        public override void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            if (!this.TryGetOrAddDatabase(dbId, out var db))
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
                this.FlushDatabase(ref databaseMapSnapshot[dbId], unsafeTruncateLog);
            }
        }

        /// <summary>
        /// Continuously try to take a database read lock that ensures no db swap operations occur
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
                dbIdsToCheckpoint = new int[activeDbIdsLength];
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
                    this.TryAddDatabase(0, ref defaultDbCopy);
                    return;
                case MultiDatabaseManager mdbm:
                    var activeDbIdsSize = mdbm.activeDbIdsLength;
                    var activeDbIdsSnapshot = mdbm.activeDbIds;
                    var databasesMapSnapshot = mdbm.databases.Map;

                    for (var i = 0; i < activeDbIdsSize; i++)
                    {
                        var dbId = activeDbIdsSnapshot[i];
                        var dbCopy = new GarnetDatabase(ref databasesMapSnapshot[dbId], enableAof);
                        this.TryAddDatabase(dbId, ref dbCopy);
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
                        databaseMapSnapshot[dbId].LastSaveTime = DateTimeOffset.UtcNow;
                        ResumeCheckpoints(dbId);
                    }, TaskContinuationOptions.ExecuteSynchronously);

                currIdx++;
            }

            try
            {
                await Task.WhenAll(checkpointTasks);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"Checkpointing threw exception");
            }
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
