// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.Metrics;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Single logical database management
    /// </summary>
    internal class SingleDatabaseManager : DatabaseManagerBase
    {
        /// <inheritdoc/>
        public override GarnetDatabase DefaultDatabase => defaultDatabase;

        /// <inheritdoc/>
        public override int DatabaseCount => 1;

        public override int MaxDatabaseId => 0;

        readonly GarnetDatabase defaultDatabase;

        readonly StoreWrapper storeWrapper;


        public SingleDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate, StoreWrapper storeWrapper, bool createDefaultDatabase = true) :
            base(createDatabaseDelegate, storeWrapper)
        {
            Logger = storeWrapper.loggerFactory?.CreateLogger(nameof(SingleDatabaseManager));
            this.storeWrapper = storeWrapper;

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                defaultDatabase = createDatabaseDelegate(0);
            }
        }

        public SingleDatabaseManager(SingleDatabaseManager src, bool enableAof) : this(src.CreateDatabaseDelegate, src.StoreWrapper, createDefaultDatabase: false)
        {
            defaultDatabase = new GarnetDatabase(0, src.DefaultDatabase, enableAof);
        }

        /// <inheritdoc/>
        public override GarnetDatabase TryGetOrAddDatabase(int dbId, out bool success, out bool added)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            success = true;
            added = false;
            return defaultDatabase;
        }

        /// <inheritdoc/>
        public override void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null)
        {
            long storeVersion = 0, objectStoreVersion = 0;
            try
            {
                if (replicaRecover)
                {
                    // Note: Since replicaRecover only pertains to cluster-mode, we can use the default store pointers (since multi-db mode is disabled in cluster-mode)
                    if (metadata!.storeIndexToken != default && metadata.storeHlogToken != default)
                    {
                        storeVersion = !recoverMainStoreFromToken ? MainStore.Recover() : MainStore.Recover(metadata.storeIndexToken, metadata.storeHlogToken);
                    }

                    if (ObjectStore != null)
                    {
                        if (metadata.objectStoreIndexToken != default && metadata.objectStoreHlogToken != default)
                        {
                            objectStoreVersion = !recoverObjectStoreFromToken ? ObjectStore.Recover() : ObjectStore.Recover(metadata.objectStoreIndexToken, metadata.objectStoreHlogToken);
                        }
                    }

                    if (storeVersion > 0 || objectStoreVersion > 0)
                        defaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
                }
                else
                {
                    RecoverDatabaseCheckpoint(defaultDatabase, out storeVersion, out objectStoreVersion);
                }
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
            if (ObjectStore != null && storeVersion != objectStoreVersion)
            {
                Logger?.LogInformation("Main store and object store checkpoint versions do not match; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
                if (StoreWrapper.serverOptions.FailOnRecoveryError)
                    throw new GarnetException("Main store and object store checkpoint versions do not match");
            }
        }

        /// <inheritdoc/>
        public override bool TryPauseCheckpoints(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return TryPauseCheckpoints(defaultDatabase);
        }

        /// <inheritdoc/>
        public override void ResumeCheckpoints(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            ResumeCheckpoints(defaultDatabase);
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, ILogger logger = null, CancellationToken token = default)
        {
            // Check if checkpoint already in progress
            if (!TryPauseCheckpoints(defaultDatabase.Id))
                return false;

            var checkpointTask = TakeCheckpointAsync(defaultDatabase, logger: logger, token: token).ContinueWith(
                t =>
                {
                    try
                    {
                        if (t.IsCompletedSuccessfully)
                        {
                            var storeTailAddress = t.Result.Item1;
                            var objectStoreTailAddress = t.Result.Item2;

                            if (storeTailAddress.HasValue)
                                defaultDatabase.LastSaveStoreTailAddress = storeTailAddress.Value;
                            if (ObjectStore != null && objectStoreTailAddress.HasValue)
                                defaultDatabase.LastSaveObjectStoreTailAddress = objectStoreTailAddress.Value;

                            defaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
                        }
                    }
                    finally
                    {
                        ResumeCheckpoints(defaultDatabase.Id);
                    }
                }, TaskContinuationOptions.ExecuteSynchronously).GetAwaiter();

            if (background)
                return true;

            checkpointTask.GetResult();
            return true;
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, int dbId, ILogger logger = null, CancellationToken token = default)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return TakeCheckpoint(background, logger, token);
        }

        /// <inheritdoc/>
        public override async Task TakeOnDemandCheckpointAsync(DateTimeOffset entryTime, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            // Take lock to ensure no other task will be taking a checkpoint
            while (!TryPauseCheckpoints(dbId))
                await Task.Yield();

            try
            {
                // If an external task has taken a checkpoint beyond the provided entryTime return
                if (defaultDatabase.LastSaveTime > entryTime)
                    return;

                // Necessary to take a checkpoint because the latest checkpoint is before entryTime
                var result = await TakeCheckpointAsync(defaultDatabase, logger: Logger);

                var storeTailAddress = result.Item1;
                var objectStoreTailAddress = result.Item2;

                if (storeTailAddress.HasValue)
                    defaultDatabase.LastSaveStoreTailAddress = storeTailAddress.Value;
                if (ObjectStore != null && objectStoreTailAddress.HasValue)
                    defaultDatabase.LastSaveObjectStoreTailAddress = objectStoreTailAddress.Value;

                defaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
            }
            finally
            {
                ResumeCheckpoints(dbId);
            }
        }

        /// <inheritdoc/>
        public override async Task TaskCheckpointBasedOnAofSizeLimitAsync(long aofSizeLimit,
            CancellationToken token = default, ILogger logger = null)
        {
            var aofSize = AppendOnlyFile.TailAddress - AppendOnlyFile.BeginAddress;
            if (aofSize <= aofSizeLimit) return;

            if (!TryPauseCheckpointsContinuousAsync(defaultDatabase.Id, token: token).GetAwaiter().GetResult())
                return;

            try
            {
                // Checkpoint will be triggered from AOF replay
                if (storeWrapper.clusterProvider.IsReplica())
                {
                    logger?.LogInformation("Replica skipping {method}", nameof(TaskCheckpointBasedOnAofSizeLimitAsync));
                    return;
                }

                logger?.LogInformation("Enforcing AOF size limit currentAofSize: {aofSize} >  AofSizeLimit: {aofSizeLimit}",
                    aofSize, aofSizeLimit);

                var result = await TakeCheckpointAsync(defaultDatabase, logger: logger, token: token);

                var storeTailAddress = result.Item1;
                var objectStoreTailAddress = result.Item2;

                if (storeTailAddress.HasValue)
                    defaultDatabase.LastSaveStoreTailAddress = storeTailAddress.Value;
                if (ObjectStore != null && objectStoreTailAddress.HasValue)
                    defaultDatabase.LastSaveObjectStoreTailAddress = objectStoreTailAddress.Value;

                defaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
            }
            finally
            {
                ResumeCheckpoints(defaultDatabase.Id);
            }
        }

        /// <inheritdoc/>
        public override async Task CommitToAofAsync(CancellationToken token = default, ILogger logger = null)
        {
            try
            {
                await AppendOnlyFile.CommitAsync(token: token);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex,
                    "Exception raised while committing to AOF. AOF tail address = {tailAddress}; AOF committed until address = {commitAddress}; ",
                    AppendOnlyFile.TailAddress, AppendOnlyFile.CommittedUntilAddress);
                throw;
            }
        }

        /// <inheritdoc/>
        public override async Task CommitToAofAsync(int dbId, CancellationToken token = default, ILogger logger = null)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            await CommitToAofAsync(token, logger);
        }

        /// <inheritdoc/>
        public override async Task WaitForCommitToAofAsync(CancellationToken token = default, ILogger logger = null)
        {
            await AppendOnlyFile.WaitForCommitAsync(token: token);
        }

        /// <inheritdoc/>
        public override void RecoverAOF() => RecoverDatabaseAOF(defaultDatabase);

        /// <inheritdoc/>
        public override long ReplayAOF(long untilAddress = -1)
        {
            if (!StoreWrapper.serverOptions.EnableAOF)
                return -1;

            // When replaying AOF we do not want to write record again to AOF.
            // So initialize local AofProcessor with recordToAof: false.
            var aofProcessor = new AofProcessor(StoreWrapper, recordToAof: false, logger: Logger);

            try
            {
                return ReplayDatabaseAOF(aofProcessor, defaultDatabase, untilAddress);
            }
            finally
            {
                aofProcessor.Dispose();
            }
        }

        /// <inheritdoc/>
        public override void DoCompaction(CancellationToken token = default, ILogger logger = null) => DoCompaction(defaultDatabase);

        /// <inheritdoc/>
        public override bool GrowIndexesIfNeeded(CancellationToken token = default) =>
            GrowIndexesIfNeeded(defaultDatabase);

        /// <inheritdoc/>
        public override void ExecuteObjectCollection() =>
            ExecuteObjectCollection(defaultDatabase, Logger);

        /// <inheritdoc/>
        public override void ExpiredKeyDeletionScan() =>
            ExpiredKeyDeletionScan(defaultDatabase);

        /// <inheritdoc/>
        public override void StartObjectSizeTrackers(CancellationToken token = default) =>
            ObjectStoreSizeTracker?.Start(token);

        /// <inheritdoc/>
        public override void Reset(int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            ResetDatabase(defaultDatabase);
        }

        /// <inheritdoc/>
        public override void ResetRevivificationStats()
        {
            MainStore.ResetRevivificationStats();
            ObjectStore?.ResetRevivificationStats();
        }

        /// <inheritdoc/>
        public override void EnqueueCommit(AofEntryType entryType, long version, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            EnqueueDatabaseCommit(defaultDatabase, entryType, version);
        }

        public override GarnetDatabase[] GetDatabasesSnapshot() => [defaultDatabase];

        /// <inheritdoc/>
        public override GarnetDatabase TryGetDatabase(int dbId, out bool found)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            found = true;
            return defaultDatabase;
        }

        /// <inheritdoc/>
        public override void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            var safeTruncateAof = StoreWrapper.serverOptions.EnableCluster && StoreWrapper.serverOptions.EnableAOF;

            FlushDatabase(defaultDatabase, unsafeTruncateLog, !safeTruncateAof);

            if (safeTruncateAof)
                SafeTruncateAOF(AofEntryType.FlushDb, unsafeTruncateLog);
        }

        /// <inheritdoc/>
        public override void FlushAllDatabases(bool unsafeTruncateLog)
        {
            var safeTruncateAof = StoreWrapper.serverOptions.EnableCluster && StoreWrapper.serverOptions.EnableAOF;

            FlushDatabase(defaultDatabase, unsafeTruncateLog, !safeTruncateAof);

            if (safeTruncateAof)
                SafeTruncateAOF(AofEntryType.FlushAll, unsafeTruncateLog);
        }

        /// <inheritdoc/>
        public override bool TrySwapDatabases(int dbId1, int dbId2, CancellationToken token = default) => false;

        /// <inheritdoc/>
        public override IDatabaseManager Clone(bool enableAof) => new SingleDatabaseManager(this, enableAof);

        /// <inheritdoc/>
        public override FunctionsState CreateFunctionsState(int dbId = 0, byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return new(AppendOnlyFile, VersionMap, StoreWrapper.customCommandManager, null, ObjectStoreSizeTracker,
                StoreWrapper.GarnetObjectSerializer, respProtocolVersion);
        }

        private async Task<bool> TryPauseCheckpointsContinuousAsync(int dbId,
            CancellationToken token = default)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            var checkpointsPaused = TryPauseCheckpoints(defaultDatabase);

            while (!checkpointsPaused && !token.IsCancellationRequested && !Disposed)
            {
                await Task.Yield();
                checkpointsPaused = TryPauseCheckpoints(defaultDatabase);
            }

            return checkpointsPaused;
        }

        public override (long numExpiredKeysFound, long totalRecordsScanned) ExpiredKeyDeletionScan(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);
            var (k1, t1) = MainStoreExpiredKeyDeletionScan(DefaultDatabase);
            var (k2, t2) = StoreWrapper.serverOptions.DisableObjects ? (0, 0) : ObjectStoreExpiredKeyDeletionScan(DefaultDatabase);
            return (k1 + k2, t1 + t2);
        }

        public override (HybridLogScanMetrics mainStore, HybridLogScanMetrics objectStore)[] CollectHybridLogStats() => [CollectHybridLogStatsForDb(defaultDatabase)];

        private void SafeTruncateAOF(AofEntryType entryType, bool unsafeTruncateLog)
        {
            StoreWrapper.clusterProvider.SafeTruncateAOF(AppendOnlyFile.TailAddress);
            if (StoreWrapper.clusterProvider.IsPrimary())
            {
                AofHeader header = new()
                {
                    opType = entryType,
                    storeVersion = 0,
                    sessionID = -1,
                    unsafeTruncateLog = unsafeTruncateLog ? (byte)0 : (byte)1,
                    databaseId = (byte)defaultDatabase.Id
                };
                AppendOnlyFile?.Enqueue(header, out _);
            }
        }

        public override void Dispose()
        {
            if (Disposed) return;

            DefaultDatabase.Dispose();

            Disposed = true;
        }
    }
}