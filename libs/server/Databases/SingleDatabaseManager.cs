// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
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
        public override ref GarnetDatabase DefaultDatabase => ref defaultDatabase;

        /// <inheritdoc/>
        public override int DatabaseCount => 1;

        GarnetDatabase defaultDatabase;

        public SingleDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate, StoreWrapper storeWrapper, bool createDefaultDatabase = true) :
            base(createDatabaseDelegate, storeWrapper)
        {
            Logger = storeWrapper.loggerFactory?.CreateLogger(nameof(SingleDatabaseManager));

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                defaultDatabase = createDatabaseDelegate(0);
            }
        }

        public SingleDatabaseManager(SingleDatabaseManager src, bool enableAof) : this(src.CreateDatabaseDelegate, src.StoreWrapper, createDefaultDatabase: false)
        {
            CopyDatabases(src, enableAof);
        }

        /// <inheritdoc/>
        public override ref GarnetDatabase TryGetOrAddDatabase(int dbId, out bool success, out bool added)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            success = true;
            added = false;
            return ref DefaultDatabase;
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
                        DefaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
                }
                else
                {
                    RecoverDatabaseCheckpoint(ref DefaultDatabase, out storeVersion, out objectStoreVersion);
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

            return TryPauseCheckpoints(ref DefaultDatabase);
        }

        /// <inheritdoc/>
        public override void ResumeCheckpoints(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            ResumeCheckpoints(ref DefaultDatabase);
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, ILogger logger = null, CancellationToken token = default)
        {
            if (!TryPauseCheckpointsContinuousAsync(DefaultDatabase.Id, token: token).GetAwaiter().GetResult())
                return false;

            var checkpointTask = TakeCheckpointAsync(DefaultDatabase, logger: logger, token: token).ContinueWith(
                t =>
                {
                    try
                    {
                        if (t.IsCompletedSuccessfully)
                        {
                            var storeTailAddress = t.Result.Item1;
                            var objectStoreTailAddress = t.Result.Item2;

                            if (storeTailAddress.HasValue)
                                DefaultDatabase.LastSaveStoreTailAddress = storeTailAddress.Value;
                            if (ObjectStore != null && objectStoreTailAddress.HasValue)
                                DefaultDatabase.LastSaveObjectStoreTailAddress = objectStoreTailAddress.Value;

                            DefaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
                        }
                    }
                    finally
                    {
                        ResumeCheckpoints(DefaultDatabase.Id);
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
            var checkpointsPaused = TryPauseCheckpoints(dbId);

            try
            {
                // If an external task has taken a checkpoint beyond the provided entryTime return
                if (!checkpointsPaused || DefaultDatabase.LastSaveTime > entryTime)
                    return;

                // Necessary to take a checkpoint because the latest checkpoint is before entryTime
                var result = await TakeCheckpointAsync(DefaultDatabase, logger: Logger);

                var storeTailAddress = result.Item1;
                var objectStoreTailAddress = result.Item2;

                if (storeTailAddress.HasValue)
                    DefaultDatabase.LastSaveStoreTailAddress = storeTailAddress.Value;
                if (ObjectStore != null && objectStoreTailAddress.HasValue)
                    DefaultDatabase.LastSaveObjectStoreTailAddress = objectStoreTailAddress.Value;

                DefaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
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

            if (!TryPauseCheckpointsContinuousAsync(DefaultDatabase.Id, token: token).GetAwaiter().GetResult())
                return;

            logger?.LogInformation("Enforcing AOF size limit currentAofSize: {aofSize} >  AofSizeLimit: {aofSizeLimit}",
                aofSize, aofSizeLimit);

            try
            {
                var result = await TakeCheckpointAsync(DefaultDatabase, logger: logger, token: token);

                var storeTailAddress = result.Item1;
                var objectStoreTailAddress = result.Item2;

                if (storeTailAddress.HasValue)
                    DefaultDatabase.LastSaveStoreTailAddress = storeTailAddress.Value;
                if (ObjectStore != null && objectStoreTailAddress.HasValue)
                    DefaultDatabase.LastSaveObjectStoreTailAddress = objectStoreTailAddress.Value;

                DefaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
            }
            finally
            {
                ResumeCheckpoints(DefaultDatabase.Id);
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
        public override void RecoverAOF() => RecoverDatabaseAOF(ref DefaultDatabase);

        /// <inheritdoc/>
        public override long ReplayAOF(long untilAddress = -1)
        {
            if (!StoreWrapper.serverOptions.EnableAOF)
                return -1;

            // When replaying AOF we do not want to write record again to AOF.
            // So initialize local AofProcessor with recordToAof: false.
            var aofProcessor = new AofProcessor(StoreWrapper, recordToAof: false, Logger);

            try
            {
                return ReplayDatabaseAOF(aofProcessor, ref DefaultDatabase, untilAddress);
            }
            finally
            {
                aofProcessor.Dispose();
            }
        }

        /// <inheritdoc/>
        public override void DoCompaction(CancellationToken token = default, ILogger logger = null) => DoCompaction(ref DefaultDatabase);

        public override bool GrowIndexesIfNeeded(CancellationToken token = default) =>
            GrowIndexesIfNeeded(ref DefaultDatabase);

        /// <inheritdoc/>
        public override void StartObjectSizeTrackers(CancellationToken token = default) =>
            ObjectStoreSizeTracker?.Start(token);

        /// <inheritdoc/>
        public override void Reset(int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            ResetDatabase(ref DefaultDatabase);
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

            EnqueueDatabaseCommit(ref DefaultDatabase, entryType, version);
        }

        public override GarnetDatabase[] GetDatabasesSnapshot() => [DefaultDatabase];

        /// <inheritdoc/>
        public override ref GarnetDatabase TryGetDatabase(int dbId, out bool found)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            found = true;
            return ref DefaultDatabase;
        }

        /// <inheritdoc/>
        public override void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            var safeTruncateAof = StoreWrapper.serverOptions.EnableCluster && StoreWrapper.serverOptions.EnableAOF;

            FlushDatabase(ref DefaultDatabase, unsafeTruncateLog, !safeTruncateAof);

            if (safeTruncateAof)
                SafeTruncateAOF(AofEntryType.FlushDb, unsafeTruncateLog);
        }

        /// <inheritdoc/>
        public override void FlushAllDatabases(bool unsafeTruncateLog)
        {
            var safeTruncateAof = StoreWrapper.serverOptions.EnableCluster && StoreWrapper.serverOptions.EnableAOF;

            FlushDatabase(ref DefaultDatabase, unsafeTruncateLog, !safeTruncateAof);

            if (safeTruncateAof)
                SafeTruncateAOF(AofEntryType.FlushAll, unsafeTruncateLog);
        }

        /// <inheritdoc/>
        public override bool TrySwapDatabases(int dbId1, int dbId2, CancellationToken token = default) => false;

        /// <inheritdoc/>
        public override IDatabaseManager Clone(bool enableAof) => new SingleDatabaseManager(this, enableAof);

        /// <inheritdoc/>
        public override FunctionsState CreateFunctionsState(int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return new(AppendOnlyFile, VersionMap, StoreWrapper.customCommandManager, null, ObjectStoreSizeTracker,
                StoreWrapper.GarnetObjectSerializer);
        }

        private void CopyDatabases(SingleDatabaseManager src, bool enableAof)
        {
            DefaultDatabase = new GarnetDatabase(ref src.DefaultDatabase, enableAof);
        }

        private async Task<bool> TryPauseCheckpointsContinuousAsync(int dbId,
            CancellationToken token = default)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            var checkpointsPaused = TryPauseCheckpoints(ref DefaultDatabase);

            while (!checkpointsPaused && !token.IsCancellationRequested && !Disposed)
            {
                await Task.Yield();
                checkpointsPaused = TryPauseCheckpoints(ref DefaultDatabase);
            }

            return checkpointsPaused;
        }

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
                    databaseId = (byte)DefaultDatabase.Id
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