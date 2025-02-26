// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    internal class SingleDatabaseManager : DatabaseManagerBase
    {
        /// <inheritdoc/>
        public override ref GarnetDatabase DefaultDatabase => ref defaultDatabase;

        /// <inheritdoc/>
        public override int DatabaseCount => 1;

        GarnetDatabase defaultDatabase;

        public SingleDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createsDatabaseDelegate, StoreWrapper storeWrapper, bool createDefaultDatabase = true) : 
            base(createsDatabaseDelegate, storeWrapper)
        {
            this.Logger = storeWrapper.loggerFactory?.CreateLogger(nameof(SingleDatabaseManager));

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                defaultDatabase = createsDatabaseDelegate(0, out _, out _);
            }
        }

        public SingleDatabaseManager(SingleDatabaseManager src, bool enableAof) : this(src.CreateDatabaseDelegate, src.StoreWrapper, createDefaultDatabase: false)
        {
            this.CopyDatabases(src, enableAof);
        }

        /// <inheritdoc/>
        public override bool TryGetOrAddDatabase(int dbId, out GarnetDatabase db)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            db = DefaultDatabase;
            return true;
        }

        public override void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null)
        {
            long storeVersion = -1, objectStoreVersion = -1;
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
                Logger?.LogInformation(ex, $"No Hybrid Log found for recovery; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}");
            }
            catch (Exception ex)
            {
                Logger?.LogInformation(ex, $"Error during recovery of store; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}");
                
                if (StoreWrapper.serverOptions.FailOnRecoveryError)
                    throw;
            }
        }

        /// <inheritdoc/>
        public override bool TryPauseCheckpoints(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return TryPauseCheckpoints(ref DefaultDatabase);
        }

        /// <inheritdoc/>
        public override async Task<bool> TryPauseCheckpointsContinuousAsync(int dbId,
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

        /// <inheritdoc/>
        public override void ResumeCheckpoints(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            ResumeCheckpoints(ref DefaultDatabase);
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, StoreType storeType = StoreType.All, ILogger logger = null,
            CancellationToken token = default)
        {
            if (!TryPauseCheckpointsContinuousAsync(DefaultDatabase.Id, token: token).GetAwaiter().GetResult())
                return false;

            var checkpointTask = TakeCheckpointAsync(DefaultDatabase, storeType, logger: logger, token: token).ContinueWith(
                _ =>
                {
                    DefaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
                    ResumeCheckpoints(DefaultDatabase.Id);
                }, TaskContinuationOptions.ExecuteSynchronously).GetAwaiter();

            if (background)
                return true;

            checkpointTask.GetResult();
            return true;
        }

        /// <inheritdoc/>
        public override bool TakeCheckpoint(bool background, int dbId, StoreType storeType = StoreType.All,
            ILogger logger = null,
            CancellationToken token = default)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return TakeCheckpoint(background, storeType, logger, token);
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
                await TakeCheckpointAsync(DefaultDatabase, StoreType.All, logger: Logger);

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

            logger?.LogInformation($"Enforcing AOF size limit currentAofSize: {aofSize} >  AofSizeLimit: {aofSizeLimit}");

            try
            {
                await TakeCheckpointAsync(DefaultDatabase, StoreType.All, logger: logger, token: token);

                DefaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
            }
            finally
            {
                ResumeCheckpoints(DefaultDatabase.Id);
            }
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
        public override void Reset(int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            ResetDatabase(ref DefaultDatabase);
        }

        /// <inheritdoc/>
        public override void EnqueueCommit(bool isMainStore, long version, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            EnqueueDatabaseCommit(ref DefaultDatabase, isMainStore, version);
        }

        /// <inheritdoc/>
        public override bool TryGetDatabase(int dbId, out GarnetDatabase db)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            db = DefaultDatabase;
            return true;
        }

        /// <inheritdoc/>
        public override void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            FlushDatabase(ref DefaultDatabase, unsafeTruncateLog);
        }

        /// <inheritdoc/>
        public override void FlushAllDatabases(bool unsafeTruncateLog) =>
            FlushDatabase(ref DefaultDatabase, unsafeTruncateLog);

        /// <inheritdoc/>
        public override bool TrySwapDatabases(int dbId1, int dbId2) => false;

        /// <inheritdoc/>
        public override IDatabaseManager Clone(bool enableAof) => new SingleDatabaseManager(this, enableAof);

        public override FunctionsState CreateFunctionsState(int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return new(AppendOnlyFile, VersionMap, StoreWrapper.customCommandManager, null, ObjectStoreSizeTracker,
                StoreWrapper.GarnetObjectSerializer);
        }

        protected override ref GarnetDatabase GetDatabaseByRef(int dbId)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return ref DefaultDatabase;
        }

        private void CopyDatabases(SingleDatabaseManager src, bool enableAof)
        {
            this.DefaultDatabase = new GarnetDatabase(ref src.DefaultDatabase, enableAof);
        }

        public override void Dispose()
        {
            if (Disposed) return;

            DefaultDatabase.Dispose();

            Disposed = true;
        }
    }
}
