// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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

        public SingleDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createsDatabaseDelegate, StoreWrapper storeWrapper, ILoggerFactory loggerFactory = null) : 
            base(storeWrapper)
        {
            this.Logger = loggerFactory?.CreateLogger(nameof(SingleDatabaseManager));
            defaultDatabase = createsDatabaseDelegate(0, out _, out _);
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
        public override void RecoverAOF() => RecoverDatabaseAOF(ref DefaultDatabase);

        /// <inheritdoc/>
        public override long ReplayAOF(long untilAddress = -1)
        {
            if (!StoreWrapper.serverOptions.EnableAOF)
                return -1;

            long replicationOffset = 0;
            try
            {
                // When replaying AOF we do not want to write record again to AOF.
                // So initialize local AofProcessor with recordToAof: false.
                var aofProcessor = new AofProcessor(StoreWrapper, recordToAof: false, Logger);

                aofProcessor.Recover(0, untilAddress);
                DefaultDatabase.LastSaveTime = DateTimeOffset.UtcNow;
                replicationOffset = aofProcessor.ReplicationOffset;

                aofProcessor.Dispose();
            }
            catch (Exception ex)
            {
                Logger?.LogError(ex, "Error during recovery of AofProcessor");
                if (StoreWrapper.serverOptions.FailOnRecoveryError)
                    throw;
            }

            return replicationOffset;
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

        public override void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            FlushDatabase(ref DefaultDatabase, unsafeTruncateLog);
        }

        public override void FlushAllDatabases(bool unsafeTruncateLog) =>
            FlushDatabase(ref DefaultDatabase, unsafeTruncateLog);

        public override FunctionsState CreateFunctionsState(CustomCommandManager customCommandManager, GarnetObjectSerializer garnetObjectSerializer, int dbId = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            return new(AppendOnlyFile, VersionMap, customCommandManager, null, ObjectStoreSizeTracker, garnetObjectSerializer);
        }

        public override void Dispose()
        {
            if (Disposed) return;

            DefaultDatabase.Dispose();

            Disposed = true;
        }
    }
}
