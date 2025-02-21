// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using static System.Formats.Asn1.AsnWriter;

namespace Garnet.server
{
    internal class SingleDatabaseManager : DatabaseManagerBase
    {
        /// <inheritdoc/>
        public override ref GarnetDatabase DefaultDatabase => ref defaultDatabase;

        /// <inheritdoc/>
        public override int DatabaseCount => 1;

        GarnetDatabase defaultDatabase;

        public SingleDatabaseManager(StoreWrapper.DatabaseCreatorDelegate createsDatabaseDelegate)
        {
            defaultDatabase = createsDatabaseDelegate(0, out _, out _);
        }

        /// <inheritdoc/>
        public override bool TryGetOrAddDatabase(int dbId, out GarnetDatabase db)
        {
            ArgumentOutOfRangeException.ThrowIfNotEqual(dbId, 0);

            db = DefaultDatabase;
            return true;
        }

        public override void RecoverCheckpoint(bool failOnRecoveryError, bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null)
        {
            long storeVersion = -1, objectStoreVersion = -1;
            try
            {
                if (replicaRecover)
                {
                    // Note: Since replicaRecover only pertains to cluster-mode, we can use the default store pointers (since multi-db mode is disabled in cluster-mode)
                    if (metadata.storeIndexToken != default && metadata.storeHlogToken != default)
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
                    RecoverDatabaseCheckpoint(ref DefaultDatabase);
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
