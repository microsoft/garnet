// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    internal abstract class DatabaseManagerBase : IDatabaseManager
    {
        /// <summary>
        /// Reference to default database (DB 0)
        /// </summary>
        public abstract ref GarnetDatabase DefaultDatabase { get; }

        /// <inheritdoc/>
        public TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> MainStore => DefaultDatabase.MainStore;

        /// <inheritdoc/>
        public TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStore => DefaultDatabase.ObjectStore;

        /// <inheritdoc/>
        public TsavoriteLog AppendOnlyFile => DefaultDatabase.AppendOnlyFile;

        /// <inheritdoc/>
        public DateTimeOffset LastSaveTime => DefaultDatabase.LastSaveTime;

        /// <inheritdoc/>
        public CacheSizeTracker ObjectStoreSizeTracker => DefaultDatabase.ObjectStoreSizeTracker;

        /// <inheritdoc/>
        public WatchVersionMap VersionMap => DefaultDatabase.VersionMap;

        /// <inheritdoc/>
        public abstract int DatabaseCount { get; }

        /// <inheritdoc/>
        public abstract bool TryGetOrAddDatabase(int dbId, out GarnetDatabase db);

        public abstract void RecoverCheckpoint(bool failOnRecoveryError, bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null);

        public abstract FunctionsState CreateFunctionsState(CustomCommandManager customCommandManager, GarnetObjectSerializer garnetObjectSerializer, int dbId = 0);

        protected bool Disposed;

        protected void RecoverDatabaseCheckpoint(ref GarnetDatabase db)
        {
            var storeVersion = db.MainStore.Recover();
            long objectStoreVersion = -1;
            if (db.ObjectStore != null)
                objectStoreVersion = db.ObjectStore.Recover();

            if (storeVersion > 0 || objectStoreVersion > 0)
            {
                db.LastSaveTime = DateTimeOffset.UtcNow;
            }
        }

        private void RecoverDatabaseAOF(ref GarnetDatabase db)
        {
            if (db.AppendOnlyFile == null) return;

            db.AppendOnlyFile.Recover();
            logger?.LogInformation("Recovered AOF: begin address = {beginAddress}, tail address = {tailAddress}", db.AppendOnlyFile.BeginAddress, db.AppendOnlyFile.TailAddress);
        }

        public abstract void Dispose();
    }
}
