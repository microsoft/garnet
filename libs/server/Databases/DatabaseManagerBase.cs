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
        /// <inheritdoc/>
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

        /// <summary>
        /// The main logger instance associated with the database manager.
        /// </summary>
        protected ILogger Logger;

        /// <summary>
        /// Store Wrapper
        /// </summary>
        public readonly StoreWrapper StoreWrapper;

        /// <inheritdoc/>
        public abstract bool TryGetOrAddDatabase(int dbId, out GarnetDatabase db);

        public abstract void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null);

        /// <inheritdoc/>
        public abstract void RecoverAOF();

        /// <inheritdoc/>
        public abstract long ReplayAOF(long untilAddress = -1);

        /// <inheritdoc/>
        public abstract void Reset(int dbId = 0);

        /// <inheritdoc/>
        public abstract void EnqueueCommit(bool isMainStore, long version, int dbId = 0);

        /// <inheritdoc/>
        public abstract bool TryGetDatabase(int dbId, out GarnetDatabase db);

        /// <inheritdoc/>
        public abstract void FlushDatabase(bool unsafeTruncateLog, int dbId = 0);

        /// <inheritdoc/>
        public abstract void FlushAllDatabases(bool unsafeTruncateLog);

        public abstract FunctionsState CreateFunctionsState(CustomCommandManager customCommandManager, GarnetObjectSerializer garnetObjectSerializer, int dbId = 0);

        protected bool Disposed;

        protected DatabaseManagerBase(StoreWrapper storeWrapper)
        {
            this.StoreWrapper = storeWrapper;
        }

        protected void RecoverDatabaseCheckpoint(ref GarnetDatabase db, out long storeVersion, out long objectStoreVersion)
        {
            storeVersion = db.MainStore.Recover();
            objectStoreVersion = -1;

            if (db.ObjectStore != null)
                objectStoreVersion = db.ObjectStore.Recover();

            if (storeVersion > 0 || objectStoreVersion > 0)
            {
                db.LastSaveTime = DateTimeOffset.UtcNow;
            }
        }

        protected void RecoverDatabaseAOF(ref GarnetDatabase db)
        {
            if (db.AppendOnlyFile == null) return;

            db.AppendOnlyFile.Recover();
            Logger?.LogInformation($"Recovered AOF: begin address = {db.AppendOnlyFile.BeginAddress}, tail address = {db.AppendOnlyFile.TailAddress}");
        }

        protected void ResetDatabase(ref GarnetDatabase db)
        {
            try
            {
                if (db.MainStore.Log.TailAddress > 64)
                    db.MainStore.Reset();
                if (db.ObjectStore?.Log.TailAddress > 64)
                    db.ObjectStore?.Reset();
                db.AppendOnlyFile?.Reset();

                var lastSave = DateTimeOffset.FromUnixTimeSeconds(0);
                db.LastSaveTime = lastSave;
            }
            catch (Exception ex)
            {
                Logger?.LogError(ex, "Error during reset of store");
            }
        }

        protected void EnqueueDatabaseCommit(ref GarnetDatabase db, bool isMainStore, long version)
        {
            if (db.AppendOnlyFile == null) return;

            AofHeader header = new()
            {
                opType = isMainStore ? AofEntryType.MainStoreCheckpointCommit : AofEntryType.ObjectStoreCheckpointCommit,
                storeVersion = version,
                sessionID = -1
            };

            db.AppendOnlyFile.Enqueue(header, out _);
        }

        protected void FlushDatabase(ref GarnetDatabase db, bool unsafeTruncateLog)
        {
            db.MainStore.Log.ShiftBeginAddress(db.MainStore.Log.TailAddress, truncateLog: unsafeTruncateLog);
            db.ObjectStore?.Log.ShiftBeginAddress(db.ObjectStore.Log.TailAddress, truncateLog: unsafeTruncateLog);
        }

        public abstract void Dispose();
    }
}
