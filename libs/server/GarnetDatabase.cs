// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Represents a logical database in Garnet
    /// </summary>
    public class GarnetDatabase : IDisposable
    {
        /// <summary>
        /// Default size for version map
        /// </summary>
        // TODO: Change map size to a reasonable number
        const int DefaultVersionMapSize = 1 << 16;

        /// <summary>
        /// Database ID
        /// </summary>
        public int Id { get; }

        /// <summary>
        /// Store
        /// </summary>
        public TsavoriteKV<StoreFunctions, StoreAllocator> Store { get; }

        /// <summary>
        /// Epoch instance used by server
        /// </summary>
        public LightEpoch Epoch { get; }

        /// <summary>
        /// Common state machine driver used by Garnet
        /// </summary>
        public StateMachineDriver StateMachineDriver { get; }

        /// <summary>
        /// Size Tracker
        /// </summary>
        public CacheSizeTracker SizeTracker { get; }

        /// <summary>
        /// AOF log
        /// </summary>
        public GarnetAppendOnlyFile AppendOnlyFile { get; }

        /// <summary>
        /// Version map
        /// </summary>
        public WatchVersionMap VersionMap { get; }

        /// <summary>
        /// Tail address of store log at last save
        /// </summary>
        public long LastSaveStoreTailAddress;

        /// <summary>
        /// Last time checkpoint of database was taken
        /// </summary>
        public DateTimeOffset LastSaveTime;

        /// <summary>
        /// True if database's store index has maxed-out
        /// </summary>
        public bool StoreIndexMaxedOut;

        /// <summary>
        /// Reader-Writer lock for database checkpointing
        /// </summary>
        public SingleWriterMultiReaderLock CheckpointingLock;

        /// <summary>
        /// Per-DB VectorManager
        /// 
        /// Contexts, metadata, and associated namespaces are DB-specific, and meaningless
        /// outside of the container DB.
        /// </summary>
        public readonly VectorManager VectorManager;

        /// <summary>
        /// Storage session intended for store-wide object collection operations
        /// </summary>
        internal StorageSession StoreCollectionDbStorageSession;

        /// <summary>
        /// Storage session intended for store expired key deletion operations
        /// </summary>
        internal StorageSession StoreExpiredKeyDeletionDbStorageSession;

        internal StorageSession HybridLogStatScanStorageSession;

        private KVSettings KvSettings;

        bool disposed = false;

        public GarnetDatabase(int id, TsavoriteKV<StoreFunctions, StoreAllocator> store, KVSettings kvSettings, LightEpoch epoch, StateMachineDriver stateMachineDriver,
                CacheSizeTracker sizeTracker, GarnetAppendOnlyFile appendOnlyFile, bool storeIndexMaxedOut, VectorManager vectorManager)
            : this()
        {
            Id = id;
            Store = store;
            KvSettings = kvSettings;
            Epoch = epoch;
            StateMachineDriver = stateMachineDriver;
            SizeTracker = sizeTracker;
            AppendOnlyFile = appendOnlyFile;
            StoreIndexMaxedOut = storeIndexMaxedOut;
            VectorManager = vectorManager;
        }

        public GarnetDatabase(int id, GarnetDatabase srcDb, bool enableAof, bool copyLastSaveData = false) : this()
        {
            Id = id;
            Store = srcDb.Store;
            KvSettings = srcDb.KvSettings;
            Epoch = srcDb.Epoch;
            StateMachineDriver = srcDb.StateMachineDriver;
            SizeTracker = srcDb.SizeTracker;
            AppendOnlyFile = enableAof ? srcDb.AppendOnlyFile : null;
            StoreIndexMaxedOut = srcDb.StoreIndexMaxedOut;
            VectorManager = srcDb.VectorManager;

            if (copyLastSaveData)
            {
                LastSaveTime = srcDb.LastSaveTime;
                LastSaveStoreTailAddress = srcDb.LastSaveStoreTailAddress;
            }
        }

        public GarnetDatabase()
        {
            VersionMap = new WatchVersionMap(DefaultVersionMapSize);
            LastSaveStoreTailAddress = 0;
            LastSaveTime = DateTimeOffset.FromUnixTimeSeconds(0);
        }

        /// <summary>
        /// Dispose method
        /// </summary>
        public void Dispose()
        {
            if (disposed)
                return;
            disposed = true;

            // Shutdown vector replays and cleanup operations
            VectorManager?.Dispose();

            // Wait for checkpoints to complete and disable checkpointing
            while (!CheckpointingLock.TryWriteLock())
                _ = Thread.Yield();

            Store?.Dispose();
            KvSettings?.LogDevice?.Dispose();
            KvSettings?.ObjectLogDevice?.Dispose();
            AppendOnlyFile?.Dispose();
            StoreCollectionDbStorageSession?.Dispose();
            StoreExpiredKeyDeletionDbStorageSession?.Dispose();

            SizeTracker?.Stop();
        }
    }
}