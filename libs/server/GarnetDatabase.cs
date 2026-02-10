// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        /// Device used for AOF logging
        /// </summary>
        public IDevice AofDevice { get; }

        /// <summary>
        /// AOF log
        /// </summary>
        public TsavoriteLog AppendOnlyFile { get; }

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
        /// Storage session intended for store-wide object collection operations
        /// </summary>
        internal StorageSession StoreCollectionDbStorageSession;

        /// <summary>
        /// Storage session intended for store expired key deletion operations
        /// </summary>
        internal StorageSession StoreExpiredKeyDeletionDbStorageSession;

        internal StorageSession HybridLogStatScanStorageSession;

        private KVSettings kvSettings;

        bool disposed = false;

        public GarnetDatabase(KVSettings kvSettings, int id, TsavoriteKV<StoreFunctions, StoreAllocator> store, LightEpoch epoch, StateMachineDriver stateMachineDriver,
                CacheSizeTracker sizeTracker, IDevice aofDevice, TsavoriteLog appendOnlyFile, bool storeIndexMaxedOut)
            : this()
        {
            this.kvSettings = kvSettings;
            Id = id;
            Store = store;
            Epoch = epoch;
            StateMachineDriver = stateMachineDriver;
            SizeTracker = sizeTracker;
            AofDevice = aofDevice;
            AppendOnlyFile = appendOnlyFile;
            StoreIndexMaxedOut = storeIndexMaxedOut;
        }

        public GarnetDatabase(int id, GarnetDatabase srcDb, bool enableAof, bool copyLastSaveData = false) : this()
        {
            kvSettings = srcDb.kvSettings;
            Id = id;
            Store = srcDb.Store;
            Epoch = srcDb.Epoch;
            StateMachineDriver = srcDb.StateMachineDriver;
            SizeTracker = srcDb.SizeTracker;
            AofDevice = enableAof ? srcDb.AofDevice : null;
            AppendOnlyFile = enableAof ? srcDb.AppendOnlyFile : null;
            StoreIndexMaxedOut = srcDb.StoreIndexMaxedOut;

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

            // Wait for checkpoints to complete and disable checkpointing
            CheckpointingLock.CloseLock();

            Store?.Dispose();
            AofDevice?.Dispose();
            AppendOnlyFile?.Dispose();
            StoreCollectionDbStorageSession?.Dispose();
            StoreExpiredKeyDeletionDbStorageSession?.Dispose();

            kvSettings?.LogDevice?.Dispose();
            kvSettings?.ObjectLogDevice?.Dispose();

            SizeTracker?.Stop();
        }
    }
}