// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

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
        /// Main Store
        /// </summary>
        public TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> MainStore { get; }

        /// <summary>
        /// Object Store
        /// </summary>
        public TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStore { get; }

        /// <summary>
        /// Epoch instance used by server
        /// </summary>
        public LightEpoch Epoch { get; }

        /// <summary>
        /// Common state machine driver used by Garnet
        /// </summary>
        public StateMachineDriver StateMachineDriver { get; }

        /// <summary>
        /// Size Tracker for Object Store
        /// </summary>
        public CacheSizeTracker ObjectStoreSizeTracker { get; }

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
        /// Tail address of main store log at last save
        /// </summary>
        public long LastSaveStoreTailAddress;

        /// <summary>
        /// Tail address of object store log at last save
        /// </summary>
        public long LastSaveObjectStoreTailAddress;

        /// <summary>
        /// Last time checkpoint of database was taken
        /// </summary>
        public DateTimeOffset LastSaveTime;

        /// <summary>
        /// True if database's main store index has maxed-out
        /// </summary>
        public bool MainStoreIndexMaxedOut;

        /// <summary>
        /// True if database's object store index has maxed-out
        /// </summary>
        public bool ObjectStoreIndexMaxedOut;

        /// <summary>
        /// Reader-Writer lock for database checkpointing
        /// </summary>
        public SingleWriterMultiReaderLock CheckpointingLock;

        /// <summary>
        /// Storage session intended for store-wide object collection operations
        /// </summary>
        internal StorageSession ObjectStoreCollectionDbStorageSession;

        /// <summary>
        /// Storage session intended for main-store expired key deletion operations
        /// </summary>
        internal StorageSession MainStoreExpiredKeyDeletionDbStorageSession;

        /// <summary>
        /// Storage session intended for object-store expired key deletion operations
        /// </summary>
        internal StorageSession ObjectStoreExpiredKeyDeletionDbStorageSession;


        internal StorageSession HybridLogStatScanStorageSession;

        readonly KVSettings<SpanByte, SpanByte> KvSettings;
        readonly KVSettings<byte[], IGarnetObject> ObjKvSettings;

        bool disposed = false;

        public GarnetDatabase(int id, TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> mainStore,
            TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore,
            KVSettings<SpanByte, SpanByte> kvSettings, KVSettings<byte[], IGarnetObject> objKvSettings,
            LightEpoch epoch, StateMachineDriver stateMachineDriver,
            CacheSizeTracker objectStoreSizeTracker, IDevice aofDevice, TsavoriteLog appendOnlyFile,
            bool mainStoreIndexMaxedOut, bool objectStoreIndexMaxedOut) : this()
        {
            Id = id;
            MainStore = mainStore;
            ObjectStore = objectStore;
            KvSettings = kvSettings;
            ObjKvSettings = objKvSettings;
            Epoch = epoch;
            StateMachineDriver = stateMachineDriver;
            ObjectStoreSizeTracker = objectStoreSizeTracker;
            AofDevice = aofDevice;
            AppendOnlyFile = appendOnlyFile;
            MainStoreIndexMaxedOut = mainStoreIndexMaxedOut;
            ObjectStoreIndexMaxedOut = objectStoreIndexMaxedOut;
        }

        public GarnetDatabase(int id, GarnetDatabase srcDb, bool enableAof, bool copyLastSaveData = false) : this()
        {
            Id = id;
            MainStore = srcDb.MainStore;
            ObjectStore = srcDb.ObjectStore;
            KvSettings = srcDb.KvSettings;
            ObjKvSettings = srcDb.ObjKvSettings;
            Epoch = srcDb.Epoch;
            StateMachineDriver = srcDb.StateMachineDriver;
            ObjectStoreSizeTracker = srcDb.ObjectStoreSizeTracker;
            AofDevice = enableAof ? srcDb.AofDevice : null;
            AppendOnlyFile = enableAof ? srcDb.AppendOnlyFile : null;
            MainStoreIndexMaxedOut = srcDb.MainStoreIndexMaxedOut;
            ObjectStoreIndexMaxedOut = srcDb.ObjectStoreIndexMaxedOut;

            if (copyLastSaveData)
            {
                LastSaveTime = srcDb.LastSaveTime;
                LastSaveStoreTailAddress = srcDb.LastSaveStoreTailAddress;
                LastSaveObjectStoreTailAddress = srcDb.LastSaveObjectStoreTailAddress;
            }
        }

        public GarnetDatabase()
        {
            VersionMap = new WatchVersionMap(DefaultVersionMapSize);
            LastSaveStoreTailAddress = 0;
            LastSaveObjectStoreTailAddress = 0;
            LastSaveTime = DateTimeOffset.FromUnixTimeSeconds(0);
        }

        /// <summary>
        /// Dispose method
        /// </summary>
        public void Dispose()
        {
            if (disposed) return;

            // Wait for checkpoints to complete and disable checkpointing
            CheckpointingLock.CloseLock();

            MainStore?.Dispose();
            ObjectStore?.Dispose();

            KvSettings?.LogDevice?.Dispose();
            if (ObjKvSettings != null)
            {
                ObjKvSettings.LogDevice?.Dispose();
                ObjKvSettings.ObjectLogDevice?.Dispose();
            }

            AofDevice?.Dispose();
            AppendOnlyFile?.Dispose();
            ObjectStoreCollectionDbStorageSession?.Dispose();
            MainStoreExpiredKeyDeletionDbStorageSession?.Dispose();
            ObjectStoreExpiredKeyDeletionDbStorageSession?.Dispose();

            if (ObjectStoreSizeTracker != null)
            {
                // If tracker has previously started, wait for it to stop
                if (!ObjectStoreSizeTracker.TryPreventStart())
                {
                    while (!ObjectStoreSizeTracker.Stopped)
                        Thread.Yield();
                }
            }
            disposed = true;
        }
    }
}