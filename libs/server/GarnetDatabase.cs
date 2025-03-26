﻿using System;
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
    public struct GarnetDatabase : IDisposable
    {
        /// <summary>
        /// Default size for version map
        /// </summary>
        // TODO: Change map size to a reasonable number
        const int DefaultVersionMapSize = 1 << 16;

        /// <summary>
        /// Database ID
        /// </summary>
        public int Id;

        /// <summary>
        /// Main Store
        /// </summary>
        public TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> MainStore;

        /// <summary>
        /// Object Store
        /// </summary>
        public TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStore;

        /// <summary>
        /// Epoch instance used by server
        /// </summary>
        public LightEpoch Epoch;

        /// <summary>
        /// Common state machine driver used by Garnet
        /// </summary>
        public StateMachineDriver StateMachineDriver;

        /// <summary>
        /// Size Tracker for Object Store
        /// </summary>
        public CacheSizeTracker ObjectStoreSizeTracker;

        /// <summary>
        /// Device used for AOF logging
        /// </summary>
        public IDevice AofDevice;

        /// <summary>
        /// AOF log
        /// </summary>
        public TsavoriteLog AppendOnlyFile;

        /// <summary>
        /// Version map
        /// </summary>
        public WatchVersionMap VersionMap;

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

        bool disposed = false;

        public GarnetDatabase(int id, TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> mainStore,
            TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore,
            LightEpoch epoch, StateMachineDriver stateMachineDriver,
            CacheSizeTracker objectStoreSizeTracker, IDevice aofDevice, TsavoriteLog appendOnlyFile,
            bool mainStoreIndexMaxedOut, bool objectStoreIndexMaxedOut) : this()
        {
            Id = id;
            MainStore = mainStore;
            ObjectStore = objectStore;
            Epoch = epoch;
            StateMachineDriver = stateMachineDriver;
            ObjectStoreSizeTracker = objectStoreSizeTracker;
            AofDevice = aofDevice;
            AppendOnlyFile = appendOnlyFile;
            MainStoreIndexMaxedOut = mainStoreIndexMaxedOut;
            ObjectStoreIndexMaxedOut = objectStoreIndexMaxedOut;
        }

        public GarnetDatabase(ref GarnetDatabase srcDb, bool enableAof) : this()
        {
            Id = srcDb.Id;
            MainStore = srcDb.MainStore;
            ObjectStore = srcDb.ObjectStore;
            Epoch = srcDb.Epoch;
            StateMachineDriver = srcDb.StateMachineDriver;
            ObjectStoreSizeTracker = srcDb.ObjectStoreSizeTracker;
            AofDevice = enableAof ? srcDb.AofDevice : null;
            AppendOnlyFile = enableAof ? srcDb.AppendOnlyFile : null;
            MainStoreIndexMaxedOut = srcDb.MainStoreIndexMaxedOut;
            ObjectStoreIndexMaxedOut = srcDb.ObjectStoreIndexMaxedOut;
        }

        public GarnetDatabase()
        {
            VersionMap = new WatchVersionMap(DefaultVersionMapSize);
            LastSaveStoreTailAddress = 0;
            LastSaveObjectStoreTailAddress = 0;
            LastSaveTime = DateTimeOffset.FromUnixTimeSeconds(0);
        }

        /// <summary>
        /// Returns true if current struct hasn't been initialized
        /// </summary>
        /// <returns>True if default struct</returns>
        public bool IsDefault() => MainStore == null;

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
            AofDevice?.Dispose();
            AppendOnlyFile?.Dispose();

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

        /// <summary>
        /// Dummy database to return by default
        /// </summary>
        internal static GarnetDatabase Empty;
    }
}