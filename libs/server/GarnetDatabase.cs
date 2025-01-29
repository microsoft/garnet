using System;
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
        /// Main Store
        /// </summary>
        public TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> MainStore;

        /// <summary>
        /// Object Store
        /// </summary>
        public TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStore;

        /// <summary>
        /// Size Tracker for Object Store
        /// </summary>
        public CacheSizeTracker ObjectSizeTracker;

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

        bool disposed = false;

        public GarnetDatabase(TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> mainStore,
            TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore,
            CacheSizeTracker objectSizeTracker, IDevice aofDevice, TsavoriteLog appendOnlyFile)
        {
            MainStore = mainStore;
            ObjectStore = objectStore;
            ObjectSizeTracker = objectSizeTracker;
            AofDevice = aofDevice;
            AppendOnlyFile = appendOnlyFile;
            VersionMap = new WatchVersionMap(DefaultVersionMapSize);
        }

        public void Dispose()
        {
            if (disposed) return;

            MainStore?.Dispose();
            ObjectStore?.Dispose();
            AofDevice?.Dispose();
            AppendOnlyFile?.Dispose();

            disposed = true;
        }
    }
}
