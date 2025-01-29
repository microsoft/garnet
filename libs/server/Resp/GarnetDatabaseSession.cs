using System;
using Tsavorite.core;

namespace Garnet.server
{
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>;
    using LockableGarnetApi = GarnetApi<LockableContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>>;

    internal struct GarnetDatabaseSession : IDisposable
    {
        /// <summary>
        /// Storage session 
        /// </summary>
        public StorageSession StorageSession;

        /// <summary>
        /// Garnet API
        /// </summary>
        public BasicGarnetApi GarnetApi;

        /// <summary>
        /// Lockable Garnet API
        /// </summary>
        public LockableGarnetApi LockableGarnetApi;

        bool disposed = false;

        public GarnetDatabaseSession(StorageSession storageSession, BasicGarnetApi garnetApi, LockableGarnetApi lockableGarnetApi)
        {
            this.StorageSession = storageSession;
            this.GarnetApi = garnetApi;
            this.LockableGarnetApi = lockableGarnetApi;
        }

        public void Dispose()
        {
            if (disposed)
                return;

            StorageSession?.Dispose();

            disposed = true;
        }
    }
}
