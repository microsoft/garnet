using System;
using Tsavorite.core;

namespace Garnet.server
{
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>,
        BasicContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions,
            /* VectorStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>>;
    using LockableGarnetApi = GarnetApi<LockableContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>,
        LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>,
            GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>>,
        LockableContext<SpanByte, SpanByte, VectorInput, SpanByte, long, VectorSessionFunctions,
            /* VectorStoreFunctions */ StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
            SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>>;

    /// <summary>
    /// Represents a logical database session in Garnet
    /// </summary>
    internal class GarnetDatabaseSession : IDisposable
    {
        /// <summary>
        /// Database ID
        /// </summary>
        public int Id { get; }

        /// <summary>
        /// Storage session 
        /// </summary>
        public StorageSession StorageSession { get; }

        /// <summary>
        /// Garnet API
        /// </summary>
        public BasicGarnetApi GarnetApi { get; }

        /// <summary>
        /// Lockable Garnet API
        /// </summary>
        public LockableGarnetApi LockableGarnetApi { get; }

        /// <summary>
        /// Transaction manager
        /// </summary>
        public TransactionManager TransactionManager { get; }

        bool disposed = false;

        public GarnetDatabaseSession(int id, StorageSession storageSession, BasicGarnetApi garnetApi, LockableGarnetApi lockableGarnetApi, TransactionManager txnManager)
        {
            this.Id = id;
            this.StorageSession = storageSession;
            this.GarnetApi = garnetApi;
            this.LockableGarnetApi = lockableGarnetApi;
            this.TransactionManager = txnManager;
        }

        public GarnetDatabaseSession(int id, GarnetDatabaseSession srcSession)
        {
            this.Id = id;
            this.StorageSession = srcSession.StorageSession;
            this.GarnetApi = srcSession.GarnetApi;
            this.LockableGarnetApi = srcSession.LockableGarnetApi;
            this.TransactionManager = srcSession.TransactionManager;
        }

        /// <summary>
        /// Dispose method
        /// </summary>
        public void Dispose()
        {
            if (disposed)
                return;

            StorageSession?.Dispose();

            disposed = true;
        }
    }
}