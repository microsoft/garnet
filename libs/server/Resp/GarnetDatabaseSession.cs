﻿using System;
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

    /// <summary>
    /// Represents a logical database session in Garnet
    /// </summary>
    internal struct GarnetDatabaseSession : IDisposable
    {
        /// <summary>
        /// Database ID
        /// </summary>
        public int Id;

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

        /// <summary>
        /// Transaction manager
        /// </summary>
        public TransactionManager TransactionManager;

        bool disposed = false;

        public GarnetDatabaseSession(int id, StorageSession storageSession, BasicGarnetApi garnetApi, LockableGarnetApi lockableGarnetApi, TransactionManager txnManager)
        {
            this.Id = id;
            this.StorageSession = storageSession;
            this.GarnetApi = garnetApi;
            this.LockableGarnetApi = lockableGarnetApi;
            this.TransactionManager = txnManager;
        }

        /// <summary>
        /// Returns true if current struct hasn't been initialized
        /// </summary>
        /// <returns>True if default struct</returns>
        public bool IsDefault() => StorageSession == null;

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

        /// <summary>
        /// Instance of empty database session
        /// </summary>
        internal static GarnetDatabaseSession Empty;
    }
}