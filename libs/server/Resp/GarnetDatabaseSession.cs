using System;
using Tsavorite.core;

namespace Garnet.server
{
    using BasicGarnetApi = GarnetApi<BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;
    using TransactionalGarnetApi = GarnetApi<TransactionalContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions,
            /* MainStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        TransactionalContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions,
            /* ObjectStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>,
        TransactionalContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions,
            /* UnifiedStoreFunctions */ StoreFunctions<SpanByteComparer, DefaultRecordDisposer>,
            ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>>>;

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
        public TransactionalGarnetApi TransactionalGarnetApi { get; }

        /// <summary>
        /// Transaction manager
        /// </summary>
        public TransactionManager TransactionManager { get; }

        bool disposed = false;

        public GarnetDatabaseSession(int id, StorageSession storageSession, BasicGarnetApi garnetApi, TransactionalGarnetApi lockableGarnetApi, TransactionManager txnManager)
        {
            this.Id = id;
            this.StorageSession = storageSession;
            this.GarnetApi = garnetApi;
            this.TransactionalGarnetApi = lockableGarnetApi;
            this.TransactionManager = txnManager;
        }

        public GarnetDatabaseSession(int id, GarnetDatabaseSession srcSession)
        {
            this.Id = id;
            this.StorageSession = srcSession.StorageSession;
            this.GarnetApi = srcSession.GarnetApi;
            this.TransactionalGarnetApi = srcSession.TransactionalGarnetApi;
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