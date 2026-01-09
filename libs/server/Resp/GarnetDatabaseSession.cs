// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Garnet.server
{
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