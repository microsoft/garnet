// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Storage Session - the internal layer that Garnet uses to perform storage operations
    /// </summary>
    sealed partial class StorageSession : IDisposable
    {
        int bitmapBufferSize = 1 << 15;
        SectorAlignedMemory sectorAlignedMemoryBitmap;
        readonly long HeadAddress;

        /// <summary>
        /// Session Contexts for main store
        /// </summary>
        public BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> basicContext;
        public LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> lockableContext;

        SectorAlignedMemory sectorAlignedMemoryHll;
        readonly int hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
        readonly int sectorAlignedMemoryPoolAlignment = 32;

        /// <summary>
        /// Session Contexts for object store
        /// </summary>
        public BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreBasicContext;
        public LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreLockableContext;

        public readonly ScratchBufferManager scratchBufferManager;
        public readonly FunctionsState functionsState;

        public TransactionManager txnManager;
        readonly ILogger logger;
        private readonly CollectionItemBroker itemBroker;

        public int SessionID => basicContext.Session.ID;
        public int ObjectStoreSessionID => objectStoreBasicContext.Session.ID;

        public readonly int ObjectScanCountLimit;

        public StorageSession(StoreWrapper storeWrapper,
            ScratchBufferManager scratchBufferManager,
            GarnetSessionMetrics sessionMetrics,
            GarnetLatencyMetricsSession LatencyMetrics,
            CollectionItemBroker itemBroker,
            ILogger logger = null)
        {
            this.sessionMetrics = sessionMetrics;
            this.LatencyMetrics = LatencyMetrics;
            this.scratchBufferManager = scratchBufferManager;
            this.logger = logger;
            this.itemBroker = itemBroker;

            functionsState = storeWrapper.CreateFunctionsState();

            var functions = new MainStoreFunctions(functionsState);
            var session = storeWrapper.store.NewSession<SpanByte, SpanByteAndMemory, long, MainStoreFunctions>(functions);

            var objstorefunctions = new ObjectStoreFunctions(functionsState);
            var objectStoreSession = storeWrapper.objectStore?.NewSession<SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>(objstorefunctions);

            basicContext = session.BasicContext;
            lockableContext = session.LockableContext;
            if (objectStoreSession != null)
            {
                objectStoreBasicContext = objectStoreSession.BasicContext;
                objectStoreLockableContext = objectStoreSession.LockableContext;
            }

            HeadAddress = storeWrapper.store.Log.HeadAddress;
            ObjectScanCountLimit = storeWrapper.serverOptions.ObjectScanCountLimit;
        }

        public void Dispose()
        {
            sectorAlignedMemoryBitmap?.Dispose();
            basicContext.Session.Dispose();
            objectStoreBasicContext.Session?.Dispose();
            sectorAlignedMemoryHll?.Dispose();
        }
    }
}