// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

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
        internal ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> session;
        public BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext;
        public LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> lockableContext;
        public DualContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> dualContext;

        SectorAlignedMemory sectorAlignedMemoryHll;
        readonly int hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
        readonly int sectorAlignedMemoryPoolAlignment = 32;

        /// <summary>
        /// Session Contexts for object store
        /// </summary>
        internal ClientSession<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreSession;
        public BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreBasicContext;
        public LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreLockableContext;
        public DualContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreDualContext;

        public readonly ScratchBufferManager scratchBufferManager;
        public readonly FunctionsState functionsState;

        public TransactionManager txnManager;
        readonly ILogger logger;
        private readonly CollectionItemBroker itemBroker;

        internal TsavoriteKernel TsavoriteKernel => txnManager.TsavoriteKernel;

        public int SessionID => basicContext.Session.ID;
        public int ObjectStoreSessionID => objectStoreBasicContext.Session.ID;

        public readonly int ObjectScanCountLimit;

        public StorageSession(StoreWrapper storeWrapper,
            ScratchBufferManager scratchBufferManager,
            GarnetSessionMetrics sessionMetrics,
            GarnetLatencyMetricsSession LatencyMetrics,
            ILogger logger = null)
        {
            this.sessionMetrics = sessionMetrics;
            this.LatencyMetrics = LatencyMetrics;
            this.scratchBufferManager = scratchBufferManager;
            this.logger = logger;
            this.itemBroker = storeWrapper.itemBroker;

            functionsState = storeWrapper.CreateFunctionsState();

            var functions = new MainSessionFunctions(functionsState);
            session = storeWrapper.store.NewSession<SpanByte, SpanByteAndMemory, long, MainSessionFunctions>(functions);

            var objstorefunctions = new ObjectSessionFunctions(functionsState);
            objectStoreSession = storeWrapper.objectStore?.NewSession<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions>(objstorefunctions);

            basicContext = session.BasicContext;
            lockableContext = session.LockableContext;
            dualContext = session.DualContext;
            if (objectStoreSession != null)
            {
                objectStoreBasicContext = objectStoreSession.BasicContext;
                objectStoreLockableContext = objectStoreSession.LockableContext;
                objectStoreDualContext = objectStoreSession.DualContext;
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