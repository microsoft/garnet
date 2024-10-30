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
        /// Dual Session Contexts for main store
        /// </summary>
        internal DualContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator,
                                 byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator, GarnetDualInputConverter> dualContext;

        private DualItemContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> MainContext => dualContext.ItemContext1;
        private ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> MainSession => dualContext.Session1;
        private DualItemContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> ObjectContext => dualContext.ItemContext2;
        private ClientSession<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> ObjectSession => dualContext.Session2;

        SectorAlignedMemory sectorAlignedMemoryHll;
        readonly int hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
        readonly int sectorAlignedMemoryPoolAlignment = 32;

        public readonly ScratchBufferManager scratchBufferManager;
        public readonly FunctionsState functionsState;

        public TransactionManager txnManager;
        readonly ILogger logger;
        private readonly CollectionItemBroker itemBroker;

        internal TsavoriteKernel Kernel => txnManager.TsavoriteKernel;

        public int SessionID => dualContext.Session1.ID;

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
            var objstorefunctions = new ObjectSessionFunctions(functionsState);
            dualContext = new(storeWrapper.store, functions, storeWrapper.objectStore, objstorefunctions, new GarnetDualInputConverter(), pendingMetrics: this);

            HeadAddress = storeWrapper.store.Log.HeadAddress;
            ObjectScanCountLimit = storeWrapper.serverOptions.ObjectScanCountLimit;
        }

        internal long GetMainStoreKeyHashCode64(ref SpanByte key) => SpanByteComparer.StaticGetHashCode64(ref key);
        internal long GetObjectStoreKeyHashCode64(ref byte[] key) => ByteArrayKeyComparer.StaticGetHashCode64(ref key);

        public void Dispose()
        {
            sectorAlignedMemoryBitmap?.Dispose();
            dualContext.Dispose();
            sectorAlignedMemoryHll?.Dispose();
        }
    }
}