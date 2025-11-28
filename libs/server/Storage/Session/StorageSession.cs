// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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
        public BasicContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> basicContext;
        public LockableContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> lockableContext;

        SectorAlignedMemory sectorAlignedMemoryHll1;
        SectorAlignedMemory sectorAlignedMemoryHll2;
        readonly int hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
        readonly int sectorAlignedMemoryPoolAlignment = 32;

        internal SessionParseState parseState;

        /// <summary>
        /// Session Contexts for object store
        /// </summary>
        public BasicContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreBasicContext;
        public LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreLockableContext;

        public readonly ScratchBufferBuilder scratchBufferBuilder;
        public readonly FunctionsState functionsState;
        public readonly StreamManager streamManager;

        public TransactionManager txnManager;
        public StateMachineDriver stateMachineDriver;
        readonly ILogger logger;
        private readonly CollectionItemBroker itemBroker;

        public int SessionID => basicContext.Session.ID;
        public int ObjectStoreSessionID => objectStoreBasicContext.Session.ID;

        public readonly int ObjectScanCountLimit;

        public StorageSession(StoreWrapper storeWrapper,
            ScratchBufferBuilder scratchBufferBuilder,
            GarnetSessionMetrics sessionMetrics,
            GarnetLatencyMetricsSession LatencyMetrics,
            int dbId,
            ILogger logger = null,
            byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            this.sessionMetrics = sessionMetrics;
            this.LatencyMetrics = LatencyMetrics;
            this.scratchBufferBuilder = scratchBufferBuilder;
            this.logger = logger;
            this.itemBroker = storeWrapper.itemBroker;
            parseState.Initialize();

            functionsState = storeWrapper.CreateFunctionsState(dbId, respProtocolVersion);

            var functions = new MainSessionFunctions(functionsState);

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound);

            this.stateMachineDriver = db.StateMachineDriver;
            var session = db.MainStore.NewSession<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions>(functions);

            var objectStoreFunctions = new ObjectSessionFunctions(functionsState);
            var objectStoreSession = db.ObjectStore?.NewSession<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions>(objectStoreFunctions);

            streamManager = storeWrapper.streamManager;
            basicContext = session.BasicContext;
            lockableContext = session.LockableContext;
            if (objectStoreSession != null)
            {
                objectStoreBasicContext = objectStoreSession.BasicContext;
                objectStoreLockableContext = objectStoreSession.LockableContext;
            }

            HeadAddress = db.MainStore.Log.HeadAddress;
            ObjectScanCountLimit = storeWrapper.serverOptions.ObjectScanCountLimit;
        }

        public void UpdateRespProtocolVersion(byte respProtocolVersion)
        {
            functionsState.respProtocolVersion = respProtocolVersion;
        }

        public void Dispose()
        {
            _zcollectTaskLock.CloseLock();
            _hcollectTaskLock.CloseLock();

            sectorAlignedMemoryBitmap?.Dispose();
            basicContext.Session.Dispose();
            objectStoreBasicContext.Session?.Dispose();
            sectorAlignedMemoryHll1?.Dispose();
            sectorAlignedMemoryHll2?.Dispose();
        }
    }
}