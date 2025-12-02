// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

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
        public BasicContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> basicContext;
        public TransactionalContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> transactionalContext;
        public ConsistentReadContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> consistentReadContext;
        public TransactionalConsistentReadContext<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> transactionalConsistentReadContext;

        SectorAlignedMemory sectorAlignedMemoryHll1;
        SectorAlignedMemory sectorAlignedMemoryHll2;
        readonly int hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
        readonly int sectorAlignedMemoryPoolAlignment = 32;

        internal SessionParseState parseState;

        /// <summary>
        /// Session Contexts for object store
        /// </summary>
        public BasicContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreBasicContext;
        public TransactionalContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreTransactionalContext;
        public ConsistentReadContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreConsistentReadContext;
        public TransactionalConsistentReadContext<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreTransactionalConsistentReadContext;

        /// <summary>
        /// Session Contexts for unified store
        /// </summary>
        public BasicContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreBasicContext;
        public TransactionalContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreTransactionalContext;
        public ConsistentReadContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreConsistentReadContext;
        public TransactionalConsistentReadContext<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreTransactionalConsistentReadContext;

        public readonly ScratchBufferBuilder scratchBufferBuilder;
        public readonly FunctionsState functionsState;

        public TransactionManager txnManager;
        public StateMachineDriver stateMachineDriver;
        readonly ILogger logger;
        private readonly CollectionItemBroker itemBroker;

        public int SessionID => basicContext.Session.ID;
        public int ObjectStoreSessionID => objectStoreBasicContext.Session.ID;

        public readonly int ObjectScanCountLimit;

        /// <summary>
        /// Flag indicating if this is storage session that uses consistent read context
        /// </summary>
        readonly bool IsConsistentReadSession;

        public StorageSession(StoreWrapper storeWrapper,
            ScratchBufferBuilder scratchBufferBuilder,
            GarnetSessionMetrics sessionMetrics,
            GarnetLatencyMetricsSession LatencyMetrics,
            int dbId,
            ConsistentReadContextCallbacks consistentReadContextCallbacks,
            ILogger logger = null,
            byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            this.sessionMetrics = sessionMetrics;
            this.LatencyMetrics = LatencyMetrics;
            this.scratchBufferBuilder = scratchBufferBuilder;
            this.logger = logger;
            this.itemBroker = storeWrapper.itemBroker;
            this.IsConsistentReadSession = consistentReadContextCallbacks != null;
            parseState.Initialize();

            functionsState = storeWrapper.CreateFunctionsState(dbId, respProtocolVersion);
            functionsState.consistentReadContextCallbacks = consistentReadContextCallbacks;

            var functions = new MainSessionFunctions(functionsState);

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound);

            this.stateMachineDriver = db.StateMachineDriver;
            var session = db.Store.NewSession<RawStringInput, SpanByteAndMemory, long, MainSessionFunctions>(functions);

            var objectStoreFunctions = new ObjectSessionFunctions(functionsState);
            var objectStoreSession = db.Store.NewSession<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions>(objectStoreFunctions);

            var unifiedStoreFunctions = new UnifiedSessionFunctions(functionsState);
            var unifiedStoreSession = db.Store.NewSession<UnifiedStoreInput, GarnetUnifiedStoreOutput, long, UnifiedSessionFunctions>(unifiedStoreFunctions);

            basicContext = session.BasicContext;
            transactionalContext = session.TransactionalContext;
            consistentReadContext = session.ConsistentReadContext;
            transactionalConsistentReadContext = session.TransactionalConsistentReadContext;

            objectStoreBasicContext = objectStoreSession.BasicContext;
            objectStoreTransactionalContext = objectStoreSession.TransactionalContext;
            objectStoreConsistentReadContext = objectStoreSession.ConsistentReadContext;
            objectStoreTransactionalConsistentReadContext = objectStoreSession.TransactionalConsistentReadContext;

            unifiedStoreBasicContext = unifiedStoreSession.BasicContext;
            unifiedStoreTransactionalContext = unifiedStoreSession.TransactionalContext;
            unifiedStoreConsistentReadContext = unifiedStoreSession.ConsistentReadContext;
            unifiedStoreTransactionalConsistentReadContext = unifiedStoreSession.TransactionalConsistentReadContext;

            HeadAddress = db.Store.Log.HeadAddress;
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
            unifiedStoreBasicContext.Session?.Dispose();
            sectorAlignedMemoryHll1?.Dispose();
            sectorAlignedMemoryHll2?.Dispose();
        }
    }
}