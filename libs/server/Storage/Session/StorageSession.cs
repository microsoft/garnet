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
        public BasicContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> stringBasicContext;
        public TransactionalContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> stringTransactionalContext;
        public ConsistentReadContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> consistentReadContext;
        public TransactionalConsistentReadContext<StringInput, SpanByteAndMemory, long, MainSessionFunctions, StoreFunctions, StoreAllocator> transactionalConsistentReadContext;

        SectorAlignedMemory sectorAlignedMemoryHll1;
        SectorAlignedMemory sectorAlignedMemoryHll2;
        readonly int hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
        readonly int sectorAlignedMemoryPoolAlignment = 32;

        internal SessionParseState parseState;

        /// <summary>
        /// Session Contexts for object store
        /// </summary>
        public BasicContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectBasicContext;
        public TransactionalContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectTransactionalContext;
        public ConsistentReadContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreConsistentReadContext;
        public TransactionalConsistentReadContext<ObjectInput, ObjectOutput, long, ObjectSessionFunctions, StoreFunctions, StoreAllocator> objectStoreTransactionalConsistentReadContext;

        /// <summary>
        /// Session Contexts for unified store
        /// </summary>
        public BasicContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedBasicContext;
        public TransactionalContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedTransactionalContext;
        public ConsistentReadContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreConsistentReadContext;
        public TransactionalConsistentReadContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedStoreTransactionalConsistentReadContext;

        public readonly ScratchBufferBuilder scratchBufferBuilder;
        public readonly FunctionsState functionsState;

        public TransactionManager txnManager;
        public StateMachineDriver stateMachineDriver;
        readonly ILogger logger;
        private readonly CollectionItemBroker itemBroker;

        public int SessionID => stringBasicContext.Session.ID;
        public int ObjectStoreSessionID => objectBasicContext.Session.ID;

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
            var session = db.Store.NewSession<StringInput, SpanByteAndMemory, long, MainSessionFunctions>(functions, IsConsistentReadSession);
            stringBasicContext = session.BasicContext;
            stringTransactionalContext = session.TransactionalContext;
            consistentReadContext = session.ConsistentReadContext;
            transactionalConsistentReadContext = session.TransactionalConsistentReadContext;

            if (!storeWrapper.serverOptions.DisableObjects)
            {
                var objectStoreFunctions = new ObjectSessionFunctions(functionsState);
                var objectStoreSession = db.Store.NewSession<ObjectInput, ObjectOutput, long, ObjectSessionFunctions>(objectStoreFunctions, IsConsistentReadSession);
                objectBasicContext = objectStoreSession.BasicContext;
                objectTransactionalContext = objectStoreSession.TransactionalContext;
                objectStoreConsistentReadContext = objectStoreSession.ConsistentReadContext;
                objectStoreTransactionalConsistentReadContext = objectStoreSession.TransactionalConsistentReadContext;
            }

            var unifiedStoreFunctions = new UnifiedSessionFunctions(functionsState);
            var unifiedStoreSession = db.Store.NewSession<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions>(unifiedStoreFunctions, IsConsistentReadSession);
            unifiedBasicContext = unifiedStoreSession.BasicContext;
            unifiedTransactionalContext = unifiedStoreSession.TransactionalContext;
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
            stringBasicContext.Session.Dispose();
            objectBasicContext.Session?.Dispose();
            unifiedBasicContext.Session?.Dispose();
            sectorAlignedMemoryHll1?.Dispose();
            sectorAlignedMemoryHll2?.Dispose();
        }
    }
}