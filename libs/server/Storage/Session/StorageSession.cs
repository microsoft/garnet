// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Garnet.common;
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

        /// <summary>
        /// Session Contexts for main store
        /// </summary>
        public StringBasicContext stringBasicContext;
        public StringTransactionalContext stringTransactionalContext;
        public ConsistentReadStringBasicContext consistentReadContext;
        public ConsistentReadStringTransactionalContext transactionalConsistentReadContext;

        SectorAlignedMemory sectorAlignedMemoryHll1;
        SectorAlignedMemory sectorAlignedMemoryHll2;
        readonly int hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
        readonly int sectorAlignedMemoryPoolAlignment = 32;

        internal SessionParseState parseState;

        /// <summary>
        /// Session Contexts for object store
        /// </summary>
        public ObjectBasicContext objectBasicContext;
        public ObjectTransactionalContext objectTransactionalContext;
        public ConsistentReadObjectBasicContext objectStoreConsistentReadContext;
        public ConsistentReadObjectTransactionalContext objectStoreTransactionalConsistentReadContext;

        /// <summary>
        /// Session Contexts for vector store
        /// </summary>
        public VectorBasicContext vectorBasicContext;
        public VectorTransactionalContext vectorTransactionalContext;

        /// <summary>
        /// Session Contexts for unified store
        /// </summary>
        public UnifiedBasicContext unifiedBasicContext;
        public UnifiedTransactionalContext unifiedTransactionalContext;
        public ConsistentReadUnifiedBasicContext unifiedStoreConsistentReadContext;
        public ConsistentReadUnifiedTransactionalContext unifiedStoreTransactionalConsistentReadContext;

        internal readonly ScratchBufferBuilder scratchBufferBuilder;
        public readonly FunctionsState functionsState;
        internal readonly ScratchBufferAllocator scratchBufferAllocator;

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

        /// <summary>
        /// Read session state use to enforce prefix consistency with sharded-log
        /// </summary>
        readonly ReadSessionState readSessionState;

        /// <summary>
        /// Vector manage instance
        /// </summary>
        public readonly VectorManager vectorManager;

        public StorageSession(StoreWrapper storeWrapper,
            ScratchBufferBuilder scratchBufferBuilder,
            ScratchBufferAllocator scratchBufferAllocator,
            GarnetSessionMetrics sessionMetrics,
            GarnetLatencyMetricsSession LatencyMetrics,
            int dbId,
            ReadSessionState readSessionState,
            VectorManager vectorManager,
            ILogger logger = null,
            byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            this.sessionMetrics = sessionMetrics;
            this.LatencyMetrics = LatencyMetrics;
            this.scratchBufferBuilder = scratchBufferBuilder;
            this.scratchBufferAllocator = scratchBufferAllocator;
            this.logger = logger;
            this.itemBroker = storeWrapper.itemBroker;
            this.IsConsistentReadSession = readSessionState != null;
            this.readSessionState = readSessionState;
            parseState.Initialize();
            this.vectorManager = vectorManager;

            functionsState = storeWrapper.CreateFunctionsState(dbId, respProtocolVersion);

            var functions = new MainSessionFunctions(functionsState, readSessionState);

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound);

            this.stateMachineDriver = db.StateMachineDriver;
            var session = db.Store.NewSession<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions>(functions, IsConsistentReadSession);

            if (!storeWrapper.serverOptions.DisableObjects)
            {
                var objectStoreFunctions = new ObjectSessionFunctions(functionsState, readSessionState);
                var objectStoreSession = db.Store.NewSession<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions>(objectStoreFunctions, IsConsistentReadSession);
                objectBasicContext = objectStoreSession.BasicContext;
                objectTransactionalContext = objectStoreSession.TransactionalContext;
                objectStoreConsistentReadContext = objectStoreSession.ConsistentReadContext;
                objectStoreTransactionalConsistentReadContext = objectStoreSession.TransactionalConsistentReadContext;
            }

            var unifiedStoreFunctions = new UnifiedSessionFunctions(functionsState, readSessionState);
            var unifiedStoreSession = db.Store.NewSession<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions>(unifiedStoreFunctions, IsConsistentReadSession);

            var vectorFunctions = new VectorSessionFunctions(functionsState, readSessionState);
            var vectorSession = db.Store.NewSession<VectorElementKey, VectorInput, VectorOutput, long, VectorSessionFunctions>(vectorFunctions);

            stringBasicContext = session.BasicContext;
            stringTransactionalContext = session.TransactionalContext;
            consistentReadContext = session.ConsistentReadContext;
            transactionalConsistentReadContext = session.TransactionalConsistentReadContext;

            unifiedBasicContext = unifiedStoreSession.BasicContext;
            unifiedTransactionalContext = unifiedStoreSession.TransactionalContext;
            unifiedStoreConsistentReadContext = unifiedStoreSession.ConsistentReadContext;
            unifiedStoreTransactionalConsistentReadContext = unifiedStoreSession.TransactionalConsistentReadContext;

            vectorBasicContext = vectorSession.BasicContext;
            vectorTransactionalContext = vectorSession.TransactionalContext;

            ObjectScanCountLimit = storeWrapper.serverOptions.ObjectScanCountLimit;
        }

        public void UpdateRespProtocolVersion(byte respProtocolVersion)
        {
            functionsState.respProtocolVersion = respProtocolVersion;
        }

        public void Dispose()
        {
            while (!_zcollectTaskLock.TryWriteLock())
                _ = Thread.Yield();

            while (!_hcollectTaskLock.TryWriteLock())
                _ = Thread.Yield();

            sectorAlignedMemoryBitmap?.Dispose();
            stringBasicContext.Session.Dispose();
            objectBasicContext.Session?.Dispose();
            unifiedBasicContext.Session?.Dispose();
            vectorBasicContext.Session?.Dispose();
            sectorAlignedMemoryHll1?.Dispose();
            sectorAlignedMemoryHll2?.Dispose();
        }
    }
}