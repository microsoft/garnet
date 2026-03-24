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
        readonly long HeadAddress;

        /// <summary>
        /// Session Contexts for main store
        /// </summary>
        public StringBasicContext stringBasicContext;
        public StringTransactionalContext stringTransactionalContext;

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

        public readonly ScratchBufferBuilder scratchBufferBuilder;
        public readonly FunctionsState functionsState;

        public TransactionManager txnManager;
        public StateMachineDriver stateMachineDriver;
        readonly ILogger logger;
        private readonly CollectionItemBroker itemBroker;

        public int SessionID => stringBasicContext.Session.ID;
        public int ObjectStoreSessionID => objectBasicContext.Session.ID;

        public readonly int ObjectScanCountLimit;

        public readonly VectorManager vectorManager;

        public StorageSession(StoreWrapper storeWrapper,
            ScratchBufferBuilder scratchBufferBuilder,
            GarnetSessionMetrics sessionMetrics,
            GarnetLatencyMetricsSession LatencyMetrics,
            int dbId,
            VectorManager vectorManager,
            ILogger logger = null,
            byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            this.sessionMetrics = sessionMetrics;
            this.LatencyMetrics = LatencyMetrics;
            this.scratchBufferBuilder = scratchBufferBuilder;
            this.logger = logger;
            this.itemBroker = storeWrapper.itemBroker;
            parseState.Initialize();
            this.vectorManager = vectorManager;

            functionsState = storeWrapper.CreateFunctionsState(dbId, respProtocolVersion);

            var functions = new MainSessionFunctions(functionsState);

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound);

            this.stateMachineDriver = db.StateMachineDriver;
            var session = db.Store.NewSession<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions>(functions);

            if (!storeWrapper.serverOptions.DisableObjects)
            {
                var objectStoreFunctions = new ObjectSessionFunctions(functionsState);
                var objectStoreSession = db.Store.NewSession<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions>(objectStoreFunctions);

                objectBasicContext = objectStoreSession.BasicContext;
                objectTransactionalContext = objectStoreSession.TransactionalContext;
            }

            var unifiedStoreFunctions = new UnifiedSessionFunctions(functionsState);
            var unifiedStoreSession = db.Store.NewSession<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions>(unifiedStoreFunctions);

            var vectorFunctions = new VectorSessionFunctions(functionsState);
            var vectorSession = db.Store.NewSession<VectorElementKey, VectorInput, VectorOutput, long, VectorSessionFunctions>(vectorFunctions);

            stringBasicContext = session.BasicContext;
            stringTransactionalContext = session.TransactionalContext;

            unifiedBasicContext = unifiedStoreSession.BasicContext;
            unifiedTransactionalContext = unifiedStoreSession.TransactionalContext;

            vectorBasicContext = vectorSession.BasicContext;
            vectorTransactionalContext = vectorSession.TransactionalContext;

            HeadAddress = db.Store.Log.HeadAddress;
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