// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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
        internal readonly RuntimeServerConfig runtimeConfig;

        public int SessionID => stringBasicContext.Session.ID;
        public int ObjectStoreSessionID => objectBasicContext.Session.ID;

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

        /// <summary>
        /// Log of the store this session operates on, or null when the store has no storage tier to evict to.
        /// </summary>
        readonly LogAccessor<StoreFunctions, StoreAllocator> evictingLog;

        /// <summary>
        /// Whether an operation of this session may find its record on disk, and so may wait on the device: the store
        /// has evicted part of its log to its storage tier.
        /// </summary>
        /// <remarks>
        /// This is the state of the store when it is asked. A store that starts to evict just after can still send an
        /// operation the caller decided on here to the device.
        /// </remarks>
        internal bool MayReadFromDisk
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => evictingLog is { } log && log.HeadAddress > log.BeginAddress;
        }

        /// <summary>
        /// Whether an operation of this session that adds a record may wait for a flush to the device before it can allocate
        /// the record: the store's log, or the AOF the operation appends to, has no page in memory it can evict without one.
        /// </summary>
        /// <remarks>
        /// As with <see cref="MayReadFromDisk"/>, this is the state of the logs when it is asked. Other sessions can fill the
        /// page the caller decided on here, so an operation can still wait for a flush that was not in sight.
        /// </remarks>
        internal bool MayWaitToAllocate
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (evictingLog is { } log && log.AllocationMayWaitForFlush)
                || (functionsState.appendOnlyFile is { } aof && aof.Log.AllocationMayWaitForFlush);
        }

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
            this.runtimeConfig = storeWrapper.runtimeConfig;
            this.IsConsistentReadSession = readSessionState != null;
            this.readSessionState = readSessionState;
            parseState.Initialize();
            this.vectorManager = vectorManager;

            functionsState = storeWrapper.CreateFunctionsState(dbId, respProtocolVersion);

            var functions = new MainSessionFunctions(functionsState, readSessionState);

            var dbFound = storeWrapper.TryGetDatabase(dbId, out var db);
            Debug.Assert(dbFound);

            this.stateMachineDriver = db.StateMachineDriver;
            if (storeWrapper.serverOptions.EnableStorageTier)
                evictingLog = db.Store.Log;
            var session = db.Store.NewSession<FixedSpanByteKey, StringInput, StringOutput, long, MainSessionFunctions>(functions, IsConsistentReadSession);

            if (!storeWrapper.serverOptions.DisableObjects)
            {
                var objectStoreFunctions = new ObjectSessionFunctions(functionsState, readSessionState);
                var objectStoreSession = db.Store.NewSession<FixedSpanByteKey, ObjectInput, ObjectOutput, long, ObjectSessionFunctions>(objectStoreFunctions, IsConsistentReadSession, sessionIdOverride: session.ID);
                objectBasicContext = objectStoreSession.BasicContext;
                objectTransactionalContext = objectStoreSession.TransactionalContext;
                objectStoreConsistentReadContext = objectStoreSession.ConsistentReadContext;
                objectStoreTransactionalConsistentReadContext = objectStoreSession.TransactionalConsistentReadContext;
            }

            var unifiedStoreFunctions = new UnifiedSessionFunctions(functionsState, readSessionState);
            var unifiedStoreSession = db.Store.NewSession<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions>(unifiedStoreFunctions, IsConsistentReadSession, sessionIdOverride: session.ID);

            var vectorFunctions = new VectorSessionFunctions(functionsState, readSessionState);
            var vectorSession = db.Store.NewSession<VectorElementKey, VectorInput, VectorOutput, long, VectorSessionFunctions>(vectorFunctions, sessionIdOverride: session.ID);

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
        }

        public void UpdateRespProtocolVersion(byte respProtocolVersion)
        {
            functionsState.respProtocolVersion = respProtocolVersion;
        }

        /// <summary>
        /// Operations of this session's string, object and unified contexts that have gone to the device and not yet been
        /// completed.
        /// </summary>
        internal int PendingOperationCount
            => stringBasicContext.Session.PendingOperationCount
                + (objectBasicContext.Session?.PendingOperationCount ?? 0)
                + unifiedBasicContext.Session.PendingOperationCount;

        /// <summary>
        /// The network session this session runs the commands of, which can park while a command waits, or null.
        /// </summary>
        internal RespServerSession parkingSession;

        /// <summary>
        /// Set while this session replays a record on a thread that must not wait. A wait the record reaches before it
        /// has changed anything throws <see cref="ReplayMustWaitException"/> instead, so the record is replayed again once
        /// the wait is over.
        /// </summary>
        internal bool replayMustNotWait;

        /// <summary>
        /// Hands waits on a latched key by this session's string, object and unified operations to <paramref name="session"/>,
        /// and has the store callbacks that change a record disarm its restart point.
        /// </summary>
        /// <param name="session">Decides how an operation waits on a latched key, and parks for other waits.</param>
        /// <remarks>
        /// The vector session is left without a handler: vector commands run in place (see
        /// <see cref="RespServerSession.LockWaitClass"/>).
        /// </remarks>
        internal void SetLockContentionHandler(RespServerSession session)
        {
            parkingSession = session;
            functionsState.restartPoint = session.restartPoint;
            stringBasicContext.Session.LockContentionHandler = session;
            if (objectBasicContext.Session is { } objectSession)
                objectSession.LockContentionHandler = session;
            unifiedBasicContext.Session.LockContentionHandler = session;
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