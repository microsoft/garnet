﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server.Objects.List;
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
        /// Session for main store
        /// </summary>
        public readonly ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> session;

        public BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> basicContext;
        public LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> lockableContext;

        SectorAlignedMemory sectorAlignedMemoryHll;
        readonly int hllBufferSize = HyperLogLog.DefaultHLL.DenseBytes;
        readonly int sectorAlignedMemoryPoolAlignment = 32;

        /// <summary>
        /// Session for object store
        /// </summary>
        public readonly ClientSession<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreSession;

        public BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreBasicContext;
        public LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreLockableContext;

        public readonly ScratchBufferManager scratchBufferManager;
        public readonly FunctionsState functionsState;

        public TransactionManager txnManager;
        readonly ILogger logger;
        private readonly ListItemBroker itemBroker;

        public int SessionID => session.ID;
        public int ObjectStoreSessionID => objectStoreSession.ID;

        public readonly int ObjectScanCountLimit;

        public StorageSession(StoreWrapper storeWrapper,
            ScratchBufferManager scratchBufferManager,
            GarnetSessionMetrics sessionMetrics,
            GarnetLatencyMetricsSession LatencyMetrics, 
            ListItemBroker itemBroker, 
            ILogger logger = null)
        {
            this.sessionMetrics = sessionMetrics;
            this.LatencyMetrics = LatencyMetrics;
            this.scratchBufferManager = scratchBufferManager;
            this.logger = logger;
            this.itemBroker = itemBroker;

            functionsState = storeWrapper.CreateFunctionsState();

            var functions = new MainStoreFunctions(functionsState);
            session = storeWrapper.store.NewSession<SpanByte, SpanByteAndMemory, long, MainStoreFunctions>(functions);

            var objstorefunctions = new ObjectStoreFunctions(functionsState);
            objectStoreSession = storeWrapper.objectStore?.NewSession<SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>(objstorefunctions);

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
            session.Dispose();
            objectStoreSession?.Dispose();
            sectorAlignedMemoryHll?.Dispose();
        }
    }
}