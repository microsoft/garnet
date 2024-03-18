// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using BasicGarnetApi = GarnetApi<BasicContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions>, BasicContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions>>;

    /// <summary>
    /// Local server session
    /// </summary>
    public class LocalServerSession : IDisposable
    {
        readonly GarnetSessionMetrics sessionMetrics;
        readonly GarnetLatencyMetricsSession LatencyMetrics;
        readonly ILogger logger = null;
        readonly StoreWrapper storeWrapper;
        readonly StorageSession storageSession;
        readonly ScratchBufferManager scratchBufferManager;

        /// <summary>
        /// Basic Garnet API
        /// </summary>
        public BasicGarnetApi BasicGarnetApi;

        /// <summary>
        /// Create new local server session
        /// </summary>
        public LocalServerSession(StoreWrapper storeWrapper)
        {
            this.storeWrapper = storeWrapper;

            this.sessionMetrics = storeWrapper.serverOptions.MetricsSamplingFrequency > 0 ? new GarnetSessionMetrics() : null;
            this.LatencyMetrics = storeWrapper.serverOptions.LatencyMonitor ? new GarnetLatencyMetricsSession(storeWrapper.monitor) : null;
            logger = storeWrapper.sessionLogger != null ? new SessionLogger(storeWrapper.sessionLogger, $"[local] [local] [{GetHashCode():X8}] ") : null;

            logger?.LogDebug("Starting LocalServerSession");

            // Initialize session-local scratch buffer of size 64 bytes, used for constructing arguments in GarnetApi
            this.scratchBufferManager = new ScratchBufferManager();

            // Create storage session and API
            this.storageSession = new StorageSession(storeWrapper, scratchBufferManager, sessionMetrics, LatencyMetrics, logger);

            this.BasicGarnetApi = new BasicGarnetApi(storageSession, storageSession.basicContext, storageSession.objectStoreBasicContext);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            logger?.LogDebug("Disposing LocalServerSession");

            if (storeWrapper.serverOptions.MetricsSamplingFrequency > 0 || storeWrapper.serverOptions.LatencyMonitor)
                storeWrapper.monitor.AddMetricsHistory(sessionMetrics, LatencyMetrics);

            storageSession.Dispose();
        }
    }
}