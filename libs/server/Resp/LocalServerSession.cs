// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
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
        readonly ScratchBufferBuilder scratchBufferBuilder;

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
            this.scratchBufferBuilder = new ScratchBufferBuilder();

            var dbRes = storeWrapper.TryGetOrAddDatabase(0, out var database, out _);
            Debug.Assert(dbRes, "Should always be able to get DB 0");

            // Create storage session and API
            this.storageSession = new StorageSession(storeWrapper, scratchBufferBuilder, sessionMetrics, LatencyMetrics, dbId: 0, database.VectorManager, logger);

            this.BasicGarnetApi = new BasicGarnetApi(storageSession, storageSession.stringBasicContext, storageSession.objectBasicContext, storageSession.unifiedBasicContext);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            logger?.LogDebug("Disposing LocalServerSession");

            if (storeWrapper.serverOptions.MetricsSamplingFrequency > 0 || storeWrapper.serverOptions.LatencyMonitor)
                storeWrapper.monitor.AddMetricsHistorySessionDispose(sessionMetrics, LatencyMetrics);

            storageSession.Dispose();
        }
    }
}