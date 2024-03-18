// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.devices
{
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Tsavorite trace helper
    /// </summary>
    public class TsavoriteTraceHelper
    {
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly ILogger performanceLogger;

        /// <summary>
        /// Create a trace helper for Tsavorite
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="logLevelLimit"></param>
        /// <param name="performanceLogger"></param>
        public TsavoriteTraceHelper(ILogger logger, LogLevel logLevelLimit, ILogger performanceLogger)
        {
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.performanceLogger = performanceLogger;
        }

        /// <summary>
        /// Is tracing at most detailed level
        /// </summary>
        public bool IsTracingAtMostDetailedLevel => logLevelLimit == LogLevel.Trace;


        /// <summary>
        /// Perf warning
        /// </summary>
        /// <param name="details"></param>
        public void TsavoritePerfWarning(string details)
        {
            if (logLevelLimit <= LogLevel.Warning)
            {
                performanceLogger?.LogWarning("Performance issue detected: {details}", details);
            }
        }

        /// <summary>
        /// Storage progress
        /// </summary>
        /// <param name="details"></param>
        public void TsavoriteStorageProgress(string details)
        {
            if (logLevelLimit <= LogLevel.Trace)
            {
                logger?.LogTrace("{details}", details);
            }
        }

        /// <summary>
        /// Azure storage access completed
        /// </summary>
        public void TsavoriteAzureStorageAccessCompleted(string intent, long size, string operation, string target, double latency, int attempt)
        {
            if (logLevelLimit <= LogLevel.Debug)
            {
                logger?.LogDebug("storage access completed intent={intent} size={size} operation={operation} target={target} latency={latency} attempt={attempt}",
                    intent, size, operation, target, latency, attempt);
            }
        }


        // ----- lease management events


        /// <summary>
        /// Lease acquired
        /// </summary>
        public void LeaseAcquired()
        {
            if (logLevelLimit <= LogLevel.Information)
            {
                logger?.LogInformation("PartitionLease acquired");
            }
        }

        /// <summary>
        /// Lease renewed
        /// </summary>
        public void LeaseRenewed(double elapsedSeconds, double timing)
        {
            if (logLevelLimit <= LogLevel.Debug)
            {
                logger?.LogDebug("PartitionLease renewed after {elapsedSeconds:F2}s timing={timing:F2}s", elapsedSeconds, timing);
            }
        }

        /// <summary>
        /// Lease released
        /// </summary>
        public void LeaseReleased(double elapsedSeconds)
        {
            if (logLevelLimit <= LogLevel.Information)
            {
                logger?.LogInformation("PartitionLease released after {elapsedSeconds:F2}s", elapsedSeconds);
            }
        }

        /// <summary>
        /// Lease lost
        /// </summary>
        public void LeaseLost(double elapsedSeconds, string operation)
        {
            if (logLevelLimit <= LogLevel.Warning)
            {
                logger?.LogWarning("PartitionLease lost after {elapsedSeconds:F2}s in {operation}", elapsedSeconds, operation);
            }
        }

        /// <summary>
        /// Lease progress
        /// </summary>
        public void LeaseProgress(string operation)
        {
            if (logLevelLimit <= LogLevel.Debug)
            {
                logger?.LogDebug("PartitionLease progress: {operation}", operation);
            }
        }
    }
}