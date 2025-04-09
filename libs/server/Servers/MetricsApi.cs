// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using Garnet.common;

namespace Garnet.server
{
    /// <summary>
    /// Metrics API
    /// </summary>
    public class MetricsApi
    {
        readonly GarnetProvider provider;

        /// <summary>
        /// Construct new Metrics API instance
        /// </summary>
        public MetricsApi(GarnetProvider provider)
        {
            this.provider = provider;
        }

        /// <summary>
        /// Get info metrics for specified info type
        /// </summary>
        /// <param name="infoMetricsType"></param>
        /// <param name="dbId">Database ID for database-specific metrics</param>
        /// <returns></returns>
        public MetricsItem[] GetInfoMetrics(InfoMetricsType infoMetricsType, int dbId = 0)
        {
            GarnetInfoMetrics info = new();
            return info.GetMetric(infoMetricsType, dbId, provider.StoreWrapper);
        }

        /// <summary>
        /// Get info metrics for specified info types
        /// </summary>
        /// <param name="infoMetricsTypes">Info types to get, null to get all</param>
        /// <param name="dbId">Database ID for database-specific metrics</param>
        /// <returns></returns>
        public IEnumerable<(InfoMetricsType, MetricsItem[])> GetInfoMetrics(InfoMetricsType[] infoMetricsTypes = null, int dbId = 0)
        {
            GarnetInfoMetrics info = new();
            infoMetricsTypes ??= GarnetInfoMetrics.DefaultInfo;
            return info.GetInfoMetrics(infoMetricsTypes, dbId, provider.StoreWrapper);
        }

        /// <summary>
        /// Get header for given info metrics type
        /// </summary>
        /// <param name="infoMetricsType">Info types to get, null to get all</param>
        /// <param name="dbId">Database ID for database-specific metrics</param>
        /// <returns></returns>
        public static string GetHeader(InfoMetricsType infoMetricsType, int dbId = 0)
            => GarnetInfoMetrics.GetSectionHeader(infoMetricsType, dbId);

        /// <summary>
        /// Reset info metrics
        /// </summary>
        /// <param name="infoMetricsType"></param>
        public void ResetInfoMetrics(InfoMetricsType infoMetricsType)
        {
            if (provider.StoreWrapper.monitor != null)
                provider.StoreWrapper.monitor.resetEventFlags[infoMetricsType] = true;
        }

        /// <summary>
        /// Reset info metrics
        /// </summary>
        /// <param name="infoMetricsTypes">Info types to reset, null to reset all</param>
        public void ResetInfoMetrics(InfoMetricsType[] infoMetricsTypes = null)
        {
            infoMetricsTypes ??= GarnetInfoMetrics.DefaultInfo;
            for (int i = 0; i < infoMetricsTypes.Length; i++)
                ResetInfoMetrics(infoMetricsTypes[i]);
        }

        /// <summary>
        /// Get latency metrics (histogram) for specified latency type
        /// </summary>
        /// <param name="latencyMetricsType"></param>
        /// <returns></returns>
        public MetricsItem[] GetLatencyMetrics(LatencyMetricsType latencyMetricsType)
        {
            if (provider.StoreWrapper.monitor?.GlobalMetrics.globalLatencyMetrics == null) return [];
            return provider.StoreWrapper.monitor.GlobalMetrics.globalLatencyMetrics.GetLatencyMetrics(latencyMetricsType);
        }

        /// <summary>
        /// Get latency metrics (histograms) for specified latency types
        /// </summary>
        /// <param name="latencyMetricsTypes">Latency types to get, null to get all</param>
        /// <returns></returns>
        public IEnumerable<(LatencyMetricsType, MetricsItem[])> GetLatencyMetrics(LatencyMetricsType[] latencyMetricsTypes = null)
        {
            if (provider.StoreWrapper.monitor?.GlobalMetrics.globalLatencyMetrics == null) return [];
            latencyMetricsTypes ??= GarnetLatencyMetrics.defaultLatencyTypes;
            return provider.StoreWrapper.monitor?.GlobalMetrics.globalLatencyMetrics.GetLatencyMetrics(latencyMetricsTypes);
        }

        /// <summary>
        /// Reset latency histogram for eventType
        /// </summary>
        /// <param name="latencyMetricsType">Latency types to reset, null to reset all</param>
        public void ResetLatencyMetrics(LatencyMetricsType latencyMetricsType)
        {
            if (provider.StoreWrapper.monitor != null)
                provider.StoreWrapper.monitor.resetLatencyMetrics[latencyMetricsType] = true;
        }

        /// <summary>
        /// Reset latency histogram for eventTypes
        /// </summary>
        /// <param name="latencyMetricsTypes"></param>
        public void ResetLatencyMetrics(LatencyMetricsType[] latencyMetricsTypes = null)
        {
            latencyMetricsTypes ??= GarnetLatencyMetrics.defaultLatencyTypes;
            for (int i = 0; i < latencyMetricsTypes.Length; i++)
                ResetLatencyMetrics(latencyMetricsTypes[i]);
        }
    }
}