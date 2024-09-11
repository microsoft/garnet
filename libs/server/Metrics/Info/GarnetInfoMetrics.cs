// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using Garnet.common;

namespace Garnet.server
{
    class GarnetInfoMetrics
    {
        public static readonly InfoMetricsType[] defaultInfo = Enum.GetValues<InfoMetricsType>()
            .Where(e => e switch
            {
                InfoMetricsType.STOREHASHTABLE => false,
                InfoMetricsType.OBJECTSTOREHASHTABLE => false,
                InfoMetricsType.STOREREVIV => false,
                InfoMetricsType.OBJECTSTOREREVIV => false,
                _ => true
            })
            .ToArray();

        MetricsItem[] serverInfo = null;
        MetricsItem[] memoryInfo = null;
        MetricsItem[] clusterInfo = null;
        MetricsItem[] replicationInfo = null;
        MetricsItem[] statsInfo = null;
        MetricsItem[] storeInfo = null;
        MetricsItem[] objectStoreInfo = null;
        MetricsItem[] storeHashDistrInfo = null;
        MetricsItem[] objectStoreHashDistrInfo = null;
        MetricsItem[] storeRevivInfo = null;
        MetricsItem[] objectStoreRevivInfo = null;
        MetricsItem[] persistenceInfo = null;
        MetricsItem[] clientsInfo = null;
        MetricsItem[] keyspaceInfo = null;
        MetricsItem[] bufferPoolStats = null;

        public GarnetInfoMetrics() { }

        private void PopulateServerInfo(StoreWrapper storeWrapper)
        {
            var uptime = TimeSpan.FromTicks(DateTimeOffset.UtcNow.Ticks - storeWrapper.startupTime);
            serverInfo =
            [
                new("garnet_version", storeWrapper.version),
                new("os", Environment.OSVersion.ToString()),
                new("processor_count", Environment.ProcessorCount.ToString()),
                new("arch_bits", Environment.Is64BitProcess ? "64" : "32"),
                new("uptime_in_seconds", ((int)uptime.TotalSeconds).ToString()),
                new("uptime_in_days", ((int)uptime.TotalDays).ToString()),
                new("monitor_task", storeWrapper.serverOptions.MetricsSamplingFrequency > 0 ? "enabled" : "disabled"),
                new("monitor_freq", storeWrapper.serverOptions.MetricsSamplingFrequency.ToString()),
                new("latency_monitor", storeWrapper.serverOptions.LatencyMonitor ? "enabled" : "disabled"),
                new("run_id", storeWrapper.run_id),
                new("redis_version", storeWrapper.redisProtocolVersion),
                new("redis_mode", storeWrapper.serverOptions.EnableCluster ? "cluster" : "standalone"),
            ];
        }

        private void PopulateMemoryInfo(StoreWrapper storeWrapper)
        {
            var main_store_index_size = storeWrapper.store.IndexSize * 64;
            var main_store_log_memory_size = storeWrapper.store.Log.MemorySizeBytes;
            var main_store_read_cache_size = (storeWrapper.store.ReadCache != null ? storeWrapper.store.ReadCache.MemorySizeBytes : 0);
            var total_main_store_size = main_store_index_size + main_store_log_memory_size + main_store_read_cache_size;

            var object_store_index_size = -1L;
            var object_store_log_memory_references_size = -1L;
            var object_store_read_cache_size = -1L;
            var total_object_store_size = -1L;
            var disableObj = storeWrapper.serverOptions.DisableObjects;

            if (!disableObj)
            {
                object_store_index_size = storeWrapper.objectStore.IndexSize * 64;
                object_store_log_memory_references_size = storeWrapper.objectStore.Log.MemorySizeBytes;
                object_store_read_cache_size = (storeWrapper.objectStore.ReadCache != null ? storeWrapper.objectStore.ReadCache.MemorySizeBytes : 0);
                total_object_store_size = object_store_index_size + object_store_log_memory_references_size + object_store_read_cache_size;
            }

            memoryInfo =
            [
                new("system_page_size", Environment.SystemPageSize.ToString()),
                new("total_system_memory", SystemMetrics.GetTotalMemory().ToString()),
                new("total_system_memory(MB)", SystemMetrics.GetTotalMemory(1 << 20).ToString()),
                new("available_system_memory", SystemMetrics.GetPhysicalAvailableMemory().ToString()),
                new("available_system_memory(MB)", SystemMetrics.GetPhysicalAvailableMemory(1 << 20).ToString()),
                new("proc_paged_memory_size", SystemMetrics.GetPagedMemorySize().ToString()),
                new("proc_paged_memory_size(MB)", SystemMetrics.GetPagedMemorySize(1 << 20).ToString()),
                new("proc_peak_paged_memory_size", SystemMetrics.GetPeakPagedMemorySize().ToString()),
                new("proc_peak_paged_memory_size(MB)", SystemMetrics.GetPeakPagedMemorySize(1 << 20).ToString()),
                new("proc_pageable_memory_size", SystemMetrics.GetPagedSystemMemorySize().ToString()),
                new("proc_pageable_memory_size(MB)", SystemMetrics.GetPagedSystemMemorySize(1 << 20).ToString()),
                new("proc_private_memory_size", SystemMetrics.GetPrivateMemorySize64().ToString()),
                new("proc_private_memory_size(MB)", SystemMetrics.GetPrivateMemorySize64(1 << 20).ToString()),
                new("proc_virtual_memory_size", SystemMetrics.GetVirtualMemorySize64().ToString()),
                new("proc_virtual_memory_size(MB)", SystemMetrics.GetVirtualMemorySize64(1 << 20).ToString()),
                new("proc_peak_virtual_memory_size", SystemMetrics.GetPeakVirtualMemorySize64().ToString()),
                new("proc_peak_virtual_memory_size(MB)", SystemMetrics.GetPeakVirtualMemorySize64(1 << 20).ToString()),
                new("proc_physical_memory_size", SystemMetrics.GetPhysicalMemoryUsage().ToString()),
                new("proc_physical_memory_size(MB)", SystemMetrics.GetPhysicalMemoryUsage(1 << 20).ToString()),
                new("proc_peak_physical_memory_size", SystemMetrics.GetPeakPhysicalMemoryUsage().ToString()),
                new("proc_peak_physical_memory_size(MB)", SystemMetrics.GetPeakPhysicalMemoryUsage(1 << 20).ToString()),
                new("main_store_index_size", main_store_index_size.ToString()),
                new("main_store_log_memory_size", main_store_log_memory_size.ToString()),
                new("main_store_read_cache_size", main_store_read_cache_size.ToString()),
                new("total_main_store_size", total_main_store_size.ToString()),
                new("object_store_index_size", object_store_index_size.ToString()),
                new("object_store_log_memory_references_size", object_store_log_memory_references_size.ToString()),
                new("object_store_read_cache_size", object_store_read_cache_size.ToString()),
                new("total_object_store_size", total_object_store_size.ToString())
            ];
        }

        private void PopulateClusterInfo(StoreWrapper storeWrapper)
        {
            clusterInfo = [new("cluster_enabled", storeWrapper.serverOptions.EnableCluster ? "1" : "0")];
        }

        private void PopulateReplicationInfo(StoreWrapper storeWrapper)
        {
            if (storeWrapper.clusterProvider == null)
            {
                replicationInfo =
                [
                    new("role", "master"),
                    new("connected_slaves", "0"),
                    new("master_failover_state", "no-failover"),
                    new("master_replid", Generator.DefaultHexId()),
                    new("master_replid2", Generator.DefaultHexId()),
                    new("master_repl_offset", "N/A"),
                    new("second_repl_offset", "N/A"),
                    new("store_current_safe_aof_address", "N/A"),
                    new("store_recovered_safe_aof_address", "N/A"),
                    new("object_store_current_safe_aof_address", "N/A"),
                    new("object_store_recovered_safe_aof_address", "N/A")
               ];
            }
            else
            {
                replicationInfo = storeWrapper.clusterProvider.GetReplicationInfo();
            }
        }

        private void PopulateStatsInfo(StoreWrapper storeWrapper)
        {
            var clusterEnabled = storeWrapper.serverOptions.EnableCluster;
            var metricsDisabled = storeWrapper.monitor == null;
            var globalMetrics = metricsDisabled ? default : storeWrapper.monitor.GlobalMetrics;
            var tt = metricsDisabled ? 0 : (double)(globalMetrics.globalSessionMetrics.get_total_found() + globalMetrics.globalSessionMetrics.get_total_notfound());
            var garnet_hit_rate = metricsDisabled ? 0 : (tt > 0 ? (double)globalMetrics.globalSessionMetrics.get_total_found() / tt : 0) * 100;
            statsInfo =
                [
                    new("total_connections_active", metricsDisabled ? "0" : globalMetrics.total_connections_received.ToString()),
                    new("total_connections_disposed", metricsDisabled ? "0" : globalMetrics.total_connections_disposed.ToString()),
                    new("total_commands_processed", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_commands_processed().ToString()),
                    new("instantaneous_ops_per_sec", metricsDisabled ? "0" : globalMetrics.instantaneous_cmd_per_sec.ToString()),
                    new("total_net_input_bytes", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_net_input_bytes().ToString()),
                    new("total_net_output_bytes", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_net_output_bytes().ToString()),
                    new("instantaneous_net_input_KBps", metricsDisabled ? "0" : globalMetrics.instantaneous_net_input_tpt.ToString()),
                    new("instantaneous_net_output_KBps", metricsDisabled ? "0" : globalMetrics.instantaneous_net_output_tpt.ToString()),
                    new("total_pending", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_pending().ToString()),
                    new("total_found", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_found().ToString()),
                    new("total_notfound", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_notfound().ToString()),
                    new("garnet_hit_rate", garnet_hit_rate.ToString("N2", CultureInfo.InvariantCulture)),
                    new("total_cluster_commands_processed", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_cluster_commands_processed().ToString()),
                    new("total_write_commands_processed", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_write_commands_processed().ToString()),
                    new("total_read_commands_processed", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_read_commands_processed().ToString()),
                    new("total_number_resp_server_session_exceptions", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_number_resp_server_session_exceptions().ToString())
                ];

            if (clusterEnabled)
            {
                var gossipStats = storeWrapper.clusterProvider.GetGossipStats(metricsDisabled);
                var tmp = new MetricsItem[statsInfo.Length + gossipStats.Length];
                Array.Copy(statsInfo, 0, tmp, 0, statsInfo.Length);
                Array.Copy(gossipStats, 0, tmp, statsInfo.Length, gossipStats.Length);
                statsInfo = tmp;
            }
        }

        private void PopulateStoreStats(StoreWrapper storeWrapper)
        {
            storeInfo =
                [
                    new("CurrentVersion", storeWrapper.store.CurrentVersion.ToString()),
                    new("LastCheckpointedVersion", storeWrapper.store.LastCheckpointedVersion.ToString()),
                    new("RecoveredVersion", storeWrapper.store.RecoveredVersion.ToString()),
                    new("SystemState", storeWrapper.store.SystemState.ToString()),
                    new("IndexSize", storeWrapper.store.IndexSize.ToString()),
                    new("LogDir", storeWrapper.serverOptions.LogDir),
                    new("Log.BeginAddress", storeWrapper.store.Log.BeginAddress.ToString()),
                    new("Log.BufferSize", storeWrapper.store.Log.BufferSize.ToString()),
                    new("Log.EmptyPageCount", storeWrapper.store.Log.EmptyPageCount.ToString()),
                    new("Log.FixedRecordSize", storeWrapper.store.Log.FixedRecordSize.ToString()),
                    new("Log.HeadAddress", storeWrapper.store.Log.HeadAddress.ToString()),
                    new("Log.MemorySizeBytes", storeWrapper.store.Log.MemorySizeBytes.ToString()),
                    new("Log.SafeReadOnlyAddress", storeWrapper.store.Log.SafeReadOnlyAddress.ToString()),
                    new("Log.TailAddress", storeWrapper.store.Log.TailAddress.ToString())
                ];
        }

        private void PopulateObjectStoreStats(StoreWrapper storeWrapper)
        {
            objectStoreInfo =
                [
                    new("CurrentVersion", storeWrapper.objectStore.CurrentVersion.ToString()),
                    new("LastCheckpointedVersion", storeWrapper.objectStore.LastCheckpointedVersion.ToString()),
                    new("RecoveredVersion", storeWrapper.objectStore.RecoveredVersion.ToString()),
                    new("SystemState", storeWrapper.objectStore.SystemState.ToString()),
                    new("IndexSize", storeWrapper.objectStore.IndexSize.ToString()),
                    new("LogDir", storeWrapper.serverOptions.LogDir),
                    new("Log.BeginAddress", storeWrapper.objectStore.Log.BeginAddress.ToString()),
                    new("Log.BufferSize", storeWrapper.objectStore.Log.BufferSize.ToString()),
                    new("Log.EmptyPageCount", storeWrapper.objectStore.Log.EmptyPageCount.ToString()),
                    new("Log.FixedRecordSize", storeWrapper.objectStore.Log.FixedRecordSize.ToString()),
                    new("Log.HeadAddress", storeWrapper.objectStore.Log.HeadAddress.ToString()),
                    new("Log.MemorySizeBytes", storeWrapper.objectStore.Log.MemorySizeBytes.ToString()),
                    new("Log.SafeReadOnlyAddress", storeWrapper.objectStore.Log.SafeReadOnlyAddress.ToString()),
                    new("Log.TailAddress", storeWrapper.objectStore.Log.TailAddress.ToString())
                ];
        }

        private void PopulateStoreHashDistribution(StoreWrapper storeWrapper) => storeHashDistrInfo = [new("", storeWrapper.store.DumpDistribution())];

        private void PopulateObjectStoreHashDistribution(StoreWrapper storeWrapper) => objectStoreHashDistrInfo = [new("", storeWrapper.objectStore.DumpDistribution())];

        private void PopulateStoreRevivInfo(StoreWrapper storeWrapper) => storeRevivInfo = [new("", storeWrapper.store.DumpRevivificationStats())];

        private void PopulateObjectStoreRevivInfo(StoreWrapper storeWrapper) => objectStoreRevivInfo = [new("", storeWrapper.objectStore.DumpRevivificationStats())];

        private void PopulatePersistenceInfo(StoreWrapper storeWrapper)
        {
            bool aofEnabled = storeWrapper.serverOptions.EnableAOF;
            persistenceInfo =
                [
                    new("CommittedBeginAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.CommittedBeginAddress.ToString()),
                    new("CommittedUntilAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.CommittedUntilAddress.ToString()),
                    new("FlushedUntilAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.FlushedUntilAddress.ToString()),
                    new("BeginAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.BeginAddress.ToString()),
                    new("TailAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.TailAddress.ToString()),
                    new("SafeAofAddress", !aofEnabled ? "N/A" : storeWrapper.SafeAofAddress.ToString())
                ];
        }

        private void PopulateClientsInfo(StoreWrapper storeWrapper)
        {
            var metricsDisabled = storeWrapper.monitor == null;
            var globalMetrics = metricsDisabled ? default : storeWrapper.monitor.GlobalMetrics;
            clientsInfo = [new("connected_clients", metricsDisabled ? "0" : (globalMetrics.total_connections_received - globalMetrics.total_connections_disposed).ToString())];
        }

        private void PopulateKeyspaceInfo(StoreWrapper storeWrapper)
        {
            keyspaceInfo = null;
        }

        private void PopulateClusterBufferPoolStats(StoreWrapper storeWrapper)
        {
            if (storeWrapper.clusterProvider != null)
            {
                bufferPoolStats = storeWrapper.clusterProvider.GetBufferPoolStats();
            }
        }

        public static string GetSectionHeader(InfoMetricsType infoType)
        {
            return infoType switch
            {
                InfoMetricsType.SERVER => "Server",
                InfoMetricsType.MEMORY => "Memory",
                InfoMetricsType.CLUSTER => "Cluster",
                InfoMetricsType.REPLICATION => "Replication",
                InfoMetricsType.STATS => "Stats",
                InfoMetricsType.STORE => "Main Store",
                InfoMetricsType.OBJECTSTORE => "Object Store",
                InfoMetricsType.STOREHASHTABLE => "Main Store Hash Table Distribution",
                InfoMetricsType.OBJECTSTOREHASHTABLE => "Object Store Hash Table Distribution",
                InfoMetricsType.STOREREVIV => "Main Store Deleted Record Revivification",
                InfoMetricsType.OBJECTSTOREREVIV => "Object Store Deleted Record Revivification",
                InfoMetricsType.PERSISTENCE => "Persistence",
                InfoMetricsType.CLIENTS => "Clients",
                InfoMetricsType.KEYSPACE => "Keyspace",
                InfoMetricsType.MODULES => "Modules",
                InfoMetricsType.BPSTATS => "BufferPool Stats",
                _ => "Default",
            };
        }

        private static string GetSectionRespInfo(InfoMetricsType infoType, MetricsItem[] info)
        {
            var section = $"# {GetSectionHeader(infoType)}\r\n";
            if (info == null)
                return section;

            // For some metrics we have a multi-string in the value and no name, so don't print a stray leading ':'.
            if (string.IsNullOrEmpty(info[0].Name))
            {
                Debug.Assert(info.Length == 1, "Unexpected empty name in first entry of multi-entry metrics info");
                section += $"{info[0].Value}\r\n";
            }
            else
            {
                for (var i = 0; i < info.Length; i++)
                    section += $"{info[i].Name}:{info[i].Value}\r\n";
            }
            return section;
        }

        public string GetRespInfo(InfoMetricsType section, StoreWrapper storeWrapper)
        {
            switch (section)
            {
                case InfoMetricsType.SERVER:
                    PopulateServerInfo(storeWrapper);
                    return GetSectionRespInfo(section, serverInfo);
                case InfoMetricsType.MEMORY:
                    PopulateMemoryInfo(storeWrapper);
                    return GetSectionRespInfo(section, memoryInfo);
                case InfoMetricsType.CLUSTER:
                    PopulateClusterInfo(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.CLUSTER, clusterInfo);
                case InfoMetricsType.REPLICATION:
                    PopulateReplicationInfo(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.REPLICATION, replicationInfo);
                case InfoMetricsType.STATS:
                    PopulateStatsInfo(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.STATS, statsInfo);
                case InfoMetricsType.STORE:
                    PopulateStoreStats(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.STORE, storeInfo);
                case InfoMetricsType.OBJECTSTORE:
                    if (storeWrapper.objectStore == null) return "";
                    PopulateObjectStoreStats(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.OBJECTSTORE, objectStoreInfo);
                case InfoMetricsType.STOREHASHTABLE:
                    PopulateStoreHashDistribution(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.STOREHASHTABLE, storeHashDistrInfo);
                case InfoMetricsType.OBJECTSTOREHASHTABLE:
                    if (storeWrapper.objectStore == null) return "";
                    PopulateObjectStoreHashDistribution(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.OBJECTSTOREHASHTABLE, objectStoreHashDistrInfo);
                case InfoMetricsType.STOREREVIV:
                    PopulateStoreRevivInfo(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.STOREREVIV, storeRevivInfo);
                case InfoMetricsType.OBJECTSTOREREVIV:
                    if (storeWrapper.objectStore == null) return "";
                    PopulateObjectStoreRevivInfo(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.OBJECTSTOREREVIV, objectStoreRevivInfo);
                case InfoMetricsType.PERSISTENCE:
                    if (storeWrapper.appendOnlyFile == null) return "";
                    PopulatePersistenceInfo(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.PERSISTENCE, persistenceInfo);
                case InfoMetricsType.CLIENTS:
                    PopulateClientsInfo(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.CLIENTS, clientsInfo);
                case InfoMetricsType.KEYSPACE:
                    PopulateKeyspaceInfo(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.KEYSPACE, keyspaceInfo);
                case InfoMetricsType.MODULES:
                    return GetSectionRespInfo(section, null);
                case InfoMetricsType.BPSTATS:
                    PopulateClusterBufferPoolStats(storeWrapper);
                    return GetSectionRespInfo(InfoMetricsType.BPSTATS, bufferPoolStats);
                default:
                    return "";
            }
        }

        public string GetRespInfo(InfoMetricsType[] sections, StoreWrapper storeWrapper)
        {
            var response = "";
            for (var i = 0; i < sections.Length; i++)
            {
                var section = sections[i];
                var resp = GetRespInfo(section, storeWrapper);
                if (string.IsNullOrEmpty(resp)) continue;
                response += resp;
                response += sections.Length - 1 == i ? "" : "\r\n";
            }
            return response;
        }

        private MetricsItem[] GetMetricInternal(InfoMetricsType section, StoreWrapper storeWrapper)
        {
            switch (section)
            {
                case InfoMetricsType.SERVER:
                    PopulateServerInfo(storeWrapper);
                    return serverInfo;
                case InfoMetricsType.MEMORY:
                    PopulateMemoryInfo(storeWrapper);
                    return memoryInfo;
                case InfoMetricsType.CLUSTER:
                    PopulateClusterInfo(storeWrapper);
                    return clusterInfo;
                case InfoMetricsType.REPLICATION:
                    PopulateReplicationInfo(storeWrapper);
                    return replicationInfo;
                case InfoMetricsType.STATS:
                    PopulateStatsInfo(storeWrapper);
                    return statsInfo;
                case InfoMetricsType.STORE:
                    PopulateStoreStats(storeWrapper);
                    return storeInfo;
                case InfoMetricsType.OBJECTSTORE:
                    if (storeWrapper.objectStore == null) return null;
                    PopulateObjectStoreStats(storeWrapper);
                    return objectStoreInfo;
                case InfoMetricsType.STOREHASHTABLE:
                    PopulateStoreHashDistribution(storeWrapper);
                    return storeHashDistrInfo;
                case InfoMetricsType.OBJECTSTOREHASHTABLE:
                    if (storeWrapper.objectStore == null) return null;
                    PopulateObjectStoreHashDistribution(storeWrapper);
                    return objectStoreHashDistrInfo;
                case InfoMetricsType.STOREREVIV:
                    PopulateStoreRevivInfo(storeWrapper);
                    return storeRevivInfo;
                case InfoMetricsType.OBJECTSTOREREVIV:
                    if (storeWrapper.objectStore == null) return null;
                    PopulateObjectStoreRevivInfo(storeWrapper);
                    return objectStoreRevivInfo;
                case InfoMetricsType.PERSISTENCE:
                    if (storeWrapper.appendOnlyFile == null) return null;
                    PopulatePersistenceInfo(storeWrapper);
                    return persistenceInfo;
                case InfoMetricsType.CLIENTS:
                    PopulateClientsInfo(storeWrapper);
                    return clientsInfo;
                case InfoMetricsType.KEYSPACE:
                    PopulateKeyspaceInfo(storeWrapper);
                    return keyspaceInfo;
                case InfoMetricsType.MODULES:
                    return null;
                default:
                    return null;
            }
        }

        public MetricsItem[] GetMetric(InfoMetricsType section, StoreWrapper storeWrapper) => GetMetricInternal(section, storeWrapper);

        public IEnumerable<(InfoMetricsType, MetricsItem[])> GetInfoMetrics(InfoMetricsType[] sections, StoreWrapper storeWrapper)
        {
            for (var i = 0; i < sections.Length; i++)
            {
                var infoType = sections[i];
                var infoItems = GetMetricInternal(infoType, storeWrapper);
                if (infoItems != null)
                    yield return (infoType, infoItems);
            }
        }
    }
}