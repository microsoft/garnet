// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Garnet.common;

namespace Garnet.server
{
    class GarnetInfoMetrics
    {
        public static readonly InfoMetricsType[] defaultInfo = Enum.GetValues(typeof(InfoMetricsType)).Cast<InfoMetricsType>()
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

        public GarnetInfoMetrics() { }

        private void PopulateServerInfo(StoreWrapper storeWrapper)
        {
            var count = 11;
            serverInfo = new MetricsItem[count];
            serverInfo[0] = (new("garnet_version", storeWrapper.version));
            serverInfo[1] = (new("garnet_mode", storeWrapper.serverOptions.EnableCluster ? "cluster" : "standalone"));
            serverInfo[2] = (new("os", Environment.OSVersion.ToString()));
            serverInfo[3] = (new("processor_count", Environment.ProcessorCount.ToString()));
            serverInfo[4] = (new("arch_bits", Environment.Is64BitProcess ? "64" : "32"));
            var uptime = TimeSpan.FromTicks(DateTimeOffset.UtcNow.Ticks - storeWrapper.startupTime);
            serverInfo[5] = (new("uptime_in_seconds", uptime.TotalSeconds.ToString()));
            serverInfo[6] = (new("uptime_in_days", uptime.TotalDays.ToString()));
            serverInfo[7] = (new("monitor_task", storeWrapper.serverOptions.MetricsSamplingFrequency > 0 ? "enabled" : "disabled"));
            serverInfo[8] = (new("monitor_freq", storeWrapper.serverOptions.MetricsSamplingFrequency.ToString()));
            serverInfo[9] = (new("latency_monitor", storeWrapper.serverOptions.LatencyMonitor ? "enabled" : "disabled"));
            serverInfo[10] = (new("run_id", storeWrapper.run_id));
        }

        private void PopulateMemoryInfo(StoreWrapper storeWrapper)
        {
            if (storeWrapper.objectStore != null)
                memoryInfo = new MetricsItem[29];
            else
                memoryInfo = new MetricsItem[25];

            memoryInfo[0] = (new("system_page_size", Environment.SystemPageSize.ToString()));
            memoryInfo[1] = (new("total_system_memory", SystemMetrics.GetTotalMemory().ToString()));
            memoryInfo[2] = (new("total_system_memory(MB)", SystemMetrics.GetTotalMemory(1 << 20).ToString()));
            memoryInfo[3] = (new("available_system_memory", SystemMetrics.GetPhysicalAvailableMemory().ToString()));
            memoryInfo[4] = (new("available_system_memory(MB)", SystemMetrics.GetPhysicalAvailableMemory(1 << 20).ToString()));
            memoryInfo[5] = (new("proc_paged_memory_size", SystemMetrics.GetPagedMemorySize().ToString()));
            memoryInfo[6] = (new("proc_paged_memory_size(MB)", SystemMetrics.GetPagedMemorySize(1 << 20).ToString()));
            memoryInfo[7] = (new("proc_peak_paged_memory_size", SystemMetrics.GetPeakPagedMemorySize().ToString()));
            memoryInfo[8] = (new("proc_peak_paged_memory_size(MB)", SystemMetrics.GetPeakPagedMemorySize(1 << 20).ToString()));
            memoryInfo[9] = (new("proc_pageable_memory_size", SystemMetrics.GetPagedSystemMemorySize().ToString()));
            memoryInfo[10] = (new("proc_pageable_memory_size(MB)", SystemMetrics.GetPagedSystemMemorySize(1 << 20).ToString()));
            memoryInfo[11] = (new("proc_private_memory_size", SystemMetrics.GetPrivateMemorySize64().ToString()));
            memoryInfo[12] = (new("proc_private_memory_size(MB)", SystemMetrics.GetPrivateMemorySize64(1 << 20).ToString()));
            memoryInfo[13] = (new("proc_virtual_memory_size", SystemMetrics.GetVirtualMemorySize64().ToString()));
            memoryInfo[14] = (new("proc_virtual_memory_size(MB)", SystemMetrics.GetVirtualMemorySize64(1 << 20).ToString()));
            memoryInfo[15] = (new("proc_peak_virtual_memory_size", SystemMetrics.GetPeakVirtualMemorySize64().ToString()));
            memoryInfo[16] = (new("proc_peak_virtual_memory_size(MB)", SystemMetrics.GetPeakVirtualMemorySize64(1 << 20).ToString()));
            memoryInfo[17] = (new("proc_physical_memory_size", SystemMetrics.GetPhysicalMemoryUsage().ToString()));
            memoryInfo[18] = (new("proc_physical_memory_size(MB)", SystemMetrics.GetPhysicalMemoryUsage(1 << 20).ToString()));
            memoryInfo[19] = (new("proc_peak_physical_memory_size", SystemMetrics.GetPeakPhysicalMemoryUsage().ToString()));
            memoryInfo[20] = (new("proc_peak_physical_memory_size(MB)", SystemMetrics.GetPeakPhysicalMemoryUsage(1 << 20).ToString()));

            long main_store_index_size = storeWrapper.store.IndexSize * 64;
            long main_store_log_memory_size = storeWrapper.store.Log.MemorySizeBytes;
            long main_store_read_cache_size = (storeWrapper.store.ReadCache != null ? storeWrapper.store.ReadCache.MemorySizeBytes : 0);
            long total_main_store_size = main_store_index_size + main_store_log_memory_size + main_store_read_cache_size;
            memoryInfo[21] = (new("main_store_index_size", main_store_index_size.ToString()));
            memoryInfo[22] = (new("main_store_log_memory_size", main_store_log_memory_size.ToString()));
            memoryInfo[23] = (new("main_store_read_cache_size", main_store_read_cache_size.ToString()));
            memoryInfo[24] = (new("total_main_store_size", total_main_store_size.ToString()));

            if (storeWrapper.objectStore != null)
            {
                long object_store_index_size = storeWrapper.objectStore.IndexSize * 64;
                long object_store_log_memory_references_size = storeWrapper.objectStore.Log.MemorySizeBytes;
                long object_store_read_cache_size = (storeWrapper.objectStore.ReadCache != null ? storeWrapper.objectStore.ReadCache.MemorySizeBytes : 0);
                long total_object_store_size = object_store_index_size + object_store_log_memory_references_size + object_store_read_cache_size;
                memoryInfo[25] = (new("object_store_index_size", object_store_index_size.ToString()));
                memoryInfo[26] = (new("object_store_log_memory_references_size", object_store_log_memory_references_size.ToString()));
                memoryInfo[27] = (new("object_store_read_cache_size", object_store_read_cache_size.ToString()));
                memoryInfo[28] = (new("total_object_store_size", total_object_store_size.ToString()));
            }
        }

        private void PopulateClusterInfo(StoreWrapper storeWrapper)
        {
            clusterInfo = new MetricsItem[1];
            clusterInfo[0] = (new("cluster_enabled", storeWrapper.serverOptions.EnableCluster ? "1" : "0"));
        }

        private void PopulateReplicationInfo(StoreWrapper storeWrapper)
        {
            replicationInfo = storeWrapper.clusterProvider != null ? storeWrapper.clusterProvider.GetReplicationInfo() : GetReplicationInfo();
        }

        MetricsItem[] GetReplicationInfo()
        {
            int replicationInfoCount = 11;
            var replicationInfo = new MetricsItem[replicationInfoCount];

            replicationInfo[0] = new("role", "master");
            replicationInfo[1] = new("connected_slaves", "0");
            replicationInfo[2] = new("master_failover_state", "no-failover");
            replicationInfo[3] = new("master_replid", Generator.DefaultHexId());
            replicationInfo[4] = new("master_replid2", Generator.DefaultHexId());
            replicationInfo[5] = new("master_repl_offset", "N/A");
            replicationInfo[6] = new("second_repl_offset", "N/A");
            replicationInfo[7] = new("store_current_safe_aof_address", "N/A");
            replicationInfo[8] = new("store_recovered_safe_aof_address", "N/A");
            replicationInfo[9] = new("object_store_current_safe_aof_address", "N/A");
            replicationInfo[10] = new("object_store_recovered_safe_aof_address", "N/A");

            return replicationInfo;
        }

        private void PopulateStatsInfo(StoreWrapper storeWrapper)
        {
            var clusterEnabled = storeWrapper.serverOptions.EnableCluster;
            var basicInfoCount = 16;
            var count = clusterEnabled ? basicInfoCount + 10 : basicInfoCount;
            var metricsDisabled = storeWrapper.monitor == null;
            statsInfo = new MetricsItem[count];
            GarnetServerMetrics globalMetrics = metricsDisabled ? default : storeWrapper.monitor.GlobalMetrics;
            statsInfo[0] = (new("total_connections_active", metricsDisabled ? "0" : globalMetrics.total_connections_received.ToString()));
            statsInfo[1] = (new("total_connections_disposed", metricsDisabled ? "0" : globalMetrics.total_connections_disposed.ToString()));
            statsInfo[2] = (new("total_commands_processed", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_commands_processed().ToString()));
            statsInfo[3] = (new("instantaneous_ops_per_sec", metricsDisabled ? "0" : globalMetrics.instantaneous_cmd_per_sec.ToString()));
            statsInfo[4] = (new("total_net_input_bytes", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_net_input_bytes().ToString()));
            statsInfo[5] = (new("total_net_output_bytes", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_net_output_bytes().ToString()));
            statsInfo[6] = (new("instantaneous_net_input_KBps", metricsDisabled ? "0" : globalMetrics.instantaneous_net_input_tpt.ToString()));
            statsInfo[7] = (new("instantaneous_net_output_KBps", metricsDisabled ? "0" : globalMetrics.instantaneous_net_output_tpt.ToString()));
            statsInfo[8] = (new("total_pending", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_pending().ToString()));
            statsInfo[9] = (new("total_found", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_found().ToString()));
            statsInfo[10] = (new("total_notfound", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_notfound().ToString()));
            double tt = metricsDisabled ? 0 : (double)(globalMetrics.globalSessionMetrics.get_total_found() + globalMetrics.globalSessionMetrics.get_total_notfound());
            double garnet_hit_rate = metricsDisabled ? 0 : (tt > 0 ? (double)globalMetrics.globalSessionMetrics.get_total_found() / tt : 0) * 100;
            statsInfo[11] = (new("garnet_hit_rate", garnet_hit_rate.ToString("N2")));
            statsInfo[12] = (new("total_cluster_commands_processed", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_cluster_commands_processed().ToString()));
            statsInfo[13] = (new("total_write_commands_processed", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_write_commands_processed().ToString()));
            statsInfo[14] = (new("total_read_commands_processed", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_read_commands_processed().ToString()));
            statsInfo[15] = (new("total_number_resp_server_session_exceptions", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_number_resp_server_session_exceptions().ToString()));

            if (clusterEnabled)
                storeWrapper.clusterProvider.GetGossipInfo(statsInfo, basicInfoCount, metricsDisabled);
        }

        private void PopulateStoreStats(StoreWrapper storeWrapper)
        {
            storeInfo = new MetricsItem[14];
            storeInfo[0] = (new("CurrentVersion", storeWrapper.store.CurrentVersion.ToString()));
            storeInfo[1] = (new("LastCheckpointedVersion", storeWrapper.store.LastCheckpointedVersion.ToString()));
            storeInfo[2] = (new("RecoveredVersion", storeWrapper.store.RecoveredVersion.ToString()));
            storeInfo[3] = (new("SystemState", storeWrapper.store.SystemState.ToString()));
            storeInfo[4] = (new("IndexSize", storeWrapper.store.IndexSize.ToString()));
            storeInfo[5] = (new("LogDir", storeWrapper.serverOptions.LogDir));

            storeInfo[6] = (new("Log.BeginAddress", storeWrapper.store.Log.BeginAddress.ToString()));
            storeInfo[7] = (new("Log.BufferSize", storeWrapper.store.Log.BufferSize.ToString()));
            storeInfo[8] = (new("Log.EmptyPageCount", storeWrapper.store.Log.EmptyPageCount.ToString()));
            storeInfo[9] = (new("Log.FixedRecordSize", storeWrapper.store.Log.FixedRecordSize.ToString()));
            storeInfo[10] = (new("Log.HeadAddress", storeWrapper.store.Log.HeadAddress.ToString()));
            storeInfo[11] = (new("Log.MemorySizeBytes", storeWrapper.store.Log.MemorySizeBytes.ToString()));
            storeInfo[12] = (new("Log.SafeReadOnlyAddress", storeWrapper.store.Log.SafeReadOnlyAddress.ToString()));
            storeInfo[13] = (new("Log.TailAddress", storeWrapper.store.Log.TailAddress.ToString()));
        }

        private void PopulateObjectStoreStats(StoreWrapper storeWrapper)
        {
            objectStoreInfo = new MetricsItem[14];
            objectStoreInfo[0] = (new("CurrentVersion", storeWrapper.objectStore.CurrentVersion.ToString()));
            objectStoreInfo[1] = (new("LastCheckpointedVersion", storeWrapper.objectStore.LastCheckpointedVersion.ToString()));
            objectStoreInfo[2] = (new("RecoveredVersion", storeWrapper.objectStore.RecoveredVersion.ToString()));
            objectStoreInfo[3] = (new("SystemState", storeWrapper.objectStore.SystemState.ToString()));
            objectStoreInfo[4] = (new("IndexSize", storeWrapper.objectStore.IndexSize.ToString()));
            objectStoreInfo[5] = (new("LogDir", storeWrapper.serverOptions.LogDir));

            objectStoreInfo[6] = (new("Log.BeginAddress", storeWrapper.objectStore.Log.BeginAddress.ToString()));
            objectStoreInfo[7] = (new("Log.BufferSize", storeWrapper.objectStore.Log.BufferSize.ToString()));
            objectStoreInfo[8] = (new("Log.EmptyPageCount", storeWrapper.objectStore.Log.EmptyPageCount.ToString()));
            objectStoreInfo[9] = (new("Log.FixedRecordSize", storeWrapper.objectStore.Log.FixedRecordSize.ToString()));
            objectStoreInfo[10] = (new("Log.HeadAddress", storeWrapper.objectStore.Log.HeadAddress.ToString()));
            objectStoreInfo[11] = (new("Log.MemorySizeBytes", storeWrapper.objectStore.Log.MemorySizeBytes.ToString()));
            objectStoreInfo[12] = (new("Log.SafeReadOnlyAddress", storeWrapper.objectStore.Log.SafeReadOnlyAddress.ToString()));
            objectStoreInfo[13] = (new("Log.TailAddress", storeWrapper.objectStore.Log.TailAddress.ToString()));
        }

        public void PopulateStoreHashDistribution(StoreWrapper storeWrapper) => storeHashDistrInfo = new MetricsItem[] { new("", storeWrapper.store.DumpDistribution()) };

        public void PopulateObjectStoreHashDistribution(StoreWrapper storeWrapper) => objectStoreHashDistrInfo = new MetricsItem[] { new("", storeWrapper.objectStore.DumpDistribution()) };

        public void PopulateStoreRevivInfo(StoreWrapper storeWrapper) => storeRevivInfo = new MetricsItem[] { new("", storeWrapper.store.DumpRevivificationStats()) };

        public void PopulateObjectStoreRevivInfo(StoreWrapper storeWrapper) => objectStoreRevivInfo = new MetricsItem[] { new("", storeWrapper.objectStore.DumpRevivificationStats()) };

        public void PopulatePersistenceInfo(StoreWrapper storeWrapper)
        {
            persistenceInfo = new MetricsItem[6];
            bool aofEnabled = storeWrapper.serverOptions.EnableAOF;
            persistenceInfo[0] = (new("CommittedBeginAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.CommittedBeginAddress.ToString()));
            persistenceInfo[1] = (new("CommittedUntilAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.CommittedUntilAddress.ToString()));
            persistenceInfo[2] = (new("FlushedUntilAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.FlushedUntilAddress.ToString()));
            persistenceInfo[3] = (new("BeginAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.BeginAddress.ToString()));
            persistenceInfo[4] = (new("TailAddress", !aofEnabled ? "N/A" : storeWrapper.appendOnlyFile.TailAddress.ToString()));
            persistenceInfo[5] = (new("SafeAofAddress", !aofEnabled ? "N/A" : storeWrapper.SafeAofAddress.ToString()));
        }

        private void PopulateClientsInfo(StoreWrapper storeWrapper)
        {
            var metricsDisabled = storeWrapper.monitor == null;
            clientsInfo = new MetricsItem[1];
            GarnetServerMetrics globalMetrics = metricsDisabled ? default : storeWrapper.monitor.GlobalMetrics;
            clientsInfo[0] = (new("connected_clients", metricsDisabled ? "0" : globalMetrics.total_connections_received.ToString()));
        }

        private void PopulateKeyspaceInfo(StoreWrapper storeWrapper)
        {
            keyspaceInfo = null;
        }

        public static string GetSectionHeader(InfoMetricsType infoType)
        {
            switch (infoType)
            {
                case InfoMetricsType.SERVER:
                    return "Server";
                case InfoMetricsType.MEMORY:
                    return "Memory";
                case InfoMetricsType.CLUSTER:
                    return "Cluster";
                case InfoMetricsType.REPLICATION:
                    return "Replication";
                case InfoMetricsType.STATS:
                    return "Stats";
                case InfoMetricsType.STORE:
                    return "Main Store";
                case InfoMetricsType.OBJECTSTORE:
                    return "Object Store";
                case InfoMetricsType.STOREHASHTABLE:
                    return "Main Store Hash Table Distribution";
                case InfoMetricsType.OBJECTSTOREHASHTABLE:
                    return "Object Store Hash Table Distribution";
                case InfoMetricsType.STOREREVIV:
                    return "Main Store Deleted Record Revivification";
                case InfoMetricsType.OBJECTSTOREREVIV:
                    return "Object Store Deleted Record Revivification";
                case InfoMetricsType.PERSISTENCE:
                    return "Persistence";
                case InfoMetricsType.CLIENTS:
                    return "Clients";
                case InfoMetricsType.KEYSPACE:
                    return "Keyspace";
                default:
                    return "Default";
            }
        }

        private string GetSectionRespInfo(InfoMetricsType infoType, MetricsItem[] info)
        {
            if (info == null)
                return "";
            string section = $"# {GetSectionHeader(infoType)}\r\n";

            // For some metrics we have a multi-string in the value and no name, so don't print a stray leading ':'.
            if (string.IsNullOrEmpty(info[0].Name))
            {
                Debug.Assert(info.Length == 1, "Unexpected empty name in first entry of multi-entry metrics info");
                section += $"{info[0].Value}\r\n";
            }
            else
            {
                for (int i = 0; i < info.Length; i++)
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
                default:
                    return "";
            }
        }

        public string GetRespInfo(InfoMetricsType[] sections, StoreWrapper storeWrapper)
        {
            var response = "";
            for (int i = 0; i < sections.Length; i++)
            {
                var section = sections[i];
                response += GetRespInfo(section, storeWrapper);
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
                default:
                    return null;
            }
        }

        public MetricsItem[] GetMetric(InfoMetricsType section, StoreWrapper storeWrapper) => GetMetricInternal(section, storeWrapper);

        public IEnumerable<(InfoMetricsType, MetricsItem[])> GetInfoMetrics(InfoMetricsType[] sections, StoreWrapper storeWrapper)
        {
            for (int i = 0; i < sections.Length; i++)
            {
                var infoType = sections[i];
                var infoItems = GetMetricInternal(infoType, storeWrapper);
                if (infoItems != null)
                    yield return (infoType, infoItems);
            }
        }
    }
}