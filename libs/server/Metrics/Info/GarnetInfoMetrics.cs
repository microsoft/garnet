// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using Garnet.common;
using Garnet.server.Metrics;

namespace Garnet.server
{
    class GarnetInfoMetrics
    {
        public static readonly InfoMetricsType[] DefaultInfo = [.. Enum.GetValues<InfoMetricsType>()
            .Where(e => e switch
            {
                InfoMetricsType.STOREHASHTABLE => false,
                InfoMetricsType.STOREREVIV => false,
                InfoMetricsType.HLOGSCAN => false,
                _ => true
            })];

        MetricsItem[] serverInfo = null;
        MetricsItem[] memoryInfo = null;
        MetricsItem[] clusterInfo = null;
        MetricsItem[] replicationInfo = null;
        MetricsItem[] statsInfo = null;
        MetricsItem[][] storeInfo = null;
        MetricsItem[][] storeHashDistrInfo = null;
        MetricsItem[][] storeRevivInfo = null;
        MetricsItem[][] persistenceInfo = null;
        MetricsItem[] clientsInfo = null;
        MetricsItem[] keyspaceInfo = null;
        MetricsItem[] bufferPoolStats = null;
        MetricsItem[] checkpointStats = null;
        MetricsItem[][] hlogScanStats = null;

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
                new("run_id", storeWrapper.RunId),
                new("redis_version", storeWrapper.redisProtocolVersion),
                new("redis_mode", storeWrapper.serverOptions.EnableCluster ? "cluster" : "standalone"),
            ];
        }

        private void PopulateMemoryInfo(StoreWrapper storeWrapper)
        {
            var store_index_size = 0L;
            var store_log_memory_size = 0L;
            var store_read_cache_size = 0L;
            var store_heap_memory_target_size = 0L;
            var store_heap_memory_size = 0L;
            var store_read_cache_heap_memory_size = 0L;
            long total_store_size;

            var enableAof = storeWrapper.serverOptions.EnableAOF;
            var aof_log_memory_size = enableAof ? 0 : -1L;

            var databases = storeWrapper.GetDatabasesSnapshot();

            foreach (var db in databases)
            {
                store_index_size += db.Store.IndexSize * 64;
                store_log_memory_size += db.Store.Log.MemorySizeBytes;
                store_read_cache_size += db.Store.ReadCache?.MemorySizeBytes ?? 0;

                aof_log_memory_size += db.AppendOnlyFile?.MemorySizeBytes ?? 0;

                store_heap_memory_target_size += db.SizeTracker?.mainLogTracker.TargetSize ?? 0;
                store_heap_memory_size += db.SizeTracker?.mainLogTracker.LogHeapSizeBytes ?? 0;
                store_read_cache_heap_memory_size += db.SizeTracker?.readCacheTracker?.LogHeapSizeBytes ?? 0;
            }

            total_store_size = store_index_size + store_log_memory_size + store_read_cache_size +
                               store_heap_memory_size + store_read_cache_heap_memory_size;

            var gcMemoryInfo = GC.GetGCMemoryInfo();
            var gcAvailableMemory = gcMemoryInfo.TotalCommittedBytes - gcMemoryInfo.HeapSizeBytes;

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
                new("gc_committed_bytes", gcMemoryInfo.TotalCommittedBytes.ToString()),
                new("gc_heap_bytes", gcMemoryInfo.HeapSizeBytes.ToString()),
                new("gc_managed_memory_bytes_excluding_heap", gcAvailableMemory.ToString()),
                new("gc_fragmented_bytes", gcMemoryInfo.FragmentedBytes.ToString()),
                new("store_index_size", store_index_size.ToString()),
                new("store_log_memory_size", store_log_memory_size.ToString()),
                new("store_read_cache_size", store_read_cache_size.ToString()),
                new("total_main_store_size", total_store_size.ToString()),
                new("store_heap_memory_target_size", store_heap_memory_target_size.ToString()),
                new("store_heap_memory_size", store_heap_memory_size.ToString()),
                new("store_read_cache_heap_memory_size", store_read_cache_heap_memory_size.ToString()),
                new("aof_memory_size", aof_log_memory_size.ToString())
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
                    new("total_connections_active", metricsDisabled ? "0" : globalMetrics.total_connections_active.ToString()),
                    new("total_connections_received", metricsDisabled ? "0" : globalMetrics.total_connections_received.ToString()),
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
                    new("total_number_resp_server_session_exceptions", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_number_resp_server_session_exceptions().ToString()),
                    new("total_transaction_commands_received", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_transaction_commands_received().ToString()),
                    new("total_transaction_commands_execution_failed", metricsDisabled ? "0" : globalMetrics.globalSessionMetrics.get_total_transaction_commands_execution_failed().ToString()),
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
            var databases = storeWrapper.GetDatabasesSnapshot();

            storeInfo = new MetricsItem[storeWrapper.MaxDatabaseId + 1][];
            foreach (var db in databases)
            {
                var storeStats = GetDatabaseStoreStats(storeWrapper, db);
                storeInfo[db.Id] = storeStats;
            }
        }

        private MetricsItem[] GetDatabaseStoreStats(StoreWrapper storeWrapper, GarnetDatabase db) =>
        [
            new($"CurrentVersion", db.Store.CurrentVersion.ToString()),
            new($"LastCheckpointedVersion", db.Store.LastCheckpointedVersion.ToString()),
            new($"SystemState", db.Store.SystemState.ToString()),
            new($"IndexMemorySize", db.Store.IndexSize.ToString()),
            new($"LogDir", storeWrapper.serverOptions.LogDir),
            new($"Log.BeginAddress", db.Store.Log.BeginAddress.ToString()),
            new($"Log.BufferSize", db.Store.Log.BufferSize.ToString()),
            new($"Log.AllocatedPageCount", db.Store.Log.AllocatedPageCount.ToString()),
            new($"Log.HeadAddress", db.Store.Log.HeadAddress.ToString()),
            new($"Log.MemorySizeBytes", db.Store.Log.MemorySizeBytes.ToString()),
            new($"Log.HeapSizeBytes", db.Store.Log.HeapSizeBytes.ToString()),
            new($"Log.SafeReadOnlyAddress", db.Store.Log.SafeReadOnlyAddress.ToString()),
            new($"Log.TailAddress", db.Store.Log.TailAddress.ToString()),
            new($"ReadCache.BeginAddress", db.Store.ReadCache?.BeginAddress.ToString() ?? "N/A"),
            new($"ReadCache.BufferSize", db.Store.ReadCache?.BufferSize.ToString() ?? "N/A"),
            new($"ReadCache.AllocatedPageCount", db.Store.ReadCache?.AllocatedPageCount.ToString() ?? "N/A"),
            new($"ReadCache.HeadAddress", db.Store.ReadCache?.HeadAddress.ToString() ?? "N/A"),
            new($"ReadCache.MemorySizeBytes", db.Store.ReadCache?.MemorySizeBytes.ToString() ?? "N/A"),
            new($"ReadCache.HeapSizeBytes", db.Store.ReadCache?.HeapSizeBytes.ToString() ?? "N/A"),
            new($"ReadCache.TailAddress", db.Store.ReadCache?.TailAddress.ToString() ?? "N/A"),
        ];

        private void PopulateStoreHashDistribution(StoreWrapper storeWrapper)
        {
            var databases = storeWrapper.GetDatabasesSnapshot();

            storeHashDistrInfo = new MetricsItem[storeWrapper.MaxDatabaseId + 1][];
            foreach (var db in databases)
            {
                storeHashDistrInfo[db.Id] = [new("", db.Store.DumpDistribution())];
            }
        }

        private void PopulateStoreRevivInfo(StoreWrapper storeWrapper)
        {
            var databases = storeWrapper.GetDatabasesSnapshot();

            storeRevivInfo = new MetricsItem[storeWrapper.MaxDatabaseId + 1][];
            foreach (var db in databases)
            {
                storeRevivInfo[db.Id] = [new("", db.Store.DumpRevivificationStats())];
            }
        }

        private void PopulatePersistenceInfo(StoreWrapper storeWrapper)
        {
            var databases = storeWrapper.GetDatabasesSnapshot();

            persistenceInfo = new MetricsItem[storeWrapper.MaxDatabaseId + 1][];
            foreach (var db in databases)
            {
                var persistenceStats = GetDatabasePersistenceStats(storeWrapper, db);
                persistenceInfo[db.Id] = persistenceStats;
            }
        }

        private MetricsItem[] GetDatabasePersistenceStats(StoreWrapper storeWrapper, GarnetDatabase db)
        {
            var aofEnabled = storeWrapper.serverOptions.EnableAOF;

            return
            [
                new($"CommittedBeginAddress", !aofEnabled ? "N/A" : db.AppendOnlyFile.CommittedBeginAddress.ToString()),
                new($"CommittedUntilAddress", !aofEnabled ? "N/A" : db.AppendOnlyFile.CommittedUntilAddress.ToString()),
                new($"FlushedUntilAddress", !aofEnabled ? "N/A" : db.AppendOnlyFile.FlushedUntilAddress.ToString()),
                new($"BeginAddress", !aofEnabled ? "N/A" : db.AppendOnlyFile.BeginAddress.ToString()),
                new($"TailAddress", !aofEnabled ? "N/A" : db.AppendOnlyFile.TailAddress.ToString()),
                new($"SafeAofAddress", !aofEnabled ? "N/A" : storeWrapper.safeAofAddress.ToString())
            ];
        }

        private void PopulateClientsInfo(StoreWrapper storeWrapper)
        {
            var metricsDisabled = storeWrapper.monitor == null;
            var globalMetrics = metricsDisabled ? default : storeWrapper.monitor.GlobalMetrics;
            clientsInfo = [new("connected_clients", metricsDisabled ? "0" : (globalMetrics.total_connections_received - globalMetrics.total_connections_disposed).ToString())];
        }

        private void PopulateKeyspaceInfo()
        {
            keyspaceInfo = null;
        }

        private void PopulateClusterBufferPoolStats(StoreWrapper storeWrapper)
        {
            var server = storeWrapper.Servers;
            bufferPoolStats = new MetricsItem[server.Length];
            for (var i = 0; i < server.Length; i++)
                bufferPoolStats[i] = new($"server_socket_{i}", ((GarnetServerTcp)server[i]).GetBufferPoolStats());
            if (storeWrapper.clusterProvider != null)
                bufferPoolStats = [.. bufferPoolStats, .. storeWrapper.clusterProvider.GetBufferPoolStats()];
        }

        private void PopulateCheckpointInfo(StoreWrapper storeWrapper)
        {
            checkpointStats = storeWrapper.clusterProvider?.GetCheckpointInfo();
        }

        private void PopulateHlogScanInfo(StoreWrapper storeWrapper)
        {
            (HybridLogScanMetrics mainStoreMetrics, HybridLogScanMetrics objectStoreMetrics)[] res = storeWrapper.HybridLogDistributionScan();
            var result = new List<MetricsItem[]>();
            for (int i = 0; i < res.Length; i++)
            {
                var mainStoreMetric = res[i].mainStoreMetrics.DumpScanMetricsInfo();
                mainStoreMetric = string.IsNullOrEmpty(mainStoreMetric) ? "Empty" : mainStoreMetric;
                var objectStoreMetric = res[i].objectStoreMetrics.DumpScanMetricsInfo();
                objectStoreMetric = string.IsNullOrEmpty(objectStoreMetric) ? "Empty" : objectStoreMetric;
                result.Add(
                    [
                        new MetricsItem($"MainStore_HLog_{i}", mainStoreMetric),
                        new MetricsItem($"ObjectStore_HLog_{i}", objectStoreMetric)
                    ]);
            }

            hlogScanStats = result.ToArray();
        }

        public static string GetSectionHeader(InfoMetricsType infoType, int dbId)
        {
            // No word separators inside section names, some clients will then fail to process INFO output.
            // https://github.com/microsoft/garnet/pull/1019#issuecomment-2660752028
            return infoType switch
            {
                InfoMetricsType.SERVER => "Server",
                InfoMetricsType.MEMORY => "Memory",
                InfoMetricsType.CLUSTER => "Cluster",
                InfoMetricsType.REPLICATION => "Replication",
                InfoMetricsType.STATS => "Stats",
                InfoMetricsType.STORE => $"Store_DB_{dbId}",
                InfoMetricsType.STOREHASHTABLE => $"StoreHashTableDistribution_DB_{dbId}",
                InfoMetricsType.STOREREVIV => $"StoreDeletedRecordRevivification_DB_{dbId}",
                InfoMetricsType.PERSISTENCE => $"Persistence_DB_{dbId}",
                InfoMetricsType.CLIENTS => "Clients",
                InfoMetricsType.KEYSPACE => "Keyspace",
                InfoMetricsType.MODULES => "Modules",
                InfoMetricsType.BPSTATS => "BufferPoolStats",
                InfoMetricsType.CINFO => "CheckpointInfo",
                InfoMetricsType.HLOGSCAN => $"MainStoreHLogScan_DB_{dbId}",
                _ => "Default",
            };
        }

        private static void GetSectionRespInfo(string sectionHeader, MetricsItem[] info, StringBuilder sbResponse)
        {
            sbResponse.Append($"# {sectionHeader}\r\n");
            if (info == null)
                return;

            // For some metrics we have a multi-string in the value and no name, so don't print a stray leading ':'.
            if (string.IsNullOrEmpty(info[0].Name))
            {
                Debug.Assert(info.Length == 1, "Unexpected empty name in first entry of multi-entry metrics info");
                sbResponse.Append($"{info[0].Value}\r\n");
            }
            else
            {
                for (var i = 0; i < info.Length; i++)
                    sbResponse.Append($"{info[i].Name}:{info[i].Value}\r\n");
            }
        }

        private void GetRespInfo(InfoMetricsType section, int dbId, StoreWrapper storeWrapper, StringBuilder sbResponse)
        {
            var header = GetSectionHeader(section, dbId);

            switch (section)
            {
                case InfoMetricsType.SERVER:
                    PopulateServerInfo(storeWrapper);
                    GetSectionRespInfo(header, serverInfo, sbResponse);
                    return;
                case InfoMetricsType.MEMORY:
                    PopulateMemoryInfo(storeWrapper);
                    GetSectionRespInfo(header, memoryInfo, sbResponse);
                    return;
                case InfoMetricsType.CLUSTER:
                    PopulateClusterInfo(storeWrapper);
                    GetSectionRespInfo(header, clusterInfo, sbResponse);
                    return;
                case InfoMetricsType.REPLICATION:
                    PopulateReplicationInfo(storeWrapper);
                    GetSectionRespInfo(header, replicationInfo, sbResponse);
                    return;
                case InfoMetricsType.STATS:
                    PopulateStatsInfo(storeWrapper);
                    GetSectionRespInfo(header, statsInfo, sbResponse);
                    return;
                case InfoMetricsType.STORE:
                    PopulateStoreStats(storeWrapper);
                    GetSectionRespInfo(header, storeInfo[dbId], sbResponse);
                    return;
                case InfoMetricsType.STOREHASHTABLE:
                    PopulateStoreHashDistribution(storeWrapper);
                    GetSectionRespInfo(header, storeHashDistrInfo[dbId], sbResponse);
                    return;
                case InfoMetricsType.STOREREVIV:
                    PopulateStoreRevivInfo(storeWrapper);
                    GetSectionRespInfo(header, storeRevivInfo[dbId], sbResponse);
                    return;
                case InfoMetricsType.PERSISTENCE:
                    if (!storeWrapper.serverOptions.EnableAOF)
                        return;
                    PopulatePersistenceInfo(storeWrapper);
                    GetSectionRespInfo(header, persistenceInfo[dbId], sbResponse);
                    return;
                case InfoMetricsType.CLIENTS:
                    PopulateClientsInfo(storeWrapper);
                    GetSectionRespInfo(header, clientsInfo, sbResponse);
                    return;
                case InfoMetricsType.KEYSPACE:
                    PopulateKeyspaceInfo();
                    GetSectionRespInfo(header, keyspaceInfo, sbResponse);
                    return;
                case InfoMetricsType.MODULES:
                    GetSectionRespInfo(header, null, sbResponse);
                    return;
                case InfoMetricsType.BPSTATS:
                    PopulateClusterBufferPoolStats(storeWrapper);
                    GetSectionRespInfo(header, bufferPoolStats, sbResponse);
                    return;
                case InfoMetricsType.CINFO:
                    PopulateCheckpointInfo(storeWrapper);
                    GetSectionRespInfo(header, checkpointStats, sbResponse);
                    return;
                case InfoMetricsType.HLOGSCAN:
                    PopulateHlogScanInfo(storeWrapper);
                    GetSectionRespInfo(header, hlogScanStats[dbId], sbResponse);
                    return;
                default:
                    return;
            }
        }

        public string GetRespInfo(InfoMetricsType[] sections, int dbId, StoreWrapper storeWrapper)
        {
            var sbResponse = new StringBuilder();
            for (var i = 0; i < sections.Length; i++)
            {
                GetRespInfo(sections[i], dbId, storeWrapper, sbResponse);

                if (i != sections.Length - 1)
                    sbResponse.Append("\r\n");
            }

            return sbResponse.ToString();
        }

        private MetricsItem[] GetMetricInternal(InfoMetricsType section, int dbId, StoreWrapper storeWrapper)
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
                    return storeInfo[dbId];
                case InfoMetricsType.STOREHASHTABLE:
                    PopulateStoreHashDistribution(storeWrapper);
                    return storeHashDistrInfo[dbId];
                case InfoMetricsType.STOREREVIV:
                    PopulateStoreRevivInfo(storeWrapper);
                    return storeRevivInfo[dbId];
                case InfoMetricsType.PERSISTENCE:
                    if (!storeWrapper.serverOptions.EnableAOF)
                        return null;
                    PopulatePersistenceInfo(storeWrapper);
                    return persistenceInfo[dbId];
                case InfoMetricsType.CLIENTS:
                    PopulateClientsInfo(storeWrapper);
                    return clientsInfo;
                case InfoMetricsType.KEYSPACE:
                    PopulateKeyspaceInfo();
                    return keyspaceInfo;
                case InfoMetricsType.MODULES:
                    return null;
                default:
                    return null;
            }
        }

        public MetricsItem[] GetMetric(InfoMetricsType section, int dbId, StoreWrapper storeWrapper) => GetMetricInternal(section, dbId, storeWrapper);

        public IEnumerable<(InfoMetricsType, MetricsItem[])> GetInfoMetrics(InfoMetricsType[] sections, int dbId, StoreWrapper storeWrapper)
        {
            foreach (var section in sections)
            {
                var infoItems = GetMetricInternal(section, dbId, storeWrapper);
                if (infoItems != null)
                    yield return (section, infoItems);
            }
        }
    }
}