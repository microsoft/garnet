﻿// Copyright (c) Microsoft Corporation.
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
        public static readonly InfoMetricsType[] DefaultInfo = [.. Enum.GetValues<InfoMetricsType>()
            .Where(e => e switch
            {
                InfoMetricsType.STOREHASHTABLE => false,
                InfoMetricsType.OBJECTSTOREHASHTABLE => false,
                InfoMetricsType.STOREREVIV => false,
                InfoMetricsType.OBJECTSTOREREVIV => false,
                _ => true
            })];

        MetricsItem[] serverInfo = null;
        MetricsItem[] memoryInfo = null;
        MetricsItem[] clusterInfo = null;
        MetricsItem[] replicationInfo = null;
        MetricsItem[] statsInfo = null;
        MetricsItem[][] storeInfo = null;
        MetricsItem[][] objectStoreInfo = null;
        MetricsItem[][] storeHashDistrInfo = null;
        MetricsItem[][] objectStoreHashDistrInfo = null;
        MetricsItem[][] storeRevivInfo = null;
        MetricsItem[][] objectStoreRevivInfo = null;
        MetricsItem[][] persistenceInfo = null;
        MetricsItem[] clientsInfo = null;
        MetricsItem[] keyspaceInfo = null;
        MetricsItem[] bufferPoolStats = null;

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
                new("run_id", storeWrapper.runId),
                new("redis_version", storeWrapper.redisProtocolVersion),
                new("redis_mode", storeWrapper.serverOptions.EnableCluster ? "cluster" : "standalone"),
            ];
        }

        private void PopulateMemoryInfo(StoreWrapper storeWrapper)
        {
            var main_store_index_size = -1L;
            var main_store_log_memory_size = -1L;
            var main_store_read_cache_size = -1L;
            var total_main_store_size = -1L;

            var object_store_index_size = -1L;
            var object_store_log_memory_size = -1L;
            var object_store_read_cache_log_memory_size = -1L;
            var object_store_heap_memory_size = -1L;
            var object_store_read_cache_heap_memory_size = -1L;
            var total_object_store_size = -1L;

            var aof_log_memory_size = -1L;

            var databases = storeWrapper.GetDatabasesSnapshot();
            var disableObj = storeWrapper.serverOptions.DisableObjects;

            foreach (var db in databases)
            {
                main_store_index_size += db.MainStore.IndexSize * 64;
                main_store_log_memory_size += db.MainStore.Log.MemorySizeBytes;
                main_store_read_cache_size += db.MainStore.ReadCache?.MemorySizeBytes ?? 0;


                aof_log_memory_size = db.AppendOnlyFile?.MemorySizeBytes ?? -1;

                if (!disableObj)
                {
                    object_store_index_size += db.ObjectStore.IndexSize * 64;
                    object_store_log_memory_size += db.ObjectStore.Log.MemorySizeBytes;
                    object_store_read_cache_log_memory_size += db.ObjectStore.ReadCache?.MemorySizeBytes ?? 0;
                    object_store_heap_memory_size += db.ObjectStoreSizeTracker?.mainLogTracker.LogHeapSizeBytes ?? 0;
                    object_store_read_cache_heap_memory_size += db.ObjectStoreSizeTracker?.readCacheTracker?.LogHeapSizeBytes ?? 0;
                }
            }

            total_main_store_size = main_store_index_size + main_store_log_memory_size + main_store_read_cache_size;

            if (!disableObj)
            {
                total_object_store_size = object_store_index_size + object_store_log_memory_size +
                                           object_store_read_cache_log_memory_size + object_store_heap_memory_size +
                                           object_store_read_cache_heap_memory_size;
            }

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
                new("main_store_index_size", main_store_index_size.ToString()),
                new("main_store_log_memory_size", main_store_log_memory_size.ToString()),
                new("main_store_read_cache_size", main_store_read_cache_size.ToString()),
                new("total_main_store_size", total_main_store_size.ToString()),
                new("object_store_index_size", object_store_index_size.ToString()),
                new("object_store_log_memory_size", object_store_log_memory_size.ToString()),
                new("object_store_heap_memory_size", object_store_heap_memory_size.ToString()),
                new("object_store_read_cache_log_memory_size", object_store_read_cache_log_memory_size.ToString()),
                new("object_store_read_cache_heap_memory_size", object_store_read_cache_heap_memory_size.ToString()),
                new("total_object_store_size", total_object_store_size.ToString()),
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
            new($"CurrentVersion", db.MainStore.CurrentVersion.ToString()),
            new($"LastCheckpointedVersion", db.MainStore.LastCheckpointedVersion.ToString()),
            new($"SystemState", db.MainStore.SystemState.ToString()),
            new($"IndexSize", db.MainStore.IndexSize.ToString()),
            new($"LogDir", storeWrapper.serverOptions.LogDir),
            new($"Log.BeginAddress", db.MainStore.Log.BeginAddress.ToString()),
            new($"Log.BufferSize", db.MainStore.Log.BufferSize.ToString()),
            new($"Log.EmptyPageCount", db.MainStore.Log.EmptyPageCount.ToString()),
            new($"Log.FixedRecordSize", db.MainStore.Log.FixedRecordSize.ToString()),
            new($"Log.HeadAddress", db.MainStore.Log.HeadAddress.ToString()),
            new($"Log.MemorySizeBytes", db.MainStore.Log.MemorySizeBytes.ToString()),
            new($"Log.SafeReadOnlyAddress", db.MainStore.Log.SafeReadOnlyAddress.ToString()),
            new($"Log.TailAddress", db.MainStore.Log.TailAddress.ToString()),
            new($"ReadCache.BeginAddress", db.MainStore.ReadCache?.BeginAddress.ToString() ?? "N/A"),
            new($"ReadCache.BufferSize", db.MainStore.ReadCache?.BufferSize.ToString() ?? "N/A"),
            new($"ReadCache.EmptyPageCount", db.MainStore.ReadCache?.EmptyPageCount.ToString() ?? "N/A"),
            new($"ReadCache.HeadAddress", db.MainStore.ReadCache?.HeadAddress.ToString() ?? "N/A"),
            new($"ReadCache.MemorySizeBytes", db.MainStore.ReadCache?.MemorySizeBytes.ToString() ?? "N/A"),
            new($"ReadCache.TailAddress", db.MainStore.ReadCache?.TailAddress.ToString() ?? "N/A"),
        ];

        private void PopulateObjectStoreStats(StoreWrapper storeWrapper)
        {
            var databases = storeWrapper.GetDatabasesSnapshot();

            objectStoreInfo = new MetricsItem[storeWrapper.MaxDatabaseId + 1][];
            foreach (var db in databases)
            {
                var storeStats = GetDatabaseObjectStoreStats(storeWrapper, db);
                objectStoreInfo[db.Id] = storeStats;
            }
        }

        private MetricsItem[] GetDatabaseObjectStoreStats(StoreWrapper storeWrapper, GarnetDatabase db) =>
        [
            new($"CurrentVersion", db.ObjectStore.CurrentVersion.ToString()),
            new($"LastCheckpointedVersion", db.ObjectStore.LastCheckpointedVersion.ToString()),
            new($"SystemState", db.ObjectStore.SystemState.ToString()),
            new($"IndexSize", db.ObjectStore.IndexSize.ToString()),
            new($"LogDir", storeWrapper.serverOptions.LogDir),
            new($"Log.BeginAddress", db.ObjectStore.Log.BeginAddress.ToString()),
            new($"Log.BufferSize", db.ObjectStore.Log.BufferSize.ToString()),
            new($"Log.EmptyPageCount", db.ObjectStore.Log.EmptyPageCount.ToString()),
            new($"Log.FixedRecordSize", db.ObjectStore.Log.FixedRecordSize.ToString()),
            new($"Log.HeadAddress", db.ObjectStore.Log.HeadAddress.ToString()),
            new($"Log.MemorySizeBytes", db.ObjectStore.Log.MemorySizeBytes.ToString()),
            new($"Log.SafeReadOnlyAddress", db.ObjectStore.Log.SafeReadOnlyAddress.ToString()),
            new($"Log.TailAddress", db.ObjectStore.Log.TailAddress.ToString()),
            new($"ReadCache.BeginAddress", db.ObjectStore.ReadCache?.BeginAddress.ToString() ?? "N/A"),
            new($"ReadCache.BufferSize", db.ObjectStore.ReadCache?.BufferSize.ToString() ?? "N/A"),
            new($"ReadCache.EmptyPageCount", db.ObjectStore.ReadCache?.EmptyPageCount.ToString() ?? "N/A"),
            new($"ReadCache.HeadAddress", db.ObjectStore.ReadCache?.HeadAddress.ToString() ?? "N/A"),
            new($"ReadCache.MemorySizeBytes", db.ObjectStore.ReadCache?.MemorySizeBytes.ToString() ?? "N/A"),
            new($"ReadCache.TailAddress", db.ObjectStore.ReadCache?.TailAddress.ToString() ?? "N/A"),
        ];

        private void PopulateStoreHashDistribution(StoreWrapper storeWrapper)
        {
            var databases = storeWrapper.GetDatabasesSnapshot();

            storeHashDistrInfo = new MetricsItem[storeWrapper.MaxDatabaseId + 1][];
            foreach (var db in databases)
            {
                storeHashDistrInfo[db.Id] = [new("", db.MainStore.DumpDistribution())];
            }
        }

        private void PopulateObjectStoreHashDistribution(StoreWrapper storeWrapper)
        {
            var databases = storeWrapper.GetDatabasesSnapshot();

            objectStoreHashDistrInfo = new MetricsItem[storeWrapper.MaxDatabaseId + 1][];
            foreach (var db in databases)
            {
                objectStoreHashDistrInfo[db.Id] = [new("", db.ObjectStore.DumpDistribution())];
            }
        }

        private void PopulateStoreRevivInfo(StoreWrapper storeWrapper)
        {
            var databases = storeWrapper.GetDatabasesSnapshot();

            storeRevivInfo = new MetricsItem[storeWrapper.MaxDatabaseId + 1][];
            foreach (var db in databases)
            {
                storeRevivInfo[db.Id] = [new("", db.MainStore.DumpRevivificationStats())];
            }
        }

        private void PopulateObjectStoreRevivInfo(StoreWrapper storeWrapper)
        {
            var databases = storeWrapper.GetDatabasesSnapshot();

            objectStoreRevivInfo = new MetricsItem[storeWrapper.MaxDatabaseId + 1][];
            foreach (var db in databases)
            {
                objectStoreRevivInfo[db.Id] = [new("", db.ObjectStore.DumpRevivificationStats())];
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

        private void PopulateKeyspaceInfo(StoreWrapper storeWrapper)
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

        public static string GetSectionHeader(InfoMetricsType infoType, int dbId)
        {
            return infoType switch
            {
                InfoMetricsType.SERVER => "Server",
                InfoMetricsType.MEMORY => "Memory",
                InfoMetricsType.CLUSTER => "Cluster",
                InfoMetricsType.REPLICATION => "Replication",
                InfoMetricsType.STATS => "Stats",
                InfoMetricsType.STORE => $"MainStore (DB {dbId})",
                InfoMetricsType.OBJECTSTORE => $"ObjectStore (DB {dbId})",
                InfoMetricsType.STOREHASHTABLE => $"MainStoreHashTableDistribution (DB {dbId})",
                InfoMetricsType.OBJECTSTOREHASHTABLE => $"ObjectStoreHashTableDistribution (DB {dbId})",
                InfoMetricsType.STOREREVIV => $"MainStoreDeletedRecordRevivification (DB {dbId})",
                InfoMetricsType.OBJECTSTOREREVIV => $"ObjectStoreDeletedRecordRevivification (DB {dbId})",
                InfoMetricsType.PERSISTENCE => $"Persistence (DB {dbId})",
                InfoMetricsType.CLIENTS => "Clients",
                InfoMetricsType.KEYSPACE => "Keyspace",
                InfoMetricsType.MODULES => "Modules",
                InfoMetricsType.BPSTATS => "BufferPoolStats",
                _ => "Default",
            };
        }

        private static string GetSectionRespInfo(string sectionHeader, MetricsItem[] info)
        {
            var section = $"# {sectionHeader}\r\n";
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

        private string GetRespInfo(InfoMetricsType section, int dbId, StoreWrapper storeWrapper)
        {
            var header = GetSectionHeader(section, dbId);

            switch (section)
            {
                case InfoMetricsType.SERVER:
                    PopulateServerInfo(storeWrapper);
                    return GetSectionRespInfo(header, serverInfo);
                case InfoMetricsType.MEMORY:
                    PopulateMemoryInfo(storeWrapper);
                    return GetSectionRespInfo(header, memoryInfo);
                case InfoMetricsType.CLUSTER:
                    PopulateClusterInfo(storeWrapper);
                    return GetSectionRespInfo(header, clusterInfo);
                case InfoMetricsType.REPLICATION:
                    PopulateReplicationInfo(storeWrapper);
                    return GetSectionRespInfo(header, replicationInfo);
                case InfoMetricsType.STATS:
                    PopulateStatsInfo(storeWrapper);
                    return GetSectionRespInfo(header, statsInfo);
                case InfoMetricsType.STORE:
                    PopulateStoreStats(storeWrapper);
                    return GetSectionRespInfo(header, storeInfo[dbId]);
                case InfoMetricsType.OBJECTSTORE:
                    if (storeWrapper.serverOptions.DisableObjects) return "";
                    PopulateObjectStoreStats(storeWrapper);
                    return GetSectionRespInfo(header, objectStoreInfo[dbId]);
                case InfoMetricsType.STOREHASHTABLE:
                    PopulateStoreHashDistribution(storeWrapper);
                    return GetSectionRespInfo(header, storeHashDistrInfo[dbId]);
                case InfoMetricsType.OBJECTSTOREHASHTABLE:
                    if (storeWrapper.serverOptions.DisableObjects) return "";
                    PopulateObjectStoreHashDistribution(storeWrapper);
                    return GetSectionRespInfo(header, objectStoreHashDistrInfo[dbId]);
                case InfoMetricsType.STOREREVIV:
                    PopulateStoreRevivInfo(storeWrapper);
                    return GetSectionRespInfo(header, storeRevivInfo[dbId]);
                case InfoMetricsType.OBJECTSTOREREVIV:
                    if (storeWrapper.serverOptions.DisableObjects) return "";
                    PopulateObjectStoreRevivInfo(storeWrapper);
                    return GetSectionRespInfo(header, objectStoreRevivInfo[dbId]);
                case InfoMetricsType.PERSISTENCE:
                    if (!storeWrapper.serverOptions.EnableAOF) return "";
                    PopulatePersistenceInfo(storeWrapper);
                    return GetSectionRespInfo(header, persistenceInfo[dbId]);
                case InfoMetricsType.CLIENTS:
                    PopulateClientsInfo(storeWrapper);
                    return GetSectionRespInfo(header, clientsInfo);
                case InfoMetricsType.KEYSPACE:
                    PopulateKeyspaceInfo(storeWrapper);
                    return GetSectionRespInfo(header, keyspaceInfo);
                case InfoMetricsType.MODULES:
                    return GetSectionRespInfo(header, null);
                case InfoMetricsType.BPSTATS:
                    PopulateClusterBufferPoolStats(storeWrapper);
                    return GetSectionRespInfo(header, bufferPoolStats);
                default:
                    return "";
            }
        }

        public string GetRespInfo(InfoMetricsType[] sections, int dbId, StoreWrapper storeWrapper)
        {
            var response = "";
            for (var i = 0; i < sections.Length; i++)
            {
                var section = sections[i];
                var resp = GetRespInfo(section, dbId, storeWrapper);
                if (string.IsNullOrEmpty(resp)) continue;
                response += resp;
                response += sections.Length - 1 == i ? "" : "\r\n";
            }
            return response;
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
                case InfoMetricsType.OBJECTSTORE:
                    if (storeWrapper.serverOptions.DisableObjects) return null;
                    PopulateObjectStoreStats(storeWrapper);
                    return objectStoreInfo[dbId];
                case InfoMetricsType.STOREHASHTABLE:
                    PopulateStoreHashDistribution(storeWrapper);
                    return storeHashDistrInfo[dbId];
                case InfoMetricsType.OBJECTSTOREHASHTABLE:
                    if (storeWrapper.serverOptions.DisableObjects) return null;
                    PopulateObjectStoreHashDistribution(storeWrapper);
                    return objectStoreHashDistrInfo[dbId];
                case InfoMetricsType.STOREREVIV:
                    PopulateStoreRevivInfo(storeWrapper);
                    return storeRevivInfo[dbId];
                case InfoMetricsType.OBJECTSTOREREVIV:
                    if (storeWrapper.serverOptions.DisableObjects) return null;
                    PopulateObjectStoreRevivInfo(storeWrapper);
                    return objectStoreRevivInfo[dbId];
                case InfoMetricsType.PERSISTENCE:
                    if (!storeWrapper.serverOptions.EnableAOF) return null;
                    PopulatePersistenceInfo(storeWrapper);
                    return persistenceInfo[dbId];
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

        public MetricsItem[] GetMetric(InfoMetricsType section, int dbId, StoreWrapper storeWrapper) => GetMetricInternal(section, dbId, storeWrapper);

        public IEnumerable<(InfoMetricsType, MetricsItem[])> GetInfoMetrics(InfoMetricsType[] sections, int dbId, StoreWrapper storeWrapper)
        {
            for (var i = 0; i < sections.Length; i++)
            {
                var infoType = sections[i];
                var infoItems = GetMetricInternal(infoType, dbId, storeWrapper);
                if (infoItems != null)
                    yield return (infoType, infoItems);
            }
        }
    }
}