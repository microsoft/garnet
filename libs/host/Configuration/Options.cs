// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Azure.Identity;
using CommandLine;
using Garnet.common;
using Garnet.server;
using Garnet.server.Auth.Aad;
using Garnet.server.Auth.Settings;
using Garnet.server.TLS;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using Tsavorite.devices;

namespace Garnet
{
    /// <summary>
    /// This class contains all configurable settings for Garnet
    /// In order to add a new configurable setting:
    /// 1. Add a new property and decorate it with an OptionAttribute.
    /// 2. If needed, decorate with a new or existing ValidationAttribute from OptionsValidators.cs
    /// 3. Add a default value for the new property in defaults.conf (do NOT use the OptionAttribute.Default property, it can cause unexpected behavior)
    /// 4. If needed, add a matching property in Garnet.server/Servers/GarnetServerOptions.cs and initialize it in Options.GetServerOptions()
    /// 5. If new setting has a matching setting in redis.conf, add the matching setting to RedisOptions.cs
    /// </summary>
    internal sealed class Options : ICloneable
    {
        [IntRangeValidation(0, 65535)]
        [Option("port", Required = false, HelpText = "Port to run server on")]
        public int Port { get; set; }

        [IpAddressValidation(false)]
        [Option("bind", Required = false, HelpText = "Whitespace or comma separated string of IP addresses to bind server to (default: any)")]
        public string Address { get; set; }

        [IntRangeValidation(0, 65535)]
        [Option("cluster-announce-port", Required = false, HelpText = "Port that this node advertises to other nodes to connect to for gossiping.")]
        public int ClusterAnnouncePort { get; set; }

        [IpAddressValidation(false)]
        [Option("cluster-announce-ip", Required = false, HelpText = "IP address that this node advertises to other nodes to connect to for gossiping.")]
        public string ClusterAnnounceIp { get; set; }

        [MemorySizeValidation]
        [Option('m', "memory", Required = false, HelpText = "Total log memory used in bytes (rounds down to power of 2)")]
        public string MemorySize { get; set; }

        [MemorySizeValidation]
        [Option('p', "page", Required = false, HelpText = "Size of each page in bytes (rounds down to power of 2)")]
        public string PageSize { get; set; }

        [MemorySizeValidation]
        [Option('s', "segment", Required = false, HelpText = "Size of each log segment in bytes on disk (rounds down to power of 2)")]
        public string SegmentSize { get; set; }

        [MemorySizeValidation]
        [Option('i', "index", Required = false, HelpText = "Start size of hash index in bytes (rounds down to power of 2)")]
        public string IndexSize { get; set; }

        [MemorySizeValidation(false)]
        [Option("index-max-size", Required = false, HelpText = "Max size of hash index in bytes (rounds down to power of 2)")]
        public string IndexMaxSize { get; set; }

        [PercentageValidation(false)]
        [Option("mutable-percent", Required = false, HelpText = "Percentage of log memory that is kept mutable")]
        public int MutablePercent { get; set; }

        [OptionValidation]
        [Option("readcache", Required = false, HelpText = "Enables read cache for faster access to on-disk records.")]
        public bool? EnableReadCache { get; set; }

        [MemorySizeValidation]
        [Option("readcache-memory", Required = false, HelpText = "Total read cache log memory used in bytes (rounds down to power of 2)")]
        public string ReadCacheMemorySize { get; set; }

        [MemorySizeValidation]
        [Option("readcache-page", Required = false, HelpText = "Size of each read cache page in bytes (rounds down to power of 2)")]
        public string ReadCachePageSize { get; set; }

        [MemorySizeValidation(false)]
        [Option("heap-memory", Required = false, HelpText = "Heap memory size in bytes (Sum of size taken up by all object instances in the heap)")]
        public string HeapMemorySize { get; set; }

        [MemorySizeValidation(false)]
        [Option("readcache-heap-memory", Required = false, HelpText = "Read cache heap memory size in bytes (Sum of size taken up by all object instances in the heap)")]
        public string ReadCacheHeapMemorySize { get; set; }

        [OptionValidation]
        [Option("storage-tier", Required = false, HelpText = "Enable tiering of records (hybrid log) to storage, to support a larger-than-memory store. Use --logdir to specify storage directory.")]
        public bool? EnableStorageTier { get; set; }

        [OptionValidation]
        [Option("copy-reads-to-tail", Required = false, HelpText = "When records are read from the main store's in-memory immutable region or storage device, copy them to the tail of the log.")]
        public bool? CopyReadsToTail { get; set; }

        [LogDirValidation(false, false)]
        [Option('l', "logdir", Required = false, HelpText = "Storage directory for tiered records (hybrid log), if storage tiering (--storage-tier) is enabled. Uses current directory if unspecified.")]
        public string LogDir { get; set; }

        [CheckpointDirValidation(false, false)]
        [Option('c', "checkpointdir", Required = false, HelpText = "Storage directory for checkpoints. Uses logdir if unspecified.")]
        public string CheckpointDir { get; set; }

        [OptionValidation]
        [Option('r', "recover", Required = false, HelpText = "Recover from latest checkpoint and log, if present.")]
        public bool? Recover { get; set; }

        [OptionValidation]
        [Option("no-pubsub", Required = false, HelpText = "Disable pub/sub feature on server.")]
        public bool? DisablePubSub { get; set; }

        [OptionValidation]
        [Option("incsnap", Required = false, HelpText = "Enable incremental snapshots.")]
        public bool? EnableIncrementalSnapshots { get; set; }

        [MemorySizeValidation]
        [Option("pubsub-pagesize", Required = false, HelpText = "Page size of log used for pub/sub (rounds down to power of 2)")]
        public string PubSubPageSize { get; set; }

        [OptionValidation]
        [Option("no-obj", Required = false, HelpText = "Disable support for data structure objects.")]
        public bool? DisableObjects { get; set; }

        [OptionValidation]
        [Option("cluster", Required = false, HelpText = "Enable cluster.")]
        public bool? EnableCluster { get; set; }

        [OptionValidation]
        [Option("clean-cluster-config", Required = false, HelpText = "Start with clean cluster config.")]
        public bool? CleanClusterConfig { get; set; }

        [IntRangeValidation(0, 16384)]
        [Option("pmt", Required = false, HelpText = "Number of parallel migrate tasks to spawn when SLOTS or SLOTSRANGE option is used.")]
        public int ParallelMigrateTaskCount { get; set; }

        [OptionValidation]
        [Option("fast-migrate", Required = false, HelpText = "When migrating slots 1. write directly to network buffer to avoid unecessary copies, 2. do not wait for ack from target before sending next batch of keys.")]
        public bool? FastMigrate { get; set; }

        [Option("auth", Required = false, HelpText = "Authentication mode of Garnet. This impacts how AUTH command is processed and how clients are authenticated against Garnet. Value options: NoAuth, Password, Aad, ACL")]
        public GarnetAuthenticationMode AuthenticationMode { get; set; }

        [HiddenOption]
        [Option("password", Required = false, HelpText = "Authentication string for password authentication.")]
        public string Password { get; set; }

        [Option("cluster-username", Required = false, HelpText = "Username to authenticate intra-cluster communication with.")]
        public string ClusterUsername { get; set; }

        [HiddenOption]
        [Option("cluster-password", Required = false, HelpText = "Password to authenticate intra-cluster communication with.")]
        public string ClusterPassword { get; set; }

        [FilePathValidation(true, true, false)]
        [Option("acl-file", Required = false, HelpText = "External ACL user file.")]
        public string AclFile { get; set; }

        [Option("aad-authority", Required = false, HelpText = "The authority of AAD authentication.")]
        public string AadAuthority { get; set; }

        [Option("aad-audiences", Required = false, HelpText = "The audiences of AAD token for AAD authentication. Should be a comma separated string.")]
        public string AadAudiences { get; set; }

        [Option("aad-issuers", Required = false, HelpText = "The issuers of AAD token for AAD authentication. Should be a comma separated string.")]
        public string AadIssuers { get; set; }

        [Option("aad-authorized-app-ids", Required = false, HelpText = "The authorized client app Ids for AAD authentication. Should be a comma separated string.")]
        public string AuthorizedAadApplicationIds { get; set; }

        [Option("aad-validate-acl-username", Required = false, HelpText = "Only valid for AclWithAAD mode. Validates username -  expected to be OID of client app or a valid group's object id of which the client is part of.")]
        public bool? AadValidateUsername { get; set; }

        [OptionValidation]
        [Option("aof", Required = false, HelpText = "Enable write ahead logging (append-only file).")]
        public bool? EnableAOF { get; set; }

        [MemorySizeValidation]
        [Option("aof-memory", Required = false, HelpText = "Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit")]
        public string AofMemorySize { get; set; }

        [MemorySizeValidation]
        [Option("aof-page-size", Required = false, HelpText = "Size of each AOF page in bytes(rounds down to power of 2)")]
        public string AofPageSize { get; set; }

        [IntRangeValidation(1, 64)]
        [Option("aof-sublog-count", Required = false, HelpText = "Number of AOF sublogs (=1 default single log, >1: multi-log).")]
        public int AofSublogCount { get; set; }

        [IntRangeValidation(1, 64)]
        [Option("aof-replay-subtask-count", Required = false, HelpText = "Number of logical replay tasks per sublog at replica.")]
        public int AofReplaySubtaskCount { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("aof-sublog-refresh-tail-freq", Required = false, HelpText = "Refresh sublog tail background task execution frequency.")]
        public int AofRefreshSublogTailFrequencyMs { get; set; }

        [IntRangeValidation(-1, int.MaxValue)]
        [Option("aof-commit-freq", Required = false, HelpText = "Write ahead logging (append-only file) commit issue frequency in milliseconds. 0 = issue an immediate commit per operation, -1 = manually issue commits using COMMITAOF command")]
        public int CommitFrequencyMs { get; set; }

        [OptionValidation]
        [Option("aof-commit-wait", Required = false, HelpText = "Wait for AOF to flush the commit before returning results to client. Warning: will greatly increase operation latency.")]
        public bool? WaitForCommit { get; set; }

        [MemorySizeValidation(false)]
        [Option("aof-size-limit", Required = false, HelpText = "Maximum size of AOF (rounds down to power of 2) after which unsafe truncation will be applied. Left empty AOF will grow without bound unless a checkpoint is taken")]
        public string AofSizeLimit { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("aof-size-limit-enforce-frequency", Required = false, HelpText = "Frequency (in secs) of execution of the AutoCheckpointBasedOnAofSizeLimit background task.")]
        public int AofSizeLimitEnforceFrequencySecs { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("aof-refresh-freq", Required = false, HelpText = "AOF replication (safe tail address) refresh frequency in milliseconds. 0 = auto refresh after every enqueue.")]
        public int AofReplicationRefreshFrequencyMs { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("subscriber-refresh-freq", Required = false, HelpText = "Subscriber (safe tail address) refresh frequency in milliseconds (for pub-sub). 0 = auto refresh after every enqueue.")]
        public int SubscriberRefreshFrequencyMs { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("compaction-freq", Required = false, HelpText = "Background hybrid log compaction frequency in seconds. 0 = disabled (compaction performed before checkpointing instead)")]
        public int CompactionFrequencySecs { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("expired-object-collection-freq", Required = false, HelpText = "Frequency in seconds for the background task to perform object collection which removes expired members within object from memory. 0 = disabled. Use the HCOLLECT and ZCOLLECT API to collect on-demand.")]
        public int ExpiredObjectCollectionFrequencySecs { get; set; }

        [Option("compaction-type", Required = false, HelpText = "Hybrid log compaction type. Value options: None - no compaction, Shift - shift begin address without compaction (data loss), Scan - scan old pages and move live records to tail (no data loss), Lookup - lookup each record in compaction range, for record liveness checking using hash chain (no data loss)")]
        public LogCompactionType CompactionType { get; set; }

        [OptionValidation]
        [Option("compaction-force-delete", Required = false, HelpText = "Forcefully delete the inactive segments immediately after the compaction strategy (type) is applied. If false, take a checkpoint to actually delete the older data files from disk.")]
        public bool? CompactionForceDelete { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("compaction-max-segments", Required = false, HelpText = "Number of log segments created on disk before compaction triggers.")]
        public int CompactionMaxSegments { get; set; }

        [OptionValidation]
        [Option("lua", Required = false, HelpText = "Enable Lua scripts on server.")]
        public bool? EnableLua { get; set; }

        [OptionValidation]
        [Option("lua-transaction-mode", Required = false, HelpText = "Run Lua scripts as a transaction (lock keys - run script - unlock keys).")]
        public bool? LuaTransactionMode { get; set; }

        [PercentageValidation]
        [Option("gossip-sp", Required = false, HelpText = "Percent of cluster nodes to gossip with at each gossip iteration.")]
        public int GossipSamplePercent { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("gossip-delay", Required = false, HelpText = "Cluster mode gossip protocol per node sleep (in seconds) delay to send updated config.")]
        public int GossipDelay { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("cluster-timeout", Required = false, HelpText = "Cluster node timeout is the amount of seconds a node must be unreachable.")]
        public int ClusterTimeout { get; set; }

        [IntRangeValidation(-1, int.MaxValue)]
        [Option("cluster-config-flush-frequency", Required = false, HelpText = "How frequently to flush cluster config unto disk to persist updates. =-1: never (memory only), =0: immediately (every update performs flush), >0: frequency in ms")]
        public int ClusterConfigFlushFrequencyMs { get; set; }

        [Option("cluster-tls-client-target-host", Required = false, HelpText = "Name for the client target host when using TLS connections in cluster mode.")]
        public string ClusterTlsClientTargetHost { get; set; }

        [OptionValidation]
        [Option("server-certificate-required", Required = false, HelpText = "Whether server TLS certificate is required by clients established on the server side, e.g., for cluster gossip and replication.")]
        public bool? ServerCertificateRequired { get; set; }

        [OptionValidation]
        [Option("tls", Required = false, HelpText = "Enable TLS.")]
        public bool? EnableTLS { get; set; }

        [CertFileValidation(true, true, false)]
        [Option("cert-file-name", Required = false, HelpText = "TLS certificate file name (example: testcert.pfx).")]
        public string CertFileName { get; set; }

        [HiddenOption]
        [Option("cert-password", Required = false, HelpText = "TLS certificate password (example: placeholder).")]
        public string CertPassword { get; set; }

        [Option("cert-subject-name", Required = false, HelpText = "TLS certificate subject name.")]
        public string CertSubjectName { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("cert-refresh-freq", Required = false, HelpText = "TLS certificate refresh frequency in seconds (0 to disable).")]
        public int CertificateRefreshFrequency { get; set; }

        [OptionValidation]
        [Option("client-certificate-required", Required = false, HelpText = "Whether client TLS certificate is required by the server.")]
        public bool? ClientCertificateRequired { get; set; }

        [Option("certificate-revocation-check-mode", Required = false, HelpText = "Certificate revocation check mode for certificate validation (NoCheck, Online, Offline).")]
        public X509RevocationMode CertificateRevocationCheckMode { get; set; }

        [Option("issuer-certificate-path", Required = false, HelpText = "Full path of file with issuer certificate for validation. If empty or null, validation against issuer will not be performed.")]
        public string IssuerCertificatePath { get; set; }

        [OptionValidation]
        [Option("latency-monitor", Required = false, HelpText = "Track latency of various events.")]
        public bool? LatencyMonitor { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("slowlog-log-slower-than", Required = false, HelpText = "Threshold (microseconds) for logging command in the slow log. 0 to disable.")]
        public int SlowLogThreshold { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("slowlog-max-len", Required = false, HelpText = "Maximum number of slow log entries to keep.")]
        public int SlowLogMaxEntries { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("metrics-sampling-freq", Required = false, HelpText = "Metrics sampling frequency in seconds. Value of 0 disables metrics monitor task.")]
        public int MetricsSamplingFrequency { get; set; }

        [OptionValidation]
        [Option('q', Required = false, HelpText = "Enabling quiet mode does not print server version and text art.")]
        public bool? QuietMode { get; set; }

        [Option("logger-level", Required = false, HelpText = "Logging level. Value options: Trace, Debug, Information, Warning, Error, Critical, None")]
        public LogLevel LogLevel { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("logger-freq", Required = false, HelpText = "Frequency (in seconds) of logging (used for tracking progress of long running operations e.g. migration)")]
        public int LoggingFrequency { get; set; }

        [OptionValidation]
        [Option("disable-console-logger", Required = false, HelpText = "Disable console logger.")]
        public bool? DisableConsoleLogger { get; set; }

        [FilePathValidation(false, false, false)]
        [Option("file-logger", Required = false, HelpText = "Enable file logger and write to the specified path.")]
        public string FileLogger { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("minthreads", Required = false, HelpText = "Minimum worker threads in thread pool, 0 uses the system default.")]
        public int ThreadPoolMinThreads { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("maxthreads", Required = false, HelpText = "Maximum worker threads in thread pool, 0 uses the system default.")]
        public int ThreadPoolMaxThreads { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("miniothreads", Required = false, HelpText = "Minimum IO completion threads in thread pool, 0 uses the system default.")]
        public int ThreadPoolMinIOCompletionThreads { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("maxiothreads", Required = false, HelpText = "Maximum IO completion threads in thread pool, 0 uses the system default.")]
        public int ThreadPoolMaxIOCompletionThreads { get; set; }

        [IntRangeValidation(-1, int.MaxValue)]
        [Option("network-connection-limit", Required = false, HelpText = "Maximum number of simultaneously active network connections.")]
        public int NetworkConnectionLimit { get; set; }

        [OptionValidation]
        [Option("use-azure-storage", Required = false, HelpText = "Use Azure Page Blobs for storage instead of local storage.")]
        public bool? UseAzureStorage { get; set; }

        [HttpsUrlValidation]
        [Option("storage-service-uri", Required = false, HelpText = "The URI to use when establishing connection to Azure Blobs Storage.")]
        public string AzureStorageServiceUri { get; set; }

        [Option("storage-managed-identity", Required = false, HelpText = "The managed identity to use when establishing connection to Azure Blobs Storage.")]
        public string AzureStorageManagedIdentity { get; set; }

        [HiddenOption]
        [Option("storage-string", Required = false, HelpText = "The connection string to use when establishing connection to Azure Blobs Storage.")]
        public string AzureStorageConnectionString { get; set; }

        [IntRangeValidation(-1, int.MaxValue)]
        [Option("checkpoint-throttle-delay", Required = false, HelpText = "Whether and by how much should we throttle the disk IO for checkpoints: -1 - disable throttling; >= 0 - run checkpoint flush in separate task, sleep for specified time after each WriteAsync")]
        public int CheckpointThrottleFlushDelayMs { get; set; }

        [OptionValidation]
        [Option("fast-commit", Required = false, HelpText = "Use FastCommit when writing AOF.")]
        public bool? EnableFastCommit { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("fast-commit-throttle", Required = false, HelpText = "Throttle FastCommit to write metadata once every K commits.")]
        public int FastCommitThrottleFreq { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("network-send-throttle", Required = false, HelpText = "Throttle the maximum outstanding network sends per session.")]
        public int NetworkSendThrottleMax { get; set; }

        [OptionValidation]
        [Option("sg-get", Required = false, HelpText = "Whether we use scatter gather IO for MGET or a batch of contiguous GET operations - useful to saturate disk random read IO.")]
        public bool? EnableScatterGatherGet { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("replica-sync-delay", Required = false, HelpText = "Whether and by how much (milliseconds) should we throttle the replica sync: 0 - disable throttling")]
        public int ReplicaSyncDelayMs { get; set; }

        [IntRangeValidation(-1, int.MaxValue)]
        [Option("replica-offset-max-lag", Required = false, HelpText = "Throttle ClusterAppendLog when replica.AOFTailAddress - ReplicationOffset > ReplicationOffsetMaxLag. 0: Synchronous replay,  >=1: background replay with specified lag, -1: infinite lag")]
        public int ReplicationOffsetMaxLag { get; set; }

        [OptionValidation]
        [Option("main-memory-replication", Required = false, HelpText = "Use main-memory replication model.")]
        public bool? MainMemoryReplication { get; set; }

        [OptionValidation]
        [Option("fast-aof-truncate", Required = false, HelpText = "Use fast-aof-truncate replication model.")]
        public bool? FastAofTruncate { get; set; }

        [OptionValidation]
        [Option("on-demand-checkpoint", Required = false, HelpText = "Used with main-memory replication model. Take on demand checkpoint to avoid missing data when attaching")]
        public bool? OnDemandCheckpoint { get; set; }

        [OptionValidation]
        [Option("repl-diskless-sync", Required = false, HelpText = "Whether diskless replication is enabled or not.")]
        public bool? ReplicaDisklessSync { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("repl-diskless-sync-delay", Required = false, HelpText = "Delay in diskless replication sync in seconds. =0: Immediately start diskless replication sync.")]
        public int ReplicaDisklessSyncDelay { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("repl-attach-timeout", Required = false, HelpText = "Timeout in seconds for replication attach operation.")]
        public int ReplicaAttachTimeout { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("repl-sync-timeout", Required = false, HelpText = "Timeout in seconds for replication sync operations.")]
        public int ReplicaSyncTimeout { get; set; }

        [MemorySizeValidation(false)]
        [Option("repl-diskless-sync-full-sync-aof-threshold", Required = false, HelpText = "AOF replay size threshold for diskless replication, beyond which we will perform a full sync even if a partial sync is possible. Defaults to AOF memory size if not specified.")]
        public string ReplicaDisklessSyncFullSyncAofThreshold { get; set; }

        [OptionValidation]
        [Option("aof-null-device", Required = false, HelpText = "With main-memory replication, use null device for AOF. Ensures no disk IO, but can cause data loss during replication.")]
        public bool? UseAofNullDevice { get; set; }

        [System.Text.Json.Serialization.JsonIgnore]
        [FilePathValidation(true, false, false)]
        [Option("config-import-path", Required = false, HelpText = "Import (load) configuration options from the provided path")]
        public string ConfigImportPath { get; set; }

        [System.Text.Json.Serialization.JsonIgnore]
        [Option("config-import-format", Required = false, HelpText = $"Format of configuration options in path specified by config-import-path")]
        public ConfigFileType ConfigImportFormat { get; set; }

        [System.Text.Json.Serialization.JsonIgnore]
        [Option("config-export-format", Required = false, HelpText = $"Format to export configuration options to path specified by config-export-path")]
        public ConfigFileType ConfigExportFormat { get; set; }

        [System.Text.Json.Serialization.JsonIgnore]
        [OptionValidation]
        [Option("use-azure-storage-for-config-import", Required = false, Default = false, HelpText = "Use Azure storage to import config file")]
        public bool? UseAzureStorageForConfigImport { get; set; }

        [System.Text.Json.Serialization.JsonIgnore]
        [FilePathValidation(false, false, false)]
        [Option("config-export-path", Required = false, HelpText = "Export (save) current configuration options to the provided path")]
        public string ConfigExportPath { get; set; }

        [System.Text.Json.Serialization.JsonIgnore]
        [OptionValidation]
        [Option("use-azure-storage-for-config-export", Required = false, Default = false, HelpText = "Use Azure storage to export config file")]
        public bool? UseAzureStorageForConfigExport { get; set; }

        [OptionValidation]
        [Option("use-native-device-linux", Required = false, HelpText = "Use native device on Linux for local storage")]
        public bool? UseNativeDeviceLinux { get; set; }

        [Option("reviv-bin-record-sizes", Separator = ',', Required = false,
            HelpText = "#,#,...,#: For the main store, the sizes of records in each revivification bin, in order of increasing size." +
                       "           Supersedes the default --reviv; cannot be used with --reviv-in-chain-only")]
        public IEnumerable<int> RevivBinRecordSizes { get; set; }

        [Option("reviv-bin-record-counts", Separator = ',', Required = false,
            HelpText = "#,#,...,#: For the main store, the number of records in each bin:" +
                       "    Default (not specified): If reviv-bin-record-sizes is specified, each bin is 256 records" +
                       "    # (one value): If reviv-bin-record-sizes is specified, then all bins have this number of records, else error" +
                       "    #,#,...,# (multiple values): If reviv-bin-record-sizes is specified, then it must be the same size as that array, else error" +
                       "                                 Supersedes the default --reviv; cannot be used with --reviv-in-chain-only")]
        public IEnumerable<int> RevivBinRecordCounts { get; set; }

        [DoubleRangeValidation(0, 1)]
        [Option("reviv-fraction", Required = false,
            HelpText = "#: Fraction of mutable in-memory log space, from the highest log address down to the read-only region, that is eligible for revivification.")]
        public double RevivifiableFraction { get; set; }

        [OptionValidation]
        [Option("reviv", Required = false,
            HelpText = "A shortcut to specify revivification with default power-of-2-sized bins." +
                       "    This default can be overridden by --reviv-in-chain-only or by the combination of reviv-bin-record-sizes and reviv-bin-record-counts.")]
        public bool? EnableRevivification { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("reviv-search-next-higher-bins", Required = false,
            HelpText = "Search this number of next-higher bins if the search cannot be satisfied in the best-fitting bin." +
                       "    Requires --reviv or the combination of rconeviv-bin-record-sizes and reviv-bin-record-counts")]
        public int RevivNumberOfBinsToSearch { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("reviv-bin-best-fit-scan-limit", Required = false,
            HelpText = "Number of records to scan for best fit after finding first fit." +
                       "    Requires --reviv or the combination of reviv-bin-record-sizes and reviv-bin-record-counts" +
                       "    0: Use first fit" +
                       "    #: Limit scan to this many records after first fit, up to the record count of the bin")]
        public int RevivBinBestFitScanLimit { get; set; }

        [OptionValidation]
        [Option("reviv-in-chain-only", Required = false,
            HelpText = "Revivify tombstoned records in tag chains only (do not use free list)." +
                       "    Cannot be used with reviv-bin-record-sizes or reviv-bin-record-counts.")]
        public bool? RevivInChainOnly { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("object-scan-count-limit", Required = false, HelpText = "Limit of items to return in one iteration of *SCAN command")]
        public int ObjectScanCountLimit { get; set; }

        [OptionValidation]
        [Option("enable-debug-command", Required = false, HelpText = "Enable DEBUG command for 'no', 'local' or 'all' connections")]
        public ConnectionProtectionOption EnableDebugCommand { get; set; }

        [OptionValidation]
        [Option("enable-module-command", Required = false, HelpText = "Enable MODULE command for 'no', 'local' or 'all' connections. Command can only load from paths listed in ExtensionBinPaths")]
        public ConnectionProtectionOption EnableModuleCommand { get; set; }

        [OptionValidation]
        [Option("protected-mode", Required = false, HelpText = "Enable protected mode.")]
        public CommandLineBooleanOption ProtectedMode { get; set; }

        [DirectoryPathsValidation(true, false)]
        [Option("extension-bin-paths", Separator = ',', Required = false, HelpText = "List of directories on server from which custom command binaries can be loaded by admin users. MODULE command also requires enable-module-command to be set")]
        public IEnumerable<string> ExtensionBinPaths { get; set; }

        [ModuleFilePathValidation(true, true, false)]
        [Option("loadmodulecs", Separator = ',', Required = false, HelpText = "List of modules to be loaded")]
        public IEnumerable<string> LoadModuleCS { get; set; }

        [Option("extension-allow-unsigned", Required = false, HelpText = "Allow loading custom commands from digitally unsigned assemblies (not recommended)")]
        public bool? ExtensionAllowUnsignedAssemblies { get; set; }

        [IntRangeValidation(1, int.MaxValue, isRequired: false)]
        [Option("index-resize-freq", Required = false, HelpText = "Index resize check frequency in seconds")]
        public int IndexResizeFrequencySecs { get; set; }

        [IntRangeValidation(1, 100, isRequired: false)]
        [Option("index-resize-threshold", Required = false, HelpText = "Overflow bucket count over total index size in percentage to trigger index resize")]
        public int IndexResizeThreshold { get; set; }

        [OptionValidation]
        [Option("fail-on-recovery-error", Required = false, HelpText = "Server bootup should fail if errors happen during bootup of AOF and checkpointing")]
        public bool? FailOnRecoveryError { get; set; }

        [OptionValidation]
        [Option("skip-rdb-restore-checksum-validation", Required = false, HelpText = "Skip RDB restore checksum validation")]
        public bool? SkipRDBRestoreChecksumValidation { get; set; }

        [OptionValidation]
        [Option("lua-memory-management-mode", Required = false, HelpText = "Memory management mode for Lua scripts, must be set to Tracked or Managed to impose script limits")]
        public LuaMemoryManagementMode LuaMemoryManagementMode { get; set; }

        [MemorySizeValidation(false)]
        [ForbiddenWithOption(nameof(LuaMemoryManagementMode), nameof(LuaMemoryManagementMode.Native))]
        [Option("lua-script-memory-limit", HelpText = "Memory limit for a Lua instances while running a script, lua-memory-management-mode must be set to something other than Native to use this flag")]
        public string LuaScriptMemoryLimit { get; set; }

        [IntRangeValidation(10, int.MaxValue, isRequired: false)]
        [Option("lua-script-timeout", Required = false, HelpText = "Timeout for a Lua instance while running a script, specified in positive milliseconds (0 = disabled)")]
        public int LuaScriptTimeoutMs { get; set; }

        [OptionValidation]
        [Option("lua-logging-mode", Required = false, HelpText = "Behavior of redis.log(...) when called from Lua scripts.  Defaults to Enable.")]
        public LuaLoggingMode LuaLoggingMode { get; set; }

        // Parsing is a tad tricky here as JSON wants to set to empty at certain points
        //
        // A bespoke union-on-set gets the desired semantics.
        private readonly HashSet<string> luaAllowedFunctions = [];

        [OptionValidation]
        [Option("lua-allowed-functions", Separator = ',', Required = false, HelpText = "If set, restricts the functions available in Lua scripts to given list.")]
        public IEnumerable<string> LuaAllowedFunctions
        {
            get => luaAllowedFunctions;
            set
            {
                if (value == null)
                {
                    return;
                }

                luaAllowedFunctions.UnionWith(value);
            }
        }

        [FilePathValidation(false, true, false)]
        [Option("unixsocket", Required = false, HelpText = "Unix socket address path to bind server to")]
        public string UnixSocketPath { get; set; }

        [IntRangeValidation(0, 777, isRequired: false)]
        [SupportedOSValidation(isRequired: false, nameof(OSPlatform.Linux), nameof(OSPlatform.OSX), nameof(OSPlatform.FreeBSD))]
        [Option("unixsocketperm", Required = false, HelpText = "Unix socket permissions in octal (Unix platforms only)")]
        public int UnixSocketPermission { get; set; }

        [IntRangeValidation(1, 256, isRequired: true)]
        [Option("max-databases", Required = false, HelpText = "Max number of logical databases allowed in a single Garnet server instance")]
        public int MaxDatabases { get; set; }

        [IntRangeValidation(-1, int.MaxValue, isRequired: false)]
        [Option("expired-key-deletion-scan-freq", Required = false, HelpText = "Frequency of background scan for expired key deletion, in seconds")]
        public int ExpiredKeyDeletionScanFrequencySecs { get; set; }

        [IntRangeValidation(0, int.MaxValue, includeMin: true, isRequired: false)]
        [Option("cluster-replication-reestablishment-timeout")]
        public int ClusterReplicationReestablishmentTimeout { get; set; }

        [Option("cluster-replica-resume-with-data", Required = false, HelpText = "If a Cluster Replica resumes with data, allow it to be served prior to a Primary being available")]
        public bool ClusterReplicaResumeWithData { get; set; }

        /// <summary>
        /// This property contains all arguments that were not parsed by the command line argument parser
        /// </summary>
        [System.Text.Json.Serialization.JsonIgnore]
        [Value(0)]
        public IList<string> UnparsedArguments { get; set; }

        /// <summary>
        /// Logger instance used for runtime option validation
        /// </summary>
        public ILogger runtimeLogger { get; set; }

        /// <summary>
        /// Check the validity of all options with an explicit ValidationAttribute
        /// </summary>
        /// <param name="invalidOptions">List of invalid options</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if all property values are valid</returns>
        public bool IsValid(out List<string> invalidOptions, ILogger logger = null)
        {
            invalidOptions = [];
            var isValid = true;

            this.runtimeLogger = logger;
            foreach (var prop in typeof(Options).GetProperties())
            {
                // Ignore if property is not decorated with the OptionsAttribute or the ValidationAttribute
                var validationAttr = prop.GetCustomAttributes(typeof(ValidationAttribute)).FirstOrDefault();
                if (!Attribute.IsDefined(prop, typeof(OptionAttribute)) || validationAttr == null)
                    continue;

                // Get value to validate and set validation context
                var value = prop.GetValue(this, null);
                var validationContext = new ValidationContext(this) { MemberName = prop.Name };

                // Validate the current value
                var validationResults = new List<ValidationResult>();
                var isValueValid = Validator.TryValidateProperty(value, validationContext, validationResults);

                // Append results 
                isValid = isValid && isValueValid;
                foreach (var validationResult in validationResults)
                {
                    invalidOptions.AddRange(validationResult.MemberNames);
                    logger?.LogError("{errorMessage}", validationResult.ErrorMessage);
                }
            }

            return isValid;
        }

        public GarnetServerOptions GetServerOptions(ILogger logger = null)
        {
            var useAzureStorage = UseAzureStorage.GetValueOrDefault();
            var enableStorageTier = EnableStorageTier.GetValueOrDefault();
            var enableRevivification = EnableRevivification.GetValueOrDefault();

            if (useAzureStorage && string.IsNullOrEmpty(AzureStorageConnectionString) && string.IsNullOrEmpty(AzureStorageServiceUri))
            {
                throw new InvalidAzureConfiguration("Cannot enable use-azure-storage without supplying storage-string or storage-service-uri");
            }
            if (useAzureStorage && !string.IsNullOrEmpty(AzureStorageConnectionString) && !string.IsNullOrEmpty(AzureStorageServiceUri))
            {
                throw new InvalidAzureConfiguration("Cannot enable use-azure-storage with both storage-string and storage-service-uri");
            }

            var logDir = LogDir;
            if (!useAzureStorage && enableStorageTier) logDir = new DirectoryInfo(string.IsNullOrEmpty(logDir) ? "." : logDir).FullName;
            var checkpointDir = CheckpointDir;
            if (!useAzureStorage) checkpointDir = new DirectoryInfo(string.IsNullOrEmpty(checkpointDir) ? (string.IsNullOrEmpty(logDir) ? "." : logDir) : checkpointDir).FullName;

            if (!Format.TryParseAddressList(Address, Port, out var endpoints, out _, ProtectedMode == CommandLineBooleanOption.True)
              || endpoints.Length == 0)
                throw new GarnetException($"Invalid endpoint format {Address} {Port}.");

            EndPoint[] clusterAnnounceEndpoint = null;
            if (ClusterAnnounceIp != null)
            {
                ClusterAnnouncePort = ClusterAnnouncePort == 0 ? Port : ClusterAnnouncePort;
                clusterAnnounceEndpoint = Format.TryCreateEndpoint(ClusterAnnounceIp, ClusterAnnouncePort, tryConnect: false, logger: logger).GetAwaiter().GetResult();
                if (clusterAnnounceEndpoint == null || !endpoints.Any(endpoint => endpoint.Equals(clusterAnnounceEndpoint[0])))
                    throw new GarnetException("Cluster announce endpoint does not match list of listen endpoints provided!");
            }

            if (!string.IsNullOrEmpty(UnixSocketPath))
                endpoints = [.. endpoints, new UnixDomainSocketEndPoint(UnixSocketPath)];

            // Unix file permission octal to UnixFileMode
            var unixSocketPermissions = (UnixFileMode)Convert.ToInt32(UnixSocketPermission.ToString(), 8);

            var revivBinRecordSizes = this.RevivBinRecordSizes?.ToArray();
            var revivBinRecordCounts = this.RevivBinRecordCounts?.ToArray();
            bool hasRecordSizes = revivBinRecordSizes?.Length > 0, hasRecordCounts = revivBinRecordCounts?.Length > 0;
            bool useRevivBinsPowerOf2 = enableRevivification; // may be overridden

            if (hasRecordSizes)
            {
                if (hasRecordCounts && revivBinRecordCounts.Length > 1 && revivBinRecordCounts.Length != revivBinRecordSizes.Length)
                    throw new Exception("Incompatible revivification record size and count cardinality.");
                if (RevivInChainOnly.GetValueOrDefault())
                    throw new Exception("Revivification cannot specify both record sizes and in-chain-only.");
                useRevivBinsPowerOf2 = false;
            }
            if (hasRecordCounts)
            {
                if (enableRevivification)
                    throw new Exception("Revivification cannot specify both record counts and powerof2 bins.");
                if (!hasRecordSizes)
                    throw new Exception("Revivification bin counts require bin sizes.");
                useRevivBinsPowerOf2 = false;
            }
            if (RevivBinBestFitScanLimit != 0)
            {
                if (!hasRecordSizes && !enableRevivification)
                    throw new Exception("Revivification cannot specify best fit scan limit without specifying bins.");
                if (RevivBinBestFitScanLimit < 0)
                    throw new Exception("RevivBinBestFitScanLimit must be >= 0.");
            }
            if (RevivifiableFraction != RevivificationSettings.DefaultRevivifiableFraction)
            {
                if (!hasRecordSizes && !enableRevivification)
                    throw new Exception("Revivification cannot specify RevivifiableFraction without specifying bins.");
            }

            // For backwards compatibility
            if (CompactionType == LogCompactionType.ShiftForced)
            {
                logger?.LogWarning("Compaction type ShiftForced is deprecated. Use Shift instead along with CompactionForceDelete.");
                CompactionType = LogCompactionType.Shift;
                CompactionForceDelete = true;
            }

            if (SlowLogThreshold > 0)
            {
                if (SlowLogThreshold < 100)
                    throw new Exception("SlowLogThreshold must be at least 100 microseconds.");
            }

            Func<INamedDeviceFactoryCreator> azureFactoryCreator = () =>
            {
                if (!string.IsNullOrEmpty(AzureStorageConnectionString))
                {
                    return new AzureStorageNamedDeviceFactoryCreator(AzureStorageConnectionString, logger);
                }
                var credential = new ChainedTokenCredential(
                    new WorkloadIdentityCredential(),
                    new ManagedIdentityCredential(clientId: AzureStorageManagedIdentity)
                );
                return new AzureStorageNamedDeviceFactoryCreator(AzureStorageServiceUri, credential, logger);
            };

            return new GarnetServerOptions(logger)
            {
                EndPoints = endpoints,
                ClusterAnnounceEndpoint = clusterAnnounceEndpoint?[0],
                MemorySize = MemorySize,
                PageSize = PageSize,
                SegmentSize = SegmentSize,
                IndexSize = IndexSize,
                IndexMaxSize = IndexMaxSize,
                MutablePercent = MutablePercent,
                EnableReadCache = EnableReadCache.GetValueOrDefault(),
                ReadCacheMemorySize = ReadCacheMemorySize,
                ReadCachePageSize = ReadCachePageSize,
                HeapMemorySize = HeapMemorySize,
                ReadCacheHeapMemorySize = ReadCacheHeapMemorySize,
                EnableStorageTier = enableStorageTier,
                CopyReadsToTail = CopyReadsToTail.GetValueOrDefault(),
                LogDir = logDir,
                CheckpointDir = checkpointDir,
                Recover = Recover.GetValueOrDefault(),
                EnableIncrementalSnapshots = EnableIncrementalSnapshots.GetValueOrDefault(),
                DisablePubSub = DisablePubSub.GetValueOrDefault(),
                PubSubPageSize = PubSubPageSize,
                DisableObjects = DisableObjects.GetValueOrDefault(),
                EnableCluster = EnableCluster.GetValueOrDefault(),
                CleanClusterConfig = CleanClusterConfig.GetValueOrDefault(),
                ParallelMigrateTaskCount = ParallelMigrateTaskCount,
                FastMigrate = FastMigrate.GetValueOrDefault(),
                AuthSettings = GetAuthenticationSettings(logger),
                EnableAOF = EnableAOF.GetValueOrDefault(),
                EnableLua = EnableLua.GetValueOrDefault(),
                LuaTransactionMode = LuaTransactionMode.GetValueOrDefault(),
                AofMemorySize = AofMemorySize,
                AofPageSize = AofPageSize,
                AofSublogCount = AofSublogCount,
                AofReplaySubtaskCount = AofReplaySubtaskCount,
                AofRefreshSublogTailFrequencyMs = AofRefreshSublogTailFrequencyMs,
                AofReplicationRefreshFrequencyMs = AofReplicationRefreshFrequencyMs,
                CommitFrequencyMs = CommitFrequencyMs,
                WaitForCommit = WaitForCommit.GetValueOrDefault(),
                AofSizeLimit = AofSizeLimit,
                AofSizeLimitEnforceFrequencySecs = AofSizeLimitEnforceFrequencySecs,
                CompactionFrequencySecs = CompactionFrequencySecs,
                ExpiredObjectCollectionFrequencySecs = ExpiredObjectCollectionFrequencySecs,
                CompactionType = CompactionType,
                CompactionForceDelete = CompactionForceDelete.GetValueOrDefault(),
                CompactionMaxSegments = CompactionMaxSegments,
                GossipSamplePercent = GossipSamplePercent,
                GossipDelay = GossipDelay,
                ClusterTimeout = ClusterTimeout,
                ClusterConfigFlushFrequencyMs = ClusterConfigFlushFrequencyMs,
                EnableFastCommit = EnableFastCommit.GetValueOrDefault(),
                FastCommitThrottleFreq = FastCommitThrottleFreq,
                NetworkSendThrottleMax = NetworkSendThrottleMax,
                TlsOptions = EnableTLS.GetValueOrDefault() ? new GarnetTlsOptions(
                    CertFileName, CertPassword,
                    ClientCertificateRequired.GetValueOrDefault(),
                    CertificateRevocationCheckMode,
                    IssuerCertificatePath,
                    CertSubjectName,
                    CertificateRefreshFrequency,
                    EnableCluster.GetValueOrDefault(),
                    ClusterTlsClientTargetHost,
                    ServerCertificateRequired.GetValueOrDefault(),
                    logger: logger) : null,
                LatencyMonitor = LatencyMonitor.GetValueOrDefault(),
                SlowLogThreshold = SlowLogThreshold,
                SlowLogMaxEntries = SlowLogMaxEntries,
                MetricsSamplingFrequency = MetricsSamplingFrequency,
                LogLevel = LogLevel,
                LoggingFrequency = LoggingFrequency,
                QuietMode = QuietMode.GetValueOrDefault(),
                ThreadPoolMinThreads = ThreadPoolMinThreads,
                ThreadPoolMaxThreads = ThreadPoolMaxThreads,
                ThreadPoolMinIOCompletionThreads = ThreadPoolMinIOCompletionThreads,
                ThreadPoolMaxIOCompletionThreads = ThreadPoolMaxIOCompletionThreads,
                NetworkConnectionLimit = NetworkConnectionLimit,
                DeviceFactoryCreator = useAzureStorage
                    ? azureFactoryCreator()
                    : new LocalStorageNamedDeviceFactoryCreator(useNativeDeviceLinux: UseNativeDeviceLinux.GetValueOrDefault(), logger: logger),
                CheckpointThrottleFlushDelayMs = CheckpointThrottleFlushDelayMs,
                EnableScatterGatherGet = EnableScatterGatherGet.GetValueOrDefault(),
                ReplicaSyncDelayMs = ReplicaSyncDelayMs,
                ReplicationOffsetMaxLag = ReplicationOffsetMaxLag,
                FastAofTruncate = GetFastAofTruncate(logger),
                OnDemandCheckpoint = OnDemandCheckpoint.GetValueOrDefault(),
                ReplicaDisklessSync = ReplicaDisklessSync.GetValueOrDefault(),
                ReplicaDisklessSyncDelay = ReplicaDisklessSyncDelay,
                ReplicaSyncTimeout = ReplicaSyncTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(ReplicaSyncTimeout),
                ReplicaAttachTimeout = ReplicaAttachTimeout <= 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromSeconds(ReplicaAttachTimeout),
                ReplicaDisklessSyncFullSyncAofThreshold = ReplicaDisklessSyncFullSyncAofThreshold,
                UseAofNullDevice = UseAofNullDevice.GetValueOrDefault(),
                ClusterUsername = ClusterUsername,
                ClusterPassword = ClusterPassword,
                UseNativeDeviceLinux = UseNativeDeviceLinux.GetValueOrDefault(),
                ObjectScanCountLimit = ObjectScanCountLimit,
                RevivBinRecordSizes = revivBinRecordSizes,
                RevivBinRecordCounts = revivBinRecordCounts,
                RevivifiableFraction = RevivifiableFraction,
                UseRevivBinsPowerOf2 = useRevivBinsPowerOf2,
                RevivBinBestFitScanLimit = RevivBinBestFitScanLimit,
                RevivNumberOfBinsToSearch = RevivNumberOfBinsToSearch,
                RevivInChainOnly = RevivInChainOnly.GetValueOrDefault(),
                EnableDebugCommand = EnableDebugCommand,
                EnableModuleCommand = EnableModuleCommand,
                ExtensionBinPaths = FileUtils.ConvertToAbsolutePaths(ExtensionBinPaths),
                ExtensionAllowUnsignedAssemblies = ExtensionAllowUnsignedAssemblies.GetValueOrDefault(),
                IndexResizeFrequencySecs = IndexResizeFrequencySecs,
                IndexResizeThreshold = IndexResizeThreshold,
                LoadModuleCS = LoadModuleCS,
                FailOnRecoveryError = FailOnRecoveryError.GetValueOrDefault(),
                SkipRDBRestoreChecksumValidation = SkipRDBRestoreChecksumValidation.GetValueOrDefault(),
                LuaOptions = EnableLua.GetValueOrDefault() ? new LuaOptions(LuaMemoryManagementMode, LuaScriptMemoryLimit, LuaScriptTimeoutMs == 0 ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(LuaScriptTimeoutMs), LuaLoggingMode, LuaAllowedFunctions, logger) : null,
                UnixSocketPath = UnixSocketPath,
                UnixSocketPermission = unixSocketPermissions,
                MaxDatabases = MaxDatabases,
                ExpiredKeyDeletionScanFrequencySecs = ExpiredKeyDeletionScanFrequencySecs,
                ClusterReplicationReestablishmentTimeout = ClusterReplicationReestablishmentTimeout,
                ClusterReplicaResumeWithData = ClusterReplicaResumeWithData,
            };
        }

        private IAuthenticationSettings GetAuthenticationSettings(ILogger logger = null)
        {
            switch (AuthenticationMode)
            {
                case GarnetAuthenticationMode.NoAuth:
                    return new NoAuthSettings();
                case GarnetAuthenticationMode.Password:
                    return new PasswordAuthenticationSettings(Password);
                case GarnetAuthenticationMode.Aad:
                    return new AadAuthenticationSettings(AuthorizedAadApplicationIds?.Split(','), AadAudiences?.Split(','), AadIssuers?.Split(','), IssuerSigningTokenProvider.Create(AadAuthority, logger));
                case GarnetAuthenticationMode.ACL:
                    return new AclAuthenticationPasswordSettings(AclFile, Password);
                case GarnetAuthenticationMode.AclWithAad:
                    var aadAuthSettings = new AadAuthenticationSettings(AuthorizedAadApplicationIds?.Split(','), AadAudiences?.Split(','), AadIssuers?.Split(','), IssuerSigningTokenProvider.Create(AadAuthority, logger), AadValidateUsername.GetValueOrDefault());
                    return new AclAuthenticationAadSettings(AclFile, Password, aadAuthSettings);
                default:
                    logger?.LogError("Unsupported authentication mode: {mode}", AuthenticationMode);
                    throw new Exception($"Authentication mode {AuthenticationMode} is not supported.");
            }
        }

        public bool GetFastAofTruncate(ILogger logger = null)
        {
            if (MainMemoryReplication.GetValueOrDefault())
            {
                logger?.LogError("--main-memory-replication is deprecated. Use --fast-aof-truncate instead.");
                return true;
            }
            return FastAofTruncate.GetValueOrDefault();
        }

        /// <summary>
        /// Creates a clone of the current Options object
        /// This method creates a shallow copy of the values of properties decorated with the OptionAttribute
        /// For IEnumerable types it creates a list containing a shallow copy of all the values in the original IEnumerable
        /// </summary>
        /// <returns>The cloned object</returns>
        public object Clone()
        {
            var clone = new Options();
            foreach (var prop in typeof(Options).GetProperties(BindingFlags.Instance | BindingFlags.Public))
            {
                if (!prop.CanRead || !prop.CanWrite)
                    continue;

                var optionAttr = (OptionAttribute)prop.GetCustomAttributes(typeof(OptionAttribute)).FirstOrDefault();
                if (optionAttr == null)
                    continue;

                var value = prop.GetValue(this);

                if (value == null)
                {
                    prop.SetValue(clone, null);
                    continue;
                }

                var type = prop.PropertyType;
                if (type.IsGenericType &&
                    type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    var elementType = type.GetGenericArguments()[0];
                    var listType = typeof(List<>).MakeGenericType(elementType);
                    var list = (IList)Activator.CreateInstance(listType)!;

                    foreach (var item in (IEnumerable)value)
                        list.Add(item);

                    prop.SetValue(clone, list);
                }
                else
                {
                    prop.SetValue(clone, value);
                }
            }

            return clone;
        }
    }

    /// <summary>
    /// Current supported configuration types
    /// </summary>
    internal enum ConfigFileType
    {
        // Garnet.conf file format (JSON serialized Options object)
        GarnetConf = 0,
        // Redis.conf file format
        RedisConf = 1,
    }

    public sealed class InvalidAzureConfiguration : Exception
    {
        public InvalidAzureConfiguration(string message) : base(message) { }
    }

    /// <summary>
    /// This attribute ensures that a decorated configuration option is not dumped into the log
    /// when the DumpConfig flag is set.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class HiddenOptionAttribute : Attribute
    {

    }
}