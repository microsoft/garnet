---
id: configuration
sidebar_label: Configuration
title: Garnet Configuration
---

## How to Configure Garnet

The Garnet server (GarnetServer.exe) can be configured using a configuration file (e.g. `garnet.conf` or `redis.conf`), while command line arguments can be used to override any settings specified in the file. 
Any settings not specified in either configuration file or command line arguments are set to default valued specified in the [defaults.conf](https://github.com/microsoft/garnet/blob/main/libs/host/defaults.conf) file (path to this file can be overridden via the command line arguments).

Garnet currently supports two configuration file formats:
1) The [`garnet.conf`](#garnetconf) file format (default) - a JSON-formatted collection of settings
2) The [`redis.conf`](#redisconf) file format - a collection of settings in the Redis configuration file format:
    ```
    keyword argument1 argument2 argument3 ... argumentN
    ```
    See Redis [docs](https://redis.io/docs/management/config/) for reference.
    **Important:** Not all redis.conf keywords are supported in Garnet. In order to have full configuration settings coverage, use the `garnet.conf` format.

Specifying a configuration file path (and a default file path) can be done via the command line parameters.
1) For `garnet.conf`:
    ```
    GarnetServer.exe --config-import-path <file-path>
    ```
2) For `redis.conf`:
    ```
    GarnetServer.exe --config-import-path <file-path> --config-import-format RedisConf
    ```

**Note:** To change the path (and/or format) of the defaults configuration file, use the `config-default-import-path` and `config-default-import-format` keywords respectively.

## garnet.conf

The default configuration file format for Garnet, which supports the full-range of configurable Garnet settings.
`garnet.conf` is a JSON-formatted file containing a collection of configuration settings. For all the available settings, see the `defaults.conf` file or refer to the complete Garnet settings [list](#configurable-settings).

## redis.conf

Garnet supports the `redis.conf` file format as a configuration file. Note that not all `redis.conf` keywords are supported. Please see the following list of supported `redis.conf` keywords:

| `redis.conf` keyword      | `garnet.conf` keyword | Notes |
| ----------- | ----------- | ----------- |
| `bind`      | `Address`       | Only the first IP address specified is used; all other addresses are ignored. |
| `protected-mode`   | `ProtectedMode`        | |
| `enable-debug-command`   | `EnableDebugCommand`        | Enable the DEBUG command for `no`, `local`, or `yes` (all) connections. |
| `enable-module-command`   | `EnableModuleCommand`        | Enable the MODULE command for `no`, `local`, or `yes` (all) connections. |
| `port`   | `Port`        | |
| `maxmemory`      | `LogMemorySize`       | |
| `logfile`   | `FileLogger`        | |
| `dir`      | `CheckpointDir`       | |
| `cluster-enabled`   | `EnableCluster`        | |
| `requirepass`      | `Password`       | |
| `aclfile`   | `AclFile`        | |
| `cluster-node-timeout`   | `ClusterTimeout`        | |
| `tls-port`   | `EnableTLS`        | Value used to indicate if TLS should be used, port number is otherwise ignored
| `tls-cert-file`   | `CertFileName`        | Garnet currently supports TLS using a .pfx file and passphrase, while Redis supports TLS using .crt and .key files. In order to use TLS in Garnet while using redis.conf, convert your certificate to .pfx format (see details in the [security](security.md#using-garnetserver-with-tls) section), then use the .pfx file path as the `tls-cert-file` value. If a passphrase was used when creating the original certificate, specify it in the `tls-key-file-pass` parameter as you would in Redis (or via the `--cert-password` command line argument). When starting the server, use the `--cert-subject-name` command line argument to set the certificate subject name, if applicable. |
| `tls-key-file-pass`   | `CertPassword`        | See `tls-cert-file` notes |
| `tls-auth-clients`   | `ClientCertificateRequired`        | See `tls-cert-file` notes |
| `latency-tracking`   | `LatencyMonitor`        | |
| `commandstats-tracking`   | `CommandStatsMonitor`        | |
| `loglevel`   | `LogLevel`        | |
| `io-threads`   | `ThreadPoolMinThreads`        | |
| `repl-diskless-sync-delay`   | `ReplicaSyncDelayMs`        | |
| `unixsocket`   | `UnixSocketPath`        | |
| `unixsocketperm`   | `UnixSocketPermission`        | |
| `slowlog-log-slower-than`   | `SlowLogThreshold`        | |
| `slowlog-max-len`   | `SlowLogMaxEntries`        | |
| `databases`   | `MaxDatabases`        | |

## Command line arguments

Any setting in Garnet can be also configured by specifying a command line argument. 
If the setting is also specified in the configuration file, it will be overridden by the value specified in the command line. 
For all available command line settings, run `GarnetServer.exe -h` or `GarnetServer.exe -help`, or refer to the complete Garnet settings [list](#configurable-settings).

## Configurable Settings

| `garnet.conf`<br/>keyword | Command line keyword(s) | Type | Valid Values | Description |
| ----------- | ----------- | ----------- | ----------- | ----------- |
| **Port** | ```--port``` | ```int``` | Integer in range:<br/>[0, 65535] | Port to run server on |
| **Address** | ```--bind``` | ```string``` | IP Address in v4/v6 format | Whitespace or comma separated string of IP addresses to bind server to (default: any) |
| **ClusterAnnouncePort** | ```--cluster-announce-port``` | ```int``` | Integer in range:<br/>[0, 65535] | Port that this node advertises to other nodes to connect to for gossiping. |
| **ClusterAnnounceIp** | ```--cluster-announce-ip``` | ```string``` | IP Address in v4/v6 format | IP address that this node advertises to other nodes to connect to for gossiping. |
| **ClusterAnnounceHostname** | ```--cluster-announce-hostname``` | ```string``` |  | Hostname that this node advertises to other nodes to connect to for gossiping. |
| **ClusterPreferredEndpointType** | ```--cluster-preferred-endpoint-type``` | ```ClusterPreferredEndpointType``` | ip, hostname, unknown | Determines the endpoint type to be advertised to other nodes. (value options: ip, hostname, unknown) |
| **LogMemorySize** | ```-m```<br/>```--memory``` | ```string``` | Memory size | Total main-log memory (inline and heap) to use, in bytes. Does not need to be a power of 2 |
| **PageSize** | ```-p```<br/>```--page``` | ```string``` | Memory size | Size of each main-log page in bytes (rounds down to power of 2; minimum 512). |
| **PageCount** | ```--pagecount``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Number of main-log pages (rounds down to power of 2). This allows specifying less pages initially than LogMemorySize divided by PageSize. |
| **SegmentSize** | ```-s```<br/>```--segment``` | ```string``` | Memory size | Size of each main-log segment in bytes on disk (rounds down to power of 2) |
| **ObjectLogSegmentSize** | ```--object-log-segment``` | ```string``` | Memory size | Size of each object-log segment in bytes on disk (rounds down to power of 2) |
| **IndexMemorySize** | ```-i```<br/>```--index``` | ```string``` | Memory size | Start size of hash index in bytes (rounds down to power of 2) |
| **IndexMaxMemorySize** | ```--index-max-size``` | ```string``` | Memory size | Max size of hash index in bytes (rounds down to power of 2) |
| **MutablePercent** | ```--mutable-percent``` | ```int``` | Integer in range:<br/>[0, 100] | Percentage of log memory that is kept mutable |
| **EnableReadCache** | ```--readcache``` | ```bool``` |  | Enables read cache for faster access to on-disk records. |
| **ReadCacheMemorySize** | ```--readcache-memory``` | ```string``` | Memory size | Total readcache-log memory (inline and heap) to use if readcache is enabled, in bytes. Does not need to be a power of 2 |
| **ReadCachePageSize** | ```--readcache-page``` | ```string``` | Memory size | Size of each read cache page in bytes (rounds down to power of 2; minimum 512). |
| **ReadCachePageCount** | ```--readcache-pagecount``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Number of readcache-log pages (rounds down to power of 2). This allows specifying less pages initially than ReadCacheMemorySize divided by ReadCachePageSize. |
| **EnableStorageTier** | ```--storage-tier``` | ```bool``` |  | Enable tiering of records (hybrid log) to storage, to support a larger-than-memory store. Use --logdir to specify storage directory. |
| **CopyReadsToTail** | ```--copy-reads-to-tail``` | ```bool``` |  | When records are read from the store's in-memory immutable region or storage device, copy them to the tail of the log. |
| **LogDir** | ```-l```<br/>```--logdir``` | ```string``` |  | Storage directory for tiered records (hybrid log), if storage tiering (--storage-tier) is enabled. Uses current directory if unspecified. |
| **CheckpointDir** | ```-c```<br/>```--checkpointdir``` | ```string``` |  | Storage directory for checkpoints. Uses logdir if unspecified. |
| **Recover** | ```-r```<br/>```--recover``` | ```bool``` |  | Recover from latest checkpoint and log, if present. |
| **DisablePubSub** | ```--no-pubsub``` | ```bool``` |  | Disable pub/sub feature on server. |
| **PubSubPageSize** | ```--pubsub-pagesize``` | ```string``` | Memory size | Page size of log used for pub/sub (rounds down to power of 2) |
| **DisableObjects** | ```--no-obj``` | ```bool``` |  | Disable support for data structure objects. |
| **EnableCluster** | ```--cluster``` | ```bool``` |  | Enable cluster. |
| **CleanClusterConfig** | ```--clean-cluster-config``` | ```bool``` |  | Start with clean cluster config. |
| **ParallelMigrateTaskCount** | ```--pmt``` | ```int``` | Integer in range:<br/>[0, 16384] | Number of parallel migrate tasks to spawn when SLOTS or SLOTSRANGE option is used. |
| **FastMigrate** | ```--fast-migrate``` | ```bool``` |  | When migrating slots 1. write directly to network buffer to avoid unnecessary copies, 2. do not wait for ack from target before sending next batch of keys. |
| **AuthenticationMode** | ```--auth``` | ```GarnetAuthenticationMode``` | NoAuth, Password, Aad, ACL, AclWithAad | Authentication mode of Garnet. This impacts how AUTH command is processed and how clients are authenticated against Garnet. Value options: NoAuth, Password, Aad, ACL |
| **Password** | ```--password``` | ```string``` |  | Authentication string for password authentication. |
| **ClusterUsername** | ```--cluster-username``` | ```string``` |  | Username to authenticate intra-cluster communication with. |
| **ClusterPassword** | ```--cluster-password``` | ```string``` |  | Password to authenticate intra-cluster communication with. |
| **AclFile** | ```--acl-file``` | ```string``` |  | External ACL user file. |
| **AclStrictCustomCommands** | ```--acl-strict-custom-commands``` | ```bool``` |  | If true (default), the server refuses to start when an ACL rule references a custom (extension) command name that no loaded module has registered. Set to false to load unresolved names as-is and log warnings. |
| **AadAuthority** | ```--aad-authority``` | ```string``` |  | The authority of AAD authentication. |
| **AadAudiences** | ```--aad-audiences``` | ```string``` |  | The audiences of AAD token for AAD authentication. Should be a comma separated string. |
| **AadIssuers** | ```--aad-issuers``` | ```string``` |  | The issuers of AAD token for AAD authentication. Should be a comma separated string. |
| **AuthorizedAadApplicationIds** | ```--aad-authorized-app-ids``` | ```string``` |  | The authorized client app Ids for AAD authentication. Should be a comma separated string. |
| **AadValidateUsername** | ```--aad-validate-acl-username``` | ```bool``` |  | Only valid for AclWithAAD mode. Validates username -  expected to be OID of client app or a valid group's object id of which the client is part of. |
| **EnableAOF** | ```--aof``` | ```bool``` |  | Enable write ahead logging (append-only file). |
| **AofMemorySize** | ```--aof-memory``` | ```string``` | Memory size | Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit. Must be at least twice AofPageSize. |
| **AofPageSize** | ```--aof-page-size``` | ```string``` | Memory size | Size of each AOF page in bytes (rounds down to power of 2). Must be at least twice the main-log PageSize, since an AOF entry can be as large as the underlying main-log record being written; object commands like LPUSH/HSET can push this even higher. When you raise this, also raise --aof-memory to at least 2x this value. |
| **AofSegmentSize** | ```--aof-segment-size``` | ```string``` | Memory size | Size of each AOF segment (file) in bytes on disk (rounds down to power of 2). This is the granularity at which AOF files are created and truncated. |
| **AofPhysicalSublogCount** | ```--aof-physical-sublog-count``` | ```int``` | Integer in range:<br/>[1, 4] | Number of AOF physical sublogs (i.e. TsavoriteLog instances) used (=1 equivalent to the legacy single log implementation >1: sharded log implementation. |
| **AofReplayTaskCount** | ```--aof-replay-task-count``` | ```int``` | Integer in range:<br/>[1, 256] | Number of replay tasks per physical sublog at the replica. |
| **AofReplayMaxDrift** | ```--aof-replay-max-drift``` | ```long``` |  | Maximum allowed drift in key sequence numbers between physical sublog replay drivers. When a driver is ahead of the slowest peer by more than this value, it yields. Only effective when aof-physical-sublog-count > 1. -1 = disabled. |
| **AofTailWitnessFreqMs** | ```--aof-tail-witness-freq``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Polling frequency of the background task responsible for moving time ahead for all physical sublogs (Used only with physical sublog value >1). |
| **CommitFrequencyMs** | ```--aof-commit-freq``` | ```int``` | Integer in range:<br/>[-1, MaxValue] | Write ahead logging (append-only file) commit issue frequency in milliseconds. 0 = issue an immediate commit per operation, -1 = manually issue commits using COMMITAOF command |
| **WaitForCommit** | ```--aof-commit-wait``` | ```bool``` |  | Wait for AOF to flush the commit before returning results to client. Warning: will greatly increase operation latency. |
| **AofSizeLimit** | ```--aof-size-limit``` | ```string``` | Memory size | Maximum size of AOF (rounds down to power of 2) after which unsafe truncation will be applied. Left empty AOF will grow without bound unless a checkpoint is taken |
| **AofSizeLimitEnforceFrequencySecs** | ```--aof-size-limit-enforce-frequency``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Frequency (in secs) of execution of the AutoCheckpointBasedOnAofSizeLimit background task. |
| **CompactionFrequencySecs** | ```--compaction-freq``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Background hybrid log compaction frequency in seconds. 0 = disabled (compaction performed before checkpointing instead) |
| **ExpiredObjectCollectionFrequencySecs** | ```--expired-object-collection-freq``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Frequency in seconds for the background task to perform object collection which removes expired members within object from memory. 0 = disabled. Use the HCOLLECT and ZCOLLECT API to collect on-demand. |
| **CompactionType** | ```--compaction-type``` | ```LogCompactionType``` | None, Shift, Lookup, Scan | Hybrid log compaction type. Value options: None - no compaction, Shift - shift begin address without compaction (data loss), Lookup - lookup each record in compaction range, for record liveness checking using hash chain (no data loss; recommended for production use), Scan - scan old pages and move live records to tail (no data loss; NOT RECOMMENDED - builds a temporary parallel KV index proportional to the keyspace, causing significant transient memory use; prefer Lookup) |
| **CompactionForceDelete** | ```--compaction-force-delete``` | ```bool``` |  | Forcefully delete the inactive segments immediately after the compaction strategy (type) is applied. If false, take a checkpoint to actually delete the older data files from disk. |
| **CompactionMaxSegments** | ```--compaction-max-segments``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Number of log segments created on disk before compaction triggers. |
| **EnableLua** | ```--lua``` | ```bool``` |  | Enable Lua scripts on server. |
| **LuaTransactionMode** | ```--lua-transaction-mode``` | ```bool``` |  | Run Lua scripts as a transaction (lock keys - run script - unlock keys). |
| **GossipSamplePercent** | ```--gossip-sp``` | ```int``` | Integer in range:<br/>[0, 100] | Percent of cluster nodes to gossip with at each gossip iteration. |
| **GossipDelay** | ```--gossip-delay``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Cluster mode gossip protocol per node sleep (in seconds) delay to send updated config. |
| **ClusterTimeout** | ```--cluster-timeout``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Cluster node timeout is the amount of seconds a node must be unreachable. |
| **ClusterConfigFlushFrequencyMs** | ```--cluster-config-flush-frequency``` | ```int``` | Integer in range:<br/>[-1, MaxValue] | How frequently to flush cluster config unto disk to persist updates. =-1: never (memory only), =0: immediately (every update performs flush), >0: frequency in ms |
| **ClusterTlsClientTargetHost** | ```--cluster-tls-client-target-host``` | ```string``` |  | Name for the client target host when using TLS connections in cluster mode. |
| **ServerCertificateRequired** | ```--server-certificate-required``` | ```bool``` |  | Whether server TLS certificate is required by clients established on the server side, e.g., for cluster gossip and replication. |
| **EnableTLS** | ```--tls``` | ```bool``` |  | Enable TLS. |
| **CertFileName** | ```--cert-file-name``` | ```string``` |  | TLS certificate file name. Accepts a PKCS#12/PFX file or a PEM-encoded certificate (.pem, .crt, .cer); the format is auto-detected from the file's contents (example: testcert.pfx). |
| **CertPassword** | ```--cert-password``` | ```string``` |  | TLS certificate password (example: placeholder). For a PEM-encoded cert-file-name, this is instead treated as the path to a separate PEM private key file, if the key isn't already included in cert-file-name. |
| **CertSubjectName** | ```--cert-subject-name``` | ```string``` |  | TLS certificate subject name. |
| **CertificateRefreshFrequency** | ```--cert-refresh-freq``` | ```int``` | Integer in range:<br/>[0, MaxValue] | TLS certificate refresh frequency in seconds (0 to disable). |
| **ClientCertificateRequired** | ```--client-certificate-required``` | ```bool``` |  | Whether client TLS certificate is required by the server. |
| **CertificateRevocationCheckMode** | ```--certificate-revocation-check-mode``` | ```X509RevocationMode``` | NoCheck, Online, Offline | Certificate revocation check mode for certificate validation (NoCheck, Online, Offline). |
| **IssuerCertificatePath** | ```--issuer-certificate-path``` | ```string``` |  | Full path of file with issuer certificate for validation. If empty or null, validation against issuer will not be performed. |
| **LatencyMonitor** | ```--latency-monitor``` | ```bool``` |  | Track latency of various events. |
| **CommandStatsMonitor** | ```--commandstats-monitor``` | ```bool``` |  | Track per-command usage statistics (calls, failures, rejections). Exposed via INFO COMMANDSTATS. |
| **SlowLogThreshold** | ```--slowlog-log-slower-than``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Threshold (microseconds) for logging command in the slow log. 0 to disable. |
| **SlowLogMaxEntries** | ```--slowlog-max-len``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Maximum number of slow log entries to keep. |
| **MetricsSamplingFrequency** | ```--metrics-sampling-freq``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Metrics sampling frequency in seconds. Value of 0 disables metrics monitor task. |
| **QuietMode** | ```-q``` | ```bool``` |  | Enabling quiet mode does not print server version and text art. |
| **LogLevel** | ```--logger-level``` | ```LogLevel``` | Trace, Debug, Information, Warning, Error, Critical, None | Logging level. Value options: Trace, Debug, Information, Warning, Error, Critical, None |
| **LoggingFrequency** | ```--logger-freq``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Frequency (in seconds) of logging (used for tracking progress of long running operations e.g. migration) |
| **DisableConsoleLogger** | ```--disable-console-logger``` | ```bool``` |  | Disable console logger. |
| **FileLogger** | ```--file-logger``` | ```string``` |  | Enable file logger and write to the specified path. |
| **ThreadPoolMinThreads** | ```--minthreads``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Minimum worker threads in thread pool, 0 uses the system default. |
| **ThreadPoolMaxThreads** | ```--maxthreads``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Maximum worker threads in thread pool, 0 uses the system default. |
| **ThreadPoolMinIOCompletionThreads** | ```--miniothreads``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Minimum IO completion threads in thread pool, 0 uses the system default. |
| **ThreadPoolMaxIOCompletionThreads** | ```--maxiothreads``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Maximum IO completion threads in thread pool, 0 uses the system default. |
| **NetworkConnectionLimit** | ```--network-connection-limit``` | ```int``` | Integer in range:<br/>[-1, MaxValue] | Maximum number of simultaneously active network connections. |
| **UseAzureStorage** | ```--use-azure-storage``` | ```bool``` |  | Use Azure Page Blobs for storage instead of local storage. |
| **AzureStorageServiceUri** | ```--storage-service-uri``` | ```string``` |  | The URI to use when establishing connection to Azure Blobs Storage. |
| **AzureStorageManagedIdentity** | ```--storage-managed-identity``` | ```string``` |  | The managed identity to use when establishing connection to Azure Blobs Storage. |
| **AzureStorageConnectionString** | ```--storage-string``` | ```string``` |  | The connection string to use when establishing connection to Azure Blobs Storage. |
| **CheckpointThrottleFlushDelayMs** | ```--checkpoint-throttle-delay``` | ```int``` | Integer in range:<br/>[-1, MaxValue] | Whether and by how much should we throttle the disk IO for checkpoints: -1 - disable throttling; >= 0 - run checkpoint flush in separate task, sleep for specified time after each WriteAsync |
| **FastCommitThrottleFreq** | ```--fast-commit-throttle``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Throttle FastCommit to write metadata once every K commits. |
| **NetworkSendThrottleMax** | ```--network-send-throttle``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Throttle the maximum outstanding network sends per session. |
| **EnableScatterGatherGet** | ```--sg-get``` | ```bool``` |  | Whether to use scatter-gather IO for a run of contiguous GET operations - useful to saturate disk random read IO. MGET always uses scatter-gather. |
| **ReplicaSyncDelayMs** | ```--replica-sync-delay``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Whether and by how much (milliseconds) should we throttle the replica sync: 0 - disable throttling |
| **ReplicationOffsetMaxLag** | ```--replica-offset-max-lag``` | ```int``` | Integer in range:<br/>[-1, MaxValue] | Throttle ClusterAppendLog when replica.AOFTailAddress - ReplicationOffset > ReplicationOffsetMaxLag. 0: Synchronous replay,  >=1: background replay with specified lag, -1: infinite lag |
| **MainMemoryReplication** | ```--main-memory-replication``` | ```bool``` |  | Use main-memory replication model. |
| **FastAofTruncate** | ```--fast-aof-truncate``` | ```bool``` |  | Use fast-aof-truncate replication model. |
| **OnDemandCheckpoint** | ```--on-demand-checkpoint``` | ```bool``` |  | Used with fast-aof-truncate replication model. Take on demand checkpoint to avoid missing data when attaching |
| **ReplicaDisklessSync** | ```--repl-diskless-sync``` | ```bool``` |  | Whether diskless replication is enabled or not. |
| **ReplicaDisklessSyncDelay** | ```--repl-diskless-sync-delay``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Delay in diskless replication sync in seconds. =0: Immediately start diskless replication sync. |
| **ReplicaAttachTimeout** | ```--repl-attach-timeout``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Timeout in seconds for replication attach operation. |
| **ReplicaSyncTimeout** | ```--repl-sync-timeout``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Timeout in seconds for replication sync operations. |
| **ReplicaDisklessSyncFullSyncAofThreshold** | ```--repl-diskless-sync-full-sync-aof-threshold``` | ```string``` | Memory size | AOF replay size threshold for diskless replication, beyond which we will perform a full sync even if a partial sync is possible. Defaults to AOF memory size if not specified. |
| **UseAofNullDevice** | ```--aof-null-device``` | ```bool``` |  | With fast-aof-truncate replication, use null device for AOF. Ensures no disk IO, but can cause data loss during replication. |
| **ConfigImportPath** | ```--config-import-path``` | ```string``` |  | Import (load) configuration options from the provided path |
| **ConfigImportFormat** | ```--config-import-format``` | ```ConfigFileType``` | GarnetConf, RedisConf | Format of configuration options in path specified by config-import-path |
| **ConfigExportFormat** | ```--config-export-format``` | ```ConfigFileType``` | GarnetConf, RedisConf | Format to export configuration options to path specified by config-export-path |
| **UseAzureStorageForConfigImport** | ```--use-azure-storage-for-config-import``` | ```bool``` |  | Use Azure storage to import config file |
| **ConfigExportPath** | ```--config-export-path``` | ```string``` |  | Export (save) current configuration options to the provided path |
| **UseAzureStorageForConfigExport** | ```--use-azure-storage-for-config-export``` | ```bool``` |  | Use Azure storage to export config file |
| **UseNativeDeviceLinux** | ```--use-native-device-linux``` | ```bool``` |  | DEPRECATED: use DeviceType (--device-type) of Native instead. |
| **DeviceType** | ```--device-type``` | ```DeviceType``` | Default, Native, RandomAccess, FileStream, AzureStorage, LocalMemory, Null | Device type (Default, Native, RandomAccess, FileStream, AzureStorage, LocalMemory, Null) |
| **DeviceIoBackend** | ```--device-io-backend``` | ```IoBackend``` | Default, Libaio, Uring | Linux-only IO backend for DeviceType=Native: Default (=libaio), Libaio, or Uring (io_uring). The shipped native library is built with -DUSE_URING=ON and requires liburing.so.2 at load time for all backends; a -DUSE_URING=OFF rebuild only needs libaio. |
| **DeviceCompletionThreads** | ```--device-completion-threads``` | ```int``` | Integer in range:<br/>[1, 64] | Linux-only: Number of IO completion drain threads for DeviceType=Native (default 4, max 64). Under high concurrent pending-read load a single drainer convoys on the completion-signal path and collapses throughput; 4 removes that on both backends. On io_uring this scales further/more CPU-efficiently; libaio benefits less beyond a few. |
| **DeviceThrottleLimit** | ```--device-throttle-limit``` | ```int``` | Integer in range:<br/>[0, 65536] | Per-device max number of in-flight IOs (IDevice.ThrottleLimit). 0 = use the device's built-in default (120 for the in-box Tsavorite devices). Raising this lets disk-bound workloads keep the queue depth high enough to saturate fast NVMe / io_uring backends. For DeviceType=LocalMemory (which has no device-wide throttle) this instead sets the per-ring in-flight capacity, rounded up to a power of two. |
| **RevivBinRecordSizes** | ```--reviv-bin-record-sizes``` | ```IEnumerable<int>``` |  | #,#,...,#: The sizes of records in each revivification bin, in order of increasing size.           Supersedes the default --reviv; cannot be used with --reviv-in-chain-only |
| **RevivBinRecordCounts** | ```--reviv-bin-record-counts``` | ```IEnumerable<int>``` |  | #,#,...,#: The number of records in each bin:    Default (not specified): If reviv-bin-record-sizes is specified, each bin is 256 records    # (one value): If reviv-bin-record-sizes is specified, then all bins have this number of records, else error    #,#,...,# (multiple values): If reviv-bin-record-sizes is specified, then it must be the same size as that array, else error                                 Supersedes the default --reviv; cannot be used with --reviv-in-chain-only |
| **RevivifiableFraction** | ```--reviv-fraction``` | ```double``` | Double in range:<br/>[0, 1] | #: Fraction of mutable in-memory log space, from the highest log address down to the read-only region, that is eligible for revivification. |
| **EnableRevivification** | ```--reviv``` | ```bool``` |  | A shortcut to specify revivification with default power-of-2-sized bins.    This default can be overridden by --reviv-in-chain-only or by the combination of reviv-bin-record-sizes and reviv-bin-record-counts. |
| **RevivNumberOfBinsToSearch** | ```--reviv-search-next-higher-bins``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Search this number of next-higher bins if the search cannot be satisfied in the best-fitting bin.    Requires --reviv or the combination of rconeviv-bin-record-sizes and reviv-bin-record-counts |
| **RevivBinBestFitScanLimit** | ```--reviv-bin-best-fit-scan-limit``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Number of records to scan for best fit after finding first fit.    Requires --reviv or the combination of reviv-bin-record-sizes and reviv-bin-record-counts    0: Use first fit    #: Limit scan to this many records after first fit, up to the record count of the bin |
| **RevivInChainOnly** | ```--reviv-in-chain-only``` | ```bool``` |  | Revivify tombstoned records in tag chains only (do not use free list).    Cannot be used with reviv-bin-record-sizes or reviv-bin-record-counts. |
| **ObjectScanCountLimit** | ```--object-scan-count-limit``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Limit of items to return in one iteration of *SCAN command |
| **EnableDebugCommand** | ```--enable-debug-command``` | ```ConnectionProtectionOption``` | no, local, yes | Enable DEBUG command for 'no', 'local' or 'all' connections |
| **EnableModuleCommand** | ```--enable-module-command``` | ```ConnectionProtectionOption``` | no, local, yes | Enable MODULE command for 'no', 'local' or 'all' connections. Command can only load from paths listed in ExtensionBinPaths |
| **ProtectedMode** | ```--protected-mode``` | ```CommandLineBooleanOption``` | true, false, yes, no | Enable protected mode. |
| **ExtensionBinPaths** | ```--extension-bin-paths``` | ```IEnumerable<string>``` |  | List of directories on server from which custom command binaries can be loaded by admin users. MODULE command also requires enable-module-command to be set |
| **LoadModuleCS** | ```--loadmodulecs``` | ```IEnumerable<string>``` |  | List of modules to be loaded |
| **ExtensionAllowUnsignedAssemblies** | ```--extension-allow-unsigned``` | ```bool``` |  | Allow loading custom commands from digitally unsigned assemblies (not recommended) |
| **IndexResizeFrequencySecs** | ```--index-resize-freq``` | ```int``` | Integer in range:<br/>[1, MaxValue] | Hash-index resize check frequency in seconds |
| **IndexResizeThreshold** | ```--index-resize-threshold``` | ```int``` | Integer in range:<br/>[1, 100] | Hash-index Overflow bucket count over total index size in percentage to trigger index resize |
| **MaxInlineKeySize** | ```--max-inline-key-size``` | ```string``` | Memory size | Maximum size of a key stored inline in the in-memory portion of the main log. Accepts a memory size (e.g. "1k", "128"). Must be in range [0, 1022] bytes; default is 1022. |
| **MaxInlineValueSize** | ```--max-inline-value-size``` | ```string``` | Memory size | Maximum size of a value stored inline in the in-memory portion of the main log. Accepts a memory size (e.g. "4k", "15m"). Must be in range [0, 16777214] bytes; default is min (1m, PageSize / 2). |
| **InitialIORecordSize** | ```--initial-io-record-size``` | ```string``` | Memory size | Initial IO read size for records on disk. Accepts a memory size (e.g. 4k, 8k). Default is 128 bytes. |
| **FailOnRecoveryError** | ```--fail-on-recovery-error``` | ```bool``` |  | Server bootup should fail if errors happen during bootup of AOF and checkpointing |
| **LuaMemoryManagementMode** | ```--lua-memory-management-mode``` | ```LuaMemoryManagementMode``` | Native, Tracked, Managed | Memory management mode for Lua scripts, must be set to Tracked or Managed to impose script limits |
| **LuaScriptMemoryLimit** | ```--lua-script-memory-limit``` | ```string``` | Memory size | Memory limit for a Lua instances while running a script, lua-memory-management-mode must be set to something other than Native to use this flag |
| **LuaScriptTimeoutMs** | ```--lua-script-timeout``` | ```int``` | Integer in range:<br/>[10, MaxValue] | Timeout for a Lua instance while running a script, specified in positive milliseconds (0 = disabled) |
| **LuaLoggingMode** | ```--lua-logging-mode``` | ```LuaLoggingMode``` | Enable, Silent, Disable | Behavior of redis.log(...) when called from Lua scripts.  Defaults to Enable. |
| **UnixSocketPath** | ```--unixsocket``` | ```string``` |  | Unix socket address path to bind server to |
| **UnixSocketPermission** | ```--unixsocketperm``` | ```int``` | Integer in range:<br/>[0, 777] | Unix socket permissions in octal (Unix platforms only) |
| **MaxDatabases** | ```--max-databases``` | ```int``` | Integer in range:<br/>[1, 256] | Max number of logical databases allowed in a single Garnet server instance |
| **ExpiredKeyDeletionScanFrequencySecs** | ```--expired-key-deletion-scan-freq``` | ```int``` | Integer in range:<br/>[-1, MaxValue] | Frequency of background scan for expired key deletion, in seconds |
| **ClusterReplicationReestablishmentTimeout** | ```--cluster-replication-reestablishment-timeout``` | ```int``` | Integer in range:<br/>[0, MaxValue] |  |
| **ClusterReplicaResumeWithData** | ```--cluster-replica-resume-with-data``` | ```bool``` |  | If a Cluster Replica resumes with data, allow it to be served prior to a Primary being available |
| **EnableVectorSetPreview** | ```--enable-vector-set-preview``` | ```bool``` |  | Enable Vector Sets (preview) - this feature (and associated commands) are incomplete, unstable, and subject to change while still in preview |
| **VectorSetReplayTaskCount** | ```--vector-set-replay-task-count``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Configure how many replay tasks are used to replay VectorSet operations at the replica (default: 0 uses the machine CPU count) |
| **EnableRangeIndexPreview** | ```--enable-range-index-preview``` | ```bool``` |  | Enable Range Index (preview) - this feature (and associated RI.* commands) are incomplete, unstable, and subject to change while still in preview |
| **VectorSetQuantizationTaskCount** | ```--vector-set-quantization-task-count``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Configure how many quantization tasks are used to optimize Vector Set operations (default: 0 uses the machine CPU count; maximum: the machine CPU count) |

[^1]: A string representing a memory size. Can either be a number of bytes, or follow this pattern: 1k, 1kb, 5M, 5Mb, 10g, 10GB etc.
