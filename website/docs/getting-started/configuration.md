---
id: configuration
sidebar_label: Configuration
title: Garnet Configuration
---

## How to Configure Garnet

The Garnet server (GarnetServer.exe) can be configured using a configuration file (e.g. `garnet.conf` or `redis.conf`), while command line arguments can be used to override any settings specified in the file. /
Any settings not specified in either configuration file or command line arguments are set to default valued specified in the `defaults.conf` file (path to this file can be overridden via the command line arguments).

Garnet currently supports two configuration file formats:
1) The [`garnet.conf`](#garnetconf) file format (default) - a JSON-formatted collection of settings
2) The [`redis.conf`](#redisconf) file format - a collection of settings in the Redis configuration file format:
    ```
    keyword argument1 argument2 argument3 ... argumentN
    ```
    See Redis [docs](https://redis.io/docs/management/config/) for reference./
    **Important:** Not all redis.conf keywords are supported in Garnet. In order to have full configuration settings coverage, use the `garnet.conf` format.

Specifying a configuration file path (and a default file path) can be done via the command line parameters./
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

The default configuration file format for Garnet, which supports the full-range of configurable Garnet settings./
`garnet.conf` is a JSON-formatted file containing a collection of configuration settings. For all the available settings, see the `defaults.conf` file or refer to the complete Garnet settings [list](#configurable-settings).

## redis.conf

Garnet supports the `redis.conf` file format as a configuration file. Note that not all `redis.conf` keywords are supported. Please see the following list of supported `redis.conf` keywords:

| `redis.conf` keyword      | `garnet.conf` keyword | Notes |
| ----------- | ----------- | ----------- |
| `bind`      | `bind`       | Only first address used |
| `port`   | `port`        | |
| `maxmemory`      | `memory`       | |
| `logfile`   | `file-logger`        | |
| `loglevel`   | `logger-level`        | |
| `dir`      | `checkpointdir`       | |
| `requirepass`      | `password`       | |
| `aclfile`   | `acl-file`        | |
| `cluster-enabled`   | `cluster`        | |
| `cluster-node-timeout`   | `cluster-timeout`        | |
| `tls-port`   | `tls`        | Value used to indicate if TLS should be used, port number is otherwise ignored
| `tls-cert-file`   | `cert-file-name`        | Garnet currently supports TLS using a .pfx file and passphrase, while Redis supportes TLS using .crt and .key files. In order to use TLS in Garnet while using redis.conf, convert your certificate to .pfx format (see details in the [security](security.md#using-garnetserver-with-tls) section), then use the .pfx file path as the `tls-cert-file` value. If a passphrase was used when creating the original certificate, specify it in the `tls-key-file-pass` parameter as you would in Redis (or via the `--cert-password` command line argument). When starting the server, use the `--cert-subject-name` command line argument to set the certificate subject name, if applicable. |
| `tls-key-file`   | `cert-password`        | See `tls-cert-file` notes |
| `tls-auth-clients`   | `client-certificate-required`        | See `tls-cert-file` notes |
| `latency-tracking`   | `latency-monitor`        | |
| `io-threads`   | `numthreads`        | |
| `repl-diskless-sync-delay`   | `replica-sync-delay`        | |

## Command line arguments

Any setting in Garnet can be also configured by specifying a command line argument. /
If the setting is also specified in the configuration file, it will be overridden by the value specified in the command line. /
For all available command line settings, run `GarnetServer.exe -h` or `GarnetServer.exe -help`, or refer to the complete Garnet settings [list](#configurable-settings).

## Configurable Settings

| `garnet.conf`<br/>keyword | Command line keyword(s) | Type | Valid Values | Description |
| ----------- | ----------- | ----------- | ----------- | ----------- |
| **Port** | ```--port``` | ```int``` | Integer in range:<br/>[0, 65535] | Port to run server on |
| **Address** | ```--bind``` | ```string``` | IP Address in v4/v6 format | IP address to bind server to (default: any) |
| **MemorySize** | ```-m```<br/>```--memory``` | ```string``` | Memory size | Total log memory used in bytes (rounds down to power of 2) |
| **PageSize** | ```-p```<br/>```--page``` | ```string``` | Memory size | Size of each page in bytes (rounds down to power of 2) |
| **SegmentSize** | ```-s```<br/>```--segment``` | ```string``` | Memory size | Size of each log segment in bytes on disk (rounds down to power of 2) |
| **IndexSize** | ```-i```<br/>```--index``` | ```string``` | Memory size | Size of hash index in bytes (rounds down to power of 2) |
| **IndexMaxSize** | ```--index-max-size``` | ```string``` | Memory size | Max size of hash index in bytes (rounds down to power of 2) |
| **MutablePercent** | ```--mutable-percent``` | ```int``` |  | Percentage of log memory that is kept mutable |
| **ObjectStoreHeapMemorySize** | ```--obj-heap-memory``` | ```string``` | Memory size | Object store heap memory size in bytes |
| **ObjectStoreLogMemorySize** | ```--obj-log-memory``` | ```string``` | Memory size | Object store log memory used in bytes excluding heap memory |
| **ObjectStorePageSize** | ```--obj-page``` | ```string``` | Memory size | Size of each object store page in bytes (rounds down to power of 2) |
| **ObjectStoreSegmentSize** | ```--obj-segment``` | ```string``` | Memory size | Size of each object store log segment in bytes on disk (rounds down to power of 2) |
| **ObjectStoreIndexSize** | ```--obj-index``` | ```string``` | Memory size | Size of object store hash index in bytes (rounds down to power of 2) |
| **ObjectStoreIndexMaxSize** | ```--obj-index-max-size``` | ```string``` | Memory size | Max size of object store hash index in bytes (rounds down to power of 2) |
| **ObjectStoreMutablePercent** | ```--obj-mutable-percent``` | ```int``` |  | Percentage of object store log memory that is kept mutable |
| **EnableStorageTier** | ```--storage-tier``` | ```bool``` |  | Enable tiering of records (hybrid log) to storage, to support a larger-than-memory store. Use --logdir to specify storage directory. |
| **CopyReadsToTail** | ```--copy-reads-to-tail``` | ```bool``` |  | When records are read from the main store's in-memory immutable region or storage device, copy them to the tail of the log. |
| **ObjectStoreCopyReadsToTail** | ```--obj-copy-reads-to-tail``` | ```bool``` |  | When records are read from the object store's in-memory immutable region or storage device, copy them to the tail of the log. |
| **LogDir** | ```-l```<br/>```--logdir``` | ```string``` |  | Storage directory for tiered records (hybrid log), if storage tiering (--storage-tier) is enabled. Uses current directory if unspecified. |
| **CheckpointDir** | ```-c```<br/>```--checkpointdir``` | ```string``` |  | Storage directory for checkpoints. Uses logdir if unspecified. |
| **Recover** | ```-r```<br/>```--recover``` | ```bool``` |  | Recover from latest checkpoint and log, if present. |
| **DisablePubSub** | ```--no-pubsub``` | ```bool``` |  | Disable pub/sub feature on server. |
| **EnableIncrementalSnapshots** | ```--incsnap``` | ```bool``` |  | Enable incremental snapshots. |
| **PubSubPageSize** | ```--pubsub-pagesize``` | ```string``` | Memory size | Page size of log used for pub/sub (rounds down to power of 2) |
| **DisableObjects** | ```--no-obj``` | ```bool``` |  | Disable support for data structure objects. |
| **EnableCluster** | ```--cluster``` | ```bool``` |  | Enable cluster. |
| **CleanClusterConfig** | ```--clean-cluster-config``` | ```bool``` |  | Start with clean cluster config. |
| **AuthenticationMode** | ```--auth``` | ```GarnetAuthenticationMode``` | NoAuth, Password, Aad, ACL | Authentication mode of Garnet. This impacts how AUTH command is processed and how clients are authenticated against Garnet. Value options: NoAuth, Password, Aad, ACL |
| **Password** | ```--password``` | ```string``` |  | Authentication string for password authentication. |
| **ClusterUsername** | ```--cluster-username``` | ```string``` |  | Username to authenticate intra-cluster communication with. |
| **ClusterPassword** | ```--cluster-password``` | ```string``` |  | Password to authenticate intra-cluster communication with. |
| **AclFile** | ```--acl-file``` | ```string``` |  | External ACL user file. |
| **AadAuthority** | ```--aad-authority``` | ```string``` |  | The authority of AAD authentication. |
| **AadAudiences** | ```--aad-audiences``` | ```string``` |  | The audiences of AAD token for AAD authentication. Should be a comma separated string. |
| **AadIssuers** | ```--aad-issuers``` | ```string``` |  | The issuers of AAD token for AAD authentication. Should be a comma separated string. |
| **AuthorizedAadApplicationIds** | ```--aad-authorized-app-ids``` | ```string``` |  | The authorized client app Ids for AAD authentication. Should be a comma separated string. |
| **EnableAOF** | ```--aof``` | ```bool``` |  | Enable write ahead logging (append-only file). |
| **AofMemorySize** | ```--aof-memory``` | ```string``` | Memory size | Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit |
| **AofPageSize** | ```--aof-page-size``` | ```string``` | Memory size | Size of each AOF page in bytes(rounds down to power of 2) |
| **CommitFrequencyMs** | ```--aof-commit-freq``` | ```int``` | Integer in range:<br/>[-1, MaxValue] | Write ahead logging (append-only file) commit issue frequency in milliseconds. 0 = issue an immediate commit per operation, -1 = manually issue commits using COMMITAOF command |
| **WaitForCommit** | ```--aof-commit-wait``` | ```bool``` |  | Wait for AOF to flush the commit before returning results to client. Warning: will greatly increase operation latency. |
| **AofSizeLimit** | ```--aof-size-limit``` | ```string``` | Memory size | Maximum size of AOF (rounds down to power of 2) after which unsafe truncation will be applied. Left empty AOF will grow without bound unless a checkpoint is taken |
| **CompactionFrequencySecs** | ```--compaction-freq``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Background hybrid log compaction frequency in seconds. 0 = disabled (compaction performed before checkpointing instead) |
| **CompactionType** | ```--compaction-type``` | ```LogCompactionType``` | None, Shift, Scan, Lookup | Hybrid log compaction type. Value options: None - No compaction, Shift - shift begin address without compaction (data loss), Scan - scan old pages and move live records to tail (no data loss), Lookup - lookup each record in compaction range, for record liveness checking using hash chain (no data loss) |
| **CompactionForceDelete** | ```--compaction-force-delete``` | ```bool``` |  | Forcefully delete the inactive segments immediately after the compaction strategy (type) is applied. If false, take a checkpoint to actually delete the older data files from disk. |
| **CompactionMaxSegments** | ```--compaction-max-segments``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Number of log segments created on disk before compaction triggers. |
| **ObjectStoreCompactionMaxSegments** | ```--obj-compaction-max-segments``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Number of object store log segments created on disk before compaction triggers. |
| **GossipSamplePercent** | ```--gossip-sp``` | ```int``` |  | Percent of cluster nodes to gossip with at each gossip iteration. |
| **GossipDelay** | ```--gossip-delay``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Cluster mode gossip protocol per node sleep (in seconds) delay to send updated config. |
| **ClusterTimeout** | ```--cluster-timeout``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Cluster node timeout is the amount of seconds a node must be unreachable. |
| **ClusterTlsClientTargetHost** | ```--cluster-tls-client-target-host``` | ```string``` |  | Name for the client target host when using TLS connections in cluster mode. |
| **EnableTLS** | ```--tls``` | ```bool``` |  | Enable TLS. |
| **CertFileName** | ```--cert-file-name``` | ```string``` |  | TLS certificate file name (example: testcert.pfx). |
| **CertPassword** | ```--cert-password``` | ```string``` |  | TLS certificate password (example: placeholder). |
| **CertSubjectName** | ```--cert-subject-name``` | ```string``` |  | TLS certificate subject name. |
| **CertificateRefreshFrequency** | ```--cert-refresh-freq``` | ```int``` | Integer in range:<br/>[0, MaxValue] | TLS certificate refresh frequency in seconds (0 to disable). |
| **ClientCertificateRequired** | ```--client-certificate-required``` | ```bool``` |  | Whether TLS client certificate required. |
| **CertificateRevocationCheckMode** | ```--certificate-revocation-check-mode``` | ```X509RevocationMode``` | NoCheck, Online, Offline | Certificate revocation check mode for certificate validation (NoCheck, Online, Offline). |
| **IssuerCertificatePath** | ```--issuer-certificate-path``` | ```string``` |  | Full path of file with issuer certificate for validation. If empty or null, validation against issuer will not be performed. |
| **LatencyMonitor** | ```--latency-monitor``` | ```bool``` |  | Track latency of various events. |
| **MetricsSamplingFrequency** | ```--metrics-sampling-freq``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Metrics sampling frequency in seconds. Value of 0 disables metrics monitor task. |
| **QuietMode** | ```-q```<br/>```--``` | ```bool``` |  | Enabling quiet mode does not print server version and text art. |
| **LogLevel** | ```--logger-level``` | ```LogLevel``` | Trace, Debug, Information, Warning, Error, Critical, None | Logging level. Value options: Trace, Debug, Information, Warning, Error, Critical, None |
| **DisableConsoleLogger** | ```--disable-console-logger``` | ```bool``` |  | Disable console logger. |
| **FileLogger** | ```--file-logger``` | ```string``` |  | Enable file logger and write to the specified path. |
| **ThreadPoolMinThreads** | ```--minthreads``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Minimum worker and completion threads in thread pool, 0 uses the system default. |
| **ThreadPoolMaxThreads** | ```--maxthreads``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Maximum worker and completion threads in thread pool, 0 uses the system default. |
| **UseAzureStorage** | ```--use-azure-storage``` | ```bool``` |  | Use Azure Page Blobs for storage instead of local storage. |
| **AzureStorageConnectionString** | ```--storage-string``` | ```string``` |  | The connection string to use when establishing connection to Azure Blobs Storage. |
| **CheckpointThrottleFlushDelayMs** | ```--checkpoint-throttle-delay``` | ```int``` | Integer in range:<br/>[-1, MaxValue] | Whether and by how much should we throttle the disk IO for checkpoints: -1 - disable throttling; >= 0 - run checkpoint flush in separate task, sleep for specified time after each WriteAsync |
| **EnableFastCommit** | ```--fast-commit``` | ```bool``` |  | Use FastCommit when writing AOF. |
| **FastCommitThrottleFreq** | ```--fast-commit-throttle``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Throttle FastCommit to write metadata once every K commits. |
| **NetworkSendThrottleMax** | ```--network-send-throttle``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Throttle the maximum outstanding network sends per session. |
| **EnableScatterGatherGet** | ```--sg-get``` | ```bool``` |  | Whether we use scatter gather IO for MGET or a batch of contiguous GET operations - useful to saturate disk random read IO. |
| **ReplicaSyncDelayMs** | ```--replica-sync-delay``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Whether and by how much (milliseconds) should we throttle the replica sync: 0 - disable throttling |
| **MainMemoryReplication** | ```--main-memory-replication``` | ```bool``` |  | Use main-memory replication model. |
| **OnDemandCheckpoint** | ```--on-demand-checkpoint``` | ```bool``` |  | Used with main-memory replication model. Take on demand checkpoint to avoid missing data when attaching |
| **UseAofNullDevice** | ```--aof-null-device``` | ```bool``` |  | With main-memory replication, use null device for AOF. Ensures no disk IO, but can cause data loss during replication. |
| **ConfigImportPath** | ```--config-import-path``` | ```string``` |  | Import (load) configuration options from the provided path |
| **ConfigImportFormat** | ```--config-import-format``` | ```ConfigFileType``` | GarnetConf, RedisConf | Format of configuration options in path specified by config-import-path |
| **ConfigExportFormat** | ```--config-export-format``` | ```ConfigFileType``` | GarnetConf, RedisConf | Format to export configuration options to path specified by config-export-path |
| **UseAzureStorageForConfigImport** | ```--use-azure-storage-for-config-import``` | ```bool``` |  | Use Azure storage to import config file |
| **ConfigExportPath** | ```--config-export-path``` | ```string``` |  | Export (save) current configuration options to the provided path |
| **UseAzureStorageForConfigExport** | ```--use-azure-storage-for-config-export``` | ```bool``` |  | Use Azure storage to export config file |
| **UseNativeDeviceLinux** | ```--use-native-device-linux``` | ```bool``` |  | Use native device on Linux for local storage |
| **RevivBinRecordSizes** | ```--reviv-bin-record-sizes``` | ```int[]``` |  | #,#,...,#: For the main store, the sizes of records in each revivification bin, in order of increasing size.           Supersedes the default --reviv; cannot be used with --reviv-in-chain-only |
| **RevivBinRecordCounts** | ```--reviv-bin-record-counts``` | ```int[]``` |  | #,#,...,#: For the main store, the number of records in each bin:    Default (not specified): If reviv-bin-record-sizes is specified, each bin is 256 records    # (one value): If reviv-bin-record-sizes is specified, then all bins have this number of records, else error    #,#,...,# (multiple values): If reviv-bin-record-sizes is specified, then it must be the same size as that array, else error                                 Supersedes the default --reviv; cannot be used with --reviv-in-chain-only |
| **RevivifiableFraction** | ```--reviv-fraction``` | ```double``` | Double in range:<br/>[0, 1] | #: Fraction of mutable in-memory log space, from the highest log address down to the read-only region, that is eligible for revivification.        Applies to both main and object store. |
| **EnableRevivification** | ```--reviv``` | ```bool``` |  | A shortcut to specify revivification with default power-of-2-sized bins.    This default can be overridden by --reviv-in-chain-only or by the combination of reviv-bin-record-sizes and reviv-bin-record-counts. |
| **RevivNumberOfBinsToSearch** | ```--reviv-search-next-higher-bins``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Search this number of next-higher bins if the search cannot be satisfied in the best-fitting bin.    Requires --reviv or the combination of rconeviv-bin-record-sizes and reviv-bin-record-counts |
| **RevivBinBestFitScanLimit** | ```--reviv-bin-best-fit-scan-limit``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Number of records to scan for best fit after finding first fit.    Requires --reviv or the combination of reviv-bin-record-sizes and reviv-bin-record-counts    0: Use first fit    #: Limit scan to this many records after first fit, up to the record count of the bin |
| **RevivInChainOnly** | ```--reviv-in-chain-only``` | ```bool``` |  | Revivify tombstoned records in tag chains only (do not use free list).    Cannot be used with reviv-bin-record-sizes or reviv-bin-record-counts. Propagates to object store by default. |
| **RevivObjBinRecordCount** | ```--reviv-obj-bin-record-count``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Number of records in the single free record bin for the object store. The Object store has only a single bin, unlike the main store.        Ignored unless the main store is using the free record list. |
| **ObjectScanCountLimit** | ```--object-scan-count-limit``` | ```int``` | Integer in range:<br/>[0, MaxValue] | Limit of items to return in one iteration of *SCAN command |
| **ExtensionBinPaths** | ```--extension-bin-paths``` | ```string[]``` |  | List of directories on server from which custom command binaries can be loaded by admin users |
| **ExtensionAllowUnsignedAssemblies** | ```--extension-allow-unsigned``` | ```bool``` |  | Allow loading custom commands from digitally unsigned assemblies (not recommended) |
| **IndexResizeFrequencySecs** | ```--index-resize-freq``` | ```int``` | Integer in range:<br/>[1, MaxValue] | Index resize check frequency in seconds |
| **IndexResizeThreshold** | ```--index-resize-threshold``` | ```int``` | Integer in range:<br/>[1, 100] | Overflow bucket count over total index size in percentage to trigger index resize |


[^1]: A string representing a memory size. Can either be a number of bytes, or follow this pattern: 1k, 1kb, 5M, 5Mb, 10g, 10GB etc.
