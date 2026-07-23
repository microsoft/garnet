// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using Garnet.server.Auth.Settings;
using Garnet.server.TLS;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Options when creating Garnet server
    /// </summary>
    public class GarnetServerOptions : ServerOptions
    {
        /// <summary>
        /// Support data structure objects.
        /// </summary>
        public bool DisableObjects = false;

        /// <summary>
        /// Enable cluster.
        /// </summary>
        public bool EnableCluster = false;

        /// <summary>
        /// Start with clean cluster config
        /// </summary>
        public bool CleanClusterConfig = false;

        /// <summary>
        /// Number of parallel migrate tasks to spawn when SLOTS or SLOTSRANGE option is used.
        /// </summary>
        public int ParallelMigrateTaskCount = 1;

        /// <summary>
        /// When migrating slots 1. write directly to network buffer to avoid unecessary copies, 2. do not wait for ack from target before sending next batch of keys.
        /// </summary>
        public bool FastMigrate = false;

        /// <summary>
        /// Authentication settings
        /// </summary>
        public IAuthenticationSettings AuthSettings = null;

        /// <summary>
        /// If true (default), the server refuses to start when an ACL rule references a custom command
        /// name that no loaded module has registered. Set to false to load unresolved names as-is and
        /// log warnings.
        /// </summary>
        public bool AclStrictCustomCommands = true;

        /// <summary>
        /// Enable append-only file (write ahead log)
        /// </summary>
        public bool EnableAOF = false;

        /// <summary>
        /// Enable Lua scripts on server
        /// </summary>
        public bool EnableLua = false;

        /// <summary>
        /// Run Lua scripts as a transaction (lock keys - run script - unlock keys)
        /// </summary>
        public bool LuaTransactionMode = false;

        /// <summary>
        /// Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit.
        /// </summary>
        public string AofMemorySize = "128m";

        /// <summary>
        /// Aof page size in bytes (rounds down to power of 2).
        /// </summary>
        public string AofPageSize = "32m";

        /// <summary>
        /// Size of each AOF segment (file) in bytes on disk (rounds down to power of 2).
        /// This is the granularity at which AOF files are created and truncated.
        /// </summary>
        public string AofSegmentSize = "1g";

        /// <summary>
        /// Number of AOF physical sublogs (i.e. TsavoriteLog instances) used (=1 equivalent to the legacy single log implementation >1: sharded log implementation.
        /// </summary>
        public int AofPhysicalSublogCount = 1;

        /// <summary>
        /// Number of replay tasks per physical sublog at the replica.
        /// </summary>
        public int AofReplayTaskCount = 1;

        /// <summary>
        /// Maximum allowed drift (in key sequence numbers) between the fastest and slowest physical sublog replay drivers.
        /// When a driver is ahead of the slowest peer by more than this value, it yields until the gap closes.
        /// Only effective when AofPhysicalSublogCount > 1. -1 = disabled (no throttling).
        /// </summary>
        public long AofReplayMaxDrift = -1;

        /// <summary>
        /// Polling frequency of the background task responsible for moving time ahead for all physical sublogs (Used only with physical sublog value >1).
        /// </summary>
        public int AofTailWitnessFreqMs = 100;

        /// <summary>
        /// Write ahead logging (append-only file) commit issue frequency in milliseconds.
        /// 0 = issue an immediate commit per operation
        /// -1 = manually issue commits using COMMITAOF command (no auto-commit)
        /// </summary>
        public int CommitFrequencyMs = 0;

        /// <summary>
        /// Frequency of background scan for expired key deletion, in seconds.
        /// </summary>
        public int ExpiredKeyDeletionScanFrequencySecs = -1;

        /// <summary>
        /// Index resize check frequency in seconds.
        /// </summary>
        public int IndexResizeFrequencySecs = 60;

        /// <summary>
        /// Overflow bucket count over total index size in percentage to trigger index resize.
        /// </summary>
        public int IndexResizeThreshold = 50;

        /// <summary>
        /// Maximum size of a key stored inline in the in-memory portion of the main log.
        /// Accepts a memory size (e.g. \"1k\", \"128\"). Must be in range [0, 1022] bytes; default is 1022.
        /// </summary>
        public string MaxInlineKeySize = null;

        /// <summary>
        /// Maximum size of a value stored inline in the in-memory portion of the main log.
        /// Accepts a memory size (e.g. \"4k\", \"15m\"). Must be in range [0, 16777214] bytes; default is min (1m, PageSize / 2).
        /// </summary>
        public string MaxInlineValueSize = null;

        /// <summary>
        /// Initial IO read size for records on disk. Accepts bytes or k/m/g suffixes (e.g. "4k", "8k").
        /// null means use the default (<see cref="IStreamBuffer.DefaultInitialIORecordSize"/> bytes).
        /// </summary>
        public string InitialIORecordSize = null;

        /// <summary>
        /// Wait for AOF to commit before returning results to client.
        /// Warning: will greatly increase operation latency.
        /// </summary>
        public bool WaitForCommit = false;

        /// <summary>
        /// Aof size limit in bytes
        /// </summary>
        public string AofSizeLimit = "";

        /// <summary>
        /// Frequency (in ms) of execution of the AutoCheckpointBasedOnAofSizeLimit background task.
        /// </summary>
        public int AofSizeLimitEnforceFrequencySecs = 5;

        /// <summary>
        /// Hybrid log compaction frequency in seconds. 0 = disabled
        /// </summary>
        public int CompactionFrequencySecs = 0;

        /// <summary>
        /// Frequency in seconds for the background task to perform object collection which removes expired members within object from memory. 0 = disabled. Use the HCOLLECT and ZCOLLECT API to collect on-demand.
        /// </summary>
        public int ExpiredObjectCollectionFrequencySecs = 0;

        /// <summary>
        /// Hybrid log compaction type.
        ///  None - no compaction.
        ///  Shift - shift begin address without compaction (data loss).
        ///  Scan - scan old pages and move live records to tail (no data loss).
        ///  Lookup - lookup each record in compaction range, for record liveness checking using hash chain (no data loss).
        /// </summary>
        public LogCompactionType CompactionType = LogCompactionType.None;

        /// <summary>
        /// Forcefully delete the inactive segments immediately after the compaction strategy (type) is applied.
        /// If false, take a checkpoint to actually delete the older data files from disk.
        /// </summary>
        public bool CompactionForceDelete = false;

        /// <summary>
        /// Number of log segments created on disk before compaction triggers.
        /// </summary>
        public int CompactionMaxSegments = 32;

        /// <summary>
        /// Percent of cluster nodes to gossip with at each gossip iteration.
        /// </summary>
        public int GossipSamplePercent = 100;

        /// <summary>
        /// Cluster mode gossip protocol per node sleep (in seconds) delay to send updated config.
        /// </summary>
        public int GossipDelay = 5;

        /// <summary>
        /// Cluster node timeout is the amount of seconds a node must be unreachable. 
        /// </summary>
        public int ClusterTimeout = 60;

        /// <summary>
        /// How frequently to flush cluster config unto disk to persist updates. =-1: never (memory only), =0: immediately (every update performs flush), >0: frequency in ms
        /// </summary>
        public int ClusterConfigFlushFrequencyMs = 0;

        /// <summary>
        /// TLS options
        /// </summary>
        public IGarnetTlsOptions TlsOptions;

        /// <summary>
        /// Username for clients used by cluster backend
        /// </summary>
        public string ClusterUsername;

        /// <summary>
        /// Password for clients used by cluster backend
        /// </summary>
        public string ClusterPassword;

        /// <summary>
        /// Enable per command latency tracking for all commands
        /// </summary>
        public bool LatencyMonitor = false;

        /// <summary>
        /// Enable per-command usage statistics tracking (calls, failures, rejections).
        /// Exposed via INFO COMMANDSTATS.
        /// </summary>
        public bool CommandStatsMonitor = false;

        /// <summary>
        /// Threshold (microseconds) for logging command in the slow log. 0 to disable
        /// </summary>
        public int SlowLogThreshold = 0;

        /// <summary>
        /// Maximum number of slow log entries to keep
        /// </summary>
        public int SlowLogMaxEntries = 128;

        /// <summary>
        /// Metrics sampling frequency
        /// </summary>
        public int MetricsSamplingFrequency = 0;

        /// <summary>
        /// Logging level. Value options: Trace, Debug, Information, Warning, Error, Critical, None
        /// </summary>
        public LogLevel LogLevel = LogLevel.Error;

        /// <summary>
        /// Frequency (in seconds) of logging (used for tracking progress of long running operations e.g. migration)
        /// </summary>
        public int LoggingFrequency = 5;

        /// <summary>
        /// Metrics sampling frequency
        /// </summary>
        public bool QuietMode = false;

        /// <summary>
        /// SAVE and BGSAVE: We will take a full (index + log) checkpoint when ReadOnlyAddress of log increases by this amount, from the last full checkpoint.
        /// </summary>
        public long FullCheckpointLogInterval = 1L << 30;

        /// <summary>
        /// SAVE and BGSAVE: Use fold-over checkpoints instead of snapshots.
        /// </summary>
        public bool UseFoldOverCheckpoints = false;

        /// <summary>
        /// Minimum worker threads in thread pool (0 for default)
        /// </summary>
        public int ThreadPoolMinThreads = 0;

        /// <summary>
        /// Maximum worker threads in thread pool (0 for default)
        /// </summary>
        public int ThreadPoolMaxThreads = 0;

        /// <summary>
        /// Minimum IO completion threads in thread pool (0 for default)
        /// </summary>
        public int ThreadPoolMinIOCompletionThreads = 0;

        /// <summary>
        /// Maximum IO completion threads in thread pool (0 for default)
        /// </summary>
        public int ThreadPoolMaxIOCompletionThreads = 0;

        /// <summary>
        /// Maximum client connection limit
        /// </summary>
        public int NetworkConnectionLimit = -1;

        /// <summary>
        /// Instance of interface to create named device factories
        /// </summary>
        public INamedDeviceFactoryCreator DeviceFactoryCreator = null;

        /// <summary>
        /// Whether and by how much should we throttle the disk IO for checkpoints (default = 0)
        /// -1   - disable throttling
        /// >= 0 - run checkpoint flush in separate task, sleep for specified time after each WRiteAsync
        /// </summary>
        public int CheckpointThrottleFlushDelayMs = 0;


        /// <summary>
        /// Throttle FastCommit to write metadata once every K commits
        /// </summary>
        public int FastCommitThrottleFreq = 1000;

        /// <summary>
        /// Throttle the maximum outstanding network sends per session
        /// </summary>
        public int NetworkSendThrottleMax = 8;

        /// <summary>
        /// Whether to use scatter-gather IO for a run of contiguous GET operations - useful to saturate
        /// disk random read IO. MGET always uses scatter-gather regardless of this setting.
        /// </summary>
        public bool EnableScatterGatherGet = true;

        /// <summary>
        /// Whether and by how much should we throttle replica sync frequency (default = 5ms)
        /// 0   - disable throttling
        /// </summary>
        public int ReplicaSyncDelayMs = 5;

        /// <summary>
        /// Throttle ClusterAppendLog when replica.AOFTailAddress - ReplicationOffset > ReplicationOffsetMaxLag. 0: Synchronous replay,  >=1: background replay with specified lag, -1: infinite lag
        /// </summary>
        public int ReplicationOffsetMaxLag = -1;

        /// <summary>
        /// Whether we truncate AOF as soon as replicas are fed (not just after checkpoints)
        /// </summary>
        public bool FastAofTruncate = false;

        /// <summary>
        /// Used with main-memory replication model. Take on demand checkpoint to avoid missing data when attaching
        /// </summary>
        public bool OnDemandCheckpoint = true;

        /// <summary>
        /// Whether diskless replication is enabled or not.
        /// </summary>
        public bool ReplicaDisklessSync = false;

        /// <summary>
        /// Delay in diskless replication sync in seconds. =0: Immediately start diskless replication sync.
        /// </summary>
        public int ReplicaDisklessSyncDelay = 5;

        /// <summary>
        /// Timeout in seconds for replication sync operations
        /// </summary>
        public TimeSpan ReplicaSyncTimeout = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Timeout in seconds for replication attach operation
        /// </summary>
        public TimeSpan ReplicaAttachTimeout = TimeSpan.FromSeconds(60);

        /// <summary>
        /// AOF replay size threshold for diskless replication, beyond which we will perform a full sync even if a partial sync is possible. Defaults to AOF memory size if not specified.
        /// </summary>
        public string ReplicaDisklessSyncFullSyncAofThreshold = string.Empty;

        /// <summary>
        /// With main-memory replication, whether we use null device for AOF. Ensures no disk IO, but can cause data loss during replication.
        /// </summary>
        public bool UseAofNullDevice = false;

        /// <summary>
        /// Use specified device type
        /// </summary>
        public DeviceType DeviceType = DeviceType.Default;

        /// <summary>
        /// For DeviceType.Native on Linux: which IO backend to use (libaio or io_uring).
        /// Ignored for other device types or platforms. io_uring requires the native library
        /// to be built with -DUSE_URING=ON.
        /// </summary>
        public NativeStorageDevice.IoBackend DeviceIoBackend = NativeStorageDevice.IoBackend.Default;

        /// <summary>
        /// For DeviceType.Native on Linux: number of background IO completion drain threads.
        /// Defaults to 4: under high concurrent pending-read load (e.g. disk-served vector search)
        /// a single drainer convoys on the per-session completion-signal path and collapses
        /// throughput past moderate concurrency; 4 drainers remove that collapse on both backends.
        /// io_uring scales further/more CPU-efficiently with additional threads, libaio less so.
        /// </summary>
        public int DeviceCompletionThreads = 4;

        /// <summary>
        /// Per-device max number of in-flight IOs (<see cref="IDevice.ThrottleLimit"/>).
        /// 0 means "use the device's built-in default" (120 for the in-box Tsavorite devices).
        /// </summary>
        public int DeviceThrottleLimit = 0;

        /// <summary>
        /// Limit of items to return in one iteration of *SCAN command
        /// </summary>
        public int ObjectScanCountLimit = 1000;

        /// <summary>
        /// Sizes of records in each revivification bin, in order of increasing size. See Options helptext for details.
        /// </summary>
        public int[] RevivBinRecordSizes = null;

        /// <summary>
        /// Number of records in each bin. See Options helptext for details.
        /// </summary>
        public int[] RevivBinRecordCounts = null;

        /// <summary>
        /// How much of the in-memory storage space, from the highest log address down, is eligible for revivification.
        /// It may be important for recent records to remain in mutable memory as long as possible before entering the read-only
        /// memory region or being evicted to disk.
        /// </summary>
        public double RevivifiableFraction;

        /// <summary>
        /// A shortcut to specify revivification with power-of-2-sized bins. See Options helptext for details.
        /// </summary>
        public bool UseRevivBinsPowerOf2;

        /// <summary>
        /// Search this number of next-higher bins if the search cannot be satisfied in the best-fitting bin. See Options helptext for details.
        /// </summary>
        public int RevivNumberOfBinsToSearch;

        /// <summary>
        /// Number of records to scan for best fit after finding first fit. See Options helptext for details.
        /// </summary>
        public int RevivBinBestFitScanLimit;

        /// <summary>
        /// Revivify tombstoned records in tag chains only (do not use free list). See Options helptext for details.
        /// </summary>
        public bool RevivInChainOnly;

        /// <summary>Max size of hash index (cache lines) after rounding down size in bytes to power of 2.</summary>
        public int AdjustedIndexMaxCacheLines;

        /// <summary>Max size of object store hash index (cache lines) after rounding down size in bytes to power of 2.</summary>
        public int AdjustedObjectStoreIndexMaxCacheLines;

        /// <summary>
        /// Enables the DEBUG command
        /// </summary>
        public ConnectionProtectionOption EnableDebugCommand;

        /// <summary>
        /// Enables the MODULE command
        /// </summary>
        public ConnectionProtectionOption EnableModuleCommand;

        /// <summary>
        /// Directories on server from which custom command binaries can be loaded by admin users
        /// </summary>
        public string[] ExtensionBinPaths;

        /// <summary>
        /// Allow loading custom commands from digitally unsigned assemblies
        /// </summary>
        public bool ExtensionAllowUnsignedAssemblies;

        /// <summary>List of modules to load</summary>
        public IEnumerable<string> LoadModuleCS;

        /// <summary>Whether the read cache is enabled</summary>
        public bool EnableReadCache = false;

        /// <summary>
        /// Total readcache-log memory (inline and heap) to use if readcache is enabled, in bytes. Does not need to be a power of 2
        /// </summary>
        public string ReadCacheMemorySize = "1g";

        /// <summary>
        /// Size of each read cache page in bytes (rounds down to power of 2)
        /// </summary>
        public string ReadCachePageSize = "4m";

        /// <summary>
        /// Number of readcache-log pages (rounds down to power of 2). This allows specifying less pages initially than ReadCacheMemorySize divided by ReadCachePageSize.
        /// </summary>
        public int ReadCachePageCount = 0;

        /// <summary>Options for Lua script execution</summary>
        public LuaOptions LuaOptions;

        /// <summary>
        /// Unix socket address path to bind server to
        /// </summary>
        public string UnixSocketPath { get; set; }

        /// <summary>
        /// Unix socket file permissions
        /// </summary>
        public UnixFileMode UnixSocketPermission { get; set; }

        /// <summary>
        /// Max number of logical databases allowed
        /// </summary>
        public int MaxDatabases = 16;

        /// <summary>
        /// Allow more than one logical database in server
        /// </summary>
        public bool AllowMultiDb => !EnableCluster && MaxDatabases > 1;

        /// <summary>
        /// Gets the base directory for storing checkpoints
        /// </summary>
        public string CheckpointBaseDirectory => (CheckpointDir ?? LogDir) ?? string.Empty;

        /// <summary>
        /// Gets the base directory for storing main-store checkpoints
        /// </summary>
        public string StoreCheckpointBaseDirectory => Path.Combine(CheckpointBaseDirectory, "Store");

        /// <summary>
        /// Seconds between attempts to re-establish replication between a Primary and Replica if the replication connection
        /// has faulted.
        /// 
        /// 0 disables.
        /// 
        /// Attempts will only be made if Primary and Replica are exchanging GOSSIP messages.
        /// </summary>
        public int ClusterReplicationReestablishmentTimeout = 0;

        /// <summary>
        /// If true, a Cluster Replica will load any AOF/Checkpoint data from disk when it starts
        /// and NOT dump it's data until a Primary connects.
        /// </summary>
        public bool ClusterReplicaResumeWithData = false;

        /// <summary>
        /// Check if the startup configuration allows the possibility of data loss during replication
        /// NOTE: null device cannot or FastAofTruncate without OnDemandCheckpoint cannot guarantee the integrity of replication
        /// since the AOF is being truncated aggresively.
        /// </summary>
        public bool AllowDataLoss
            => UseAofNullDevice || (FastAofTruncate && !OnDemandCheckpoint);

        /// <summary>
        /// If true, enable Vector Set commands.
        /// 
        /// This is a preview feature, subject to substantial change, and should not be relied upon.
        /// </summary>
        public bool EnableVectorSetPreview = false;

        /// <summary>
        /// Configure how many replay tasks are used to replay VectorSet operations at the replica (default: 0 uses the machine CPU count).
        /// </summary>
        public int VectorSetReplayTaskCount = 0;

        /// <summary>
        /// If true, enable Range Index commands (RI.CREATE, RI.SET, RI.GET, etc.).
        ///
        /// This is a preview feature, subject to substantial change, and should not be relied upon.
        /// </summary>
        public bool EnableRangeIndexPreview = false;

        /// <summary>
        /// Configure how many quantization tasks are used to optimize Vector Set operations (default: 0 uses the machine CPU count).
        /// </summary>
        public int VectorSetQuantizationTaskCount = 0;

        /// <summary>
        /// Get the directory name for database checkpoints
        /// </summary>
        /// <param name="dbId">Database Id</param>
        /// <returns>Directory name</returns>
        public static string GetCheckpointDirectoryName(int dbId) => $"checkpoints{(dbId == 0 ? string.Empty : $"_{dbId}")}";

        /// <summary>
        /// Get the directory for database checkpoints
        /// </summary>
        /// <param name="dbId">Database Id</param>
        /// <returns>Directory</returns>
        public string GetStoreCheckpointDirectory(int dbId) =>
            Path.Combine(StoreCheckpointBaseDirectory, GarnetServerOptions.GetCheckpointDirectoryName(dbId));

        /// <summary>
        /// Gets the base directory for storing AOF commits
        /// </summary>
        public string AppendOnlyFileBaseDirectory => CheckpointDir ?? string.Empty;

        /// <summary>
        /// Get the directory name for database AOF commits
        /// </summary>
        /// <param name="dbId">Database Id</param>
        /// <returns>Directory name</returns>
        public static string GetAppendOnlyFileDirectoryName(int dbId) => $"AOF{(dbId == 0 ? string.Empty : $"_{dbId}")}";

        /// <summary>
        /// Get the directory for database AOF commits
        /// </summary>
        /// <param name="dbId">Database Id</param>
        /// <returns>Directory</returns>
        public string GetAppendOnlyFileDirectory(int dbId) =>
            Path.Combine(AppendOnlyFileBaseDirectory, GetAppendOnlyFileDirectoryName(dbId));

        /// <summary>
        /// Constructor
        /// </summary>
        public GarnetServerOptions(ILogger logger = null) : base(logger)
        {
            this.logger = logger;
        }

        /// <summary>
        /// Initialize Garnet server options
        /// </summary>
        /// <param name="loggerFactory"></param>
        public void Initialize(ILoggerFactory loggerFactory = null)
        {
        }

        /// <summary>
        /// Get main store settings
        /// </summary>
        /// <param name="loggerFactory">Logger factory for debugging and error tracing</param>
        /// <param name="epoch">Epoch instance used by server</param>
        /// <param name="stateMachineDriver">Common state machine driver used by Garnet</param>
        /// <param name="logFactory">Tsavorite Log factory instance</param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public KVSettings GetSettings(ILoggerFactory loggerFactory, LightEpoch epoch, StateMachineDriver stateMachineDriver, out INamedDeviceFactory logFactory)
        {
            if (MutablePercent is < 10 or > 95)
                throw new Exception("MutablePercent must be between 10 and 95");

            var indexCacheLines = IndexSizeCachelines("hash index size", IndexMemorySize);

            var pageSize = 1L << PageSizeBits();
            KVSettings kvSettings = new()
            {
                IndexSize = indexCacheLines * 64L,
                PreallocateLog = false,
                MutableFraction = MutablePercent / 100.0,
                PageSize = pageSize,
                Epoch = epoch,
                StateMachineDriver = stateMachineDriver,
                MaxInlineKeySize = MaxInlineKeySizeBytes(),
                MaxInlineValueSize = MaxInlineValueSizeBytes(pageSize),
                InitialIORecordSize = GetInitialIORecordSizeBytes(),
                loggerFactory = loggerFactory,
                logger = loggerFactory?.CreateLogger("TsavoriteKV [main]")
            };

            if (!string.IsNullOrEmpty(LogMemorySize))
                kvSettings.LogMemorySize = ParseSize(LogMemorySize, out _);

            if (PageCount != 0)
                kvSettings.PageCount = PageCount;

            logger?.LogInformation("[Store] Using page size of {PageSize}", PrettySize(kvSettings.PageSize));
            logger?.LogInformation("[Store] Each page can hold ~{PageSize} key-value pairs of objects", kvSettings.PageSize / 24);

            if (kvSettings.LogMemorySize > 0)
            {
                var pageCount = kvSettings.LogMemorySize / kvSettings.PageSize;
                if (kvSettings.PageCount > pageCount)
                    logger?.LogInformation("[Store] Warning: overriding specified PageCount of {kvSettingsPageCount} with smaller page count calculated from LogMemorySize limit divided by PageSize: {pageCount}", kvSettings.PageCount, pageCount);

                var bufferSize = NextPowerOf2(pageCount);
                logger?.LogInformation("[Store] There are {LogPages} log pages in memory, of which {pageCount} are initially allocated", PrettySize(bufferSize), pageCount);
                logger?.LogInformation("[Store] Log memory size limit of {LogMemorySize} will be enforced", PrettySize(kvSettings.LogMemorySize));
            }
            else
            {
                if (kvSettings.PageCount == 0)
                    throw new TsavoriteException($"Store Log Memory size or PageCount must be specified");
                var bufferSize = (int)NextPowerOf2(kvSettings.PageCount);
                if (kvSettings.PageCount < bufferSize)
                {
                    logger?.LogInformation("[Store] Warning: overriding specified PageCount of {kvSettingsPageCount} with next power of 2 page count {bufferSize}", kvSettings.PageCount, bufferSize);
                    kvSettings.PageCount = bufferSize;
                }
                logger?.LogInformation("[Store] There are {LogPages} log pages in memory, all of which will be used because there is no MemorySize limit", PrettySize(bufferSize));
                logger?.LogInformation("[Store] No log memory size limit will be enforced");
            }

            kvSettings.SegmentSize = 1L << SegmentSizeBits(isObj: false);
            kvSettings.ObjectLogSegmentSize = 1L << SegmentSizeBits(isObj: true);
            logger?.LogInformation("[Store] Using disk segment size of {SegmentSize}", PrettySize(kvSettings.SegmentSize));

            logger?.LogInformation("[Store] Using hash index size of {IndexMemorySize} ({indexCacheLines} cache lines)", PrettySize(kvSettings.IndexSize), PrettySize(indexCacheLines));
            logger?.LogInformation("[Store] Hash index size is optimized for up to ~{distinctKeys} distinct keys", PrettySize(indexCacheLines * 4L));

            AdjustedIndexMaxCacheLines = IndexMaxMemorySize == string.Empty ? 0 : IndexSizeCachelines("hash index max size", IndexMaxMemorySize);
            if (AdjustedIndexMaxCacheLines != 0 && AdjustedIndexMaxCacheLines < indexCacheLines)
                throw new Exception($"Index size {IndexMemorySize} should not be less than index max size {IndexMaxMemorySize}");

            if (AdjustedIndexMaxCacheLines > 0)
            {
                logger?.LogInformation("[Store] Using hash index max size of {MaxSize}, ({CacheLines} cache lines)", PrettySize(AdjustedIndexMaxCacheLines * 64L), PrettySize(AdjustedIndexMaxCacheLines));
                logger?.LogInformation("[Store] Hash index max size is optimized for up to ~{distinctKeys} distinct keys", PrettySize(AdjustedIndexMaxCacheLines * 4L));
            }
            logger?.LogInformation("[Store] Using log mutable percentage of {MutablePercent}%", MutablePercent);

            if (DeviceType == DeviceType.Default)
                DeviceType = Devices.GetDefaultDeviceType();
            DeviceFactoryCreator ??= new LocalStorageNamedDeviceFactoryCreator(
                deviceType: DeviceType,
                ioBackend: DeviceIoBackend,
                numCompletionThreads: DeviceCompletionThreads,
                throttleLimit: DeviceThrottleLimit > 0 ? DeviceThrottleLimit : null,
                logger: logger);
            if (DeviceType == DeviceType.Native && OperatingSystem.IsLinux())
                logger?.LogInformation("Using device type {deviceType} (io-backend={ioBackend}, completion-threads={ct}, throttle-limit={tl})", DeviceType, DeviceIoBackend, DeviceCompletionThreads, DeviceThrottleLimit > 0 ? DeviceThrottleLimit.ToString() : "device-default");
            else
                logger?.LogInformation("Using device type {deviceType} (throttle-limit={tl})", DeviceType, DeviceThrottleLimit > 0 ? DeviceThrottleLimit.ToString() : "device-default");

            if (LatencyMonitor && MetricsSamplingFrequency == 0)
                throw new Exception("LatencyMonitor requires MetricsSamplingFrequency to be set");

            // Read cache related settings
            if (EnableReadCache)
            {
                if (!EnableStorageTier)
                    throw new Exception("Read cache requires storage tiering to be enabled");
                kvSettings.ReadCacheEnabled = true;

                if (ReadCachePageCount != 0)
                    kvSettings.ReadCachePageCount = ReadCachePageCount;

                kvSettings.ReadCachePageSize = 1L << ReadCachePageSizeBits();
                kvSettings.ReadCacheMemorySize = ParseSize(ReadCacheMemorySize, out _);

                if (kvSettings.ReadCacheMemorySize > 0)
                {
                    var pageCount = kvSettings.ReadCacheMemorySize / kvSettings.ReadCachePageSize;
                    if (kvSettings.PageCount > pageCount)
                        logger?.LogInformation("[Store] Warning: Read cache overriding specified PageCount of {kvSettingsReadCachePageCount} with smaller page count calculated from LogMemorySize limit divided by PageSize: {pageCount}", kvSettings.ReadCachePageCount, pageCount);

                    var bufferSize = NextPowerOf2(pageCount);
                    logger?.LogInformation("[Store] Read cache enabled with {LogPages} log pages in memory, of which {pageCount} are initially allocated", PrettySize(bufferSize), pageCount);
                    logger?.LogInformation("[Store] Read cache Log memory size limit of {LogMemorySize} will be enforced", PrettySize(kvSettings.ReadCacheMemorySize));
                }
                else
                {
                    if (kvSettings.ReadCachePageCount == 0)
                        throw new TsavoriteException($"Read Cache Log Memory size or PageCount must be specified");
                    var bufferSize = (int)NextPowerOf2(kvSettings.ReadCachePageCount);
                    if (kvSettings.PageCount < bufferSize)
                    {
                        logger?.LogInformation("[Store] Warning: Read cache overriding specified PageCount of {kvSettingsReadCachePageCount} with next power of 2 page count {bufferSize}", kvSettings.ReadCachePageCount, bufferSize);
                        kvSettings.PageCount = bufferSize;
                    }
                    logger?.LogInformation("[Store] Read cache enabled with {LogPages} log pages in memory, all of which will be used because there is no MemorySize limit", PrettySize(bufferSize));
                    logger?.LogInformation("[Store] No Read cache log memory size limit will be enforced");
                }
            }

            if (EnableStorageTier)
            {
                if (LogDir is null or "")
                    LogDir = Directory.GetCurrentDirectory();
                logFactory = GetInitializedDeviceFactory(LogDir);

                // These must match GetInitializedSegmentFileDevice.GetStoreHLogDevice
                kvSettings.LogDevice = logFactory.Get(new FileDescriptor("Store", "hlog"));
                if (!DisableObjects)
                    kvSettings.ObjectLogDevice = logFactory.Get(new FileDescriptor("Store", "hlog_objs"));
            }
            else
            {
                if (LogDir != null)
                    throw new Exception("LogDir specified without enabling tiered storage (UseStorage)");
                kvSettings.LogDevice = new NullDevice();
                logFactory = null;
            }

            if (CopyReadsToTail)
                kvSettings.ReadCopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog);

            if (RevivInChainOnly)
            {
                logger?.LogInformation("[Store] Using Revivification in-chain only");
                kvSettings.RevivificationSettings = RevivificationSettings.InChainOnly.Clone();
            }
            else if (UseRevivBinsPowerOf2)
            {
                logger?.LogInformation("[Store] Using Revivification with power-of-2 bins");
                kvSettings.RevivificationSettings = RevivificationSettings.PowerOf2Bins.Clone();
                kvSettings.RevivificationSettings.NumberOfBinsToSearch = RevivNumberOfBinsToSearch;
                kvSettings.RevivificationSettings.RevivifiableFraction = RevivifiableFraction;
            }
            else if (RevivBinRecordSizes?.Length > 0)
            {
                logger?.LogInformation("[Store] Using Revivification with custom bins");

                // We use this in the RevivBinRecordCounts and RevivObjBinRecordCount Options help text, so assert it here because we can't use an interpolated string there.
                System.Diagnostics.Debug.Assert(RevivificationBin.DefaultRecordsPerBin == 256);
                kvSettings.RevivificationSettings = new()
                {
                    NumberOfBinsToSearch = RevivNumberOfBinsToSearch,
                    FreeRecordBins = new RevivificationBin[RevivBinRecordSizes.Length],
                    RevivifiableFraction = RevivifiableFraction
                };
                for (var ii = 0; ii < RevivBinRecordSizes.Length; ii++)
                {
                    var recordCount = RevivBinRecordCounts?.Length switch
                    {
                        0 => RevivificationBin.DefaultRecordsPerBin,
                        1 => RevivBinRecordCounts[0],
                        _ => RevivBinRecordCounts[ii]
                    };
                    kvSettings.RevivificationSettings.FreeRecordBins[ii] = new()
                    {
                        RecordSize = RevivBinRecordSizes[ii],
                        NumberOfRecords = recordCount,
                        BestFitScanLimit = RevivBinBestFitScanLimit
                    };
                }
            }
            else
            {
                logger?.LogInformation("[Store] Not using Revivification");
            }

            return kvSettings;
        }

        /// <summary>
        /// Get memory size
        /// </summary>
        /// <returns></returns>
        public int MemorySizeBits(string memorySize)
        {
            var size = ParseSize(memorySize, out _);
            var adjustedSize = NextPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower memory size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get read-cache page size in bits, enforcing the shared <see cref="ServerOptions.MinPageSizeBytes"/> minimum.
        /// </summary>
        internal int ReadCachePageSizeBits() => ValidatedPageSizeBits(ReadCachePageSize, nameof(ReadCachePageSize));

        /// <summary>
        /// Parse <see cref="MaxInlineKeySize"/> as a byte count.
        /// Returns the default value (1022 bytes, the Tsavorite limit) if the value is null or empty.
        /// </summary>
        /// <returns>The byte-length value used for <c>KVSettings.MaxInlineKeySize</c>.</returns>
        /// <exception cref="Exception">Thrown when the value cannot be parsed or is outside the allowed range.</exception>
        public int MaxInlineKeySizeBytes()
        {
            const long MinBytes = 0;                    // TODO: LogSettings.MinMaxInlineSize
            const long MaxBytes = 1022;                 // TODO: LogSettings.MaxInlineKeySizeLimit (= (1 << kKeyLengthBits) - 2)

            if (string.IsNullOrEmpty(MaxInlineKeySize))
                return KVSettings.DefaultMaxInlineKeySize;

            if (!TryParseSize(MaxInlineKeySize, out var sizeInBytes))
                throw new Exception($"Unable to parse {nameof(MaxInlineKeySize)} value '{MaxInlineKeySize}'. Expected a memory size string (e.g. '1k', '128').");

            if (sizeInBytes < MinBytes || sizeInBytes > MaxBytes)
                throw new Exception($"{nameof(MaxInlineKeySize)} value '{MaxInlineKeySize}' ({sizeInBytes} bytes) is outside the allowed range [{MinBytes}, {MaxBytes}] bytes.");

            return (int)sizeInBytes;
        }

        /// <summary>
        /// Parse and validate <see cref="MaxInlineValueSize"/> as a byte count.
        /// Tsavorite requires this to be at most 0xFFFFFE (the RecordDataHeader value-length field's
        /// inline limit; see <c>LogSettings.MaxInlineValueSizeLimit</c> — this value MUST be kept in sync because LogSettings
        /// is internal to Tsavorite.core and not visible from Garnet.server). If this value is not specified, it defaults
        /// to the minimum of 1m or <paramref name="pageSize"/> / 2; otherwise, the value must be &lt;= pageSize / 2.
        /// <returns>The byte-length value used for <c>KVSettings.MaxInlineValueSize</c>.</returns>
        /// <exception cref="Exception">Thrown when the value cannot be parsed or is outside the allowed byte range.</exception>
        /// </summary>
        public int MaxInlineValueSizeBytes(long pageSize)
        {
            const long MinBytes = 0;                    // TODO: LogSettings.MinMaxInlineSize
            const long MaxBytes = 0xFFFFFE;             // TODO: LogSettings.MaxInlineValueSizeLimit (= (1 << kValueLengthBits) - 2)

            if (string.IsNullOrEmpty(MaxInlineValueSize))
                return (int)Math.Min(pageSize / 2, KVSettings.DefaultMaxInlineValueSize);

            if (!TryParseSize(MaxInlineValueSize, out var sizeInBytes))
                throw new Exception($"Unable to parse {nameof(MaxInlineValueSize)} value '{MaxInlineValueSize}'. Expected a memory size string (e.g. '4k', '16m').");

            if (sizeInBytes < MinBytes || sizeInBytes > MaxBytes)
                throw new Exception($"{nameof(MaxInlineValueSize)} value '{MaxInlineValueSize}' ({sizeInBytes} bytes) is outside the allowed range [{MinBytes}, {MaxBytes}] bytes.");

            // This check guarantees at least one record fits on a page, because the minimum page size is 4k, and 2k is larger than
            // the PageHeader plus non-value components of a record (RecordInfo, RecordDataHeader, Key, and possible Optional fields).
            if (sizeInBytes > pageSize / 2)
                throw new Exception($"{nameof(MaxInlineValueSize)} value '{MaxInlineValueSize}' ({sizeInBytes} bytes) is greater than half the page size ({pageSize / 2} bytes for PageSize {pageSize} bytes).");

            return (int)sizeInBytes;
        }

        /// <summary>
        /// Parse <see cref="InitialIORecordSize"/> as a byte count.
        /// Returns <see cref="KVSettings.UseDefaultInitialIORecordSize"/> if the value is null or empty (use default).
        /// </summary>
        /// <returns>The byte value used for <c>KVSettings.InitialIORecordSize</c>, or <see cref="KVSettings.UseDefaultInitialIORecordSize"/> if unset.</returns>
        /// <exception cref="Exception">Thrown when the value cannot be parsed.</exception>
        public int GetInitialIORecordSizeBytes()
        {
            if (string.IsNullOrEmpty(InitialIORecordSize))
                return KVSettings.UseDefaultInitialIORecordSize;

            if (!TryParseSize(InitialIORecordSize, out var sizeInBytes))
                throw new Exception($"Unable to parse {nameof(InitialIORecordSize)} value '{InitialIORecordSize}'. Expected a memory size string (e.g. '4k', '8k').");

            if (sizeInBytes <= 0)
                throw new Exception($"{nameof(InitialIORecordSize)} value '{InitialIORecordSize}' ({sizeInBytes} bytes) must be positive.");

            if (sizeInBytes > 1L << PageSizeBits())
                throw new Exception($"{nameof(InitialIORecordSize)} value '{InitialIORecordSize}' ({sizeInBytes} bytes) exceeds the page size ({1L << PageSizeBits()} bytes).");

            return (int)sizeInBytes;
        }

        /// <summary>
        /// Get AOF settings
        /// </summary>
        /// <param name="dbId">DB ID</param>
        /// <param name="tsavoriteLogSettings">Tsavorite log settings</param>
        public void GetAofSettings(int dbId, out TsavoriteLogSettings[] tsavoriteLogSettings)
        {
            // Validate sizes up-front (invariant across sublogs) so we don't allocate devices
            // or commit managers that would need to be disposed if validation fails.
            var memorySizeBits = AofMemorySizeBits();
            var pageSizeBits = AofPageSizeBits();
            var segmentSizeBits = AofSegmentSizeBits();

            // Tsavorite requires MemorySize >= 2 * PageSize (so at least two pages fit in memory).
            // With power-of-two rounded sizes this means memorySizeBits >= pageSizeBits + 1.
            // Catch this here so we can produce a Garnet-specific error that names the actual
            // AofMemorySize / AofPageSize config options, rather than the generic Tsavorite
            // "MemorySize must be at least twice the page size" message which does not say AOF.
            if (memorySizeBits <= pageSizeBits)
            {
                var effectiveMemory = PrettySize(1L << memorySizeBits);
                var effectivePage = PrettySize(1L << pageSizeBits);
                var minMemory = PrettySize(1L << (pageSizeBits + 1));
                var msg = $"AofMemorySize (effective {effectiveMemory} after rounding down to a power of two) " +
                          $"must be at least twice AofPageSize (effective {effectivePage}). " +
                          $"Increase --aof-memory to at least {minMemory}, or reduce --aof-page-size.";
                logger?.LogError("{msg}", msg);
                throw new Exception(msg);
            }

            if (pageSizeBits > segmentSizeBits)
            {
                var effectivePage = PrettySize(1L << pageSizeBits);
                var effectiveSegment = PrettySize(1L << segmentSizeBits);
                var msg = $"AofPageSize (effective {effectivePage} after rounding down to a power of two) " +
                          $"cannot be larger than AofSegmentSize (effective {effectiveSegment}). " +
                          $"Increase --aof-segment-size to at least {effectivePage}, or reduce --aof-page-size.";
                logger?.LogError("{msg}", msg);
                throw new Exception(msg);
            }

            // AofPageSize must be at least twice the main-log PageSize. An AOF entry mirrors a single
            // main-log write (key + value/input + small overhead); raw-string SETs can be as large as
            // the main-log PageSize (values above MaxInlineValueSize overflow to the heap on the main
            // store but are still written in full to the AOF), and object commands like LPUSH/HSET can
            // push the AOF entry even higher. Throw a clear error here so users do not hit the runtime
            // "Entry does not fit on page" path with a stalled session.
            var mainPageSizeBits = PageSizeBits();
            if (pageSizeBits < mainPageSizeBits + 1)
            {
                var effectiveAofPage = PrettySize(1L << pageSizeBits);
                var effectiveMainPage = PrettySize(1L << mainPageSizeBits);
                var minAofPage = PrettySize(1L << (mainPageSizeBits + 1));
                var msg = $"AofPageSize (effective {effectiveAofPage} after rounding down to a power of two) " +
                          $"must be at least twice the main-log PageSize (effective {effectiveMainPage}). " +
                          $"Increase --aof-page-size to at least {minAofPage} (and --aof-memory to at least {PrettySize(1L << (mainPageSizeBits + 2))}), " +
                          $"or reduce --page.";
                logger?.LogError("{msg}", msg);
                throw new Exception(msg);
            }

            tsavoriteLogSettings = new TsavoriteLogSettings[AofPhysicalSublogCount];
            for (var i = 0; i < AofPhysicalSublogCount; i++)
            {
                tsavoriteLogSettings[i] = new TsavoriteLogSettings
                {
                    MemorySizeBits = memorySizeBits,
                    PageSizeBits = pageSizeBits,
                    SegmentSizeBits = segmentSizeBits,
                    LogDevice = GetAofDevice(dbId, subLogIdx: AofPhysicalSublogCount == 1 ? -1 : i),
                    TryRecoverLatest = false,
                    FastCommitMode = true,
                    AutoCommit = AofAutoCommit && (AofPhysicalSublogCount == 1),
                    MutableFraction = 0.9,
                    Epoch = null
                };

                var aofDir = GetAppendOnlyFileDirectory(dbId);
                // We use Tsavorite's default checkpoint manager for AOF, since cookie is not needed for AOF commits
                tsavoriteLogSettings[i].LogCommitManager = new DeviceLogCommitCheckpointManager(
                    FastAofTruncate ? new NullNamedDeviceFactoryCreator() : DeviceFactoryCreator,
                        new DefaultCheckpointNamingScheme(aofDir, AofPhysicalSublogCount == 1 ? -1 : i),
                        removeOutdated: true,
                        fastCommitThrottleFreq: FastCommitThrottleFreq);
            }
        }

        /// <summary>
        /// Gets a new instance of device factory initialized with the supplied baseName.
        /// </summary>
        /// <param name="baseName"></param>
        /// <returns></returns>
        public INamedDeviceFactory GetInitializedDeviceFactory(string baseName)
        {
            return DeviceFactoryCreator.Create(baseName);
        }

        /// <summary>
        /// Get AOF memory size in bits
        /// </summary>
        /// <returns></returns>
        public int AofMemorySizeBits()
        {
            var size = ParseSize(AofMemorySize, out _);
            var adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower AOF memory size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get AOF Page size in bits
        /// </summary>
        /// <returns></returns>
        public int AofPageSizeBits()
        {
            var size = ParseSize(AofPageSize, out _);
            var adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower AOF page size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get AOF segment size in bits
        /// </summary>
        /// <returns></returns>
        public int AofSegmentSizeBits()
        {
            var size = ParseSize(AofSegmentSize, out _);
            var adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower AOF segment size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get maximum AOF size in bits
        /// </summary>
        /// <returns></returns>
        public int AofSizeLimitSizeBits()
        {
            var size = ParseSize(AofSizeLimit, out _);
            var adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower AOF memory size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get integer value of ReplicaDisklessSyncFullSyncAofThreshold
        /// </summary>
        /// <returns></returns>
        public long ReplicaDisklessSyncFullSyncAofThresholdValue()
            => ParseSize(string.IsNullOrEmpty(ReplicaDisklessSyncFullSyncAofThreshold) ? AofMemorySize : ReplicaDisklessSyncFullSyncAofThreshold, out _);

        /// <summary>
        /// Get device for AOF
        /// </summary>
        /// <returns></returns>
        IDevice GetAofDevice(int dbId, int subLogIdx = -1)
        {
            if (UseAofNullDevice && EnableCluster && !FastAofTruncate)
                throw new Exception("Cannot use null device for AOF when cluster is enabled and you are not using main memory replication");
            if (UseAofNullDevice) return new NullDevice();

            if (subLogIdx == -1)
            {
                return GetInitializedDeviceFactory(AppendOnlyFileBaseDirectory)
                    .Get(new FileDescriptor(GetAppendOnlyFileDirectoryName(dbId), "aof.log"));
            }
            else
            {
                return GetInitializedDeviceFactory(AppendOnlyFileBaseDirectory)
                    .Get(new FileDescriptor(GetAppendOnlyFileDirectoryName(dbId), $"aof.{subLogIdx}.log"));
            }
        }

        /// <summary>
        /// Indicates whether AOF auto-commit is enabled.
        /// </summary>
        public bool AofAutoCommit
            => CommitFrequencyMs == 0;

        /// <summary>
        /// Check if multi-log is enabled
        /// </summary>
        /// <returns></returns>
        public bool MultiLogEnabled
            => AofPhysicalSublogCount > 1 || AofReplayTaskCount > 1;

        /// <summary>
        /// Number of virtual sublogs expected
        /// </summary>
        public int AofVirtualSublogCount
            => AofPhysicalSublogCount * AofReplayTaskCount;
    }
}