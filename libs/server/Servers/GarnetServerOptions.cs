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
        /// Heap memory size limit of object store.
        /// </summary>
        public string ObjectStoreHeapMemorySize = "";

        /// <summary>
        /// Object store log memory used in bytes excluding heap memory.
        /// </summary>
        public string ObjectStoreLogMemorySize = "32m";

        /// <summary>
        /// Size of each object store page in bytes (rounds down to power of 2).
        /// </summary>
        public string ObjectStorePageSize = "4k";

        /// <summary>
        /// Size of each object store log segment in bytes on disk (rounds down to power of 2).
        /// </summary>
        public string ObjectStoreSegmentSize = "32m";

        /// <summary>
        /// Size of object store hash index in bytes (rounds down to power of 2).
        /// </summary>
        public string ObjectStoreIndexSize = "16m";

        /// <summary>
        /// Max size of object store hash index in bytes (rounds down to power of 2). 
        /// If unspecified, index size doesn't grow (default behavior).
        /// </summary>
        public string ObjectStoreIndexMaxSize = string.Empty;

        /// <summary>
        /// Percentage of object store log memory that is kept mutable.
        /// </summary>
        public int ObjectStoreMutablePercent = 90;

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
        /// Enable append-only file (write ahead log)
        /// </summary>
        public bool EnableAOF = false;

        // Enable Lua scripts on server
        public bool EnableLua = false;

        // Run Lua scripts as a transaction (lock keys - run script - unlock keys)
        public bool LuaTransactionMode = false;

        /// <summary>
        /// Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit.
        /// </summary>
        public string AofMemorySize = "64m";

        /// <summary>
        /// Aof page size in bytes (rounds down to power of 2).
        /// </summary>
        public string AofPageSize = "4m";

        /// <summary>
        /// AOF replication (safe tail address) refresh frequency in milliseconds. 0 = auto refresh after every enqueue.
        /// </summary>
        public int AofReplicationRefreshFrequencyMs = 10;

        /// <summary>
        /// Subscriber (safe tail address) refresh frequency in milliseconds (for pub-sub). 0 = auto refresh after every enqueue.
        /// </summary>
        public int SubscriberRefreshFrequencyMs = 0;

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
        /// Number of object store log segments created on disk before compaction triggers.
        /// </summary>
        public int ObjectStoreCompactionMaxSegments = 32;

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
        public int LoggingFrequency = TimeSpan.FromSeconds(5).Seconds;

        /// <summary>
        /// Metrics sampling frequency
        /// </summary>
        public bool QuietMode = false;

        /// <summary>
        /// SAVE and BGSAVE: Enable incremental snapshots, try to write only changes compared to base snapshot
        /// </summary>
        public bool EnableIncrementalSnapshots = false;

        /// <summary>
        /// SAVE and BGSAVE: We will take a full (index + log) checkpoint when ReadOnlyAddress of log increases by this amount, from the last full checkpoint.
        /// </summary>
        public long FullCheckpointLogInterval = 1L << 30;

        /// <summary>
        /// SAVE and BGSAVE: Limit on size of delta log for incremental snapshot, we perform a non-incremental checkpoint after this limit is reached.
        /// </summary>
        public long IncrementalSnapshotLogSizeLimit = 1L << 30;

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
        /// Enable FastCommit mode for TsavoriteLog
        /// </summary>
        public bool EnableFastCommit = true;

        /// <summary>
        /// Throttle FastCommit to write metadata once every K commits
        /// </summary>
        public int FastCommitThrottleFreq = 1000;

        /// <summary>
        /// Throttle the maximum outstanding network sends per session
        /// </summary>
        public int NetworkSendThrottleMax = 8;

        /// <summary>
        /// Whether we use scatter gather IO for MGET operations - useful to saturate disk random read IO
        /// </summary>
        public bool EnableScatterGatherGet = false;

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

        // <summary>
        // Use specified device type
        // </summary>
        public DeviceType DeviceType = DeviceType.Default;

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

        /// <summary>
        /// Number of records in the single free record bin for the object store.
        /// </summary>
        public int RevivObjBinRecordCount;

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

        public bool EnableReadCache = false;

        public string ReadCacheMemorySize = "16g";

        public string ReadCachePageSize = "32m";

        public string ObjectStoreReadCachePageSize = "1m";

        public string ObjectStoreReadCacheLogMemorySize = "32m";

        public string ObjectStoreReadCacheHeapMemorySize = "";

        public bool EnableObjectStoreReadCache = false;

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
        public string MainStoreCheckpointBaseDirectory => Path.Combine(CheckpointBaseDirectory, "Store");

        /// <summary>
        /// Gets the base directory for storing object-store checkpoints
        /// </summary>
        public string ObjectStoreCheckpointBaseDirectory => Path.Combine(CheckpointBaseDirectory, "ObjectStore");

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
        /// Get the directory name for database checkpoints
        /// </summary>
        /// <param name="dbId">Database Id</param>
        /// <returns>Directory name</returns>
        public string GetCheckpointDirectoryName(int dbId) => $"checkpoints{(dbId == 0 ? string.Empty : $"_{dbId}")}";

        /// <summary>
        /// Get the directory for main-store database checkpoints
        /// </summary>
        /// <param name="dbId">Database Id</param>
        /// <returns>Directory</returns>
        public string GetMainStoreCheckpointDirectory(int dbId) =>
            Path.Combine(MainStoreCheckpointBaseDirectory, GetCheckpointDirectoryName(dbId));

        /// <summary>
        /// Get the directory for object-store database checkpoints
        /// </summary>
        /// <param name="dbId">Database Id</param>
        /// <returns>Directory</returns>
        public string GetObjectStoreCheckpointDirectory(int dbId) =>
            Path.Combine(ObjectStoreCheckpointBaseDirectory, GetCheckpointDirectoryName(dbId));

        /// <summary>
        /// Gets the base directory for storing AOF commits
        /// </summary>
        public string AppendOnlyFileBaseDirectory => CheckpointDir ?? string.Empty;

        /// <summary>
        /// Get the directory name for database AOF commits
        /// </summary>
        /// <param name="dbId">Database Id</param>
        /// <returns>Directory name</returns>
        public string GetAppendOnlyFileDirectoryName(int dbId) => $"AOF{(dbId == 0 ? string.Empty : $"_{dbId}")}";

        /// <summary>
        /// Get the directory for database AOF commits
        /// </summary>
        /// <param name="dbId">Database Id</param>
        /// <returns>Directory</returns>
        public string GetAppendOnlyFileDirectory(int dbId) =>
            Path.Combine(AppendOnlyFileBaseDirectory, GetAppendOnlyFileDirectoryName(dbId));

        // Enable STREAMS on server
        public bool EnableStreams = false;

        /// <summary>
        /// Page size for BTree index for STREAM
        /// </summary>
        public string StreamPageSize = "4m";

        /// <summary>
        /// Memory for STREAM
        /// </summary>
        public string StreamMemorySize = "1g";

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
        public KVSettings<SpanByte, SpanByte> GetSettings(ILoggerFactory loggerFactory, LightEpoch epoch, StateMachineDriver stateMachineDriver,
            out INamedDeviceFactory logFactory)
        {
            if (MutablePercent is < 10 or > 95)
                throw new Exception("MutablePercent must be between 10 and 95");

            var indexCacheLines = IndexSizeCachelines("hash index size", IndexSize);

            KVSettings<SpanByte, SpanByte> kvSettings = new()
            {
                IndexSize = indexCacheLines * 64L,
                PreallocateLog = false,
                MutableFraction = MutablePercent / 100.0,
                PageSize = 1L << PageSizeBits(),
                Epoch = epoch,
                StateMachineDriver = stateMachineDriver,
                loggerFactory = loggerFactory,
                logger = loggerFactory?.CreateLogger("TsavoriteKV [main]")
            };

            logger?.LogInformation("[Store] Using page size of {PageSize}", PrettySize(kvSettings.PageSize));

            kvSettings.MemorySize = 1L << MemorySizeBits(MemorySize, PageSize, out var storeEmptyPageCount);
            kvSettings.MinEmptyPageCount = storeEmptyPageCount;

            long effectiveSize = kvSettings.MemorySize - storeEmptyPageCount * kvSettings.PageSize;
            if (storeEmptyPageCount == 0)
                logger?.LogInformation("[Store] Using log memory size of {MemorySize}", PrettySize(kvSettings.MemorySize));
            else
                logger?.LogInformation("[Store] Using log memory size of {MemorySize}, with {storeEmptyPageCount} empty pages, for effective size of {effectiveSize}",
                    PrettySize(kvSettings.MemorySize), storeEmptyPageCount, PrettySize(effectiveSize));

            logger?.LogInformation("[Store] There are {LogPages} log pages in memory", PrettySize(kvSettings.MemorySize / kvSettings.PageSize));

            kvSettings.SegmentSize = 1L << SegmentSizeBits();
            logger?.LogInformation("[Store] Using disk segment size of {SegmentSize}", PrettySize(kvSettings.SegmentSize));

            logger?.LogInformation("[Store] Using hash index size of {IndexSize} ({indexCacheLines} cache lines)", PrettySize(kvSettings.IndexSize), PrettySize(indexCacheLines));
            logger?.LogInformation("[Store] Hash index size is optimized for up to ~{distinctKeys} distinct keys", PrettySize(indexCacheLines * 4L));

            AdjustedIndexMaxCacheLines = IndexMaxSize == string.Empty ? 0 : IndexSizeCachelines("hash index max size", IndexMaxSize);
            if (AdjustedIndexMaxCacheLines != 0 && AdjustedIndexMaxCacheLines < indexCacheLines)
                throw new Exception($"Index size {IndexSize} should not be less than index max size {IndexMaxSize}");

            if (AdjustedIndexMaxCacheLines > 0)
            {
                logger?.LogInformation("[Store] Using hash index max size of {MaxSize}, ({CacheLines} cache lines)", PrettySize(AdjustedIndexMaxCacheLines * 64L), PrettySize(AdjustedIndexMaxCacheLines));
                logger?.LogInformation("[Store] Hash index max size is optimized for up to ~{distinctKeys} distinct keys", PrettySize(AdjustedIndexMaxCacheLines * 4L));
            }
            logger?.LogInformation("[Store] Using log mutable percentage of {MutablePercent}%", MutablePercent);

            if (DeviceType == DeviceType.Default)
            {
                DeviceType = Devices.GetDefaultDeviceType();
            }
            DeviceFactoryCreator ??= new LocalStorageNamedDeviceFactoryCreator(deviceType: DeviceType, logger: logger);

            logger?.LogInformation("Using device type {deviceType}", DeviceType);

            if (LatencyMonitor && MetricsSamplingFrequency == 0)
                throw new Exception("LatencyMonitor requires MetricsSamplingFrequency to be set");

            // Read cache related settings
            if (EnableReadCache && !EnableStorageTier)
            {
                throw new Exception("Read cache requires storage tiering to be enabled");
            }

            if (EnableReadCache)
            {
                kvSettings.ReadCacheEnabled = true;
                kvSettings.ReadCachePageSize = ParseSize(ReadCachePageSize, out _);
                kvSettings.ReadCacheMemorySize = ParseSize(ReadCacheMemorySize, out _);
                logger?.LogInformation("[Store] Read cache enabled with page size of {ReadCachePageSize} and memory size of {ReadCacheMemorySize}",
                    PrettySize(kvSettings.ReadCachePageSize), PrettySize(kvSettings.ReadCacheMemorySize));
            }

            if (EnableStorageTier)
            {
                if (LogDir is null or "")
                    LogDir = Directory.GetCurrentDirectory();
                logFactory = GetInitializedDeviceFactory(LogDir);
                kvSettings.LogDevice = logFactory.Get(new FileDescriptor("Store", "hlog"));
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
                for (var ii = 0; ii < RevivBinRecordSizes.Length; ++ii)
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
        /// Get stream page size
        /// </summary>
        /// <returns></returns>
        public long StreamPageSizeBytes()
        {
            long size = ParseSize(StreamPageSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower stream page size than specified (power of 2)");
            return adjustedSize;
        }

        /// <summary>
        /// Get stream memory size
        /// </summary>
        /// <returns></returns>
        public long StreamMemorySizeBytes()
        {
            long size = ParseSize(StreamMemorySize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower stream page size than specified (power of 2)");
            return adjustedSize;
        }

        /// <summary>
        /// Get memory size
        /// </summary>
        /// <returns></returns>
        public static int MemorySizeBits(string memorySize, string storePageSize, out int emptyPageCount)
        {
            emptyPageCount = 0;
            long size = ParseSize(memorySize, out _);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
            {
                adjustedSize *= 2;
                long pageSize = ParseSize(storePageSize, out _);
                pageSize = PreviousPowerOf2(pageSize);
                emptyPageCount = (int)((adjustedSize - size) / pageSize);
            }
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get KVSettings for the object store log
        /// </summary>
        public KVSettings<byte[], IGarnetObject> GetObjectStoreSettings(ILoggerFactory loggerFactory, LightEpoch epoch, StateMachineDriver stateMachineDriver,
            out long objHeapMemorySize, out long objReadCacheHeapMemorySize)
        {
            objReadCacheHeapMemorySize = default;

            if (ObjectStoreMutablePercent is < 10 or > 95)
                throw new Exception("ObjectStoreMutablePercent must be between 10 and 95");

            var indexCacheLines = IndexSizeCachelines("object store hash index size", ObjectStoreIndexSize);
            KVSettings<byte[], IGarnetObject> kvSettings = new()
            {
                IndexSize = indexCacheLines * 64L,
                PreallocateLog = false,
                MutableFraction = ObjectStoreMutablePercent / 100.0,
                PageSize = 1L << ObjectStorePageSizeBits(),
                Epoch = epoch,
                StateMachineDriver = stateMachineDriver,
                loggerFactory = loggerFactory,
                logger = loggerFactory?.CreateLogger("TsavoriteKV  [obj]")
            };

            logger?.LogInformation("[Object Store] Using page size of {PageSize}", PrettySize(kvSettings.PageSize));
            logger?.LogInformation("[Object Store] Each page can hold ~{PageSize} key-value pairs of objects", kvSettings.PageSize / 24);

            kvSettings.MemorySize = 1L << MemorySizeBits(ObjectStoreLogMemorySize, ObjectStorePageSize, out var objectStoreEmptyPageCount);
            kvSettings.MinEmptyPageCount = objectStoreEmptyPageCount;

            long effectiveSize = kvSettings.MemorySize - objectStoreEmptyPageCount * kvSettings.PageSize;
            if (objectStoreEmptyPageCount == 0)
                logger?.LogInformation("[Object Store] Using log memory size of {MemorySize}", PrettySize(kvSettings.MemorySize));
            else
                logger?.LogInformation("[Object Store] Using log memory size of {MemorySize}, with {objectStoreEmptyPageCount} empty pages, for effective size of {effectiveSize}", PrettySize(kvSettings.MemorySize), objectStoreEmptyPageCount, PrettySize(effectiveSize));

            logger?.LogInformation("[Object Store] This can hold ~{PageSize} key-value pairs of objects in memory total", effectiveSize / 24);

            logger?.LogInformation("[Object Store] There are {LogPages} log pages in memory", PrettySize(kvSettings.MemorySize / kvSettings.PageSize));

            kvSettings.SegmentSize = 1L << ObjectStoreSegmentSizeBits();
            logger?.LogInformation("[Object Store] Using disk segment size of {SegmentSize}", PrettySize(kvSettings.SegmentSize));

            logger?.LogInformation("[Object Store] Using hash index size of {IndexSize} ({indexCacheLines} cache lines)", PrettySize(kvSettings.IndexSize), PrettySize(indexCacheLines));
            logger?.LogInformation("[Object Store] Hash index size is optimized for up to ~{distinctKeys} distinct keys", PrettySize(indexCacheLines * 4L));

            AdjustedObjectStoreIndexMaxCacheLines = ObjectStoreIndexMaxSize == string.Empty ? 0 : IndexSizeCachelines("hash index max size", ObjectStoreIndexMaxSize);
            if (AdjustedObjectStoreIndexMaxCacheLines != 0 && AdjustedObjectStoreIndexMaxCacheLines < indexCacheLines)
                throw new Exception($"Index size {IndexSize} should not be less than index max size {IndexMaxSize}");

            if (AdjustedObjectStoreIndexMaxCacheLines > 0)
            {
                logger?.LogInformation("[Object Store] Using hash index max size of {MaxSize}, ({CacheLines} cache lines)", PrettySize(AdjustedObjectStoreIndexMaxCacheLines * 64L), PrettySize(AdjustedObjectStoreIndexMaxCacheLines));
                logger?.LogInformation("[Object Store] Hash index max size is optimized for up to ~{distinctKeys} distinct keys", PrettySize(AdjustedObjectStoreIndexMaxCacheLines * 4L));
            }
            logger?.LogInformation("[Object Store] Using log mutable percentage of {ObjectStoreMutablePercent}%", ObjectStoreMutablePercent);

            objHeapMemorySize = ParseSize(ObjectStoreHeapMemorySize, out _);
            logger?.LogInformation("[Object Store] Heap memory size is {objHeapMemorySize}", objHeapMemorySize > 0 ? PrettySize(objHeapMemorySize) : "unlimited");

            // Read cache related settings
            if (EnableObjectStoreReadCache && !EnableStorageTier)
            {
                throw new Exception("Read cache requires storage tiering to be enabled");
            }

            if (EnableObjectStoreReadCache)
            {
                kvSettings.ReadCacheEnabled = true;
                kvSettings.ReadCachePageSize = ParseSize(ObjectStoreReadCachePageSize, out _);
                kvSettings.ReadCacheMemorySize = ParseSize(ObjectStoreReadCacheLogMemorySize, out _);
                logger?.LogInformation("[Object Store] Read cache enabled with page size of {ReadCachePageSize} and memory size of {ReadCacheMemorySize}",
                    PrettySize(kvSettings.ReadCachePageSize), PrettySize(kvSettings.ReadCacheMemorySize));

                objReadCacheHeapMemorySize = ParseSize(ObjectStoreReadCacheHeapMemorySize, out _);
                logger?.LogInformation("[Object Store] Read cache heap memory size is {objReadCacheHeapMemorySize}", objReadCacheHeapMemorySize > 0 ? PrettySize(objReadCacheHeapMemorySize) : "unlimited");
            }

            if (EnableStorageTier)
            {
                if (LogDir is null or "")
                    LogDir = Directory.GetCurrentDirectory();
                kvSettings.LogDevice = GetInitializedDeviceFactory(LogDir).Get(new FileDescriptor("ObjectStore", "hlog"));
                kvSettings.ObjectLogDevice = GetInitializedDeviceFactory(LogDir).Get(new FileDescriptor("ObjectStore", "hlog.obj"));
            }
            else
            {
                if (LogDir != null)
                    throw new Exception("LogDir specified without enabling tiered storage (UseStorage)");
                kvSettings.LogDevice = kvSettings.ObjectLogDevice = new NullDevice();
            }

            if (ObjectStoreCopyReadsToTail)
                kvSettings.ReadCopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog);

            if (RevivInChainOnly)
            {
                logger?.LogInformation("[Object Store] Using Revivification in-chain only");
                kvSettings.RevivificationSettings = RevivificationSettings.InChainOnly.Clone();
            }
            else if (UseRevivBinsPowerOf2 || RevivBinRecordSizes?.Length > 0)
            {
                logger?.LogInformation("[Object Store] Using Revivification with a single fixed-size bin");
                kvSettings.RevivificationSettings = RevivificationSettings.DefaultFixedLength.Clone();
                kvSettings.RevivificationSettings.RevivifiableFraction = RevivifiableFraction;
                kvSettings.RevivificationSettings.FreeRecordBins[0].NumberOfRecords = RevivObjBinRecordCount;
                kvSettings.RevivificationSettings.FreeRecordBins[0].BestFitScanLimit = RevivBinBestFitScanLimit;
            }
            else
            {
                logger?.LogInformation("[Object Store] Not using Revivification");
            }

            return kvSettings;
        }

        /// <summary>
        /// Get AOF settings
        /// </summary>
        /// <param name="dbId">DB ID</param>
        /// <param name="tsavoriteLogSettings">Tsavorite log settings</param>
        public void GetAofSettings(int dbId, out TsavoriteLogSettings tsavoriteLogSettings)
        {
            tsavoriteLogSettings = new TsavoriteLogSettings
            {
                MemorySizeBits = AofMemorySizeBits(),
                PageSizeBits = AofPageSizeBits(),
                LogDevice = GetAofDevice(dbId),
                TryRecoverLatest = false,
                SafeTailRefreshFrequencyMs = EnableCluster ? AofReplicationRefreshFrequencyMs : -1,
                FastCommitMode = EnableFastCommit,
                AutoCommit = CommitFrequencyMs == 0,
                MutableFraction = 0.9,
            };
            if (tsavoriteLogSettings.PageSize > tsavoriteLogSettings.MemorySize)
            {
                logger?.LogError("AOF Page size cannot be more than the AOF memory size.");
                throw new Exception("AOF Page size cannot be more than the AOF memory size.");
            }

            var aofDir = GetAppendOnlyFileDirectory(dbId);
            // We use Tsavorite's default checkpoint manager for AOF, since cookie is not needed for AOF commits
            tsavoriteLogSettings.LogCommitManager = new DeviceLogCommitCheckpointManager(
                FastAofTruncate ? new NullNamedDeviceFactoryCreator() : DeviceFactoryCreator,
                    new DefaultCheckpointNamingScheme(aofDir),
                    removeOutdated: true,
                    fastCommitThrottleFreq: EnableFastCommit ? FastCommitThrottleFreq : 0);
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
            long size = ParseSize(AofMemorySize, out _);
            long adjustedSize = PreviousPowerOf2(size);
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
            long size = ParseSize(AofPageSize, out _);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower AOF page size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get maximum AOF size in bits
        /// </summary>
        /// <returns></returns>
        public int AofSizeLimitSizeBits()
        {
            long size = ParseSize(AofSizeLimit, out _);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower AOF memory size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get object store page size
        /// </summary>
        /// <returns></returns>
        public int ObjectStorePageSizeBits()
        {
            long size = ParseSize(ObjectStorePageSize, out _);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower object store page size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get integer value of ReplicaDisklessSyncFullSyncAofThreshold
        /// </summary>
        /// <returns></returns>
        public long ReplicaDisklessSyncFullSyncAofThresholdValue()
            => ParseSize(string.IsNullOrEmpty(ReplicaDisklessSyncFullSyncAofThreshold) ? AofMemorySize : ReplicaDisklessSyncFullSyncAofThreshold, out _);

        /// <summary>
        /// Get object store segment size
        /// </summary>
        /// <returns></returns>
        public int ObjectStoreSegmentSizeBits()
        {
            long size = ParseSize(ObjectStoreSegmentSize, out _);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation("Warning: using lower object store disk segment size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get device for AOF
        /// </summary>
        /// <returns></returns>
        IDevice GetAofDevice(int dbId)
        {
            if (UseAofNullDevice && EnableCluster && !FastAofTruncate)
                throw new Exception("Cannot use null device for AOF when cluster is enabled and you are not using main memory replication");
            if (UseAofNullDevice) return new NullDevice();

            return GetInitializedDeviceFactory(AppendOnlyFileBaseDirectory)
                .Get(new FileDescriptor(GetAppendOnlyFileDirectoryName(dbId), "aof.log"));
        }
    }
}