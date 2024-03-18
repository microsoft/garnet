// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.IO;
using Garnet.server.Auth;
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
        /// Total memory size limit of object store including heap memory of objects.
        /// </summary>
        public string ObjectStoreTotalMemorySize = "";

        /// <summary>
        /// Object store log memory used in bytes excluding heap memory.
        /// </summary>
        public string ObjectStoreLogMemorySize = "32m";

        /// <summary>
        /// Size of each object store page in bytes (rounds down to power of 2).
        /// </summary>
        public string ObjectStorePageSize = "1m";

        /// <summary>
        /// Size of each object store log segment in bytes on disk (rounds down to power of 2).
        /// </summary>
        public string ObjectStoreSegmentSize = "32m";

        /// <summary>
        /// Size of object store hash index in bytes (rounds down to power of 2).
        /// </summary>
        public string ObjectStoreIndexSize = "1g";

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
        /// Authentication settings
        /// </summary>
        public IAuthenticationSettings AuthSettings = null;

        /// <summary>
        /// Enable append-only file (write ahead log)
        /// </summary>
        public bool EnableAOF = false;

        /// <summary>
        /// Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit.
        /// </summary>
        public string AofMemorySize = "64m";

        /// <summary>
        /// Aof page size in bytes (rounds down to power of 2).
        /// </summary>
        public string AofPageSize = "4m";

        /// <summary>
        /// Write ahead logging (append-only file) commit issue frequency in milliseconds.
        /// 0 = issue an immediate commit per operation
        /// -1 = manually issue commits using COMMITAOF command (no auto-commit)
        /// </summary>
        public int CommitFrequencyMs = 0;

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
        /// Hybrid log compaction frequency in seconds. 0 = disabled
        /// </summary>
        public int CompactionFrequencySecs = 0;

        /// <summary>
        /// Hybrid log compaction type. Shift = shift begin address without compaction (data loss), Scan = scan old pages and move live records to tail (no data loss - take a checkpoint to actually delete the older data files from disk).
        /// </summary>
        public LogCompactionType CompactionType = LogCompactionType.Shift;

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
        /// Metrics sampling frequency
        /// </summary>
        public int MetricsSamplingFrequency = 0;

        /// <summary>
        /// Metrics sampling frequency
        /// </summary>
        public LogLevel LogLevel = LogLevel.Error;

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
        /// Minimum worker and completion port threads in thread pool (0 for default)
        /// </summary>
        public int ThreadPoolMinThreads = 0;

        /// <summary>
        /// Maximum worker and completion port threads in thread pool (0 for default)
        /// </summary>
        public int ThreadPoolMaxThreads = 0;

        /// <summary>
        /// Creator of device factories
        /// </summary>
        public Func<INamedDeviceFactory> DeviceFactoryCreator = null;

        /// <summary>
        /// Whether and by how much should we throttle the disk IO for checkpoints (default = 0)
        /// -1   - disable throttling
        /// >= 0 - run checkpoint flush in separate task, sleep for specified time after each WRiteAsync
        /// </summary>
        public int CheckpointThrottleFlushDelayMs = 0;

        /// <summary>
        /// Enable FastCommit mode for TsavoriteLog
        /// </summary>
        public bool EnableFastCommit = false;

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
        /// Whether we truncate AOF as soon as replicas are fed (not just after checkpoints)
        /// </summary>
        public bool MainMemoryReplication = false;

        /// <summary>
        /// Used with main-memory replication model. Take on demand checkpoint to avoid missing data when attaching
        /// </summary>
        public bool OnDemandCheckpoint = false;

        /// <summary>
        /// With main-memory replication, whether we use null device for AOF. Ensures no disk IO, but can cause data loss during replication.
        /// </summary>
        public bool UseAofNullDevice = false;

        /// <summary>
        /// Use native device on Linux for local storage
        /// </summary>
        public bool UseNativeDeviceLinux = false;



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
        public int AdjustedIndexMaxSize;

        /// <summary>Max size of object store hash index (cache lines) after rounding down size in bytes to power of 2.</summary>
        public int AdjustedObjectStoreIndexMaxSize;

        /// <summary>
        /// Directories on server from which custom command binaries can be loaded by admin users
        /// </summary>
        public string[] ExtensionBinPaths;

        /// <summary>
        /// Allow loading custom commands from digitally unsigned assemblies
        /// </summary>
        public bool ExtensionAllowUnsignedAssemblies;

        /// <summary>
        /// Constructor
        /// </summary>
        public GarnetServerOptions(ILogger logger = null) : base(logger)
        {
            this.logger = logger;
        }

        /// <summary>
        /// Get log settings
        /// </summary>
        /// <param name="logSettings"></param>
        /// <param name="indexSize"></param>
        /// <param name="revivSettings"></param>
        /// <param name="logFactory"></param>
        public void GetSettings(out LogSettings logSettings, out int indexSize, out RevivificationSettings revivSettings, out INamedDeviceFactory logFactory)
        {
            if (MutablePercent < 10 || MutablePercent > 95)
                throw new Exception("MutablePercent must be between 10 and 95");

            logSettings = new LogSettings
            {
                PreallocateLog = false,
                MutableFraction = MutablePercent / 100.0,
                PageSizeBits = PageSizeBits()
            };
            logger?.LogInformation($"[Store] Using page size of {PrettySize((long)Math.Pow(2, logSettings.PageSizeBits))}");

            logSettings.MemorySizeBits = MemorySizeBits(MemorySize, PageSize, out var storeEmptyPageCount);
            logSettings.MinEmptyPageCount = storeEmptyPageCount;

            long effectiveSize = (1L << logSettings.MemorySizeBits) - storeEmptyPageCount * (1L << logSettings.PageSizeBits);
            if (storeEmptyPageCount == 0)
                logger?.LogInformation($"[Store] Using log memory size of {PrettySize((long)Math.Pow(2, logSettings.MemorySizeBits))}");
            else
                logger?.LogInformation($"[Store] Using log memory size of {PrettySize((long)Math.Pow(2, logSettings.MemorySizeBits))}, with {storeEmptyPageCount} empty pages, for effective size of {PrettySize(effectiveSize)}");

            logger?.LogInformation($"[Store] There are {PrettySize(1 << (logSettings.MemorySizeBits - logSettings.PageSizeBits))} log pages in memory");

            logSettings.SegmentSizeBits = SegmentSizeBits();
            logger?.LogInformation($"[Store] Using disk segment size of {PrettySize((long)Math.Pow(2, logSettings.SegmentSizeBits))}");

            indexSize = IndexSizeCachelines("hash index size", IndexSize);
            logger?.LogInformation($"[Store] Using hash index size of {PrettySize(indexSize * 64L)} ({PrettySize(indexSize)} cache lines)");

            AdjustedIndexMaxSize = IndexMaxSize == string.Empty ? 0 : IndexSizeCachelines("hash index max size", IndexMaxSize);
            if (AdjustedIndexMaxSize != 0 && AdjustedIndexMaxSize < indexSize)
                throw new Exception($"Index size {IndexSize} should not be less than index max size {IndexMaxSize}");

            if (AdjustedIndexMaxSize > 0)
                logger?.LogInformation($"[Store] Using hash index max size of {PrettySize(AdjustedIndexMaxSize * 64L)}, ({PrettySize(AdjustedIndexMaxSize)} cache lines)");

            logger?.LogInformation($"[Store] Using log mutable percentage of {MutablePercent}%");

            if (DeviceFactoryCreator == null)
                DeviceFactoryCreator = () => new LocalStorageNamedDeviceFactory(useNativeDeviceLinux: UseNativeDeviceLinux, logger: logger);

            if (LatencyMonitor && MetricsSamplingFrequency == 0)
                throw new Exception("LatencyMonitor requires MetricsSamplingFrequency to be set");

            if (EnableStorageTier)
            {
                if (LogDir is null or "")
                    LogDir = Directory.GetCurrentDirectory();
                logFactory = GetInitializedDeviceFactory(LogDir);
                logSettings.LogDevice = logFactory.Get(new FileDescriptor("Store", "hlog"));
            }
            else
            {
                if (LogDir != null)
                    throw new Exception("LogDir specified without enabling tiered storage (UseStorage)");
                logSettings.LogDevice = new NullDevice();
                logFactory = null;
            }

            if (CopyReadsToTail)
                logSettings.ReadCopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog);

            if (RevivInChainOnly)
            {
                logger?.LogInformation($"[Store] Using Revivification in-chain only");
                revivSettings = RevivificationSettings.InChainOnly.Clone();
            }
            else if (UseRevivBinsPowerOf2)
            {
                logger?.LogInformation($"[Store] Using Revivification with power-of-2 bins");
                revivSettings = RevivificationSettings.PowerOf2Bins.Clone();
                revivSettings.NumberOfBinsToSearch = RevivNumberOfBinsToSearch;
                revivSettings.RevivifiableFraction = RevivifiableFraction;
            }
            else if (RevivBinRecordSizes?.Length > 0)
            {
                logger?.LogInformation($"[Store] Using Revivification with custom bins");

                // We use this in the RevivBinRecordCounts and RevivObjBinRecordCount Options help text, so assert it here because we can't use an interpolated string there.
                System.Diagnostics.Debug.Assert(RevivificationBin.DefaultRecordsPerBin == 256);
                revivSettings = new()
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
                    revivSettings.FreeRecordBins[ii] = new()
                    {
                        RecordSize = RevivBinRecordSizes[ii],
                        NumberOfRecords = recordCount,
                        BestFitScanLimit = RevivBinBestFitScanLimit
                    };
                }
            }
            else
            {
                logger?.LogInformation($"[Store] Not using Revivification");
                revivSettings = default;
            }
        }

        /// <summary>
        /// Get memory size
        /// </summary>
        /// <returns></returns>
        public int MemorySizeBits(string memorySize, string storePageSize, out int emptyPageCount)
        {
            emptyPageCount = 0;
            long size = ParseSize(memorySize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
            {
                adjustedSize *= 2;
                long pageSize = ParseSize(storePageSize);
                pageSize = PreviousPowerOf2(pageSize);
                emptyPageCount = (int)((adjustedSize - size) / pageSize);
            }
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get object store settings
        /// </summary>
        /// <param name="objLogSettings"></param>
        /// <param name="objRevivSettings"></param>
        /// <param name="objIndexSize"></param>
        /// <param name="objTotalMemorySize"></param>

        public void GetObjectStoreSettings(out LogSettings objLogSettings, out RevivificationSettings objRevivSettings, out int objIndexSize, out long objTotalMemorySize)
        {
            if (ObjectStoreMutablePercent < 10 || ObjectStoreMutablePercent > 95)
                throw new Exception("ObjectStoreMutablePercent must be between 10 and 95");

            objLogSettings = new LogSettings
            {
                PreallocateLog = false,
                MutableFraction = ObjectStoreMutablePercent / 100.0,
                PageSizeBits = ObjectStorePageSizeBits()
            };
            logger?.LogInformation($"[Object Store] Using page size of {PrettySize((long)Math.Pow(2, objLogSettings.PageSizeBits))}");
            logger?.LogInformation($"[Object Store] Each page can hold ~{(long)(Math.Pow(2, objLogSettings.PageSizeBits) / 24)} key-value pairs of objects");

            objLogSettings.MemorySizeBits = MemorySizeBits(ObjectStoreLogMemorySize, ObjectStorePageSize, out var objectStoreEmptyPageCount);
            objLogSettings.MinEmptyPageCount = objectStoreEmptyPageCount;

            long effectiveSize = (1L << objLogSettings.MemorySizeBits) - objectStoreEmptyPageCount * (1L << objLogSettings.PageSizeBits);
            if (objectStoreEmptyPageCount == 0)
                logger?.LogInformation($"[Object Store] Using log memory size of {PrettySize((long)Math.Pow(2, objLogSettings.MemorySizeBits))}");
            else
                logger?.LogInformation($"[Object Store] Using log memory size of {PrettySize((long)Math.Pow(2, objLogSettings.MemorySizeBits))}, with {objectStoreEmptyPageCount} empty pages, for effective size of {PrettySize(effectiveSize)}");

            logger?.LogInformation($"[Object Store] This can hold ~{effectiveSize / 24} key-value pairs of objects in memory total");

            logger?.LogInformation($"[Object Store] There are {PrettySize(1 << (objLogSettings.MemorySizeBits - objLogSettings.PageSizeBits))} log pages in memory");

            objLogSettings.SegmentSizeBits = ObjectStoreSegmentSizeBits();
            logger?.LogInformation($"[Object Store] Using disk segment size of {PrettySize((long)Math.Pow(2, objLogSettings.SegmentSizeBits))}");

            objIndexSize = IndexSizeCachelines("object store hash index size", ObjectStoreIndexSize);
            logger?.LogInformation($"[Object Store] Using hash index size of {PrettySize(objIndexSize * 64L)} ({PrettySize(objIndexSize)} cache lines)");

            AdjustedObjectStoreIndexMaxSize = ObjectStoreIndexMaxSize == string.Empty ? 0 : IndexSizeCachelines("hash index max size", ObjectStoreIndexMaxSize);
            if (AdjustedObjectStoreIndexMaxSize != 0 && AdjustedObjectStoreIndexMaxSize < objIndexSize)
                throw new Exception($"Index size {IndexSize} should not be less than index max size {IndexMaxSize}");

            if (AdjustedObjectStoreIndexMaxSize > 0)
                logger?.LogInformation($"[Object Store] Using hash index max size of {PrettySize(AdjustedObjectStoreIndexMaxSize * 64L)}, ({PrettySize(AdjustedObjectStoreIndexMaxSize)} cache lines)");

            logger?.LogInformation($"[Object Store] Using log mutable percentage of {ObjectStoreMutablePercent}%");

            objTotalMemorySize = ParseSize(ObjectStoreTotalMemorySize);
            logger?.LogInformation($"[Object Store] Total memory size including heap objects is {(objTotalMemorySize > 0 ? PrettySize(objTotalMemorySize) : "unlimited")}");

            if (EnableStorageTier)
            {
                if (LogDir is null or "")
                    LogDir = Directory.GetCurrentDirectory();
                objLogSettings.LogDevice = GetInitializedDeviceFactory(LogDir).Get(new FileDescriptor("ObjectStore", "hlog"));
                objLogSettings.ObjectLogDevice = GetInitializedDeviceFactory(LogDir).Get(new FileDescriptor("ObjectStore", "hlog.obj"));
            }
            else
            {
                if (LogDir != null)
                    throw new Exception("LogDir specified without enabling tiered storage (UseStorage)");
                objLogSettings.LogDevice = objLogSettings.ObjectLogDevice = new NullDevice();
            }

            if (ObjectStoreCopyReadsToTail)
                objLogSettings.ReadCopyOptions = new(ReadCopyFrom.AllImmutable, ReadCopyTo.MainLog);

            if (RevivInChainOnly)
            {
                logger?.LogInformation($"[Object Store] Using Revivification in-chain only");
                objRevivSettings = RevivificationSettings.InChainOnly.Clone();
            }
            else if (UseRevivBinsPowerOf2 || RevivBinRecordSizes?.Length > 0)
            {
                logger?.LogInformation($"[Object Store] Using Revivification with a single fixed-size bin");
                objRevivSettings = RevivificationSettings.DefaultFixedLength.Clone();
                objRevivSettings.RevivifiableFraction = RevivifiableFraction;
                objRevivSettings.FreeRecordBins[0].NumberOfRecords = RevivObjBinRecordCount;
                objRevivSettings.FreeRecordBins[0].BestFitScanLimit = RevivBinBestFitScanLimit;
            }
            else
            {
                logger?.LogInformation($"[Object Store] Not using Revivification");
                objRevivSettings = default;
            }
        }

        /// <summary>
        /// Get AOF settings
        /// </summary>
        /// <param name="tsavoriteLogSettings"></param>
        public void GetAofSettings(out TsavoriteLogSettings tsavoriteLogSettings)
        {
            tsavoriteLogSettings = new TsavoriteLogSettings
            {
                MemorySizeBits = AofMemorySizeBits(),
                PageSizeBits = AofPageSizeBits(),
                LogDevice = GetAofDevice(),
                TryRecoverLatest = false,
                AutoRefreshSafeTailAddress = true,
                FastCommitMode = EnableFastCommit,
                AutoCommit = CommitFrequencyMs == 0,
                MutableFraction = 0.9,
            };
            if (tsavoriteLogSettings.PageSize > tsavoriteLogSettings.MemorySize)
            {
                logger?.LogError("AOF Page size cannot be more than the AOF memory size.");
                throw new Exception("AOF Page size cannot be more than the AOF memory size.");
            }
            tsavoriteLogSettings.LogCommitManager = new DeviceLogCommitCheckpointManager(
                MainMemoryReplication ? new NullNamedDeviceFactory() : DeviceFactoryCreator(),
                    new DefaultCheckpointNamingScheme(CheckpointDir + "/AOF"),
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
            var deviceFactory = GetDeviceFactory();
            deviceFactory.Initialize(baseName);
            return deviceFactory;
        }

        /// <summary>
        /// Get AOF memory size in bits
        /// </summary>
        /// <returns></returns>
        public int AofMemorySizeBits()
        {
            long size = ParseSize(AofMemorySize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower AOF memory size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get AOF Page size in bits
        /// </summary>
        /// <returns></returns>
        public int AofPageSizeBits()
        {
            long size = ParseSize(AofPageSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower AOF page size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get maximum AOF size in bits
        /// </summary>
        /// <returns></returns>
        public int AofSizeLimitSizeBits()
        {
            long size = ParseSize(AofSizeLimit);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower AOF memory size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get object store page size
        /// </summary>
        /// <returns></returns>
        public int ObjectStorePageSizeBits()
        {
            long size = ParseSize(ObjectStorePageSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower object store page size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get object store segment size
        /// </summary>
        /// <returns></returns>
        public int ObjectStoreSegmentSizeBits()
        {
            long size = ParseSize(ObjectStoreSegmentSize);
            long adjustedSize = PreviousPowerOf2(size);
            if (size != adjustedSize)
                logger?.LogInformation($"Warning: using lower object store disk segment size than specified (power of 2)");
            return (int)Math.Log(adjustedSize, 2);
        }

        /// <summary>
        /// Get device for AOF
        /// </summary>
        /// <returns></returns>
        IDevice GetAofDevice()
        {
            if (!MainMemoryReplication && UseAofNullDevice)
                throw new Exception("Cannot use null device for AOF when not using main memory replication");
            if (MainMemoryReplication && UseAofNullDevice) return new NullDevice();
            else return GetInitializedDeviceFactory(CheckpointDir).Get(new FileDescriptor("AOF", "aof.log"));
        }

        /// <summary>
        /// Get device factory
        /// </summary>
        /// <returns></returns>
        public INamedDeviceFactory GetDeviceFactory() => DeviceFactoryCreator();
    }
}