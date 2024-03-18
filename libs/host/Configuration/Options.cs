// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using CommandLine;
using Garnet.server;
using Garnet.server.Auth;
using Garnet.server.Auth.Aad;
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
    /// 3. Add a default value for the new property in defaults.conf
    /// 4. If needed, add a matching property in Garnet.server/Servers/GarnetServerOptions.cs and initialize it in Options.GetServerOptions()
    /// 5. If new setting has a matching setting in redis.conf, add the matching setting to RedisOptions.cs
    /// </summary>
    internal sealed class Options
    {
        [IntRangeValidation(0, 65535)]
        [Option("port", Required = false, HelpText = "Port to run server on")]
        public int Port { get; set; }

        [IpAddressValidation(false)]
        [Option("bind", Required = false, HelpText = "IP address to bind server to (default: any)")]
        public string Address { get; set; }

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
        [Option('i', "index", Required = false, HelpText = "Size of hash index in bytes (rounds down to power of 2)")]
        public string IndexSize { get; set; }

        [MemorySizeValidation(false)]
        [Option("index-max-size", Required = false, HelpText = "Max size of hash index in bytes (rounds down to power of 2)")]
        public string IndexMaxSize { get; set; }

        [PercentageValidation(false)]
        [Option("mutable-percent", Required = false, HelpText = "Percentage of log memory that is kept mutable")]
        public int MutablePercent { get; set; }

        [MemorySizeValidation(false)]
        [Option("obj-total-memory", Required = false, HelpText = "Total object store log memory used including heap memory in bytes")]
        public string ObjectStoreTotalMemorySize { get; set; }

        [MemorySizeValidation]
        [Option("obj-memory", Required = false, HelpText = "Object store log memory used in bytes excluding heap memory")]
        public string ObjectStoreLogMemorySize { get; set; }

        [MemorySizeValidation]
        [Option("obj-page", Required = false, HelpText = "Size of each object store page in bytes (rounds down to power of 2)")]
        public string ObjectStorePageSize { get; set; }

        [MemorySizeValidation]
        [Option("obj-segment", Required = false, HelpText = "Size of each object store log segment in bytes on disk (rounds down to power of 2)")]
        public string ObjectStoreSegmentSize { get; set; }

        [MemorySizeValidation]
        [Option("obj-index", Required = false, HelpText = "Size of object store hash index in bytes (rounds down to power of 2)")]
        public string ObjectStoreIndexSize { get; set; }

        [MemorySizeValidation(false)]
        [Option("obj-index-max-size", Required = false, HelpText = "Max size of object store hash index in bytes (rounds down to power of 2)")]
        public string ObjectStoreIndexMaxSize { get; set; }

        [PercentageValidation]
        [Option("obj-mutable-percent", Required = false, HelpText = "Percentage of object store log memory that is kept mutable")]
        public int ObjectStoreMutablePercent { get; set; }

        [OptionValidation]
        [Option("storage-tier", Required = false, HelpText = "Enable tiering of records (hybrid log) to storage, to support a larger-than-memory store. Use --logdir to specify storage directory.")]
        public bool? EnableStorageTier { get; set; }

        [OptionValidation]
        [Option("copy-reads-to-tail", Required = false, HelpText = "When records are read from the main store's in-memory immutable region or storage device, copy them to the tail of the log.")]
        public bool? CopyReadsToTail { get; set; }

        [OptionValidation]
        [Option("obj-copy-reads-to-tail", Required = false, HelpText = "When records are read from the object store's in-memory immutable region or storage device, copy them to the tail of the log.")]
        public bool? ObjectStoreCopyReadsToTail { get; set; }

        [LogDirValidation(false, false)]
        [Option('l', "logdir", Required = false, HelpText = "Storage directory for tiered records (hybrid log), if storage tiering (--storage) is enabled. Uses current directory if unspecified.")]
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

        [Option("auth", Required = false, HelpText = "Authentication mode of Garnet. This impacts how AUTH command is processed and how clients are authenticated against Garnet. Value options: NoAuth, Password, Aad, ACL")]
        public GarnetAuthenticationMode AuthenticationMode { get; set; }

        [Option("password", Required = false, HelpText = "Authentication string for password authentication.")]
        public string Password { get; set; }

        [Option("cluster-username", Required = false, HelpText = "Username to authenticate intra-cluster communication with.")]
        public string ClusterUsername { get; set; }

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

        [Option("aad-authorized-app-ids", Required = false, Separator = ',', HelpText = "The authorized client app Ids for AAD authentication. Should be a comma separated string.")]
        public string AuthorizedAadApplicationIds { get; set; }

        [OptionValidation]
        [Option("aof", Required = false, HelpText = "Enable write ahead logging (append-only file).")]
        public bool? EnableAOF { get; set; }

        [MemorySizeValidation]
        [Option("aof-memory", Required = false, HelpText = "Total AOF memory buffer used in bytes (rounds down to power of 2) - spills to disk after this limit")]
        public string AofMemorySize { get; set; }

        [MemorySizeValidation]
        [Option("aof-page-size", Required = false, HelpText = "Size of each AOF page in bytes(rounds down to power of 2)")]
        public string AofPageSize { get; set; }

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
        [Option("compaction-freq", Required = false, HelpText = "Background hybrid log compaction frequency in seconds. 0 = disabled (compaction performed before checkpointing instead)")]
        public int CompactionFrequencySecs { get; set; }

        [Option("compaction-type", Required = false, HelpText = "Hybrid log compaction type. Value options: None - No compaction, Shift - shift begin address without compaction (data loss), ShiftForced - shift begin address without compaction (data loss). Immediately deletes files - do not use if you plan to recover after failure, Scan - scan old pages and move live records to tail (no data loss - take a checkpoint to actually delete the older data files from disk), Lookup - Lookup each record in compaction range, for record liveness checking using hash chain (no data loss - take a checkpoint to actually delete the older data files from disk)")]
        public LogCompactionType CompactionType { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("compaction-max-segments", Required = false, HelpText = "Number of log segments created on disk before compaction triggers.")]
        public int CompactionMaxSegments { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("obj-compaction-max-segments", Required = false, HelpText = "Number of object store log segments created on disk before compaction triggers.")]
        public int ObjectStoreCompactionMaxSegments { get; set; }

        [PercentageValidation]
        [Option("gossip-sp", Required = false, HelpText = "Percent of cluster nodes to gossip with at each gossip iteration.")]
        public int GossipSamplePercent { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("gossip-delay", Required = false, HelpText = "Cluster mode gossip protocol per node sleep (in seconds) delay to send updated config.")]
        public int GossipDelay { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("cluster-timeout", Required = false, HelpText = "Cluster node timeout is the amount of seconds a node must be unreachable.")]
        public int ClusterTimeout { get; set; }

        [Option("cluster-tls-client-target-host", Required = false, HelpText = "Name for the client target host when using TLS connections in cluster mode.")]
        public string ClusterTlsClientTargetHost { get; set; }

        [OptionValidation]
        [Option("tls", Required = false, HelpText = "Enable TLS.")]
        public bool? EnableTLS { get; set; }

        [CertFileValidation(true, true, false)]
        [Option("cert-file-name", Required = false, HelpText = "TLS certificate file name (example: testcert.pfx).")]
        public string CertFileName { get; set; }

        [Option("cert-password", Required = false, HelpText = "TLS certificate password (example: placeholder).")]
        public string CertPassword { get; set; }

        [Option("cert-subject-name", Required = false, HelpText = "TLS certificate subject name.")]
        public string CertSubjectName { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("cert-refresh-freq", Required = false, HelpText = "TLS certificate refresh frequency in seconds (0 to disable).")]
        public int CertificateRefreshFrequency { get; set; }

        [OptionValidation]
        [Option("client-certificate-required", Required = false, HelpText = "Whether TLS client certificate required.")]
        public bool? ClientCertificateRequired { get; set; }

        [Option("certificate-revocation-check-mode", Required = false, HelpText = "Certificate revocation check mode for certificate validation (NoCheck, Online, Offline).")]
        public X509RevocationMode CertificateRevocationCheckMode { get; set; }

        [Option("issuer-certificate-path", Required = false, HelpText = "Full path of file with issuer certificate for validation. If empty or null, validation against issuer will not be performed.")]
        public string IssuerCertificatePath { get; set; }

        [OptionValidation]
        [Option("latency-monitor", Required = false, HelpText = "Track latency of various events.")]
        public bool? LatencyMonitor { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("metrics-sampling-freq", Required = false, HelpText = "Metrics sampling frequency in seconds. Value of 0 disables metrics monitor task.")]
        public int MetricsSamplingFrequency { get; set; }

        [OptionValidation]
        [Option('q', Required = false, HelpText = "Enabling quiet mode does not print server version and text art.")]
        public bool? QuietMode { get; set; }

        [Option("logger-level", Required = false, HelpText = "Logging level. Value options: Trace, Debug, Information, Warning, Error, Critical, None")]
        public LogLevel LogLevel { get; set; }

        [OptionValidation]
        [Option("disable-console-logger", Required = false, HelpText = "Disable console logger.")]
        public bool? DisableConsoleLogger { get; set; }

        [FilePathValidation(false, false, false)]
        [Option("file-logger", Required = false, HelpText = "Enable file logger and write to the specified path.")]
        public string FileLogger { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("minthreads", Required = false, HelpText = "Minimum worker and completion threads in thread pool, 0 uses the system default.")]
        public int ThreadPoolMinThreads { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("maxthreads", Required = false, HelpText = "Maximum worker and completion threads in thread pool, 0 uses the system default.")]
        public int ThreadPoolMaxThreads { get; set; }

        [OptionValidation]
        [Option("use-azure-storage", Required = false, HelpText = "Use Azure Page Blobs for storage instead of local storage.")]
        public bool? UseAzureStorage { get; set; }

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

        [OptionValidation]
        [Option("main-memory-replication", Required = false, HelpText = "Use main-memory replication model.")]
        public bool? MainMemoryReplication { get; set; }

        [OptionValidation]
        [Option("on-demand-checkpoint", Required = false, HelpText = "Used with main-memory replication model. Take on demand checkpoint to avoid missing data when attaching")]
        public bool? OnDemandCheckpoint { get; set; }

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
            HelpText = "#: Fraction of mutable in-memory log space, from the highest log address down to the read-only region, that is eligible for revivification." +
                       "        Applies to both main and object store.")]
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
                       "    Cannot be used with reviv-bin-record-sizes or reviv-bin-record-counts. Propagates to object store by default.")]
        public bool? RevivInChainOnly { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("reviv-obj-bin-record-count", Required = false,
            HelpText = "Number of records in the single free record bin for the object store. The Object store has only a single bin, unlike the main store." +
                       "        Ignored unless the main store is using the free record list.")]
        public int RevivObjBinRecordCount { get; set; }

        [IntRangeValidation(0, int.MaxValue)]
        [Option("object-scan-count-limit", Required = false, HelpText = "Limit of items to return in one iteration of *SCAN command")]
        public int ObjectScanCountLimit { get; set; }

        [DirectoryPathsValidation(true, false)]
        [Option("extension-bin-paths", Separator = ',', Required = false, HelpText = "List of directories on server from which custom command binaries can be loaded by admin users")]
        public IEnumerable<string> ExtensionBinPaths { get; set; }

        [Option("extension-allow-unsigned", Required = false, HelpText = "Allow loading custom commands from digitally unsigned assemblies (not recommended)")]
        public bool? ExtensionAllowUnsignedAssemblies { get; set; }

        [IntRangeValidation(1, int.MaxValue, isRequired: false)]
        [Option("index-resize-freq", Required = false, HelpText = "Index resize check frequency in seconds")]
        public int IndexResizeFrequencySecs { get; set; }

        [IntRangeValidation(1, 100, isRequired: false)]
        [Option("index-resize-threshold", Required = false, HelpText = "Overflow bucket count over total index size in percentage to trigger index resize")]
        public int IndexResizeThreshold { get; set; }

        /// <summary>
        /// This property contains all arguments that were not parsed by the command line argument parser
        /// </summary>
        [System.Text.Json.Serialization.JsonIgnore]
        [Value(0)]
        public IList<string> UnparsedArguments { get; set; }

        /// <summary>
        /// Check the validity of all options with an explicit ValidationAttribute
        /// </summary>
        /// <param name="invalidOptions">List of invalid options</param>
        /// <param name="logger">Logger</param>
        /// <returns>True if all property values are valid</returns>
        public bool IsValid(out List<string> invalidOptions, ILogger logger)
        {
            invalidOptions = new List<string>();
            bool isValid = true;

            foreach (PropertyInfo prop in typeof(Options).GetProperties())
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
                    logger?.LogError(validationResult.ErrorMessage);
                }
            }

            return isValid;
        }

        public GarnetServerOptions GetServerOptions(ILogger logger = null)
        {
            var useAzureStorage = UseAzureStorage.GetValueOrDefault();
            var enableStorageTier = EnableStorageTier.GetValueOrDefault();
            var enableRevivification = EnableRevivification.GetValueOrDefault();

            if (useAzureStorage && string.IsNullOrEmpty(AzureStorageConnectionString))
                throw new Exception("Cannot enable use-azure-storage without supplying storage-string.");

            var logDir = LogDir;
            if (!useAzureStorage && enableStorageTier) logDir = new DirectoryInfo(string.IsNullOrEmpty(logDir) ? "." : logDir).FullName;
            var checkpointDir = CheckpointDir;
            if (!useAzureStorage) checkpointDir = new DirectoryInfo(string.IsNullOrEmpty(checkpointDir) ? "." : checkpointDir).FullName;

            var address = !string.IsNullOrEmpty(this.Address) && this.Address.Equals("localhost", StringComparison.CurrentCultureIgnoreCase)
                ? IPAddress.Loopback.ToString()
                : this.Address;

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

            return new GarnetServerOptions(logger)
            {
                Port = Port,
                Address = address,
                MemorySize = MemorySize,
                PageSize = PageSize,
                SegmentSize = SegmentSize,
                IndexSize = IndexSize,
                IndexMaxSize = IndexMaxSize,
                MutablePercent = MutablePercent,
                ObjectStoreTotalMemorySize = ObjectStoreTotalMemorySize,
                ObjectStoreLogMemorySize = ObjectStoreLogMemorySize,
                ObjectStorePageSize = ObjectStorePageSize,
                ObjectStoreSegmentSize = ObjectStoreSegmentSize,
                ObjectStoreIndexSize = ObjectStoreIndexSize,
                ObjectStoreIndexMaxSize = ObjectStoreIndexMaxSize,
                ObjectStoreMutablePercent = ObjectStoreMutablePercent,
                EnableStorageTier = enableStorageTier,
                CopyReadsToTail = CopyReadsToTail.GetValueOrDefault(),
                ObjectStoreCopyReadsToTail = ObjectStoreCopyReadsToTail.GetValueOrDefault(),
                LogDir = logDir,
                CheckpointDir = checkpointDir,
                Recover = Recover.GetValueOrDefault(),
                EnableIncrementalSnapshots = EnableIncrementalSnapshots.GetValueOrDefault(),
                DisablePubSub = DisablePubSub.GetValueOrDefault(),
                PubSubPageSize = PubSubPageSize,
                DisableObjects = DisableObjects.GetValueOrDefault(),
                EnableCluster = EnableCluster.GetValueOrDefault(),
                CleanClusterConfig = CleanClusterConfig.GetValueOrDefault(),
                AuthSettings = GetAuthenticationSettings(logger),
                EnableAOF = EnableAOF.GetValueOrDefault(),
                AofMemorySize = AofMemorySize,
                AofPageSize = AofPageSize,
                CommitFrequencyMs = CommitFrequencyMs,
                WaitForCommit = WaitForCommit.GetValueOrDefault(),
                AofSizeLimit = AofSizeLimit,
                CompactionFrequencySecs = CompactionFrequencySecs,
                CompactionType = CompactionType,
                CompactionMaxSegments = CompactionMaxSegments,
                ObjectStoreCompactionMaxSegments = ObjectStoreCompactionMaxSegments,
                GossipSamplePercent = GossipSamplePercent,
                GossipDelay = GossipDelay,
                ClusterTimeout = ClusterTimeout,
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
                    logger: logger) : null,
                LatencyMonitor = LatencyMonitor.GetValueOrDefault(),
                MetricsSamplingFrequency = MetricsSamplingFrequency,
                LogLevel = LogLevel,
                QuietMode = QuietMode.GetValueOrDefault(),
                ThreadPoolMinThreads = ThreadPoolMinThreads,
                ThreadPoolMaxThreads = ThreadPoolMaxThreads,
                DeviceFactoryCreator = useAzureStorage
                    ? () => new AzureStorageNamedDeviceFactory(AzureStorageConnectionString, logger)
                    : () => new LocalStorageNamedDeviceFactory(useNativeDeviceLinux: UseNativeDeviceLinux.GetValueOrDefault(), logger: logger),
                CheckpointThrottleFlushDelayMs = CheckpointThrottleFlushDelayMs,
                EnableScatterGatherGet = EnableScatterGatherGet.GetValueOrDefault(),
                ReplicaSyncDelayMs = ReplicaSyncDelayMs,
                MainMemoryReplication = MainMemoryReplication.GetValueOrDefault(),
                OnDemandCheckpoint = OnDemandCheckpoint.GetValueOrDefault(),
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
                RevivObjBinRecordCount = RevivObjBinRecordCount,
                ExtensionBinPaths = ExtensionBinPaths?.ToArray(),
                ExtensionAllowUnsignedAssemblies = ExtensionAllowUnsignedAssemblies.GetValueOrDefault(),
                IndexResizeFrequencySecs = IndexResizeFrequencySecs,
                IndexResizeThreshold = IndexResizeThreshold
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
                    return new AclAuthenticationSettings(AclFile, Password);
                default:
                    logger?.LogError("Unsupported authentication mode: {mode}", AuthenticationMode);
                    throw new Exception($"Authentication mode {AuthenticationMode} is not supported.");
            }
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
}