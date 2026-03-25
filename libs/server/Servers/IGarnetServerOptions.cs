// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server.Auth.Settings;

namespace Garnet.server
{
    /// <summary>
    /// Interface for Garnet server configuration properties used for JIT branch elimination.
    ///
    /// When a zero-size struct implements this interface with constant-returning property getters
    /// and is used as a type parameter (e.g., RespServerSession&lt;TServerOptions&gt;), the JIT:
    ///   1. Creates a separate native code specialization for each struct type
    ///   2. Inlines the constant-returning getters
    ///   3. Sees constant branch conditions (e.g., if (false)) and eliminates dead code entirely
    ///
    /// This eliminates per-command branch overhead for config checks like EnableCluster,
    /// LatencyMonitor, MetricsSamplingFrequency > 0, etc. on the hot path.
    ///
    /// Two implementations are provided:
    ///   - RuntimeServerOptions: delegates to a GarnetServerOptions instance (AOT-compatible, no branch elimination)
    ///   - Dynamically emitted structs via GarnetOptionsFactory: hardcoded constants (full branch elimination)
    ///
    /// Instance (not static) properties are used because Reflection.Emit cannot implement
    /// static abstract interface members. For zero-size structs, default(T) is free and the JIT
    /// inlines instance getters identically to static ones.
    /// </summary>
    public interface IGarnetServerOptions
    {
        // ---------------------------------------------------------------
        // ServerOptions (base class) boolean properties
        // ---------------------------------------------------------------

        /// <summary>
        /// Enable tiering of records (hybrid log) to storage.
        /// </summary>
        bool EnableStorageTier { get; }

        /// <summary>
        /// Copy reads from immutable region or storage device to the tail of the log.
        /// </summary>
        bool CopyReadsToTail { get; }

        /// <summary>
        /// Recover from latest checkpoint.
        /// </summary>
        bool Recover { get; }

        /// <summary>
        /// Disable pub/sub feature on server.
        /// </summary>
        bool DisablePubSub { get; }

        /// <summary>
        /// Fail server bootup on AOF and checkpoint recovery errors.
        /// </summary>
        bool FailOnRecoveryError { get; }

        // ---------------------------------------------------------------
        // GarnetServerOptions boolean properties
        // ---------------------------------------------------------------

        /// <summary>
        /// Disable support for data structure objects.
        /// </summary>
        bool DisableObjects { get; }

        /// <summary>
        /// Enable cluster mode.
        /// </summary>
        bool EnableCluster { get; }

        /// <summary>
        /// Start with clean cluster config.
        /// </summary>
        bool CleanClusterConfig { get; }

        /// <summary>
        /// Fast slot migration without waiting for ACK.
        /// </summary>
        bool FastMigrate { get; }

        /// <summary>
        /// Enable append-only file (write ahead log).
        /// </summary>
        bool EnableAOF { get; }

        /// <summary>
        /// Enable Lua scripts on server.
        /// </summary>
        bool EnableLua { get; }

        /// <summary>
        /// Run Lua scripts as a transaction (lock-run-unlock).
        /// </summary>
        bool LuaTransactionMode { get; }

        /// <summary>
        /// Wait for AOF to commit before returning results to client.
        /// </summary>
        bool WaitForCommit { get; }

        /// <summary>
        /// Forcefully delete inactive segments after compaction.
        /// </summary>
        bool CompactionForceDelete { get; }

        /// <summary>
        /// Enable per-command latency tracking for all commands.
        /// </summary>
        bool LatencyMonitor { get; }

        /// <summary>
        /// Disable startup banner and status messages.
        /// </summary>
        bool QuietMode { get; }

        /// <summary>
        /// Enable incremental snapshots for SAVE and BGSAVE.
        /// </summary>
        bool EnableIncrementalSnapshots { get; }

        /// <summary>
        /// Use fold-over checkpoints instead of snapshots.
        /// </summary>
        bool UseFoldOverCheckpoints { get; }

        /// <summary>
        /// Enable FastCommit mode for TsavoriteAof.
        /// </summary>
        bool EnableFastCommit { get; }

        /// <summary>
        /// Use scatter gather IO for MGET operations.
        /// </summary>
        bool EnableScatterGatherGet { get; }

        /// <summary>
        /// Truncate AOF as soon as replicas are fed.
        /// </summary>
        bool FastAofTruncate { get; }

        /// <summary>
        /// Take on-demand checkpoint to avoid missing data when attaching replica.
        /// </summary>
        bool OnDemandCheckpoint { get; }

        /// <summary>
        /// Enable diskless replication sync.
        /// </summary>
        bool ReplicaDisklessSync { get; }

        /// <summary>
        /// Use null device for AOF (main-memory replication).
        /// </summary>
        bool UseAofNullDevice { get; }

        /// <summary>
        /// Use power-of-2-sized bins for revivification.
        /// </summary>
        bool UseRevivBinsPowerOf2 { get; }

        /// <summary>
        /// Revivify tombstoned records in tag chains only (no free list).
        /// </summary>
        bool RevivInChainOnly { get; }

        /// <summary>
        /// Allow loading custom commands from digitally unsigned assemblies.
        /// </summary>
        bool ExtensionAllowUnsignedAssemblies { get; }

        /// <summary>
        /// Enable read cache for on-disk records.
        /// </summary>
        bool EnableReadCache { get; }

        /// <summary>
        /// Cluster replica loads AOF/Checkpoint data from disk on start.
        /// </summary>
        bool ClusterReplicaResumeWithData { get; }

        /// <summary>
        /// Allow more than one logical database in server.
        /// </summary>
        bool AllowMultiDb { get; }

        // ---------------------------------------------------------------
        // Non-boolean fields accessed in hot-path branch conditions.
        // JIT constant-folds comparisons like MetricsSamplingFrequency > 0
        // and eliminates the dead branch entirely.
        // ---------------------------------------------------------------

        /// <summary>
        /// Metrics sampling frequency (0 = disabled). Checked per-command.
        /// </summary>
        int MetricsSamplingFrequency { get; }

        /// <summary>
        /// Threshold (microseconds) for logging command in the slow log (0 = disabled). Checked per-command.
        /// </summary>
        int SlowLogThreshold { get; }

        /// <summary>
        /// Maximum number of slow log entries to keep.
        /// </summary>
        int SlowLogMaxEntries { get; }

        /// <summary>
        /// Limit of items to return in one iteration of *SCAN command.
        /// </summary>
        int ObjectScanCountLimit { get; }

        /// <summary>
        /// Max number of logical databases allowed.
        /// </summary>
        int MaxDatabases { get; }

        /// <summary>
        /// Enables the DEBUG command. Checked per-command dispatch.
        /// </summary>
        ConnectionProtectionOption EnableDebugCommand { get; }

        /// <summary>
        /// Enables the MODULE command. Checked per-command dispatch.
        /// </summary>
        ConnectionProtectionOption EnableModuleCommand { get; }

        /// <summary>
        /// AOF commit frequency in milliseconds. 0 = per-op, -1 = manual.
        /// </summary>
        int CommitFrequencyMs { get; }

        /// <summary>
        /// Hybrid log compaction frequency in seconds (0 = disabled).
        /// </summary>
        int CompactionFrequencySecs { get; }

        /// <summary>
        /// Index resize check frequency in seconds.
        /// </summary>
        int IndexResizeFrequencySecs { get; }

        /// <summary>
        /// Max size of hash index (cache lines) after rounding. 0 = no max.
        /// </summary>
        int AdjustedIndexMaxCacheLines { get; }

        /// <summary>
        /// Max size of object store hash index (cache lines) after rounding. 0 = no max.
        /// </summary>
        int AdjustedObjectStoreIndexMaxCacheLines { get; }

        /// <summary>
        /// Frequency of background scan for expired key deletion (seconds). -1 = disabled.
        /// </summary>
        int ExpiredKeyDeletionScanFrequencySecs { get; }

        /// <summary>
        /// Frequency for background object collection of expired members (seconds). 0 = disabled.
        /// </summary>
        int ExpiredObjectCollectionFrequencySecs { get; }

        /// <summary>
        /// Throttle the maximum outstanding network sends per session.
        /// </summary>
        int NetworkSendThrottleMax { get; }
    }
}