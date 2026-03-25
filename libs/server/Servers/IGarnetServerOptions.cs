// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server.Auth.Settings;

namespace Garnet.server
{
    /// <summary>
    /// Interface for Garnet server options with static abstract properties.
    /// When used as a struct type parameter constraint, the JIT specializes code per struct type,
    /// inlines constant-returning properties, and eliminates dead branches entirely.
    /// Non-boolean fields used in hot-path comparisons (e.g., MetricsSamplingFrequency > 0)
    /// also benefit: the JIT constant-folds the comparison and removes the branch.
    /// </summary>
    public interface IGarnetServerOptions
    {
        // ---------------------------------------------------------------
        // ServerOptions (base class) boolean properties
        // ---------------------------------------------------------------

        /// <summary>
        /// Enable tiering of records (hybrid log) to storage.
        /// </summary>
        static abstract bool EnableStorageTier { get; }

        /// <summary>
        /// Copy reads from immutable region or storage device to the tail of the log.
        /// </summary>
        static abstract bool CopyReadsToTail { get; }

        /// <summary>
        /// Recover from latest checkpoint.
        /// </summary>
        static abstract bool Recover { get; }

        /// <summary>
        /// Disable pub/sub feature on server.
        /// </summary>
        static abstract bool DisablePubSub { get; }

        /// <summary>
        /// Fail server bootup on AOF and checkpoint recovery errors.
        /// </summary>
        static abstract bool FailOnRecoveryError { get; }

        // ---------------------------------------------------------------
        // GarnetServerOptions boolean properties
        // ---------------------------------------------------------------

        /// <summary>
        /// Disable support for data structure objects.
        /// </summary>
        static abstract bool DisableObjects { get; }

        /// <summary>
        /// Enable cluster mode.
        /// </summary>
        static abstract bool EnableCluster { get; }

        /// <summary>
        /// Start with clean cluster config.
        /// </summary>
        static abstract bool CleanClusterConfig { get; }

        /// <summary>
        /// Fast slot migration without waiting for ACK.
        /// </summary>
        static abstract bool FastMigrate { get; }

        /// <summary>
        /// Enable append-only file (write ahead log).
        /// </summary>
        static abstract bool EnableAOF { get; }

        /// <summary>
        /// Enable Lua scripts on server.
        /// </summary>
        static abstract bool EnableLua { get; }

        /// <summary>
        /// Run Lua scripts as a transaction (lock-run-unlock).
        /// </summary>
        static abstract bool LuaTransactionMode { get; }

        /// <summary>
        /// Wait for AOF to commit before returning results to client.
        /// </summary>
        static abstract bool WaitForCommit { get; }

        /// <summary>
        /// Forcefully delete inactive segments after compaction.
        /// </summary>
        static abstract bool CompactionForceDelete { get; }

        /// <summary>
        /// Enable per-command latency tracking for all commands.
        /// </summary>
        static abstract bool LatencyMonitor { get; }

        /// <summary>
        /// Disable startup banner and status messages.
        /// </summary>
        static abstract bool QuietMode { get; }

        /// <summary>
        /// Enable incremental snapshots for SAVE and BGSAVE.
        /// </summary>
        static abstract bool EnableIncrementalSnapshots { get; }

        /// <summary>
        /// Use fold-over checkpoints instead of snapshots.
        /// </summary>
        static abstract bool UseFoldOverCheckpoints { get; }

        /// <summary>
        /// Enable FastCommit mode for TsavoriteAof.
        /// </summary>
        static abstract bool EnableFastCommit { get; }

        /// <summary>
        /// Use scatter gather IO for MGET operations.
        /// </summary>
        static abstract bool EnableScatterGatherGet { get; }

        /// <summary>
        /// Truncate AOF as soon as replicas are fed.
        /// </summary>
        static abstract bool FastAofTruncate { get; }

        /// <summary>
        /// Take on-demand checkpoint to avoid missing data when attaching replica.
        /// </summary>
        static abstract bool OnDemandCheckpoint { get; }

        /// <summary>
        /// Enable diskless replication sync.
        /// </summary>
        static abstract bool ReplicaDisklessSync { get; }

        /// <summary>
        /// Use null device for AOF (main-memory replication).
        /// </summary>
        static abstract bool UseAofNullDevice { get; }

        /// <summary>
        /// Use power-of-2-sized bins for revivification.
        /// </summary>
        static abstract bool UseRevivBinsPowerOf2 { get; }

        /// <summary>
        /// Revivify tombstoned records in tag chains only (no free list).
        /// </summary>
        static abstract bool RevivInChainOnly { get; }

        /// <summary>
        /// Allow loading custom commands from digitally unsigned assemblies.
        /// </summary>
        static abstract bool ExtensionAllowUnsignedAssemblies { get; }

        /// <summary>
        /// Enable read cache for on-disk records.
        /// </summary>
        static abstract bool EnableReadCache { get; }

        /// <summary>
        /// Cluster replica loads AOF/Checkpoint data from disk on start.
        /// </summary>
        static abstract bool ClusterReplicaResumeWithData { get; }

        /// <summary>
        /// Allow more than one logical database in server.
        /// </summary>
        static abstract bool AllowMultiDb { get; }

        // ---------------------------------------------------------------
        // Non-boolean fields accessed in hot-path branch conditions.
        // JIT constant-folds comparisons like MetricsSamplingFrequency > 0
        // and eliminates the dead branch entirely.
        // ---------------------------------------------------------------

        /// <summary>
        /// Metrics sampling frequency (0 = disabled). Checked per-command.
        /// </summary>
        static abstract int MetricsSamplingFrequency { get; }

        /// <summary>
        /// Threshold (microseconds) for logging command in the slow log (0 = disabled). Checked per-command.
        /// </summary>
        static abstract int SlowLogThreshold { get; }

        /// <summary>
        /// Maximum number of slow log entries to keep.
        /// </summary>
        static abstract int SlowLogMaxEntries { get; }

        /// <summary>
        /// Limit of items to return in one iteration of *SCAN command.
        /// </summary>
        static abstract int ObjectScanCountLimit { get; }

        /// <summary>
        /// Max number of logical databases allowed.
        /// </summary>
        static abstract int MaxDatabases { get; }

        /// <summary>
        /// Enables the DEBUG command. Checked per-command dispatch.
        /// </summary>
        static abstract ConnectionProtectionOption EnableDebugCommand { get; }

        /// <summary>
        /// Enables the MODULE command. Checked per-command dispatch.
        /// </summary>
        static abstract ConnectionProtectionOption EnableModuleCommand { get; }

        /// <summary>
        /// AOF commit frequency in milliseconds. 0 = per-op, -1 = manual.
        /// </summary>
        static abstract int CommitFrequencyMs { get; }

        /// <summary>
        /// Hybrid log compaction frequency in seconds (0 = disabled).
        /// </summary>
        static abstract int CompactionFrequencySecs { get; }

        /// <summary>
        /// Index resize check frequency in seconds.
        /// </summary>
        static abstract int IndexResizeFrequencySecs { get; }

        /// <summary>
        /// Max size of hash index (cache lines) after rounding. 0 = no max.
        /// </summary>
        static abstract int AdjustedIndexMaxCacheLines { get; }

        /// <summary>
        /// Max size of object store hash index (cache lines) after rounding. 0 = no max.
        /// </summary>
        static abstract int AdjustedObjectStoreIndexMaxCacheLines { get; }

        /// <summary>
        /// Frequency of background scan for expired key deletion (seconds). -1 = disabled.
        /// </summary>
        static abstract int ExpiredKeyDeletionScanFrequencySecs { get; }

        /// <summary>
        /// Frequency for background object collection of expired members (seconds). 0 = disabled.
        /// </summary>
        static abstract int ExpiredObjectCollectionFrequencySecs { get; }

        /// <summary>
        /// Throttle the maximum outstanding network sends per session.
        /// </summary>
        static abstract int NetworkSendThrottleMax { get; }
    }
}