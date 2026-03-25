// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.server.Auth.Settings;

namespace Garnet.server
{
    /// <summary>
    /// AOT-compatible fallback struct that delegates to a stored GarnetServerOptions instance at runtime.
    /// Unlike the dynamically-emitted structs from GarnetServerFactory, property values here are NOT constants,
    /// so the JIT cannot eliminate branches. However, this struct compiles under NativeAOT where
    /// Reflection.Emit is unavailable.
    /// 
    /// Usage:
    ///   RuntimeServerOptions.Instance = opts;
    ///   using var server = new GarnetServer&lt;RuntimeServerOptions&gt;(opts);
    /// </summary>
    public struct RuntimeServerOptions : IGarnetServerOptions
    {
        /// <summary>
        /// The shared options instance. Must be set before creating a GarnetServer&lt;RuntimeServerOptions&gt;.
        /// </summary>
        public static GarnetServerOptions Instance;

        // ServerOptions booleans
        public bool EnableStorageTier => Instance.EnableStorageTier;
        public bool CopyReadsToTail => Instance.CopyReadsToTail;
        public bool Recover => Instance.Recover;
        public bool DisablePubSub => Instance.DisablePubSub;
        public bool FailOnRecoveryError => Instance.FailOnRecoveryError;

        // GarnetServerOptions booleans
        public bool DisableObjects => Instance.DisableObjects;
        public bool EnableCluster => Instance.EnableCluster;
        public bool CleanClusterConfig => Instance.CleanClusterConfig;
        public bool FastMigrate => Instance.FastMigrate;
        public bool EnableAOF => Instance.EnableAOF;
        public bool EnableLua => Instance.EnableLua;
        public bool LuaTransactionMode => Instance.LuaTransactionMode;
        public bool WaitForCommit => Instance.WaitForCommit;
        public bool CompactionForceDelete => Instance.CompactionForceDelete;
        public bool LatencyMonitor => Instance.LatencyMonitor;
        public bool QuietMode => Instance.QuietMode;
        public bool EnableIncrementalSnapshots => Instance.EnableIncrementalSnapshots;
        public bool UseFoldOverCheckpoints => Instance.UseFoldOverCheckpoints;
        public bool EnableFastCommit => Instance.EnableFastCommit;
        public bool EnableScatterGatherGet => Instance.EnableScatterGatherGet;
        public bool FastAofTruncate => Instance.FastAofTruncate;
        public bool OnDemandCheckpoint => Instance.OnDemandCheckpoint;
        public bool ReplicaDisklessSync => Instance.ReplicaDisklessSync;
        public bool UseAofNullDevice => Instance.UseAofNullDevice;
        public bool UseRevivBinsPowerOf2 => Instance.UseRevivBinsPowerOf2;
        public bool RevivInChainOnly => Instance.RevivInChainOnly;
        public bool ExtensionAllowUnsignedAssemblies => Instance.ExtensionAllowUnsignedAssemblies;
        public bool EnableReadCache => Instance.EnableReadCache;
        public bool ClusterReplicaResumeWithData => Instance.ClusterReplicaResumeWithData;
        public bool AllowMultiDb => Instance.AllowMultiDb;

        // Non-boolean hot-path fields
        public int MetricsSamplingFrequency => Instance.MetricsSamplingFrequency;
        public int SlowLogThreshold => Instance.SlowLogThreshold;
        public int SlowLogMaxEntries => Instance.SlowLogMaxEntries;
        public int ObjectScanCountLimit => Instance.ObjectScanCountLimit;
        public int MaxDatabases => Instance.MaxDatabases;
        public ConnectionProtectionOption EnableDebugCommand => Instance.EnableDebugCommand;
        public ConnectionProtectionOption EnableModuleCommand => Instance.EnableModuleCommand;
        public int CommitFrequencyMs => Instance.CommitFrequencyMs;
        public int CompactionFrequencySecs => Instance.CompactionFrequencySecs;
        public int IndexResizeFrequencySecs => Instance.IndexResizeFrequencySecs;
        public int AdjustedIndexMaxCacheLines => Instance.AdjustedIndexMaxCacheLines;
        public int AdjustedObjectStoreIndexMaxCacheLines => Instance.AdjustedObjectStoreIndexMaxCacheLines;
        public int ExpiredKeyDeletionScanFrequencySecs => Instance.ExpiredKeyDeletionScanFrequencySecs;
        public int ExpiredObjectCollectionFrequencySecs => Instance.ExpiredObjectCollectionFrequencySecs;
        public int NetworkSendThrottleMax => Instance.NetworkSendThrottleMax;
    }
}
