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
        public static bool EnableStorageTier => Instance.EnableStorageTier;
        public static bool CopyReadsToTail => Instance.CopyReadsToTail;
        public static bool Recover => Instance.Recover;
        public static bool DisablePubSub => Instance.DisablePubSub;
        public static bool FailOnRecoveryError => Instance.FailOnRecoveryError;

        // GarnetServerOptions booleans
        public static bool DisableObjects => Instance.DisableObjects;
        public static bool EnableCluster => Instance.EnableCluster;
        public static bool CleanClusterConfig => Instance.CleanClusterConfig;
        public static bool FastMigrate => Instance.FastMigrate;
        public static bool EnableAOF => Instance.EnableAOF;
        public static bool EnableLua => Instance.EnableLua;
        public static bool LuaTransactionMode => Instance.LuaTransactionMode;
        public static bool WaitForCommit => Instance.WaitForCommit;
        public static bool CompactionForceDelete => Instance.CompactionForceDelete;
        public static bool LatencyMonitor => Instance.LatencyMonitor;
        public static bool QuietMode => Instance.QuietMode;
        public static bool EnableIncrementalSnapshots => Instance.EnableIncrementalSnapshots;
        public static bool UseFoldOverCheckpoints => Instance.UseFoldOverCheckpoints;
        public static bool EnableFastCommit => Instance.EnableFastCommit;
        public static bool EnableScatterGatherGet => Instance.EnableScatterGatherGet;
        public static bool FastAofTruncate => Instance.FastAofTruncate;
        public static bool OnDemandCheckpoint => Instance.OnDemandCheckpoint;
        public static bool ReplicaDisklessSync => Instance.ReplicaDisklessSync;
        public static bool UseAofNullDevice => Instance.UseAofNullDevice;
        public static bool UseRevivBinsPowerOf2 => Instance.UseRevivBinsPowerOf2;
        public static bool RevivInChainOnly => Instance.RevivInChainOnly;
        public static bool ExtensionAllowUnsignedAssemblies => Instance.ExtensionAllowUnsignedAssemblies;
        public static bool EnableReadCache => Instance.EnableReadCache;
        public static bool ClusterReplicaResumeWithData => Instance.ClusterReplicaResumeWithData;
        public static bool AllowMultiDb => Instance.AllowMultiDb;

        // Non-boolean hot-path fields
        public static int MetricsSamplingFrequency => Instance.MetricsSamplingFrequency;
        public static int SlowLogThreshold => Instance.SlowLogThreshold;
        public static int SlowLogMaxEntries => Instance.SlowLogMaxEntries;
        public static int ObjectScanCountLimit => Instance.ObjectScanCountLimit;
        public static int MaxDatabases => Instance.MaxDatabases;
        public static ConnectionProtectionOption EnableDebugCommand => Instance.EnableDebugCommand;
        public static ConnectionProtectionOption EnableModuleCommand => Instance.EnableModuleCommand;
        public static int CommitFrequencyMs => Instance.CommitFrequencyMs;
        public static int CompactionFrequencySecs => Instance.CompactionFrequencySecs;
        public static int IndexResizeFrequencySecs => Instance.IndexResizeFrequencySecs;
        public static int AdjustedIndexMaxCacheLines => Instance.AdjustedIndexMaxCacheLines;
        public static int AdjustedObjectStoreIndexMaxCacheLines => Instance.AdjustedObjectStoreIndexMaxCacheLines;
        public static int ExpiredKeyDeletionScanFrequencySecs => Instance.ExpiredKeyDeletionScanFrequencySecs;
        public static int ExpiredObjectCollectionFrequencySecs => Instance.ExpiredObjectCollectionFrequencySecs;
        public static int NetworkSendThrottleMax => Instance.NetworkSendThrottleMax;
    }
}
