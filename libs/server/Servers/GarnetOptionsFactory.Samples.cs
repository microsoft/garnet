// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// =============================================================================
// SAMPLE: How to use GarnetOptionsFactory for JIT branch elimination
//
// This file is NOT compiled — it illustrates how to use the JIT config inlining
// infrastructure once RespServerSession<TServerOptions> is made generic.
// =============================================================================

#if SAMPLE_CODE_DO_NOT_COMPILE

using Garnet.networking;
using Garnet.server;

namespace Garnet.server.Samples
{
    // =========================================================================
    // APPROACH 1: Type-safe factory (recommended for dynamic/runtime configs)
    //
    // GarnetOptionsFactory emits a zero-size struct at runtime with constant
    // getters matching the GarnetServerOptions values. The user writes a factory
    // subclass where the compiler fully type-checks the constructor call.
    // =========================================================================

    /// <summary>
    /// Factory that creates RespServerSession instances with JIT-optimized config.
    /// Define once, reuse across connections — just update the per-connection fields.
    /// </summary>
    class RespSessionFactory : TypedOptionsFactory<ServerSessionBase>
    {
        public long Id;
        public INetworkSender Sender;
        public StoreWrapper StoreWrapper;
        public SubscribeBroker Broker;

        // The compiler fully type-checks this constructor call.
        // If RespServerSession's constructor signature changes, this fails at build time.
        public override ServerSessionBase Create<TServerOptions>()
            => new RespServerSession<TServerOptions>(Id, Sender, StoreWrapper, Broker, null, true);
    }

    /// <summary>
    /// Example GarnetProvider using the type-safe factory.
    /// </summary>
    class SampleGarnetProvider
    {
        readonly StoreWrapper storeWrapper;
        readonly SubscribeBroker broker;
        readonly RespSessionFactory sessionFactory;
        long lastSessionId;

        public SampleGarnetProvider(StoreWrapper storeWrapper, SubscribeBroker broker)
        {
            this.storeWrapper = storeWrapper;
            this.broker = broker;

            // Create factory once at startup — reused for every connection
            this.sessionFactory = new RespSessionFactory
            {
                StoreWrapper = storeWrapper,
                Broker = broker
            };
        }

        public IMessageConsumer GetSession(INetworkSender networkSender)
        {
            // Update per-connection fields
            sessionFactory.Id = Interlocked.Increment(ref lastSessionId);
            sessionFactory.Sender = networkSender;

            // Factory emits e.g. GarnetOpts_0 with constant getters,
            // then calls sessionFactory.Create<GarnetOpts_0>().
            // The JIT compiles RespServerSession<GarnetOpts_0>.ProcessMessages()
            // with all config branches eliminated.
            return GarnetOptionsFactory.Create(storeWrapper.serverOptions, sessionFactory);
        }
    }

    // =========================================================================
    // APPROACH 2: Hand-written struct (maximum optimization, compile-time known config)
    //
    // When the configuration is known at compile time, write a struct directly.
    // Every property returns a constant — the JIT eliminates all dead branches.
    // No Reflection.Emit needed.
    // =========================================================================

    /// <summary>
    /// Example: a production config with cluster disabled and no metrics.
    /// The JIT eliminates all cluster/metrics/slowlog code paths entirely.
    /// </summary>
    struct ProductionStandaloneOptions : IGarnetServerOptions
    {
        // All false/zero — these code paths are eliminated by the JIT
        public bool EnableCluster => false;
        public bool LatencyMonitor => false;
        public bool EnableScatterGatherGet => false;
        public bool WaitForCommit => false;
        public int MetricsSamplingFrequency => 0;
        public int SlowLogThreshold => 0;

        // Features that are enabled
        public bool EnableAOF => true;
        public bool EnableFastCommit => true;
        public bool EnableLua => true;
        public int MaxDatabases => 16;

        // ... remaining IGarnetServerOptions properties with their constant values
        public bool EnableStorageTier => false;
        public bool CopyReadsToTail => false;
        public bool Recover => false;
        public bool DisablePubSub => false;
        public bool FailOnRecoveryError => false;
        public bool DisableObjects => false;
        public bool CleanClusterConfig => false;
        public bool FastMigrate => false;
        public bool LuaTransactionMode => false;
        public bool CompactionForceDelete => false;
        public bool QuietMode => true;
        public bool EnableIncrementalSnapshots => false;
        public bool UseFoldOverCheckpoints => false;
        public bool FastAofTruncate => false;
        public bool OnDemandCheckpoint => true;
        public bool ReplicaDisklessSync => false;
        public bool UseAofNullDevice => false;
        public bool UseRevivBinsPowerOf2 => false;
        public bool RevivInChainOnly => false;
        public bool ExtensionAllowUnsignedAssemblies => false;
        public bool EnableReadCache => false;
        public bool ClusterReplicaResumeWithData => false;
        public bool AllowMultiDb => true;
        public int SlowLogMaxEntries => 128;
        public int ObjectScanCountLimit => 1000;
        public ConnectionProtectionOption EnableDebugCommand => ConnectionProtectionOption.No;
        public ConnectionProtectionOption EnableModuleCommand => ConnectionProtectionOption.No;
        public int CommitFrequencyMs => 0;
        public int CompactionFrequencySecs => 0;
        public int IndexResizeFrequencySecs => 60;
        public int AdjustedIndexMaxCacheLines => 0;
        public int AdjustedObjectStoreIndexMaxCacheLines => 0;
        public int ExpiredKeyDeletionScanFrequencySecs => -1;
        public int ExpiredObjectCollectionFrequencySecs => 0;
        public int NetworkSendThrottleMax => 8;
    }

    class SampleHandwrittenUsage
    {
        void Example(GarnetServerOptions opts, INetworkSender sender, StoreWrapper wrapper, SubscribeBroker broker)
        {
            // Directly construct with the hand-written struct — no Emit needed.
            // The JIT specializes RespServerSession<ProductionStandaloneOptions>
            // and eliminates all branches where config is false/zero.
            var session = new RespServerSession<ProductionStandaloneOptions>(
                1, sender, wrapper, broker, null, true);
        }
    }

    // =========================================================================
    // APPROACH 3: RuntimeServerOptions (AOT-compatible fallback)
    //
    // Delegates to a GarnetServerOptions instance at runtime.
    // No branch elimination (values aren't constants), but compiles under
    // NativeAOT where Reflection.Emit is unavailable.
    // =========================================================================

    class SampleAotUsage
    {
        void Example(GarnetServerOptions opts, INetworkSender sender, StoreWrapper wrapper, SubscribeBroker broker)
        {
            // Set the shared instance before creating sessions
            RuntimeServerOptions.Instance = opts;

            // Construct with RuntimeServerOptions — works everywhere,
            // but the JIT cannot eliminate branches.
            var session = new RespServerSession<RuntimeServerOptions>(
                1, sender, wrapper, broker, null, true);
        }
    }
}

#endif
