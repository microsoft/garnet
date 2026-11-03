// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.ACL;
using Garnet.server.Auth.Settings;
using Garnet.server.Lua;
using Garnet.server.Metrics;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Wrapper for store and store-specific information
    /// </summary>   
    public sealed partial class StoreWrapper
    {
        internal readonly string version;
        internal readonly string redisProtocolVersion;
        readonly IGarnetServer[] servers;
        internal readonly long startupTimestamp;

        /// <summary>
        /// Default database (DB 0)
        /// </summary>
        public GarnetDatabase DefaultDatabase => databaseManager.DefaultDatabase;

        /// <summary>
        /// Store (of DB 0)
        /// </summary>
        public TsavoriteKV<StoreFunctions, StoreAllocator> store => databaseManager.Store;

        /// <summary>
        /// AOF (of DB 0)
        /// </summary>
        public GarnetAppendOnlyFile appendOnlyFile => databaseManager.AppendOnlyFile;

        /// <summary>
        /// Get total AOF size (i.e. diff TailAddres - BeginAddress)
        /// </summary>
        /// <returns></returns>
        public long AofSize() => appendOnlyFile.Log.TailAddress.AggregateDiff(appendOnlyFile.Log.BeginAddress);

        /// <summary>
        /// AOF BeginAddress
        /// </summary>
        public AofAddress BeginAddress => appendOnlyFile.Log.BeginAddress;

        /// <summary>
        /// AOF TailAddress
        /// </summary>
        public AofAddress TailAddress => appendOnlyFile.Log.TailAddress;

        /// <summary>
        /// Last save time (of DB 0)
        /// </summary>
        public DateTimeOffset lastSaveTime => databaseManager.LastSaveTime;

        /// <summary>
        /// Store size tracker (of DB 0)
        /// </summary>
        public CacheSizeTracker sizeTracker => databaseManager.SizeTracker;

        public IStoreFunctions storeFunctions => store.StoreFunctions;

        /// <summary>
        /// Server options
        /// </summary>
        public readonly GarnetServerOptions serverOptions;

        /// <summary>
        /// Runtime-adjustable configuration values (seeded from <see cref="serverOptions"/> at startup,
        /// updated through CONFIG SET). Reachable across the server and cluster layers.
        /// </summary>
        public readonly RuntimeServerConfig runtimeConfig;

        /// <summary>
        /// Subscribe broker
        /// </summary>
        public readonly SubscribeBroker subscribeBroker;

        /// <summary>
        /// Get servers
        /// </summary>
        public IGarnetServer[] Servers => servers;

        /// <summary>
        /// Access control list governing all commands
        /// </summary>
        public readonly AccessControlList accessControlList;

        /// <summary>
        /// Logger factory
        /// </summary>
        public readonly ILoggerFactory loggerFactory;

        /// <summary>
        /// Object serializer
        /// </summary>
        public readonly GarnetObjectSerializer GarnetObjectSerializer;

        /// <summary>
        /// The main logger instance associated with this store.
        /// </summary>
        public readonly ILogger logger;

        /// <summary>
        /// Lua script cache
        /// </summary>
        public readonly ConcurrentDictionary<ScriptHashKey, LuaScriptHandle> storeScriptCache;

        /// <summary>
        /// Logging frequency
        /// </summary>
        public readonly TimeSpan loggingFrequency;

        /// <summary>
        /// RangeIndex (BfTree) manager shared across sessions
        /// </summary>
        internal readonly RangeIndexManager rangeIndexManager;

        /// <summary>
        /// Definition for delegate creating a new logical database
        /// </summary>
        public delegate GarnetDatabase DatabaseCreatorDelegate(int dbId);

        /// <summary>
        /// Number of active databases
        /// </summary>
        public int DatabaseCount => databaseManager.DatabaseCount;

        /// <summary>
        /// Current max database ID
        /// </summary>
        public int MaxDatabaseId => databaseManager.MaxDatabaseId;

        /// <summary>
        /// Shared timeout manager for all <see cref="LuaRunner"/> across all sessions.
        /// </summary>
        internal readonly LuaTimeoutManager luaTimeoutManager;

        private IDatabaseManager databaseManager;
        SingleWriterMultiReaderLock databaseManagerLock;

        internal readonly CollectionItemBroker itemBroker;
        internal readonly CustomCommandManager customCommandManager;
        internal readonly GarnetServerMonitor monitor;
        internal readonly IClusterProvider clusterProvider;

        /// <summary>
        /// State describing this node's role when running under standalone Sentinel
        /// control plane (<see cref="GarnetServerOptions.EnableStandaloneReplication"/>).
        /// <c>null</c> when the feature is disabled, or when this node is in cluster mode.
        ///
        /// <para>The state is intentionally tiny: just enough for INFO replication,
        /// ROLE, and the standalone <c>REPLICAOF</c> command to surface the right
        /// picture to Sentinel and to clients. It is updated by the
        /// <c>REPLICAOF host port</c> / <c>REPLICAOF NO ONE</c> command handlers
        /// on this node.</para>
        /// </summary>
        internal readonly LocalReplicationState LocalReplicationState;

        /// <summary>
        /// Outbound replication client used when this node is a standalone replica under
        /// the Sentinel control plane. Lazily constructed on first
        /// <c>REPLICAOF host port</c>; <c>null</c> in cluster mode or when the option
        /// is disabled.
        /// </summary>
        internal StandaloneReplicaClient StandaloneReplicaClient;

        /// <summary>
        /// Registry of stock RESP replicas attached to this node as primary, used to
        /// populate the <c>connected_slaves</c> / <c>slave&lt;N&gt;</c> fields of
        /// <c>INFO replication</c> outside cluster mode. Sentinel discovers a primary's
        /// replicas exclusively from those fields, so without this registry an
        /// orchestrator sees zero replicas and can never select one to promote.
        /// </summary>
        internal readonly ReplicaRegistry replicaRegistry = new();

        /// <summary>
        /// Stable replication ID for this node in standalone mode, minted once on first
        /// use and then held for the lifetime of the process.
        ///
        /// <para>Stability matters: Redis reports the same <c>master_replid</c> across
        /// successive INFO calls, and an orchestrator correlates a primary's identity
        /// with it. Minting a fresh value per INFO call (as an earlier revision did via
        /// <c>Generator.DefaultHexId()</c>) makes the node look like a different primary
        /// every time it is polled.</para>
        /// </summary>
        string primaryReplId;

        /// <summary>
        /// Returns the node's stable replication ID, minting one on first call.
        /// </summary>
        public string GetOrCreatePrimaryReplId()
        {
            if (!serverOptions.EnableCluster)
                return runId;

            if (primaryReplId == null)
            {
                lock (replicaRegistry)
                {
                    // CreateHexId, not DefaultHexId: the latter returns an all-zero
                    // placeholder. A real primary advertises a random 40-char replication
                    // ID, and clients/orchestrators may treat an all-zero ID as
                    // "no replication history" and force a full resync.
                    primaryReplId ??= Generator.CreateHexId();
                }
            }

            return primaryReplId;
        }

        /// <summary>
        /// Replication offset reported to clients in standalone mode.
        ///
        /// <para>Redis exposes a monotonically increasing byte count here, and clients and
        /// orchestrators parse it as an integer, so an earlier revision's literal "N/A"
        /// was a bug. When an append-only file is configured this reports the AOF tail
        /// offset, which advances as writes are appended.</para>
        ///
        /// <para><b>Known limitation:</b> with AOF disabled (the default) there is no
        /// replication stream to measure, so this reports 0. A real primary reports 0 in
        /// the same situation (a freshly started primary with no writes), and Redis
        /// replicas likewise report offset 0 while idle, so the value is not misleading
        /// — but it does not track writes until a replication stream exists. Phase 2's
        /// outbound stream will make this a true byte offset.</para>
        /// </summary>
        public long PrimaryReplOffset
        {
            get
            {
                if (appendOnlyFile != null)
                {
                    try
                    {
                        return appendOnlyFile.Log.TailAddress.AggregateDiff(appendOnlyFile.Log.BeginAddress);
                    }
                    catch
                    {
                        return 0;
                    }
                }

                return 0;
            }
        }
        internal readonly SlowLogContainer slowLogContainer;
        internal readonly ILogger sessionLogger;
        internal long safeAofAddress = -1;

        private readonly bool enforceConsistentRead;

        // Standalone instance node_id
        internal readonly string runId;

        /// <summary>
        /// Run ID identifies instance history when taking a checkpoint.
        /// </summary>
        public string RunId => serverOptions.EnableCluster ? clusterProvider.GetRunId() : runId;

        internal readonly CancellationTokenSource ctsCommit;

        /// <summary>
        /// True if StoreWrapper instance is disposed
        /// </summary>
        bool disposed;

        /// <summary>
        /// Garnet checkpoint manager
        /// </summary>
        public GarnetCheckpointManager StoreCheckpointManager => (GarnetCheckpointManager)store?.CheckpointManager;

        /// <summary>
        /// Constructor
        /// </summary>
        public StoreWrapper(
            string version,
            string redisProtocolVersion,
            IGarnetServer[] servers,
            CustomCommandManager customCommandManager,
            GarnetServerOptions serverOptions,
            SubscribeBroker subscribeBroker,
            AccessControlList accessControlList = null,
            DatabaseCreatorDelegate createDatabaseDelegate = null,
            IDatabaseManager databaseManager = null,
            IClusterFactory clusterFactory = null,
            ILoggerFactory loggerFactory = null,
            RuntimeServerConfig runtimeConfig = null)
        {
            this.version = version;
            this.redisProtocolVersion = redisProtocolVersion;
            this.servers = servers;
            this.startupTimestamp = Stopwatch.GetTimestamp();
            this.serverOptions = serverOptions;
            // Share the runtime config table across cloned wrappers (e.g. the AOF copy ctor) so that a
            // CONFIG SET is observed consistently; otherwise seed a fresh table from the startup options.
            // A freshly created table is bound to this wrapper as its owner so update actions can enact
            // task lifecycle changes; a clone reuses the shared table (and its original owner).
            this.runtimeConfig = runtimeConfig ?? new RuntimeServerConfig(serverOptions, this);
            this.subscribeBroker = subscribeBroker;
            this.customCommandManager = customCommandManager;
            this.loggerFactory = loggerFactory;
            this.databaseManager = databaseManager ?? DatabaseManagerFactory.CreateDatabaseManager(serverOptions, createDatabaseDelegate, this);
            // Standalone replication under Sentinel is opt-in. Even in cluster mode this
            // is allocated (to a no-op state) so command handlers don't have to null-check
            // before reading it; the actual role transitions only fire when the option is
            // enabled AND we are not in cluster mode.
            this.LocalReplicationState = new LocalReplicationState();
            this.monitor = serverOptions.MetricsSamplingFrequency > 0 || serverOptions.CommandStatsMonitor || serverOptions.LatencyMonitor
                ? new GarnetServerMonitor(this, serverOptions, servers,
                    loggerFactory?.CreateLogger("GarnetServerMonitor"))
                : null;
            this.enforceConsistentRead = serverOptions.EnableCluster && serverOptions.EnableAOF && serverOptions.MultiLogEnabled;
            this.logger = loggerFactory?.CreateLogger("StoreWrapper");
            this.sessionLogger = loggerFactory?.CreateLogger("Session");
            this.accessControlList = accessControlList;
            this.GarnetObjectSerializer = new GarnetObjectSerializer(this.customCommandManager);
            this.taskManager = new TaskManager(loggerFactory?.CreateLogger("TaskManager"));
            this.loggingFrequency = TimeSpan.FromSeconds(serverOptions.LoggingFrequency);
            this.rangeIndexManager = DefaultDatabase.RangeIndexManager;

            logger?.LogTrace("StoreWrapper logging frequency: {loggingFrequency} seconds.", this.loggingFrequency);

            // The slow log threshold is runtime-adjustable, so the container is always allocated: a session
            // may start recording after a CONFIG SET even when the startup threshold disabled the slow log.
            this.slowLogContainer = new SlowLogContainer(serverOptions.SlowLogMaxEntries);

            if (!serverOptions.DisableObjects)
                this.itemBroker = new CollectionItemBroker();

            // Initialize store scripting cache
            if (serverOptions.EnableLua)
            {
                this.storeScriptCache = [];

                if (!LuaRunner.TryProbeSupport(out var errorMessage))
                {
                    logger?.LogCritical("Lua probe failed, Lua scripts will fail {errorMessage}", errorMessage);
                }

                if (serverOptions.LuaOptions.Timeout != Timeout.InfiniteTimeSpan)
                {
                    this.luaTimeoutManager = new(serverOptions.LuaOptions.Timeout, loggerFactory?.CreateLogger<LuaTimeoutManager>());
                }
            }

            if (accessControlList == null)
            {
                // If ACL authentication is enabled, initiate access control list
                // NOTE: This is a temporary workflow. ACL should always be initiated and authenticator
                //       should become a parameter of AccessControlList.
                if ((this.serverOptions.AuthSettings != null) && (this.serverOptions.AuthSettings.GetType().BaseType ==
                                                                  typeof(AclAuthenticationSettings)))
                {
                    // Create a new access control list and register it with the authentication settings
                    var aclAuthenticationSettings =
                        (AclAuthenticationSettings)this.serverOptions.AuthSettings;

                    if (!string.IsNullOrEmpty(aclAuthenticationSettings.AclConfigurationFile))
                    {
                        logger?.LogInformation("Reading ACL configuration file '{filepath}'",
                            aclAuthenticationSettings.AclConfigurationFile);
                        this.accessControlList = new AccessControlList(aclAuthenticationSettings.DefaultPassword,
                            aclAuthenticationSettings.AclConfigurationFile);
                    }
                    else
                    {
                        // If no configuration file is specified, initiate ACL with default settings
                        this.accessControlList = new AccessControlList(aclAuthenticationSettings.DefaultPassword);
                    }
                }
                else
                {
                    this.accessControlList = new AccessControlList();
                }
            }

            if (clusterFactory != null)
                clusterProvider = clusterFactory.CreateClusterProvider(this, rangeIndexManager);
            ctsCommit = new();

            if (!serverOptions.EnableCluster)
            {
                runId = Generator.CreateHexId();
                if (StoreCheckpointManager != null)
                {
                    StoreCheckpointManager.CurrentHistoryId = runId;
                }
            }
        }

        /// <summary>
        /// Copy Constructor
        /// </summary>
        /// <param name="storeWrapper">Source instance</param>
        /// <param name="recordToAof">Enable AOF in database manager</param>
        public StoreWrapper(StoreWrapper storeWrapper, bool recordToAof) : this(storeWrapper.version,
            storeWrapper.redisProtocolVersion,
            storeWrapper.servers,
            storeWrapper.customCommandManager,
            storeWrapper.serverOptions,
            storeWrapper.subscribeBroker,
            storeWrapper.accessControlList,
            databaseManager: storeWrapper.databaseManager.Clone(recordToAof),
            clusterFactory: null,
            loggerFactory: storeWrapper.loggerFactory,
            runtimeConfig: storeWrapper.runtimeConfig)
        {
        }

        /// <summary>
        /// Get the peer endpoint without changing listener bindings.
        /// </summary>
        public IPEndPoint GetClusterPeerEndpoint()
        {
            if (serverOptions.ClusterPort is < 1 or > 65535)
                throw new GarnetException("Cluster peer port must be between 1 and 65535.");

            var advertisedEndpoint = GetClusterEndpoint();
            var hasAddressOverride = !string.IsNullOrEmpty(serverOptions.ClusterAddress);
            var address = hasAddressOverride ? IPAddress.Parse(serverOptions.ClusterAddress) : advertisedEndpoint.Address;
            var port = serverOptions.ClusterPort ?? advertisedEndpoint.Port;
            if (hasAddressOverride && (address.Equals(IPAddress.Any) || address.Equals(IPAddress.IPv6Any)))
                throw new GarnetException("Cluster peer address must be a concrete IP address.");
            return new IPEndPoint(address, port);
        }

        /// <summary>
        /// Get the client-advertised endpoint.
        /// </summary>
        /// <returns></returns>
        public IPEndPoint GetClusterEndpoint()
        {
            IPEndPoint localEndPoint = null;
            if (serverOptions.ClusterAnnounceEndpoint == null)
            {
                foreach (var server in servers)
                {
                    if (server is GarnetServerTcp tcpServer && tcpServer.EndPoint is IPEndPoint point)
                    {
                        localEndPoint = point;
                        break;
                    }
                }
            }
            else
            {
                if (serverOptions.ClusterAnnounceEndpoint is IPEndPoint point)
                    localEndPoint = point;
            }

            // Fail if we cannot advertise an endpoint for remote nodes to connect to
            if (localEndPoint == null)
                throw new GarnetException("Cluster mode requires definition of at least one TCP socket through either the --bind or --cluster-announce-ip options!");

            if (localEndPoint.Address.Equals(IPAddress.Any))
            {
                using (Socket socket = new(AddressFamily.InterNetwork, SocketType.Dgram, 0))
                {
                    socket.Connect("8.8.8.8", 65530);
                    var endPoint = socket.LocalEndPoint as IPEndPoint;
                    return new IPEndPoint(endPoint.Address, localEndPoint.Port);
                }
            }
            else if (localEndPoint.Address.Equals(IPAddress.IPv6Any))
            {
                using (Socket socket = new(AddressFamily.InterNetworkV6, SocketType.Dgram, 0))
                {
                    socket.Connect("2001:4860:4860::8888", 65530);
                    var endPoint = socket.LocalEndPoint as IPEndPoint;
                    return new IPEndPoint(endPoint.Address, localEndPoint.Port);
                }
            }
            return localEndPoint;
        }

        internal async ValueTask RecoverAsync()
        {
            if (serverOptions.EnableCluster)
            {
                if (serverOptions.Recover)
                {
                    await clusterProvider.RecoverAsync().ConfigureAwait(false);
                }

                databaseManager.RecoverVectorSets();
            }
            else
            {
                if (serverOptions.Recover)
                {
                    await RecoverCheckpointAsync().ConfigureAwait(false);

                    databaseManager.RecoverVectorSets();

                    await RecoverAOFAsync().ConfigureAwait(false);
                    databaseManager.VerifyRecoveryIsComplete();
                    _ = ReplayAOF(AofAddress.Create(length: serverOptions.AofPhysicalSublogCount, value: -1));
                }
                else
                {
                    databaseManager.RecoverVectorSets();
                }
            }
        }

        /// <summary>
        /// Take checkpoint of all active databases (or a specified database)
        /// </summary>
        /// <param name="background">True if method can return before checkpoint is taken</param>
        /// <param name="dbId">ID of database to checkpoint, or -1 (default) to checkpoint all active databases</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="logger">Logger</param>
        /// <returns>
        /// <see cref="CheckpointStatus.AlreadyInProgress"/> if another checkpointing process is already in progress,
        /// <see cref="CheckpointStatus.Failed"/> if a foreground checkpoint did not complete, otherwise
        /// <see cref="CheckpointStatus.Success"/>
        /// </returns>
        public Task<CheckpointStatus> TakeCheckpointAsync(bool background, int dbId = -1, CancellationToken token = default, ILogger logger = null)
        {
            if (dbId > 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.TakeCheckpointAsync)} with DB ID: {dbId}");

            return databaseManager.TakeCheckpointAsync(background, dbId, token, logger);
        }

        /// <summary>
        /// Take a checkpoint if no checkpoint was taken after the provided time offset
        /// </summary>
        /// <param name="entryTime"></param>
        /// <param name="dbId"></param>
        /// <returns></returns>
        public async Task TakeOnDemandCheckpointAsync(DateTimeOffset entryTime, int dbId = 0)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.TakeOnDemandCheckpointAsync)} with DB ID: {dbId}");

            await databaseManager.TakeOnDemandCheckpointAsync(entryTime, dbId).ConfigureAwait(false);
        }

        /// <summary>
        /// Recover checkpoint
        /// </summary>
        public async ValueTask RecoverCheckpointAsync(bool replicaRecover = false, bool recoverFromToken = false, CheckpointMetadata metadata = null)
        {
            // Do NOT start the background size-tracker resizer here. It must not independently shift head / evict / free pages
            // concurrently with recovery, which owns the log: recovery does its own budget-aware eviction and directly resets
            // log addresses (RecoveryReset). Racing the resizer against that can lose a page read/flush completion and hang the
            // main recovery thread in WaitRead/WaitFlush. The size tracker object still tracks size during recovery. The resizer
            // is started once checkpoint recovery is done: in ReplayAOF, which is ordinary store traffic and must be budget-managed,
            // and otherwise in StoreWrapper.Start().
            await databaseManager.RecoverCheckpointAsync(replicaRecover, recoverFromToken, metadata).ConfigureAwait(false);
        }

        /// <summary>
        /// Mark the beginning of a checkpoint by taking and a lock to avoid concurrent checkpointing
        /// </summary>
        /// <param name="dbId">ID of database to lock</param>
        /// <returns>True if lock acquired</returns>
        public bool TryPauseCheckpoints(int dbId = 0)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.TryPauseCheckpoints)} with DB ID: {dbId}");

            return databaseManager.TryPauseCheckpoints(dbId);
        }

        /// <summary>
        /// Release checkpoint task lock
        /// </summary>
        /// <param name="dbId">ID of database to unlock</param>
        public void ResumeCheckpoints(int dbId = 0)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.ResumeCheckpoints)} with DB ID: {dbId}");

            databaseManager.ResumeCheckpoints(dbId);
        }

        /// <summary>
        /// Recover AOF
        /// </summary>
        public ValueTask RecoverAOFAsync() => databaseManager.RecoverAOFAsync();

        /// <summary>
        /// Verify that the state recovered from checkpoint and AOF can reconstruct everything that was durably
        /// acknowledged before shutdown. Reports an incomplete recovery as an error, and throws when the server
        /// is configured to fail on recovery errors.
        /// </summary>
        /// <param name="canBeRepairedBySync">True if a full sync from a primary will reconcile this node, in which
        /// case an incomplete local recovery is reported but is not fatal</param>
        public void VerifyRecoveryIsComplete(bool canBeRepairedBySync = false)
            => databaseManager.VerifyRecoveryIsComplete(canBeRepairedBySync);

        /// <summary>
        /// Main-log heap size sampled at the end of AOF replay, or -1 if no replay has run.
        /// </summary>
        /// <remarks>
        /// Replay is memory-budget-managed (see <see cref="ReplayAOF"/>), and this records how much heap it actually held.
        /// Sampling after startup cannot show that: the resizer keeps trimming once the server is up, so an unmanaged replay
        /// and a managed one converge to the same floor despite very different peaks.
        /// </remarks>
        public long AofReplayEndHeapSizeBytes { get; private set; } = -1;

        /// <summary>
        /// When replaying AOF we do not want to write AOF records again.
        /// </summary>
        public AofAddress ReplayAOF(AofAddress untilAddress)
        {
            // AOF replay is ordinary store traffic, so it must be memory-budget-managed like normal operation. Checkpoint
            // recovery runs before this and cannot tolerate a concurrent resizer, so this is the earliest point at which the
            // size trackers can be started. Starting is idempotent; Start() calls this again for the non-recovery path.
            StartSizeTrackers();

            var replayedUntil = databaseManager.ReplayAOF(untilAddress);
            AofReplayEndHeapSizeBytes = store.Log.HeapSizeBytes;
            return replayedUntil;
        }

        /// <summary>
        /// Append a checkpoint commit to the AOF
        /// </summary>
        /// <param name="entryType"></param>
        /// <param name="version"></param>
        /// <param name="dbId"></param>
        public void EnqueueCommit(AofEntryType entryType, long version, int dbId = 0)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.EnqueueCommit)} with DB ID: {dbId}");

            this.databaseManager.EnqueueCommit(entryType, version, dbId);
        }

        /// <summary>
        /// Asynchronously wait for commits from all active databases
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>false if commit is skipped for config reasons</returns>
        internal async ValueTask<bool> WaitForCommitAsync(CancellationToken token = default)
        {
            if (!serverOptions.EnableAOF) return false;

            await databaseManager.WaitForCommitToAofAsync(token).ConfigureAwait(false);
            return true;
        }

        /// <summary>
        /// Asynchronously wait for AOF commits on all active databases,
        /// unless specific database ID specified (by default: -1 = all)
        /// </summary>
        /// <param name="dbId">Specific database ID to commit AOF for (optional)</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>false if commit is skipped for config reasons</returns>
        internal async ValueTask<bool> CommitAOFAsync(int dbId = -1, CancellationToken token = default)
        {
            if (!serverOptions.EnableAOF) return false;

            if (dbId == -1)
            {
                await databaseManager.CommitToAofAsync(token, logger).ConfigureAwait(false);
                return true;
            }

            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.CommitToAofAsync)} with DB ID: {dbId}");

            await databaseManager.CommitToAofAsync(dbId, token).ConfigureAwait(false);
            return true;
        }

        /// <summary>
        /// Create database functions state
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <param name="respProtocolVersion">Resp protocol version</param>
        /// <returns>Functions state</returns>
        /// <exception cref="GarnetException"></exception>
        internal FunctionsState CreateFunctionsState(int dbId = 0, byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.CreateFunctionsState)} with DB ID: {dbId}");

            return databaseManager.CreateFunctionsState(dbId, respProtocolVersion);
        }

        /// <summary>
        /// Reset
        /// </summary>
        /// <param name="dbId">Database ID</param>
        public void Reset(int dbId = 0)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.Reset)} with DB ID: {dbId}");

            databaseManager.Reset(dbId);
        }

        /// <summary>
        /// Get a snapshot of all active databases
        /// </summary>
        /// <returns>Array of active databases</returns>
        public GarnetDatabase[] GetDatabasesSnapshot() => databaseManager.GetDatabasesSnapshot();

        /// <summary>
        /// Get database DB ID
        /// </summary>
        /// <param name="dbId">DB Id</param>
        /// <param name="database">Retrieved database</param>
        /// <returns>True if database was found</returns>
        public bool TryGetDatabase(int dbId, out GarnetDatabase database)
        {

            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
            {
                database = default;
                return false;
            }

            database = databaseManager.TryGetDatabase(dbId, out var success);
            return success;
        }

        /// <summary>
        /// Try to get or add a new database
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <param name="database">Retrieved or added database</param>
        /// <param name="added">True if database was added</param>
        /// <returns>True if database was found or added</returns>
        public bool TryGetOrAddDatabase(int dbId, out GarnetDatabase database, out bool added)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
            {
                database = default;
                added = false;
                return false;
            }

            database = databaseManager.TryGetOrAddDatabase(dbId, out var success, out added);

            database.VectorManager?.Initialize();

            return success;
        }

        /// <summary>
        /// Flush database with specified ID
        /// </summary>
        /// <param name="unsafeTruncateLog">Truncate log</param>
        /// <param name="dbId">Database ID</param>
        public void FlushDatabase(bool unsafeTruncateLog, int dbId = 0)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.FlushDatabase)} with DB ID: {dbId}");

            databaseManager.FlushDatabase(unsafeTruncateLog, dbId);
        }

        /// <summary>
        /// Flush all active databases 
        /// </summary>
        /// <param name="unsafeTruncateLog">Truncate log</param>
        public void FlushAllDatabases(bool unsafeTruncateLog)
        {
            databaseManager.FlushAllDatabases(unsafeTruncateLog);
        }

        /// <summary>
        /// Try to swap between two database instances
        /// </summary>
        /// <param name="dbId1">First database ID</param>
        /// <param name="dbId2">Second database ID</param>
        /// <returns>True if swap successful</returns>
        public bool TrySwapDatabases(int dbId1, int dbId2)
        {
            if (databaseManager is SingleDatabaseManager) return false;

            return this.databaseManager.TrySwapDatabases(dbId1, dbId2, ctsCommit.Token);
        }

        /// <summary>
        /// Resets the revivification stats.
        /// </summary>
        public void ResetRevivificationStats() => databaseManager.ResetRevivificationStats();

        /// <summary>
        /// Expired key deletion scan for a specific database ID.
        /// </summary>
        /// <param name="dbId"></param>
        /// <returns></returns>
        public (long numExpiredKeysFound, long totalRecordsScanned) ExpiredKeyDeletionScan(int dbId)
            => databaseManager.ExpiredKeyDeletionScan(dbId);

        /// <summary>
        /// Count live keys and live keys with an expiration set for a specific database ID.
        /// Used to populate the <c>INFO KEYSPACE</c> section.
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <returns>A tuple of (live keys, live keys with an expiration set).</returns>
        public (long keyCount, long expireCount) GetKeyspaceStats(int dbId)
            => databaseManager.GetKeyspaceStats(dbId);

        public (HybridLogScanMetrics, HybridLogScanMetrics)[] HybridLogDistributionScan() => databaseManager.CollectHybridLogStats();

        internal void Start()
        {
            monitor?.Start();
            clusterProvider?.Start();
            luaTimeoutManager?.Start();

            StartBackgroundTasks();

            StartSizeTrackers();    // We may have already started this for AOF replay during recovery.
        }

        private void StartSizeTrackers() => databaseManager.StartSizeTrackers(ctsCommit.Token);

        /// <summary>
        /// Cached callback for <see cref="HasKeysInSlots"/>. Allocated once per StoreWrapper, reused
        /// across calls (cluster control-plane operations are serialised by the cluster manager).
        /// </summary>
        private StorageSession.ArrayKeyIterationFunctions.HasKeysInSlotsScan hasKeysInSlotsFuncs;

        public bool HasKeysInSlots(List<int> slots)
        {
            if (slots.Count == 0) return false;

            // Single lookup-based push scan over the unified context. Since the migration to a
            // single store, the unified context surfaces every record (string + object) so one
            // scan suffices. No tempKv is allocated. IterateLookupSnapshot pins both untilAddress
            // and maxAddress to a single captured TailAddress so the scan is a consistent
            // point-in-time view (records RCU'd above the snapshot don't suppress in-range ones).
            var functionsState = databaseManager.CreateFunctionsState();
            var unifiedFunctions = new UnifiedSessionFunctions(functionsState);
            using var unifiedSession = store.NewSession<FixedSpanByteKey, UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions>(unifiedFunctions);

            hasKeysInSlotsFuncs ??= new StorageSession.ArrayKeyIterationFunctions.HasKeysInSlotsScan();
            hasKeysInSlotsFuncs.Initialize(slots);

            _ = unifiedSession.IterateLookupSnapshot(ref hasKeysInSlotsFuncs);
            return hasKeysInSlotsFuncs.Found;
        }

        /// <summary>
        /// Check if database manager supports multiple databases. 
        /// If not - try to swap it with a new MultiDatabaseManager.
        /// </summary>
        /// <returns>True if database manager supports multiple databases</returns>
        private bool CheckMultiDatabaseCompatibility()
        {
            if (!serverOptions.AllowMultiDb)
                return false;

            if (databaseManager is MultiDatabaseManager)
                return true;

            databaseManagerLock.WriteLock();
            try
            {
                if (databaseManager is SingleDatabaseManager singleDatabaseManager)
                    databaseManager = new MultiDatabaseManager(singleDatabaseManager);

                return true;
            }
            finally
            {
                databaseManagerLock.WriteUnlock();
            }
        }

        /// <summary>
        /// Check whether to perform consistent read
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool EnforceConsistentRead()
            => enforceConsistentRead && clusterProvider.IsReplica();

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            if (disposed)
                return;
            disposed = true;

            clusterProvider?.Dispose();
            itemBroker?.Dispose();
            monitor?.Dispose();
            luaTimeoutManager?.Dispose();
            ctsCommit?.Cancel();
            taskManager.Dispose();
            checkpointFrequencyChanged.Dispose();
            rangeIndexManager?.Dispose();
            StandaloneReplicaClient?.Dispose();
            databaseManager.Dispose();

            ctsCommit?.Dispose();
        }

        /// <summary>
        /// Push a runtime <c>aof-sync-max-lag-bytes</c> change into the always-constructed primary-side
        /// <see cref="AofBackpressure"/> of every database, so a CONFIG SET retunes (or enables/disables)
        /// the live gate without a restart. Best-effort: the gate stores the budget in plain fields, so the
        /// new value propagates to appenders by cache coherence rather than a fence on the append hot path.
        /// </summary>
        /// <param name="newValueBytes">New whole-log lag budget in bytes (&lt;= 0 disables the gate).</param>
        internal void ApplyAofSyncMaxLagBytes(long newValueBytes)
        {
            foreach (var db in GetDatabasesSnapshot())
                db?.AppendOnlyFile?.backpressure?.SetBudget(newValueBytes);
        }

    }
}