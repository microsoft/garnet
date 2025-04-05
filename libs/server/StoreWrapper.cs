// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.ACL;
using Garnet.server.Auth.Settings;
using Garnet.server.Lua;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Wrapper for store and store-specific information
    /// </summary>   
    public sealed class StoreWrapper
    {
        internal readonly string version;
        internal readonly string redisProtocolVersion;
        readonly IGarnetServer[] servers;
        internal readonly long startupTime;

        /// <summary>
        /// Default database (DB 0)
        /// </summary>
        public GarnetDatabase DefaultDatabase => databaseManager.DefaultDatabase;

        /// <summary>
        /// Store (of DB 0)
        /// </summary>
        public TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> store => databaseManager.MainStore;

        /// <summary>
        /// Object store (of DB 0)
        /// </summary>
        public TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore => databaseManager.ObjectStore;

        /// <summary>
        /// AOF (of DB 0)
        /// </summary>
        public TsavoriteLog appendOnlyFile => databaseManager.AppendOnlyFile;

        /// <summary>
        /// Last save time (of DB 0)
        /// </summary>
        public DateTimeOffset lastSaveTime => databaseManager.LastSaveTime;

        /// <summary>
        /// Object store size tracker (of DB 0)
        /// </summary>
        public CacheSizeTracker objectStoreSizeTracker => databaseManager.ObjectStoreSizeTracker;

        /// <summary>
        /// Server options
        /// </summary>
        public readonly GarnetServerOptions serverOptions;

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
        public readonly ConcurrentDictionary<ScriptHashKey, byte[]> storeScriptCache;

        /// <summary>
        /// Logging frequency
        /// </summary>
        public readonly TimeSpan loggingFrequency;

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
        internal readonly SlowLogContainer slowLogContainer;
        internal readonly ILogger sessionLogger;
        internal long safeAofAddress = -1;

        // Standalone instance node_id
        internal readonly string runId;

        internal readonly CancellationTokenSource ctsCommit;

        // True if StoreWrapper instance is disposed
        bool disposed;

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
            ILoggerFactory loggerFactory = null)
        {
            this.version = version;
            this.redisProtocolVersion = redisProtocolVersion;
            this.servers = servers;
            this.startupTime = DateTimeOffset.UtcNow.Ticks;
            this.serverOptions = serverOptions;
            this.subscribeBroker = subscribeBroker;
            this.customCommandManager = customCommandManager;
            this.databaseManager = databaseManager ?? DatabaseManagerFactory.CreateDatabaseManager(serverOptions, createDatabaseDelegate, this);
            this.monitor = serverOptions.MetricsSamplingFrequency > 0
                ? new GarnetServerMonitor(this, serverOptions, servers,
                    loggerFactory?.CreateLogger("GarnetServerMonitor"))
                : null;
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory?.CreateLogger("StoreWrapper");
            this.sessionLogger = loggerFactory?.CreateLogger("Session");
            this.accessControlList = accessControlList;
            this.GarnetObjectSerializer = new GarnetObjectSerializer(this.customCommandManager);
            this.loggingFrequency = TimeSpan.FromSeconds(serverOptions.LoggingFrequency);

            if (serverOptions.SlowLogThreshold > 0)
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
                clusterProvider = clusterFactory.CreateClusterProvider(this);
            ctsCommit = new();
            runId = Generator.CreateHexId();
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
            loggerFactory: storeWrapper.loggerFactory)
        {
        }

        /// <summary>
        /// Get IP
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
                    return endPoint;
                }
            }
            else if (localEndPoint.Address.Equals(IPAddress.IPv6Any))
            {
                using (Socket socket = new(AddressFamily.InterNetworkV6, SocketType.Dgram, 0))
                {
                    socket.Connect("2001:4860:4860::8888", 65530);
                    var endPoint = socket.LocalEndPoint as IPEndPoint;
                    return endPoint;
                }
            }
            return localEndPoint;
        }

        internal void Recover()
        {
            if (serverOptions.EnableCluster)
            {
                if (serverOptions.Recover)
                {
                    clusterProvider.Recover();
                }
            }
            else
            {
                if (serverOptions.Recover)
                {
                    RecoverCheckpoint();
                    RecoverAOF();
                    ReplayAOF();
                }
            }
        }

        /// <summary>
        /// Take checkpoint of all active databases
        /// </summary>
        /// <param name="background">True if method can return before checkpoint is taken</param>
        /// <param name="logger">Logger</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>False if another checkpointing process is already in progress</returns>
        public bool TakeCheckpoint(bool background, ILogger logger = null,
            CancellationToken token = default) => databaseManager.TakeCheckpoint(background, logger, token);

        /// <summary>
        /// Take checkpoint of all active database IDs or a specified database ID
        /// </summary>
        /// <param name="background">True if method can return before checkpoint is taken</param>
        /// <param name="dbId">ID of database to checkpoint (default: -1 - checkpoint all active databases)</param>
        /// <param name="logger">Logger</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>False if another checkpointing process is already in progress</returns>
        public bool TakeCheckpoint(bool background, int dbId = -1, ILogger logger = null, CancellationToken token = default)
        {
            if (dbId == -1)
            {
                return databaseManager.TakeCheckpoint(background, logger, token);
            }

            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.TakeCheckpoint)} with DB ID: {dbId}");

            return databaseManager.TakeCheckpoint(background, dbId, logger, token);
        }

        /// <summary>
        /// Take a checkpoint if no checkpoint was taken after the provided time offset
        /// </summary>
        /// <param name="entryTime"></param>
        /// <param name="dbId"></param>
        /// <returns></returns>
        public async Task TakeOnDemandCheckpoint(DateTimeOffset entryTime, int dbId = 0)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.TakeOnDemandCheckpointAsync)} with DB ID: {dbId}");

            await databaseManager.TakeOnDemandCheckpointAsync(entryTime, dbId);
        }

        /// <summary>
        /// Recover checkpoint
        /// </summary>
        public void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false,
            bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null)
            => databaseManager.RecoverCheckpoint(replicaRecover, recoverMainStoreFromToken, recoverObjectStoreFromToken, metadata);

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
        public void RecoverAOF() => databaseManager.RecoverAOF();

        /// <summary>
        /// When replaying AOF we do not want to write AOF records again.
        /// </summary>
        public long ReplayAOF(long untilAddress = -1) => this.databaseManager.ReplayAOF();

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
        /// Commit AOF for all active databases
        /// </summary>
        /// <param name="spinWait">True if should wait until all commits complete</param>
        internal void CommitAOF(bool spinWait)
        {
            if (!serverOptions.EnableAOF) return;

            var task = databaseManager.CommitToAofAsync();
            if (!spinWait) return;

            task.GetAwaiter().GetResult();
        }

        /// <summary>
        /// Wait for commits from all active databases
        /// </summary>
        internal void WaitForCommit() =>
            WaitForCommitAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously wait for commits from all active databases
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>ValueTask</returns>
        internal async ValueTask WaitForCommitAsync(CancellationToken token = default)
        {
            if (!serverOptions.EnableAOF) return;

            await databaseManager.WaitForCommitToAofAsync(token);
        }

        /// <summary>
        /// Asynchronously wait for AOF commits on all active databases,
        /// unless specific database ID specified (by default: -1 = all)
        /// </summary>
        /// <param name="dbId">Specific database ID to commit AOF for (optional)</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>ValueTask</returns>
        internal async ValueTask CommitAOFAsync(int dbId = -1, CancellationToken token = default)
        {
            if (!serverOptions.EnableAOF) return;

            if (dbId == -1)
            {
                await databaseManager.CommitToAofAsync(token, logger);
                return;
            }

            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.CommitToAofAsync)} with DB ID: {dbId}");

            await databaseManager.CommitToAofAsync(dbId, token);
        }

        /// <summary>
        /// Create database functions state
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <returns>Functions state</returns>
        /// <exception cref="GarnetException"></exception>
        internal FunctionsState CreateFunctionsState(int dbId = 0)
        {
            if (dbId != 0 && !CheckMultiDatabaseCompatibility())
                throw new GarnetException($"Unable to call {nameof(databaseManager.CreateFunctionsState)} with DB ID: {dbId}");

            return databaseManager.CreateFunctionsState(dbId);
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

        async Task AutoCheckpointBasedOnAofSizeLimit(long aofSizeLimit, CancellationToken token = default, ILogger logger = null)
        {
            try
            {
                while (true)
                {
                    await Task.Delay(1000, token);
                    if (token.IsCancellationRequested) break;

                    await databaseManager.TaskCheckpointBasedOnAofSizeLimitAsync(aofSizeLimit, token, logger);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Exception received at AutoCheckpointTask");
            }
        }

        async Task CommitTask(int commitFrequencyMs, ILogger logger = null, CancellationToken token = default)
        {
            try
            {
                while (true)
                {
                    if (token.IsCancellationRequested) break;

                    // if we are replica and in auto-commit - do not commit as it will clobber the AOF addresses
                    if (serverOptions.EnableFastCommit && (clusterProvider?.IsReplica() ?? false))
                    {
                        await Task.Delay(commitFrequencyMs, token);
                    }
                    else
                    {
                        await databaseManager.CommitToAofAsync(token, logger);

                        await Task.Delay(commitFrequencyMs, token);
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "CommitTask exception received.");
            }
        }

        async Task CompactionTask(int compactionFrequencySecs, CancellationToken token = default)
        {
            Debug.Assert(compactionFrequencySecs > 0);
            try
            {
                while (true)
                {
                    if (token.IsCancellationRequested) return;

                    databaseManager.DoCompaction(token, logger);

                    if (!serverOptions.CompactionForceDelete)
                        logger?.LogInformation("NOTE: Take a checkpoint (SAVE/BGSAVE) in order to actually delete the older data segments (files) from disk");
                    else
                        logger?.LogInformation("NOTE: Compaction will delete files, make sure checkpoint/recovery is not being used");

                    await Task.Delay(compactionFrequencySecs * 1000, token);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "CompactionTask exception received.");
            }
        }

        async Task ObjectCollectTask(int objectCollectFrequencySecs, CancellationToken token = default)
        {
            Debug.Assert(objectCollectFrequencySecs > 0);
            try
            {
                var scratchBufferManager = new ScratchBufferManager();
                using var storageSession = new StorageSession(this, scratchBufferManager, null, null, logger);

                if (serverOptions.DisableObjects)
                {
                    logger?.LogWarning("ExpiredObjectCollectionFrequencySecs option is configured but Object store is disabled. Stopping the background hash collect task.");
                    return;
                }

                while (true)
                {
                    if (token.IsCancellationRequested) return;

                    ExecuteHashCollect(scratchBufferManager, storageSession);
                    ExecuteSortedSetCollect(scratchBufferManager, storageSession);

                    await Task.Delay(TimeSpan.FromSeconds(objectCollectFrequencySecs), token);
                }
            }
            catch (TaskCanceledException) when (token.IsCancellationRequested)
            {
                // Suppress the exception if the task was cancelled because of store wrapper disposal
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "Unknown exception received for background hash collect task. Object collect task won't be resumed.");
            }

            static void ExecuteHashCollect(ScratchBufferManager scratchBufferManager, StorageSession storageSession)
            {
                var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HCOLLECT };
                var input = new ObjectInput(header);

                ReadOnlySpan<ArgSlice> key = [ArgSlice.FromPinnedSpan("*"u8)];
                storageSession.HashCollect(key, ref input, ref storageSession.objectStoreBasicContext);
                scratchBufferManager.Reset();
            }

            static void ExecuteSortedSetCollect(ScratchBufferManager scratchBufferManager, StorageSession storageSession)
            {
                storageSession.SortedSetCollect(ref storageSession.objectStoreBasicContext);
                scratchBufferManager.Reset();
            }
        }

        /// <summary>Grows indexes of both main store and object store if current size is too small.</summary>
        /// <param name="token"></param>
        private async void IndexAutoGrowTask(CancellationToken token)
        {
            try
            {
                var allIndexesMaxedOut = false;

                while (!allIndexesMaxedOut)
                {
                    if (token.IsCancellationRequested) break;

                    await Task.Delay(TimeSpan.FromSeconds(serverOptions.IndexResizeFrequencySecs), token);

                    allIndexesMaxedOut = databaseManager.GrowIndexesIfNeeded(token);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(IndexAutoGrowTask)} exception received");
            }
        }

        internal void Start()
        {
            monitor?.Start();
            clusterProvider?.Start();
            luaTimeoutManager?.Start();

            if (serverOptions.AofSizeLimit.Length > 0)
            {
                var aofSizeLimitBytes = 1L << serverOptions.AofSizeLimitSizeBits();
                Task.Run(async () => await AutoCheckpointBasedOnAofSizeLimit(aofSizeLimitBytes, ctsCommit.Token, logger));
            }

            if (serverOptions.CommitFrequencyMs > 0 && serverOptions.EnableAOF)
            {
                Task.Run(async () => await CommitTask(serverOptions.CommitFrequencyMs, logger, ctsCommit.Token));
            }

            if (serverOptions.CompactionFrequencySecs > 0 && serverOptions.CompactionType != LogCompactionType.None)
            {
                Task.Run(async () => await CompactionTask(serverOptions.CompactionFrequencySecs, ctsCommit.Token));
            }

            if (serverOptions.ExpiredObjectCollectionFrequencySecs > 0)
            {
                Task.Run(async () => await ObjectCollectTask(serverOptions.ExpiredObjectCollectionFrequencySecs, ctsCommit.Token));
            }

            if (serverOptions.AdjustedIndexMaxCacheLines > 0 || serverOptions.AdjustedObjectStoreIndexMaxCacheLines > 0)
            {
                Task.Run(() => IndexAutoGrowTask(ctsCommit.Token));
            }

            databaseManager.StartObjectSizeTrackers(ctsCommit.Token);
        }

        public bool HasKeysInSlots(List<int> slots)
        {
            if (slots.Count > 0)
            {
                bool hasKeyInSlots = false;
                {
                    using var iter = store.Iterate<SpanByte, SpanByte, Empty, SimpleSessionFunctions<SpanByte, SpanByte, Empty>>(new SimpleSessionFunctions<SpanByte, SpanByte, Empty>());
                    while (!hasKeyInSlots && iter.GetNext(out RecordInfo record))
                    {
                        ref var key = ref iter.GetKey();
                        ushort hashSlotForKey = HashSlotUtils.HashSlot(ref key);
                        if (slots.Contains(hashSlotForKey))
                        {
                            hasKeyInSlots = true;
                        }
                    }
                }

                if (!hasKeyInSlots && !serverOptions.DisableObjects)
                {
                    var functionsState = databaseManager.CreateFunctionsState();
                    var objstorefunctions = new ObjectSessionFunctions(functionsState);
                    var objectStoreSession = objectStore?.NewSession<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions>(objstorefunctions);
                    var iter = objectStoreSession.Iterate();
                    while (!hasKeyInSlots && iter.GetNext(out RecordInfo record))
                    {
                        ref var key = ref iter.GetKey();
                        ushort hashSlotForKey = HashSlotUtils.HashSlot(key.AsSpan());
                        if (slots.Contains(hashSlotForKey))
                        {
                            hasKeyInSlots = true;
                        }
                    }
                }

                return hasKeyInSlots;
            }

            return false;
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
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            if (disposed) return;
            disposed = true;

            itemBroker?.Dispose();
            monitor?.Dispose();
            luaTimeoutManager?.Dispose();
            ctsCommit?.Cancel();
            databaseManager.Dispose();

            ctsCommit?.Dispose();
            clusterProvider?.Dispose();
        }
    }
}