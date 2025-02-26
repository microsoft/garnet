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
        readonly IGarnetServer server;
        internal readonly long startupTime;

        /// <summary>
        /// Default database (DB 0)
        /// </summary>
        public ref GarnetDatabase DefaultDatabase => ref databaseManager.DefaultDatabase;

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
        /// Version map (of DB 0)
        /// </summary>
        internal WatchVersionMap versionMap => databaseManager.VersionMap;

        /// <summary>
        /// Server options
        /// </summary>
        public readonly GarnetServerOptions serverOptions;

        /// <summary>
        /// Subscribe broker
        /// </summary>
        public readonly SubscribeBroker subscribeBroker;

        /// <summary>
        /// Get server
        /// </summary>
        public GarnetServerTcp TcpServer => (GarnetServerTcp)server;

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
        public delegate GarnetDatabase DatabaseCreatorDelegate(int dbId, out string storeCheckpointDir, out string aofDir);

        internal readonly IDatabaseManager databaseManager;

        internal readonly CollectionItemBroker itemBroker;
        internal readonly CustomCommandManager customCommandManager;
        internal readonly GarnetServerMonitor monitor;
        internal readonly IClusterProvider clusterProvider;
        internal readonly SlowLogContainer slowLogContainer;
        internal readonly ILogger sessionLogger;
        internal long safeAofAddress = -1;

        // Standalone instance node_id
        internal readonly string runId;

        readonly CancellationTokenSource ctsCommit;

        // True if this server supports more than one logical database
        readonly bool allowMultiDb;

        // True if StoreWrapper instance is disposed
        bool disposed = false;

        /// <summary>
        /// Constructor
        /// </summary>
        public StoreWrapper(
            string version,
            string redisProtocolVersion,
            IGarnetServer server,
            DatabaseCreatorDelegate createDatabaseDelegate,
            CustomCommandManager customCommandManager,
            GarnetServerOptions serverOptions,
            SubscribeBroker subscribeBroker,
            AccessControlList accessControlList = null,
            IClusterFactory clusterFactory = null,
            ILoggerFactory loggerFactory = null)
        {
            this.version = version;
            this.redisProtocolVersion = redisProtocolVersion;
            this.server = server;
            this.startupTime = DateTimeOffset.UtcNow.Ticks;
            this.serverOptions = serverOptions;
            this.subscribeBroker = subscribeBroker;
            this.customCommandManager = customCommandManager;
            this.databaseManager = serverOptions.EnableCluster || serverOptions.MaxDatabases == 1
                ? new SingleDatabaseManager(createDatabaseDelegate, this)
                : new MultiDatabaseManager(createDatabaseDelegate, this);
            this.monitor = serverOptions.MetricsSamplingFrequency > 0
                ? new GarnetServerMonitor(this, serverOptions, server,
                    loggerFactory?.CreateLogger("GarnetServerMonitor"))
                : null;
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory?.CreateLogger("StoreWrapper");
            this.sessionLogger = loggerFactory?.CreateLogger("Session");
            this.accessControlList = accessControlList;
            this.GarnetObjectSerializer = new GarnetObjectSerializer(this.customCommandManager);
            this.loggingFrequency = TimeSpan.FromSeconds(serverOptions.LoggingFrequency);

            // If cluster mode is off and more than one database allowed multi-db mode is turned on
            this.allowMultiDb = !this.serverOptions.EnableCluster && this.serverOptions.MaxDatabases > 1;

            if (serverOptions.SlowLogThreshold > 0)
                this.slowLogContainer = new SlowLogContainer(serverOptions.SlowLogMaxEntries);

            if (!serverOptions.DisableObjects)
                this.itemBroker = new CollectionItemBroker();

            // Initialize store scripting cache
            if (serverOptions.EnableLua)
                this.storeScriptCache = [];

            if (accessControlList == null)
            {
                // If ACL authentication is enabled, initiate access control list
                // NOTE: This is a temporary workflow. ACL should always be initiated and authenticator
                //       should become a parameter of AccessControlList.
                if ((this.serverOptions.AuthSettings != null) && (this.serverOptions.AuthSettings.GetType().BaseType ==
                                                                  typeof(AclAuthenticationSettings)))
                {
                    // Create a new access control list and register it with the authentication settings
                    AclAuthenticationSettings aclAuthenticationSettings =
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
        /// <param name="recordToAof">Enable AOF in StoreWrapper copy</param>
        public StoreWrapper(StoreWrapper storeWrapper, bool recordToAof) : this(
            storeWrapper.version,
            storeWrapper.redisProtocolVersion,
            storeWrapper.server,
            null,
            storeWrapper.customCommandManager,
            storeWrapper.serverOptions,
            storeWrapper.subscribeBroker,
            storeWrapper.accessControlList,
            null,
            storeWrapper.loggerFactory)
        {
            this.databaseManager = storeWrapper.databaseManager.Clone(recordToAof);
        }

        /// <summary>
        /// Get IP
        /// </summary>
        /// <returns></returns>
        public string GetIp()
        {
            if (TcpServer.EndPoint is not IPEndPoint localEndpoint)
                throw new NotImplementedException("Cluster mode for unix domain sockets has not been implemented");

            if (localEndpoint.Address.Equals(IPAddress.Any))
            {
                using (Socket socket = new(AddressFamily.InterNetwork, SocketType.Dgram, 0))
                {
                    socket.Connect("8.8.8.8", 65530);
                    var endPoint = socket.LocalEndPoint as IPEndPoint;
                    return endPoint.Address.ToString();
                }
            }
            else if (localEndpoint.Address.Equals(IPAddress.IPv6Any))
            {
                using (Socket socket = new(AddressFamily.InterNetworkV6, SocketType.Dgram, 0))
                {
                    socket.Connect("2001:4860:4860::8888", 65530);
                    var endPoint = socket.LocalEndPoint as IPEndPoint;
                    return endPoint.Address.ToString();
                }
            }
            return localEndpoint.Address.ToString();
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

        public void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false,
            bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null)
            => databaseManager.RecoverCheckpoint(replicaRecover, recoverMainStoreFromToken, recoverObjectStoreFromToken, metadata);

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
        /// <param name="isMainStore"></param>
        /// <param name="version"></param>
        /// <param name="dbId"></param>
        public void EnqueueCommit(bool isMainStore, long version, int dbId = 0) =>
            this.databaseManager.EnqueueCommit(isMainStore, version, dbId);

        /// <summary>
        /// Reset
        /// </summary>
        /// <param name="dbId">Database ID</param>
        public void Reset(int dbId = 0) => databaseManager.Reset(dbId);

        /// <summary>
        /// Try to swap between two database instances
        /// </summary>
        /// <param name="dbId1">First database ID</param>
        /// <param name="dbId2">Second database ID</param>
        /// <returns>True if swap successful</returns>
        public bool TrySwapDatabases(int dbId1, int dbId2) => this.databaseManager.TrySwapDatabases(dbId1, dbId2);

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
                        var activeDbIdsSize = this.activeDbIdsLength;

                        if (!allowMultiDb || activeDbIdsSize == 1)
                        {
                            await appendOnlyFile.CommitAsync(null, token);
                        }
                        else
                        {
                            MultiDatabaseCommit(ref aofTasks, token);
                        }

                        await Task.Delay(commitFrequencyMs, token);
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "CommitTask exception received, AOF tail address = {tailAddress}; AOF committed until address = {commitAddress}; ", appendOnlyFile.TailAddress, appendOnlyFile.CommittedUntilAddress);
            }
        }

        /// <summary>
        /// Perform AOF commits on all active databases
        /// </summary>
        /// <param name="tasks">Optional reference to pre-allocated array of tasks to use</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="spinWait">True if should wait until all tasks complete</param>
        void MultiDatabaseCommit(ref Task[] tasks, CancellationToken token, bool spinWait = true)
        {
            var databasesMapSnapshot = databases.Map;

            var activeDbIdsSize = activeDbIdsLength;
            var activeDbIdsSnapshot = activeDbIds;

            tasks ??= new Task[activeDbIdsSize];

            // Take a read lock to make sure that swap-db operation is not in progress
            var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
            if (!lockAcquired) return;

            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                var db = databasesMapSnapshot[dbId];
                Debug.Assert(!db.IsDefault());

                tasks[i] = db.AppendOnlyFile.CommitAsync(null, token).AsTask();
            }

            var completion = Task.WhenAll(tasks).ContinueWith(_ => databasesLock.ReadUnlock(), token);

            if (!spinWait)
                return;

            completion.Wait(token);
        }

        async Task CompactionTask(int compactionFrequencySecs, CancellationToken token = default)
        {
            Debug.Assert(compactionFrequencySecs > 0);
            try
            {
                while (true)
                {
                    if (token.IsCancellationRequested) return;

                    var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
                    if (!lockAcquired) continue;

                    try
                    {
                        var databasesMapSnapshot = databases.Map;

                        var activeDbIdsSize = activeDbIdsLength;
                        var activeDbIdsSnapshot = activeDbIds;

                        for (var i = 0; i < activeDbIdsSize; i++)
                        {
                            var dbId = activeDbIdsSnapshot[i];
                            var db = databasesMapSnapshot[dbId];
                            Debug.Assert(!db.IsDefault());

                            DoCompaction(ref db, serverOptions.CompactionMaxSegments, serverOptions.ObjectStoreCompactionMaxSegments, 1, serverOptions.CompactionType, serverOptions.CompactionForceDelete);
                        }
                    }
                    finally
                    {
                        databasesLock.ReadUnlock();
                    }

                    if (!serverOptions.CompactionForceDelete)
                        logger?.LogInformation("NOTE: Take a checkpoint (SAVE/BGSAVE) in order to actually delete the older data segments (files) from disk");
                    else
                        logger?.LogInformation("NOTE: Compaction will delete files, make sure checkpoint/recovery is not being used");

                    await Task.Delay(compactionFrequencySecs * 1000, token);
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "CompactionTask exception received, AOF tail address = {tailAddress}; AOF committed until address = {commitAddress}; ", appendOnlyFile.TailAddress, appendOnlyFile.CommittedUntilAddress);
            }
        }

        async Task HashCollectTask(int hashCollectFrequencySecs, CancellationToken token = default)
        {
            Debug.Assert(hashCollectFrequencySecs > 0);
            try
            {
                var scratchBufferManager = new ScratchBufferManager();
                using var storageSession = new StorageSession(this, scratchBufferManager, null, null, logger);

                if (objectStore is null)
                {
                    logger?.LogWarning("HashCollectFrequencySecs option is configured but Object store is disabled. Stopping the background hash collect task.");
                    return;
                }

                while (true)
                {
                    if (token.IsCancellationRequested) return;

                    ExecuteHashCollect(scratchBufferManager, storageSession);

                    await Task.Delay(TimeSpan.FromSeconds(hashCollectFrequencySecs), token);
                }
            }
            catch (TaskCanceledException) when (token.IsCancellationRequested)
            {
                // Suppress the exception if the task was cancelled because of store wrapper disposal
            }
            catch (Exception ex)
            {
                logger?.LogCritical(ex, "Unknown exception received for background hash collect task. Hash collect task won't be resumed.");
            }

            static void ExecuteHashCollect(ScratchBufferManager scratchBufferManager, StorageSession storageSession)
            {
                var header = new RespInputHeader(GarnetObjectType.Hash) { HashOp = HashOperation.HCOLLECT };
                var input = new ObjectInput(header);

                ReadOnlySpan<ArgSlice> key = [ArgSlice.FromPinnedSpan("*"u8)];
                storageSession.HashCollect(key, ref input, ref storageSession.objectStoreBasicContext);
                scratchBufferManager.Reset();
            }
        }

        /// <summary>
        /// Commit AOF for all active databases
        /// </summary>
        /// <param name="spinWait">True if should wait until all commits complete</param>
        internal void CommitAOF(bool spinWait)
        {
            if (!serverOptions.EnableAOF || appendOnlyFile == null) return;

            var activeDbIdsSize = this.activeDbIdsLength;
            if (!allowMultiDb || activeDbIdsSize == 1)
            {
                appendOnlyFile.Commit(spinWait);
                return;
            }

            Task[] tasks = null;
            MultiDatabaseCommit(ref tasks, CancellationToken.None, spinWait);
        }

        /// <summary>
        /// Wait for commits from all active databases
        /// </summary>
        /// </summary>
        internal void WaitForCommit()
        {
            if (!serverOptions.EnableAOF || appendOnlyFile == null) return;

            var activeDbIdsSize = this.activeDbIdsLength;
            if (!allowMultiDb || activeDbIdsSize == 1)
            {
                appendOnlyFile.WaitForCommit();
                return;
            }

            // Take a read lock to make sure that swap-db operation is not in progress
            var lockAcquired = TryGetDatabasesReadLockAsync().Result;
            if (!lockAcquired) return;

            var databasesMapSnapshot = databases.Map;
            var activeDbIdsSnapshot = activeDbIds;

            var tasks = new Task[activeDbIdsSize];
            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                var db = databasesMapSnapshot[dbId];
                tasks[i] = db.AppendOnlyFile.WaitForCommitAsync().AsTask();
            }

            Task.WhenAll(tasks).ContinueWith(_ => databasesLock.ReadUnlock()).Wait();
        }

        /// <summary>
        /// Asynchronously wait for commits from all active databases
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>ValueTask</returns>
        internal async ValueTask WaitForCommitAsync(CancellationToken token = default)
        {
            if (!serverOptions.EnableAOF || appendOnlyFile == null) return;

            var activeDbIdsSize = this.activeDbIdsLength;
            if (!allowMultiDb || activeDbIdsSize == 1)
            {
                await appendOnlyFile.WaitForCommitAsync(token: token);
                return;
            }

            // Take a read lock to make sure that swap-db operation is not in progress
            var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
            if (!lockAcquired) return;

            var databasesMapSnapshot = databases.Map;
            var activeDbIdsSnapshot = activeDbIds;

            var tasks = new Task[activeDbIdsSize];
            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                var db = databasesMapSnapshot[dbId];
                tasks[i] = db.AppendOnlyFile.WaitForCommitAsync(token: token).AsTask();
            }

            await Task.WhenAll(tasks).ContinueWith(_ => databasesLock.ReadUnlock(), token);
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
            if (!serverOptions.EnableAOF || appendOnlyFile == null) return;

            var activeDbIdsSize = this.activeDbIdsLength;
            if (!allowMultiDb || activeDbIdsSize == 1 || dbId != -1)
            {
                if (dbId == -1) dbId = 0;
                var dbFound = TryGetDatabase(dbId, out var db);
                Debug.Assert(dbFound);
                await db.AppendOnlyFile.CommitAsync(token: token);
                return;
            }

            var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
            if (!lockAcquired) return;

            var databasesMapSnapshot = databases.Map;
            var activeDbIdsSnapshot = activeDbIds;

            var tasks = new Task[activeDbIdsSize];
            for (var i = 0; i < activeDbIdsSize; i++)
            {
                dbId = activeDbIdsSnapshot[i];
                var db = databasesMapSnapshot[dbId];
                tasks[i] = db.AppendOnlyFile.CommitAsync(token: token).AsTask();
            }

            await Task.WhenAll(tasks).ContinueWith(_ => databasesLock.ReadUnlock(), token);
        }

        internal void Start()
        {
            monitor?.Start();
            clusterProvider?.Start();

            if (serverOptions.AofSizeLimit.Length > 0)
            {
                var AofSizeLimitBytes = 1L << serverOptions.AofSizeLimitSizeBits();
                Task.Run(async () => await AutoCheckpointBasedOnAofSizeLimit(AofSizeLimitBytes, ctsCommit.Token, logger));
            }

            if (serverOptions.CommitFrequencyMs > 0 && appendOnlyFile != null)
            {
                Task.Run(async () => await CommitTask(serverOptions.CommitFrequencyMs, logger, ctsCommit.Token));
            }

            if (serverOptions.CompactionFrequencySecs > 0 && serverOptions.CompactionType != LogCompactionType.None)
            {
                Task.Run(async () => await CompactionTask(serverOptions.CompactionFrequencySecs, ctsCommit.Token));
            }

            if (serverOptions.HashCollectFrequencySecs > 0)
            {
                Task.Run(async () => await HashCollectTask(serverOptions.HashCollectFrequencySecs, ctsCommit.Token));
            }

            if (serverOptions.AdjustedIndexMaxCacheLines > 0 || serverOptions.AdjustedObjectStoreIndexMaxCacheLines > 0)
            {
                Task.Run(() => IndexAutoGrowTask(ctsCommit.Token));
            }

            objectStoreSizeTracker?.Start(ctsCommit.Token);

            var activeDbIdsSize = activeDbIdsLength;

            if (allowMultiDb && activeDbIdsSize > 1)
            {
                var databasesMapSnapshot = databases.Map;
                var activeDbIdsSnapshot = activeDbIds;

                for (var i = 1; i < activeDbIdsSize; i++)
                {
                    var dbId = activeDbIdsSnapshot[i];
                    var db = databasesMapSnapshot[dbId];
                    db.ObjectStoreSizeTracker?.Start(ctsCommit.Token);
                }
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
                    allIndexesMaxedOut = true;

                    if (token.IsCancellationRequested) break;

                    await Task.Delay(TimeSpan.FromSeconds(serverOptions.IndexResizeFrequencySecs), token);

                    var lockAcquired = TryGetDatabasesReadLockAsync(token).Result;
                    if (!lockAcquired) return;

                    try
                    {
                        var activeDbIdsSize = activeDbIdsLength;
                        var activeDbIdsSnapshot = activeDbIds;

                        var databasesMapSnapshot = databases.Map;

                        for (var i = 0; i < activeDbIdsSize; i++)
                        {
                            var dbId = activeDbIdsSnapshot[i];
                            var db = databasesMapSnapshot[dbId];
                            var dbUpdated = false;

                            if (!db.MainStoreIndexMaxedOut)
                            {
                                var dbMainStore = databasesMapSnapshot[dbId].MainStore;
                                if (GrowIndexIfNeeded(StoreType.Main,
                                        serverOptions.AdjustedIndexMaxCacheLines, dbMainStore.OverflowBucketAllocations,
                                        () => dbMainStore.IndexSize, () => dbMainStore.GrowIndex()))
                                {
                                    db.MainStoreIndexMaxedOut = true;
                                    dbUpdated = true;
                                }
                                else
                                {
                                    allIndexesMaxedOut = false;
                                }
                            }

                            if (!db.ObjectStoreIndexMaxedOut)
                            {
                                var dbObjectStore = databasesMapSnapshot[dbId].ObjectStore;
                                if (GrowIndexIfNeeded(StoreType.Object,
                                        serverOptions.AdjustedObjectStoreIndexMaxCacheLines,
                                        dbObjectStore.OverflowBucketAllocations,
                                        () => dbObjectStore.IndexSize, () => dbObjectStore.GrowIndex()))
                                {
                                    db.ObjectStoreIndexMaxedOut = true;
                                    dbUpdated = true;
                                }
                                else
                                {
                                    allIndexesMaxedOut = false;
                                }
                            }

                            if (dbUpdated)
                            {
                                databasesMapSnapshot[dbId] = db;
                            }
                        }
                    }
                    finally
                    {
                        databasesLock.ReadUnlock();
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, $"{nameof(IndexAutoGrowTask)} exception received");
            }
        }

        /// <summary>
        /// Grows index if current size is smaller than max size.
        /// Decision is based on whether overflow bucket allocation is more than a threshold which indicates a contention
        /// in the index leading many allocations to the same bucket.
        /// </summary>
        /// <param name="storeType"></param>
        /// <param name="indexMaxSize"></param>
        /// <param name="overflowCount"></param>
        /// <param name="indexSizeRetriever"></param>
        /// <param name="growAction"></param>
        /// <returns>True if index has reached its max size</returns>
        private bool GrowIndexIfNeeded(StoreType storeType, long indexMaxSize, long overflowCount, Func<long> indexSizeRetriever, Action growAction)
        {
            logger?.LogDebug($"{nameof(IndexAutoGrowTask)}[{{storeType}}]: checking index size {{indexSizeRetriever}} against max {{indexMaxSize}} with overflow {{overflowCount}}", storeType, indexSizeRetriever(), indexMaxSize, overflowCount);

            if (indexSizeRetriever() < indexMaxSize &&
                overflowCount > (indexSizeRetriever() * serverOptions.IndexResizeThreshold / 100))
            {
                logger?.LogInformation($"{nameof(IndexAutoGrowTask)}[{{storeType}}]: overflowCount {{overflowCount}} ratio more than threshold {{indexResizeThreshold}}%. Doubling index size...", storeType, overflowCount, serverOptions.IndexResizeThreshold);
                growAction();
            }

            if (indexSizeRetriever() < indexMaxSize) return false;

            logger?.LogDebug($"{nameof(IndexAutoGrowTask)}[{{storeType}}]: index size {{indexSizeRetriever}} reached index max size {{indexMaxSize}}", storeType, indexSizeRetriever(), indexMaxSize);
            return true;
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
            ctsCommit?.Cancel();

            ctsCommit?.Dispose();
            clusterProvider?.Dispose();
        }

        /// <summary>
        /// Mark the beginning of a checkpoint by taking and a lock to avoid concurrent checkpointing
        /// </summary>
        /// <param name="dbId">ID of database to lock</param>
        /// <returns>True if lock acquired</returns>
        public bool TryPauseCheckpoints(int dbId = 0)
            => databaseManager.TryPauseCheckpoints(dbId);

        /// <summary>
        /// Release checkpoint task lock
        /// </summary>
        /// <param name="dbId">ID of database to unlock</param>
        public void ResumeCheckpoints(int dbId = 0)
            => databaseManager.ResumeCheckpoints(dbId);

        /// <summary>
        /// Take a checkpoint if no checkpoint was taken after the provided time offset
        /// </summary>
        /// <param name="entryTime"></param>
        /// <param name="dbId"></param>
        /// <returns></returns>
        public async Task TakeOnDemandCheckpoint(DateTimeOffset entryTime, int dbId = 0) =>
            await databaseManager.TakeOnDemandCheckpointAsync(entryTime, dbId);

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

                if (!hasKeyInSlots && objectStore != null)
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
    }
}