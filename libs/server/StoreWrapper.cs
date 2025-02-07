// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq.Expressions;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.ACL;
using Garnet.server.Auth.Settings;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using IStreamProvider = Garnet.common.IStreamProvider;

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
        /// Store (of DB 0)
        /// </summary>
        public TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> store;

        /// <summary>
        /// Object store (of DB 0)
        /// </summary>
        public TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore;

        /// <summary>
        /// AOF (of DB 0)
        /// </summary>
        public TsavoriteLog appendOnlyFile;

        /// <summary>
        /// Last save time (of DB 0)
        /// </summary>
        public DateTimeOffset lastSaveTime;

        /// <summary>
        /// Object store size tracker (of DB 0)
        /// </summary>
        public CacheSizeTracker objectStoreSizeTracker;

        /// <summary>
        /// Version map (of DB 0)
        /// </summary>
        internal WatchVersionMap versionMap;

        /// <summary>
        /// Server options
        /// </summary>
        public readonly GarnetServerOptions serverOptions;

        /// <summary>
        /// Subscribe broker
        /// </summary>
        public readonly SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscribeBroker;

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
        /// Number of current logical databases
        /// </summary>
        public int DatabaseCount => activeDbIdsLength;

        /// <summary>
        /// Definition for delegate creating a new logical database
        /// </summary>
        public delegate GarnetDatabase DatabaseCreatorDelegate(int dbId, out string storeCheckpointDir, out string aofDir);

        /// <summary>
        /// Delegate for creating a new logical database
        /// </summary>
        internal readonly DatabaseCreatorDelegate createDatabasesDelegate;

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
        SingleWriterMultiReaderLock checkpointTaskLock;

        // True if this server supports more than one logical database
        readonly bool allowMultiDb;
        
        // Map of databases by database ID (by default: of size 1, contains only DB 0)
        ExpandableMap<GarnetDatabase> databases; 
        
        // Array containing active database IDs
        int[] activeDbIds;

        // Total number of current active database IDs
        int activeDbIdsLength;

        // Last DB ID activated
        int lastActivatedDbId = -1;

        // Reusable task array for tracking checkpointing of multiple DBs
        // Used by recurring checkpointing task if multiple DBs exist
        Task[] checkpointTasks;

        // Reusable task array for tracking aof commits of multiple DBs
        // Used by recurring aof commits task if multiple DBs exist
        Task[] aofTasks;
        readonly object activeDbIdsLock = new();

        // File name of serialized binary file containing all DB IDs
        const string DatabaseIdsFileName = "dbIds.dat";

        // Path of serialization for the DB IDs file used when committing / recovering to / from AOF
        readonly string aofDatabaseIdsPath;
        
        // Path of serialization for the DB IDs file used when committing / recovering to / from a checkpoint
        readonly string checkpointDatabaseIdsPath;

        // Stream provider for serializing DB IDs file
        readonly IStreamProvider databaseIdsStreamProvider;

        /// <summary>
        /// Constructor
        /// </summary>
        public StoreWrapper(
            string version,
            string redisProtocolVersion,
            IGarnetServer server,
            DatabaseCreatorDelegate createsDatabaseDelegate,
            CustomCommandManager customCommandManager,
            GarnetServerOptions serverOptions,
            SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> subscribeBroker,
            bool createDefaultDatabase = true,
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
            this.lastSaveTime = DateTimeOffset.FromUnixTimeSeconds(0);
            this.customCommandManager = customCommandManager;
            this.createDatabasesDelegate = createsDatabaseDelegate;
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

            // If more than one database allowed, multi-db mode is turned on
            this.allowMultiDb = this.serverOptions.MaxDatabases > 1;

            // Create default databases map of size 1
            databases = new ExpandableMap<GarnetDatabase>(1, 0, this.serverOptions.MaxDatabases - 1);

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                var db = createDatabasesDelegate(0, out var storeCheckpointDir, out var aofPath);
                checkpointDatabaseIdsPath = storeCheckpointDir == null ? null : Path.Combine(storeCheckpointDir, DatabaseIdsFileName);
                aofDatabaseIdsPath = aofPath == null ? null : Path.Combine(aofPath, DatabaseIdsFileName);

                // Set new database in map
                if (!this.TryAddDatabase(0, ref db))
                    throw new GarnetException("Failed to set initial database in databases map");

                this.InitializeFieldsFromDatabase(databases.Map[0]);
            }

            if (allowMultiDb)
            {
                databaseIdsStreamProvider = serverOptions.StreamProviderCreator();
            }

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
            storeWrapper.createDatabasesDelegate,
            storeWrapper.customCommandManager,
            storeWrapper.serverOptions,
            storeWrapper.subscribeBroker,
            createDefaultDatabase: false,
            storeWrapper.accessControlList,
            null,
            storeWrapper.loggerFactory)
        {
            this.CopyDatabases(storeWrapper, recordToAof);
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

        internal FunctionsState CreateFunctionsState(int dbId = 0)
        {
            Debug.Assert(dbId == 0 || allowMultiDb);

            if (dbId == 0)
                return new(appendOnlyFile, versionMap, customCommandManager, null, objectStoreSizeTracker, GarnetObjectSerializer);
            
            if (!this.TryGetOrAddDatabase(dbId, out var db))
                throw new GarnetException($"Database with ID {dbId} was not found.");

            return new(db.AppendOnlyFile, db.VersionMap, customCommandManager, null, db.ObjectStoreSizeTracker,
                GarnetObjectSerializer);
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
        /// Caller will have to decide if recover is necessary, so we do not check if recover option is enabled
        /// </summary>
        public void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null)
        {
            long storeVersion = -1, objectStoreVersion = -1;
            try
            {
                if (replicaRecover)
                {
                    // Note: Since replicaRecover only pertains to cluster-mode, we can use the default store pointers (since multi-db mode is disabled in cluster-mode)
                    if (metadata.storeIndexToken != default && metadata.storeHlogToken != default)
                    {
                        storeVersion = !recoverMainStoreFromToken ? store.Recover() : store.Recover(metadata.storeIndexToken, metadata.storeHlogToken);
                    }

                    if (!serverOptions.DisableObjects)
                    {
                        if (metadata.objectStoreIndexToken != default && metadata.objectStoreHlogToken != default)
                        {
                            objectStoreVersion = !recoverObjectStoreFromToken ? objectStore.Recover() : objectStore.Recover(metadata.objectStoreIndexToken, metadata.objectStoreHlogToken);
                        }
                    }

                    if (storeVersion > 0 || objectStoreVersion > 0)
                        lastSaveTime = DateTimeOffset.UtcNow;
                }
                else
                {
                    if (allowMultiDb && checkpointDatabaseIdsPath != null)
                        RecoverDatabases(checkpointDatabaseIdsPath);

                    var databasesMapSnapshot = databases.Map;

                    var activeDbIdsSize = activeDbIdsLength;
                    var activeDbIdsSnapshot = activeDbIds;

                    for (var i = 0; i < activeDbIdsSize; i++)
                    {
                        var dbId = activeDbIdsSnapshot[i];
                        var db = databasesMapSnapshot[dbId];
                        storeVersion = db.MainStore.Recover();
                        if (db.ObjectStore != null) objectStoreVersion = db.ObjectStore.Recover();

                        var lastSave = DateTimeOffset.UtcNow;
                        db.LastSaveTime = lastSave;
                        if (dbId == 0 && (storeVersion > 0 || objectStoreVersion > 0))
                            lastSaveTime = lastSave;
                    }
                }
            }
            catch (TsavoriteNoHybridLogException ex)
            {
                // No hybrid log being found is not the same as an error in recovery. e.g. fresh start
                logger?.LogInformation(ex, "No Hybrid Log found for recovery; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
            }
            catch (Exception ex)
            {
                logger?.LogInformation(ex, "Error during recovery of store; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
                if (serverOptions.FailOnRecoveryError)
                    throw;
            }
        }

        /// <summary>
        /// Recover AOF
        /// </summary>
        public void RecoverAOF()
        {
            if (allowMultiDb && aofDatabaseIdsPath != null)
                RecoverDatabases(aofDatabaseIdsPath);

            var databasesMapSnapshot = databases.Map;

            var activeDbIdsSize = activeDbIdsLength;
            var activeDbIdsSnapshot = activeDbIds;

            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                var db = databasesMapSnapshot[dbId];
                Debug.Assert(!db.IsDefault());

                if (db.AppendOnlyFile == null) continue;

                db.AppendOnlyFile.Recover();
                logger?.LogInformation("Recovered AOF: begin address = {beginAddress}, tail address = {tailAddress}", db.AppendOnlyFile.BeginAddress, db.AppendOnlyFile.TailAddress);
            }
        }

        /// <summary>
        /// Reset
        /// </summary>
        public void Reset(int dbId = 0)
        {
            try
            {
                var db = databases.Map[dbId];
                if (db.MainStore.Log.TailAddress > 64)
                    db.MainStore.Reset();
                if (db.ObjectStore?.Log.TailAddress > 64)
                    db.ObjectStore?.Reset();
                db.AppendOnlyFile?.Reset();

                var lastSave = DateTimeOffset.FromUnixTimeSeconds(0);
                if (dbId == 0)
                    lastSaveTime = lastSave;
                db.LastSaveTime = lastSave;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error during reset of store");
            }
        }

        /// <summary>
        /// When replaying AOF we do not want to write AOF records again.
        /// </summary>
        public long ReplayAOF(long untilAddress = -1)
        {
            if (!serverOptions.EnableAOF)
                return -1;

            long replicationOffset = 0;
            try
            {
                // When replaying AOF we do not want to write record again to AOF.
                // So initialize local AofProcessor with recordToAof: false.
                var aofProcessor = new AofProcessor(this, recordToAof: false, logger);

                var databasesMapSnapshot = databases.Map;

                var activeDbIdsSize = activeDbIdsLength;
                var activeDbIdsSnapshot = activeDbIds;

                for (var i = 0; i < activeDbIdsSize; i++)
                {
                    var dbId = activeDbIdsSnapshot[i];

                    aofProcessor.Recover(dbId, dbId == 0 ? untilAddress : -1);

                    var lastSave = DateTimeOffset.UtcNow;
                    databasesMapSnapshot[dbId].LastSaveTime = lastSave;

                    if (dbId == 0)
                    {
                        replicationOffset = aofProcessor.ReplicationOffset;
                        this.lastSaveTime = lastSave;
                    }
                }

                aofProcessor.Dispose();
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error during recovery of AofProcessor");
                if (serverOptions.FailOnRecoveryError)
                    throw;
            }

            return replicationOffset;
        }

        /// <summary>
        /// Try to add a new database
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <param name="db">Database</param>
        /// <returns></returns>
        public bool TryAddDatabase(int dbId, ref GarnetDatabase db)
        {
            if (!allowMultiDb || !databases.TrySetValue(dbId, ref db))
                return false;

            HandleDatabaseAdded(dbId);
            return true;
        }

        /// <summary>
        /// Try to get or add a new database
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <param name="db">Database</param>
        /// <returns>True if database was retrieved or added successfully</returns>
        public bool TryGetOrAddDatabase(int dbId, out GarnetDatabase db)
        {
            db = default;

            if (!allowMultiDb || !databases.TryGetOrSet(dbId, () => createDatabasesDelegate(dbId, out _, out _), out db, out var added))
                return false;

            if (added)
                HandleDatabaseAdded(dbId);

            return true;
        }

        async Task AutoCheckpointBasedOnAofSizeLimit(long aofSizeLimit, CancellationToken token = default, ILogger logger = null)
        {
            try
            {
                int[] dbIdsToCheckpoint = null;

                while (true)
                {
                    await Task.Delay(1000, token);
                    if (token.IsCancellationRequested) break;

                    var aofSizeAtLimit = -1l;
                    var activeDbIdsSize = activeDbIdsLength;

                    if (!allowMultiDb || activeDbIdsSize == 1)
                    {
                        var dbAofSize = appendOnlyFile.TailAddress - appendOnlyFile.BeginAddress;
                        if (dbAofSize > aofSizeLimit)
                            aofSizeAtLimit = dbAofSize;

                        if (aofSizeAtLimit != -1)
                        {
                            logger?.LogInformation("Enforcing AOF size limit currentAofSize: {currAofSize} >  AofSizeLimit: {AofSizeLimit}", aofSizeAtLimit, aofSizeLimit);
                            await CheckpointTask(StoreType.All, logger: logger);
                        }

                        return;
                    }

                    var databasesMapSnapshot = databases.Map;
                    var activeDbIdsSnapshot = activeDbIds;

                    if (dbIdsToCheckpoint == null || dbIdsToCheckpoint.Length < activeDbIdsSize)
                        dbIdsToCheckpoint = new int[activeDbIdsSize];

                    var dbIdsIdx = 0;
                    for (var i = 0; i < activeDbIdsSize; i++)
                    {
                        var dbId = activeDbIdsSnapshot[i];
                        var db = databasesMapSnapshot[dbId];
                        Debug.Assert(!db.IsDefault());

                        var dbAofSize = db.AppendOnlyFile.TailAddress - db.AppendOnlyFile.BeginAddress;
                        if (dbAofSize > aofSizeLimit)
                        {
                            dbIdsToCheckpoint[dbIdsIdx++] = dbId;
                            break;
                        }
                    }
                    
                    if (dbIdsIdx > 0)
                    {
                        logger?.LogInformation("Enforcing AOF size limit currentAofSize: {currAofSize} >  AofSizeLimit: {AofSizeLimit}", aofSizeAtLimit, aofSizeLimit);
                        CheckpointDatabases(StoreType.All, ref dbIdsToCheckpoint, ref checkpointTasks, logger: logger, token: token);
                    }
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

            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                var db = databasesMapSnapshot[dbId];
                Debug.Assert(!db.IsDefault());

                tasks[i] = db.AppendOnlyFile.CommitAsync(null, token).AsTask();
            }

            var completion = Task.WhenAll(tasks).ContinueWith(_ =>
            {
                if (aofDatabaseIdsPath != null)
                    WriteDatabaseIdsSnapshot(aofDatabaseIdsPath);
            }, token);

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

        void DoCompaction(ref GarnetDatabase db)
        {
            // Periodic compaction -> no need to compact before checkpointing
            if (serverOptions.CompactionFrequencySecs > 0) return;

            DoCompaction(ref db, serverOptions.CompactionMaxSegments, serverOptions.ObjectStoreCompactionMaxSegments, 1, serverOptions.CompactionType, serverOptions.CompactionForceDelete);
        }

        /// <summary>
        /// Append a checkpoint commit to the AOF
        /// </summary>
        /// <param name="isMainStore"></param>
        /// <param name="version"></param>
        /// <param name="dbId"></param>
        public void EnqueueCommit(bool isMainStore, long version, int dbId = 0)
        {
            AofHeader header = new()
            {
                opType = isMainStore ? AofEntryType.MainStoreCheckpointCommit : AofEntryType.ObjectStoreCheckpointCommit,
                storeVersion = version,
                sessionID = -1
            };

            var aof = databases.Map[dbId].AppendOnlyFile;
            aof?.Enqueue(header, out _);
        }

        void DoCompaction(ref GarnetDatabase db, int mainStoreMaxSegments, int objectStoreMaxSegments, int numSegmentsToCompact, LogCompactionType compactionType, bool compactionForceDelete)
        {
            if (compactionType == LogCompactionType.None) return;

            var mainStoreLog = db.MainStore.Log;

            long mainStoreMaxLogSize = (1L << serverOptions.SegmentSizeBits()) * mainStoreMaxSegments;

            if (mainStoreLog.ReadOnlyAddress - mainStoreLog.BeginAddress > mainStoreMaxLogSize)
            {
                long readOnlyAddress = mainStoreLog.ReadOnlyAddress;
                long compactLength = (1L << serverOptions.SegmentSizeBits()) * (mainStoreMaxSegments - numSegmentsToCompact);
                long untilAddress = readOnlyAddress - compactLength;
                logger?.LogInformation("Begin main store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, mainStoreLog.BeginAddress, readOnlyAddress, mainStoreLog.TailAddress);

                switch (compactionType)
                {
                    case LogCompactionType.Shift:
                        mainStoreLog.ShiftBeginAddress(untilAddress, true, compactionForceDelete);
                        break;

                    case LogCompactionType.Scan:
                        mainStoreLog.Compact<SpanByte, Empty, Empty, SpanByteFunctions<Empty, Empty>>(new SpanByteFunctions<Empty, Empty>(), untilAddress, CompactionType.Scan);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof(ref db);
                            mainStoreLog.Truncate();
                        }
                        break;

                    case LogCompactionType.Lookup:
                        mainStoreLog.Compact<SpanByte, Empty, Empty, SpanByteFunctions<Empty, Empty>>(new SpanByteFunctions<Empty, Empty>(), untilAddress, CompactionType.Lookup);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof(ref db);
                            mainStoreLog.Truncate();
                        }
                        break;

                    default:
                        break;
                }

                logger?.LogInformation("End main store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, mainStoreLog.BeginAddress, readOnlyAddress, mainStoreLog.TailAddress);
            }

            if (db.ObjectStore == null) return;

            var objectStoreLog = db.ObjectStore.Log;

            long objectStoreMaxLogSize = (1L << serverOptions.ObjectStoreSegmentSizeBits()) * objectStoreMaxSegments;

            if (objectStoreLog.ReadOnlyAddress - objectStoreLog.BeginAddress > objectStoreMaxLogSize)
            {
                long readOnlyAddress = objectStoreLog.ReadOnlyAddress;
                long compactLength = (1L << serverOptions.ObjectStoreSegmentSizeBits()) * (objectStoreMaxSegments - numSegmentsToCompact);
                long untilAddress = readOnlyAddress - compactLength;
                logger?.LogInformation("Begin object store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, objectStoreLog.BeginAddress, readOnlyAddress, objectStoreLog.TailAddress);

                switch (compactionType)
                {
                    case LogCompactionType.Shift:
                        objectStoreLog.ShiftBeginAddress(untilAddress, compactionForceDelete);
                        break;

                    case LogCompactionType.Scan:
                        objectStoreLog.Compact<IGarnetObject, IGarnetObject, Empty, SimpleSessionFunctions<byte[], IGarnetObject, Empty>>(
                            new SimpleSessionFunctions<byte[], IGarnetObject, Empty>(), untilAddress, CompactionType.Scan);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof(ref db);
                            objectStoreLog.Truncate();
                        }
                        break;

                    case LogCompactionType.Lookup:
                        objectStoreLog.Compact<IGarnetObject, IGarnetObject, Empty, SimpleSessionFunctions<byte[], IGarnetObject, Empty>>(
                            new SimpleSessionFunctions<byte[], IGarnetObject, Empty>(), untilAddress, CompactionType.Lookup);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof(ref db);
                            objectStoreLog.Truncate();
                        }
                        break;

                    default:
                        break;
                }

                logger?.LogInformation("End object store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, mainStoreLog.BeginAddress, readOnlyAddress, mainStoreLog.TailAddress);
            }
        }

        void CompactionCommitAof(ref GarnetDatabase db)
        {
            // If we are the primary, we commit the AOF.
            // If we are the replica, we commit the AOF only if fast commit is disabled
            // because we do not want to clobber AOF addresses.
            // TODO: replica should instead wait until the next AOF commit is done via primary
            if (serverOptions.EnableAOF)
            {
                if (serverOptions.EnableCluster && clusterProvider.IsReplica())
                {
                    if (!serverOptions.EnableFastCommit)
                        db.AppendOnlyFile?.CommitAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                }
                else
                {
                    db.AppendOnlyFile?.CommitAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                }
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
        internal void WaitForCommit()
        {
            if (!serverOptions.EnableAOF || appendOnlyFile == null) return;

            var activeDbIdsSize = this.activeDbIdsLength;
            if (!allowMultiDb || activeDbIdsSize == 1)
            {
                appendOnlyFile.WaitForCommit();
                return;
            }

            var databasesMapSnapshot = databases.Map;
            var activeDbIdsSnapshot = activeDbIds;

            var tasks = new Task[activeDbIdsSize];
            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                var db = databasesMapSnapshot[dbId];
                tasks[i] = db.AppendOnlyFile.WaitForCommitAsync().AsTask();
            }

            Task.WaitAll(tasks);
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

            var databasesMapSnapshot = databases.Map;
            var activeDbIdsSnapshot = activeDbIds;

            var tasks = new Task[activeDbIdsSize];
            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                var db = databasesMapSnapshot[dbId];
                tasks[i] = db.AppendOnlyFile.WaitForCommitAsync(token: token).AsTask();
            }

            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Asynchronously wait for AOF commits on all active databases
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>ValueTask</returns>
        internal async ValueTask CommitAOFAsync(CancellationToken token = default)
        {
            if (!serverOptions.EnableAOF || appendOnlyFile == null) return;

            var activeDbIdsSize = this.activeDbIdsLength;
            if (!allowMultiDb || activeDbIdsSize == 1)
            {
                await appendOnlyFile.CommitAsync(token: token);
                return;
            }

            var databasesMapSnapshot = databases.Map;
            var activeDbIdsSnapshot = activeDbIds;

            var tasks = new Task[activeDbIdsSize];
            for (var i = 0; i < activeDbIdsSize; i++)
            {
                var dbId = activeDbIdsSnapshot[i];
                var db = databasesMapSnapshot[dbId];
                tasks[i] = db.AppendOnlyFile.CommitAsync(token: token).AsTask();
            }

            await Task.WhenAll(tasks);
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
                var databaseDataInitialized = new bool[serverOptions.MaxDatabases];
                var databaseMainStoreIndexMaxedOut = new bool[serverOptions.MaxDatabases];
                var databaseObjectStoreIndexMaxedOut = new bool[serverOptions.MaxDatabases];

                var allIndexesMaxedOut = false;

                while (!allIndexesMaxedOut)
                {
                    allIndexesMaxedOut = true;

                    if (token.IsCancellationRequested) break;

                    await Task.Delay(TimeSpan.FromSeconds(serverOptions.IndexResizeFrequencySecs), token);

                    var databasesMapSnapshot = databases.Map;

                    var activeDbIdsSize = activeDbIdsLength;
                    var activeDbIdsSnapshot = activeDbIds;

                    for (var i = 0; i < activeDbIdsSize; i++)
                    {
                        var dbId = activeDbIdsSnapshot[i];
                        if (!databaseDataInitialized[dbId])
                        {
                            Debug.Assert(!databasesMapSnapshot[dbId].IsDefault());
                            databaseMainStoreIndexMaxedOut[dbId] = serverOptions.AdjustedIndexMaxCacheLines == 0;
                            databaseObjectStoreIndexMaxedOut[dbId] = serverOptions.AdjustedObjectStoreIndexMaxCacheLines == 0;
                            databaseDataInitialized[dbId] = true;
                        }

                        if (!databaseMainStoreIndexMaxedOut[dbId])
                        {
                            var dbMainStore = databasesMapSnapshot[dbId].MainStore;
                            databaseMainStoreIndexMaxedOut[dbId] = GrowIndexIfNeeded(StoreType.Main,
                                serverOptions.AdjustedIndexMaxCacheLines, dbMainStore.OverflowBucketAllocations,
                                () => dbMainStore.IndexSize, () => dbMainStore.GrowIndex());

                            if (!databaseMainStoreIndexMaxedOut[dbId])
                                allIndexesMaxedOut = false;
                        }

                        if (!databaseObjectStoreIndexMaxedOut[dbId])
                        {
                            var dbObjectStore = databasesMapSnapshot[dbId].ObjectStore;
                            databaseObjectStoreIndexMaxedOut[dbId] = GrowIndexIfNeeded(StoreType.Object,
                                serverOptions.AdjustedObjectStoreIndexMaxCacheLines,
                                dbObjectStore.OverflowBucketAllocations,
                                () => dbObjectStore.IndexSize, () => dbObjectStore.GrowIndex());

                            if (!databaseObjectStoreIndexMaxedOut[dbId])
                                allIndexesMaxedOut = false;
                        }
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
            // Wait for checkpoints to complete and disable checkpointing
            checkpointTaskLock.WriteLock();

            itemBroker?.Dispose();
            monitor?.Dispose();
            ctsCommit?.Cancel();

            while (objectStoreSizeTracker != null && !objectStoreSizeTracker.Stopped)
                Thread.Yield();

            // Disable changes to databases map and dispose all databases
            databases.mapLock.WriteLock();
            foreach (var db in databases.Map)
                db.Dispose();

            ctsCommit?.Dispose();
            clusterProvider?.Dispose();
        }

        /// <summary>
        /// Mark the beginning of a checkpoint by taking and a lock to avoid concurrent checkpoint tasks
        /// </summary>
        /// <returns></returns>
        public bool TryPauseCheckpoints()
            => checkpointTaskLock.TryWriteLock();

        /// <summary>
        /// Release checkpoint task lock
        /// </summary>
        public void ResumeCheckpoints()
            => checkpointTaskLock.WriteUnlock();

        /// <summary>
        /// Take a checkpoint if no checkpoint was taken after the provided time offset
        /// </summary>
        /// <param name="entryTime"></param>
        /// <param name="dbId"></param>
        /// <returns></returns>
        public async Task TakeOnDemandCheckpoint(DateTimeOffset entryTime, int dbId = 0)
        {
            // Take lock to ensure no other task will be taking a checkpoint
            while (!TryPauseCheckpoints())
                await Task.Yield();

            // If an external task has taken a checkpoint beyond the provided entryTime return
            if (databases.Map[dbId].LastSaveTime > entryTime)
            {
                ResumeCheckpoints();
                return;
            }

            // Necessary to take a checkpoint because the latest checkpoint is before entryTime
            await CheckpointTask(StoreType.All, dbId, lockAcquired: true, logger: logger);
        }

        /// <summary>
        /// Take checkpoint of all active databases
        /// </summary>
        /// <param name="background"></param>
        /// <param name="storeType"></param>
        /// <param name="dbId"></param>
        /// <param name="logger"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public bool TakeCheckpoint(bool background, StoreType storeType = StoreType.All, int dbId = -1, ILogger logger = null, CancellationToken token = default)
        {
            var activeDbIdsSize = activeDbIdsLength;

            if (!allowMultiDb || activeDbIdsSize == 1 || dbId != -1)
            {
                if (dbId == -1) dbId = 0;
                var checkpointTask = Task.Run(async () => await CheckpointTask(storeType, dbId, logger: logger), token);
                if (background)
                    return true;
                
                checkpointTask.Wait(token);
                return true;
            }

            var activeDbIdsSnapshot = activeDbIds;

            var tasks = new Task[activeDbIdsSize];
            return CheckpointDatabases(storeType, ref activeDbIdsSnapshot, ref tasks, logger: logger, token: token);
        }

        /// <summary>
        /// Asynchronously checkpoint a single database
        /// </summary>
        /// <param name="storeType">Store type to checkpoint</param>
        /// <param name="dbId">ID of database to checkpoint (default: DB 0)</param>
        /// <param name="lockAcquired">True if lock previously acquired</param>
        /// <param name="logger">Logger</param>
        /// <returns>Task</returns>
        private async Task CheckpointTask(StoreType storeType, int dbId = 0, bool lockAcquired = false, ILogger logger = null)
        {
            try
            {
                if (!lockAcquired)
                {
                    // Take lock to ensure no other task will be taking a checkpoint
                    while (!TryPauseCheckpoints())
                        await Task.Yield();
                }

                await CheckpointDatabaseTask(dbId, storeType, logger);

                if (checkpointDatabaseIdsPath != null)
                    WriteDatabaseIdsSnapshot(checkpointDatabaseIdsPath);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Checkpointing threw exception");
            }
            finally
            {
                ResumeCheckpoints();
            }
        }

        /// <summary>
        /// Asynchronously checkpoint multiple databases and wait for all to complete
        /// </summary>
        /// <param name="storeType">Store type to checkpoint</param>
        /// <param name="dbIds">IDs of active databases to checkpoint</param>
        /// <param name="tasks">Optional tasks to use for asynchronous execution (must be the same size as dbIds)</param>
        /// <param name="logger">Logger</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>False if checkpointing already in progress</returns>
        private bool CheckpointDatabases(StoreType storeType, ref int[] dbIds, ref Task[] tasks, ILogger logger = null, CancellationToken token = default)
        {
            try
            {
                Debug.Assert(tasks != null);

                // Prevent parallel checkpoint
                if (!TryPauseCheckpoints()) return false;

                var currIdx = 0;
                if (dbIds == null)
                {
                    var activeDbIdsSize = activeDbIdsLength;
                    var activeDbIdsSnapshot = activeDbIds;
                    while(currIdx < activeDbIdsSize)
                    {
                        var dbId = activeDbIdsSnapshot[currIdx];
                        tasks[currIdx] = CheckpointDatabaseTask(dbId, storeType, logger);
                        currIdx++;
                    }
                }
                else
                {
                    while (currIdx < dbIds.Length && (currIdx == 0 || dbIds[currIdx] != 0))
                    {
                        tasks[currIdx] = CheckpointDatabaseTask(dbIds[currIdx], storeType, logger);
                        currIdx++;
                    }
                }

                for (var i = 0; i < currIdx; i++)
                    tasks[i].Wait(token);

                if (checkpointDatabaseIdsPath != null)
                    WriteDatabaseIdsSnapshot(checkpointDatabaseIdsPath);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Checkpointing threw exception");
            }
            finally
            {
                ResumeCheckpoints();
            }

            return true;
        }

        /// <summary>
        /// Asynchronously checkpoint a single database
        /// </summary>
        /// <param name="dbId">ID of database to checkpoint</param>
        /// <param name="storeType">Store type to checkpoint</param>
        /// <param name="logger">Logger</param>
        /// <returns>Task</returns>
        private async Task CheckpointDatabaseTask(int dbId, StoreType storeType, ILogger logger)
        {
            var databasesMapSnapshot = databases.Map;
            var db = databasesMapSnapshot[dbId];
            if (db.IsDefault()) return;

            DoCompaction(ref db);
            var lastSaveStoreTailAddress = db.MainStore.Log.TailAddress;
            var lastSaveObjectStoreTailAddress = (db.ObjectStore?.Log.TailAddress).GetValueOrDefault();

            var full = db.LastSaveStoreTailAddress == 0 ||
                       lastSaveStoreTailAddress - db.LastSaveStoreTailAddress >= serverOptions.FullCheckpointLogInterval ||
                       (db.ObjectStore != null && (db.LastSaveObjectStoreTailAddress == 0 ||
                                                   lastSaveObjectStoreTailAddress - db.LastSaveObjectStoreTailAddress >= serverOptions.FullCheckpointLogInterval));

            var tryIncremental = serverOptions.EnableIncrementalSnapshots;
            if (db.MainStore.IncrementalSnapshotTailAddress >= serverOptions.IncrementalSnapshotLogSizeLimit)
                tryIncremental = false;
            if (db.ObjectStore?.IncrementalSnapshotTailAddress >= serverOptions.IncrementalSnapshotLogSizeLimit)
                tryIncremental = false;

            var checkpointType = serverOptions.UseFoldOverCheckpoints ? CheckpointType.FoldOver : CheckpointType.Snapshot;
            await InitiateCheckpoint(dbId, db, full, checkpointType, tryIncremental, storeType, logger);
            if (full)
            {
                if (storeType is StoreType.Main or StoreType.All)
                    db.LastSaveStoreTailAddress = lastSaveStoreTailAddress;
                if (storeType is StoreType.Object or StoreType.All)
                    db.LastSaveObjectStoreTailAddress = lastSaveObjectStoreTailAddress;
            }

            var lastSave = DateTimeOffset.UtcNow;
            if (dbId == 0)
                lastSaveTime = lastSave;
            db.LastSaveTime = lastSave;
        }

        private async Task InitiateCheckpoint(int dbId, GarnetDatabase db, bool full, CheckpointType checkpointType, bool tryIncremental, StoreType storeType, ILogger logger = null)
        {
            logger?.LogInformation("Initiating checkpoint; full = {full}, type = {checkpointType}, tryIncremental = {tryIncremental}, storeType = {storeType}, dbId = {dbId}", full, checkpointType, tryIncremental, storeType, dbId);

            long CheckpointCoveredAofAddress = 0;
            if (db.AppendOnlyFile != null)
            {
                if (serverOptions.EnableCluster)
                    clusterProvider.OnCheckpointInitiated(out CheckpointCoveredAofAddress);
                else
                    CheckpointCoveredAofAddress = db.AppendOnlyFile.TailAddress;

                if (CheckpointCoveredAofAddress > 0)
                    logger?.LogInformation("Will truncate AOF to {tailAddress} after checkpoint (files deleted after next commit)", CheckpointCoveredAofAddress);
            }

            (bool success, Guid token) storeCheckpointResult = default;
            (bool success, Guid token) objectStoreCheckpointResult = default;
            if (full)
            {
                if (storeType is StoreType.Main or StoreType.All)
                    storeCheckpointResult = await db.MainStore.TakeFullCheckpointAsync(checkpointType);

                if (db.ObjectStore != null && (storeType == StoreType.Object || storeType == StoreType.All))
                    objectStoreCheckpointResult = await db.ObjectStore.TakeFullCheckpointAsync(checkpointType);
            }
            else
            {
                if (storeType is StoreType.Main or StoreType.All)
                    storeCheckpointResult = await db.MainStore.TakeHybridLogCheckpointAsync(checkpointType, tryIncremental);

                if (db.ObjectStore != null && (storeType == StoreType.Object || storeType == StoreType.All))
                    objectStoreCheckpointResult = await db.ObjectStore.TakeHybridLogCheckpointAsync(checkpointType, tryIncremental);
            }

            // If cluster is enabled the replication manager is responsible for truncating AOF
            if (serverOptions.EnableCluster && serverOptions.EnableAOF)
            {
                clusterProvider.SafeTruncateAOF(storeType, full, CheckpointCoveredAofAddress, storeCheckpointResult.token, objectStoreCheckpointResult.token);
            }
            else
            {
                db.AppendOnlyFile?.TruncateUntil(CheckpointCoveredAofAddress);
                db.AppendOnlyFile?.Commit();
            }

            if (db.ObjectStore != null)
            {
                // During the checkpoint, we may have serialized Garnet objects in (v) versions of objects.
                // We can now safely remove these serialized versions as they are no longer needed.
                using (var iter1 = db.ObjectStore.Log.Scan(db.ObjectStore.Log.ReadOnlyAddress, db.ObjectStore.Log.TailAddress, ScanBufferingMode.SinglePageBuffering, includeSealedRecords: true))
                {
                    while (iter1.GetNext(out _, out _, out var value))
                    {
                        if (value != null)
                            ((GarnetObjectBase)value).serialized = null;
                    }
                }
            }

            logger?.LogInformation("Completed checkpoint");
        }

        public bool HasKeysInSlots(List<int> slots, int dbId = 0)
        {
            if (slots.Count > 0)
            {
                var dbFound = TryGetDatabase(dbId, out var db);
                Debug.Assert(dbFound);

                bool hasKeyInSlots = false;
                {
                    using var iter = db.MainStore.Iterate<SpanByte, SpanByte, Empty, SimpleSessionFunctions<SpanByte, SpanByte, Empty>>(new SimpleSessionFunctions<SpanByte, SpanByte, Empty>());
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

                if (!hasKeyInSlots && db.ObjectStore != null)
                {
                    var functionsState = CreateFunctionsState();
                    var objstorefunctions = new ObjectSessionFunctions(functionsState);
                    var objectStoreSession = db.ObjectStore?.NewSession<ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions>(objstorefunctions);
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
        /// Get database DB ID
        /// </summary>
        /// <param name="dbId">DB Id</param>
        /// <param name="db">Database</param>
        /// <returns>True if database found</returns>
        public bool TryGetDatabase(int dbId, out GarnetDatabase db)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;

            if (dbId == 0)
            {
                db = databasesMapSnapshot[0];
                Debug.Assert(!db.IsDefault());
                return true;
            }

            // Check if database already exists
            if (dbId < databasesMapSize)
            {
                db = databasesMapSnapshot[dbId];
                if (!db.IsDefault()) return true;
            }

            // Try to retrieve or add database
            return this.TryGetOrAddDatabase(dbId, out db);
        }

        /// <summary>
        /// Copy active databases from specified StoreWrapper instance
        /// </summary>
        /// <param name="src">Source StoreWrapper</param>
        /// <param name="recordToAof">True if should enable AOF in copied databases</param>
        private void CopyDatabases(StoreWrapper src, bool recordToAof)
        {
            var databasesMapSize = src.databases.ActualSize;
            var databasesMapSnapshot = src.databases.Map;

            for (var dbId = 0; dbId < databasesMapSize; dbId++)
            {
                var db = databasesMapSnapshot[dbId];
                if (db.IsDefault()) continue;

                var dbCopy = new GarnetDatabase(db.MainStore, db.ObjectStore, db.ObjectStoreSizeTracker,
                    recordToAof ? db.AofDevice : null, recordToAof ? db.AppendOnlyFile : null);
                this.TryAddDatabase(dbId, ref dbCopy);
            }

            InitializeFieldsFromDatabase(databases.Map[0]);
        }

        /// <summary>
        /// Initializes default fields to those of a specified database (i.e. DB 0)
        /// </summary>
        /// <param name="db">Input database</param>
        private void InitializeFieldsFromDatabase(GarnetDatabase db)
        {
            // Set fields to default database
            this.store = db.MainStore;
            this.objectStore = db.ObjectStore;
            this.objectStoreSizeTracker = db.ObjectStoreSizeTracker;
            this.appendOnlyFile = db.AppendOnlyFile;
            this.versionMap = db.VersionMap;
        }

        /// <summary>
        /// Serializes an array of active DB IDs to file (excluding DB 0)
        /// </summary>
        /// <param name="path">Path of serialized file</param>
        private void WriteDatabaseIdsSnapshot(string path)
        {
            var activeDbIdsSize = activeDbIdsLength;
            var activeDbIdsSnapshot = activeDbIds;

            var dbIdsWithLength = new int[activeDbIdsSize];
            dbIdsWithLength[0] = activeDbIdsSize - 1;
            Array.Copy(activeDbIdsSnapshot, 1, dbIdsWithLength, 1, activeDbIdsSize - 1);

            var dbIdData = new byte[sizeof(int) * dbIdsWithLength.Length];

            Buffer.BlockCopy(dbIdsWithLength, 0, dbIdData, 0, dbIdData.Length);
            databaseIdsStreamProvider.Write(path, dbIdData);
        }

        /// <summary>
        /// Recover databases from serialized DB IDs file
        /// </summary>
        /// <param name="path">Path of serialized file</param>
        private void RecoverDatabases(string path)
        {
            using var stream = databaseIdsStreamProvider.Read(path);
            using var streamReader = new BinaryReader(stream);

            if (streamReader.BaseStream.Length > 0)
            {
                // Read length
                var idsCount = streamReader.ReadInt32();

                // Read DB IDs
                var dbIds = new int[idsCount];
                var dbIdData = streamReader.ReadBytes((int)streamReader.BaseStream.Length);
                Buffer.BlockCopy(dbIdData, 0, dbIds, 0, dbIdData.Length);

                // Add databases
                foreach (var dbId in dbIds)
                {
                    this.TryGetOrAddDatabase(dbId, out _);
                }
            }
        }

        /// <summary>
        /// Handle a new database added
        /// </summary>
        /// <param name="dbId">ID of database added</param>
        private void HandleDatabaseAdded(int dbId)
        {
            // If size tracker exists and is stopped, start it (only if DB 0 size tracker is started as well)
            var db = databases.Map[dbId];
            if (dbId != 0 && objectStoreSizeTracker != null && !objectStoreSizeTracker.Stopped &&
                db.ObjectStoreSizeTracker != null && db.ObjectStoreSizeTracker.Stopped)
                db.ObjectStoreSizeTracker.Start(ctsCommit.Token);

            var dbIdIdx = Interlocked.Increment(ref lastActivatedDbId);

            // If there is no size increase needed for activeDbIds, set the added ID in the array
            if (activeDbIds != null && dbIdIdx < activeDbIds.Length)
            {
                activeDbIds[dbIdIdx] = dbId;
                Interlocked.Increment(ref activeDbIdsLength);
                return;
            }

            lock (activeDbIdsLock)
            {
                // Select the next size of activeDbIds (as multiple of 2 from the existing size)
                var newSize = activeDbIds?.Length ?? 1;
                while (dbIdIdx + 1 > newSize)
                {
                    newSize = Math.Min(this.serverOptions.MaxDatabases, newSize * 2);
                }

                // Set an updated instance of activeDbIds
                var activeDbIdsSnapshot = activeDbIds;
                var activeDbIdsUpdated = new int[newSize];
                
                if (activeDbIdsSnapshot != null)
                {
                    Array.Copy(activeDbIdsSnapshot, activeDbIdsUpdated, dbIdIdx);
                }

                // Set the last added ID
                activeDbIdsUpdated[dbIdIdx] = dbId;

                checkpointTasks = new Task[newSize];
                aofTasks = new Task[newSize];
                activeDbIds = activeDbIdsUpdated;
                activeDbIdsLength = dbIdIdx + 1;
            }
        }
    }
}