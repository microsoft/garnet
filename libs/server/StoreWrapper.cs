// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        /// Store
        /// </summary>
        public TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> store;

        /// <summary>
        /// Object store
        /// </summary>
        public TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore;

        /// <summary>
        /// Server options
        /// </summary>
        public readonly GarnetServerOptions serverOptions;
        internal readonly IClusterProvider clusterProvider;

        /// <summary>
        /// Get server
        /// </summary>
        public GarnetServerTcp GetTcpServer() => (GarnetServerTcp)server;

        /// <summary>
        /// Access control list governing all commands
        /// </summary>
        public readonly AccessControlList accessControlList;

        /// <summary>
        /// AOF
        /// </summary>
        public TsavoriteLog appendOnlyFile;

        /// <summary>
        /// Last save time
        /// </summary>
        public DateTimeOffset lastSaveTime;

        /// <summary>
        /// Logger factory
        /// </summary>
        public readonly ILoggerFactory loggerFactory;

        internal readonly CollectionItemBroker itemBroker;
        internal readonly CustomCommandManager customCommandManager;
        internal readonly GarnetServerMonitor monitor;
        internal WatchVersionMap versionMap;

        internal CacheSizeTracker objectStoreSizeTracker;

        public readonly GarnetObjectSerializer GarnetObjectSerializer;

        /// <summary>
        /// The main logger instance associated with this store.
        /// </summary>
        public readonly ILogger logger;

        internal readonly ILogger sessionLogger;
        readonly CancellationTokenSource ctsCommit;

        internal long SafeAofAddress = -1;

        // Standalone instance node_id
        internal readonly string run_id;
        private SingleWriterMultiReaderLock _checkpointTaskLock;

        /// <summary>
        /// Lua script cache
        /// </summary>
        public readonly ConcurrentDictionary<ScriptHashKey, byte[]> storeScriptCache;

        public readonly TimeSpan loggingFrequncy;

        /// <summary>
        /// Number of current logical databases
        /// </summary>
        public int databaseCount = 1;

        /// <summary>
        /// Delegate for creating a new logical database
        /// </summary>
        internal readonly DatabaseCreatorDelegate createDatabasesDelegate;

        readonly bool allowMultiDb;
        internal ExpandableMap<GarnetDatabase> databases;

        string databaseIdsFileName = "dbIds.dat";
        string aofDatabaseIdsPath;
        string checkpointDatabaseIdsPath;
        IStreamProvider databaseIdsStreamProvider;

        public delegate GarnetDatabase DatabaseCreatorDelegate(int dbId, out string storeCheckpointDir, out string aofDir);

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
            this.loggingFrequncy = TimeSpan.FromSeconds(serverOptions.LoggingFrequency);

            this.allowMultiDb = this.serverOptions.MaxDatabases > 1;

            // Create databases map
            databases = new ExpandableMap<GarnetDatabase>(1, 0, this.serverOptions.MaxDatabases - 1);

            // Create default database of index 0 (unless specified otherwise)
            if (createDefaultDatabase)
            {
                var db = createDatabasesDelegate(0, out var storeCheckpointDir, out var aofPath);
                checkpointDatabaseIdsPath = storeCheckpointDir == null ? null : Path.Combine(storeCheckpointDir, databaseIdsFileName);
                aofDatabaseIdsPath = aofPath == null ? null : Path.Combine(aofPath, databaseIdsFileName);

                if (!databases.TrySetValue(0, ref db))
                    throw new GarnetException("Failed to set initial database in databases map");

                this.InitializeFieldsFromDatabase(databases.Map[0]);
            }

            if (allowMultiDb)
            {
                databaseIdsStreamProvider = serverOptions.StreamProviderCreator();
            }

            if (logger != null)
            {
                var configMemoryLimit = (store.IndexSize * 64) + store.Log.MaxMemorySizeBytes +
                                        (store.ReadCache?.MaxMemorySizeBytes ?? 0) +
                                        (appendOnlyFile?.MaxMemorySizeBytes ?? 0);
                if (objectStore != null)
                    configMemoryLimit += (objectStore.IndexSize * 64) + objectStore.Log.MaxMemorySizeBytes +
                                         (objectStore.ReadCache?.MaxMemorySizeBytes ?? 0) +
                                         (objectStoreSizeTracker?.TargetSize ?? 0) +
                                         (objectStoreSizeTracker?.ReadCacheTargetSize ?? 0);
                logger.LogInformation("Total configured memory limit: {configMemoryLimit}", configMemoryLimit);
            }

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
            run_id = Generator.CreateHexId();
        }

        /// <summary>
        /// Copy Constructor
        /// </summary>
        /// <param name="storeWrapper">Source instance</param>
        /// <param name="recordToAof"></param>
        public StoreWrapper(StoreWrapper storeWrapper, bool recordToAof) : this(
            storeWrapper.version,
            storeWrapper.redisProtocolVersion,
            storeWrapper.server,
            storeWrapper.createDatabasesDelegate,
            storeWrapper.customCommandManager,
            storeWrapper.serverOptions,
            createDefaultDatabase: false,
            storeWrapper.accessControlList,
            null,
            storeWrapper.loggerFactory)
        {
            this.clusterProvider = storeWrapper.clusterProvider;
            this.CopyDatabases(storeWrapper, recordToAof);
        }

        private void CopyDatabases(StoreWrapper src, bool recordToAof)
        {
            var databasesMapSize = src.databases.ActualSize;
            var databasesMapSnapshot = src.databases.Map;

            for (var dbId = 0; dbId < databasesMapSize; dbId++)
            {
                var db = databasesMapSnapshot[dbId];
                var dbCopy = new GarnetDatabase(db.MainStore, db.ObjectStore, db.ObjectSizeTracker,
                    recordToAof ? db.AofDevice : null, recordToAof ? db.AppendOnlyFile : null);
                databases.TrySetValue(dbId, ref dbCopy);
            }

            InitializeFieldsFromDatabase(databases.Map[0]);
        }

        private void InitializeFieldsFromDatabase(GarnetDatabase db)
        {
            // Set fields to default database
            this.store = db.MainStore;
            this.objectStore = db.ObjectStore;
            this.objectStoreSizeTracker = db.ObjectSizeTracker;
            this.appendOnlyFile = db.AppendOnlyFile;
            this.versionMap = db.VersionMap;
        }

        /// <summary>
        /// Get IP
        /// </summary>
        /// <returns></returns>
        public string GetIp()
        {
            var localEndpoint = GetTcpServer().GetEndPoint;
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
            
            if (!databases.TryGetOrSet(dbId, () => createDatabasesDelegate(dbId, out _, out _), out var db, out _))
                throw new GarnetException($"Database with ID {dbId} was not found.");
            
            return new(db.AppendOnlyFile, db.VersionMap, customCommandManager, null, db.ObjectSizeTracker,
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
                }
                else
                {
                    if (allowMultiDb && checkpointDatabaseIdsPath != null)
                        RecoverDatabases(checkpointDatabaseIdsPath);

                    var databasesMapSize = databases.ActualSize;
                    var databasesMapSnapshot = databases.Map;
                    for (var i = 0; i < databasesMapSize; i++)
                    {
                        var db = databasesMapSnapshot[i];
                        storeVersion = db.MainStore.Recover();
                        if (db.ObjectStore != null) objectStoreVersion = db.ObjectStore.Recover();
                    }
                }
                if (storeVersion > 0 || objectStoreVersion > 0)
                    lastSaveTime = DateTimeOffset.UtcNow;
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

        private void WriteDatabaseIdsSnapshot(int[] activeDbIds, int offset, int actualLength, string path)
        {
            var dbIdsWithLength = new int[actualLength + 1];
            dbIdsWithLength[0] = actualLength;
            Array.Copy(activeDbIds, offset, dbIdsWithLength, 1, actualLength);

            var dbIdData = new byte[sizeof(int) * dbIdsWithLength.Length];

            Buffer.BlockCopy(dbIdsWithLength, 0, dbIdData, 0, dbIdData.Length);
            databaseIdsStreamProvider.Write(path, dbIdData);
        }

        private void RecoverDatabases(string path)
        {
            using var stream = databaseIdsStreamProvider.Read(path);
            using var streamReader = new BinaryReader(stream);

            if (streamReader.BaseStream.Length > 0)
            {
                var idsCount = streamReader.ReadInt32();
                var dbIds = new int[idsCount];
                var dbIdData = streamReader.ReadBytes((int)streamReader.BaseStream.Length);
                Buffer.BlockCopy(dbIdData, 0, dbIds, 0, dbIdData.Length);

                var databasesMapSize = databases.ActualSize;
                var databasesMapSnapshot = databases.Map;

                foreach (var dbId in dbIds)
                {
                    if (dbId < databasesMapSize && !databasesMapSnapshot[dbId].IsDefault()) 
                        continue;

                    var db = createDatabasesDelegate(dbId, out _, out _);
                    databases.TrySetValue(dbId, ref db);
                }
            }
        }

        /// <summary>
        /// Recover AOF
        /// </summary>
        public void RecoverAOF()
        {
            if (allowMultiDb && aofDatabaseIdsPath != null)
                RecoverDatabases(aofDatabaseIdsPath);

            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            for (var i = 0; i < databasesMapSize; i++)
            {
                var db = databasesMapSnapshot[i];
                if (db.AppendOnlyFile == null) continue;
                db.AppendOnlyFile.Recover();
                logger?.LogInformation("Recovered AOF: begin address = {beginAddress}, tail address = {tailAddress}", db.AppendOnlyFile.BeginAddress, db.AppendOnlyFile.TailAddress);
            }
        }

        /// <summary>
        /// Reset
        /// </summary>
        public void Reset()
        {
            try
            {
                if (store.Log.TailAddress > 64)
                    store.Reset();
                if (objectStore?.Log.TailAddress > 64)
                    objectStore?.Reset();
                appendOnlyFile?.Reset();
                lastSaveTime = DateTimeOffset.FromUnixTimeSeconds(0);
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

                var databasesMapSize = databases.ActualSize;
                var databasesMapSnapshot = databases.Map;

                for (var dbId = 0; dbId < databasesMapSize; dbId++)
                {
                    if (databasesMapSnapshot[dbId].IsDefault()) continue;
                    aofProcessor.Recover(dbId, dbId == 0 ? untilAddress : -1);
                    if (dbId == 0)
                        replicationOffset = aofProcessor.ReplicationOffset;
                }

                aofProcessor.Dispose();
                lastSaveTime = DateTimeOffset.UtcNow;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Error during recovery of AofProcessor");
                if (serverOptions.FailOnRecoveryError)
                    throw;
            }
            return replicationOffset;
        }

        async Task AutoCheckpointBasedOnAofSizeLimit(long aofSizeLimit, CancellationToken token = default, ILogger logger = null)
        {
            try
            {
                while (true)
                {
                    await Task.Delay(1000);
                    if (token.IsCancellationRequested) break;

                    var aofSizeAtLimit = -1l;
                    var databasesMapSize = databases.ActualSize;
                    var databasesMapSnapshot = databases.Map;

                    for (var i = 0; i < databasesMapSize; i++)
                    {
                        var db = databasesMapSnapshot[i];
                        if (db.IsDefault()) continue;

                        var dbAofSize = db.AppendOnlyFile.TailAddress - db.AppendOnlyFile.BeginAddress;
                        if (dbAofSize > aofSizeLimit)
                        {
                            aofSizeAtLimit = dbAofSize;
                            break;
                        }
                    }
                    
                    if (aofSizeAtLimit != -1)
                    {
                        logger?.LogInformation("Enforcing AOF size limit currentAofSize: {currAofSize} >  AofSizeLimit: {AofSizeLimit}", aofSizeAtLimit, aofSizeLimit);
                        TakeCheckpoint(false, logger: logger);
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
                int[] activeDbIds = null;
                Task[] tasks = null;

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
                        var databasesMapSize = databases.ActualSize;

                        if (!allowMultiDb || databasesMapSize == 1)
                        {
                            await appendOnlyFile.CommitAsync(null, token);
                        }
                        else
                        {
                            MultiDatabaseCommit(ref activeDbIds, ref tasks, token);
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

        void MultiDatabaseCommit(ref int[] activeDbIds, ref Task[] tasks, CancellationToken token)
        {
            var databasesMapSize = databases.ActualSize;
            var databasesMapSnapshot = databases.Map;
            if (activeDbIds == null || activeDbIds.Length < databasesMapSize)
            {
                activeDbIds = new int[databasesMapSize];
                tasks = new Task[databasesMapSize];
            }

            var dbIdsIdx = 0;
            for (var dbId = 0; dbId < databasesMapSize; dbId++)
            {
                var db = databasesMapSnapshot[dbId];
                if (db.IsDefault()) continue;

                tasks[dbIdsIdx] = db.AppendOnlyFile.CommitAsync(null, token).AsTask();
                activeDbIds[dbIdsIdx] = dbId;

                dbIdsIdx++;
            }

            for (var i = 0; i < dbIdsIdx; i++)
            {
                tasks[i].Wait(token);
                tasks[i] = null;
            }

            if (aofDatabaseIdsPath != null && dbIdsIdx != 0)
                WriteDatabaseIdsSnapshot(activeDbIds, 1, dbIdsIdx - 1, aofDatabaseIdsPath);
        }

        async Task CompactionTask(int compactionFrequencySecs, CancellationToken token = default)
        {
            Debug.Assert(compactionFrequencySecs > 0);
            try
            {
                while (true)
                {
                    if (token.IsCancellationRequested) return;

                    var databasesMapSize = databases.ActualSize;
                    var databasesMapSnapshot = databases.Map;

                    for (var i = 0; i < databasesMapSize; i++)
                    {
                        var db = databasesMapSnapshot[i];
                        if (db.IsDefault()) continue;

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
        public void EnqueueCommit(bool isMainStore, long version)
        {
            AofHeader header = new()
            {
                opType = isMainStore ? AofEntryType.MainStoreCheckpointCommit : AofEntryType.ObjectStoreCheckpointCommit,
                storeVersion = version,
                sessionID = -1
            };
            appendOnlyFile?.Enqueue(header, out _);
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
                            CompactionCommitAof();
                            mainStoreLog.Truncate();
                        }
                        break;

                    case LogCompactionType.Lookup:
                        mainStoreLog.Compact<SpanByte, Empty, Empty, SpanByteFunctions<Empty, Empty>>(new SpanByteFunctions<Empty, Empty>(), untilAddress, CompactionType.Lookup);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof();
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
                logger?.LogInformation("Begin object store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, objectStore.Log.BeginAddress, readOnlyAddress, objectStore.Log.TailAddress);

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
                            CompactionCommitAof();
                            objectStoreLog.Truncate();
                        }
                        break;

                    case LogCompactionType.Lookup:
                        objectStoreLog.Compact<IGarnetObject, IGarnetObject, Empty, SimpleSessionFunctions<byte[], IGarnetObject, Empty>>(
                            new SimpleSessionFunctions<byte[], IGarnetObject, Empty>(), untilAddress, CompactionType.Lookup);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof();
                            objectStoreLog.Truncate();
                        }
                        break;

                    default:
                        break;
                }

                logger?.LogInformation("End object store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, mainStoreLog.BeginAddress, readOnlyAddress, mainStoreLog.TailAddress);
            }
        }

        void CompactionCommitAof()
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
                        appendOnlyFile?.CommitAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                }
                else
                {
                    appendOnlyFile?.CommitAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }
        }

        internal void CommitAOF(bool spinWait)
        {
            var databasesMapSize = databases.ActualSize;
            if (!allowMultiDb || databasesMapSize == 1)
            {
                appendOnlyFile.Commit(spinWait);
                return;
            }

            int[] activeDbIds = null;
            Task[] tasks = null;

            MultiDatabaseCommit(ref activeDbIds, ref tasks, CancellationToken.None);
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
        }

        /// <summary>Grows indexes of both main store and object store if current size is too small.</summary>
        /// <param name="token"></param>
        private async void IndexAutoGrowTask(CancellationToken token)
        {
            try
            {
                var databaseMainStoreIndexMaxedOut = new bool[serverOptions.MaxDatabases];
                var databaseObjectStoreIndexMaxedOut = new bool[serverOptions.MaxDatabases];

                var databasesMapSize = databases.ActualSize;
                var databasesMapSnapshot = databases.Map;

                for (var i = 0; i < databasesMapSize; i++)
                {
                    if (databasesMapSnapshot[i].IsDefault()) continue;

                    databaseMainStoreIndexMaxedOut[i] = serverOptions.AdjustedIndexMaxCacheLines == 0;
                    databaseObjectStoreIndexMaxedOut[i] = serverOptions.AdjustedObjectStoreIndexMaxCacheLines == 0;
                }

                var allIndexesMaxedOut = false;

                while (!allIndexesMaxedOut)
                {
                    allIndexesMaxedOut = true;

                    if (token.IsCancellationRequested) break;

                    await Task.Delay(TimeSpan.FromSeconds(serverOptions.IndexResizeFrequencySecs), token);

                    databasesMapSize = databases.ActualSize;
                    databasesMapSnapshot = databases.Map;

                    for (var i = 0; i < databasesMapSize; i++)
                    {
                        if (!databaseMainStoreIndexMaxedOut[i])
                        {
                            var dbMainStore = databasesMapSnapshot[i].MainStore;
                            databaseMainStoreIndexMaxedOut[i] = GrowIndexIfNeeded(StoreType.Main,
                                serverOptions.AdjustedIndexMaxCacheLines, dbMainStore.OverflowBucketAllocations,
                                () => dbMainStore.IndexSize, () => dbMainStore.GrowIndex());

                            if (!databaseMainStoreIndexMaxedOut[i])
                                allIndexesMaxedOut = false;
                        }

                        if (!databaseObjectStoreIndexMaxedOut[i])
                        {
                            var dbObjectStore = databasesMapSnapshot[i].ObjectStore;
                            databaseObjectStoreIndexMaxedOut[i] = GrowIndexIfNeeded(StoreType.Object,
                                serverOptions.AdjustedObjectStoreIndexMaxCacheLines,
                                dbObjectStore.OverflowBucketAllocations,
                                () => dbObjectStore.IndexSize, () => dbObjectStore.GrowIndex());

                            if (!databaseObjectStoreIndexMaxedOut[i])
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
            _checkpointTaskLock.WriteLock();

            // Disable changes to databases map and dispose all databases
            databases.mapLock.WriteLock();
            foreach (var db in databases.Map)
                db.Dispose();

            itemBroker?.Dispose();
            monitor?.Dispose();
            ctsCommit?.Cancel();

            while (objectStoreSizeTracker != null && !objectStoreSizeTracker.Stopped)
                Thread.Yield();

            ctsCommit?.Dispose();
            clusterProvider?.Dispose();
        }

        /// <summary>
        /// Mark the beginning of a checkpoint by taking and a lock to avoid concurrent checkpoint tasks
        /// </summary>
        /// <returns></returns>
        public bool TryPauseCheckpoints()
            => _checkpointTaskLock.TryWriteLock();

        /// <summary>
        /// Release checkpoint task lock
        /// </summary>
        public void ResumeCheckpoints()
            => _checkpointTaskLock.WriteUnlock();

        /// <summary>
        /// Take a checkpoint if no checkpoint was taken after the provided time offset
        /// </summary>
        /// <param name="entryTime"></param>
        /// <returns></returns>
        public async Task TakeOnDemandCheckpoint(DateTimeOffset entryTime)
        {
            // Take lock to ensure no other task will be taking a checkpoint
            while (!TryPauseCheckpoints())
                await Task.Yield();

            // If an external task has taken a checkpoint beyond the provided entryTime return
            if (this.lastSaveTime > entryTime)
            {
                ResumeCheckpoints();
                return;
            }

            // Necessary to take a checkpoint because the latest checkpoint is before entryTime
            await CheckpointTask(StoreType.All, logger: logger);
        }

        /// <summary>
        /// Take checkpoint
        /// </summary>
        /// <param name="background"></param>
        /// <param name="storeType"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public bool TakeCheckpoint(bool background, StoreType storeType = StoreType.All, ILogger logger = null)
        {
            // Prevent parallel checkpoint
            if (!TryPauseCheckpoints()) return false;
            if (background)
                Task.Run(async () => await CheckpointTask(storeType, logger));
            else
                CheckpointTask(storeType, logger).ConfigureAwait(false).GetAwaiter().GetResult();
            return true;
        }

        private async Task CheckpointTask(StoreType storeType, ILogger logger = null)
        {
            try
            {
                var databasesMapSize = databases.ActualSize;
                var databasesMapSnapshot = databases.Map;

                var activeDbIds = allowMultiDb ? new List<int>() : null;

                for (var dbId = 0; dbId < databasesMapSize; dbId++)
                {
                    var db = databasesMapSnapshot[dbId];
                    if (db.IsDefault()) continue;

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

                    if(allowMultiDb && dbId != 0)
                        activeDbIds!.Add(dbId);
                }

                if (allowMultiDb && checkpointDatabaseIdsPath != null && activeDbIds!.Count > 0)
                    WriteDatabaseIdsSnapshot(activeDbIds.ToArray(), 0, activeDbIds.Count, checkpointDatabaseIdsPath);

                lastSaveTime = DateTimeOffset.UtcNow;
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
                    var functionsState = CreateFunctionsState();
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

        public void GetDatabaseStores(int dbId,
            out TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> mainStore,
            out TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objStore)
        {
            if (dbId == 0)
            {
                mainStore = this.store;
                objStore = this.objectStore;
            }
            else
            {
                var dbFound = this.TryGetOrSetDatabase(dbId, out var db);
                Debug.Assert(dbFound);
                mainStore = db.MainStore;
                objStore = db.ObjectStore;
            }
        }

        public bool TryGetOrSetDatabase(int dbId, out GarnetDatabase db)
        {
            db = default;

            if (!allowMultiDb || !databases.TryGetOrSet(dbId, () => createDatabasesDelegate(dbId, out _, out _), out db, out var added))
                return false;

            if (added)
                Interlocked.Increment(ref databaseCount);
            
            return true;
        }
    }
}