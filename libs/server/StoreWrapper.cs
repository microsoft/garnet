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
    public sealed class StoreWrapper : IDisposable
    {
        internal readonly string version;
        internal readonly string redisProtocolVersion;
        readonly IGarnetServer server;
        internal readonly long startupTime;

        /// <summary>
        /// Store
        /// </summary>
        public readonly TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> store;

        /// <summary>
        /// Object store
        /// </summary>
        public readonly TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore;

        /// <summary>
        /// Server options
        /// </summary>
        public readonly GarnetServerOptions serverOptions;

        /// <summary>
        /// Subscribe broker
        /// </summary>
        public readonly SubscribeBroker subscribeBroker;

        internal readonly IClusterProvider clusterProvider;

        /// <summary>
        /// Get server
        /// </summary>
        public GarnetServerTcp TcpServer => (GarnetServerTcp)server;

        /// <summary>
        /// Access control list governing all commands
        /// </summary>
        public readonly AccessControlList accessControlList;

        /// <summary>
        /// AOF
        /// </summary>
        public readonly TsavoriteLog appendOnlyFile;

        /// <summary>
        /// Last save time
        /// </summary>
        public DateTimeOffset lastSaveTime;
        internal long lastSaveStoreTailAddress;
        internal long lastSaveObjectStoreTailAddress;

        /// <summary>
        /// Logger factory
        /// </summary>
        public readonly ILoggerFactory loggerFactory;

        internal readonly CollectionItemBroker itemBroker;
        internal readonly CustomCommandManager customCommandManager;
        internal readonly GarnetServerMonitor monitor;
        internal readonly WatchVersionMap versionMap;

        internal readonly CacheSizeTracker objectStoreSizeTracker;

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
        internal readonly SlowLogContainer slowLogContainer;

        /// <summary>
        /// Lua script cache
        /// </summary>
        public readonly ConcurrentDictionary<ScriptHashKey, byte[]> storeScriptCache;

        /// <summary>
        /// Shared timeout manager for all <see cref="LuaRunner"/> across all sessions.
        /// </summary>
        internal readonly LuaTimeoutManager luaTimeoutManager;

        public readonly TimeSpan loggingFrequncy;

        /// <summary>
        /// NOTE: For now we support only a single database
        /// </summary>
        public readonly int databaseNum = 1;

        /// <summary>
        /// Constructor
        /// </summary>
        public StoreWrapper(
            string version,
            string redisProtocolVersion,
            IGarnetServer server,
            TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> store,
            TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> objectStore,
            CacheSizeTracker objectStoreSizeTracker,
            CustomCommandManager customCommandManager,
            TsavoriteLog appendOnlyFile,
            GarnetServerOptions serverOptions,
            SubscribeBroker subscribeBroker,
            AccessControlList accessControlList = null,
            IClusterFactory clusterFactory = null,
            ILoggerFactory loggerFactory = null
            )
        {
            this.version = version;
            this.redisProtocolVersion = redisProtocolVersion;
            this.server = server;
            this.startupTime = DateTimeOffset.UtcNow.Ticks;
            this.store = store;
            this.objectStore = objectStore;
            this.appendOnlyFile = appendOnlyFile;
            this.serverOptions = serverOptions;
            this.subscribeBroker = subscribeBroker;
            lastSaveTime = DateTimeOffset.FromUnixTimeSeconds(0);
            this.customCommandManager = customCommandManager;
            this.monitor = serverOptions.MetricsSamplingFrequency > 0 ? new GarnetServerMonitor(this, serverOptions, server, loggerFactory?.CreateLogger("GarnetServerMonitor")) : null;
            this.objectStoreSizeTracker = objectStoreSizeTracker;
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory?.CreateLogger("StoreWrapper");
            this.sessionLogger = loggerFactory?.CreateLogger("Session");
            // TODO Change map size to a reasonable number
            this.versionMap = new WatchVersionMap(1 << 16);
            this.accessControlList = accessControlList;
            this.GarnetObjectSerializer = new GarnetObjectSerializer(this.customCommandManager);
            this.loggingFrequncy = TimeSpan.FromSeconds(serverOptions.LoggingFrequency);

            if (serverOptions.SlowLogThreshold > 0)
                this.slowLogContainer = new SlowLogContainer(serverOptions.SlowLogMaxEntries);
            if (!serverOptions.DisableObjects)
                this.itemBroker = new CollectionItemBroker();

            // Initialize store scripting cache
            if (serverOptions.EnableLua)
            {
                this.storeScriptCache = [];

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
                if ((this.serverOptions.AuthSettings != null) && (this.serverOptions.AuthSettings.GetType().BaseType == typeof(AclAuthenticationSettings)))
                {
                    // Create a new access control list and register it with the authentication settings
                    AclAuthenticationSettings aclAuthenticationSettings = (AclAuthenticationSettings)this.serverOptions.AuthSettings;

                    if (!string.IsNullOrEmpty(aclAuthenticationSettings.AclConfigurationFile))
                    {
                        logger?.LogInformation("Reading ACL configuration file '{filepath}'", aclAuthenticationSettings.AclConfigurationFile);
                        this.accessControlList = new AccessControlList(aclAuthenticationSettings.DefaultPassword, aclAuthenticationSettings.AclConfigurationFile);
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

        internal FunctionsState CreateFunctionsState()
            => new(appendOnlyFile, versionMap, customCommandManager, null, objectStoreSizeTracker, GarnetObjectSerializer);

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
            long storeVersion = 0, objectStoreVersion = 0;
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
                    if (objectStore != null)
                    {
                        // Get store recover version
                        var _storeVersion = store.GetRecoverVersion();
                        // Get object store recover version
                        var _objectStoreVersion = objectStore.GetRecoverVersion();

                        // Choose the minimum common recover version for both stores
                        if (_storeVersion < _objectStoreVersion)
                            _objectStoreVersion = _storeVersion;
                        else if (objectStoreVersion > 0) // handle the case where object store was disabled at checkpointing time
                            _storeVersion = _objectStoreVersion;

                        // Recover to the minimum common recover version
                        storeVersion = store.Recover(recoverTo: _storeVersion);
                        objectStoreVersion = objectStore.Recover(recoverTo: _objectStoreVersion);
                        logger?.LogInformation("Recovered store to version {storeVersion} and object store to version {objectStoreVersion}", storeVersion, objectStoreVersion);
                    }
                    else
                    {
                        storeVersion = store.Recover();
                        logger?.LogInformation("Recovered store to version {storeVersion}", storeVersion);
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

            // After recovery, we check if store versions match
            if (objectStore != null && storeVersion != objectStoreVersion)
            {
                logger?.LogInformation("Main store and object store checkpoint versions do not match; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
                if (serverOptions.FailOnRecoveryError)
                    throw new GarnetException("Main store and object store checkpoint versions do not match");
            }
        }

        /// <summary>
        /// Recover AOF
        /// </summary>
        public void RecoverAOF()
        {
            if (appendOnlyFile == null) return;
            appendOnlyFile.Recover();
            logger?.LogInformation("Recovered AOF: begin address = {beginAddress}, tail address = {tailAddress}", appendOnlyFile.BeginAddress, appendOnlyFile.TailAddress);
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
                aofProcessor.Recover(untilAddress);
                aofProcessor.Dispose();
                replicationOffset = aofProcessor.ReplicationOffset;
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

        async Task AutoCheckpointBasedOnAofSizeLimit(long AofSizeLimit, CancellationToken token = default, ILogger logger = null)
        {
            try
            {
                while (true)
                {
                    await Task.Delay(1000);
                    if (token.IsCancellationRequested) break;
                    var currAofSize = appendOnlyFile.TailAddress - appendOnlyFile.BeginAddress;

                    if (currAofSize > AofSizeLimit)
                    {
                        logger?.LogInformation("Enforcing AOF size limit currentAofSize: {currAofSize} >  AofSizeLimit: {AofSizeLimit}", currAofSize, AofSizeLimit);
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
                        await appendOnlyFile.CommitAsync(null, token);
                        await Task.Delay(commitFrequencyMs, token);
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "CommitTask exception received, AOF tail address = {tailAddress}; AOF committed until address = {commitAddress}; ", appendOnlyFile.TailAddress, appendOnlyFile.CommittedUntilAddress);
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
                    DoCompaction(serverOptions.CompactionMaxSegments, serverOptions.ObjectStoreCompactionMaxSegments, 1, serverOptions.CompactionType, serverOptions.CompactionForceDelete);
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

        void DoCompaction()
        {
            // Periodic compaction -> no need to compact before checkpointing
            if (serverOptions.CompactionFrequencySecs > 0) return;

            DoCompaction(serverOptions.CompactionMaxSegments, serverOptions.ObjectStoreCompactionMaxSegments, 1, serverOptions.CompactionType, serverOptions.CompactionForceDelete);
        }

        /// <summary>
        /// Append a checkpoint commit to the AOF
        /// </summary>
        /// <param name="entryType"></param>
        /// <param name="version"></param>
        public void EnqueueCommit(AofEntryType entryType, long version)
        {
            AofHeader header = new()
            {
                opType = entryType,
                storeVersion = version,
                sessionID = -1
            };
            appendOnlyFile?.Enqueue(header, out _);
        }

        void DoCompaction(int mainStoreMaxSegments, int objectStoreMaxSegments, int numSegmentsToCompact, LogCompactionType compactionType, bool compactionForceDelete)
        {
            if (compactionType == LogCompactionType.None) return;

            long mainStoreMaxLogSize = (1L << serverOptions.SegmentSizeBits()) * mainStoreMaxSegments;

            if (store.Log.ReadOnlyAddress - store.Log.BeginAddress > mainStoreMaxLogSize)
            {
                long readOnlyAddress = store.Log.ReadOnlyAddress;
                long compactLength = (1L << serverOptions.SegmentSizeBits()) * (mainStoreMaxSegments - numSegmentsToCompact);
                long untilAddress = readOnlyAddress - compactLength;
                logger?.LogInformation("Begin main store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, store.Log.BeginAddress, readOnlyAddress, store.Log.TailAddress);

                switch (compactionType)
                {
                    case LogCompactionType.Shift:
                        store.Log.ShiftBeginAddress(untilAddress, true, compactionForceDelete);
                        break;

                    case LogCompactionType.Scan:
                        store.Log.Compact<SpanByte, Empty, Empty, SpanByteFunctions<Empty, Empty>>(new SpanByteFunctions<Empty, Empty>(), untilAddress, CompactionType.Scan);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof();
                            store.Log.Truncate();
                        }
                        break;

                    case LogCompactionType.Lookup:
                        store.Log.Compact<SpanByte, Empty, Empty, SpanByteFunctions<Empty, Empty>>(new SpanByteFunctions<Empty, Empty>(), untilAddress, CompactionType.Lookup);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof();
                            store.Log.Truncate();
                        }
                        break;

                    default:
                        break;
                }

                logger?.LogInformation("End main store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, store.Log.BeginAddress, readOnlyAddress, store.Log.TailAddress);
            }

            if (objectStore == null) return;

            long objectStoreMaxLogSize = (1L << serverOptions.ObjectStoreSegmentSizeBits()) * objectStoreMaxSegments;

            if (objectStore.Log.ReadOnlyAddress - objectStore.Log.BeginAddress > objectStoreMaxLogSize)
            {
                long readOnlyAddress = objectStore.Log.ReadOnlyAddress;
                long compactLength = (1L << serverOptions.ObjectStoreSegmentSizeBits()) * (objectStoreMaxSegments - numSegmentsToCompact);
                long untilAddress = readOnlyAddress - compactLength;
                logger?.LogInformation("Begin object store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, objectStore.Log.BeginAddress, readOnlyAddress, objectStore.Log.TailAddress);

                switch (compactionType)
                {
                    case LogCompactionType.Shift:
                        objectStore.Log.ShiftBeginAddress(untilAddress, compactionForceDelete);
                        break;

                    case LogCompactionType.Scan:
                        objectStore.Log.Compact<IGarnetObject, IGarnetObject, Empty, SimpleSessionFunctions<byte[], IGarnetObject, Empty>>(
                            new SimpleSessionFunctions<byte[], IGarnetObject, Empty>(), untilAddress, CompactionType.Scan);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof();
                            objectStore.Log.Truncate();
                        }
                        break;

                    case LogCompactionType.Lookup:
                        objectStore.Log.Compact<IGarnetObject, IGarnetObject, Empty, SimpleSessionFunctions<byte[], IGarnetObject, Empty>>(
                            new SimpleSessionFunctions<byte[], IGarnetObject, Empty>(), untilAddress, CompactionType.Lookup);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof();
                            objectStore.Log.Truncate();
                        }
                        break;

                    default:
                        break;
                }

                logger?.LogInformation("End object store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, store.Log.BeginAddress, readOnlyAddress, store.Log.TailAddress);
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

        internal void Start()
        {
            monitor?.Start();
            clusterProvider?.Start();
            luaTimeoutManager?.Start();

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
                bool indexMaxedOut = serverOptions.AdjustedIndexMaxCacheLines == 0;
                bool objectStoreIndexMaxedOut = serverOptions.AdjustedObjectStoreIndexMaxCacheLines == 0;
                while (!indexMaxedOut || !objectStoreIndexMaxedOut)
                {
                    if (token.IsCancellationRequested) break;

                    await Task.Delay(TimeSpan.FromSeconds(serverOptions.IndexResizeFrequencySecs), token);

                    if (!indexMaxedOut)
                        indexMaxedOut = GrowIndexIfNeeded(StoreType.Main, serverOptions.AdjustedIndexMaxCacheLines, store.OverflowBucketAllocations,
                            () => store.IndexSize, async () => await store.GrowIndexAsync());

                    if (!objectStoreIndexMaxedOut)
                        objectStoreIndexMaxedOut = GrowIndexIfNeeded(StoreType.Object, serverOptions.AdjustedObjectStoreIndexMaxCacheLines, objectStore.OverflowBucketAllocations,
                            () => objectStore.IndexSize, async () => await objectStore.GrowIndexAsync());
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

            itemBroker?.Dispose();
            monitor?.Dispose();
            luaTimeoutManager?.Dispose();
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
            await CheckpointTask(logger);
        }

        /// <summary>
        /// Take checkpoint
        /// </summary>
        /// <param name="background"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public bool TakeCheckpoint(bool background, ILogger logger = null)
        {
            // Prevent parallel checkpoint
            if (!TryPauseCheckpoints()) return false;
            if (background)
                Task.Run(async () => await CheckpointTask(logger));
            else
                CheckpointTask(logger).ConfigureAwait(false).GetAwaiter().GetResult();
            return true;
        }

        private async Task CheckpointTask(ILogger logger = null)
        {
            try
            {
                DoCompaction();
                var lastSaveStoreTailAddress = store.Log.TailAddress;
                var lastSaveObjectStoreTailAddress = (objectStore?.Log.TailAddress).GetValueOrDefault();

                var full = false;
                if (this.lastSaveStoreTailAddress == 0 || lastSaveStoreTailAddress - this.lastSaveStoreTailAddress >= serverOptions.FullCheckpointLogInterval)
                    full = true;
                if (objectStore != null && (this.lastSaveObjectStoreTailAddress == 0 || lastSaveObjectStoreTailAddress - this.lastSaveObjectStoreTailAddress >= serverOptions.FullCheckpointLogInterval))
                    full = true;

                var tryIncremental = serverOptions.EnableIncrementalSnapshots;
                if (store.IncrementalSnapshotTailAddress >= serverOptions.IncrementalSnapshotLogSizeLimit)
                    tryIncremental = false;
                if (objectStore?.IncrementalSnapshotTailAddress >= serverOptions.IncrementalSnapshotLogSizeLimit)
                    tryIncremental = false;

                var checkpointType = serverOptions.UseFoldOverCheckpoints ? CheckpointType.FoldOver : CheckpointType.Snapshot;
                await InitiateCheckpoint(full, checkpointType, tryIncremental, logger);
                if (full)
                {
                    this.lastSaveStoreTailAddress = lastSaveStoreTailAddress;
                    if (objectStore != null)
                        this.lastSaveObjectStoreTailAddress = lastSaveObjectStoreTailAddress;
                }
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

        private async Task InitiateCheckpoint(bool full, CheckpointType checkpointType, bool tryIncremental, ILogger logger = null)
        {
            logger?.LogInformation("Initiating checkpoint; full = {full}, type = {checkpointType}, tryIncremental = {tryIncremental}", full, checkpointType, tryIncremental);

            long CheckpointCoveredAofAddress = 0;
            if (appendOnlyFile != null)
            {
                if (serverOptions.EnableCluster)
                    clusterProvider.OnCheckpointInitiated(out CheckpointCoveredAofAddress);
                else
                    CheckpointCoveredAofAddress = appendOnlyFile.TailAddress;

                if (CheckpointCoveredAofAddress > 0)
                    logger?.LogInformation("Will truncate AOF to {tailAddress} after checkpoint (files deleted after next commit)", CheckpointCoveredAofAddress);
            }

            (bool success, Guid token) checkpointResult = default;
            IStateMachine sm;
            if (full)
            {
                sm = objectStore == null ?
                        Checkpoint.Full(store, checkpointType, out checkpointResult.token) :
                        Checkpoint.Full(store, objectStore, checkpointType, out checkpointResult.token);
            }
            else
            {
                tryIncremental = tryIncremental && store.CanTakeIncrementalCheckpoint(checkpointType, out checkpointResult.token);
                if (objectStore != null)
                    tryIncremental = tryIncremental && objectStore.CanTakeIncrementalCheckpoint(checkpointType, out var guid2) && checkpointResult.token == guid2;

                if (tryIncremental)
                {
                    sm = objectStore == null ?
                        Checkpoint.IncrementalHybridLogOnly(store, checkpointResult.token) :
                        Checkpoint.IncrementalHybridLogOnly(store, objectStore, checkpointResult.token);
                }
                else
                {
                    sm = objectStore == null ?
                        Checkpoint.HybridLogOnly(store, checkpointType, out checkpointResult.token) :
                        Checkpoint.HybridLogOnly(store, objectStore, checkpointType, out checkpointResult.token);
                }
            }

            checkpointResult.success = await serverOptions.StateMachineDriver.RunAsync(sm);

            // If cluster is enabled the replication manager is responsible for truncating AOF
            if (serverOptions.EnableCluster && serverOptions.EnableAOF)
            {
                clusterProvider.SafeTruncateAOF(StoreType.All, full, CheckpointCoveredAofAddress, checkpointResult.token, checkpointResult.token);
            }
            else
            {
                appendOnlyFile?.TruncateUntil(CheckpointCoveredAofAddress);
                appendOnlyFile?.Commit();
            }

            if (objectStore != null)
            {
                // During the checkpoint, we may have serialized Garnet objects in (v) versions of objects.
                // We can now safely remove these serialized versions as they are no longer needed.
                using (var iter1 = objectStore.Log.Scan(objectStore.Log.ReadOnlyAddress, objectStore.Log.TailAddress, ScanBufferingMode.SinglePageBuffering, includeSealedRecords: true))
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

        public void ExecuteFlushDb(RespCommand cmd, bool unsafeTruncateLog, byte databaseId)
        {
            store.Log.ShiftBeginAddress(store.Log.TailAddress, truncateLog: unsafeTruncateLog);
            objectStore?.Log.ShiftBeginAddress(objectStore.Log.TailAddress, truncateLog: unsafeTruncateLog);

            if (serverOptions.EnableCluster && serverOptions.EnableAOF)
            {
                clusterProvider.SafeTruncateAOF(appendOnlyFile.TailAddress);
                if (clusterProvider.IsPrimary())
                {
                    AofHeader header = new()
                    {
                        opType = cmd == RespCommand.FLUSHDB ? AofEntryType.FlushDb : AofEntryType.FlushAll,
                        storeVersion = 0,
                        sessionID = -1,
                        unsafeTruncateLog = unsafeTruncateLog ? (byte)0 : (byte)1,
                        databaseId = databaseId
                    };
                    appendOnlyFile?.Enqueue(header, out _);
                }
            }
            else
                appendOnlyFile?.TruncateUntil(appendOnlyFile.TailAddress);
        }
    }
}