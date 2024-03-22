// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Garnet.server.ACL;
using Garnet.server.Auth;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Wrapper for store and store-specific information
    /// </summary>   
    public sealed class StoreWrapper
    {
        internal readonly string version;
        readonly IGarnetServer server;
        internal readonly long startupTime;

        /// <summary>
        /// Store
        /// </summary>
        public readonly TsavoriteKV<SpanByte, SpanByte> store;

        /// <summary>
        /// Object store
        /// </summary>
        public readonly TsavoriteKV<byte[], IGarnetObject> objectStore;

        /// <summary>
        /// Server options
        /// </summary>
        public readonly GarnetServerOptions serverOptions;
        internal readonly IClusterProvider clusterProvider;

        /// <summary>
        /// Get server
        /// </summary>
        public GarnetServerTcp GetServer() => (GarnetServerTcp)server;

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

        internal readonly string localEndpoint;


        internal readonly CustomCommandManager customCommandManager;
        internal readonly GarnetServerMonitor monitor;
        internal readonly WatchVersionMap versionMap;

        internal readonly CacheSizeTracker objectStoreSizeTracker;

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
        /// Constructor
        /// </summary>
        public StoreWrapper(
            string version,
            IGarnetServer server,
            TsavoriteKV<SpanByte, SpanByte> store,
            TsavoriteKV<byte[], IGarnetObject> objectStore,
            CacheSizeTracker objectStoreSizeTracker,
            CustomCommandManager customCommandManager,
            TsavoriteLog appendOnlyFile,
            GarnetServerOptions serverOptions,
            AccessControlList accessControlList = null,
            IClusterFactory clusterFactory = null,
            ILoggerFactory loggerFactory = null
            )
        {
            this.version = version;
            this.server = server;
            this.startupTime = DateTimeOffset.UtcNow.Ticks;
            this.store = store;
            this.objectStore = objectStore;
            this.appendOnlyFile = appendOnlyFile;
            this.serverOptions = serverOptions;
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

            if (accessControlList == null)
            {
                // If ACL authentication is enabled, initiate access control list
                // NOTE: This is a temporary workflow. ACL should always be initiated and authenticator
                //       should become a parameter of AccessControlList.
                if ((this.serverOptions.AuthSettings != null) && (this.serverOptions.AuthSettings.GetType() == typeof(AclAuthenticationSettings)))
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

            string address = serverOptions.Address ?? GetIp();
            int port = serverOptions.Port;
            localEndpoint = address + ":" + port;

            logger?.LogInformation("Local endpoint: {localEndpoint}", localEndpoint);

            ctsCommit = new();
            run_id = Generator.CreateHexId();
        }

        public byte[] SerializeGarnetObject(IGarnetObject garnetObject)
        {
            using var ms = new MemoryStream();
            var serializer = new GarnetObjectSerializer(customCommandManager);
            serializer.BeginSerialize(ms);
            serializer.Serialize(ref garnetObject);
            serializer.EndSerialize();
            return ms.ToArray();
        }

        public IGarnetObject DeserializeGarnetObject(byte[] data)
        {
            using var ms = new MemoryStream(data);
            var serializer = new GarnetObjectSerializer(customCommandManager);
            serializer.BeginDeserialize(ms);
            serializer.Deserialize(out var obj);
            serializer.EndDeserialize();
            return obj;
        }

        /// <summary>
        /// Get IP
        /// </summary>
        /// <returns></returns>
        public static string GetIp()
        {
            string localIP;
            using (Socket socket = new(AddressFamily.InterNetwork, SocketType.Dgram, 0))
            {
                socket.Connect("8.8.8.8", 65530);
                IPEndPoint endPoint = socket.LocalEndPoint as IPEndPoint;
                localIP = endPoint.Address.ToString();
            }
            return localIP;
        }

        internal FunctionsState CreateFunctionsState()
            => new(appendOnlyFile, versionMap, customCommandManager.commandMap, customCommandManager.objectCommandMap, null, objectStoreSizeTracker);

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
        public void RecoverCheckpoint(bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false,
            Guid storeIndexToken = default, Guid storeHlogToken = default, Guid objectStoreIndexToken = default, Guid objectStoreHlogToken = default)
        {
            long storeVersion = -1, objectStoreVersion = -1;
            try
            {
                storeVersion = !recoverMainStoreFromToken ? store.Recover() : store.Recover(storeIndexToken, storeHlogToken);
                if (objectStore != null) objectStoreVersion = !recoverObjectStoreFromToken ? objectStore.Recover() : objectStore.Recover(objectStoreIndexToken, objectStoreHlogToken);
                lastSaveTime = DateTimeOffset.UtcNow;
            }
            catch (Exception ex)
            {
                logger?.LogInformation(ex, "Error during recovery of store; storeVersion = {storeVersion}; objectStoreVersion = {objectStoreVersion}", storeVersion, objectStoreVersion);
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
                logger?.LogInformation(ex, "Error during reset of store");
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
                //When replaying AOF we do not want to write record again to AOF.
                //So initialize local AofProcessor with recordToAof: false.
                var aofProcessor = new AofProcessor(this, recordToAof: false, logger);
                aofProcessor.Recover(untilAddress);
                aofProcessor.Dispose();
                replicationOffset = aofProcessor.ReplicationOffset;
                lastSaveTime = DateTimeOffset.UtcNow;
            }
            catch (Exception ex)
            {
                logger?.LogInformation(ex, "Error during recovery of AofProcessor");
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
                    if (serverOptions.EnableFastCommit && clusterProvider.IsReplica())
                    {
                        await Task.Delay(commitFrequencyMs, token);
                    }
                    else
                    {
                        await appendOnlyFile.CommitAsync(token);
                        await Task.Delay(commitFrequencyMs, token);
                    }
                }
            }
            catch (Exception ex)
            {
                logger?.LogInformation(ex, "CommitTask exception received, AOF tail address = {tailAddress}; AOF committed until address = {commitAddress}; ", appendOnlyFile.TailAddress, appendOnlyFile.CommittedUntilAddress);
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
                    DoCompaction(serverOptions.CompactionMaxSegments, serverOptions.ObjectStoreCompactionMaxSegments, 1, serverOptions.CompactionType);
                    if (serverOptions.CompactionType != LogCompactionType.ShiftForced)
                        logger?.LogInformation("NOTE: Take a checkpoint (SAVE/BGSAVE) in order to actually delete the older data segments (files) from disk");
                    else
                        logger?.LogInformation("NOTE: ShiftForced compaction type - make sure checkpoint/recovery is not being used");

                    await Task.Delay(compactionFrequencySecs * 1000, token);
                }
            }
            catch (Exception ex)
            {
                logger?.LogInformation(ex, "CompactionTask exception received, AOF tail address = {tailAddress}; AOF committed until address = {commitAddress}; ", appendOnlyFile.TailAddress, appendOnlyFile.CommittedUntilAddress);
            }
        }

        void DoCompaction()
        {
            // Periodic compaction -> no need to compact before checkpointing
            if (serverOptions.CompactionFrequencySecs > 0) return;
            if (serverOptions.CompactionType == LogCompactionType.ShiftForced)
            {
                string error = "Cannot use ShiftForced with checkpointing";
                logger.LogError(error);
                Debug.Fail(error);
                return;
            }

            DoCompaction(serverOptions.CompactionMaxSegments, serverOptions.ObjectStoreCompactionMaxSegments, 1, serverOptions.CompactionType);
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
                version = version,
                sessionID = -1
            };
            appendOnlyFile?.Enqueue(header, out _);
        }

        void DoCompaction(int mainStoreMaxSegments, int objectStoreMaxSegments, int numSegmentsToCompact, LogCompactionType compactionType)
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
                        store.Log.ShiftBeginAddress(untilAddress, true, false);
                        break;

                    case LogCompactionType.ShiftForced:
                        store.Log.ShiftBeginAddress(untilAddress, true, true);
                        break;

                    case LogCompactionType.Scan:
                        store.Log.Compact<SpanByte, Empty, Empty, SpanByteFunctions<Empty, Empty>>(new SpanByteFunctions<Empty, Empty>(), untilAddress, CompactionType.Scan);
                        break;

                    case LogCompactionType.Lookup:
                        store.Log.Compact<SpanByte, Empty, Empty, SpanByteFunctions<Empty, Empty>>(new SpanByteFunctions<Empty, Empty>(), untilAddress, CompactionType.Lookup);
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
                        objectStore.Log.ShiftBeginAddress(untilAddress, true, false);
                        break;

                    case LogCompactionType.ShiftForced:
                        objectStore.Log.ShiftBeginAddress(untilAddress, true, true);
                        break;

                    case LogCompactionType.Scan:
                        objectStore.Log.Compact<IGarnetObject, IGarnetObject, Empty, SimpleFunctions<byte[], IGarnetObject, Empty>>(
                            new SimpleFunctions<byte[], IGarnetObject, Empty>(), untilAddress, CompactionType.Scan);
                        break;

                    case LogCompactionType.Lookup:
                        objectStore.Log.Compact<IGarnetObject, IGarnetObject, Empty, SimpleFunctions<byte[], IGarnetObject, Empty>>(
                            new SimpleFunctions<byte[], IGarnetObject, Empty>(), untilAddress, CompactionType.Lookup);
                        break;

                    default:
                        break;
                }

                logger?.LogInformation("End object store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}", untilAddress, store.Log.BeginAddress, readOnlyAddress, store.Log.TailAddress);
            }
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

            if (serverOptions.AdjustedIndexMaxSize > 0 || serverOptions.AdjustedObjectStoreIndexMaxSize > 0)
            {
                Task.Run(() => IndexAutoGrowTask(ctsCommit.Token));
            }
        }

        /// <summary>Grows indexes of both main store and object store if current size is too small.</summary>
        /// <param name="token"></param>
        private async void IndexAutoGrowTask(CancellationToken token)
        {
            try
            {
                bool indexMaxedOut = serverOptions.AdjustedIndexMaxSize == 0;
                bool objectStoreIndexMaxedOut = serverOptions.AdjustedObjectStoreIndexMaxSize == 0;
                while (!indexMaxedOut || !objectStoreIndexMaxedOut)
                {
                    if (token.IsCancellationRequested) break;

                    await Task.Delay(TimeSpan.FromSeconds(serverOptions.IndexResizeFrequencySecs), token);

                    if (!indexMaxedOut)
                        indexMaxedOut = GrowIndexIfNeeded(StoreType.Main, serverOptions.AdjustedIndexMaxSize, store.OverflowBucketAllocations,
                            () => store.IndexSize, () => store.GrowIndex());

                    if (!objectStoreIndexMaxedOut)
                        objectStoreIndexMaxedOut = GrowIndexIfNeeded(StoreType.Object, serverOptions.AdjustedObjectStoreIndexMaxSize, objectStore.OverflowBucketAllocations,
                            () => objectStore.IndexSize, () => objectStore.GrowIndex());
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "IndexAutoGrowTask exception received");
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
            logger?.LogDebug($"IndexAutoGrowTask[{storeType}]: checking index size {indexSizeRetriever()} against max {indexMaxSize} with overflow {overflowCount}");

            if (indexSizeRetriever() < indexMaxSize &&
                overflowCount > (indexSizeRetriever() * serverOptions.IndexResizeThreshold / 100))
            {
                logger?.LogInformation($"IndexAutoGrowTask[{storeType}]: overflowCount {overflowCount} ratio more than threshold {serverOptions.IndexResizeThreshold}%. Doubling index size...");
                growAction();
            }

            if (indexSizeRetriever() < indexMaxSize) return false;

            logger?.LogDebug($"IndexAutoGrowTask[{storeType}]: index size {indexSizeRetriever()} reached index max size {indexMaxSize}");
            return true;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            //Wait for checkpoints to complete and disable checkpointing
            _checkpointTaskLock.WriteLock();

            monitor?.Dispose();
            ctsCommit?.Cancel();
            ctsCommit?.Dispose();
            clusterProvider?.Dispose();
        }

        bool StartCheckpoint()
            => _checkpointTaskLock.TryWriteLock();

        void CompleteCheckpoint()
            => _checkpointTaskLock.WriteUnlock();

        /// <summary>
        /// Take a checkpoint if no checkpoint was taken after the provided time offset
        /// </summary>
        /// <param name="afterTime"></param>
        /// <returns></returns>
        public async Task TakeOnDemandCheckpoint(DateTimeOffset afterTime)
        {
            //Take lock to ensure not other task will be taking a checkpoint
            while (!StartCheckpoint())
                await Task.Yield();

            //If an external task has taken a checkpoint after the provided afterTime return
            if (this.lastSaveTime > afterTime)
            {
                CompleteCheckpoint();
                return;
            }

            //If no newer checkpoint was taken compared to the provided afterTime take a checkpoint
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
            if (!StartCheckpoint()) return false;
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
                DoCompaction();
                var lastSaveStoreTailAddress = store.Log.TailAddress;
                var lastSaveObjectStoreTailAddress = (objectStore?.Log.TailAddress).GetValueOrDefault();

                bool full = false;
                if (this.lastSaveStoreTailAddress == 0 || lastSaveStoreTailAddress - this.lastSaveStoreTailAddress >= serverOptions.FullCheckpointLogInterval)
                    full = true;
                if (objectStore != null && (this.lastSaveObjectStoreTailAddress == 0 || lastSaveObjectStoreTailAddress - this.lastSaveObjectStoreTailAddress >= serverOptions.FullCheckpointLogInterval))
                    full = true;

                bool tryIncremental = serverOptions.EnableIncrementalSnapshots;
                if (store.IncrementalSnapshotTailAddress >= serverOptions.IncrementalSnapshotLogSizeLimit)
                    tryIncremental = false;
                if (objectStore?.IncrementalSnapshotTailAddress >= serverOptions.IncrementalSnapshotLogSizeLimit)
                    tryIncremental = false;

                CheckpointType checkpointType = serverOptions.UseFoldOverCheckpoints ? CheckpointType.FoldOver : CheckpointType.Snapshot;
                await InitiateCheckpoint(full, checkpointType, tryIncremental, storeType, logger);
                if (full)
                {
                    if (storeType == StoreType.Main || storeType == StoreType.All)
                        this.lastSaveStoreTailAddress = lastSaveStoreTailAddress;
                    if (storeType == StoreType.Object || storeType == StoreType.All)
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
                CompleteCheckpoint();
            }
        }

        private async Task InitiateCheckpoint(bool full, CheckpointType checkpointType, bool tryIncremental, StoreType storeType, ILogger logger = null)
        {
            logger?.LogInformation("Initiating checkpoint; full = {full}, type = {checkpointType}, tryIncremental = {tryIncremental}, storeType = {storeType}", full, checkpointType, tryIncremental, storeType);

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

            (bool success, Guid token) storeCheckpointResult = default;
            (bool success, Guid token) objectStoreCheckpointResult = default;
            if (full)
            {
                if (storeType == StoreType.Main || storeType == StoreType.All)
                    storeCheckpointResult = await store.TakeFullCheckpointAsync(checkpointType);

                if (storeType == StoreType.Object || storeType == StoreType.All)
                    if (objectStore != null) objectStoreCheckpointResult = await objectStore.TakeFullCheckpointAsync(checkpointType);
            }
            else
            {
                if (storeType == StoreType.Main || storeType == StoreType.All)
                    storeCheckpointResult = await store.TakeHybridLogCheckpointAsync(checkpointType, tryIncremental);

                if (storeType == StoreType.Object || storeType == StoreType.All)
                    if (objectStore != null) objectStoreCheckpointResult = await objectStore.TakeHybridLogCheckpointAsync(checkpointType, tryIncremental);
            }

            // If cluster is enabled the replication manager is responsible for truncating AOF
            if (serverOptions.EnableCluster && serverOptions.EnableAOF)
            {
                clusterProvider.SafeTruncateAOF(storeType, full, CheckpointCoveredAofAddress, storeCheckpointResult.token, objectStoreCheckpointResult.token);
            }
            else
            {
                appendOnlyFile?.TruncateUntil(CheckpointCoveredAofAddress);
                appendOnlyFile?.Commit();
            }
            logger?.LogInformation("Completed checkpoint");
        }
    }
}