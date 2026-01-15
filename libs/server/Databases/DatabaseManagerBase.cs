// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Garnet.server.Metrics;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Base class for logical database management
    /// </summary>
    internal abstract class DatabaseManagerBase : IDatabaseManager
    {
        /// <inheritdoc/>
        public abstract GarnetDatabase DefaultDatabase { get; }

        /// <inheritdoc/>
        public abstract int DatabaseCount { get; }

        /// <inheritdoc/>
        public abstract int MaxDatabaseId { get; }

        /// <inheritdoc/>
        public abstract GarnetDatabase TryGetOrAddDatabase(int dbId, out bool success, out bool added);

        /// <inheritdoc/>
        public abstract bool TryPauseCheckpoints(int dbId);

        /// <inheritdoc/>
        public abstract void ResumeCheckpoints(int dbId);

        /// <inheritdoc/>
        public abstract void RecoverCheckpoint(bool replicaRecover = false, bool recoverFromToken = false, CheckpointMetadata metadata = null);

        /// <inheritdoc/>
        public abstract bool TakeCheckpoint(bool background, ILogger logger = null, CancellationToken token = default);

        /// <inheritdoc/>
        public abstract bool TakeCheckpoint(bool background, int dbId, ILogger logger = null, CancellationToken token = default);

        /// <inheritdoc/>
        public abstract Task TakeOnDemandCheckpointAsync(DateTimeOffset entryTime, int dbId = 0);

        /// <inheritdoc/>
        public abstract Task TaskCheckpointBasedOnAofSizeLimitAsync(long aofSizeLimit,
            CancellationToken token = default, ILogger logger = null);

        /// <inheritdoc/>
        public abstract Task CommitToAofAsync(CancellationToken token = default, ILogger logger = null);

        /// <inheritdoc/>
        public abstract Task CommitToAofAsync(int dbId, CancellationToken token = default, ILogger logger = null);

        /// <inheritdoc/>
        public abstract Task WaitForCommitToAofAsync(CancellationToken token = default, ILogger logger = null);

        /// <inheritdoc/>
        public abstract void RecoverAOF();

        /// <inheritdoc/>
        public abstract long ReplayAOF(long untilAddress = -1);

        /// <inheritdoc/>
        public abstract void DoCompaction(CancellationToken token = default, ILogger logger = null);

        /// <inheritdoc/>
        public abstract bool GrowIndexesIfNeeded(CancellationToken token = default);

        /// <inheritdoc/>
        public abstract void ExecuteObjectCollection();

        /// <inheritdoc/>
        public abstract void ExpiredKeyDeletionScan();

        /// <inheritdoc/>
        public abstract void StartSizeTrackers(CancellationToken token = default);

        /// <inheritdoc/>
        public abstract void Reset(int dbId = 0);

        /// <inheritdoc/>
        public abstract void ResetRevivificationStats();

        /// <inheritdoc/>
        public abstract void EnqueueCommit(AofEntryType entryType, long version, int dbId = 0);

        /// <inheritdoc/>
        public abstract GarnetDatabase[] GetDatabasesSnapshot();

        /// <inheritdoc/>
        public abstract GarnetDatabase TryGetDatabase(int dbId, out bool found);

        /// <inheritdoc/>
        public abstract void FlushDatabase(bool unsafeTruncateLog, int dbId = 0);

        /// <inheritdoc/>
        public abstract void FlushAllDatabases(bool unsafeTruncateLog);

        /// <inheritdoc/>
        public abstract bool TrySwapDatabases(int dbId1, int dbId2, CancellationToken token = default);

        /// <inheritdoc/>
        public abstract FunctionsState CreateFunctionsState(int dbId = 0, byte respProtocolVersion = ServerOptions.DEFAULT_RESP_VERSION);

        /// <inheritdoc/>
        public abstract void Dispose();

        /// <inheritdoc/>
        public abstract IDatabaseManager Clone(bool enableAof);

        /// <inheritdoc/>
        public TsavoriteKV<StoreFunctions, StoreAllocator> Store => DefaultDatabase.Store;

        /// <inheritdoc/>
        public TsavoriteLog AppendOnlyFile => DefaultDatabase.AppendOnlyFile;

        /// <inheritdoc/>
        public DateTimeOffset LastSaveTime => DefaultDatabase.LastSaveTime;

        /// <inheritdoc/>
        public CacheSizeTracker SizeTracker => DefaultDatabase.SizeTracker;

        /// <inheritdoc/>
        public WatchVersionMap VersionMap => DefaultDatabase.VersionMap;

        /// <summary>
        /// Store Wrapper
        /// </summary>
        public readonly StoreWrapper StoreWrapper;

        /// <summary>
        /// Delegate for creating a new logical database
        /// </summary>
        public readonly StoreWrapper.DatabaseCreatorDelegate CreateDatabaseDelegate;

        /// <summary>
        /// The main logger instance associated with the database manager.
        /// </summary>
        protected ILogger Logger;

        /// <summary>
        /// The logger factory used to create logger instances
        /// </summary>
        protected ILoggerFactory LoggerFactory;

        /// <summary>
        /// True if instance has been previously disposed
        /// </summary>
        protected bool Disposed;

        protected DatabaseManagerBase(StoreWrapper.DatabaseCreatorDelegate createDatabaseDelegate, StoreWrapper storeWrapper)
        {
            this.CreateDatabaseDelegate = createDatabaseDelegate;
            this.StoreWrapper = storeWrapper;
            this.LoggerFactory = storeWrapper.loggerFactory;
        }

        /// <summary>
        /// Recover single database from checkpoint
        /// </summary>
        /// <param name="db">Database to recover</param>
        /// <param name="storeVersion">Store version</param>
        protected void RecoverDatabaseCheckpoint(GarnetDatabase db, out long storeVersion)
        {
            storeVersion = 0;

            storeVersion = db.Store.Recover();
            Logger?.LogInformation("Recovered store to version {storeVersion}", storeVersion);

            if (storeVersion > 0)
            {
                db.LastSaveTime = DateTimeOffset.UtcNow;
            }
        }

        /// <summary>
        /// Asynchronously checkpoint a single database
        /// </summary>
        /// <param name="db">Database to checkpoint</param>
        /// <param name="logger">Logger</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Tuple of store tail address and object store tail address</returns>
        protected async Task<long?> TakeCheckpointAsync(GarnetDatabase db, ILogger logger = null, CancellationToken token = default)
        {
            try
            {
                DoCompaction(db, isFromCheckpoint: true, logger);
                var lastSaveStoreTailAddress = db.Store.Log.TailAddress;

                var full = db.LastSaveStoreTailAddress == 0 ||
                           lastSaveStoreTailAddress - db.LastSaveStoreTailAddress >= StoreWrapper.serverOptions.FullCheckpointLogInterval;

                var tryIncremental = StoreWrapper.serverOptions.EnableIncrementalSnapshots;
                if (db.Store.IncrementalSnapshotTailAddress >= StoreWrapper.serverOptions.IncrementalSnapshotLogSizeLimit)
                    tryIncremental = false;

                var checkpointType = StoreWrapper.serverOptions.UseFoldOverCheckpoints ? CheckpointType.FoldOver : CheckpointType.Snapshot;
                await InitiateCheckpointAsync(db, full, checkpointType, tryIncremental, logger);

                return full ? lastSaveStoreTailAddress : null;
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Checkpointing threw exception, DB ID: {id}", db.Id);
            }

            return null;
        }

        /// <summary>
        /// Try to take checkpointing lock for specified database
        /// </summary>
        /// <param name="db">Database to checkpoint</param>
        /// <returns>True if acquired a lock</returns>
        protected static bool TryPauseCheckpoints(GarnetDatabase db)
            => db.CheckpointingLock.TryWriteLock();

        /// <summary>
        /// Release existing checkpointing lock for 
        /// </summary>
        /// <param name="db">Database to checkpoint</param>
        protected static void ResumeCheckpoints(GarnetDatabase db)
            => db.CheckpointingLock.WriteUnlock();

        /// <summary>
        /// Recover a single database from AOF
        /// </summary>
        /// <param name="db">Database to recover</param>
        protected void RecoverDatabaseAOF(GarnetDatabase db)
        {
            if (db.AppendOnlyFile == null) return;

            db.AppendOnlyFile.Recover();
            Logger?.LogInformation("Recovered AOF: begin address = {beginAddress}, tail address = {tailAddress}, DB ID: {id}",
                db.AppendOnlyFile.BeginAddress, db.AppendOnlyFile.TailAddress, db.Id);
        }

        /// <summary>
        /// Replay AOF for specified database
        /// </summary>
        /// <param name="aofProcessor">AOF processor</param>
        /// <param name="db">Database to replay</param>
        /// <param name="untilAddress">Tail address</param>
        /// <returns>Tail address</returns>
        protected long ReplayDatabaseAOF(AofProcessor aofProcessor, GarnetDatabase db, long untilAddress = -1)
        {
            long replicationOffset = 0;
            try
            {
                replicationOffset = aofProcessor.Recover(db, untilAddress);
                db.LastSaveTime = DateTimeOffset.UtcNow;
            }
            catch (Exception ex)
            {
                Logger?.LogError(ex, "Error during recovery of AofProcessor");
                if (StoreWrapper.serverOptions.FailOnRecoveryError)
                    throw;
            }

            return replicationOffset;
        }

        /// <summary>
        /// Reset database
        /// </summary>
        /// <param name="db">Database to reset</param>
        protected void ResetDatabase(GarnetDatabase db)
        {
            try
            {
                if (db.Store.Log.TailAddress > PageHeader.Size)
                    db.Store.Reset();
                db.AppendOnlyFile?.Reset();

                var lastSave = DateTimeOffset.FromUnixTimeSeconds(0);
                db.LastSaveTime = lastSave;
            }
            catch (Exception ex)
            {
                Logger?.LogError(ex, "Error during reset of store");
            }
        }

        /// <summary>
        /// Enqueue AOF commit for single database
        /// </summary>
        /// <param name="db">Database to enqueue commit for</param>
        /// <param name="entryType">AOF entry type</param>
        /// <param name="version">Store version</param>
        protected static void EnqueueDatabaseCommit(GarnetDatabase db, AofEntryType entryType, long version)
        {
            if (db.AppendOnlyFile == null) return;

            AofHeader header = new()
            {
                opType = entryType,
                storeVersion = version,
                sessionID = -1
            };

            db.AppendOnlyFile.Enqueue(header, out _);
        }

        /// <summary>
        /// Flush a single database
        /// </summary>
        /// <param name="db">Database to flush</param>
        /// <param name="unsafeTruncateLog">Truncate log</param>
        /// <param name="truncateAof">Truncate AOF log</param>
        protected static void FlushDatabase(GarnetDatabase db, bool unsafeTruncateLog, bool truncateAof = true)
        {
            db.Store.Log.ShiftBeginAddress(db.Store.Log.TailAddress, truncateLog: unsafeTruncateLog);

            if (truncateAof)
                db.AppendOnlyFile?.TruncateUntil(db.AppendOnlyFile.TailAddress);
        }

        /// <summary>
        /// Grow store indexes for specified database, if necessary
        /// </summary>
        /// <param name="db">Database to grow store indexes for</param>
        /// <returns>True if both store indexes are maxed out</returns>
        protected bool GrowIndexesIfNeeded(GarnetDatabase db)
        {
            var indexesMaxedOut = true;

            if (!DefaultDatabase.StoreIndexMaxedOut)
            {
                var store = DefaultDatabase.Store;
                if (GrowIndexIfNeeded(StoreWrapper.serverOptions.AdjustedIndexMaxCacheLines, store.OverflowBucketAllocations,
                        () => store.IndexSize, async () => await store.GrowIndexAsync()))
                {
                    db.StoreIndexMaxedOut = true;
                }
                else
                {
                    indexesMaxedOut = false;
                }
            }

            return indexesMaxedOut;
        }

        /// <summary>
        /// Executes a store-wide object collect operation for the specified database
        /// </summary>
        /// <param name="db">Database for object collection</param>
        /// <param name="logger">Logger</param>
        protected void ExecuteObjectCollection(GarnetDatabase db, ILogger logger = null)
        {
            if (db.StoreCollectionDbStorageSession == null)
            {
                var scratchBufferManager = new ScratchBufferBuilder();
                db.StoreCollectionDbStorageSession =
                    new StorageSession(StoreWrapper, scratchBufferManager, null, null, db.Id, Logger);
            }

            ExecuteHashCollect(db.StoreCollectionDbStorageSession);
            ExecuteSortedSetCollect(db.StoreCollectionDbStorageSession);
        }

        /// <summary>
        /// Execute a store-wide expired key deletion scan operation for the specified database
        /// </summary>
        /// <param name="db">Database</param>
        protected void ExpiredKeyDeletionScan(GarnetDatabase db)
        {
            _ = StoreExpiredKeyDeletionScan(db);
        }

        /// <summary>
        /// Run compaction on specified database
        /// </summary>
        /// <param name="db">Database to run compaction on</param>
        /// <param name="logger">Logger</param>
        /// <param name="isFromCheckpoint">True if called from checkpointing, false if called from background task</param>
        protected void DoCompaction(GarnetDatabase db, bool isFromCheckpoint = false, ILogger logger = null)
        {
            try
            {
                // If periodic compaction is enabled and this is called from checkpointing, skip compaction
                if (isFromCheckpoint && StoreWrapper.serverOptions.CompactionFrequencySecs > 0) return;

                DoCompaction(db, StoreWrapper.serverOptions.CompactionMaxSegments, 1,
                    StoreWrapper.serverOptions.CompactionType, StoreWrapper.serverOptions.CompactionForceDelete);
            }
            catch (Exception ex)
            {
                logger?.LogError(ex,
                    "Exception raised during compaction. AOF tail address = {tailAddress}; AOF committed until address = {commitAddress}; DB ID = {id}",
                    db.AppendOnlyFile.TailAddress, db.AppendOnlyFile.CommittedUntilAddress, db.Id);
                throw;
            }
        }

        /// <summary>
        /// Grows index if current size is smaller than max size.
        /// Decision is based on whether overflow bucket allocation is more than a threshold which indicates a contention
        /// in the index leading many allocations to the same bucket.
        /// </summary>
        /// <param name="indexMaxSize"></param>
        /// <param name="overflowCount"></param>
        /// <param name="indexSizeRetriever"></param>
        /// <param name="growAction"></param>
        /// <returns>True if index has reached its max size</returns>
        protected bool GrowIndexIfNeeded(long indexMaxSize, long overflowCount, Func<long> indexSizeRetriever, Action growAction)
        {
            Logger?.LogDebug(
                $"IndexAutoGrowTask: checking index size {{indexSizeRetriever}} against max {{indexMaxSize}} with overflow {{overflowCount}}",
                indexSizeRetriever(), indexMaxSize, overflowCount);

            if (indexSizeRetriever() < indexMaxSize &&
                overflowCount > (indexSizeRetriever() * StoreWrapper.serverOptions.IndexResizeThreshold / 100))
            {
                Logger?.LogInformation(
                    $"IndexAutoGrowTask: overflowCount {{overflowCount}} ratio more than threshold {{indexResizeThreshold}}%. Doubling index size...",
                    overflowCount, StoreWrapper.serverOptions.IndexResizeThreshold);
                growAction();
            }

            if (indexSizeRetriever() < indexMaxSize) return false;

            Logger?.LogDebug(
                $"IndexAutoGrowTask: checking index size {{indexSizeRetriever}} against max {{indexMaxSize}} with overflow {{overflowCount}}",
                indexSizeRetriever(), indexMaxSize, overflowCount);
            return true;
        }

        private void DoCompaction(GarnetDatabase db, int mainStoreMaxSegments, int numSegmentsToCompact, LogCompactionType compactionType, bool compactionForceDelete)
        {
            if (compactionType == LogCompactionType.None) return;

            var storeLog = db.Store.Log;

            var mainStoreMaxLogSize = (1L << StoreWrapper.serverOptions.SegmentSizeBits(isObj: false)) * mainStoreMaxSegments;

            if (storeLog.ReadOnlyAddress - storeLog.BeginAddress > mainStoreMaxLogSize)
            {
                var readOnlyAddress = storeLog.ReadOnlyAddress;
                var compactLength = (1L << StoreWrapper.serverOptions.SegmentSizeBits(isObj: false)) * (mainStoreMaxSegments - numSegmentsToCompact);
                var untilAddress = readOnlyAddress - compactLength;
                Logger?.LogInformation(
                    "Begin main store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}",
                    untilAddress, storeLog.BeginAddress, readOnlyAddress, storeLog.TailAddress);

                switch (compactionType)
                {
                    case LogCompactionType.Shift:
                        storeLog.ShiftBeginAddress(untilAddress, true, compactionForceDelete);
                        break;

                    case LogCompactionType.Scan:
                        storeLog.Compact<PinnedSpanByte, Empty, Empty>(untilAddress, CompactionType.Scan);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof(db);
                            storeLog.Truncate();
                        }
                        break;

                    case LogCompactionType.Lookup:
                        storeLog.Compact<PinnedSpanByte, Empty, Empty>(untilAddress, CompactionType.Lookup);
                        if (compactionForceDelete)
                        {
                            CompactionCommitAof(db);
                            storeLog.Truncate();
                        }
                        break;
                }

                Logger?.LogInformation(
                    "End store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}",
                    untilAddress, storeLog.BeginAddress, readOnlyAddress, storeLog.TailAddress);
            }
        }

        private void CompactionCommitAof(GarnetDatabase db)
        {
            // If we are the primary, we commit the AOF.
            // If we are the replica, we commit the AOF only if fast commit is disabled
            // because we do not want to clobber AOF addresses.
            // TODO: replica should instead wait until the next AOF commit is done via primary
            if (StoreWrapper.serverOptions.EnableAOF)
            {
                if (StoreWrapper.serverOptions.EnableCluster && StoreWrapper.clusterProvider.IsReplica())
                {
                    if (!StoreWrapper.serverOptions.EnableFastCommit)
                        db.AppendOnlyFile?.CommitAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                }
                else
                {
                    db.AppendOnlyFile?.CommitAsync().ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }
        }

        /// <summary>
        /// Asynchronously initiate a single database checkpoint
        /// </summary>
        /// <param name="db">Database to checkpoint</param>
        /// <param name="full">True if full checkpoint should be initiated</param>
        /// <param name="checkpointType">Type of checkpoint</param>
        /// <param name="tryIncremental">Try to store as incremental delta over last snapshot</param>
        /// <param name="logger">Logger</param>
        /// <returns>Task</returns>
        private async Task InitiateCheckpointAsync(GarnetDatabase db, bool full, CheckpointType checkpointType,
            bool tryIncremental, ILogger logger = null)
        {
            logger?.LogInformation("Initiating checkpoint; full = {full}, type = {checkpointType}, tryIncremental = {tryIncremental}, dbId = {dbId}", full, checkpointType, tryIncremental, db.Id);

            long checkpointCoveredAofAddress = 0;
            if (db.AppendOnlyFile != null)
            {
                if (StoreWrapper.serverOptions.EnableCluster)
                    StoreWrapper.clusterProvider.OnCheckpointInitiated(out checkpointCoveredAofAddress);
                else
                {
                    checkpointCoveredAofAddress = db.AppendOnlyFile.TailAddress;
                    StoreWrapper.StoreCheckpointManager.CurrentSafeAofAddress = checkpointCoveredAofAddress;
                }

                if (checkpointCoveredAofAddress > 0)
                    logger?.LogInformation("Will truncate AOF to {tailAddress} after checkpoint (files deleted after next commit), dbId = {dbId}", checkpointCoveredAofAddress, db.Id);
            }

            (bool success, Guid token) checkpointResult = default;

            IStateMachine sm;
            if (full)
            {
                sm = Checkpoint.Full(db.Store, checkpointType, out checkpointResult.token);
            }
            else
            {
                tryIncremental = tryIncremental && db.Store.CanTakeIncrementalCheckpoint(checkpointType, out checkpointResult.token);

                sm = tryIncremental
                    ? Checkpoint.IncrementalHybridLogOnly(db.Store, checkpointResult.token)
                    : Checkpoint.HybridLogOnly(db.Store, checkpointType, out checkpointResult.token);
            }

            checkpointResult.success = await db.StateMachineDriver.RunAsync(sm);

            // If cluster is enabled the replication manager is responsible for truncating AOF
            if (StoreWrapper.serverOptions.EnableCluster && StoreWrapper.serverOptions.EnableAOF)
            {
                StoreWrapper.clusterProvider.SafeTruncateAOF(full, checkpointCoveredAofAddress,
                    checkpointResult.token, checkpointResult.token);
            }
            else
            {
                db.AppendOnlyFile?.TruncateUntil(checkpointCoveredAofAddress);
                db.AppendOnlyFile?.Commit();
            }

            // During the checkpoint, we may have serialized Garnet objects in (v) versions of objects.
            // We can now safely remove these serialized versions as they are no longer needed.
            using var iter1 = db.Store.Log.Scan(db.Store.Log.ReadOnlyAddress, db.Store.Log.TailAddress, DiskScanBufferingMode.SinglePageBuffering, includeClosedRecords: true);
            while (iter1.GetNext())
            {
                if (!iter1.Info.ValueIsObject)
                    continue;

                var valueObject = iter1.ValueObject;
                if (valueObject != null)
                    ((GarnetObjectBase)iter1.ValueObject).ClearSerializedObjectData();
            }

            logger?.LogInformation("Completed checkpoint for DB ID: {id}", db.Id);
        }

        private static void ExecuteHashCollect(StorageSession storageSession)
        {
            var input = new ObjectInput(GarnetObjectType.Hash) { HashOp = HashOperation.HCOLLECT };

            ReadOnlySpan<PinnedSpanByte> key = [PinnedSpanByte.FromPinnedSpan("*"u8)];
            storageSession.HashCollect(key, ref input, ref storageSession.objectBasicContext);
            storageSession.scratchBufferBuilder.Reset();
        }

        private static void ExecuteSortedSetCollect(StorageSession storageSession)
        {
            storageSession.SortedSetCollect(ref storageSession.objectBasicContext);
            storageSession.scratchBufferBuilder.Reset();
        }

        /// <inheritdoc/>
        public abstract (long numExpiredKeysFound, long totalRecordsScanned) ExpiredKeyDeletionScan(int dbId);

        protected (long numExpiredKeysFound, long totalRecordsScanned) StoreExpiredKeyDeletionScan(GarnetDatabase db)
        {
            if (db.StoreExpiredKeyDeletionDbStorageSession == null)
            {
                var scratchBufferManager = new ScratchBufferBuilder();
                db.StoreExpiredKeyDeletionDbStorageSession = new StorageSession(StoreWrapper, scratchBufferManager, null, null, db.Id, Logger);
            }

            var scanFrom = StoreWrapper.store.Log.ReadOnlyAddress;
            var scanUntil = StoreWrapper.store.Log.TailAddress;
            var (deletedCount, totalCount) = db.StoreExpiredKeyDeletionDbStorageSession.ExpiredKeyDeletionScan(scanFrom, scanUntil);
            Logger?.LogDebug(
                "Store - Deleted {deletedCount} keys out {totalCount} records in range {scanFrom} to {scanUntil} for DB {id}",
                deletedCount, totalCount, scanFrom, scanUntil, db.Id);

            return (deletedCount, totalCount);
        }

        /// <inheritdoc/>
        public abstract (HybridLogScanMetrics mainStore, HybridLogScanMetrics objectStore)[] CollectHybridLogStats();

        protected (HybridLogScanMetrics mainStore, HybridLogScanMetrics objectStore) CollectHybridLogStatsForDb(GarnetDatabase db)
        {
            var functionsState = CreateFunctionsState();
            var mainStoreSessionFunctions = new MainSessionFunctions(functionsState);
            var mainStoreStats = CollectHybridLogStats(db, db.Store, mainStoreSessionFunctions);

            var objectSessionFunctions = new ObjectSessionFunctions(functionsState);
            var objectStoreStats = CollectHybridLogStats(db, db.Store, objectSessionFunctions);

            return (mainStoreStats, objectStoreStats);
        }

        private HybridLogScanMetrics CollectHybridLogStats<TFuncs, TAllocator, TInput, TOutput>(
            GarnetDatabase db,
            TsavoriteKV<TFuncs, TAllocator> store,
            ISessionFunctions<TInput, TOutput, long> sessionFunctions)
            where TFuncs : IStoreFunctions
            where TAllocator : IAllocator<TFuncs>
        {
            if (db.HybridLogStatScanStorageSession == null)
            {
                var scratchBufferManager = new ScratchBufferBuilder();
                db.HybridLogStatScanStorageSession = new StorageSession(StoreWrapper, scratchBufferManager, null, null, db.Id, Logger);
            }

            using var session = store.NewSession<TInput, TOutput, long, ISessionFunctions<TInput, TOutput, long>>(sessionFunctions);
            var basicContext = session.BasicContext;
            // region: Immutable || Mutable
            // state: RCUdSealed || RCUdUnsealed || Tombstoned || ElidedFromHashIndex || Live
            var scanMetrics = new HybridLogScanMetrics();
            var fromAddr = store.Log.HeadAddress;
            var toAddr = store.Log.TailAddress;
            using var iter = store.Log.Scan(fromAddr, toAddr, includeClosedRecords: true);
            // Records can be in readonly region, or mutable region
            while (iter.GetNext())
            {
                string region = iter.CurrentAddress >= db.Store.Log.ReadOnlyAddress ? "Mutable" : "Immutable";
                string state = "Live";
                if (iter.Info.IsSealed)
                {
                    // while the server is live, this is true for RCUd records, when we recover from checkpoints, we unseal the records, so some RCUd records may not be sealed
                    state = "RCUdSealed";
                }
                else if (iter.Info.Invalid)
                {
                    // Setting invalid is done when a record has been elided from the hash index
                    state = "ElidedFromHashIndex";
                }
                else if (iter.Info.Tombstone)
                {
                    state = "Tombstoned";
                }
                else if (!basicContext.ContainsKeyInMemory(iter.Key, out long tempKeyAddress, fromAddr).Found || iter.CurrentAddress != tempKeyAddress)
                {
                    // check if this was a record that RCUd by checking if the key when queried via hash index points to the same address
                    state = "RCUdUnsealed";
                }
                long size = iter.NextAddress - iter.CurrentAddress;
                scanMetrics.AddScanMetric(region, state, size);
            }
            return scanMetrics;
        }
    }
}