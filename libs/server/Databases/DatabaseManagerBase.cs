﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Base class for logical database management
    /// </summary>
    internal abstract class DatabaseManagerBase : IDatabaseManager
    {
        /// <inheritdoc/>
        public abstract ref GarnetDatabase DefaultDatabase { get; }

        /// <inheritdoc/>
        public abstract int DatabaseCount { get; }

        /// <inheritdoc/>
        public abstract ref GarnetDatabase TryGetOrAddDatabase(int dbId, out bool success, out bool added);

        /// <inheritdoc/>
        public abstract bool TryPauseCheckpoints(int dbId);

        /// <inheritdoc/>
        public abstract void ResumeCheckpoints(int dbId);

        /// <inheritdoc/>
        public abstract void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false,
            bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null);

        /// <inheritdoc/>
        public abstract bool TakeCheckpoint(bool background, StoreType storeType = StoreType.All, ILogger logger = null,
            CancellationToken token = default);

        /// <inheritdoc/>
        public abstract bool TakeCheckpoint(bool background, int dbId, StoreType storeType = StoreType.All, ILogger logger = null,
            CancellationToken token = default);

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
        public abstract void StartObjectSizeTrackers(CancellationToken token = default);

        /// <inheritdoc/>
        public abstract void Reset(int dbId = 0);

        /// <inheritdoc/>
        public abstract void ResetRevivificationStats();

        /// <inheritdoc/>
        public abstract void EnqueueCommit(AofEntryType entryType, long version, int dbId = 0);

        /// <inheritdoc/>
        public abstract GarnetDatabase[] GetDatabasesSnapshot();

        /// <inheritdoc/>
        public abstract ref GarnetDatabase TryGetDatabase(int dbId, out bool found);

        /// <inheritdoc/>
        public abstract void FlushDatabase(bool unsafeTruncateLog, int dbId = 0);

        /// <inheritdoc/>
        public abstract void FlushAllDatabases(bool unsafeTruncateLog);

        /// <inheritdoc/>
        public abstract bool TrySwapDatabases(int dbId1, int dbId2, CancellationToken token = default);

        /// <inheritdoc/>
        public abstract FunctionsState CreateFunctionsState(int dbId = 0);

        /// <inheritdoc/>
        public abstract void Dispose();

        /// <inheritdoc/>
        public abstract IDatabaseManager Clone(bool enableAof);

        /// <inheritdoc/>
        public TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> MainStore => DefaultDatabase.MainStore;

        /// <inheritdoc/>
        public TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStore => DefaultDatabase.ObjectStore;

        /// <inheritdoc/>
        public TsavoriteLog AppendOnlyFile => DefaultDatabase.AppendOnlyFile;

        /// <inheritdoc/>
        public DateTimeOffset LastSaveTime => DefaultDatabase.LastSaveTime;

        /// <inheritdoc/>
        public CacheSizeTracker ObjectStoreSizeTracker => DefaultDatabase.ObjectStoreSizeTracker;

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
        /// <param name="objectStoreVersion">Object store version</param>
        protected void RecoverDatabaseCheckpoint(ref GarnetDatabase db, out long storeVersion, out long objectStoreVersion)
        {
            storeVersion = db.MainStore.Recover();
            objectStoreVersion = -1;

            if (db.ObjectStore != null)
                objectStoreVersion = db.ObjectStore.Recover();

            if (storeVersion > 0 || objectStoreVersion > 0)
            {
                db.LastSaveTime = DateTimeOffset.UtcNow;
            }
        }

        /// <summary>
        /// Asynchronously checkpoint a single database
        /// </summary>
        /// <param name="storeType">Store type to checkpoint</param>
        /// <param name="db">Database to checkpoint</param>
        /// <param name="logger">Logger</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task</returns>
        protected async Task TakeCheckpointAsync(GarnetDatabase db, StoreType storeType, ILogger logger = null, CancellationToken token = default)
        {
            try
            {
                DoCompaction(ref db);
                var lastSaveStoreTailAddress = db.MainStore.Log.TailAddress;
                var lastSaveObjectStoreTailAddress = (db.ObjectStore?.Log.TailAddress).GetValueOrDefault();

                var full = db.LastSaveStoreTailAddress == 0 ||
                           lastSaveStoreTailAddress - db.LastSaveStoreTailAddress >= StoreWrapper.serverOptions.FullCheckpointLogInterval ||
                           (db.ObjectStore != null && (db.LastSaveObjectStoreTailAddress == 0 ||
                                                       lastSaveObjectStoreTailAddress - db.LastSaveObjectStoreTailAddress >= StoreWrapper.serverOptions.FullCheckpointLogInterval));

                var tryIncremental = StoreWrapper.serverOptions.EnableIncrementalSnapshots;
                if (db.MainStore.IncrementalSnapshotTailAddress >= StoreWrapper.serverOptions.IncrementalSnapshotLogSizeLimit)
                    tryIncremental = false;
                if (db.ObjectStore?.IncrementalSnapshotTailAddress >= StoreWrapper.serverOptions.IncrementalSnapshotLogSizeLimit)
                    tryIncremental = false;

                var checkpointType = StoreWrapper.serverOptions.UseFoldOverCheckpoints ? CheckpointType.FoldOver : CheckpointType.Snapshot;
                await InitiateCheckpointAsync(db, full, checkpointType, tryIncremental, storeType, logger);

                if (full)
                {
                    if (storeType is StoreType.Main or StoreType.All)
                        db.LastSaveStoreTailAddress = lastSaveStoreTailAddress;
                    if (storeType is StoreType.Object or StoreType.All)
                        db.LastSaveObjectStoreTailAddress = lastSaveObjectStoreTailAddress;
                }
            }
            catch (Exception ex)
            {
                logger?.LogError(ex, "Checkpointing threw exception, DB ID: {id}", db.Id);
            }
        }

        /// <summary>
        /// Try to take checkpointing lock for specified database
        /// </summary>
        /// <param name="db">Database to checkpoint</param>
        /// <returns>True if acquired a lock</returns>
        protected bool TryPauseCheckpoints(ref GarnetDatabase db)
            => db.CheckpointingLock.TryWriteLock();

        /// <summary>
        /// Release existing checkpointing lock for 
        /// </summary>
        /// <param name="db">Database to checkpoint</param>
        protected void ResumeCheckpoints(ref GarnetDatabase db)
            => db.CheckpointingLock.WriteUnlock();

        /// <summary>
        /// Recover a single database from AOF
        /// </summary>
        /// <param name="db">Database to recover</param>
        protected void RecoverDatabaseAOF(ref GarnetDatabase db)
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
        protected long ReplayDatabaseAOF(AofProcessor aofProcessor, ref GarnetDatabase db, long untilAddress = -1)
        {
            long replicationOffset = 0;
            try
            {
                replicationOffset = aofProcessor.Recover(ref db, untilAddress);
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
        protected void ResetDatabase(ref GarnetDatabase db)
        {
            try
            {
                if (db.MainStore.Log.TailAddress > 64)
                    db.MainStore.Reset();
                if (db.ObjectStore?.Log.TailAddress > 64)
                    db.ObjectStore?.Reset();
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
        protected void EnqueueDatabaseCommit(ref GarnetDatabase db, AofEntryType entryType, long version)
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
        protected void FlushDatabase(ref GarnetDatabase db, bool unsafeTruncateLog)
        {
            db.MainStore.Log.ShiftBeginAddress(db.MainStore.Log.TailAddress, truncateLog: unsafeTruncateLog);
            db.ObjectStore?.Log.ShiftBeginAddress(db.ObjectStore.Log.TailAddress, truncateLog: unsafeTruncateLog);
        }

        /// <summary>
        /// Grow store indexes for specified database, if necessary
        /// </summary>
        /// <param name="db">Database to grow store indexes for</param>
        /// <returns>True if both store indexes are maxed out</returns>
        protected bool GrowIndexesIfNeeded(ref GarnetDatabase db)
        {
            var indexesMaxedOut = true;

            if (!DefaultDatabase.MainStoreIndexMaxedOut)
            {
                var dbMainStore = DefaultDatabase.MainStore;
                if (GrowIndexIfNeeded(StoreType.Main,
                        StoreWrapper.serverOptions.AdjustedIndexMaxCacheLines, dbMainStore.OverflowBucketAllocations,
                        () => dbMainStore.IndexSize, async () => await dbMainStore.GrowIndexAsync()))
                {
                    db.MainStoreIndexMaxedOut = true;
                }
                else
                {
                    indexesMaxedOut = false;
                }
            }

            if (!db.ObjectStoreIndexMaxedOut)
            {
                var dbObjectStore = db.ObjectStore;
                if (GrowIndexIfNeeded(StoreType.Object,
                        StoreWrapper.serverOptions.AdjustedObjectStoreIndexMaxCacheLines,
                        dbObjectStore.OverflowBucketAllocations,
                        () => dbObjectStore.IndexSize, async () => await dbObjectStore.GrowIndexAsync()))
                {
                    db.ObjectStoreIndexMaxedOut = true;
                }
                else
                {
                    indexesMaxedOut = false;
                }
            }

            return indexesMaxedOut;
        }

        /// <summary>
        /// Run compaction on specified database
        /// </summary>
        /// <param name="db">Database to run compaction on</param>
        /// <param name="logger">Logger</param>
        protected void DoCompaction(ref GarnetDatabase db, ILogger logger = null)
        {
            try
            {
                // Periodic compaction -> no need to compact before checkpointing
                if (StoreWrapper.serverOptions.CompactionFrequencySecs > 0) return;

                DoCompaction(ref db, StoreWrapper.serverOptions.CompactionMaxSegments,
                    StoreWrapper.serverOptions.ObjectStoreCompactionMaxSegments, 1,
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
        /// <param name="storeType"></param>
        /// <param name="indexMaxSize"></param>
        /// <param name="overflowCount"></param>
        /// <param name="indexSizeRetriever"></param>
        /// <param name="growAction"></param>
        /// <returns>True if index has reached its max size</returns>
        protected bool GrowIndexIfNeeded(StoreType storeType, long indexMaxSize, long overflowCount, Func<long> indexSizeRetriever, Action growAction)
        {
            Logger?.LogDebug(
                $"IndexAutoGrowTask[{{storeType}}]: checking index size {{indexSizeRetriever}} against max {{indexMaxSize}} with overflow {{overflowCount}}",
                storeType, indexSizeRetriever(), indexMaxSize, overflowCount);

            if (indexSizeRetriever() < indexMaxSize &&
                overflowCount > (indexSizeRetriever() * StoreWrapper.serverOptions.IndexResizeThreshold / 100))
            {
                Logger?.LogInformation(
                    $"IndexAutoGrowTask[{{storeType}}]: overflowCount {{overflowCount}} ratio more than threshold {{indexResizeThreshold}}%. Doubling index size...",
                    storeType, overflowCount, StoreWrapper.serverOptions.IndexResizeThreshold);
                growAction();
            }

            if (indexSizeRetriever() < indexMaxSize) return false;

            Logger?.LogDebug(
                $"IndexAutoGrowTask[{{storeType}}]: checking index size {{indexSizeRetriever}} against max {{indexMaxSize}} with overflow {{overflowCount}}",
                storeType, indexSizeRetriever(), indexMaxSize, overflowCount);
            return true;
        }

        private void DoCompaction(ref GarnetDatabase db, int mainStoreMaxSegments, int objectStoreMaxSegments, int numSegmentsToCompact, LogCompactionType compactionType, bool compactionForceDelete)
        {
            if (compactionType == LogCompactionType.None) return;

            var mainStoreLog = db.MainStore.Log;

            var mainStoreMaxLogSize = (1L << StoreWrapper.serverOptions.SegmentSizeBits()) * mainStoreMaxSegments;

            if (mainStoreLog.ReadOnlyAddress - mainStoreLog.BeginAddress > mainStoreMaxLogSize)
            {
                var readOnlyAddress = mainStoreLog.ReadOnlyAddress;
                var compactLength = (1L << StoreWrapper.serverOptions.SegmentSizeBits()) * (mainStoreMaxSegments - numSegmentsToCompact);
                var untilAddress = readOnlyAddress - compactLength;
                Logger?.LogInformation(
                    "Begin main store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}",
                    untilAddress, mainStoreLog.BeginAddress, readOnlyAddress, mainStoreLog.TailAddress);

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
                }

                Logger?.LogInformation(
                    "End main store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}",
                    untilAddress, mainStoreLog.BeginAddress, readOnlyAddress, mainStoreLog.TailAddress);
            }

            if (db.ObjectStore == null) return;

            var objectStoreLog = db.ObjectStore.Log;

            var objectStoreMaxLogSize = (1L << StoreWrapper.serverOptions.ObjectStoreSegmentSizeBits()) * objectStoreMaxSegments;

            if (objectStoreLog.ReadOnlyAddress - objectStoreLog.BeginAddress > objectStoreMaxLogSize)
            {
                var readOnlyAddress = objectStoreLog.ReadOnlyAddress;
                var compactLength = (1L << StoreWrapper.serverOptions.ObjectStoreSegmentSizeBits()) * (objectStoreMaxSegments - numSegmentsToCompact);
                var untilAddress = readOnlyAddress - compactLength;
                Logger?.LogInformation(
                    "Begin object store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}",
                    untilAddress, objectStoreLog.BeginAddress, readOnlyAddress, objectStoreLog.TailAddress);

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
                }

                Logger?.LogInformation(
                    "End object store compact until {untilAddress}, Begin = {beginAddress}, ReadOnly = {readOnlyAddress}, Tail = {tailAddress}",
                    untilAddress, mainStoreLog.BeginAddress, readOnlyAddress, mainStoreLog.TailAddress);
            }
        }

        private void CompactionCommitAof(ref GarnetDatabase db)
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
        /// <param name="storeType">Store type to checkpoint</param>
        /// <param name="logger">Logger</param>
        /// <returns>Task</returns>
        private async Task InitiateCheckpointAsync(GarnetDatabase db, bool full, CheckpointType checkpointType,
            bool tryIncremental,
            StoreType storeType, ILogger logger = null)
        {
            logger?.LogInformation("Initiating checkpoint; full = {full}, type = {checkpointType}, tryIncremental = {tryIncremental}, storeType = {storeType}, dbId = {dbId}", full, checkpointType, tryIncremental, storeType, db.Id);

            long checkpointCoveredAofAddress = 0;
            if (db.AppendOnlyFile != null)
            {
                if (StoreWrapper.serverOptions.EnableCluster)
                    StoreWrapper.clusterProvider.OnCheckpointInitiated(out checkpointCoveredAofAddress);
                else
                    checkpointCoveredAofAddress = db.AppendOnlyFile.TailAddress;

                if (checkpointCoveredAofAddress > 0)
                    logger?.LogInformation("Will truncate AOF to {tailAddress} after checkpoint (files deleted after next commit), dbId = {dbId}", checkpointCoveredAofAddress, db.Id);
            }

            (bool success, Guid token) storeCheckpointResult = default;
            (bool success, Guid token) objectStoreCheckpointResult = default;
            if (full)
            {
                if (storeType is StoreType.Main or StoreType.All)
                {
                    storeCheckpointResult = await db.MainStore.TakeFullCheckpointAsync(checkpointType);
                    if (StoreWrapper.serverOptions.EnableCluster && StoreWrapper.clusterProvider.IsPrimary())
                        EnqueueCommit(AofEntryType.MainStoreCheckpointEndCommit, db.MainStore.CurrentVersion);
                }

                if (db.ObjectStore != null && (storeType == StoreType.Object || storeType == StoreType.All))
                {
                    objectStoreCheckpointResult = await db.ObjectStore.TakeFullCheckpointAsync(checkpointType);
                    if (StoreWrapper.serverOptions.EnableCluster && StoreWrapper.clusterProvider.IsPrimary())
                        EnqueueCommit(AofEntryType.ObjectStoreCheckpointEndCommit, db.ObjectStore.CurrentVersion);
                }
            }
            else
            {
                if (storeType is StoreType.Main or StoreType.All)
                {
                    storeCheckpointResult = await db.MainStore.TakeHybridLogCheckpointAsync(checkpointType, tryIncremental);
                    if (StoreWrapper.serverOptions.EnableCluster && StoreWrapper.clusterProvider.IsPrimary())
                        EnqueueCommit(AofEntryType.MainStoreCheckpointEndCommit, db.MainStore.CurrentVersion);
                }

                if (db.ObjectStore != null && (storeType == StoreType.Object || storeType == StoreType.All))
                {
                    objectStoreCheckpointResult = await db.ObjectStore.TakeHybridLogCheckpointAsync(checkpointType, tryIncremental);
                    if (StoreWrapper.serverOptions.EnableCluster && StoreWrapper.clusterProvider.IsPrimary())
                        EnqueueCommit(AofEntryType.ObjectStoreCheckpointEndCommit, db.ObjectStore.CurrentVersion);
                }
            }

            // If cluster is enabled the replication manager is responsible for truncating AOF
            if (StoreWrapper.serverOptions.EnableCluster && StoreWrapper.serverOptions.EnableAOF)
            {
                StoreWrapper.clusterProvider.SafeTruncateAOF(storeType, full, checkpointCoveredAofAddress,
                    storeCheckpointResult.token, objectStoreCheckpointResult.token);
            }
            else
            {
                db.AppendOnlyFile?.TruncateUntil(checkpointCoveredAofAddress);
                db.AppendOnlyFile?.Commit();
            }

            if (db.ObjectStore != null)
            {
                // During the checkpoint, we may have serialized Garnet objects in (v) versions of objects.
                // We can now safely remove these serialized versions as they are no longer needed.
                using var iter1 = db.ObjectStore.Log.Scan(db.ObjectStore.Log.ReadOnlyAddress,
                    db.ObjectStore.Log.TailAddress, ScanBufferingMode.SinglePageBuffering, includeSealedRecords: true);
                while (iter1.GetNext(out _, out _, out var value))
                {
                    if (value != null)
                        ((GarnetObjectBase)value).serialized = null;
                }
            }

            logger?.LogInformation("Completed checkpoint for DB ID: {id}", db.Id);
        }
    }
}