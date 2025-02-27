// Copyright (c) Microsoft Corporation.
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

    public interface IDatabaseManager : IDisposable
    {
        /// <summary>
        /// Reference to default database (DB 0)
        /// </summary>
        public ref GarnetDatabase DefaultDatabase { get; }

        /// <summary>
        /// Store (of DB 0)
        /// </summary>
        public TsavoriteKV<SpanByte, SpanByte, MainStoreFunctions, MainStoreAllocator> MainStore { get; }

        /// <summary>
        /// Object store (of DB 0)
        /// </summary>
        public TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> ObjectStore { get; }

        /// <summary>
        /// AOF (of DB 0)
        /// </summary>
        public TsavoriteLog AppendOnlyFile { get; }

        /// <summary>
        /// Last save time (of DB 0)
        /// </summary>
        public DateTimeOffset LastSaveTime { get; }

        /// <summary>
        /// Object store size tracker (of DB 0)
        /// </summary>
        public CacheSizeTracker ObjectStoreSizeTracker { get; }

        /// <summary>
        /// Version map (of DB 0)
        /// </summary>
        internal WatchVersionMap VersionMap { get; }

        /// <summary>
        /// Number of current logical databases
        /// </summary>
        public int DatabaseCount { get; }

        /// <summary>
        /// Try to get or add a new database
        /// </summary>
        /// <param name="dbId">Database ID</param>
        /// <param name="db">Database</param>
        /// <returns>True if database was retrieved or added successfully</returns>
        public bool TryGetOrAddDatabase(int dbId, out GarnetDatabase db);

        /// <summary>
        /// Mark the beginning of a checkpoint by taking and a lock to avoid concurrent checkpointing
        /// </summary>
        /// <param name="dbId">ID of database to lock</param>
        /// <returns>True if lock acquired</returns>
        public bool TryPauseCheckpoints(int dbId);

        /// <summary>
        /// Release checkpoint task lock
        /// </summary>
        /// <param name="dbId">ID of database to unlock</param>
        public void ResumeCheckpoints(int dbId);

        public void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null);

        /// <summary>
        /// Take checkpoint of all active databases
        /// </summary>
        /// <param name="background">True if method can return before checkpoint is taken</param>
        /// <param name="storeType">Store type to checkpoint</param>
        /// <param name="logger">Logger</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>False if another checkpointing process is already in progress</returns>
        public bool TakeCheckpoint(bool background, StoreType storeType = StoreType.All, ILogger logger = null, CancellationToken token = default);

        /// <summary>
        /// Take checkpoint of specified database ID
        /// </summary>
        /// <param name="background">True if method can return before checkpoint is taken</param>
        /// <param name="dbId">ID of database to checkpoint</param>
        /// <param name="storeType">Store type to checkpoint</param>
        /// <param name="logger">Logger</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>False if another checkpointing process is already in progress</returns>
        public bool TakeCheckpoint(bool background, int dbId, StoreType storeType = StoreType.All,
            ILogger logger = null, CancellationToken token = default);

        /// <summary>
        /// Take a checkpoint if no checkpoint was taken after the provided time offset
        /// </summary>
        /// <param name="entryTime">Time offset</param>
        /// <param name="dbId">ID of database to checkpoint (default: DB 0)</param>
        /// <returns>Task</returns>
        public Task TakeOnDemandCheckpointAsync(DateTimeOffset entryTime, int dbId = 0);

        /// <summary>
        /// Take a checkpoint of all active databases whose AOF size has reached a specified limit
        /// </summary>
        /// <param name="aofSizeLimit">AOF size limit</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="logger">Logger</param>
        /// <returns>Task</returns>
        public Task TaskCheckpointBasedOnAofSizeLimitAsync(long aofSizeLimit, CancellationToken token = default,
            ILogger logger = null);

        /// <summary>
        /// Commit to AOF for all active databases
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <param name="logger">Logger</param>
        /// <returns>Task</returns>
        public Task CommitToAofAsync(CancellationToken token = default, ILogger logger = null);

        /// <summary>
        /// Commit to AOF for specified database
        /// </summary>
        /// <param name="dbId">ID of database to commit</param>
        /// <param name="token">Cancellation token</param>
        /// <param name="logger">Logger</param>
        /// <returns>Task</returns>
        public Task CommitToAofAsync(int dbId, CancellationToken token = default, ILogger logger = null);

        /// <summary>
        /// Wait for commit to AOF for all active databases
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <param name="logger">Logger</param>
        /// <returns>Task</returns>
        public Task WaitForCommitToAofAsync(CancellationToken token = default, ILogger logger = null);

        /// <summary>
        /// Recover AOF
        /// </summary>
        public void RecoverAOF();

        /// <summary>
        /// When replaying AOF we do not want to write AOF records again.
        /// </summary>
        public long ReplayAOF(long untilAddress = -1);

        /// <summary>
        /// Do compaction
        /// </summary>
        public void DoCompaction(CancellationToken token = default);

        /// <summary>
        /// Grows indexes of both main store and object store for all active databases if current size is too small
        /// </summary>
        /// <returns>True if indexes are maxed out</returns>
        public bool GrowIndexesIfNeeded(CancellationToken token = default);

        /// <summary>
        /// Start object size trackers for all active databases
        /// </summary>
        public void StartObjectSizeTrackers(CancellationToken token = default);

        /// <summary>
        /// Reset
        /// </summary>
        /// <param name="dbId">Database ID</param>
        public void Reset(int dbId = 0);

        /// <summary>
        /// Append a checkpoint commit to the AOF
        /// </summary>
        /// <param name="isMainStore"></param>
        /// <param name="version"></param>
        /// <param name="dbId"></param>
        public void EnqueueCommit(bool isMainStore, long version, int dbId = 0);

        /// <summary>
        /// Get database DB ID
        /// </summary>
        /// <param name="dbId">DB Id</param>
        /// <param name="db">Database</param>
        /// <returns>True if database found</returns>
        public bool TryGetDatabase(int dbId, out GarnetDatabase db);

        /// <summary>
        /// Flush database with specified ID
        /// </summary>
        /// <param name="unsafeTruncateLog">Truncate log</param>
        /// <param name="dbId">Database ID</param>
        public void FlushDatabase(bool unsafeTruncateLog, int dbId = 0);

        /// <summary>
        /// Flush all active databases 
        /// </summary>
        /// <param name="unsafeTruncateLog">Truncate log</param>
        public void FlushAllDatabases(bool unsafeTruncateLog);

        /// <summary>
        /// Try to swap between two database instances
        /// </summary>
        /// <param name="dbId1">First database ID</param>
        /// <param name="dbId2">Second database ID</param>
        /// <returns>True if swap successful</returns>
        public bool TrySwapDatabases(int dbId1, int dbId2);

        /// <summary>
        /// Create a shallow copy of the IDatabaseManager instance and copy databases to the new instance
        /// </summary>
        /// <param name="enableAof">True if AOF should be enabled in the clone</param>
        /// <returns></returns>
        public IDatabaseManager Clone(bool enableAof);

        internal FunctionsState CreateFunctionsState(int dbId = 0);
    }
}
