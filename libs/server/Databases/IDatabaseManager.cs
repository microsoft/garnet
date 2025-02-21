// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    internal interface IDatabaseManager : IDisposable
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

        public void RecoverCheckpoint(bool replicaRecover = false, bool recoverMainStoreFromToken = false, bool recoverObjectStoreFromToken = false, CheckpointMetadata metadata = null);

        /// <summary>
        /// Recover AOF
        /// </summary>
        public void RecoverAOF();

        /// <summary>
        /// When replaying AOF we do not want to write AOF records again.
        /// </summary>
        public long ReplayAOF(long untilAddress = -1);

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

        internal FunctionsState CreateFunctionsState(CustomCommandManager customCommandManager, GarnetObjectSerializer garnetObjectSerializer, int dbId = 0);
    }
}
