// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    public enum AofEntryType : byte
    {
        /// <summary>
        /// Store upsert
        /// </summary>
        StoreUpsert = 0x00,
        /// <summary>
        /// Store RMW
        /// </summary>
        StoreRMW = 0x01,
        /// <summary>
        /// Store delete
        /// </summary>
        StoreDelete = 0x02,
        /// <summary>
        /// Object store upsert
        /// </summary>
        ObjectStoreUpsert = 0x10,
        /// <summary>
        /// Object store RMW
        /// </summary>
        ObjectStoreRMW = 0x11,
        /// <summary>
        /// Object store delete
        /// </summary>
        ObjectStoreDelete = 0x12,
        /// <summary>
        /// Transaction start
        /// </summary>
        TxnStart = 0x20,
        /// <summary>
        /// Transaction commit
        /// </summary>
        TxnCommit = 0x21,
        /// <summary>
        /// Transaction abort
        /// </summary>
        TxnAbort = 0x22,
        /// <summary>
        /// Checkpoint start marker for unified checkpoint
        /// </summary>
        CheckpointStartCommit = 0x30,
        /// <summary>
        /// Checkpoint end marker for unified checkpoint
        /// </summary>
        CheckpointEndCommit = 0x32,
        /// <summary>
        /// Streaming checkpoint start marker for main store
        /// </summary>
        MainStoreStreamingCheckpointStartCommit = 0x40,
        /// <summary>
        /// Streaming checkpoint start marker for object store
        /// </summary>
        ObjectStoreStreamingCheckpointStartCommit = 0x41,
        /// <summary>
        /// Streaming checkpoint end marker for main store
        /// </summary>
        MainStoreStreamingCheckpointEndCommit = 0x42,
        /// <summary>
        /// Streaming checkpoint end marker for object store
        /// </summary>
        ObjectStoreStreamingCheckpointEndCommit = 0x43,
        /// <summary>
        /// StoredProcedure
        /// </summary>
        StoredProcedure = 0x50,

        /// <summary>
        /// Flush all
        /// </summary>
        FlushAll = 0x60,
        /// <summary>
        /// Flush db
        /// </summary>
        FlushDb = 0x61,

        /// <summary>
        /// Unified store upsert sting
        /// </summary>
        UnifiedStoreStringUpsert = 0x70,
        /// <summary>
        /// Unified store upsert object
        /// </summary>
        UnifiedStoreObjectUpsert = 0x71,
        /// <summary>
        /// Unified store RMW
        /// </summary>
        UnifiedStoreRMW = 0x72,
        /// <summary>
        /// Unified store delete
        /// </summary>
        UnifiedStoreDelete = 0x73,
    }

    internal enum AofStoreType : byte
    {
        MainStoreType = 0x0,
        ObjectStoreType = 0x1,
        TxnType = 0x2,
        ReplicationType = 0x3,
        CheckpointType = 0x4,
        FlushDbType = 0x5,
    }
}