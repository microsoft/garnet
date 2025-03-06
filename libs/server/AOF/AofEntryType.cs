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
        /// Checkpoint for main store start
        /// </summary>
        MainStoreCheckpointStartCommit = 0x30,
        /// <summary>
        /// Checkpoint for object store start
        /// </summary>
        ObjectStoreCheckpointStartCommit = 0x31,
        /// <summary>
        /// Checkpoint for main store end
        /// </summary>
        MainStoreCheckpointEndCommit = 0x32,
        /// <summary>
        /// Checkpoint for object store end
        /// </summary>
        ObjectStoreCheckpointEndCommit = 0x33,
        /// <summary>
        /// Streaming checkpoint for main store start
        /// </summary>
        MainStoreStreamingCheckpointStartCommit = 0x40,
        /// <summary>
        /// Streaming checkpoint for object store start
        /// </summary>
        ObjectStoreStreamingCheckpointStartCommit = 0x41,
        /// <summary>
        /// Streaming checkpoint for main store end
        /// </summary>
        MainStoreStreamingCheckpointEndCommit = 0x42,
        /// <summary>
        /// Streaming checkpoint for object store end
        /// </summary>
        ObjectStoreStreamingCheckpointEndCommit = 0x43,
        /// <summary>
        /// StoredProcedure
        /// </summary>
        StoredProcedure = 0x50,
    }

    internal enum AofStoreType : byte
    {
        MainStoreType = 0x0,
        ObjectStoreType = 0x1,
        TxnType = 0x2,
        ReplicationType = 0x3,
        CheckpointType = 0x4
    }
}