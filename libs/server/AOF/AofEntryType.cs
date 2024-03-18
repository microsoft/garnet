// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    enum AofEntryType : byte
    {
        StoreUpsert = 0x00,
        StoreRMW = 0x01,
        StoreDelete = 0x02,
        ObjectStoreUpsert = 0x10,
        ObjectStoreRMW = 0x11,
        ObjectStoreDelete = 0x12,
        TxnStart = 0x20,
        TxnCommit = 0x21,
        TxnAbort = 0x22,
        MainStoreCheckpointCommit = 0x30,
        ObjectStoreCheckpointCommit = 0x31,
        StoredProcedure = 0x50,
    }

    enum AofStoreType : byte
    {
        MainStoreType = 0x0,
        ObjectStoreType = 0x1,
        TxnType = 0x2,
        ReplicationType = 0x3,
        CheckpointType = 0x4
    }
}