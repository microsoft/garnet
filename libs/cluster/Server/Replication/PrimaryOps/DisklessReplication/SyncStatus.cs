// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// Replication attach sync status
    /// </summary>
    enum SyncStatus : byte
    {
        SUCCESS,
        FAILED,
        INPROGRESS,
        INITIALIZING
    }

    /// <summary>
    /// Replication sync status info
    /// </summary>
    struct SyncStatusInfo
    {
        public SyncStatus syncStatus;
        public string error;
    }
}