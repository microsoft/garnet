// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    enum SyncStatus : byte
    {
        SUCCESS,
        FAILED,
        INPROGRESS,
        INITIALIZING
    }

    struct SyncStatusInfo
    {
        public SyncStatus syncStatus;
        public string error;
    }
}
