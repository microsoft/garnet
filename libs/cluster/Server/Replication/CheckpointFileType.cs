// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// Checkpoint file type
    /// </summary>
    enum CheckpointFileType : byte
    {
        /// <summary>
        /// None
        /// </summary>
        NONE = 0,
        /// <summary>
        /// Store Hybrid LOG - Main
        /// </summary>
        STORE_HLOG = 1,
        /// <summary>
        /// Store Hybrid LOG - Object
        /// </summary>
        STORE_HLOG_OBJ = 2,
        // Value 3 reserved (was STORE_DLOG, removed with incremental snapshots)
        /// <summary>
        /// Store Index
        /// </summary>
        STORE_INDEX = 4,
        /// <summary>
        /// Store Snapshot - Main
        /// </summary>
        STORE_SNAPSHOT = 5,
        /// <summary>
        /// Store Snapshot - Object
        /// </summary>
        STORE_SNAPSHOT_OBJ = 6,
    }
}