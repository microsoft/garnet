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
        STORE_HLOG,
        /// <summary>
        /// Store Hybrid LOG - Object
        /// </summary>
        STORE_HLOG_OBJ,
        /// <summary>
        /// Store Delta Log
        /// </summary>
        STORE_DLOG,
        /// <summary>
        /// Store Index
        /// </summary>
        STORE_INDEX,
        /// <summary>
        /// Store Snapshot - Main
        /// </summary>
        STORE_SNAPSHOT,
        /// <summary>
        /// Store Snapshot - Object
        /// </summary>
        STORE_SNAPSHOT_OBJ,
    }
}