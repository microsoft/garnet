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
        /// Store Hybrid LOG
        /// </summary>
        STORE_HLOG,
        /// <summary>
        /// Store Delta Log
        /// </summary>
        STORE_DLOG,
        /// <summary>
        /// Store Index
        /// </summary>
        STORE_INDEX,
        /// <summary>
        /// Store Snapshot
        /// </summary>
        STORE_SNAPSHOT,
    }
}