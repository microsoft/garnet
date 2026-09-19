// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Checkpoint file type.
    /// </summary>
    internal enum CheckpointFileType : byte
    {
        /// <summary>
        /// None.
        /// </summary>
        NONE = 0,
        /// <summary>
        /// Store hybrid log.
        /// </summary>
        STORE_HLOG = 1,
        /// <summary>
        /// Store object log.
        /// </summary>
        STORE_HLOG_OBJ = 2,
        // Value 3 reserved (was STORE_DLOG, removed with incremental snapshots)
        /// <summary>
        /// Store index.
        /// </summary>
        STORE_INDEX = 4,
        /// <summary>
        /// Store snapshot.
        /// </summary>
        STORE_SNAPSHOT = 5,
        /// <summary>
        /// Store snapshot object log.
        /// </summary>
        STORE_SNAPSHOT_OBJ = 6,
        /// <summary>
        /// RangeIndex per-flush snapshot file.
        /// </summary>
        STORE_RANGEINDEX_FLUSH = 7,
        /// <summary>
        /// RangeIndex per-checkpoint snapshot file.
        /// </summary>
        STORE_RANGEINDEX_SNAPSHOT = 8,
    }
}