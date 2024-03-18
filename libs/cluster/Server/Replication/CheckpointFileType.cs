// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.server;

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
        /// <summary>
        /// Object Store Hybrid Log - Main
        /// </summary>
        OBJ_STORE_HLOG,
        /// <summary>
        /// Object Store Hybrid Log - Object
        /// </summary>
        OBJ_STORE_HLOG_OBJ,
        /// <summary>
        /// Object Store Delta Log
        /// </summary>
        OBJ_STORE_DLOG,
        /// <summary>
        /// Object Store Index
        /// </summary>
        OBJ_STORE_INDEX,
        /// <summary>
        /// Object Store Snapshot - Main
        /// </summary>
        OBJ_STORE_SNAPSHOT,
        /// <summary>
        /// Object Store Snapshot - Object
        /// </summary>
        OBJ_STORE_SNAPSHOT_OBJ,
    }

    static class CheckpointFileTypeExtensions
    {
        public static StoreType ToStoreType(this CheckpointFileType type)
        {
            return type switch
            {
                CheckpointFileType.STORE_HLOG or
                CheckpointFileType.STORE_DLOG or
                CheckpointFileType.STORE_INDEX or
                CheckpointFileType.STORE_SNAPSHOT => StoreType.Main,
                CheckpointFileType.OBJ_STORE_HLOG or
                CheckpointFileType.OBJ_STORE_DLOG or
                CheckpointFileType.OBJ_STORE_INDEX or
                CheckpointFileType.OBJ_STORE_SNAPSHOT or
                CheckpointFileType.OBJ_STORE_SNAPSHOT_OBJ => StoreType.Object,
                _ => throw new Exception($"ToStoreType: unexpected state {type}")
            };
        }
    }
}