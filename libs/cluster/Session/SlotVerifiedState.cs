// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// SlotVerifiedState
    /// </summary>
    public enum SlotVerifiedState : byte
    {
        /// <summary>
        /// OK to server request
        /// </summary>
        OK,
        /// <summary>
        /// Slot down
        /// </summary>
        CLUSTERDOWN,
        /// <summary>
        /// Slot moved to remote node
        /// </summary>
        MOVED,
        /// <summary>
        /// Ask target node
        /// </summary>
        ASK,
        /// <summary>
        /// Crosslot operation
        /// </summary>
        CROSSLOT,
        /// <summary>
        /// Migrating cannot write
        /// </summary>
        MIGRATING,
    }
}