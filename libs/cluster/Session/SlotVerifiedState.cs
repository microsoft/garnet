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
        /// Crossslot operation
        /// </summary>
        CROSSSLOT,
        /// <summary>
        /// Used for multi-key operations referring to a collection of keys some of which have migrated
        /// </summary>
        TRYAGAIN
    }
}