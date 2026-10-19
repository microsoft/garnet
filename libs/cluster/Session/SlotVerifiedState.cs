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
        TRYAGAIN,
        /// <summary>
        /// A key is being migrated, so the request can be neither served nor redirected until the migration
        /// moves on. Never sent to a client: the caller waits for the migration to progress and verifies again.
        /// </summary>
        WAIT,
        /// <summary>
        /// A key is being migrated, and whether it is still here can only be learned by reading the device. Never sent
        /// to a client: the caller moves the request to the thread pool and verifies it again there.
        /// </summary>
        OFFLOAD
    }
}