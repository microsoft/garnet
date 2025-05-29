// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// Use to mark the status of the sketch that contains a collection of keys that need to be migrated
    /// to a target node.
    /// </summary>
    public enum SketchStatus : byte
    {
        /// <summary>
        /// Sketch is being constructed by adding keys to be sent to the target node.
        /// Reads and writes can be served if the keys exists in the database.
        /// </summary>
        INITIALIZING,

        /// <summary>
        /// Keys previously added to the sketch are being actively send to the target node.
        /// Writes to referenced keys will be delayed until sketch status reaches MIGRATED.
        /// Reads can be served without any restriction.
        /// </summary>
        TRANSMITTING,

        /// <summary>
        /// Keys previously added to the sketch are being deleted, after being sent to the target node.
        /// Reads and writes to referenced kesy will be delayed.
        /// We need to delay reads to avoid the scenario where a key exists during validation but was deleted before the actual read operation executes.
        /// </summary>
        DELETING,

        /// <summary>
        /// Keys added to the corresponding sketch are now migrated to the target node.
        /// Read and writes are free to proceed.
        /// Because keys were deleted earlier the operations will result in a -ASK redirection message.
        /// </summary>
        MIGRATED,
    }
}