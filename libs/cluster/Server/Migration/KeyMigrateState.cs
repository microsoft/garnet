// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// Used to mark the state of key owned by single given MigrateSession during key migration
    /// </summary>
    public enum KeyMigrationStatus : byte
    {
        /// <summary>
        /// Key owned by a specific MigrateSession but is not actively being migrated.
        /// Reads and writes can be served if the keys exist
        /// </summary>
        QUEUED,

        /// <summary>
        /// Key is actively being migrated by a specific MigrateSession.
        /// Writes will be delayed until status goes back to QUEUED or MIGRATED
        /// Reads can be served without any restriction.
        /// </summary>
        MIGRATING,

        /// <summary>
        /// Key is being deleted after it was sent to the target node.
        /// Reads and writes will be delayed.
        /// We need to delay reads to avoid the scenario where a key exists during validation but was deleted before read executes.
        /// </summary>
        DELETING,

        /// <summary>
        /// Key owned by a specific MigrateSession and has completed all the steps to be MIGRATED to target node.
        /// This does not mean that key existed or has not expired, just that all the steps associated with MIGRATED have completed.
        /// This can happen for a key that was provided as an argument in MIGRATE command but did not exist or expired for both main and object stores.
        /// </summary>
        MIGRATED,
    }
}