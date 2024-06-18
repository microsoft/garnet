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
        /// Key queued for migration, can serve both read and write requests
        /// </summary>
        QUEUED,

        /// <summary>
        /// Key is migrating, can serve only read requests. Write requests are delayed (i.e spin-wait)
        /// </summary>
        MIGRATING,

        /// <summary>
        /// Key is being deleted, reads and writes requests are delayed (i.e. spin-wait)
        /// </summary>
        DELETING,

        /// <summary>
        /// If Key existed it has been migrated to target node
        /// </summary>
        MIGRATED,
    }
}
