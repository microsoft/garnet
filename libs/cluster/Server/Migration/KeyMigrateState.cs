// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.cluster
{
    /// <summary>
    /// Used to mark the state of key owned by single given MigrateSession during key migration
    /// </summary>
    public enum KeyMigrateState : byte
    {
        /// <summary>
        /// In queue for migration, can serve reads and writes if key exists
        /// </summary>
        PENDING,

        /// <summary>
        /// Actively being migrated, writes will be delayed
        /// </summary>
        MIGRATING,

        /// <summary>
        /// Migrated to target node, reads and writes will be redirected through ASK
        /// </summary>
        MIGRATED
    }
}
