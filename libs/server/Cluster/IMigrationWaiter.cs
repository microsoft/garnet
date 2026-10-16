// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Garnet.server
{
    /// <summary>
    /// Notified by a cluster session when a key migration moves on after a request waited on it, which is when a
    /// key the request touches may have become accessible.
    /// </summary>
    public interface IMigrationWaiter
    {
        /// <summary>
        /// Called once per registration, on the thread that advanced the migration or removed it, so it must not
        /// block. The waiter does not know whether its key is now accessible: it verifies the request again.
        /// </summary>
        void OnMigrationProgress();
    }
}
