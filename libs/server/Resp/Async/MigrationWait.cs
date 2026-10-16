// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session parked because a key its next command touches is being migrated, so the command
    /// can be neither served nor answered until the migration moves on.
    /// </summary>
    /// <remarks>
    /// The migration calls <see cref="OnMigrationProgress"/> from the thread that advanced it. Completing the wait
    /// only queues the session's continuation to the thread pool, so that thread is not held up.
    /// </remarks>
    internal sealed class MigrationWait : SessionWaitSource, IMigrationWaiter
    {
        /// <summary>
        /// Arms the source for one wait. Called before the source is registered with the migration: a notification
        /// that arrives before it is armed is lost.
        /// </summary>
        internal new void Arm() => base.Arm();

        /// <summary>
        /// Waits for an armed source. The returned task completes when the migration moves on, and faults if the
        /// session is torn down first.
        /// </summary>
        /// <returns>A task representing the wait.</returns>
        internal ValueTask WaitAsync()
        {
            // Teardown may have run between arming and here, in which case nothing else will complete it.
            if (Cancelled)
                _ = Complete(canceled: true);

            return Pending;
        }

        /// <inheritdoc/>
        public void OnMigrationProgress() => _ = Complete(canceled: false);
    }
}
