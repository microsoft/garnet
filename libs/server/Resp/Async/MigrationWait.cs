// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;

namespace Garnet.server
{
    /// <summary>
    /// Await source for a session parked because a key its next command touches is being migrated, so the command
    /// can be neither served nor answered until the migration moves on.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The migration calls <see cref="OnMigrationProgress"/> from the thread that advanced it. Completing the wait
    /// only queues the session's continuation to the thread pool, so that thread is not held up.
    /// </para>
    /// <para>
    /// A migration can take arbitrarily long to move on, and a session that is torn down while it waits is never
    /// resumed, so the wait takes itself off the migration when it is cancelled.
    /// </para>
    /// </remarks>
    internal sealed class MigrationWait : SessionWaitSource, IMigrationWaiter
    {
        IClusterSession listedWith;

        /// <summary>
        /// Registers the wait through <paramref name="clusterSession"/>, so that a teardown from here on takes it off.
        /// </summary>
        /// <param name="clusterSession">The cluster session whose verification found the key blocked.</param>
        /// <returns>False if the migration has moved on since the verification, in which case nothing was registered.</returns>
        /// <remarks>
        /// The wait records where it is registered after joining, then checks for a teardown. A teardown that ran
        /// earlier found nothing to take the wait off and is seen by the check; one that runs later finds where it is
        /// registered, and the wait is already there. The exchange is a full fence that pairs with the one in
        /// <see cref="OnCancelling"/>, so the two cannot both miss the other side.
        /// </remarks>
        internal bool TryRegister(IClusterSession clusterSession)
        {
            if (!clusterSession.TryRegisterMigrationWaiter(this))
                return false;

            _ = Interlocked.Exchange(ref listedWith, clusterSession);
            if (Cancelled)
                clusterSession.RemoveMigrationWaiter(this);
            return true;
        }

        /// <inheritdoc/>
        protected override void OnCancelling() => Interlocked.Exchange(ref listedWith, null)?.RemoveMigrationWaiter(this);
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