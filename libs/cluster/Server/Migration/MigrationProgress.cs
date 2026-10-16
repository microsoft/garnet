// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;
using Garnet.server;

namespace Garnet.cluster
{
    /// <summary>
    /// Tells requests parked on a key that is being migrated when the migration moves on.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A request that finds its key blocked by a migration reads <see cref="Version"/> first, then checks the key, and
    /// registers against the version it read only if the key is still blocked. A migration step changes the key's
    /// state first and then calls <see cref="Signal"/>, which advances the version and notifies everything registered.
    /// A step that lands between the request's check and its registration has advanced the version by then, so the
    /// registration is refused and the request checks again instead of waiting for a notification that has already
    /// gone out. Reading the version before the check is what guarantees that a state change the check missed is
    /// one the version has not yet reflected.
    /// </para>
    /// <para>
    /// A waiter whose session is torn down while it waits stays registered until the next <see cref="Signal"/>.
    /// Every step that unblocks a key, up to and including the removal of the migration, signals, so that is
    /// bounded by the time the migration takes to move on.
    /// </para>
    /// </remarks>
    internal sealed class MigrationProgress
    {
        readonly Lock gate = new();

        long version;
        List<IMigrationWaiter> waiters = [];

        // The list the previous signal notified, kept so that parking and signalling allocate nothing once warm
        List<IMigrationWaiter> spare;

        /// <summary>
        /// Count of migration steps so far. A request reads it before it checks whether its key is accessible.
        /// </summary>
        internal long Version => Volatile.Read(ref version);

        /// <summary>
        /// Registers a waiter, unless the migration has moved on since <paramref name="version"/> was read.
        /// </summary>
        /// <param name="waiter">Waiter to notify. Notified at most once per registration.</param>
        /// <param name="version">Value of <see cref="Version"/> read before the key was checked.</param>
        /// <returns>False if the waiter was not registered because a step has been signalled since.</returns>
        internal bool TryRegister(IMigrationWaiter waiter, long version)
        {
            lock (gate)
            {
                if (this.version != version)
                    return false;

                waiters.Add(waiter);
                return true;
            }
        }

        /// <summary>
        /// Reports that a migration step has changed what keys can be accessed, and notifies every waiter.
        /// Called after the change itself, and never while holding a lock a waiter's resume could need.
        /// </summary>
        internal void Signal()
        {
            List<IMigrationWaiter> notify;
            lock (gate)
            {
                // A release store, so a request that reads the new version also sees the change that preceded it
                Volatile.Write(ref version, version + 1);

                if (waiters.Count == 0)
                    return;

                notify = waiters;
                waiters = spare ?? [];
                spare = null;
            }

            // Outside the lock, because a notified session may come straight back to register again
            foreach (var waiter in notify)
                waiter.OnMigrationProgress();

            notify.Clear();
            lock (gate)
                spare ??= notify;
        }
    }
}
