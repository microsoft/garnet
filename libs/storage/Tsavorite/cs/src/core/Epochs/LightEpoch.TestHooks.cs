// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Read-only views of <see cref="LightEpoch"/>'s internal state, used only by the unit tests in
    /// Tsavorite.test.epoch.
    /// </summary>
    public sealed unsafe partial class LightEpoch
    {
        /// <summary>
        /// The epoch table index this thread currently holds for this instance, or 0 if unprotected.
        /// </summary>
        internal int TestHookThisThreadEntry() => Metadata.Entries.GetRef(instanceId);

        /// <summary>
        /// The epoch this thread currently announces for this instance, or 0 if unprotected.
        /// </summary>
        internal long TestHookThisThreadAnnouncedEpoch()
        {
            var entry = Metadata.Entries.GetRef(instanceId);
            return entry == kInvalidIndex ? 0 : (*(tableAligned + entry)).localCurrentEpoch;
        }

        /// <summary>
        /// The epoch announced in epoch table slot <paramref name="entry"/>, or 0 if the slot is free.
        /// </summary>
        internal long TestHookAnnouncedEpochAt(int entry) => (*(tableAligned + entry)).localCurrentEpoch;

        /// <summary>
        /// The thread id recorded in epoch table slot <paramref name="entry"/>, or 0 if the slot is free.
        /// </summary>
        internal int TestHookThreadIdAt(int entry) => (*(tableAligned + entry)).threadId;

        /// <summary>
        /// Capacity of the drain list.
        /// </summary>
        internal static int TestHookDrainListCapacity => kDrainListSize;

        /// <summary>
        /// Number of entries in the epoch table.
        /// </summary>
        internal static int TestHookTableSize => kTableSize;

        /// <summary>
        /// Number of threads currently waiting for an epoch table entry.
        /// </summary>
        internal int TestHookWaiterCount => waiterCount & ~kDisposedFlag;

        /// <summary>
        /// Signals issued to waiters but not yet consumed. This is the count that overflows
        /// <see cref="System.Threading.SemaphoreSlim"/> if <see cref="Release"/> signals unconditionally.
        /// </summary>
        internal int TestHookOutstandingWaiterSignals => waiterSemaphore.CurrentCount;

        /// <summary>
        /// Signal reservations taken by <see cref="SignalWaiter"/>, an upper bound on
        /// <see cref="TestHookOutstandingWaiterSignals"/>.
        /// </summary>
        internal int TestHookPendingWaiterSignals => pendingWaiterSignals;
    }
}