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
            return entry == kInvalidIndex ? 0 : EntryAt(entry).localCurrentEpoch;
        }

        /// <summary>
        /// The epoch announced in epoch table slot <paramref name="entry"/>, or 0 if the slot is free.
        /// </summary>
        internal long TestHookAnnouncedEpochAt(int entry) => EntryAt(entry).localCurrentEpoch;

        /// <summary>
        /// The thread id recorded in epoch table slot <paramref name="entry"/>, or 0 if the slot is free.
        /// </summary>
        internal int TestHookThreadIdAt(int entry) => EntryAt(entry).threadId;

        /// <summary>
        /// Capacity of the drain list.
        /// </summary>
        internal static int TestHookDrainListCapacity => kDrainListSize;
    }
}