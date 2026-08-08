// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;

namespace Garnet.shared
{
    public sealed unsafe partial class LightEpoch
    {
        /// <summary>
        /// Asserts that a slot is reserved for the calling thread.
        /// </summary>
        [Conditional("DEBUG")]
        private static void DebugAssertEntryReserved(int index, string message = "No epoch table entry is reserved for this thread")
        {
            Debug.Assert(index != kInvalidIndex, message, "No slot is reserved for this thread.");
            Debug.Assert(index > kInvalidIndex && index <= kTableSize, message, $"Slot {index} is out of range.");
        }

        /// <summary>
        /// Asserts that no slot is reserved for the calling thread.
        /// </summary>
        [Conditional("DEBUG")]
        private static void DebugAssertEntryNotReserved(int index, string message = "An epoch table entry is already reserved for this thread")
            => Debug.Assert(index == kInvalidIndex, message, $"Slot {index} is already reserved for this thread.");

        /// <summary>
        /// Asserts that the slot at <paramref name="index"/> is acquired by the calling thread.
        /// </summary>
        [Conditional("DEBUG")]
        private void DebugAssertEpochAcquired(int index, string message = "Epoch table entry is not acquired by this thread")
        {
            DebugAssertEntryReserved(index, message);
            ref var entry = ref *(tableAligned + index);
            Debug.Assert(entry.localCurrentEpoch > 0, message, "The slot has no announced epoch.");
            Debug.Assert(entry.threadId == Metadata.threadId, message, "The slot does not carry this thread's id.");
        }
    }
}