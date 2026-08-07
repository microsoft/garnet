// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public sealed unsafe partial class LightEpoch
    {
        /// <summary>
        /// The epoch table slot at <paramref name="index"/>, by reference.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ref Entry EntryAt(int index) => ref *(tableAligned + index);

        /// <summary>
        /// Asserts that the slot at <paramref name="index"/> is acquired by the calling thread.
        /// </summary>
        [Conditional("DEBUG")]
        private void DebugAssertEpochAcquired(int index, string message = "Epoch table entry is not acquired by this thread")
        {
            ref var entry = ref EntryAt(index);
            Debug.Assert(entry.localCurrentEpoch > 0, message, "The slot has no announced epoch.");
            Debug.Assert(entry.threadId == Metadata.threadId, message, "The slot does not carry this thread's id.");
        }
    }
}