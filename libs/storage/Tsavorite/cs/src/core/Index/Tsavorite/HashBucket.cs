// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static Tsavorite.core.LogAddress;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit, Size = Constants.kEntriesPerBucket * 8)]
    internal unsafe struct HashBucket
    {
        // We use the first overflow bucket for latching, reusing all bits after the address.
        const int kSharedLatchBits = 63 - kAddressBits;
        const int kExclusiveLatchBits = 1;

        // Shift positions of latches in word
        const int kSharedLatchBitOffset = kAddressBits;
        const int kExclusiveLatchBitOffset = kSharedLatchBitOffset + kSharedLatchBits;

        // Shared latch constants
        const long kSharedLatchBitMask = ((1L << kSharedLatchBits) - 1) << kSharedLatchBitOffset;
        const long kSharedLatchIncrement = 1L << kSharedLatchBitOffset;

        // Exclusive latch constants
        const long kExclusiveLatchBitMask = 1L << kExclusiveLatchBitOffset;

        // Comnbined mask
        const long kLatchBitMask = kSharedLatchBitMask | kExclusiveLatchBitMask;

        [FieldOffset(0)]
        public fixed long bucket_entries[Constants.kEntriesPerBucket];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireSharedLatch(ref HashEntryInfo hei) => TryAcquireSharedLatch(hei.firstBucket);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireSharedLatch(HashBucket* bucket)
        {
            ref long entry_word = ref bucket->bucket_entries[Constants.kOverflowBucketIndex];

            var spinCount = Constants.kMaxLockSpins;
            while (true)
            {
                // Note: If reader starvation is encountered, consider rotating priority between reader and writer locks.
                long expected_word = entry_word;
                if ((expected_word & kExclusiveLatchBitMask) == 0) // not exclusively locked
                {
                    if ((expected_word & kSharedLatchBitMask) != kSharedLatchBitMask) // shared lock is not full
                    {
                        if (expected_word == Interlocked.CompareExchange(ref entry_word, expected_word + kSharedLatchIncrement, expected_word))
                            return true;
                    }
                }
                if (--spinCount <= 0)
                    return false;
                _ = Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseSharedLatch(ref HashEntryInfo hei) => ReleaseSharedLatch(hei.firstBucket);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseSharedLatch(HashBucket* bucket)
        {
            ref long entry_word = ref bucket->bucket_entries[Constants.kOverflowBucketIndex];
            // X and S latches means an X latch is still trying to drain readers, like this one.
            Debug.Assert((entry_word & kLatchBitMask) != kExclusiveLatchBitMask, "Trying to S unlatch an X-only latched bucket");
            Debug.Assert((entry_word & kSharedLatchBitMask) != 0, "Trying to S unlatch an unlatched bucket");
            Interlocked.Add(ref entry_word, -kSharedLatchIncrement);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireExclusiveLatch(ref HashEntryInfo hei) => TryAcquireExclusiveLatch(hei.firstBucket);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryAcquireExclusiveLatch(HashBucket* bucket)
        {
            ref long entry_word = ref bucket->bucket_entries[Constants.kOverflowBucketIndex];

            // Acquire exclusive lock (readers may still be present; we'll drain them later)
            var spinCount = Constants.kMaxLockSpins;
            while (true)
            {
                long expected_word = entry_word;
                if ((expected_word & kExclusiveLatchBitMask) == 0)
                {
                    if (expected_word == Interlocked.CompareExchange(ref entry_word, expected_word | kExclusiveLatchBitMask, expected_word))
                        break;
                }
                if (--spinCount <= 0)
                    return false;
                _ = Thread.Yield();
            }

            // Wait for readers to drain. Another session may hold an SLock on this bucket and need an epoch refresh to unlock, so limit this to avoid deadlock.
            for (var ii = 0; ii < Constants.kMaxReaderLockDrainSpins; ii++)
            {
                if ((entry_word & kSharedLatchBitMask) == 0)
                    return true;
                Thread.Yield();
            }

            // Release the exclusive bit and return false so the caller will retry the operation. Since we still have readers, we must CAS.
            while (true)
            {
                long expected_word = entry_word;
                if (Interlocked.CompareExchange(ref entry_word, expected_word & ~kExclusiveLatchBitMask, expected_word) == expected_word)
                    break;
                _ = Thread.Yield();
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryPromoteLatch(HashBucket* bucket)
        {
            ref long entry_word = ref bucket->bucket_entries[Constants.kOverflowBucketIndex];

            // Acquire shared lock
            var spinCount = Constants.kMaxLockSpins;
            while (true)
            {
                long expected_word = entry_word;
                Debug.Assert((expected_word & kSharedLatchBitMask) != 0, "Trying to promote a bucket that is not S latched to X latch");
                if ((expected_word & kExclusiveLatchBitMask) == 0)  // not exclusively locked
                {
                    long new_word = (expected_word | kExclusiveLatchBitMask) - kSharedLatchIncrement;
                    if (expected_word == Interlocked.CompareExchange(ref entry_word, new_word, expected_word))
                        break;
                }
                if (--spinCount <= 0)
                    return false;
                _ = Thread.Yield();
            }

            // Wait for readers to drain. Another session may hold an SLock on this bucket and need an epoch refresh to unlock, so limit this to avoid deadlock.
            for (var ii = 0; ii < Constants.kMaxReaderLockDrainSpins; ii++)
            {
                if ((entry_word & kSharedLatchBitMask) == 0)
                    return true;
                Thread.Yield();
            }

            // Reverse the shared-to-exclusive bit transition and return false so the caller will retry the operation. Since we still have readers, we must CAS.
            while (true)
            {
                long expected_word = entry_word;
                long new_word = (expected_word & ~kExclusiveLatchBitMask) + kSharedLatchIncrement;
                if (expected_word == Interlocked.CompareExchange(ref entry_word, new_word, expected_word))
                    break;
                _ = Thread.Yield();
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseExclusiveLatch(ref HashEntryInfo hei) => ReleaseExclusiveLatch(hei.firstBucket);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ReleaseExclusiveLatch(HashBucket* bucket)
        {
            ref long entry_word = ref bucket->bucket_entries[Constants.kOverflowBucketIndex];

            // We allow shared locking to drain the write lock, so it is OK if we have shared latches.
            Debug.Assert((entry_word & kExclusiveLatchBitMask) != 0, "Trying to X unlatch an unlatched bucket");

            // CAS is necessary to preserve the reader count, and also the address in the overflow bucket may change from unassigned to assigned.
            while (true)
            {
                var expected_word = entry_word;
                if (expected_word == Interlocked.CompareExchange(ref entry_word, expected_word & ~kExclusiveLatchBitMask, expected_word))
                    break;
                _ = Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort NumLatchedShared(HashBucket* bucket)
            => (ushort)((bucket->bucket_entries[Constants.kOverflowBucketIndex] & kSharedLatchBitMask) >> kSharedLatchBitOffset);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsLatchedExclusive(HashBucket* bucket)
            => (bucket->bucket_entries[Constants.kOverflowBucketIndex] & kExclusiveLatchBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsLatched(HashBucket* bucket)
            => (bucket->bucket_entries[Constants.kOverflowBucketIndex] & kLatchBitMask) != 0;

        public static string ToString(HashBucket* bucket)
            => $"locks {$"{(IsLatchedExclusive(bucket) ? "x" : string.Empty)}{NumLatchedShared(bucket)}"}";
    }
}