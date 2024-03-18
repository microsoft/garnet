// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    // RecordInfo layout (64 bits total):
    // [Unused1][Modified][InNewVersion][Filler][Dirty][Unused2][Sealed][Valid][Tombstone][X][SSSSSS] [RAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA]
    //     where X = exclusive lock, S = shared lock, R = readcache, A = address
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct RecordInfo
    {
        const int kTotalSizeInBytes = 8;
        const int kTotalBits = kTotalSizeInBytes * 8;

        // Previous address
        internal const int kPreviousAddressBits = 48;
        internal const long kPreviousAddressMaskInWord = (1L << kPreviousAddressBits) - 1;

        // Shift position of lock in word
        const int kLockBitsOffset = kPreviousAddressBits;

        // We use 7 lock bits: 6 shared lock bits + 1 exclusive lock bit
        internal const int kSharedLockBits = 6;
        internal const int kExclusiveLockBits = 1;

        // Shared lock constants
        const long kSharedLockBitMask = ((1L << kSharedLockBits) - 1) << kLockBitsOffset;
        const long kSharedLockIncrement = 1L << kLockBitsOffset;

        // Exclusive lock constants
        const int kExclusiveLockBitOffset = kLockBitsOffset + kSharedLockBits;
        const long kExclusiveLockBitMask = 1L << kExclusiveLockBitOffset;
        const long kLockBitMask = kSharedLockBitMask | kExclusiveLockBitMask;

        // Other marker bits. Unused* means bits not yet assigned; use the highest number when assigning
        const int kTombstoneBitOffset = kExclusiveLockBitOffset + 1;
        const int kValidBitOffset = kTombstoneBitOffset + 1;
        const int kSealedBitOffset = kValidBitOffset + 1;
        const int kUnused2BitOffset = kSealedBitOffset + 1;
        const int kDirtyBitOffset = kUnused2BitOffset + 1;
        const int kFillerBitOffset = kDirtyBitOffset + 1;
        const int kInNewVersionBitOffset = kFillerBitOffset + 1;
        const int kModifiedBitOffset = kInNewVersionBitOffset + 1;
        internal const int kUnused1BitOffset = kModifiedBitOffset + 1;

        const long kTombstoneBitMask = 1L << kTombstoneBitOffset;
        const long kValidBitMask = 1L << kValidBitOffset;
        const long kSealedBitMask = 1L << kSealedBitOffset;
        const long kUnused2BitMask = 1L << kUnused2BitOffset;
        const long kDirtyBitMask = 1L << kDirtyBitOffset;
        const long kFillerBitMask = 1L << kFillerBitOffset;
        const long kInNewVersionBitMask = 1L << kInNewVersionBitOffset;
        const long kModifiedBitMask = 1L << kModifiedBitOffset;
        const long kUnused1BitMask = 1L << kUnused1BitOffset;

        [FieldOffset(0)]
        private long word;

        // Used by routines to initialize a local recordInfo variable to serve as an initial source for srcRecordInfo, before we have 
        // an in-memory address (or even know if the key will be found in-memory).
        internal static RecordInfo InitialValid = new() { Valid = true, PreviousAddress = Constants.kTempInvalidAddress };

        public void WriteInfo(bool inNewVersion, bool tombstone, long previousAddress)
        {
            // For Recovery reasons, we need to have the record both Sealed and Invalid: 
            // - Recovery removes the Sealed bit, so we need Invalid to survive from this point on to successful CAS.
            //   Otherwise, Scan could return partial records (e.g. a checkpoint was taken that flushed midway through the record update).
            // - Revivification sets Sealed; we need to preserve it here.
            // We'll clear both on successful CAS.
            InitializeToSealedAndInvalid();
            Tombstone = tombstone;
            PreviousAddress = previousAddress;
            IsInNewVersion = inNewVersion;
        }

        public readonly bool IsLockedExclusive => (word & kExclusiveLockBitMask) != 0;
        public readonly bool IsLockedShared => (word & kSharedLockBitMask) != 0;
        public readonly bool IsLocked => (word & kLockBitMask) != 0;

        public readonly byte NumLockedShared => (byte)((word & kSharedLockBitMask) >> kLockBitsOffset);

        // We ignore locks and temp bits for disk images
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearBitsForDiskImages()
        {
            // Locks can be evicted even with RecordIsolation, if the record is locked when BlockAllocate allows an epoch refresh
            // that sends it below HeadAddress. Sealed records are normal. In Pending IO completions, RecordIsolation does not
            // lock records read from disk (they should be found in memory). But a Sealed record may become current again during
            // recovery, if the RCU-inserted record was not written to disk during a crash, etc. So clear these bits here.
            word &= ~(kLockBitMask | kDirtyBitMask | kSealedBitMask);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsClosedWord(long word) => (word & (kValidBitMask | kSealedBitMask)) != kValidBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsClosedOrLockedWord(long word) => (word & (kValidBitMask | kSealedBitMask | kLockBitMask)) != kValidBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsClosedOrTombstoned(ref OperationStatus internalStatus)
        {
            if ((word & (kValidBitMask | kSealedBitMask | kTombstoneBitMask)) != kValidBitMask)
            {
                internalStatus = IsClosedWord(word) ? OperationStatus.RETRY_LATER : OperationStatus.NOTFOUND;
                return true;
            }
            return false;
        }

        public readonly bool IsClosed => IsClosedWord(word);
        public readonly bool IsSealed => (word & kSealedBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InitializeLockShared() => word += kSharedLockIncrement;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InitializeLockExclusive() => word |= kExclusiveLockBitMask;

        /// <summary>
        /// Unlock RecordInfo that was previously locked for exclusive access, via <see cref="TryLockExclusive"/>
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockExclusive()
        {
            Debug.Assert(!IsLockedShared, "Trying to X unlock an S locked record");
            Debug.Assert(IsLockedExclusive, "Trying to X unlock an unlocked record");

            // Because we seal the source of an RCU and that source is likely locked, we cannot assert !IsSealed.
            // Debug.Assert(!IsSealed, "Trying to X unlock a Sealed record");
            word &= ~kExclusiveLockBitMask; // Safe because there should be no other threads (e.g., readers) updating the word at this point
        }

        /// <summary>
        /// Unlock a RecordInfo that was previously locked for exclusive access, via <see cref="TryLockExclusive"/>, and which is a source that has been
        /// elided from a tag chain, so must become both Sealed (no longer correct for that record) and Invalid (no longer in a tag chain).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockExclusiveAndSealInvalidate()
        {
            // This is safe to call with or without RecordIsolation, to save the cost of an "if", so don't assert the X lock.
            Debug.Assert(!IsLockedShared, "Trying to UnlockExclusiveAndSealInvalidate an S locked record");
            word = (word & ~(kExclusiveLockBitMask | kValidBitMask)) | kSealedBitMask; // Safe because there should be no other threads (e.g., readers) updating the word at this point
        }

        /// <summary>
        /// Unlock RecordInfo that was previously locked for exclusive access, via <see cref="TryLockExclusive"/>, and which is a source that has become Sealed
        /// but will remain in the tag chain so must remain Valid.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockExclusiveAndSeal()
        {
            // This is safe to call with or without RecordIsolation, to save the cost of an "if", so don't assert the X lock.
            Debug.Assert(!IsLockedShared, "Trying to UnlockExclusiveAndSeal an S locked record");
            word = (word & ~kExclusiveLockBitMask) | kSealedBitMask; // Safe because there should be no other threads (e.g., readers) updating the word at this point
        }

        /// <summary>
        /// Seal this record (currently only called to prepare it for inline revivification).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySeal(bool invalidate)
        {
            // If this fails for any reason it means another record is trying to modify (perhaps revivify) it, so return false to RETRY_LATER.
            // If invalidate, we in a situation such as revivification freelisting where we want to make sure that removing Seal will not leave
            // it eligible to be Scanned after Recovery.
            long expected_word = word;
            if (IsClosedOrLockedWord(expected_word))
                return false;
            var new_word = expected_word | kSealedBitMask;
            if (invalidate)
                new_word &= ~kValidBitMask;
            return expected_word == Interlocked.CompareExchange(ref word, new_word, expected_word);
        }

        /// <summary>
        /// Unseal this record that was previously sealed for revivification.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Unseal(bool makeValid)
        {
            // Do not assert IsSealed; we only unseal in a short scope in revivification when not using LockTable, and this saves us an "if" when calling this.
            // Do not assert Valid; we both Seal and Invalidate due to checkpoint/recovery reasons (Recovery clears Seal).
            word = (word & ~kSealedBitMask) | (makeValid ? kValidBitMask : 0);
        }

        /// <summary>
        /// Try to take an exclusive (write) lock on RecordInfo
        /// </summary>
        /// <returns>Whether lock was acquired successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockExclusive()
        {
            // Acquire exclusive lock (readers may still be present; we'll drain them later)
            for (int spinCount = Constants.kMaxLockSpins; ; Thread.Yield())
            {
                long expected_word = word;
                if (IsClosedWord(expected_word))
                    return false;
                if ((expected_word & kExclusiveLockBitMask) == 0)
                {
                    if (expected_word == Interlocked.CompareExchange(ref word, expected_word | kExclusiveLockBitMask, expected_word))
                        break;
                }
                if (--spinCount <= 0)
                    return false;
            }

            // Wait for readers to drain. Another session may hold an SLock on this record and need an epoch refresh to unlock, so limit this to avoid deadlock.
            for (var ii = 0; ii < Constants.kMaxReaderLockDrainSpins; ++ii)
            {
                if ((word & kSharedLockBitMask) == 0)
                {
                    // Someone else may have closed the record while we were draining reads.
                    if (IsClosedWord(word))
                        break;
                    return true;
                }
                Thread.Yield();
            }

            // Release the exclusive bit and return false so the caller will retry the operation.
            // To reset this bit while spinning to drain readers, we must use CAS to avoid losing a reader unlock.
            for (; ; Thread.Yield())
            {
                long expected_word = word;
                if (Interlocked.CompareExchange(ref word, expected_word & ~kExclusiveLockBitMask, expected_word) == expected_word)
                    break;
            }
            return false;
        }

        /// <summary>Unlock RecordInfo that was previously locked for shared access, via <see cref="TryLockShared"/></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockShared()
        {
            // X *and* S locks means an X lock is still trying to drain readers, like this one.
            Debug.Assert((word & kLockBitMask) != kExclusiveLockBitMask, "Trying to S unlock an X-only locked record");
            Debug.Assert(IsLockedShared, "Trying to S unlock an unlocked record");
            Debug.Assert(!IsSealed, "Trying to S unlock a Sealed record");
            Interlocked.Add(ref word, -kSharedLockIncrement);
        }

        /// <summary>
        /// Take shared (read) lock on RecordInfo
        /// </summary>
        /// <returns>Whether lock was acquired successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockShared()
        {
            // Acquire shared lock
            for (int spinCount = Constants.kMaxLockSpins; ; Thread.Yield())
            {
                long expected_word = word;
                if (IsClosedWord(expected_word))
                    return false;
                if (((expected_word & kExclusiveLockBitMask) == 0) // not exclusively locked
                    && (expected_word & kSharedLockBitMask) != kSharedLockBitMask) // shared lock is not full
                {
                    if (expected_word == Interlocked.CompareExchange(ref word, expected_word + kSharedLockIncrement, expected_word))
                        return true;
                }
                if (--spinCount <= 0)
                    return false;
            }
        }

        /// <summary>
        /// Try to reset the modified bit of the RecordInfo
        /// </summary>
        /// <returns>Whether the modified bit was reset successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryResetModifiedAtomic()
        {
            for (int spinCount = Constants.kMaxLockSpins; ; Thread.Yield())
            {
                long expected_word = word;
                if (IsClosedWord(expected_word))
                    return false;
                if ((expected_word & kModifiedBitMask) == 0)
                    return true;
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & (~kModifiedBitMask), expected_word))
                    return true;
                if (--spinCount <= 0)
                    return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryUpdateAddress(long expectedPrevAddress, long newPrevAddress)
        {
            var expected_word = word;
            RecordInfo newRI = new() { word = expected_word };
            if (newRI.PreviousAddress != expectedPrevAddress)
                return false;
            newRI.PreviousAddress = newPrevAddress;
            return expected_word == Interlocked.CompareExchange(ref word, newRI.word, expected_word);
        }

        public readonly bool IsNull() => word == 0;

        public bool Tombstone
        {
            readonly get => (word & kTombstoneBitMask) > 0;
            set
            {
                if (value) word |= kTombstoneBitMask;
                else word &= ~kTombstoneBitMask;
            }
        }

        public bool Valid
        {
            readonly get => (word & kValidBitMask) > 0;
            set
            {
                if (value) word |= kValidBitMask;
                else word &= ~kValidBitMask;
            }
        }

        public void ClearDirtyAtomic()
        {
            for (; ; Thread.Yield())
            {
                long expected_word = word;  // TODO: Interlocked.And is not supported in netstandard2.1
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & ~kDirtyBitMask, expected_word))
                    break;
            }
        }

        public bool Dirty
        {
            readonly get => (word & kDirtyBitMask) > 0;
            set
            {
                if (value) word |= kDirtyBitMask;
                else word &= ~kDirtyBitMask;
            }
        }

        public bool Modified
        {
            readonly get => (word & kModifiedBitMask) > 0;
            set
            {
                if (value) word |= kModifiedBitMask;
                else word &= ~kModifiedBitMask;
            }
        }

        public bool Filler
        {
            readonly get => (word & kFillerBitMask) > 0;
            set
            {
                if (value) word |= kFillerBitMask;
                else word &= ~kFillerBitMask;
            }
        }

        public bool IsInNewVersion
        {
            readonly get => (word & kInNewVersionBitMask) > 0;
            set
            {
                if (value) word |= kInNewVersionBitMask;
                else word &= ~kInNewVersionBitMask;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetDirtyAndModified() => word |= kDirtyBitMask | kModifiedBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetDirty() => word |= kDirtyBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetTombstone() => word |= kTombstoneBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInvalid() => word &= ~(kValidBitMask | kExclusiveLockBitMask);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeToSealedAndInvalid() => word = kSealedBitMask;    // Does not include kValidBitMask
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsealAndValidate() => word = (word & ~kSealedBitMask) | kValidBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SealAndInvalidate() => word = (word & ~kValidBitMask) | kSealedBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInvalidAtomic()
        {
            for (; ; Thread.Yield())
            {
                long expected_word = word;  // TODO: Interlocked.And is not supported in netstandard2.1
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & ~kValidBitMask, expected_word))
                    return;
            }
        }

        public readonly bool Invalid => (word & kValidBitMask) == 0;

        public readonly bool SkipOnScan => IsClosedWord(word);

        public long PreviousAddress
        {
            readonly get => word & kPreviousAddressMaskInWord;
            set
            {
                word &= ~kPreviousAddressMaskInWord;
                word |= value & kPreviousAddressMaskInWord;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength() => kTotalSizeInBytes;

        internal bool Unused1
        {
            readonly get => (word & kUnused1BitMask) != 0;
            set => word = value ? word | kUnused1BitMask : word & ~kUnused1BitMask;
        }

        internal bool Unused2
        {
            readonly get => (word & kUnused2BitMask) != 0;
            set => word = value ? word | kUnused2BitMask : word & ~kUnused2BitMask;
        }

        public override readonly string ToString()
        {
            var paRC = IsReadCache(PreviousAddress) ? "(rc)" : string.Empty;
            var locks = $"{(IsLockedExclusive ? "x" : string.Empty)}{NumLockedShared}";
            static string bstr(bool value) => value ? "T" : "F";
            return $"prev {AbsoluteAddress(PreviousAddress)}{paRC}, locks {locks}, valid {bstr(Valid)}, tomb {bstr(Tombstone)}, seal {bstr(IsSealed)},"
                 + $" mod {bstr(Modified)}, dirty {bstr(Dirty)}, fill {bstr(Filler)}, Un1 {bstr(Unused1)}, Un2 {bstr(Unused2)}";
        }
    }
}