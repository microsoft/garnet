// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    // RecordInfo layout (64 bits total):
    // [VectorSet][Modified][InNewVersion][Filler][Dirty][ETag][Sealed][Valid][Tombstone][LLLLLLL] [RAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA]
    //     where L = leftover, R = readcache, A = address
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct RecordInfo
    {
        const int kTotalSizeInBytes = 8;
        const int kTotalBits = kTotalSizeInBytes * 8;

        // Previous address
        internal const int kPreviousAddressBits = 48;
        internal const long kPreviousAddressMaskInWord = (1L << kPreviousAddressBits) - 1;

        // Leftover bits (that were reclaimed from locking)
        const int kLeftoverBitCount = 7;

        // Other marker bits. Unused* means bits not yet assigned; use the highest number when assigning
        const int kTombstoneBitOffset = kPreviousAddressBits + kLeftoverBitCount;
        const int kValidBitOffset = kTombstoneBitOffset + 1;
        const int kSealedBitOffset = kValidBitOffset + 1;
        const int kEtagBitOffset = kSealedBitOffset + 1;
        const int kDirtyBitOffset = kEtagBitOffset + 1;
        const int kFillerBitOffset = kDirtyBitOffset + 1;
        const int kInNewVersionBitOffset = kFillerBitOffset + 1;
        const int kModifiedBitOffset = kInNewVersionBitOffset + 1;
        const int kVectorSetBitOffset = kModifiedBitOffset + 1;

        const long kTombstoneBitMask = 1L << kTombstoneBitOffset;
        const long kValidBitMask = 1L << kValidBitOffset;
        const long kSealedBitMask = 1L << kSealedBitOffset;
        const long kETagBitMask = 1L << kEtagBitOffset;
        const long kDirtyBitMask = 1L << kDirtyBitOffset;
        const long kFillerBitMask = 1L << kFillerBitOffset;
        const long kInNewVersionBitMask = 1L << kInNewVersionBitOffset;
        const long kModifiedBitMask = 1L << kModifiedBitOffset;
        const long kVectorSetBitMask = 1L << kVectorSetBitOffset;

        [FieldOffset(0)]
        private long word;

        // Used by routines to initialize a local recordInfo variable to serve as an initial source for srcRecordInfo, before we have 
        // an in-memory address (or even know if the key will be found in-memory).
        internal static RecordInfo InitialValid = new() { Valid = true, PreviousAddress = Constants.kTempInvalidAddress };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInfo(bool inNewVersion, long previousAddress)
        {
            // For Recovery reasons, we need to have the record both Sealed and Invalid: 
            // - Recovery removes the Sealed bit, so we need Invalid to survive from this point on to successful CAS.
            //   Otherwise, Scan could return partial records (e.g. a checkpoint was taken that flushed midway through the record update).
            // - Revivification sets Sealed; we need to preserve it here.
            // We'll clear both on successful CAS.
            InitializeToSealedAndInvalid();
            PreviousAddress = previousAddress;
            if (inNewVersion)
                SetIsInNewVersion();
        }

        // We ignore temp bits from disk images
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearBitsForDiskImages()
        {
            // A Sealed record may become current again during recovery if the RCU-inserted record was not written to disk during a crash. So clear that bit here.
            word &= ~(kDirtyBitMask | kSealedBitMask);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsClosedWord(long word) => (word & (kValidBitMask | kSealedBitMask)) != kValidBitMask;

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

        public readonly bool IsClosed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return IsClosedWord(word); }
        }

        public readonly bool IsSealed
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (word & kSealedBitMask) != 0; }
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
            if (IsClosedWord(expected_word))
                return false;
            var new_word = expected_word | kSealedBitMask;
            if (invalidate)
                new_word &= ~kValidBitMask;
            return expected_word == Interlocked.CompareExchange(ref word, new_word, expected_word);
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

        public readonly bool Tombstone
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (word & kTombstoneBitMask) > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetTombstone() => word |= kTombstoneBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearTombstone() => word &= ~kTombstoneBitMask;

        public bool Valid
        {
            readonly get => (word & kValidBitMask) > 0;

            set
            {
                // This is only called for initialization of static .InitialValid
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

        public readonly bool Dirty
        {
            get => (word & kDirtyBitMask) > 0;
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

        public readonly bool HasFiller
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (word & kFillerBitMask) > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetHasFiller() => word |= kFillerBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearHasFiller() => word &= ~kFillerBitMask;

        public readonly bool IsInNewVersion
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (word & kInNewVersionBitMask) > 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetIsInNewVersion() => word |= kInNewVersionBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetDirtyAndModified() => word |= kDirtyBitMask | kModifiedBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetDirty() => word |= kDirtyBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInvalid() => word &= ~kValidBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeToSealedAndInvalid() => word = kSealedBitMask;    // Does not include kValidBitMask
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsealAndValidate() => word = (word & ~kSealedBitMask) | kValidBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SealAndInvalidate() => word = (word & ~kValidBitMask) | kSealedBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Seal() => word |= kSealedBitMask;

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

        public readonly bool Invalid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (word & kValidBitMask) == 0; }
        }

        public readonly bool SkipOnScan => IsClosedWord(word);

        public long PreviousAddress
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get { return word & kPreviousAddressMaskInWord; }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { word = (word & ~kPreviousAddressMaskInWord) | (value & kPreviousAddressMaskInWord); }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength() => kTotalSizeInBytes;

        public bool VectorSet
        {
            readonly get => (word & kVectorSetBitMask) != 0;
            set => word = value ? word | kVectorSetBitMask : word & ~kVectorSetBitMask;
        }

        public bool ETag
        {
            readonly get => (word & kETagBitMask) != 0;
            set => word = value ? word | kETagBitMask : word & ~kETagBitMask;
        }

        public void SetHasETag() => word |= kETagBitMask;
        public void ClearHasETag() => word &= ~kETagBitMask;

        public override readonly string ToString()
        {
            var paRC = IsReadCache(PreviousAddress) ? "(rc)" : string.Empty;
            static string bstr(bool value) => value ? "T" : "F";
            return $"prev {AbsoluteAddress(PreviousAddress)}{paRC}, valid {bstr(Valid)}, tomb {bstr(Tombstone)}, seal {bstr(IsSealed)},"
                 + $" mod {bstr(Modified)}, dirty {bstr(Dirty)}, fill {bstr(HasFiller)}, etag {bstr(ETag)}, vset {bstr(VectorSet)}";
        }
    }
}