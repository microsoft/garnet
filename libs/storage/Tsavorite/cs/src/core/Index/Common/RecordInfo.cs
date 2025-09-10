// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
    using static LogAddress;

    // RecordInfo layout (64 bits total, high to low):
    //   RecordInfo bits:
    //      [Unused1][Modified][InNewVersion][Unused2][Dirty][IsSectorForceAligned][Sealed][Valid][Tombstone]
    //      [HasExpiration][HasETag][ValueIsObject][ValueIsInline][KeyIsInline]
    //   LogAddress bits (where A = address):
    //      [AddressTypeHigh][AddressTypeLow] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] 
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct RecordInfo
    {
#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter
        const int kTotalSizeInBytes = sizeof(long);
        const int kTotalBits = kTotalSizeInBytes * 8;

        // Other marker bits. Unused* means bits not yet assigned; use the highest number when assigning
        const int kKeyIsInlineBitOffset = kAddressBits;
        const int kValueIsInlineBitOffset = kKeyIsInlineBitOffset + 1;
        const int kValueIsObjectBitOffset = kValueIsInlineBitOffset + 1;
        const int kHasETagBitOffset = kValueIsObjectBitOffset + 1;
        const int kHasExpirationBitOffset = kHasETagBitOffset + 1;
        const int kTombstoneBitOffset = kHasExpirationBitOffset + 1;
        const int kValidBitOffset = kTombstoneBitOffset + 1;
        const int kSealedBitOffset = kValidBitOffset + 1;
        const int kIsSectorForceAlignedBitOffset = kSealedBitOffset + 1;
        const int kDirtyBitOffset = kIsSectorForceAlignedBitOffset + 1;
        const int kUnused2BitOffset = kDirtyBitOffset + 1;
        const int kInNewVersionBitOffset = kUnused2BitOffset + 1;
        const int kModifiedBitOffset = kInNewVersionBitOffset + 1;
        const int kUnused1BitOffset = kModifiedBitOffset + 1;

        const long kKeyIsInlineBitMask = 1L << kKeyIsInlineBitOffset;
        const long kValueIsInlineBitMask = 1L << kValueIsInlineBitOffset;
        const long kValueIsObjectBitMask = 1L << kValueIsObjectBitOffset;
        const long kHasETagBitMask = 1L << kHasETagBitOffset;
        const long kHasExpirationBitMask = 1L << kHasExpirationBitOffset;
        const long kTombstoneBitMask = 1L << kTombstoneBitOffset;
        const long kValidBitMask = 1L << kValidBitOffset;
        const long kSealedBitMask = 1L << kSealedBitOffset;
        const long kIsSectorForceAlignedBitMask = 1L << kIsSectorForceAlignedBitOffset;
        const long kDirtyBitMask = 1L << kDirtyBitOffset;
        const long kUnused2BitMask = 1L << kUnused2BitOffset;
        const long kInNewVersionBitMask = 1L << kInNewVersionBitOffset;
        const long kModifiedBitMask = 1L << kModifiedBitOffset;
        const long kUnused1BitMask = 1L << kUnused1BitOffset;
#pragma warning restore IDE1006 // Naming Styles

        [FieldOffset(0)]
        private long word;

        // Used by routines to initialize a local recordInfo variable to serve as an initial source for srcRecordInfo, before we have 
        // an in-memory address (or even know if the key will be found in-memory).
        internal static RecordInfo InitialValid = new() { Valid = true, PreviousAddress = kTempInvalidAddress };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInfo(bool inNewVersion, long previousAddress)
        {
            // For Recovery reasons, we need to have the record both Sealed and Invalid: 
            // - Recovery removes the Sealed bit, so we need Invalid to survive from this point on to successful CAS.
            //   Otherwise, Scan could return partial records (e.g. a checkpoint was taken that flushed midway through the record update).
            // - Revivification sets Sealed; we need to preserve it here.
            // We'll clear both on successful CAS.
            InitializeNewRecord();
            PreviousAddress = previousAddress;
            if (inNewVersion)
                SetIsInNewVersion();
        }

        // We ignore temp bits from disk images
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearBitsForDiskImages()
        {
            // A Sealed record may become current again during recovery if the RCU-inserted record was not written to disk during a crash. So clear that bit here.
            // Preserve Key/ValueIsInline as they are always inline for DiskLogRecord. Preserve ValueIsObject to indicate whether a value object should be deserialized
            // or if the value should remain inline (and possibly overflow if copied to a LogRecord).
            word &= ~(kDirtyBitMask | kSealedBitMask);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal RecordInfo FromWord(long word) => new RecordInfo() { word = word };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsClosedWord(long word) => (word & (kValidBitMask | kSealedBitMask)) != kValidBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly bool IsClosedOrTombstoned(ref OperationStatus internalStatus)
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
            var expected_word = word;
            if (IsClosedWord(expected_word))
                return false;
            var new_word = expected_word | kSealedBitMask;
            if (invalidate)
                new_word &= ~kValidBitMask;
            return expected_word == Interlocked.CompareExchange(ref word, new_word, expected_word);
        }

        /// <summary>
        /// Update this record's PreviousAddress to point on-disk
        /// </summary>
        /// <param name="diskTailOffset">The cumulative offset from record expansion to be added to PreviousAddress</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UpdateToOnDiskAddress(long diskTailOffset)
        {
            while (true)
            {
                var expected_info = this;
                if (IsOnDisk(expected_info.PreviousAddress))
                    return;
                var new_info = expected_info;
                new_info.PreviousAddress = SetIsOnDisk(new_info.PreviousAddress + diskTailOffset);
                var result_info = FromWord(Interlocked.CompareExchange(ref word, new_info.word, expected_info.word));
                if (expected_info.word == result_info.word)
                    return;

                // If this fails due to .PreviousAddress changes it should be due to the OnPagesClosed thread having done the update.
                // In that case, verify the values match. This record may be in the mutable region so may have other changes to the RecordInfo.
                Debug.Assert(result_info.PreviousAddress == expected_info.PreviousAddress || result_info.PreviousAddress == new_info.PreviousAddress,
                            "Unexpected PreviousAddress change encountered when updating PreviousAddress to on-disk");
            }
        }

        /// <summary>
        /// Try to reset the modified bit of the RecordInfo
        /// </summary>
        /// <returns>Whether the modified bit was reset successfully</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryResetModifiedAtomic()
        {
            var spinCount = Constants.kMaxLockSpins;
            while (true)
            {
                var expected_word = word;
                if (IsClosedWord(expected_word))
                    return false;
                if ((expected_word & kModifiedBitMask) == 0)
                    return true;
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & (~kModifiedBitMask), expected_word))
                    return true;
                if (--spinCount <= 0)
                    return false;
                _ = Thread.Yield();
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

        public readonly bool IsNull => word == 0;

        public readonly bool Equals(RecordInfo other) => this.word == other.word;

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
            while (true)
            {
                var expected_word = word;  // TODO: Interlocked.And is not supported in netstandard2.1
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & ~kDirtyBitMask, expected_word))
                    break;
                _ = Thread.Yield();
            }
        }

        public readonly bool Dirty => (word & kDirtyBitMask) > 0;

        public bool Modified
        {
            readonly get => (word & kModifiedBitMask) > 0;
            set
            {
                if (value) word |= kModifiedBitMask;
                else word &= ~kModifiedBitMask;
            }
        }

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
        public void InitializeNewRecord()
        {
            // Initialize to Sealed and Invalid (do not include kValidBitMask) and to Inline Key and Value so no Oversize or ObjectId is expected.
            word = kSealedBitMask | kKeyIsInlineBitMask | kValueIsInlineBitMask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsealAndValidate() => word = (word & ~kSealedBitMask) | kValidBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SealAndInvalidate() => word = (word & ~kValidBitMask) | kSealedBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Seal() => word |= kSealedBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInvalidAtomic()
        {
            while (true)
            {
                var expected_word = word;  // TODO: Interlocked.And is not supported in netstandard2.1
                if (expected_word == Interlocked.CompareExchange(ref word, expected_word & ~kValidBitMask, expected_word))
                    return;
                _ = Thread.Yield();
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
            readonly get { return word & kAddressBitMask; }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { word = (word & ~kAddressBitMask) | (value & kAddressBitMask); }
        }

        public readonly bool HasETag => (word & kHasETagBitMask) != 0;
        public void SetHasETag() => word |= kHasETagBitMask;
        public void ClearHasETag() => word &= ~kHasETagBitMask;

        public readonly bool HasExpiration => (word & kHasExpirationBitMask) != 0;
        public void SetHasExpiration() => word |= kHasExpirationBitMask;
        public void ClearHasExpiration() => word &= ~kHasExpirationBitMask;

        public readonly bool HasOptionalFields => (word & (kHasETagBitMask | kHasExpirationBitMask)) != 0;

        // Note: KeyIsOveflow bit is not needed as it is the negation of KeyIsInline
        public readonly bool KeyIsInline => (word & kKeyIsInlineBitMask) != 0;
        public void SetKeyIsInline() => word |= kKeyIsInlineBitMask;
        public void ClearKeyIsInline() => word &= ~kKeyIsInlineBitMask;
        public readonly bool KeyIsOverflow => !KeyIsInline;
        public void SetKeyIsOverflow() => word &= ~kKeyIsInlineBitMask;

        // Note: a ValueIsOverflow bit is not needed as it is the negation of (ValueIsInline | ValueIsObject)
        public readonly bool ValueIsInline => (word & kValueIsInlineBitMask) != 0;
        public void SetValueIsInline() => word = (word & ~kValueIsObjectBitMask) | kValueIsInlineBitMask;
        public void ClearValueIsInline() => word &= ~kValueIsInlineBitMask;

        public readonly bool ValueIsObject => (word & kValueIsObjectBitMask) != 0;
        public void SetValueIsObject() => word = (word & ~kValueIsInlineBitMask) | kValueIsObjectBitMask;

        // Value "Overflow" is determined by lack of Inline and lack of Object
        public readonly bool ValueIsOverflow => !ValueIsInline && !ValueIsObject;
        public void SetValueIsOverflow() => word &= ~(kValueIsInlineBitMask | kValueIsObjectBitMask);

        public void SetKeyAndValueInline() => word = (word & ~kValueIsObjectBitMask) | kKeyIsInlineBitMask | kValueIsInlineBitMask;

        /// <summary>If true, the record has been force-aligned to a sector boundary.</summary>
        internal bool IsSectorForceAligned
        {
            readonly get => (word & kIsSectorForceAlignedBitMask) != 0;
            set
            {
                while (true)
                {
                    var expected_info = this;
                    if (expected_info.IsSectorForceAligned)
                        return;
                    var new_word = value ? (expected_info.word | kIsSectorForceAlignedBitMask) : (expected_info.word & ~kIsSectorForceAlignedBitMask);
                    if (Interlocked.CompareExchange(ref word, new_word, expected_info.word) == expected_info.word)
                        return;
                }
            }
        }

        public readonly bool RecordIsInline => (word & (kKeyIsInlineBitMask | kValueIsInlineBitMask)) == (kKeyIsInlineBitMask | kValueIsInlineBitMask);

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
            static string bstr(bool value) => value ? "T" : "F";
            return $"prev {AddressString(PreviousAddress)}{paRC}, valid {bstr(Valid)}, tomb {bstr(Tombstone)}, seal {bstr(IsSealed)}, IsSecAlgn {bstr(IsSectorForceAligned)},"
                 + $" mod {bstr(Modified)}, dirty {bstr(Dirty)}, KisInl {KeyIsInline}, VisInl {ValueIsInline}, VisObj {bstr(ValueIsObject)},"
                 + $" ETag {bstr(HasETag)}, Expir {bstr(HasExpiration)}, Un1 {bstr(Unused1)}, Un2 {bstr(Unused2)}";
        }
    }
}