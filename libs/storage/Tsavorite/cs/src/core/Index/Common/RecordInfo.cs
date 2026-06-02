// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    // RecordInfo layout (64 bits total, high to low):
    //   Unused/Reserved bits:
    //      [Unused1]..[Unused11]
    //   RecordInfo bits:
    //      [Sealed][Modified][InNewVersion][Valid][Tombstone]
    //   LogAddress bits (where A = address):
    //      [R][AAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA] [AAAAAAAA]
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct RecordInfo
    {
        public const int Size = sizeof(ulong);

#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter
        const int kTotalBits = Size * 8;

        // Other marker bits. Unused* means bits not yet assigned. Numbered Unused0 (highest bit position) down to Unused10
        // (just above the high-end RecordInfo bits) so the ToString output prints them in the natural high-to-low bit order.
        const int kIsReadCacheBitOffset = kAddressBits - 1;
        const int kTombstoneBitOffset = kIsReadCacheBitOffset + 1;
        const int kValidBitOffset = kTombstoneBitOffset + 1;
        const int kInNewVersionBitOffset = kValidBitOffset + 1;
        const int kModifiedBitOffset = kInNewVersionBitOffset + 1;
        const int kSealedBitOffset = kModifiedBitOffset + 1;
        const int kUnused10BitOffset = kSealedBitOffset + 1;
        const int kUnused9BitOffset = kUnused10BitOffset + 1;
        const int kUnused8BitOffset = kUnused9BitOffset + 1;
        const int kUnused7BitOffset = kUnused8BitOffset + 1;
        const int kUnused6BitOffset = kUnused7BitOffset + 1;
        const int kUnused5BitOffset = kUnused6BitOffset + 1;
        const int kUnused4BitOffset = kUnused5BitOffset + 1;
        const int kUnused3BitOffset = kUnused4BitOffset + 1;
        const int kUnused2BitOffset = kUnused3BitOffset + 1;
        const int kUnused1BitOffset = kUnused2BitOffset + 1;
        const int kUnused0BitOffset = kUnused1BitOffset + 1;

        internal const long kIsReadCacheBitMask = 1L << kIsReadCacheBitOffset;
        const long kTombstoneBitMask = 1L << kTombstoneBitOffset;
        const long kValidBitMask = 1L << kValidBitOffset;
        const long kInNewVersionBitMask = 1L << kInNewVersionBitOffset;
        const long kModifiedBitMask = 1L << kModifiedBitOffset;
        const long kSealedBitMask = 1L << kSealedBitOffset;
        const long kUnused10BitMask = 1L << kUnused10BitOffset;
        const long kUnused9BitMask = 1L << kUnused9BitOffset;
        const long kUnused8BitMask = 1L << kUnused8BitOffset;
        const long kUnused7BitMask = 1L << kUnused7BitOffset;
        const long kUnused6BitMask = 1L << kUnused6BitOffset;
        const long kUnused5BitMask = 1L << kUnused5BitOffset;
        const long kUnused4BitMask = 1L << kUnused4BitOffset;
        const long kUnused3BitMask = 1L << kUnused3BitOffset;
        const long kUnused2BitMask = 1L << kUnused2BitOffset;
        const long kUnused1BitMask = 1L << kUnused1BitOffset;
        const long kUnused0BitMask = 1L << kUnused0BitOffset;
#pragma warning restore IDE1006 // Naming Styles

        [FieldOffset(0)]
        private long word;

        // Used by routines to initialize a local recordInfo variable to serve as an initial source for srcRecordInfo, before we have 
        // an in-memory address (or even know if the key will be found in-memory).
        internal static RecordInfo InitialValid = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordInfo()
        {
            Valid = true;
            PreviousAddress = kTempInvalidAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public RecordInfo(long word)
        {
            this.word = word;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteInfo(bool inNewVersion, long previousAddress)
        {
            // For Recovery reasons, we need to have the record both Sealed and Invalid: 
            // - Recovery removes the Sealed bit, so we need Invalid to survive from this point on to successful CAS.
            //   Otherwise, Scan could return partial records (e.g. a checkpoint was taken that flushed midway through the record update).
            // - Revivification sets Sealed; we need to preserve it here.
            // We'll clear both on successful CAS.
            InitializeForNewRecord();
            PreviousAddress = previousAddress;
            if (inNewVersion)
                SetIsInNewVersion();
        }

        // We ignore temp bits from disk images
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearBitsForDiskImages()
        {
            // A Sealed record may become current again during recovery if the RCU-inserted record was not written to disk during a crash. So clear that bit here.
            word &= ~kSealedBitMask;
        }

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
            RecordInfo newRI = new(expected_word);
            if (newRI.PreviousAddress != expectedPrevAddress)
                return false;
            newRI.PreviousAddress = newPrevAddress;
            return expected_word == Interlocked.CompareExchange(ref word, newRI.word, expected_word);
        }

        public readonly bool IsNull => word == 0;

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
        public void SetModified() => word |= kModifiedBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetInvalid() => word &= ~kValidBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeForNewRecord()
        {
            // Initialize to Sealed and Invalid (do not include kValidBitMask).
            word = kSealedBitMask;
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
                var expected_word = word;
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

        internal bool IsReadCache
        {
            readonly get => (word & kIsReadCacheBitMask) != 0;
            set => word = value ? word | kIsReadCacheBitMask : word & ~kIsReadCacheBitMask;
        }

        internal bool Unused0
        {
            readonly get => (word & kUnused0BitMask) != 0;
            set => word = value ? word | kUnused0BitMask : word & ~kUnused0BitMask;
        }

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

        internal bool Unused3
        {
            readonly get => (word & kUnused3BitMask) != 0;
            set => word = value ? word | kUnused3BitMask : word & ~kUnused3BitMask;
        }

        internal bool Unused4
        {
            readonly get => (word & kUnused4BitMask) != 0;
            set => word = value ? word | kUnused4BitMask : word & ~kUnused4BitMask;
        }

        internal bool Unused5
        {
            readonly get => (word & kUnused5BitMask) != 0;
            set => word = value ? word | kUnused5BitMask : word & ~kUnused5BitMask;
        }

        internal bool Unused6
        {
            readonly get => (word & kUnused6BitMask) != 0;
            set => word = value ? word | kUnused6BitMask : word & ~kUnused6BitMask;
        }

        internal bool Unused7
        {
            readonly get => (word & kUnused7BitMask) != 0;
            set => word = value ? word | kUnused7BitMask : word & ~kUnused7BitMask;
        }

        internal bool Unused8
        {
            readonly get => (word & kUnused8BitMask) != 0;
            set => word = value ? word | kUnused8BitMask : word & ~kUnused8BitMask;
        }

        internal bool Unused9
        {
            readonly get => (word & kUnused9BitMask) != 0;
            set => word = value ? word | kUnused9BitMask : word & ~kUnused9BitMask;
        }

        internal bool Unused10
        {
            readonly get => (word & kUnused10BitMask) != 0;
            set => word = value ? word | kUnused10BitMask : word & ~kUnused10BitMask;
        }

        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            static string bstr01(bool value) => value ? "1" : "0";
            // Unused0..Unused10 printed in declared order (Unused0 leftmost = highest bit position), grouped in 4s separated by '|'.
            var unusedStr = $"{bstr01(Unused0)}{bstr01(Unused1)}{bstr01(Unused2)}{bstr01(Unused3)}"
                          + $"|{bstr01(Unused4)}{bstr01(Unused5)}{bstr01(Unused6)}{bstr01(Unused7)}"
                          + $"|{bstr01(Unused8)}{bstr01(Unused9)}{bstr01(Unused10)}";
            return $"prev {AddressString(PreviousAddress)}, valid {bstr(Valid)}, tomb {bstr(Tombstone)}, seal {bstr(IsSealed)}, rc {bstr(IsReadCache)},"
                 + $" mod {bstr(Modified)}, inv {bstr(IsInNewVersion)},"
                 + $" Unused0-10 {unusedStr}";
        }
    }
}