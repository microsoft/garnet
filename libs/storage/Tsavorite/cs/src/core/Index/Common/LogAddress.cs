// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>Static utility class for manipulating logical addresses.</summary>
    public static class LogAddress
    {
        /// <summary>Address is 48 bits, with the top bit being the readcache indicator.</summary>
        public const int kAddressBits = 48;
        /// <summary>Mask off the address from a long; e.g. <see cref="RecordInfo.PreviousAddress"/> is 48 bits, with the top bit being the readcache indicator.</summary>
        public const long kAddressBitMask = (1L << kAddressBits) - 1;

        // Get the absolute address by masking out the address type bits.
        internal const long kAbsoluteAddressBitMask = ((1L << kAddressBits) - 1) & ~RecordInfo.kIsReadCacheBitMask;

        /// <summary>Invalid record logical address; used for initialization. Zero means an IsNull RecordInfo is Invalid.</summary>
        public const long kInvalidAddress = 0L;
        /// <summary>Invalid record logical address used for some specific initializations.</summary>
        public const long kTempInvalidAddress = 1L;

        /// <summary>First valid address in the log; ensures space for page header and that 0 and 1 are never valid addresses.</summary>
        public const long FirstValidAddress = PageHeader.Size;

        /// <summary>The max valid address is the in-memory mask (which is greater than the on-disk mask) and the full absolute address range.</summary>
        public const long MaxValidAddress = kAbsoluteAddressBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsReadCache(long address) => ((ulong)address & RecordInfo.kIsReadCacheBitMask) == RecordInfo.kIsReadCacheBitMask;

        /// <summary>Get the absolute address (no readcache bit)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long AbsoluteAddress(long address) => address & kAbsoluteAddressBitMask;

        /// <summary>Utility shared between AllocatorBase and ScanIteratorBase</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetPageOfAddress(long logicalAddress, int logPageSizeBits) => AbsoluteAddress(logicalAddress) >> logPageSizeBits;

        /// <summary>Utility shared between AllocatorBase and ScanIteratorBase</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetLogicalAddressOfStartOfPage(long page, int logPageSizeBits) => page << logPageSizeBits;

        /// <summary>Pretty-print the address</summary>
        public static string AddressString(long address)
        {
            var absoluteAddress = AbsoluteAddress(address);
            if (IsReadCache(address))
                return $"rc:{absoluteAddress}";
            if (address == kInvalidAddress)
                return "kInvalid";
            if (address == kTempInvalidAddress)
                return "kTempInvalid";
            return $"log:{address}";
        }
    }
}