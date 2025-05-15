// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>Static utility class for manipulating logical addresses.</summary>
    public static class LogAddress
    {
        // Address is 50 bits, with the top 2 being the address type.
        internal const int kAddressBits = 50;
        internal const long kAddressBitMask = (1L << kAddressBits) - 1;

        internal const int kAddressTypeBits = 2; // Address type bits (2 bits for 4 types, 1 currently reserved)
        internal const int kAddressTypeBitOffset = kAddressBits - kAddressTypeBits;
        internal const long kAddressTypeBitMask = ((1L << kAddressTypeBits) - 1) << kAddressTypeBitOffset;

        // Get the absolute address by masking out the address type bits.
        internal const long kAbsoluteAddressBitMask = (1L << (kAddressBits - kAddressTypeBits)) - 1;

        // AddressType is ordered in descending order with memory locations first, to match the sequence of
        // comparisons in Internal(RUMD) and ensure OnDisk < InMemory:
        //   if (is in readcache)
        //   else (is in memory)
        //   else (is on disk)
        // 00 is the lowest and is currently reserved
        internal const long kReservedAddressTypeBitMask = 0L;
        internal const long kIsReadCacheBitMask = 0b11L << kAddressTypeBitOffset;   // 3
        internal const long kIsInLogMemoryBitMask = 0b10L << kAddressTypeBitOffset; // 2
        internal const long kIsOnDiskBitMask = 0b01L << kAddressTypeBitOffset;      // 1

        public const long kInvalidAddress = 0L;
        public const long kTempInvalidAddress = 1L;
        public const long FirstValidAddress = 64L | kIsInLogMemoryBitMask;
        public const long MaxValidAddress = kAbsoluteAddressBitMask | kIsInLogMemoryBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsReadCache(long address) => (address & kIsReadCacheBitMask) == kIsReadCacheBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long SetIsReadCache(long address) => (address & kAbsoluteAddressBitMask) | kIsReadCacheBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsInLogMemory(long address) => (address & kIsInLogMemoryBitMask) == kIsInLogMemoryBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long SetIsInLogMemory(long address) => (address & kAbsoluteAddressBitMask) | kIsInLogMemoryBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsOnDisk(long address) => (address & kIsOnDiskBitMask) == kIsOnDiskBitMask;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long SetIsOnDisk(long address) => (address & kAbsoluteAddressBitMask) | kIsOnDiskBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsInMemory(long address) => (address & kAddressTypeBitMask) >= kIsInLogMemoryBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long AbsoluteAddress(long address) => address & kAbsoluteAddressBitMask;

        public static string AddressString(long address)
        {
            var absoluteAddress = AbsoluteAddress(address);
            if (IsReadCache(address))
                return $"rc:{absoluteAddress}";
            if (IsInLogMemory(address))
                return $"log:{absoluteAddress}";
            if (IsOnDisk(address))
                return $"disk:{absoluteAddress}";
            if (address == kInvalidAddress)
                return "kInvalid";
            if (address == kTempInvalidAddress)
                return "kTempInvalid";
            return $"unk:{address}";
        }
    }
}
