// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>Static utility class for manipulating logical addresses.</summary>
    internal static class LogAddress
    {
        // Address is 50 bits, with the top 2 being the address type.
        internal const int kAddressBits = 50;
        internal const long kAddressBitMask = (1L << kAddressBits) - 1;

        internal const int kAddressTypeBits = 2; // Address type bits (2 bits for 4 types, 1 currently reserved)
        internal const int kAddressTypeBitOffset = kAddressBits;
        internal const long kAddressTypeBitMask = ((1L << kAddressTypeBits) - 1) << kAddressTypeBitOffset;

        // Get the absolute address by masking out the address type bits.
        internal const long kAbsoluteAddressBitMask = (1L << (kAddressBits - kAddressTypeBits)) - 1;

        // AddressType is ordered to match the sequence of comparisons in Internal(RUMD) and ensure OnDisk < InMemory:
        //   if (is in readcache)
        //   else (is in memory)
        //   else (is on disk)
        // 00 is the lowest and is currently reserved
        internal const long kReservedAddressTypeBitMask = 0L;
        internal const long kIsReadCacheBitMask = 0x11L << kAddressTypeBitOffset; // 3
        internal const long kIsInMemoryBitMask = 0x10L << kAddressTypeBitOffset; // 2
        internal const long kIsOnDiskBitMask = 0x01L << kAddressTypeBitOffset; // 1

        public const long kInvalidAddress = 0L;
        public const long kTempInvalidAddress = 1L;

        public const long kFirstValidAddress = 64L | kIsInMemoryBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsReadCache(long address) => (address & kIsReadCacheBitMask) == kIsReadCacheBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long SetIsReadCache(long address) => (address & kAbsoluteAddressBitMask) | kIsReadCacheBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsInMemory(long address) => (address & kIsInMemoryBitMask) == kIsInMemoryBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long SetIsInMemory(long address) => (address & kAbsoluteAddressBitMask) | kIsInMemoryBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsOnDisk(long address) => (address & kIsOnDiskBitMask) == kIsOnDiskBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long SetIsOnDisk(long address) => (address & kAbsoluteAddressBitMask) | kIsOnDiskBitMask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long AbsoluteAddress(long address) => address & kAbsoluteAddressBitMask;

        public static string AddressString(long address)
        {
            var absoluteAddress = AbsoluteAddress(address);
            if (IsReadCache(address))
                return $"ReadCache: {absoluteAddress}";
            if (IsInMemory(address))
                return $"InMemory: {absoluteAddress}";
            if (IsOnDisk(address))
                return $"OnDiskAddress: {absoluteAddress}";
            if (address == kInvalidAddress)
                return "Invalid: {address}";
            if (address == kTempInvalidAddress)
                return "TempInvalid: {address}";
            return $"Untyped: {address}";
        }
    }
}
