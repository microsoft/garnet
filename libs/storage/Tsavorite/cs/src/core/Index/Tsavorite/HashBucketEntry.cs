// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Tsavorite.core.LogAddress;

#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter

namespace Tsavorite.core
{
    // Long value layout: [1-bit tentative][13-bit TAG][50-bit address]
    // Physical little endian memory layout: [50-bit address][13-bit TAG][1-bit tentative]
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct HashBucketEntry
    {
        // Position of fields in hash-table entry
        public const int kTentativeBitShift = 63;
        public const long kTentativeBitMask = 1L << kTentativeBitShift;

        public const int kTagSize = 63 - kAddressBits;
        public const int kTagShift = 63 - kTagSize;
        public const long kTagMask = (1L << kTagSize) - 1;
        public const long kTagPositionMask = kTagMask << kTagShift;

        // Position of tag in hash value (offset is always in the least significant bits)
        public const int kHashTagShift = 64 - kTagSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort GetTag(long hashCode) => (ushort)(((ulong)hashCode >> kHashTagShift) & kTagMask);

        [FieldOffset(0)]
        public long word;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long address, ushort tag)
        {
            word = (address & kAddressBitMask)
                 | ((tag & kTagMask) << kTagShift);
        }

        public long Address
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => word & kAddressBitMask;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => word = (word & ~kAddressBitMask) | (value & kAddressBitMask);
        }

        public ushort Tag
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (ushort)((word & kTagPositionMask) >> kTagShift);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => word = (word & ~kTagPositionMask) | ((value & kTagMask) << kTagShift);
        }

        public bool Tentative
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (word & kTentativeBitMask) != 0;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => word = value ? (word | kTentativeBitMask) : (word & ~kTentativeBitMask);
        }

        public readonly bool IsReadCache
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => LogAddress.IsReadCache(word);
        }

        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            return $"addr {AddressString(Address)}, tag {Tag}, tent {bstr(Tentative)}";
        }
    }
}