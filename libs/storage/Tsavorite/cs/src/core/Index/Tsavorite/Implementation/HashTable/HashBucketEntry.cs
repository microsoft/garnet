// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    // Long value layout: [1-bit tentative][12-bit TAG][3-bit partition][48-bit address]
    // Physical little endian memory layout: [48-bit address][3-bit partition][12-bit TAG][1-bit tentative]
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct HashBucketEntry
    {
        public const int kTentativeBitShift = 63;
        public const long kTentativeBitMask = 1L << kTentativeBitShift;

        public const int kTagBits = 11;
        public const int kTagBitShift = kTentativeBitShift - 1 - kTagBits;
        public const long kTagMask = (1L << kTagBits) - 1;
        public const long kTagPositionMask = kTagMask << kTagBitShift;

        private const int kPartitionBits = 3;
        public const int kPartitionBitShift = kTagBitShift - kPartitionBits;
        public const long kPartitionMask = (1L << kPartitionBits) - 1;
        public const long kPartitionPositionMask = kPartitionMask << kPartitionBitShift;

        public const int kAddressBits = 48;
        public const long kAddressMask = (1L << kAddressBits) - 1;

        // Position of tag in hash value (offset is always in the least significant bits)
        public const int kHashTagShift = 64 - kTagBits;

        [FieldOffset(0)]
        public long word;

        public HashBucketEntry() { }

        public long Address
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => word & kAddressMask;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => word = (word & ~kAddressMask) | (value & kAddressMask);
        }

        public readonly long AbsoluteAddress => Utility.AbsoluteAddress(Address);

        public ushort PartitionId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (ushort)((word & kPartitionPositionMask) >> kPartitionBitShift);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert(((long)value & kPartitionMask) == (long) value);
                word = (word & ~kPartitionPositionMask) | ((long)value << kPartitionBitShift);
            }
        }

        public ushort Tag
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (ushort)((word & kTagPositionMask) >> kTagBitShift);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                Debug.Assert(((long)value & kTagMask) == (long)value);
                word = (word & ~kTagPositionMask) | ((long)value << kTagBitShift);
            }
        }

        public bool Tentative
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (word & kTentativeBitMask) != 0;

            set
            {
                if (value)
                    word |= kTentativeBitMask;
                else
                    word &= ~kTentativeBitMask;
            }
        }

        public readonly bool ReadCache
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => (word & Constants.kReadCacheBitMask) != 0;
        }

        public override readonly string ToString()
        {
            var addrRC = ReadCache ? "(rc)" : string.Empty;
            static string bstr(bool value) => value ? "T" : "F";
            return $"addr {AbsoluteAddress}{addrRC}, tag {Tag}, partId {PartitionId}, tent {bstr(Tentative)}";
        }
    }
}