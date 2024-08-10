// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    // Long value layout: [1-bit tentative][15-bit TAG][48-bit address]
    // Physical little endian memory layout: [48-bit address][15-bit TAG][1-bit tentative]
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct HashBucketEntry
    {
        public const int kTentativeBitShift = 63;
        public const long kTentativeBitMask = 1L << kTentativeBitShift;

        public const int kTagSize = 14;
        public const int kTagShift = kTentativeBitShift - 1 - kTagSize;
        public const long kTagMask = (1L << kTagSize) - 1;
        public const long kTagPositionMask = kTagMask << kTagShift;
        public const int kAddressBits = 48;
        public const long kAddressMask = (1L << kAddressBits) - 1;

        // Position of tag in hash value (offset is always in the least significant bits)
        public const int kHashTagShift = 64 - kTagSize;

        [FieldOffset(0)]
        public long word;
        public long Address
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => word & kAddressMask;

            set
            {
                word &= ~kAddressMask;
                word |= value & kAddressMask;
            }
        }

        public readonly long AbsoluteAddress => Utility.AbsoluteAddress(Address);

        public ushort Tag
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => (ushort)((word & kTagPositionMask) >> kTagShift);

            set
            {
                word &= ~kTagPositionMask;
                word |= (long)value << kTagShift;
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
            return $"addr {AbsoluteAddress}{addrRC}, tag {Tag}, tent {bstr(Tentative)}";
        }
    }
}