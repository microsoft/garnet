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
        public const int kTagSize = 63 - kAddressBits;
        public const int kTagShift = 63 - kTagSize;
        public const long kTagMask = (1L << kTagSize) - 1;
        public const long kTagPositionMask = kTagMask << kTagShift;

        // Position of tag in hash value (offset is always in the least significant bits)
        public const int kHashTagShift = 64 - kTagSize;

        [FieldOffset(0)]
        public long word;
        public long Address
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            readonly get => word & kAddressBitMask;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set
            {
                word &= ~kAddressBitMask;
                word |= value & kAddressBitMask;
            }
        }

        public readonly long AbsoluteAddress => LogAddress.AbsoluteAddress(Address);

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
            readonly get => (word & Constants.kTentativeBitMask) != 0;

            set
            {
                if (value)
                    word |= Constants.kTentativeBitMask;
                else
                    word &= ~Constants.kTentativeBitMask;
            }
        }

        public readonly bool IsReadCache
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => LogAddress.IsReadCache(word);
        }

        public override readonly string ToString()
        {
            var addrRC = IsReadCache ? "(rc)" : string.Empty;
            static string bstr(bool value) => value ? "T" : "F";
            return $"addr {AbsoluteAddress}{addrRC}, tag {Tag}, tent {bstr(Tentative)}";
        }
    }
}