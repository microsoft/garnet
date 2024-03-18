// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    // Long value layout: [1-bit tentative][15-bit TAG][48-bit address]
    // Physical little endian memory layout: [48-bit address][15-bit TAG][1-bit tentative]
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct HashBucketEntry
    {
        [FieldOffset(0)]
        public long word;
        public long Address
        {
            readonly get => word & Constants.kAddressMask;
            set
            {
                word &= ~Constants.kAddressMask;
                word |= value & Constants.kAddressMask;
            }
        }

        public readonly long AbsoluteAddress => Utility.AbsoluteAddress(Address);

        public ushort Tag
        {
            readonly get => (ushort)((word & Constants.kTagPositionMask) >> Constants.kTagShift);
            set
            {
                word &= ~Constants.kTagPositionMask;
                word |= (long)value << Constants.kTagShift;
            }
        }

        public bool Tentative
        {
            readonly get => (word & Constants.kTentativeBitMask) != 0;
            set
            {
                if (value)
                    word |= Constants.kTentativeBitMask;
                else
                    word &= ~Constants.kTentativeBitMask;
            }
        }

        public bool ReadCache
        {
            readonly get => (word & Constants.kReadCacheBitMask) != 0;
            set
            {
                if (value)
                    word |= Constants.kReadCacheBitMask;
                else
                    word &= ~Constants.kReadCacheBitMask;
            }
        }

        public override readonly string ToString()
        {
            var addrRC = ReadCache ? "(rc)" : string.Empty;
            static string bstr(bool value) => value ? "T" : "F";
            return $"addr {AbsoluteAddress}{addrRC}, tag {Tag}, tent {bstr(Tentative)}";
        }
    }
}