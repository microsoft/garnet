// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    /// <summary>
    /// AddressInfo struct
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public unsafe struct AddressInfo
    {
        private const int kMultiplierBits = 1;
        private static readonly int kTotalBits = sizeof(IntPtr) * 8;
        private static readonly int kAddressBits = 42 * kTotalBits / 64;
        private static readonly int kSizeBits = kTotalBits - kAddressBits - kMultiplierBits;
        private static readonly long kSizeMaskInWord = ((1L << kSizeBits) - 1) << kAddressBits;
        private static readonly long kSizeMaskInInteger = (1L << kSizeBits) - 1;
        private static readonly long kMultiplierMaskInWord = ((1L << kMultiplierBits) - 1) << (kAddressBits + kSizeBits);
        private const long kMultiplierMaskInInteger = (1L << kMultiplierBits) - 1;
        private static readonly long kAddressMask = (1L << kAddressBits) - 1;


        [FieldOffset(0)]
        private IntPtr word;

        public static void WriteInfo(AddressInfo* info, long address, long size)
        {
            info->word = default;
            info->Address = address;
            info->Size = size;
        }

        public static string ToString(AddressInfo* info)
        {
            return "RecordHeader Word = " + info->word;
        }

        public long Size
        {
            readonly get
            {
                int multiplier = (int)((((long)word & kMultiplierMaskInWord) >> (kAddressBits + kSizeBits)) & kMultiplierMaskInInteger);
                return (multiplier == 0 ? 512 : 1 << 20) * ((((long)word & kSizeMaskInWord) >> kAddressBits) & kSizeMaskInInteger);
            }
            set
            {
                int multiplier = 0;
                int val = (int)(value >> 9);
                if ((value & ((1 << 9) - 1)) != 0) val++;

                if (val >= (1 << kSizeBits))
                {
                    val = (int)(value >> 20);
                    if ((value & ((1 << 20) - 1)) != 0) val++;
                    multiplier = 1;
                    if (val >= (1 << kSizeBits))
                    {
                        throw new TsavoriteException("Unsupported object size: " + value);
                    }
                }
                var _word = (long)word;
                _word &= ~kSizeMaskInWord;
                _word &= ~kMultiplierMaskInWord;
                _word |= (val & kSizeMaskInInteger) << kAddressBits;
                _word |= (multiplier & kMultiplierMaskInInteger) << (kAddressBits + kSizeBits);
                word = (IntPtr)_word;
            }
        }

        public long Address
        {
            readonly get
            {
                return (long)word & kAddressMask;
            }
            set
            {
                var _word = (long)word;
                _word &= ~kAddressMask;
                _word |= (value & kAddressMask);
                word = (IntPtr)_word;
                if (value != Address)
                {
                    throw new TsavoriteException("Overflow in AddressInfo" + ((kAddressBits < 64) ? " - consider running the program in x64 mode for larger address space support" : ""));
                }
            }
        }
    }
}