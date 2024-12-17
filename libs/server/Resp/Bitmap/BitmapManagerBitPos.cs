﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Numerics;
using System.Runtime.Intrinsics.X86;

namespace Garnet.server
{
    public unsafe partial class BitmapManager
    {
        /// <summary>
        /// Find pos of bit set/clear for given bit offsets within a single byte.
        /// </summary>
        /// <param name="value">Byte value to search within.</param>
        /// <param name="bSetVal">Bit value to search for (0|1).</param>
        /// <param name="startBitOffset">Start most significant bit offset in byte value.</param>
        /// <param name="endBitOffset">End most significant bit offset in bitmap.</param>
        /// <returns></returns>
        private static long BitPosIndexBitSingleByteSearch(byte value, byte bSetVal, int startBitOffset = 0, int endBitOffset = 8)
        {
            Debug.Assert(startBitOffset >= 0 && startBitOffset <= 8);
            Debug.Assert(endBitOffset >= 0 && endBitOffset <= 8);
            bool bflag = (bSetVal == 0);
            long mask = bflag ? -1 : 0;

            int leftBitIndex = 1 << (8 - startBitOffset);
            int rightBitIndex = 1 << (8 - endBitOffset);

            // Create extraction mask
            long extract = leftBitIndex - rightBitIndex;

            long payload = (long)(value & extract) << 56;
            // Trim leading bits
            payload = payload << startBitOffset;

            // Transform to count leading zeros
            payload = bflag ? ~payload : payload;

            // Return not found
            if (payload == mask) return -1;

            return (long)Lzcnt.X64.LeadingZeroCount((ulong)payload);
        }

        /// <summary>
        /// Find pos of bit set/clear for given bit offset.
        /// </summary>
        /// <param name="value">Pointer to start of bitmap.</param>
        /// <param name="bSetVal">Bit value to search for (0|1).</param>
        /// <param name="offset">Bit offset in bitmap.</param>
        /// <returns></returns>
        private static long BitPosIndexBitSearch(byte* value, byte bSetVal, long offset = 0)
        {
            bool bflag = (bSetVal == 0);
            long mask = bflag ? -1 : 0;
            long startByteOffset = (offset / 8);
            int bitOffset = (int)(offset & 7);

            long payload = (long)value[startByteOffset] << 56;
            // Trim leading bits
            payload = payload << bitOffset;

            // Transform to count leading zeros
            payload = bflag ? ~payload : payload;

            // Return not found
            if (payload == mask)
                return -1;

            return (long)Lzcnt.X64.LeadingZeroCount((ulong)payload);
        }

        /// <summary>
        /// Main driver for bit position command.
        /// </summary>
        /// <param name="setVal"></param>
        /// <param name="startOffset"></param>
        /// <param name="endOffset"></param>
        /// <param name="offsetType"></param>
        /// <param name="value">Pointer to start of bitmap.</param>
        /// <param name="valLen">Length of bitmap.</param>
        /// <returns></returns>
        public static long BitPosDriver(byte setVal, long startOffset, long endOffset, byte offsetType, byte* value, int valLen)
        {
            if (offsetType == 0x0)
            {
                startOffset = startOffset < 0 ? ProcessNegativeOffset(startOffset, valLen) : startOffset;
                endOffset = endOffset < 0 ? ProcessNegativeOffset(endOffset, valLen) : endOffset;

                if (startOffset >= valLen) // If startOffset greater that valLen always bitpos -1
                    return -1;

                if (startOffset > endOffset) // If start offset beyond endOffset return 0
                    return -1;

                endOffset = endOffset >= valLen ? valLen : endOffset;
                long pos = BitPosByte(value, setVal, startOffset, endOffset);
                // check if position is exceeding the last byte in acceptable range
                return pos >= ((endOffset + 1) * 8) ? -1 : pos;
            }

            startOffset = startOffset < 0 ? ProcessNegativeOffset(startOffset, valLen * 8) : startOffset;
            endOffset = endOffset < 0 ? ProcessNegativeOffset(endOffset, valLen * 8) : endOffset;

            var startByte = (startOffset / 8);
            var endByte = (endOffset / 8);
            if (startByte == endByte)
            {
                // Search only inside single byte for pos
                var leftBitIndex = (int)(startOffset & 7);
                var rightBitIndex = (int)((endOffset + 1) & 7);
                var _ipos = BitPosIndexBitSingleByteSearch(value[startByte], setVal, leftBitIndex, rightBitIndex);
                return _ipos == -1 ? _ipos : startOffset + _ipos;
            }

            // Search prefix and terminate if found position of bit
            var _ppos = BitPosIndexBitSearch(value, setVal, startOffset);
            if (_ppos != -1) return startOffset + _ppos;

            // Adjust offsets to skip first and last byte
            var _startOffset = (startOffset / 8) + 1;
            var _endOffset = (endOffset / 8) - 1;
            var _bpos = BitPosByte(value, setVal, _startOffset, _endOffset);

            if (_bpos != -1 && _bpos < (_endOffset + 1) * 8) return _bpos;

            // Search suffix
            var _spos = BitPosIndexBitSearch(value, setVal, endOffset);
            return _spos;
        }

        /// <summary>
        /// Find pos of set/clear bit in a sequence of bytes.
        /// </summary>
        /// <param name="value">Pointer to start of bitmap.</param>
        /// <param name="bSetVal">The bit value to search for (0 for cleared bit or 1 for set bit).</param>
        /// <param name="startOffset">Starting offset into bitmap.</param>
        /// <param name="endOffset">End offset into bitmap.</param>
        /// <returns></returns>
        private static long BitPosByte(byte* value, byte bSetVal, long startOffset, long endOffset)
        {
            // Mask set to look for 0 or 1 depending on clear/set flag
            bool bflag = (bSetVal == 0);
            long mask = bflag ? -1 : 0;
            long len = (endOffset - startOffset) + 1;
            long remainder = len & 7;
            byte* curr = value + startOffset;
            byte* end = curr + (len - remainder);

            // Search for first word not matching mask.
            while (curr < end)
            {
                long v = *(long*)(curr);
                if (v != mask) break;
                curr += 8;
            }

            long pos = (((long)(curr - value)) << 3);

            long payload = 0;
            // Adjust end so we can retrieve word
            end = end + remainder;

            // Build payload at least one byte to examine
            if (curr < end) payload |= (long)curr[0] << 56;
            if (curr + 1 < end) payload |= (long)curr[1] << 48;
            if (curr + 2 < end) payload |= (long)curr[2] << 40;
            if (curr + 3 < end) payload |= (long)curr[3] << 32;
            if (curr + 4 < end) payload |= (long)curr[4] << 24;
            if (curr + 5 < end) payload |= (long)curr[5] << 16;
            if (curr + 6 < end) payload |= (long)curr[6] << 8;
            if (curr + 7 < end) payload |= (long)curr[7];

            // Transform to count leading zeros
            payload = (bSetVal == 0) ? ~payload : payload;

            if (payload == mask)
                return pos + 0;

            pos += (long)BitOperations.LeadingZeroCount((ulong)payload);

            return pos;
        }
    }
}