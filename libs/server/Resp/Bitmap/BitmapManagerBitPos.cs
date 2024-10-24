// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
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
        /// <param name="input">Input properties for bitmap operation.</param>
        /// <param name="value">Pointer to start of bitmap.</param>
        /// <param name="valLen">Length of bitmap.</param>
        /// <returns></returns>
        public static long BitPosDriver(byte* input, byte* value, int valLen)
        {
            //4 byte: length
            //1 byte: op-code
            //1 byte: setVal
            //4 byte: startOffset    // offset are byte indices not bits, therefore int is sufficient because max will be at most offset >> 3
            //4 byte: endOffset            
            byte bSetVal = *(input);
            long startOffset = *(long*)(input + sizeof(byte));
            long endOffset = *(long*)(input + sizeof(byte) + sizeof(long));
            byte offsetType = *(input + sizeof(byte) + sizeof(long) * 2);

            if (offsetType == 0x0)
            {
                startOffset = startOffset < 0 ? ProcessNegativeOffset(startOffset, valLen) : startOffset;
                endOffset = endOffset < 0 ? ProcessNegativeOffset(endOffset, valLen) : endOffset;

                if (startOffset >= valLen) // If startOffset greater that valLen always bitpos -1
                    return -1;

                if (startOffset > endOffset) // If start offset beyond endOffset return 0
                    return -1;

                endOffset = endOffset >= valLen ? valLen : endOffset;
                return BitPosByte(value, bSetVal, startOffset, endOffset);
            }
            else
            {
                startOffset = startOffset < 0 ? ProcessNegativeOffset(startOffset, valLen * 8) : startOffset;
                endOffset = endOffset < 0 ? ProcessNegativeOffset(endOffset, valLen * 8) : endOffset;

                long startByte = (startOffset / 8);
                long endByte = (endOffset / 8);
                if (startByte == endByte)
                {
                    // Search only inside single byte for pos
                    int leftBitIndex = (int)(startOffset & 7);
                    int rightBitIndex = (int)((endOffset + 1) & 7);
                    long _ipos = BitPosIndexBitSingleByteSearch(value[startByte], bSetVal, leftBitIndex, rightBitIndex);
                    return _ipos == -1 ? _ipos : startOffset + _ipos;
                }
                else
                {
                    // Search prefix and terminate if found position of bit
                    long _ppos = BitPosIndexBitSearch(value, bSetVal, startOffset);
                    if (_ppos != -1) return startOffset + _ppos;

                    // Adjust offsets to skip first and last byte
                    long _startOffset = (startOffset / 8) + 1;
                    long _endOffset = (endOffset / 8) - 1;
                    long _bpos = BitPosByte(value, bSetVal, _startOffset, _endOffset);
                    if (_bpos != -1) return _bpos;

                    // Search suffix
                    long _spos = BitPosIndexBitSearch(value, bSetVal, endOffset);
                    return _spos;
                }
            }
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
            long mask = bflag ? -1 : 0; // Mask for all 1's (-1 for 0 search) or all 0's (0 for 1 search)
            long len = (endOffset - startOffset) + 1;
            long remainder = len & 7; // Check if length is divisible by 8
            byte* curr = value + startOffset;
            byte* end = curr + (len - remainder); // Process up to the aligned part of the bitmap

            // Search for first word not matching the mask.
            while (curr < end)
            {
                long v = *(long*)(curr);
                if (v != mask) break;
                curr += 8; // Move by 64-bit chunks
            }

            // Calculate bit position from start of bitmap
            long pos = (((long)(curr - value)) << 3); // Convert byte position to bit position

            long payload = 0;
            // Adjust end to account for remainder
            end = end + remainder;

            // Build payload from remaining bytes
            if (curr < end) payload |= (long)curr[0] << 56;
            if (curr + 1 < end) payload |= (long)curr[1] << 48;
            if (curr + 2 < end) payload |= (long)curr[2] << 40;
            if (curr + 3 < end) payload |= (long)curr[3] << 32;
            if (curr + 4 < end) payload |= (long)curr[4] << 24;
            if (curr + 5 < end) payload |= (long)curr[5] << 16;
            if (curr + 6 < end) payload |= (long)curr[6] << 8;
            if (curr + 7 < end) payload |= (long)curr[7];

            // Transform payload for bit search
            payload = (bflag) ? ~payload : payload;

            // Handle edge cases where the bitmap is all 0's or all 1's
            if (payload == mask)
            {
                if (!bflag)
                {
                    return -1;    
                }
            }

            // Otherwise, count leading zeros to find the position of the first 1 or 0
            pos += (long)Lzcnt.X64.LeadingZeroCount((ulong)payload);

            // if we are exceeding it, return -1
            if (pos >= len * 8)
             return -1;

            return pos;
        }
    }
}