// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Buffers.Binary;
using System.Numerics;

namespace Garnet.server
{
    public unsafe partial class BitmapManager
    {
        /// <summary>
        /// Main driver for BITPOS command
        /// </summary>
        /// <param name="input"></param>
        /// <param name="inputLen"></param>
        /// <param name="startOffset"></param>
        /// <param name="endOffset"></param>
        /// <param name="searchFor"></param>
        /// <param name="offsetType"></param>
        /// <returns></returns>
        public static long BitPosDriver(byte* input, int inputLen, long startOffset, long endOffset, byte searchFor, byte offsetType)
        {
            if (offsetType == 0x0)
            {
                startOffset = startOffset < 0 ? ProcessNegativeOffset(startOffset, inputLen) : startOffset;
                endOffset = endOffset < 0 ? ProcessNegativeOffset(endOffset, inputLen) : endOffset;

                if (startOffset >= inputLen) // If startOffset greater that valLen always bitpos -1
                    return -1;

                if (startOffset > endOffset) // If start offset beyond endOffset return 0
                    return -1;

                endOffset = endOffset >= inputLen ? inputLen : endOffset;
                // BYTE search
                return BitPosByteSearch(input, inputLen, startOffset, endOffset, searchFor);
            }
            else
            {
                startOffset = startOffset < 0 ? ProcessNegativeOffset(startOffset, inputLen * 8) : startOffset;
                endOffset = endOffset < 0 ? ProcessNegativeOffset(endOffset, inputLen * 8) : endOffset;

                var startByteIndex = startOffset >> 3;
                var endByteIndex = endOffset >> 3;

                if (startByteIndex >= inputLen) // If startOffset greater that valLen always bitpos -1
                    return -1;

                if (startByteIndex > endByteIndex) // If start offset beyond endOffset return 0
                    return -1;

                endOffset = endByteIndex >= inputLen ? inputLen << 3 : endOffset;

                // BIT search
                return BitPosBitSearch(input, inputLen, startOffset, endOffset, searchFor);
            }
        }

        /// <summary>
        /// Search for position of bit set in byte array using bit offset for start and end range
        /// </summary>
        /// <param name="input"></param>
        /// <param name="inputLen"></param>
        /// <param name="startBitOffset"></param>
        /// <param name="endBitOffset"></param>
        /// <param name="searchFor"></param>
        /// <returns></returns>
        private static long BitPosBitSearch(byte* input, long inputLen, long startBitOffset, long endBitOffset, byte searchFor)
        {
            var searchBit = searchFor == 1;
            var invalidPayload = (byte)(searchBit ? 0x00 : 0xff);
            var currentBitOffset = (int)startBitOffset;
            while (currentBitOffset <= endBitOffset)
            {
                var byteIndex = currentBitOffset >> 3;
                var leftBitOffset = currentBitOffset & 7;
                var boundary = 8 - leftBitOffset;
                var rightBitOffset = currentBitOffset + boundary <= endBitOffset ? leftBitOffset + boundary : (int)(endBitOffset & 7) + 1;

                // Trim byte to start and end bit index
                var mask = (0xff >> leftBitOffset) ^ (0xff >> rightBitOffset);
                var payload = (long)(input[byteIndex] & mask);

                // Invalid only if equals the masked payload
                var invalidMask = invalidPayload & mask;

                // If transformed payload is invalid skip to next byte
                if (payload != invalidMask)
                {
                    payload <<= (56 + leftBitOffset);
                    payload = searchBit ? payload : ~payload;

                    var lzcnt = (long)BitOperations.LeadingZeroCount((ulong)payload);
                    return currentBitOffset + lzcnt;
                }

                currentBitOffset += boundary;
            }

            return -1;
        }

        /// <summary>
        /// Search for position of bit set in byte array using byte offset for start and end range
        /// </summary>
        /// <param name="input"></param>
        /// <param name="inputLen"></param>
        /// <param name="startOffset"></param>
        /// <param name="endOffset"></param>
        /// <param name="searchFor"></param>
        /// <returns></returns>
        private static long BitPosByteSearch(byte* input, long inputLen, long startOffset, long endOffset, byte searchFor)
        {
            // Initialize variables
            var searchBit = searchFor == 1;
            var invalidMask8 = searchBit ? 0x00 : 0xff;
            var invalidMask32 = searchBit ? 0 : -1;
            var invalidMask64 = searchBit ? 0L : -1L;
            var currentStartOffset = startOffset;

            while (currentStartOffset <= endOffset)
            {
                var remainder = endOffset - currentStartOffset + 1;
                if (remainder >= 8)
                {
                    var payload = *(long*)(input + currentStartOffset);
                    payload = BinaryPrimitives.ReverseEndianness(payload);

                    // Process only if payload is valid (i.e. not all bits are set or clear based on searchFor parameter)
                    if (payload != invalidMask64)
                    {
                        // Transform to count leading zeros
                        payload = searchBit ? payload : ~payload;
                        var lzcnt = (long)BitOperations.LeadingZeroCount((ulong)payload);
                        return (currentStartOffset << 3) + lzcnt;
                    }
                    currentStartOffset += 8;
                }
                else if (remainder >= 4)
                {
                    var payload = *(int*)(input + currentStartOffset);
                    payload = BinaryPrimitives.ReverseEndianness(payload);

                    // Process only if payload is valid (i.e. not all bits are set or clear based on searchFor parameter)
                    if (payload != invalidMask32)
                    {
                        // Transform to count leading zeros
                        payload = searchBit ? payload : ~payload;
                        var lzcnt = (long)BitOperations.LeadingZeroCount((uint)payload);
                        return (currentStartOffset << 3) + lzcnt;
                    }
                    currentStartOffset += 4;
                }
                else
                {
                    // Process only if payload is valid (i.e. not all bits are set or clear based on searchFor parameter)
                    if (input[currentStartOffset] != invalidMask8)
                    {
                        // Create a payload with the current byte shifted to the most significant byte position
                        var payload = (long)input[currentStartOffset] << 56;
                        // Transform to count leading zeros
                        payload = searchBit ? payload : ~payload;
                        var lzcnt = (long)BitOperations.LeadingZeroCount((ulong)payload);
                        return (currentStartOffset << 3) + lzcnt;
                    }
                    currentStartOffset++;
                }
            }

            // Return -1 if no matching bit is found
            return -1;
        }
    }
}