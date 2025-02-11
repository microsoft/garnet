// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.Intrinsics.X86;

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
        private static long BitPosBitSearch2(byte* input, long inputLen, long startBitOffset, long endBitOffset, byte searchFor)
        {
            // Initialize variables
            var currentBitOffset = startBitOffset;
            var searchBit = searchFor == 1;
            var mask = searchBit ? 0x00 : 0xff;

            // Iterate through the byte array
            while (currentBitOffset <= endBitOffset)
            {
                // Calculate byte and bit positions
                var byteIndex = currentBitOffset / 8;
                var bitIndex = (int)(currentBitOffset & 7);

                var payload = input[byteIndex] & (1 << 8 - (int)bitIndex) - 1;
                if (mask != payload)
                {
                    // Create a payload with the current byte shifted to the most significant byte position
                    payload = payload << 56;

                    // Trim leading bits
                    payload <<= bitIndex;

                    // Transform to count leading zeros
                    payload = searchBit ? payload : ~payload;

                    // Check if the payload matches the mask
                    // Calculate the position of the first matching bit
                    var lzcnt = (long)Lzcnt.X64.LeadingZeroCount((ulong)payload);
                    return currentBitOffset + lzcnt;
                }

                // Move to the next byte
                currentBitOffset += 8 - bitIndex;
            }

            // Return -1 if no matching bit is found
            return -1;
        }

        private static long BitPosBitSearch(byte* input, long inputLen, long startBitOffset, long endBitOffset, byte searchFor)
        {
            var searchBit = searchFor == 1;
            var invalidPayload = (byte)(searchBit ? 0x0 : 0xff);
            var currentBitOffset = (int)startBitOffset;
            while (currentBitOffset <= endBitOffset)
            {
                var byteIndex = currentBitOffset >> 3;
                var leftBitOffset = currentBitOffset & 7;
                var boundary = 8 - leftBitOffset;
                var rightBitOffset = currentBitOffset + boundary < endBitOffset ? currentBitOffset + boundary : (int)(endBitOffset & 7) + 1;

                // Trim byte to start and end bit index
                var mask = (0xff >> leftBitOffset) ^ (0xff >> rightBitOffset);
                var payload = (long)(input[byteIndex] & mask);

                // If transformed payload is invalid skip to next byte
                if (payload != invalidPayload)
                {
                    payload <<= (56 + leftBitOffset);
                    payload = searchBit ? payload : ~payload;

                    var lzcnt = (long)Lzcnt.X64.LeadingZeroCount((ulong)payload);
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
            var mask = searchBit ? 0x0 : 0xff;

            // Iterate through the byte array
            for (var i = startOffset; i <= endOffset; i++)
            {
                if (mask != input[i])
                {
                    // Create a payload with the current byte shifted to the most significant byte position
                    var payload = (long)input[i] << 56;

                    // Transform to count leading zeros
                    payload = searchBit ? payload : ~payload;

                    // Calculate the position of the first matching bit
                    var lzcnt = (long)Lzcnt.X64.LeadingZeroCount((ulong)payload);
                    return (i * 8) + lzcnt;
                }
            }

            // Return -1 if no matching bit is found
            return -1;
        }
    }
}