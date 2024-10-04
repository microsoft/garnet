// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

#pragma warning disable IDE0054 // Use compound assignment
#pragma warning disable IDE1006 // Naming Styles

namespace Garnet.server
{
    /// <summary>
    /// Bitmap management methods
    /// </summary>
    public unsafe partial class BitmapManager
    {
        static readonly byte[] lookup = [0x0, 0x8, 0x4, 0xc, 0x2, 0xa, 0x6, 0xe, 0x1, 0x9, 0x5, 0xd, 0x3, 0xb, 0x7, 0xf];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Index(long offset) => (int)(offset >> 3);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int LengthInBytes(long offset) => (int)((offset >> 3) + 1);

        /// <summary>
        /// Check to see if offset contained by value size
        /// </summary>
        /// <param name="vlen"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsLargeEnough(int vlen, long offset)
        {
            return LengthInBytes(offset) <= vlen;
        }

        /// <summary>
        /// Get minimum length from offset in CmdInput
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Length(long offset)
        {
            return LengthInBytes(offset);
        }

        /// <summary>
        /// Get bitmap allocation size
        /// </summary>
        /// <param name="valueLen"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NewBlockAllocLength(int valueLen, long offset)
        {
            int lengthInBytes = Length(offset);
            return valueLen > lengthInBytes ? valueLen : lengthInBytes;
        }

        /// <summary>
        /// Update bitmap value from input
        /// </summary>
        /// <param name="value"></param>
        /// <param name="offset"></param>
        /// <param name="set"></param>        
        public static byte UpdateBitmap(byte* value, long offset, byte set)
        {
            byte oldVal = 0;

            var byteIndex = Index(offset);
            var bitIndex = 7 - (int)(offset & 7);

            var byteVal = *(value + byteIndex);
            oldVal = (byte)(((1 << bitIndex) & byteVal) >> bitIndex);

            byteVal = (byte)((byteVal & ~(1 << bitIndex)) | (set << bitIndex));
            *(value + byteIndex) = byteVal;
            return oldVal;
        }

        /// <summary>
        /// Get bit value from value ptr at offset specified at offset.
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="valLen"></param>        
        public static byte GetBit(long offset, byte* value, int valLen)
        {
            var byteIndex = Index(offset);
            byte oldVal = 0;

            if (byteIndex >= valLen) // if offset outside allocated value size, return always zero            
                oldVal = 0;
            else
            {
                var bitIndex = 7 - (int)(offset & 7);
                var byteVal = *(value + byteIndex);
                oldVal = (byte)(((1 << bitIndex) & byteVal) >> bitIndex);
            }
            return oldVal;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long ProcessNegativeOffset(long offset, int valLen)
            => (offset % valLen) + valLen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static byte reverse(byte n)
        {
            return (byte)((lookup[n & 0b1111] << 4) | lookup[n >> 4]);
        }
    }
}