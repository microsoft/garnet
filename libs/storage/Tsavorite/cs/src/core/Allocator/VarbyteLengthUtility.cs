// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public static unsafe class VarbyteLengthUtility
    {
        // Indicator bits for version and varlen int. Since the record is always aligned to 8 bytes, we can use long operations (on values only in
        // the low byte) which are faster than byte or int.
#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter
        const long kVersionBitMask = 7 << 6;        // 2 bits for version
        const long kIsChunkedValueBitMask = 1 << 5; // 1 bit for chunked value indicator
        const long kKeyLengthBitMask = 3 << 3;      // 2 bits for the number of bytes for the key length (this is limited to 512MB)
        const long kValueLengthBitMask = 7;         // 3 bits for the number of bytes for the value length
#pragma warning restore IDE1006 // Naming Styles

        const long CurrentVersion = 0 << 6;         // Initial version is 0; shift will always be 6

        /// <summary>Version of the variable-length byte encoding for key and value lengths. There is no version info for <see cref="RecordInfo.RecordIsInline"/>
        /// records as these are image-identical to LogRecord. TODO: Include a major version for this in the Recovery version-compatibility detection</summary>
        internal static long GetVersion(byte indicatorByte) => (indicatorByte & kVersionBitMask) >> 6;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long ReadVarByteLen(int numBytes, byte* ptrToFirstByte)
        {
            var valueMask = (long)(ulong.MaxValue >> ((sizeof(ulong) - numBytes) * 8));
            var startPtr = (long*)(ptrToFirstByte + numBytes - sizeof(long));
            return *startPtr & valueMask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteVarByteLen(long value, int numBytes, ref byte* ptrToFirstByte)
        {
            var valueMask = (long)(ulong.MaxValue >> ((sizeof(ulong) - numBytes) * 8));
            var startPtr = (long*)(ptrToFirstByte + numBytes - sizeof(long));
            *startPtr = (*startPtr & ~valueMask) | (value & valueMask);
        }

        internal static int GetKeyLength(int numBytes, byte* ptrToFirstByte) => (int)ReadVarByteLen(numBytes, ptrToFirstByte);

        internal static long GetValueLength(int numBytes, byte* ptrToFirstByte) => (int)ReadVarByteLen(numBytes, ptrToFirstByte);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (int keyLengthBytes, int valueLengthBytes, bool isChunkedValue) DeconstructIndicatorByte(byte indicatorByte)
        {
            var keyLengthBytes = (int)((indicatorByte & kKeyLengthBitMask) >> 3) + 1; // add 1 due to 0-based
            var valueLengthBytes = (int)(indicatorByte & kValueLengthBitMask) + 1;     // add 1 due to 0-based
            var isChunkedValue = (indicatorByte & kIsChunkedValueBitMask) != 0;
            return (keyLengthBytes, valueLengthBytes, isChunkedValue);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte CreateIndicatorByte(int keyLength, long valueLength, out int keyByteCount, out int valueByteCount)
        {
            keyByteCount = GetByteCount(keyLength);
            valueByteCount = GetByteCount(valueLength);
            return (byte)(CurrentVersion                // Already shifted
                | ((long)(keyByteCount - 1) << 3)       // Shift key into position; subtract 1 for 0-based
                | (long)(valueByteCount - 1));          // Value does not need to be shifted; subtract 1 for 0-based
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetChunkedValueIndicator(ref byte indicatorByte) => indicatorByte = (byte)(indicatorByte | kIsChunkedValueBitMask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool HasChunkedValueIndicator(byte indicatorByte) => (indicatorByte & kIsChunkedValueBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetByteCount(long value) => ((sizeof(long) * 8) - BitOperations.LeadingZeroCount((ulong)(value | 1)) + 7) / 8;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetContinuationLength(byte* ptr, out bool hasContinuationBit)
        {
            var length = *(int*)ptr;
            hasContinuationBit = (length & IStreamBuffer.ValueChunkContinuationBit) != 0;
            return length & ~IStreamBuffer.ValueChunkContinuationBit;
        }
    }
}
