// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Utilities for varlen bytes: one indicator byte identifying the number of key and value bytes. The layout of this indicator byte is:
    /// <list type="bullet">
    ///     <item>Indicator byte: version, chunked or filler flag number of bytes in value length</item>
    ///     <item>Number of bytes in key length; may be inline length or <see cref="ObjectIdMap.ObjectIdSize"/> if Overflow</item>
    ///     <item>Number of bytes in value length; may be inline length or <see cref="ObjectIdMap.ObjectIdSize"/> if Overflow or Object</item>
    /// </list>
    /// This is followed by the actual key length and value length, which may be inline length or <see cref="ObjectIdMap.ObjectIdSize"/> if Overflow
    /// or object. For in=memory objects, the max inline key size is 16MB to remain in 3 bytes, and the max inline value size is int.MaxValue
    /// which is 4 bytes, so the total is 8 bytes which can be atomically updated.
    /// </summary>
    public static unsafe class VarbyteLengthUtility
    {
        // Indicator bits for version and varlen int. Since the record is always aligned to 8 bytes, we can use long operations (on values only in
        // the low byte) which are faster than byte or int.
#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter
        // The top 3 bytes are Indicator metadata
        const long kVersionBitMask = 7 << 6;        // 2 bits for version; currently not checked and may be repurposed
        const long CurrentVersion = 0 << 6;         // Initial version is 0; shift will always be 6

        // These share the same bit: HasFiller is only in-memory, and IsChunked is only on-disk.
        const long kIsChunkedValueBitMask = 1 << 5; // 1 bit for chunked value indicator
        internal const long kHasFillerBitMask = 1 << 5; // 1 bit for chunked value indicator

        // The bottom 5 bits are actual length bytecounts
        /// <summary>
        /// 2 bits for the number of bytes for the key length:
        /// <list>
        /// <item>In-memory: this is limited to 16MB inline, so 3 bytes; <see cref="ObjectAllocator{TStoreFunctions}"/> allows Overflow,
        ///     which takes only <see cref="ObjectIdMap.ObjectIdSize"/> (4) bytes </item>
        /// <item>On-disk: this is limited to max Overflow length, so 4 bytes</item>
        /// </list>
        /// </summary>
        const long kKeyLengthBitMask = 3 << 3;
        /// <summary>
        /// 3 bits for the number of bytes for the value length:
        /// <list>
        /// <item>In-memory: this is limited to 16MB inline, so 3 bytes; <see cref="ObjectAllocator{TStoreFunctions}"/> allows Overflow and Object,
        ///     which take only <see cref="ObjectIdMap.ObjectIdSize"/> (4) bytes</item>
        /// <item>On-disk: this is limited to either max Object length, but since we have an effective limit of <see cref="LogAddress.kAddressTypeBitOffset"/> bits
        ///     <see cref="RecordInfo.PreviousAddress"/>, this will not be greater than 6 bytes.</item>
        /// </list>
        /// </summary>
        const long kValueLengthBitMask = 7;
#pragma warning restore IDE1006 // Naming Styles

        /// <summary>Version of the variable-length byte encoding for key and value lengths. There is no version info for <see cref="RecordInfo.RecordIsInline"/>
        /// records as these are image-identical to LogRecord. TODO: Include a major version for this in the Recovery version-compatibility detection</summary>
        internal static long GetVersion(byte indicatorByte) => (indicatorByte & kVersionBitMask) >> 6;

        /// <summary>
        /// Read var-length bytes. This assumes it comes after the <see cref="RecordInfo"/> header so there is enough space to "back up" safely by sizeof(long).
        /// It backs up into the <see cref="RecordInfo"/> instead of going forward as a long "word" because this way it can apply to both in-memory and on-disk
        /// (where lengths may themselves be "long", unlike in-memory).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long ReadVarbyteLength(int numBytes, byte* ptrToFirstByte)
        {
            var valueMask = (long)(ulong.MaxValue >> ((sizeof(ulong) - numBytes) * 8));
            var startPtr = (long*)(ptrToFirstByte + numBytes - sizeof(long));
            return *startPtr & valueMask;
        }

        /// <summary>
        /// Read var-length bytes. This assumes it comes after the <see cref="RecordInfo"/> header so there is enough space to "back up" safely by sizeof(long).
        /// It backs up into the <see cref="RecordInfo"/> instead of going forward as a long "word" because this way it can apply to both in-memory and on-disk
        /// (where lengths may themselves be "long", unlike in-memory).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteVarbyteLength(long value, int numBytes, byte* ptrToFirstByte)
        {
            var valueMask = (long)(ulong.MaxValue >> ((sizeof(ulong) - numBytes) * 8));
            var startPtr = (long*)(ptrToFirstByte + numBytes - sizeof(long));
            *startPtr = (*startPtr & ~valueMask) | (value & valueMask);
        }

        internal static int GetKeyLength(int numBytes, byte* ptrToFirstByte) => (int)ReadVarbyteLength(numBytes, ptrToFirstByte);

        internal static long GetValueLength(int numBytes, byte* ptrToFirstByte) => (int)ReadVarbyteLength(numBytes, ptrToFirstByte);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte ConstructIndicatorByte(int keyLength, long valueLength, out int keyByteCount, out int valueByteCount)
        {
            keyByteCount = GetByteCount(keyLength);
            valueByteCount = GetByteCount(valueLength);
            return (byte)(CurrentVersion                // Already shifted
                | ((long)(keyByteCount - 1) << 3)       // Shift key into position; subtract 1 for 0-based
                | (long)(valueByteCount - 1));          // Value does not need to be shifted; subtract 1 for 0-based
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte ConstructIndicatorByte(int keyByteCount, int valueByteCount)
        {
            return (byte)(CurrentVersion                // Already shifted
                | ((long)(keyByteCount - 1) << 3)       // Shift key into position; subtract 1 for 0-based
                | (long)(valueByteCount - 1));          // Value does not need to be shifted; subtract 1 for 0-based
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (int keyLengthBytes, int valueLengthBytes, bool isChunkedValueOrHasFiller) DeconstructIndicatorByte(byte indicatorByte)
        {
            Debug.Assert(kIsChunkedValueBitMask == kHasFillerBitMask, "kIsChunkedValueBitMask must equal kHasFillerBitMask");   // We "union" this bit
            var keyLengthBytes = (int)((indicatorByte & kKeyLengthBitMask) >> 3) + 1;   // add 1 due to 0-based
            var valueLengthBytes = (int)(indicatorByte & kValueLengthBitMask) + 1;      // add 1 due to 0-based
            var isChunkedValueOrHasFiller = (indicatorByte & kIsChunkedValueBitMask) != 0;
            return (keyLengthBytes, valueLengthBytes, isChunkedValueOrHasFiller);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetChunkedValueIndicator(ref byte indicatorByte) => indicatorByte = (byte)(indicatorByte | kIsChunkedValueBitMask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool HasChunkedValueIndicator(byte indicatorByte) => (indicatorByte & kIsChunkedValueBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool HasFiller(byte indicatorByte) => (indicatorByte & kHasFillerBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetHasFiller(long indicatorAddress) => *(byte*)indicatorAddress = (byte)(*(byte*)indicatorAddress | kHasFillerBitMask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ClearHasFiller(long indicatorAddress) => *(byte*)indicatorAddress = (byte)(*(byte*)indicatorAddress & ~kHasFillerBitMask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetByteCount(long value) => ((sizeof(long) * 8) - BitOperations.LeadingZeroCount((ulong)(value | 1)) + 7) / 8;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetContinuationLength(byte* ptr, out bool hasContinuationBit)
        {
            var length = *(int*)ptr;
            hasContinuationBit = (length & IStreamBuffer.ValueChunkContinuationBit) != 0;
            return length & ~IStreamBuffer.ValueChunkContinuationBit;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (int length, long dataAddress) GetKeyFieldInfo(long indicatorAddress)
        {
            var ptr = (byte*)indicatorAddress;
            var (keyLengthBytes, valueLengthBytes, _ /*isChunkedValue*/) = DeconstructIndicatorByte(*ptr);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLength(keyLengthBytes, ++ptr);

            // Move past the key and value length bytes to the start of the key data
            return ((int)keyLength, (long)(ptr + keyLengthBytes + valueLengthBytes));
        }

        internal static (long length, long dataAddress) GetValueFieldInfo(long indicatorAddress)
        {
            var ptr = (byte*)indicatorAddress;
            var (keyLengthBytes, valueLengthBytes, _ /*isChunkedValue*/) = DeconstructIndicatorByte(*ptr);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLength(keyLengthBytes, ++ptr);

            // Move past the key length bytes; the next bytes are valueLength
            var valueLength = ReadVarbyteLength(valueLengthBytes, ptr += keyLengthBytes);

            // Move past the key and value length bytes and the key data to the start of the value data
            return (valueLength, (long)(ptr + valueLengthBytes + keyLength));
        }

        /// <summary>
        /// Get the value data pointer, as well as the pointer to length, length, and number of length bytes. This is to support in-place updating.
        /// </summary>
        /// <returns>The value data pointer</returns>
        internal static byte* GetFieldPtr(long indicatorAddress, bool isKey, out byte* lengthPtr, out int lengthBytes, out long length)
        {
            var ptr = (byte*)indicatorAddress;
            var (keyLengthBytes, valueLengthBytes, _ /*isChunkedValue*/) = DeconstructIndicatorByte(*ptr);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLength(keyLengthBytes, ++ptr);
            if (isKey)
            {
                lengthPtr = ptr;
                lengthBytes = keyLengthBytes;
                length = keyLength;
                return ptr + keyLengthBytes;
            }

            // Move past the key length bytes; the next bytes are valueLength. Read those, then skip over the key bytes to get the value data pointer.
            lengthPtr = ptr + keyLengthBytes;
            lengthBytes = valueLengthBytes;
            length = ReadVarbyteLength(lengthBytes, lengthPtr);
            return lengthPtr + lengthBytes + keyLength;
        }

        /// <summary>
        /// Get the value data pointer, as well as the pointer to length, length, and number of length bytes. This is to support in-place updating.
        /// </summary>
        /// <returns>The value data pointer</returns>
        internal static (int keyLength, int valueLength, int offsetToKeyStart) GetInlineKeyAndValueSizes(long indicatorAddress)
        {
            var ptr = (byte*)indicatorAddress;
            var (keyLengthBytes, valueLengthBytes, _ /*isChunkedValueOrHasFiller*/) = DeconstructIndicatorByte(*ptr);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = (int)ReadVarbyteLength(keyLengthBytes, ++ptr);

            // Move past the key bytes; the next bytes are valueLength
            var valueLength = ReadVarbyteLength(valueLengthBytes, ptr + keyLengthBytes);
            return (keyLength, (int)valueLength, RecordInfo.GetLength() + 1 + keyLengthBytes + valueLengthBytes);
        }

        /// <summary>
        /// Construct the in-memory inline varbyte indicator word consisting of keyLengthBytes, valueLengthBytes, and a "has filler" indicator.
        /// This is used to atomically update the varbyte length information so scanning will be consistent.
        /// </summary>
        /// <param name="keyLength">The inline length of the key</param>
        /// <param name="valueLength">The inline length of the value</param>
        /// <param name="hasFillerBit">Either kHasFillerBitMask if we have set a filler length into the in-memory record, or 0</param>
        /// <param name="keyLengthBytes">Receives the number of bytes in the key length</param>
        /// <param name="valueLengthBytes">Receives the number of bytes in the value length</param>
        /// <returns></returns>
        internal static unsafe long ConstructInlineVarbyteLengthWord(int keyLength, int valueLength, long hasFillerBit, out int keyLengthBytes, out int valueLengthBytes)
        {
            keyLengthBytes = GetByteCount(keyLength);
            valueLengthBytes = GetByteCount(valueLength);
            return ConstructInlineVarbyteLengthWord(keyLengthBytes, keyLength, valueLengthBytes, valueLength, hasFillerBit);
        }

        /// <summary>
        /// Construct the in-memory inline varbyte indicator word consisting of keyLengthBytes, valueLengthBytes, and a "has filler" indicator.
        /// This is used to atomically update the varbyte length information so scanning will be consistent.
        /// </summary>
        /// <param name="keyLengthBytes">Number of bytes in the key length</param>
        /// <param name="keyLength">The inline length of the key</param>
        /// <param name="valueLengthBytes">Number of bytes in the value length</param>
        /// <param name="valueLength">The inline length of the value</param>
        /// <param name="hasFillerBit">Either kHasFillerBitMask if we have set a filler length into the in-memory record, or 0</param>
        /// <returns></returns>
        internal static unsafe long ConstructInlineVarbyteLengthWord(int keyLengthBytes, int keyLength, int valueLengthBytes, int valueLength, long hasFillerBit)
        {
            var word = (long)0;
            var ptr = (byte*)&word;
            var indicatorByte = (byte)(ConstructIndicatorByte(keyLengthBytes, valueLengthBytes) | hasFillerBit);
            *ptr++ = indicatorByte;

            WriteVarbyteLength(keyLength, keyLengthBytes, ptr);
            WriteVarbyteLength(valueLength, valueLengthBytes, ptr + keyLengthBytes);
            return word;
        }

        /// <summary>
        /// Deconstruct the in-memory inline varbyte indicator word to return keyLengthBytes, valueLengthBytes, and the "has filler" indicator.
        /// This is used to atomically update the varbyte length information so scanning will be consistent.
        /// </summary>
        /// <return>keyLengthBytes, valueLengthBytes, and the "has filler" indicator</return>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe (int keyLengthBytes, int valueLengthBytes, bool hasFiller) DeconstructInlineVarbyteLengthWord(long word)
        {
            var ptr = (byte*)&word;
            (var keyLengthBytes, var valueLengthBytes, var hasFiller) = DeconstructIndicatorByte(*ptr++);
            Debug.Assert(keyLengthBytes <= 3, "Inline keyLengthBytes limit exceeded");
            Debug.Assert(valueLengthBytes <= 4, "Inline valueLengthBytes limit exceeded");

            var keyLength = (int)ReadVarbyteLength(keyLengthBytes, ptr);
            var valueLength = (int)ReadVarbyteLength(valueLengthBytes, ptr + keyLengthBytes);
            return (keyLength, valueLength, hasFiller);
        }

        /// <summary>
        /// Update the value length in the in-memory inline varbyte indicator word 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void UpdateInlineVarbyteLengthWord(long indicatorAddress, int keyLengthBytes, int valueLengthBytes, int valueLength, long hasFillerBit)
        {
            // Mask off the filler bit; we'll reset it on return.
            var word = *(long*)indicatorAddress & ~kHasFillerBitMask;
            var ptr = (byte*)&word;
            WriteVarbyteLength(valueLength, valueLengthBytes, ptr + 1 + keyLengthBytes);
            *(long*)indicatorAddress = word & hasFillerBit;
        }
    }
}
