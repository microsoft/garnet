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
        // The top 3 bits are Indicator metadata
        const long kVersionBitMask = 7 << 6;            // 2 bits for version; currently not checked and may be repurposed
        const long CurrentVersion = 0 << 6;             // Initial version is 0; shift will always be 6

        // These share the same bit: HasFiller is only in-memory, and IsChunked is only on-disk.
        internal const long kHasFillerBitMask = 1 << 5; // 1 bit for "has filler" indicator; in-memory only
        const long kIsChunkedValueBitMask = 1 << 5;     // 1 bit for chunked value indicator; on-disk only,
        TODO("remove this; we need filler on disk");

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
        /// <item>On-disk: this is limited to either max Object length, but since we have an effective limit of <see cref="LogAddress.kAddressBits"/> bits
        ///     <see cref="RecordInfo.PreviousAddress"/>, this will not be greater than 6 bytes.</item>
        /// </list>
        /// </summary>
        const long kValueLengthBitMask = 7;
#pragma warning restore IDE1006 // Naming Styles

        /// <summary>The maximum number of key length bytes in the in-memory single-long word representation. Anything over this becomes overflow.</summary>
        internal const int MaxKeyLengthBytesInWord = 3;
        /// <summary>The maximum number of value length bytes in the in-memory single-long word representation. Anything over this becomes overflow.</summary>
        internal const int MaxValueLengthBytesInWord = 4;

        /// <summary>Version of the variable-length byte encoding for key and value lengths. There is no version info for <see cref="RecordInfo.RecordIsInline"/>
        /// records as these are image-identical to LogRecord. TODO: Include a major version for this in the Recovery version-compatibility detection</summary>
        internal static long GetVersion(byte indicatorByte) => (indicatorByte & kVersionBitMask) >> 6;

        /// <summary>Read var-length bytes at the given location.</summary>
        /// <remark>This is compatible with little-endian 'long'; thus, the indicator byte is the low byte of the word, then keyLengthBytes, valueLengthBytes, keyLength, valueLength in ascending address order</remark>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long ReadVarbyteLength(int numBytes, byte* ptrToFirstByte)
        {
            long value = 0;
            for (var ii = 0; ii < numBytes; ii++)
                value |= (long)*(ptrToFirstByte + ii) << (ii * 8);
            return value;
        }

        /// <summary>Read var-length bytes in the given word. Used for in-memory <see cref="LogRecord"/> and limited to 3-byte keys and 4-byte values
        ///     which combine with the indicator byte to fit into a 'long'. The shift operations are faster than the pointer-based alternative implementation
        ///     used for disk-image generation, which has the data expanded inline so may have 4-byte keys and 8-byte values.</summary>
        /// <param name="word">The word being queried</param>
        /// <param name="precedingNumBytes">If we are querying for value, this is the number of bytes in the key; otherwise it is 0</param>
        /// <param name="targetNumBytes">The number of bytes in the target (key or value)</param>
        /// <remark>This assumes little-endian; thus, the indicator byte is the low byte of the word, then keyLengthBytes, valueLengthBytes, keyLength, valueLength in ascending address order</remark>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int ReadVarbyteLengthInWord(long word, int precedingNumBytes, int targetNumBytes)
            => (int)((word >> ((1 + precedingNumBytes) * 8)) & ((1L << (targetNumBytes * 8)) - 1));

        /// <summary>Write var-length bytes at the given location.</summary>
        /// <remark>This is compatible with little-endian 'long'; thus, the indicator byte is the low byte of the word, then keyLengthBytes, valueLengthBytes, keyLength, valueLength in ascending address order</remark>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteVarbyteLength(long value, int numBytes, byte* ptrToFirstByte)
        {
            for (var ii = 0; ii < numBytes; ii++)
            {
                *(ptrToFirstByte + ii) = (byte)(value & 0xFF);
                value >>= 8;
            }
            Debug.Assert(value == 0, "len too short");
        }

        /// <summary>Write var-length bytes into the given word. Used for in-memory <see cref="LogRecord"/> and limited to 3-byte keys and 4-byte values
        ///     which combine with the indicator byte to fit into a 'long'. The shift operations are faster than the pointer-based alternative implementation
        ///     used for disk-image generation, which has the data expanded inline so may have 4-byte keys and 8-byte values.</summary>
        /// <param name="word">The word being updated</param>
        /// <param name="value">The value being set into the word</param>
        /// <param name="precedingNumBytes">If we are setting the value, this is the number of bytes in the key; otherwise it is 0</param>
        /// <param name="targetNumBytes">The number of bytes in the target (key or value)</param>
        /// <remark>This assumes little-endian; thus, the indicator byte is the low byte of the word, then keyLengthBytes, valueLengthBytes, keyLength, valueLength in ascending address order</remark>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteVarbyteLengthInWord(ref long word, int value, int precedingNumBytes, int targetNumBytes)
        {
            int shift = (1 + precedingNumBytes) * 8;
            word = (word & ~(((1L << targetNumBytes * 8) - 1) << shift)) | ((long)value << shift);
        }

        internal static int GetKeyLength(int numBytes, byte* ptrToFirstByte) => (int)ReadVarbyteLength(numBytes, ptrToFirstByte);

        internal static long GetValueLength(int numBytes, byte* ptrToFirstByte) => ReadVarbyteLength(numBytes, ptrToFirstByte);

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
            var isChunkedValueOrHasFiller = (indicatorByte & kHasFillerBitMask) != 0;
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
        internal static (int length, long dataAddress) GetKeyFieldInfo(long indicatorAddress)
        {
            var (keyLengthBytes, valueLengthBytes, _ /*isChunkedValue*/) = DeconstructIndicatorByte(*(byte*)indicatorAddress);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: 0, keyLengthBytes);

            // Move past the key and value length bytes to the start of the key data
            return (keyLength, indicatorAddress + 1 + keyLengthBytes + valueLengthBytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (long length, long dataAddress) GetValueFieldInfo(long indicatorAddress)
        {
            var (keyLengthBytes, valueLengthBytes, _ /*isChunkedValue*/) = DeconstructIndicatorByte(*(byte*)indicatorAddress);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: 0, keyLengthBytes);

            // Move past the key length bytes; the next bytes are valueLength
            var valueLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: keyLengthBytes, valueLengthBytes);

            // Move past the key and value length bytes and the key data to the start of the value data
            return (valueLength, indicatorAddress + 1 + keyLengthBytes + valueLengthBytes + keyLength);
        }

        /// <summary>
        /// Get the value data pointer, as well as the pointer to length, length, and number of length bytes. This is to support in-place updating.
        /// </summary>
        /// <returns>The value data pointer</returns>
        internal static byte* GetFieldPtr(long indicatorAddress, bool isKey, out byte* lengthPtr, out int lengthBytes, out long length)
        {
            var ptr = (byte*)indicatorAddress;
            var (keyLengthBytes, valueLengthBytes, _ /*isChunkedValue*/) = DeconstructIndicatorByte(*ptr);
            ptr++;

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: 0, keyLengthBytes);
            if (isKey)
            {
                lengthPtr = ptr;
                lengthBytes = keyLengthBytes;
                length = keyLength;
                return ptr + keyLengthBytes + valueLengthBytes;
            }

            // Move past the key length bytes; the next bytes are valueLength. Read those, then skip over the key bytes to get the value data pointer.
            lengthPtr = ptr + keyLengthBytes;
            lengthBytes = valueLengthBytes;
            length = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: keyLengthBytes, lengthBytes);
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
            var keyLength = (int)ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: 0, keyLengthBytes);

            // Move past the key bytes; the next bytes are valueLength
            var valueLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: keyLengthBytes, valueLengthBytes);
            return (keyLength, (int)valueLength, RecordInfo.Size + 1 + keyLengthBytes + valueLengthBytes);
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

            WriteVarbyteLengthInWord(ref word, keyLength, precedingNumBytes: 0, keyLengthBytes);
            WriteVarbyteLengthInWord(ref word, valueLength, precedingNumBytes: keyLengthBytes, valueLengthBytes);
            return word;
        }

        /// <summary>
        /// Deconstruct the in-memory inline varbyte indicator word to return keyLengthBytes, valueLengthBytes, and the "has filler" indicator.
        /// This is used to atomically update the varbyte length information so scanning will be consistent.
        /// </summary>
        /// <return>keyLengthBytes, valueLengthBytes, and the "has filler" indicator</return>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe (int keyLength, int valueLength, bool hasFiller) DeconstructInlineVarbyteLengthWord(long word)
        {
            var ptr = (byte*)&word;
            (var keyLengthBytes, var valueLengthBytes, var hasFiller) = DeconstructIndicatorByte(*ptr++);
            Debug.Assert(keyLengthBytes <= 3, "Inline keyLengthBytes limit exceeded");
            Debug.Assert(valueLengthBytes <= 4, "Inline valueLengthBytes limit exceeded");

            var keyLength = ReadVarbyteLengthInWord(*(long*)ptr, precedingNumBytes: 0, keyLengthBytes);
            var valueLength = ReadVarbyteLengthInWord(*(long*)ptr, precedingNumBytes: keyLengthBytes, valueLengthBytes);
            return (keyLength, valueLength, hasFiller);
        }

        /// <summary>
        /// Update the key and value lengths in the in-memory inline varbyte indicator word 
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void UpdateInlineVarbyteLengthWord(long indicatorAddress, int keyLengthBytes, int valueLengthBytes, int valueLength, long hasFillerBit)
        {
            // Mask off the filler bit; we'll reset it on return.
            var word = *(long*)indicatorAddress & ~kHasFillerBitMask;
            WriteVarbyteLengthInWord(ref word, valueLength, precedingNumBytes: keyLengthBytes, valueLengthBytes);
            *(long*)indicatorAddress = word | hasFillerBit;
        }

        /// <summary>
        /// Update the value length high byte in the in-memory inline varbyte indicator word (it is combined with the length that is stored in the ObjectId field data
        /// of a record in the on-disk log).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void UpdateVarbyteValueLengthHighByteInWord(long indicatorAddress, byte valueLength)
        {
            var word = *(long*)indicatorAddress;
            (var keyLengthBytes, var valueLengthBytes, var _ /*hasFiller*/) = DeconstructIndicatorByte(*(byte*)&word);
            WriteVarbyteLengthInWord(ref word, valueLength, precedingNumBytes: keyLengthBytes, valueLengthBytes);
            *(long*)indicatorAddress = word;
        }
    }
}
