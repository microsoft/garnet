// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Utilities for varlen bytes: one indicator byte identifying the number of key and value bytes. The layout of this indicator byte is:
    /// <list type="bullet">
    ///     <item>Indicators: flags, such as filler and ignore optionals</item>
    ///     <item>Number of bytes in key length; may be inline length or <see cref="ObjectIdMap.ObjectIdSize"/> if Overflow. Max is </item>
    ///     <item>Number of bytes in value length; may be inline length or <see cref="ObjectIdMap.ObjectIdSize"/> if Overflow or Object</item>
    /// </list>
    /// This is followed by the actual key length and value length, which may be inline length or <see cref="ObjectIdMap.ObjectIdSize"/> if Overflow
    /// or object. For in=memory objects, the max inline key size is 16MB to remain in 3 bytes, and the max inline value size is int.MaxValue
    /// which is 4 bytes, so the total is 8 bytes which can be atomically updated.
    /// </summary>
    public static unsafe class VarbyteLengthUtility
    {
#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter
        const long kReservedBitMask = 0 << 7;           // Reserved bit

        /// <summary> If this is set, then we have extra length in the record after any optional fields. We may have some extra length that is
        /// less than the size of an int even if this bit is not set, due to record-alignment padding.</summary>
        internal const long kHasFillerBitMask = 1 << 5;

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

        /// <summary>The minimum number of length metadata bytes--NumIndicatorBytes, 1 byte key length, 1 byte value length</summary>
        public const int MinLengthMetadataBytes = NumIndicatorBytes + 2;
        /// <summary>The maximum number of length metadata bytes--NumIndicatorBytes, 4 bytes key length, 7 bytes value length</summary>
        internal const int MaxLengthMetadataBytes = NumIndicatorBytes + 11;
        /// <summary>The number of indicator bytes; currently 1 for the length indicator.</summary>
        internal const int NumIndicatorBytes = 3;

        /// <summary>The maximum number of key length bytes in the in-memory single-long word representation. We use zero-based sizes and add 1, so
        /// 1 bit allows us to specify 1 or 2 bytes; we max at 2, or <see cref="LogSettings.MaxInlineKeySizeBits"/>. Anything over this becomes overflow.</summary>
        internal const int MaxKeyLengthBytes = 2;
        /// <summary>The maximum number of value length bytes in the in-memory single-long word representation. We use zero-based sizes and add 1, so
        /// 2 bits allows us to specify 1 to 4 bytes; we max at 3, or <see cref="LogSettings.MaxInlineValueSizeBits"/>. Anything over this becomes overflow.</summary>
        internal const int MaxValueLengthBytes = 3;

#if false
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (int length, long dataAddress) GetKeyFieldInfo(long indicatorAddress)
        {
            var (keyLengthBytes, valueLengthBytes, _ /*hasFiller*/) = DeconstructIndicatorByte(*(byte*)indicatorAddress);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: 0, keyLengthBytes);

            // Move past the key and value length bytes to the start of the key data
            return (keyLength, indicatorAddress + NumIndicatorBytes + keyLengthBytes + valueLengthBytes);
        }

        /// <summary>
        /// Gets the value field information for an in-memory or on-disk with object size changes to value length restored (objects have been read).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (long length, long dataAddress) GetValueFieldInfo(long indicatorAddress)
        {
            var (keyLengthBytes, valueLengthBytes, _ /*hasFiller*/) = DeconstructIndicatorByte(*(byte*)indicatorAddress);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: 0, keyLengthBytes);

            // Move past the key length bytes; the next bytes are valueLength
            var valueLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: keyLengthBytes, valueLengthBytes);

            // Move past the key and value length bytes and the key data to the start of the value data
            return (valueLength, indicatorAddress + NumIndicatorBytes + keyLengthBytes + valueLengthBytes + keyLength);
        }

        /// <summary>Write var-length bytes into the given word. Used for in-memory <see cref="LogRecord"/> and limited to 3-byte keys and 4-byte values
        ///     which combine with the indicator byte to fit into a 'long'. The shift operations are faster than the pointer-based alternative implementation
        ///     used for disk-image generation, which has the data expanded inline so may have 4-byte keys and 8-byte values.</summary>
        /// <param name="word">The word being updated</param>
        /// <param name="targetValue">The target value being set into the word (key or value length)</param>
        /// <param name="precedingNumBytes">If we are setting the value, this is the number of bytes in the key; otherwise it is 0</param>
        /// <param name="targetNumBytes">The number of bytes in the target (key or value)</param>
        /// <remark>This assumes little-endian; thus, the indicator byte (containing flags, keyLengthBytes, valueLengthBytes) is the low byte of the word,
        ///     then keyLength, then valueLength, in ascending address order</remark>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void WriteVarbyteLengthInWord(ref long word, int targetValue, int precedingNumBytes, int targetNumBytes)
        {
            // This is ascending order, so we will shift over the lower-order bytes.
            var shift = (NumIndicatorBytes + precedingNumBytes) * 8;
            var targetMask = (1L << (targetNumBytes * 8)) - 1;

            // Mask off the target area of the word (i.e. keep everything except where we will shift-OR the target into.
            word &= ~(targetMask << shift);

            // Now mask the target value to include only what we are going to keep, then shift that into the target area of the word.
            word |= (targetValue & targetMask) << shift;
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
            => (int)((word >> ((NumIndicatorBytes + precedingNumBytes) * 8)) & ((1L << (targetNumBytes * 8)) - 1));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte ConstructIndicatorByte(int keyByteCount, int valueByteCount)
        {
            return (byte)(
                  ((long)(keyByteCount - 1) << 3)       // Shift key into position; subtract 1 for 0-based
                | (long)(valueByteCount - 1));          // Value does not need to be shifted; subtract 1 for 0-based
        }

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

        internal static int GetKeyLength(int numBytes, byte* ptrToFirstByte) => (int)ReadVarbyteLength(numBytes, ptrToFirstByte);

        internal static long GetValueLength(int numBytes, byte* ptrToFirstByte) => ReadVarbyteLength(numBytes, ptrToFirstByte);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (int keyLengthBytes, int valueLengthBytes, bool hasFiller) DeconstructIndicatorByte(byte indicatorByte)
        {
            var keyLengthBytes = (int)((indicatorByte & kKeyLengthBitMask) >> 3) + 1;   // add 1 due to 0-based
            var valueLengthBytes = (int)(indicatorByte & kValueLengthBitMask) + 1;      // add 1 due to 0-based
            var hasFiller = (indicatorByte & kHasFillerBitMask) != 0;
            return (keyLengthBytes, valueLengthBytes, hasFiller);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetByteCount(long value) => ((sizeof(long) * 8) - BitOperations.LeadingZeroCount((ulong)(value | 1)) + 7) / 8;

        /// <summary>
        /// Get the value data pointer, as well as the pointer to length, length, and number of length bytes. This is to support in-place updating.
        /// </summary>
        /// <returns>The value data pointer</returns>
        internal static byte* GetFieldPtr(long indicatorAddress, bool isKey, out byte* lengthPtr, out int lengthBytes, out long length)
        {
            var ptr = (byte*)indicatorAddress;
            var (keyLengthBytes, valueLengthBytes, _ /*hasFiller*/) = DeconstructIndicatorByte(*ptr);
            ptr += NumIndicatorBytes;

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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool HasFiller(byte indicatorByte) => (indicatorByte & kHasFillerBitMask) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetHasFiller(long indicatorAddress) => *(byte*)indicatorAddress = (byte)(*(byte*)indicatorAddress | kHasFillerBitMask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ClearHasFiller(long indicatorAddress) => *(byte*)indicatorAddress = (byte)(*(byte*)indicatorAddress & ~kHasFillerBitMask);

        /// <summary>
        /// Construct the in-memory inline varbyte indicator word consisting of keyLengthBytes, valueLengthBytes, and a "has filler" indicator.
        /// This is used to atomically update the varbyte length information so scanning will be consistent.
        /// </summary>
        /// <param name="keyLengthBytes">Number of bytes in the key length</param>
        /// <param name="keyLength">The inline length of the key</param>
        /// <param name="valueLengthBytes">Number of bytes in the value length</param>
        /// <param name="valueLength">The inline length of the value</param>
        /// <param name="flagBits"><see cref="kHasFillerBitMask"/>, <see cref="kIgnoreOptionalsBitMask"/>, or 0</param>
        /// <returns></returns>
        internal static unsafe long ConstructInlineVarbyteLengthWord(int keyLengthBytes, int keyLength, int valueLengthBytes, int valueLength, long flagBits)
        {
            if (valueLengthBytes > MaxValueLengthBytes)
                throw new ArgumentOutOfRangeException(nameof(valueLengthBytes), $"Value length bytes {valueLengthBytes} exceeds max {MaxValueLengthBytes}");
            var word = (long)0;
            var ptr = (byte*)&word;
            *ptr = (byte)(ConstructIndicatorByte(keyLengthBytes, valueLengthBytes) | flagBits);
            ptr += NumIndicatorBytes;

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
            Debug.Assert(keyLengthBytes <= MaxKeyLengthBytes, "Inline keyLengthBytes limit exceeded");
            Debug.Assert(valueLengthBytes <= MaxValueLengthBytes, "Inline valueLengthBytes limit exceeded");

            var keyLength = ReadVarbyteLengthInWord(*(long*)ptr, precedingNumBytes: 0, keyLengthBytes);
            var valueLength = ReadVarbyteLengthInWord(*(long*)ptr, precedingNumBytes: keyLengthBytes, valueLengthBytes);
            return (keyLength, valueLength, hasFiller);
        }

        /// <summary>
        /// Construct the in-memory inline varbyte indicator word consisting of keyLengthBytes, valueLengthBytes, and a "has filler" indicator.
        /// This is used to atomically update the varbyte length information so scanning will be consistent.
        /// </summary>
        /// <param name="keyLength">The inline length of the key</param>
        /// <param name="valueLength">The inline length of the value</param>
        /// <param name="flagBits">Either kHasFillerBitMask if we have set a filler length into the in-memory record, or 0</param>
        /// <param name="keyLengthBytes">Receives the number of bytes in the key length</param>
        /// <param name="valueLengthBytes">Receives the number of bytes in the value length</param>
        /// <returns></returns>
        internal static unsafe long ConstructInlineVarbyteLengthWord(int keyLength, int valueLength, long flagBits, out int keyLengthBytes, out int valueLengthBytes)
        {
            keyLengthBytes = GetByteCount(keyLength);
            valueLengthBytes = GetByteCount(valueLength);
            return ConstructInlineVarbyteLengthWord(keyLengthBytes, keyLength, valueLengthBytes, valueLength, flagBits);
        }

        /// <summary>
        /// Get the value data pointer, as well as the pointer to length, length, and number of length bytes. This is to support in-place updating.
        /// </summary>
        /// <returns>The value data pointer</returns>
        internal static (int keyLength, int valueLength, int offsetToKeyStart) GetInlineKeyAndValueSizes(long indicatorAddress)
        {
            var ptr = (byte*)indicatorAddress;
            var (keyLengthBytes, valueLengthBytes, _ /*hasFiller*/) = DeconstructIndicatorByte(*ptr);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: 0, keyLengthBytes);

            // Move past the key bytes; the next bytes are valueLength
            var valueLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: keyLengthBytes, valueLengthBytes);
            return (keyLength, valueLength, RecordInfo.Size + NumIndicatorBytes + keyLengthBytes + valueLengthBytes);
        }

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

        /// <summary>
        /// Gets the value field information for an in-memory or on-disk with object size changes to value length not yet restored (objects have not yet been read).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static (long length, long dataAddress) GetValueFieldInfoWithUnreadObjects(long indicatorAddress)
        {
            var (keyLengthBytes, valueLengthBytes, _ /*hasFiller*/) = DeconstructIndicatorByte(*(byte*)indicatorAddress);

            // Move past the indicator byte; the next bytes are key length
            var keyLength = ReadVarbyteLengthInWord(*(long*)indicatorAddress, precedingNumBytes: 0, keyLengthBytes);

            // We know the valueLength is the size of the object Id.
            // Move past the key and value length bytes and the key data to the start of the value data
            return (ObjectIdMap.ObjectIdSize, indicatorAddress + NumIndicatorBytes + keyLengthBytes + valueLengthBytes + keyLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte ConstructIndicatorByte(int keyLength, long valueLength, out int keyByteCount, out int valueByteCount)
        {
            keyByteCount = GetByteCount(keyLength);
            valueByteCount = GetByteCount(valueLength);
            return ConstructIndicatorByte(keyByteCount, valueByteCount);
        }
#endif
    }
}