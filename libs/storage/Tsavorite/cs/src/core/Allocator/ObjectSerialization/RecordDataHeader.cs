// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// The header describing the data layout of the record. The record must be pinned.
    /// The layout is:
    /// <list type="bullet">
    ///     <item>Length indicator byte: flag bits and the number of bytes in KeyLength and RecordLength (3 bits, for 0-4). The layout of this indicator byte is:
    ///     <list type="bullet">
    ///         <item>Indicator bits: 2 bits for any flags we want to add.</item>
    ///         <item>Number of bytes in Filler: 2 bits. This indicates the extra space available in the record if the used record length does not take the full allocated
    ///             space, either on initial creation or due to later value shriking or removal of Optional fields. These two bits are ignored unless RecordInfo.HasFiller
    ///             is set, in which case the value in these two bits must be nonzero, from 1-4. The 2 bits covers 0-3 and are offset by 1, as  they must be nonzero
    ///             if RecordInfo.HasFiller is set, so there are 1-4 Filler length bytes possible. These 1-4 values are interpreted as:
    ///                 <list type="bullet">
    ///                     <item>1-3: this is the number of bytes in the Filler, as there is not enough Filler space for a full int.</item>
    ///                     <item>4: there are enough extra bytes to hold an int, and that int contains the actual number of Filler bytes. This int is the last 4 bytes
    ///                         of the RecordLength; i.e. the last int before <see cref="Constants.kRecordAlignment"/>.</item>
    ///                 </list>
    ///             </item>
    ///         <item>Number of bytes in KeyLength: 2 bits. May be inline length or <see cref="ObjectIdMap.ObjectIdSize"/> if Overflow. The 2 bits covers 0-3 and
    ///             is offset by 1, as there must always be at least one key byte, so there are 1-4 KeyLength bytes possible. KeyLength is immutable for the life of
    ///             a record, but may be changed by revivification.</item>
    ///         <item>Number of bytes in RecordLength: 2 bits. Includes Key and Value length (and other attributes such as optionals) so is nonzero. The 2 bits covers 0-3 and
    ///             is offset by 1, as there must always be one byte. RecordLength is immutable for the life of the log page, including record revivification; even though
    ///             namespace and key lengths and optionals may change, the record length does not.</item>
    ///         </list>
    ///     </item>
    ///     <item>Namespace byte (with encoding indicating if there are many extra namespace bytes; if so, they precede the Key data bytes).</item>
    ///     <item>Record type byte; interpreted by caller</item>
    ///     <item>RecordLength. The entire allocated record size, from the start of the RecordInfo to the end of the allocation; rounded up to Constants.kRecordAlignment.
    ///         It must precede KeyLength so it never has to change location, e.g. if Revivification changes the number of KeyLenght bytes.</item>
    ///     <item>KeyLength. The length of the key</item>
    ///     <item>Namespace extra data, if any</item>
    ///     <item>Key data bytes</item>
    ///     <item>Content, consisting of:
    ///         <list type="bullet">
    ///             <item>Value bytes, if any; there may be none, e.g. creating a Key to server as a Tombstone or as a lock.</item>
    ///             <item>Optional fields, consisting of:</item>
    ///             <list type="bullet">
    ///                 <item>ETag, if present</item>
    ///                 <item>Expiration, if present</item>
    ///                 <item>ObjectLog position, if the Key is Overflow or the Value is Overflow or Object</item>
    ///             </list>
    ///             <item>Filler, if present</item>
    ///         </list>
    ///         We do not store ValueLength explicitly; it is derived from RecordLength minus the sizes of Namespace extra bytes if any, Key, Optionals if any, and Filler.
    ///     </item>
    /// </list>
    ///</summary>
    public unsafe struct RecordDataHeader
    {
#pragma warning disable IDE1006 // Naming Styles: Must begin with uppercase letter
        // When assigning these bits, use the highest # in kReservedBitMask#
        const ulong kReservedBitMask1 = 0 << 7;           // Reserved bit
        const ulong kReservedBitMask2 = 0 << 6;           // Reserved bit

        // The bottom 6 bits are length bytecounts
        /// <summary>
        /// 2 bits (4, 5) for the number of bytes for the Filler Length. There must always be a filler, so we can store the filler size indicator as 2 bits
        /// which when offset by 1 allows for 1-4 bytes. If the value is 4, then there are enough bytes to hold an int, and that int is the last 4
        /// bytes of the record and contains the actual filler length. Otherwise, the value is between 1-3 and is the actual filler length.
        /// </summary>
        const int kFillerLengthIndicatorBitMask = (1 << kFillerLengthIndicatorBits) - 1;
        const int kFillerLengthIndicatorBits = 2;
        const int kFillerLengthIndicatorShift = kRecordLengthIndicatorBits + kKeyLengthBits;

        /// <summary>
        /// 2 bits (2, 3) for the number of bytes for the RecordLength. This must always be nonzero up to 1 &lt;&lt; <see cref="LogSettings.PageSizeBits"/>), which is
        /// in 4 bytes, and 2 bits covers 0-3 which when adding 1 allows for 1-4 bytes.
        /// </summary>
        const int kRecordLengthIndicatorBitMask = (1 << kRecordLengthIndicatorBits) - 1;
        const int kRecordLengthIndicatorBits = 2;
        const int kRecordLengthIndicatorShift = kKeyLengthBits;     // Shift bits in the indicator byte
        const int kRecordLengthShiftInHeader = NumIndicatorBytes * 8; // Shift bytes when storing or retrieving the actual length

        /// <summary>
        /// 2 bits (0, 1) for the number of bytes for the KeyLength. There must always be a key, so we can store the max key size (which is limited by 1 &lt;&lt;
        /// <see cref="kRecordLengthIndicatorBits"/> and thus allows 4 bytes), and 2 bits covers 0-3 which when adding 1 allows for 1-4 bytes.
        /// </summary>
        const int kKeyLengthIndicatorBitMask = (1 << kKeyLengthBits) - 1;
        const int kKeyLengthBits = 2;
        const int kKeyLengthIndicatorShift = 0;
        // keyLengthShiftInHeader is calculated in the code, as it relies on kRecordLengthShiftInHeader
#pragma warning restore IDE1006 // Naming Styles

        /// <summary>The maximum number of key length bytes; <see cref="kKeyLengthBits"/>. Anything over this becomes overflow.</summary>
        internal const int MaxKeyLengthBytes = 1 << kKeyLengthBits;

        /// <summary>The maximum number of value length bytes; see <see cref="kRecordLengthIndicatorBits"/>.</summary>
        internal const int MaxRecordLengthBytes = 1 << kRecordLengthIndicatorBits;

        /// <summary>The minimum number of total data header bytes--NumIndicatorBytes, 1 byte KeyLength, 1 byte RecordLength</summary>
        public const int MinHeaderBytes = NumIndicatorBytes + 2;
        /// <summary>The maximum number of total data header bytes--NumIndicatorBytes, 4 bytes KeyLength, 4 bytes RecordLength</summary>
        internal const int MaxHeaderBytes = NumIndicatorBytes + 8;
        /// <summary>The number of data header indicator bytes; currently 3 for the length indicator, Namespace, RecordType.</summary>
        internal const int NumIndicatorBytes = 3;

        /// <summary>The flag indicating that the namespace is more that sbyte.MaxValue, in which case there is extended data before the key.</summary>
        internal const byte ExtendedNamespaceFlag = 1 << 7;
        /// <summary>Offset of the nameSpace byte in the header.</summary>
        internal const byte NamespaceOffsetInHeader = 1;
        /// <summary>Offset of the recordType byte in the header.</summary>
        internal const byte RecordTypeOffsetInHeader = 2;

        /// <summary>Pointer to the first byte of the header, which is the length indicator byte.</summary>
        internal byte* HeaderPtr;

        /// <inheritdoc/>
        public override readonly string ToString() => ToString("na", "na");

        internal readonly string ToString(string keyString, string valueString)
        {
            if (HeaderPtr == null)
                return "<empty>";
            var (numKeyLengthBytes, numRecordLengthBytes) = DeconstructKVByteLengths(out var headerLength);
            var recordLength = GetRecordLength();
            var fillerLength = GetFillerLength(recordLength);
            var (keyLength, keyAddress) = GetKeyFieldInfo();
            var (valueLength, valueAdress) = GetValueFieldInfo(*RecordInfoPtr, out _ /*keyLength*/, out _ /*numKeyLengthBytes*/, out _ /*numRecordLengthBytes*/);
            var fillerLenStr = (*RecordInfoPtr).HasFiller ? fillerLength.ToString() : "na";

            return $"rec b:{numRecordLengthBytes}/o:na/l:{recordLength}"
                 + $" | key b:{numKeyLengthBytes}/o:{keyAddress - (long)RecordInfoPtr}/l:{keyLength} {keyString}"
                 + $" | val b:na/o:{valueAdress - (long)RecordInfoPtr}/l:{valueLength}, {valueString}"
                 + $" | filLen {fillerLenStr} Namespace b:{NamespaceByte}/x:{ExtendedNamespaceLength}, RecordType {RecordType}";
        }

        internal RecordDataHeader(byte* indicatorPtr) => HeaderPtr = indicatorPtr;

        private readonly RecordInfo* RecordInfoPtr => (RecordInfo*)(HeaderPtr - RecordInfo.Size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetByteCount(long value) => ((sizeof(long) * 8) - BitOperations.LeadingZeroCount((ulong)(value | 1)) + 7) / 8;

        internal readonly int ExtendedNamespaceLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var nameSpace = *(HeaderPtr + NamespaceOffsetInHeader);
                return (nameSpace & ExtendedNamespaceFlag) == 0 ? nameSpace : nameSpace & ~ExtendedNamespaceFlag;
            }
        }

        /// <summary>Get or set the RecordType byte. Throws an exception if out of range or if there is a conflicting specification for extended-length nameSpace.</summary>
        public readonly byte NamespaceByte
        {
            get
            {
                var nameSpace = *(HeaderPtr + NamespaceOffsetInHeader);
                if ((nameSpace & ExtendedNamespaceFlag) != 0)
                    throw new TsavoriteException("Cannot get NamespaceByte when ExtendedNamespaceFlag is set");
                return nameSpace;
            }
            set
            {
                if (value > sbyte.MaxValue)
                    throw new TsavoriteException($"NamespaceByte value {value} exceeds max allowable {sbyte.MaxValue}");
                *(HeaderPtr + NamespaceOffsetInHeader) = value;
            }
        }

        /// <summary>Get or set the RecordType byte</summary>
        public readonly byte RecordType
        {
            get => *(HeaderPtr + RecordTypeOffsetInHeader);
            set => *(HeaderPtr + RecordTypeOffsetInHeader) = value;
        }

        internal readonly int Initialize(ref RecordInfo recordInfo, in RecordSizeInfo sizeInfo, byte recordType, out long keyAddress, out long valueAddress)
        {
            // Format of indicator byte is high->low: <2 bits reserved><2 bits encoded filler length><2 bits key length byte count - 1><2 bits record length byte count - 1>
            var keyLength = sizeInfo.InlineKeySize;
            var valueLength = sizeInfo.InlineValueSize;
            var recordLength = sizeInfo.AllocatedInlineRecordSize;
            var numRecordLengthBytes = GetByteCount(recordLength);
            var numKeyLengthBytes = GetByteCount(keyLength);

            // If this was from revivification, we should have <= keyLengthBytes and == recordLengthBytes. Don't change keyLengthBytes, as that would move the RecordLength
            // field in the header and that might not be an atomic update if it crosses a ulong boundary.
            if (sizeInfo.IsRevivifiedRecord)
            {
                var (revivKeyLenBytes, revivRecLenBytes) = DeconstructKVByteLengths(out _ /*headerLength*/);
                if (numKeyLengthBytes > revivKeyLenBytes || numRecordLengthBytes != revivRecLenBytes)
                    throw new TsavoriteException($"In revivification, cannot exceed previous KeyLengthBytes {revivKeyLenBytes} or change RecordLengthBytes {revivRecLenBytes}");
                numKeyLengthBytes = revivKeyLenBytes;
            }

            *HeaderPtr = (byte)(((numRecordLengthBytes - 1) << kRecordLengthIndicatorShift) | ((numKeyLengthBytes - 1) >> kKeyLengthIndicatorShift));

            // TODO: Pass in the actual Span<byte>Namespace to VarLenMethods to set sizeInfo.FieldInfo.ExtendedNamespaceSize. Here we are only concerned
            // with setting the correct length indicators; LogRecord.InitializeRecord will set the actual data for it. sizeInfo.FieldInfo.ExtendedNamespaceSize
            // has been verified by RecordSizeInfo.CalculateSizes to be within byte range.
            *(HeaderPtr + NamespaceOffsetInHeader) = (byte)(sizeInfo.FieldInfo.ExtendedNamespaceSize > 0 ? (ExtendedNamespaceFlag | (sizeInfo.FieldInfo.ExtendedNamespaceSize & 0x7f)) : 0);
            *(HeaderPtr + RecordTypeOffsetInHeader) = recordType;

            // Calculate and store the filler length, if any. Filler includes any space for optionals that won't have been set this early in the initialization process.
            // If sizeInfo indicates the record is not inline, that won't have been reflected in RecordInfo yet and thus not in optionals, but we need to reserve the
            // ObjectLogPosition space and not let it be part of FillerLength. Do this here after we have initialized the nameSpace byte.
            var headerLength = NumIndicatorBytes + numKeyLengthBytes + numRecordLengthBytes;
            var objectLogPositionLength = sizeInfo.RecordIsInline ? 0 : LogRecord.ObjectLogPositionSize;
            SetFillerLength(ref recordInfo, recordLength, fillerLength: recordLength - RecordInfo.Size - headerLength - ExtendedNamespaceLength - keyLength - valueLength - objectLogPositionLength);

            // Set RecordLength into the header. Header format is (low->high): <Indicator byte><Namespace byte><RecordType byte><RecordLength><KeyLength (may overflow ulong)>.
            // RecordLength will always fit in the header word. Zero out bits before we assign them in case we have non-zeroinitialized space.
            var recordLengthMask = (1UL << (numRecordLengthBytes * 8)) - 1;
            *(ulong*)HeaderPtr = (*(ulong*)HeaderPtr & ~(recordLengthMask << kRecordLengthShiftInHeader)) | (((ulong)recordLength & recordLengthMask) << kRecordLengthShiftInHeader);

            // Set KeyLength into the header. The key length actual bytes may fit along with everything else in the header into a single ulong; otherwise the key length bytes
            // overflow the ulong. To access they key length, offset IndicatorPtr to align to point to the bytes of a ulong with the KeyLength space as high bytes (remembering
            // that in little endian, the high bytes are the "rightmost" bytes of a byte*). If the entire header (including key length) fits in a ulong this will back up into
            // the RecordInfo space; otherwise, we will subtract the negative adjustment and thus "advance" IndicatorPtr. (We don't advance to make KeyLength the low bits,
            // because that could encounter end-of-record if length is zero). Zero out bits before we assign them in case we have non-zeroinitialized space.
            var keyLengthMask = (1UL << (numKeyLengthBytes * 8)) - 1;
            var ptrBackup = sizeof(ulong) - NumIndicatorBytes - numRecordLengthBytes - numKeyLengthBytes;   // If negative, the pointer advances
            var keyLenPtr = (ulong*)(HeaderPtr - ptrBackup);
            var keyLengthShiftInHeader = (sizeof(ulong) - numKeyLengthBytes) * 8;
            *keyLenPtr = (*keyLenPtr & ~(keyLengthMask << keyLengthShiftInHeader)) | (((ulong)keyLength & keyLengthMask) << keyLengthShiftInHeader);

            keyAddress = (long)RecordInfoPtr + GetOffsetToKeyStart(headerLength);
            valueAddress = keyAddress + keyLength;
            return headerLength;
        }

        internal readonly void InitializeForRevivification(ref RecordInfo recordInfo, ref RecordSizeInfo sizeInfo)
        {
            Debug.Assert(recordInfo.Invalid, "Expected record to be Invalid in InitializeForRevivification");
            Debug.Assert(recordInfo.KeyIsInline, "Expected Key to be inline in InitializeForRevivification");
            Debug.Assert(recordInfo.ValueIsInline, "Expected Value to be inline in InitializeForRevivification");
            Debug.Assert(!recordInfo.HasETag && !recordInfo.HasExpiration, "Expected no optionals in InitializeForRevivification");

            // See Initialize() for formatting notes.
            // The keyLengthBytes and RecordLength must be less than or equal to those before revivification (even if we could fit a larger Key, any movement
            // of RecordLength might not be atomic if it crosses the ulong boundary, so we just don't allow it).
            var (numKeyLengthBytes, numRecordLengthBytes) = DeconstructKVByteLengths(out var headerLength);
            var keyLength = GetKeyLength(numKeyLengthBytes, numRecordLengthBytes);
            Debug.Assert(GetByteCount(sizeInfo.InlineKeySize) <= numKeyLengthBytes, "Cannot exceed previous Key size bytes in InitializeForRevivification");
            var recordLength = GetRecordLength(numRecordLengthBytes);
            Debug.Assert(sizeInfo.AllocatedInlineRecordSize <= recordLength, "Cannot exceed previous Record size in InitializeForRevivification");

            // We have no optionals, so just set up with key length and recordLength; no filler.
            recordInfo.ClearHasFiller();
            *HeaderPtr = (byte)(*HeaderPtr & ~(kFillerLengthIndicatorBitMask << kFillerLengthIndicatorShift));

            *(HeaderPtr + NamespaceOffsetInHeader) = 0;
            *(HeaderPtr + RecordTypeOffsetInHeader) = 0;

            // RecordLength is already set and we don't set key here; we wait for Revivification to do that. But we must update the sizeInfo
            // to ensure the AllocatedInlineRecordSize retains recordLength when LogRecord.InitializeRecord is called.
            sizeInfo.AllocatedInlineRecordSize = recordLength;
            sizeInfo.IsRevivifiedRecord = true;
        }

        /// <summary>Set the record length; this is ONLY to be used for temporary copies (e.g. serialization for Migration and Replication).</summary>
        /// <param name="newRecordLength"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetRecordLength(int newRecordLength)
        {
            // This might leave extra bytes in the record length field if the new length uses fewer bytes than the previous length but this is only
            // temporary so it is acceptable.
            var recordLengthMask = (1UL << (DeconstructKVByteLengths(out _ /*headerLength*/).numRecordLengthBytes * 8)) - 1;
            *(ulong*)HeaderPtr = (*(ulong*)HeaderPtr & ~(recordLengthMask << kRecordLengthShiftInHeader)) | (((ulong)newRecordLength & recordLengthMask) << kRecordLengthShiftInHeader);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int numKeyLengthBytes, int numRecordLengthBytes) DeconstructKVByteLengths(out int headerLength)
        {
            var indicator = *HeaderPtr;
            var numRecordLengthBytes = ((indicator >> kRecordLengthIndicatorShift) & kRecordLengthIndicatorBitMask) + 1;    // RecordLength does not allow zero, so add 1
            var numKeyLengthBytes = ((indicator >> kKeyLengthIndicatorShift) & kKeyLengthIndicatorBitMask) + 1;             // KeyLength does not allow zero, so add 1
            headerLength = NumIndicatorBytes + numKeyLengthBytes + numRecordLengthBytes;
            return (numKeyLengthBytes, numRecordLengthBytes);
        }

        /// <summary>Get the offset of the key, relative to the <see cref="RecordInfo"/> start.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetOffsetToKeyStart(int headerLength) => RecordInfo.Size + headerLength + ExtendedNamespaceLength;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLength(RecordInfo recordInfo, int recordLength)
            => recordInfo.HasFiller ? GetFillerLength(recordLength) : 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLength(RecordInfo recordInfo)
            => recordInfo.HasFiller ? GetFillerLength(GetRecordLength(DeconstructKVByteLengths(out _ /*headerLength*/).numRecordLengthBytes)) : 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly int GetFillerLength(int recordLength)
        {
            var fillerLen = (*HeaderPtr >> kFillerLengthIndicatorShift) & kFillerLengthIndicatorBitMask;
            return fillerLen < 3 ? fillerLen + 1 : *(int*)((long)RecordInfoPtr + recordLength - LogRecord.FillerLengthSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void SetFillerLength(ref RecordInfo recordInfo, int recordLength, int fillerLength)
        {
            Debug.Assert(fillerLength >= 0, $"Filler length {fillerLength} must be nonnegative");

            if (fillerLength > 0)
            {
                recordInfo.SetHasFiller();
                if (fillerLength <= 3)
                {
                    // Store only the indicator bits; we don't have space for the full int. Mask out previous bits in the filler space first.
                    *HeaderPtr = (byte)((*HeaderPtr & ~(kFillerLengthIndicatorBitMask << kFillerLengthIndicatorShift)) | ((fillerLength - 1) << kFillerLengthIndicatorShift));
                }
                else
                {
                    // Store the indicator bits as 3, and the filler length in the int at the end of the record. 3 is "all bits set" in the filler space, so we don't need to mask out previous bits there.
                    *HeaderPtr |= 3 << kFillerLengthIndicatorShift;
                    *(int*)((long)RecordInfoPtr + recordLength - LogRecord.FillerLengthSize) = fillerLength;
                }
            }
            else
            {
                recordInfo.ClearHasFiller();
                *HeaderPtr = (byte)(*HeaderPtr & ~(kFillerLengthIndicatorBitMask << kFillerLengthIndicatorShift));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetRecordLength() => GetRecordLength(DeconstructKVByteLengths(out _ /*headerLength*/).numRecordLengthBytes);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetRecordLength(int numRecordLengthBytes)
        {
            // See notes in Initialize() about layout of RecordLength in header and for the "set" side of this--keep them in sync.
            var recordLengthMask = (1UL << (numRecordLengthBytes * 8)) - 1;
            return (int)((*(ulong*)HeaderPtr >> kRecordLengthShiftInHeader) & recordLengthMask);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetKeyLength(int numKeyLengthBytes, int numRecordLengthBytes)
        {
            var keyLengthMask = (1UL << (numKeyLengthBytes * 8)) - 1;
            var ptrBackup = sizeof(ulong) - NumIndicatorBytes - numRecordLengthBytes - numKeyLengthBytes;   // If negative, the pointer advances
            var keyLenPtr = (ulong*)(HeaderPtr - ptrBackup);
            var keyLengthShiftInHeader = (sizeof(ulong) - numKeyLengthBytes) * 8;
            return (int)((*keyLenPtr >> keyLengthShiftInHeader) & keyLengthMask);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int keyLength, int valueLength) GetKVLengths(RecordInfo recordInfo)
            => GetKVLengths(recordInfo, out _ /* recordLength */, out _ /* eTagLen */, out _ /* expirationLen */, out _ /* objectLogPositionLen */, out _ /* fillerLen */);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int keyLength, int valueLength) GetKVLengths(RecordInfo recordInfo, out int recordLength, out int eTagLen, out int expirationLen, out int objectLogPositionLen, out int fillerLen)
        {
            var (numKeyLengthBytes, numRecordLengthBytes) = DeconstructKVByteLengths(out var headerLength);

            // Include changeable fields that are set or cleared by the caller, and the objectLogPosition which is indirectly set by the caller when changing
            // the state of recordInfo.RecordIsInline. Namespace is not included; immutable and conceptually part of the key, it is not part of the record content.
            // Returning these is useful for length-change calculations, and we must retrieve them anyway to determine object size.
            eTagLen = recordInfo.HasETag ? LogRecord.ETagSize : 0;
            expirationLen = recordInfo.HasExpiration ? LogRecord.ExpirationSize : 0;
            objectLogPositionLen = recordInfo.RecordIsInline ? 0 : LogRecord.ObjectLogPositionSize;

            // See note in Initialize about layout of lengths in header
            var keyLength = GetKeyLength(numKeyLengthBytes, numRecordLengthBytes);
            recordLength = GetRecordLength(numRecordLengthBytes);
            fillerLen = GetFillerLength(recordInfo, recordLength);

            // The value length is the recordLength minus everything other than the value.
            return (keyLength, recordLength - RecordInfo.Size - headerLength - ExtendedNamespaceLength - keyLength - recordInfo.GetOptionalSize() - fillerLen);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int keyLength, long keyAddress) GetKeyFieldInfo() => GetKeyFieldInfo(out _ /*numKeyLengthBytes*/, out _ /*numRecordLengthBytes*/);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (int keyLength, long keyAddress) GetKeyFieldInfo(out int numKeyLengthBytes, out int numRecordLengthBytes)
        {
            (numKeyLengthBytes, numRecordLengthBytes) = DeconstructKVByteLengths(out var headerLength);

            // See note in Initialize about layout of lengths in header
            var keyLength = GetKeyLength(numKeyLengthBytes, numRecordLengthBytes);
            var keyAddress = (long)(HeaderPtr + headerLength + ExtendedNamespaceLength);
            return (keyLength, keyAddress);
        }

        /// <summary>
        /// Gets the value field information for an in-memory or on-disk with object size changes to value length restored (objects have been read).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (long valueLength, long valueAddress) GetValueFieldInfo(RecordInfo recordInfo)
            => GetValueFieldInfo(recordInfo, out _ /*keyLength*/, out _ /*numKeyLengthBytes*/, out _ /*numRecordLengthBytes*/);

        /// <summary>
        /// Gets the value field information for an in-memory or on-disk with object size changes to value length restored (objects have been read).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly (long valueLength, long valueAddress) GetValueFieldInfo(RecordInfo recordInfo, out int keyLength, out int numKeyLengthBytes, out int numRecordLengthBytes)
        {
            (keyLength, var keyAddress) = GetKeyFieldInfo(out numKeyLengthBytes, out numRecordLengthBytes);
            var headerLength = NumIndicatorBytes + numKeyLengthBytes + numRecordLengthBytes;

            var recordLength = GetRecordLength(numRecordLengthBytes);
            var fillerLength = GetFillerLength(recordInfo, recordLength);

            // The value length is the recordLength minus everything other than the value.
            var valueLength = recordLength - RecordInfo.Size - headerLength - ExtendedNamespaceLength - keyLength - recordInfo.GetOptionalSize() - fillerLength;

            // Move past the key and value length bytes and the key data to the start of the value data
            return (valueLength, keyAddress + keyLength);
        }

        internal readonly int GetAllocatedRecordSize() => GetRecordLength(DeconstructKVByteLengths(out _ /*headerLength*/).numRecordLengthBytes);

        internal readonly int GetActualRecordSize(RecordInfo recordInfo)
        {
            var recordLength = GetRecordLength(DeconstructKVByteLengths(out _ /*headerLength*/).numRecordLengthBytes);
            return recordLength - GetFillerLength(recordInfo, recordLength);
        }
    }
}