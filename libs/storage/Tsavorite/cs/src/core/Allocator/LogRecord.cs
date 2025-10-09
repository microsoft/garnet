// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;
    using static VarbyteLengthUtility;

    /// <summary>The in-memory record on the log. The space is laid out as:
    /// <list type="bullet">
    ///     <item><see cref="RecordInfo"/> header</item>
    ///     <item>Varbyte indicator bytes (including RecordType and Namespace) and lengths; see <see cref="VarbyteLengthUtility"/> header comments for details</item>
    ///     <item>Key data: either the inline data or an int ObjectId for a byte[] that is held in <see cref="ObjectIdMap"/></item>
    ///     <item>Value data: either the inline data or an int ObjectId for a byte[] that is held in <see cref="ObjectIdMap"/></item>
    ///     <item>Optional data (may or may not be present): ETag, Expiration</item>
    ///     <item>Pseudo-optional ObjectLogPosition indicating the position in the object log file, if the record is not fully inline.</item>
    ///     <item>Optional filler length: Extra space in the record, due to record-alignment round-up or Value shrinkage</item>
    /// </list>
    /// This lets us get to the key without intermediate computations having to account for the optional fields.
    /// Some methods have both member and static versions for ease of access and possibly performance gains.
    /// </summary>
    public unsafe partial struct LogRecord : ISourceLogRecord
    {
        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress;

        /// <summary>The ObjectIdMap if this is a record in the object log.</summary>
        internal readonly ObjectIdMap objectIdMap;

        /// <summary>Number of bytes required to store an ETag</summary>
        public const int ETagSize = sizeof(long);
        /// <summary>Invalid ETag, and also the pre-incremented value</summary>
        public const int NoETag = 0;
        /// <summary>Number of bytes required to store an Expiration</summary>
        public const int ExpirationSize = sizeof(long);
        /// <summary>Number of bytes required to the object log position</summary>
        public const int ObjectLogPositionSize = sizeof(long);
        /// <summary>Number of bytes required to store the FillerLen</summary>
        internal const int FillerLengthSize = sizeof(int);

        /// <summary>Address-only ctor. Must only be used for simple record parsing, including inline size calculations.
        /// In particular, if knowledge of whether this is a string or object record is required, or an overflow allocator is needed, this method cannot be used.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress) => this.physicalAddress = physicalAddress;

        internal readonly long IndicatorAddress => physicalAddress + RecordInfo.Size;
        private readonly long RecordTypeAddress => physicalAddress + RecordInfo.Size + 1;
        private readonly long NamespaceAddress => physicalAddress + RecordInfo.Size + 2;

        public readonly byte IndicatorByte => *(byte*)IndicatorAddress;

        /// <summary>This ctor is primarily used for internal record-creation operations for the ObjectAllocator, and is passed to IObjectSessionFunctions callbacks.</summary> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress, ObjectIdMap objectIdMap)
            : this(physicalAddress)
        {
            this.objectIdMap = objectIdMap;
        }

        /// <summary>This ctor is used construct a transient copy of an in-memory LogRecord that remaps the object Ids in <paramref name="physicalAddress"/> to the transient map. 
        /// <paramref name="physicalAddress"/> is must be a pointer to transient memory that contains a copy of the in-memory allocator page's record span, including the objectIds
        /// in Key and Value data. This is used for iteration. Note that the objects are not removed from the allocator-page map, so for iteration they may temporarily be in both.
        /// </summary> 
        /// <remarks>This is ONLY to be done for transient log records, not records on the main log.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static LogRecord CreateRemappedOverTransientMemory(long physicalAddress, ObjectIdMap allocatorMap, ObjectIdMap transientMap)
        {
            var logRecord = new LogRecord(physicalAddress, transientMap);
            if (logRecord.Info.KeyIsOverflow)
            {
                var (length, dataAddress) = GetKeyFieldInfo(logRecord.IndicatorAddress);
                var overflow = allocatorMap.GetOverflowByteArray(*(int*)dataAddress);
                *(int*)dataAddress = transientMap.AllocateAndSet(overflow);
            }

            if (logRecord.Info.ValueIsOverflow)
            {
                var (length, dataAddress) = GetValueFieldInfo(logRecord.IndicatorAddress);
                var overflow = allocatorMap.GetOverflowByteArray(*(int*)dataAddress);
                *(int*)dataAddress = transientMap.AllocateAndSet(overflow);
            }
            else if (logRecord.Info.ValueIsObject)
            {
                var (length, dataAddress) = GetValueFieldInfo(logRecord.IndicatorAddress);
                var heapObj = allocatorMap.GetHeapObject(*(int*)dataAddress);
                *(int*)dataAddress = transientMap.AllocateAndSet(heapObj);
            }
            return logRecord;
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public byte RecordType
        {
            get => *(byte*)RecordTypeAddress;
            set => *(byte*)RecordTypeAddress = value;
        }

        /// <inheritdoc/>
        public byte Namespace
        {
            get => *(byte*)NamespaceAddress;
            set => *(byte*)NamespaceAddress = value;
        }

        /// <inheritdoc/>
        public readonly ObjectIdMap ObjectIdMap => objectIdMap;

        /// <inheritdoc/>
        public readonly bool IsSet => physicalAddress != 0;

        /// <inheritdoc/>
        public readonly ref RecordInfo InfoRef => ref *(RecordInfo*)physicalAddress;

        /// <inheritdoc/>
        public readonly RecordInfo Info => *(RecordInfo*)physicalAddress;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> Key
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var (length, dataAddress) = GetKeyFieldInfo(IndicatorAddress);
                return Info.KeyIsInline ? new((byte*)dataAddress, length) : objectIdMap.GetOverflowByteArray(*(int*)dataAddress).ReadOnlySpan;
            }
        }

        /// <inheritdoc/>
        public readonly bool IsPinnedKey => Info.KeyIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedKeyPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!IsPinnedKey)
                    throw new TsavoriteException("PinnedKeyPointer is unavailable when Key is not pinned; use IsPinnedKey");
                (_ /*length*/, var dataAddress) = GetKeyFieldInfo(IndicatorAddress);
                return (byte*)dataAddress;
            }
        }

        /// <summary>Get and set the <see cref="OverflowByteArray"/> if this Key is not pinned; an exception is thrown if it is a pinned pointer (e.g. to a <see cref="SectorAlignedMemory"/>.</summary>
        public readonly OverflowByteArray KeyOverflow
        {
            get
            {
                if (Info.KeyIsInline)
                    throw new TsavoriteException("get_Overflow is unavailable when Key is inline");
                var (length, dataAddress) = GetKeyFieldInfo(IndicatorAddress);
                return objectIdMap.GetOverflowByteArray(*(int*)dataAddress);
            }
            set
            {

                var (length, dataAddress) = GetKeyFieldInfo(IndicatorAddress);
                if (!Info.KeyIsOverflow || length != ObjectIdMap.ObjectIdSize)
                    throw new TsavoriteException("set_KeyOverflow should only be called when transferring into a new record with KeyIsInline==false and key.Length==ObjectIdSize");
                *(int*)dataAddress = objectIdMap.AllocateAndSet(value);
            }
        }

        /// <inheritdoc/>
        public readonly Span<byte> ValueSpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(!Info.ValueIsObject, "ValueSpan is not valid for Object values");
                var (length, dataAddress) = GetValueFieldInfo(IndicatorAddress);
                return Info.ValueIsInline ? new((byte*)dataAddress, (int)length) : objectIdMap.GetOverflowByteArray(*(int*)dataAddress).Span;
            }
        }

        /// <inheritdoc/>
        public readonly IHeapObject ValueObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(Info.ValueIsObject, "ValueObject is not valid for Span values");
                if (!Info.ValueIsObject)
                    return default;
                var (length, dataAddress) = GetValueFieldInfo(IndicatorAddress);
                return objectIdMap.GetHeapObject(*(int*)dataAddress);
            }
            internal set
            {
                var (length, dataAddress) = GetValueFieldInfo(IndicatorAddress);

                // We cannot verify that value.Length==ObjectIdSize because we have reused the varbyte length as the high byte of the 5-byte length.
                if (!Info.ValueIsObject)
                    throw new TsavoriteException("SetValueObject should only be called by DiskLogRecord or Deserialization with ValueIsObject==true");
                *(int*)dataAddress = objectIdMap.AllocateAndSet(value);

                // We reused the varbyte length as the high byte of the 5-byte length, so reset it now to ObjectIdSize.
                UpdateVarbyteValueLengthByteInWord(IndicatorAddress, ObjectIdMap.ObjectIdSize);
            }
        }

        /// <summary>The span of the entire record, including the ObjectId space if the record has objects.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly ReadOnlySpan<byte> AsReadOnlySpan() => new((byte*)physicalAddress, GetInlineRecordSizes().actualSize);

        /// <inheritdoc/>
        public readonly bool IsPinnedValue => Info.ValueIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedValuePointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!IsPinnedValue)
                    throw new TsavoriteException("PinnedValuePointer is unavailable when Key is not pinned; use IsPinnedKey");
                (_ /*length*/, var dataAddress) = GetValueFieldInfo(IndicatorAddress);
                return (byte*)dataAddress;
            }
        }

        /// <summary>Get and set the <see cref="OverflowByteArray"/> if this Value is not pinned; an exception is thrown if it is a pinned pointer (e.g. to a <see cref="SectorAlignedMemory"/>.</summary>
        public readonly OverflowByteArray ValueOverflow
        {
            get
            {
                if (!Info.ValueIsOverflow)
                    throw new TsavoriteException("get_Overflow is unavailable when Value is not overflow");
                var (length, dataAddress) = GetValueFieldInfo(IndicatorAddress);
                return objectIdMap.GetOverflowByteArray(*(int*)dataAddress);
            }
            set
            {
                var (length, dataAddress) = GetValueFieldInfo(IndicatorAddress);
                if (!Info.ValueIsOverflow || length != ObjectIdMap.ObjectIdSize)
                    throw new TsavoriteException("SetValueObject should only be called when trnasferring into a new record with ValueIsOverflow == true and value.Length==ObjectIdSize");
                *(int*)dataAddress = objectIdMap.AllocateAndSet(value);
            }
        }

        public static int GetOptionalLength(RecordInfo info) => (info.HasETag ? ETagSize : 0) + (info.HasExpiration ? ExpirationSize : 0) + (info.RecordHasObjects ? sizeof(long) : 0);

        /// <inheritdoc/>
        public readonly long ETag => Info.HasETag ? *(long*)GetETagAddress() : NoETag;
        /// <inheritdoc/>
        public readonly long Expiration => Info.HasExpiration ? *(long*)GetExpirationAddress() : 0;

        /// <inheritdoc/>
        public readonly bool IsMemoryLogRecord => true;

        /// <inheritdoc/>
        public readonly unsafe ref LogRecord AsMemoryLogRecordRef() => ref Unsafe.AsRef(in this);

        /// <inheritdoc/>
        public readonly bool IsDiskLogRecord => false;

        /// <inheritdoc/>
        public readonly unsafe ref DiskLogRecord AsDiskLogRecordRef() => throw new InvalidOperationException("Cannot cast a memory LogRecord to a DiskLogRecord.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetRecordFieldInfo()
        {
            var (keyLength, valueLength, _ /*offsetToKeyStart*/) = GetInlineKeyAndValueSizes(IndicatorAddress);
            return new()
            {
                KeySize = keyLength,
                ValueSize = valueLength,
                ValueIsObject = Info.ValueIsObject,
                HasETag = Info.HasETag,
                HasExpiration = Info.HasExpiration
            };
        }
        #endregion // ISourceLogRecord

        /// <summary>
        /// Initialize record for <see cref="ObjectAllocator{TStoreFunctions}"/>--includes Overflow option for Key and Overflow and Object option for Value
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeRecord(ReadOnlySpan<byte> key, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            // Set varbyte lengths
            *(long*)IndicatorAddress = sizeInfo.IndicatorWord;

            var keyAddress = sizeInfo.GetKeyAddress(physicalAddress);
            var keySize = sizeInfo.FieldInfo.KeySize;
            var valueAddress = keyAddress + sizeInfo.InlineKeySize;

            // Serialize Key
            if (sizeInfo.KeyIsInline)
            {
                InfoRef.SetKeyIsInline();
                key.CopyTo(new Span<byte>((byte*)keyAddress, keySize));
            }
            else
            {
                InfoRef.SetKeyIsOverflow();
                var overflow = new OverflowByteArray(key.Length, startOffset: 0, endOffset: 0, zeroInit: false);
                key.CopyTo(overflow.Span);

                // This is record initialization so no object has been allocated for this field yet.
                var objectId = objectIdMap.Allocate();
                *(int*)keyAddress = objectId;
                objectIdMap.Set(objectId, overflow);
            }

            // Initialize Value metadata
            if (sizeInfo.ValueIsInline)
                InfoRef.SetValueIsInline();
            else
            {
                // Unlike for Keys, we do not set the objectId here; we wait for the UMD operation to do that.
                *(int*)valueAddress = ObjectIdMap.InvalidObjectId;
                if (sizeInfo.ValueIsObject)
                {
                    Debug.Assert(sizeInfo.FieldInfo.ValueSize == ObjectIdMap.ObjectIdSize, $"Expected object size ({ObjectIdMap.ObjectIdSize}) for Object ValueSize but was {sizeInfo.FieldInfo.ValueSize}");
                    InfoRef.SetValueIsObject();
                }
                else
                    InfoRef.SetValueIsOverflow();
            }

            // The rest is considered filler
            InitializeFillerLength(in sizeInfo);
        }

        /// <summary>
        /// Initialize record for <see cref="SpanByteAllocator{TStoreFunctions}"/>--does not include Overflow/Object options so is streamlined
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeRecord(ReadOnlySpan<byte> key, in RecordSizeInfo sizeInfo)
        {
            // Set varbyte lengths
            *(long*)IndicatorAddress = sizeInfo.IndicatorWord;

            InfoRef.SetKeyAndValueInline();

            // Serialize Key
            key.CopyTo(new Span<byte>((byte*)sizeInfo.GetKeyAddress(physicalAddress), sizeInfo.FieldInfo.KeySize));

            // The rest is considered filler
            InitializeFillerLength(in sizeInfo);
        }

        /// <summary>A ref to the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref *(RecordInfo*)physicalAddress;

        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;

        internal static ReadOnlySpan<byte> GetInlineKey(long physicalAddress)
        {
            Debug.Assert((*(RecordInfo*)physicalAddress).KeyIsInline, "Key must be inline");
            var (length, dataAddress) = GetKeyFieldInfo(physicalAddress + RecordInfo.Size);
            return new((byte*)dataAddress, length);
        }

        /// <summary>The actual size of the main-log (inline) portion of the record; for in-memory records it does not include filler length.</summary>
        public readonly int ActualRecordSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var (length, dataAddress) = GetValueFieldInfo(IndicatorAddress);
                return (int)(dataAddress - physicalAddress + length + OptionalLength);
            }
        }

        public readonly Span<byte> RecordSpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return Info.RecordIsInline
                    ? new((byte*)physicalAddress, GetInlineRecordSizes().actualSize)
                    : throw new TsavoriteException("RecordSpan is not valid for non-inline records");
            }
        }

        /// <summary>
        /// Asserts that <paramref name="newValueSize"/> is the same size as the value data size in the <see cref="RecordSizeInfo"/> before setting the length.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValueLength(int newValueSize, in RecordSizeInfo sizeInfo, bool zeroInit = false)
        {
            Debug.Assert(newValueSize == sizeInfo.FieldInfo.ValueSize, $"Mismatched value size; expected {sizeInfo.FieldInfo.ValueSize}, actual {newValueSize}");
            return TrySetValueLength(in sizeInfo, zeroInit);
        }

        /// <summary>
        /// Tries to set the length of the value field, with consideration to whether there is also space for the optionals (ETag, Expiration, ObjectLogPosition).
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValueLength(in RecordSizeInfo sizeInfo, bool zeroInit = false) => TrySetValueLength(in sizeInfo, zeroInit, out _ /*valueAddress*/);

        private readonly bool TrySetValueLength(in RecordSizeInfo sizeInfo, bool zeroInit, out long valueAddress)
        {
            // Get the number of bytes in existing key and value lengths.
            var (keyLengthBytes, valueLengthBytes, _ /*hasFiller*/) = DeconstructIndicatorByte(*(byte*)IndicatorAddress);
            var oldInlineValueSize = ReadVarbyteLength(valueLengthBytes, (byte*)(IndicatorAddress + NumIndicatorBytes + keyLengthBytes));
            var newInlineValueSize = sizeInfo.InlineValueSize;

            // Calculate this with our own valueLengthBytes, as the new value may result in a different byte count, which would throw things off.
            // Key does not change, so its size and size byte count remain the same.
            valueAddress = physicalAddress + RecordInfo.Size + NumIndicatorBytes + keyLengthBytes + valueLengthBytes + sizeInfo.InlineKeySize;

            // We don't need to change the size if value size hasn't changed (ignore optionalSize changes; we're not changing the data for that, only shifting them).
            // For this quick check, just check for inline differences; we'll examine overflow size changes and conversions later.
            if (Info.RecordIsInline && sizeInfo.KeyIsInline && sizeInfo.ValueIsInline && oldInlineValueSize == newInlineValueSize)
                return true;

            // It is OK if the value is shrinking so we don't need all the old valueLengthBytes, but we cannot grow past the old valueLengthBytes.
            // (If we are converting from inline to overflow that will already be accounted for because sizeInfo will be set for the ObjectId length.)
            if (sizeInfo.ValueLengthBytes > valueLengthBytes)
                return false;

            // Growth and fillerLen may be negative if shrinking.
            var inlineValueGrowth = (int)(newInlineValueSize - oldInlineValueSize);
            var oldOptionalSize = OptionalLength;
            var newOptionalSize = sizeInfo.OptionalSize;

            var optionalStartAddress = valueAddress + oldInlineValueSize;
            var fillerLenAddress = optionalStartAddress + oldOptionalSize;
            var fillerLen = GetFillerLength(fillerLenAddress);

            // See if we have enough room for the change in Value inline data. Note: This includes things like moving inline data that is less than
            // overflow length into overflow, which frees up inline space > ObjectIdMap.ObjectIsSize. We calculate the inline size required for the
            // new value (including whether it is overflow) and the existing optionals, and success is based on whether that can fit into the allocated
            // record space. We do not change the presence of optionals h ere; we just ensure there is enough for the larger of (current optionals,
            // new optionals) and a later operation will actually read/update the optional(s), including setting/clearing the flag(s).
            if (fillerLen < inlineValueGrowth + (newOptionalSize - oldOptionalSize))
                return false;

            // Update record part 1: Set varbyte value length to the full length of the value, including filler but NOT optionalSize (which is calculated
            // directly from RecordInfo's HasETag and HasExpiration bits, which we do not change here). Scan will now be able to navigate to the end of
            // the record via GetInlineRecordSizes().allocatedSize.
            var oldValueAndFillerSize = (int)(oldInlineValueSize + fillerLen);
            *(long*)IndicatorAddress = CreateIgnoreOptionalsVarbyteWord(*(long*)IndicatorAddress, keyLengthBytes, valueLengthBytes, oldValueAndFillerSize);

            // Update record part 2: Save the optionals if shifting is needed. We can't just shift now because we may be e.g. converting from inline to
            // overflow and they'd overwrite needed data.
            var shiftOptionals = inlineValueGrowth != 0 && oldOptionalSize > 0;
            var optionalFields = new OptionalFieldsShift();
            if (shiftOptionals)
                optionalFields.Save(optionalStartAddress, Info);

            // Update record part 3: Set value length, which includes converting between Inline, Overflow, and Object. This may allocate or free Heap Objects.
            // Evaluate in order of most common (i.e. most perf-critical) cases first.
            if (Info.ValueIsInline && sizeInfo.ValueIsInline)
            {
                // Both are inline, so nothing to do here; we will set the new size into the varbyte indicator word below.
            }
            else if (Info.ValueIsOverflow && sizeInfo.ValueIsOverflow)
            {
                // Both are out-of-line, so reallocate in place if needed; the caller will operate on that space after we return.
                _ = LogField.ReallocateValueOverflow(physicalAddress, valueAddress, in sizeInfo, objectIdMap);
            }
            else if (Info.ValueIsObject && sizeInfo.ValueIsObject)
            {
                // Both are object records, so nothing to change; the caller will operate on the object after we return.
            }
            else
            {
                // Overflow/Object-ness differs and we've verified there is enough space for the change, so convert. The LogField.ConvertTo* functions copy
                // existing data, as we are likely here for IPU or for the initial update going from inline to overflow with Value length == sizeof(IntPtr).
                if (Info.ValueIsInline)
                {
                    if (sizeInfo.ValueIsOverflow)
                    {
                        Debug.Assert(inlineValueGrowth == ObjectIdMap.ObjectIdSize - oldInlineValueSize,
                                    $"ValueGrowth {inlineValueGrowth} does not equal expected {oldInlineValueSize - ObjectIdMap.ObjectIdSize}");
                        _ = LogField.ConvertInlineToOverflow(ref InfoRef, physicalAddress, valueAddress, oldInlineValueSize, in sizeInfo, objectIdMap);
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsObject, "Expected ValueIsObject to be set, pt 1");
                        _ = LogField.ConvertInlineToValueObject(ref InfoRef, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                    }
                }
                else if (Info.ValueIsOverflow)
                {
                    if (sizeInfo.ValueIsInline)
                        _ = LogField.ConvertOverflowToInline(ref InfoRef, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsObject, "Expected ValueIsObject to be set, pt 2");
                        _ = LogField.ConvertOverflowToValueObject(ref InfoRef, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                    }
                }
                else
                {
                    Debug.Assert(Info.ValueIsObject, "Expected ValueIsObject to be set, pt 3");

                    if (sizeInfo.ValueIsInline)
                        _ = LogField.ConvertValueObjectToInline(ref InfoRef, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsOverflow, "Expected ValueIsOverflow to be true");
                        _ = LogField.ConvertValueObjectToOverflow(ref InfoRef, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                    }
                }
            }

            // Update record part 4: Restore optionals to their new location.
            optionalStartAddress += inlineValueGrowth;
            if (shiftOptionals)
                optionalFields.Restore(optionalStartAddress, Info);

            // Update record part 5: Update Filler length in its new location. We don't want to change the optional presence or values here,
            // so don't adjust fillerLen for that; only adjust it for value length change. Because (TrySet|Remove)(ETag|Expiration) update
            // RecordInfo (affecting OptionalSize), we cannot use the same "set valuelength to full value space minus optional size" as we
            // can't change both RecordInfo and the varbyte word atomically. Therefore we must zeroinit from the end of the current "filler
            // space" (either the address, if it was within the less-than-FillerLengthSize bytes at the end of the record, or the end of the
            // int value) if we have shrunk the value.
            fillerLen -= inlineValueGrowth;                                     // optional data is unchanged even if newOptionalSize != oldOptionalSize
            var hasFillerBit = 0L;
            var newFillerLenAddress = optionalStartAddress + oldOptionalSize;   // optional data is unchanged even if newOptionalSize != oldOptionalSize
            var endOfNewFillerSpace = newFillerLenAddress;
            if (fillerLen >= FillerLengthSize)
            {
                *(int*)newFillerLenAddress = fillerLen;
                hasFillerBit = kHasFillerBitMask;
                endOfNewFillerSpace += FillerLengthSize;
            }
            if (inlineValueGrowth < 0)
            {
                // Zeroinit any empty space we opened up by shrinking.
                var endOfOldFillerSpace = fillerLenAddress + (fillerLen >= FillerLengthSize ? FillerLengthSize : 0);
                var clearLength = (int)(endOfOldFillerSpace - endOfNewFillerSpace);
                // If old filler space was < FillerLengthSize and we only shrank by a couple bytes, we may have written FillerLengthSize leaving clearLength <= 0
                if (clearLength > 0)
                    new Span<byte>((byte*)endOfNewFillerSpace, clearLength).Clear();
            }
            else if (zeroInit)
            {
                // Zeroinit any space we grew by. For example, if we grew by one byte we might have a stale fillerLength in that byte.
                new Span<byte>((byte*)(valueAddress + oldInlineValueSize), (int)(newInlineValueSize - oldInlineValueSize)).Clear();
            }

            // Update record part 6: Finally, set varbyte value length to the actual new value length, with filler bit if we have it.
            // We'll be consistent internally and Scan will be able to navigate to the end of the record with GetInlineRecordSizes().allocatedSize.
            *(long*)IndicatorAddress = CreateUpdatedInlineVarbyteLengthWord(*(long*)IndicatorAddress, keyLengthBytes, valueLengthBytes, newInlineValueSize, hasFillerBit);

            Debug.Assert(Info.ValueIsInline == sizeInfo.ValueIsInline, "Final ValueIsInline is inconsistent");
            Debug.Assert(!Info.ValueIsInline || ValueSpan.Length <= sizeInfo.MaxInlineValueSize, $"Inline ValueSpan.Length {ValueSpan.Length} is greater than sizeInfo.MaxInlineValueSpanSize {sizeInfo.MaxInlineValueSize}");
            return true;
        }

        /// <summary>
        /// Set the value span, checking for conversion from inline and for space for optionals (ETag, Expiration).
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValueSpan(ReadOnlySpan<byte> value, in RecordSizeInfo sizeInfo, bool zeroInit = false)
        {
            RecordSizeInfo.AssertValueDataLength(value.Length, in sizeInfo);
            if (!TrySetValueLength(in sizeInfo, zeroInit, out var valueAddress))
                return false;

            var valueSpan = sizeInfo.ValueIsInline ? new((byte*)valueAddress, sizeInfo.FieldInfo.ValueSize) : objectIdMap.GetOverflowByteArray(*(int*)valueAddress).Span;
            value.CopyTo(valueSpan);
            return true;
        }

        /// <summary>
        /// Set the object, checking for conversion from inline and for space for optionals (ETag, Expiration).
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValueObject(IHeapObject value, in RecordSizeInfo sizeInfo) => TrySetValueLength(in sizeInfo) && TrySetValueObject(value);

        /// <summary>
        /// This overload must be called only when it is known the LogRecord's Value is not inline, and there is no need to check
        /// optionals (ETag or Expiration). In that case it is faster to just set the object.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValueObject(IHeapObject value)
        {
            Debug.Assert(Info.ValueIsObject, $"Cannot call this overload of {GetCurrentMethodName()} for non-object Value");

            if (Info.ValueIsInline)
            {
                Debug.Fail($"Cannot call {GetCurrentMethodName()} with no {nameof(RecordSizeInfo)} when the value is inline");
                return false;
            }

            var (valueLength, valueAddress) = GetValueFieldInfo(IndicatorAddress);

            // If there is no object there yet, allocate a slot
            var objectId = *(int*)valueAddress;
            if (objectId == ObjectIdMap.InvalidObjectId)
                objectId = *(int*)valueAddress = objectIdMap.Allocate();

            // Set the new object into the slot
            objectIdMap.Set(objectId, value);
            return true;
        }

        private readonly int ETagLen => Info.HasETag ? ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? ExpirationSize : 0;
        private readonly int ObjectLogPositionLen => Info.RecordHasObjects ? ObjectLogPositionSize : 0;

        /// <summary>A tuple of the total size of the main-log (inline) portion of the record, with and without filler length.</summary>
        public readonly (int actualSize, int allocatedSize) GetInlineRecordSizes()
        {
            if (Info.IsNull)
                return (RecordInfo.Size, RecordInfo.Size);
            var actualSize = ActualRecordSize;
            return (actualSize, actualSize + GetFillerLength());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetOptionalStartAddress()
        {
            var (valueLength, valueAddress) = GetValueFieldInfo(IndicatorAddress);
            return valueAddress + valueLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly ReadOnlySpan<byte> GetOptionalFieldsSpan() => new((byte*)GetOptionalStartAddress(), OptionalLength);

        public readonly int OptionalLength => ETagLen + ExpirationLen + ObjectLogPositionLen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetETagAddress() => GetOptionalStartAddress();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetExpirationAddress() => GetETagAddress() + ETagLen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetFillerLengthAddress() => physicalAddress + ActualRecordSize;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int GetFillerLength() => GetFillerLength(GetFillerLengthAddress());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLength(long fillerLenAddress)
        {
            if (HasFiller(IndicatorByte))
                return *(int*)fillerLenAddress;

            // Filler includes Filler space opened up by removing ETag or Expiration. If there is no Filler, we may still have a couple bytes (< Constants.FillerLenSize)
            // due to RoundUp of record size. Optimize the filler address to avoid additional "if" statements.
            var recSize = (int)(fillerLenAddress - physicalAddress);
            return RoundUp(recSize, Constants.kRecordAlignment) - recSize;
        }

        /// <summary>
        /// Set the filler length (the extra data length, if any).
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeFillerLength(in RecordSizeInfo sizeInfo)
        {
            // This assumes Key and Value lengths have been set. It is called when we have initialized a record, or reinitialized due to revivification etc.
            // Therefore the Optional ETag and Expiration flags have not yet been set in this LogRecord, so their space is considered filler here until we
            // actually set them. However ObjectLogPositionSize is necessary if the ValueIsObject flag is set, so just subtract ETag and Expiration space.
            Debug.Assert(!Info.HasOptionalFields, "Expected no optional flags in RecordInfo in InitializeFillerLength");
            var usedSize = sizeInfo.ActualInlineRecordSize - sizeInfo.ETagSize - sizeInfo.ExpirationSize;
            var fillerSize = sizeInfo.AllocatedInlineRecordSize - usedSize;

            if (fillerSize >= FillerLengthSize)
            {
                SetHasFiller(IndicatorAddress); // must do this first, for zero-init
                *(int*)(physicalAddress + usedSize) = fillerSize;
            }
        }

        /// <summary>
        /// Called during cleanup of a record allocation, before the key was copied.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeForReuse(in RecordSizeInfo sizeInfo)
        {
            Debug.Assert(!Info.HasETag && !Info.HasExpiration, "Record should not have ETag or Expiration here");

            // This assumes the record has just been allocated, so it's at the tail (or very close to it). The Key and Value have not been set.
            // Calculate fillerSize to see if we have enough to set the filler bit, then create and set the indicator word.
            var fillerSize = sizeInfo.AllocatedInlineRecordSize - sizeInfo.ActualInlineRecordSize;
            var hasFillerBit = fillerSize >= FillerLengthSize ? kHasFillerBitMask : 0;
            *(long*)IndicatorAddress = ConstructInlineVarbyteLengthWord(sizeInfo.FieldInfo.KeySize, sizeInfo.FieldInfo.ValueSize, hasFillerBit, out _ /*keyLengthBytes*/, out _ /*valueLengthBytes*/);

            // If we have enough space, set the filler.
            if (fillerSize >= FillerLengthSize)
                *(int*)(physicalAddress + sizeInfo.AllocatedInlineRecordSize) = fillerSize;
        }

        /// <summary>
        /// Set the ETag, checking for space for optionals.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetETag(long eTag)
        {
            if (Info.HasETag)
            {
                *(long*)GetETagAddress() = eTag;
                return true;
            }

            // We're adding an ETag where there wasn't one before.
            const int growth = ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var fillerLen = GetFillerLength(fillerLenAddress) - growth;
            if (fillerLen < 0)
                return false;

            // Start at FillerLen address and back up, for speed.
            var address = fillerLenAddress;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            //      - We must do this here in case there is not enough room for filler after the growth.
            if (HasFiller(IndicatorByte))
                *(int*)address = 0;

            // We don't preserve the ObjectLogPosition field; that's only for serialization. We'll set it to 0 below.
            if (Info.RecordHasObjects)
                address -= ObjectLogPositionSize;

            //  - Preserve Expiration if present; set ETag; re-enter Expiration if present
            var expiration = 0L;
            if (Info.HasExpiration)
            {
                address -= ExpirationSize;
                expiration = *(long*)address;
            }

            *(long*)address = eTag;
            InfoRef.SetHasETag();
            address += ETagSize;

            if (Info.HasExpiration)
            {
                *(long*)address = expiration;   // will be 0 or a valid expiration
                address += ExpirationSize;      // repositions to fillerAddress
            }

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to zero and adjust address for its space.
            if (Info.RecordHasObjects)
            {
                *(long*)address = 0;
                address += ObjectLogPositionSize;
            }

            //  - Set the new (reduced) FillerLength if there is still space for it.
            if (fillerLen >= FillerLengthSize)
                *(int*)address = fillerLen;
            else
                ClearHasFiller(IndicatorAddress);
            return true;
        }

        /// <summary>
        /// Remove the ETag.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool RemoveETag()
        {
            if (!Info.HasETag)
                return true;

            const int growth = -ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var fillerLen = GetFillerLength(fillerLenAddress) - growth; // This will be negative, so adds ETagSize to it

            // Start at FillerLen address and back up, for speed
            var address = fillerLenAddress;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            if (HasFiller(IndicatorByte))
                *(int*)address = 0;

            // We don't preserve the ObjectLogPosition field; that's only for serialization. Just set it to 0 here.
            if (Info.RecordHasObjects)
            {
                address -= ObjectLogPositionSize;
                *(long*)address = 0;
            }

            //  - Move Expiration, if present, up to cover ETag; then clear the ETag bit
            var expiration = 0L;
            var expirationSize = 0;
            if (Info.HasExpiration)
            {
                expirationSize = ExpirationSize;
                address -= expirationSize;
                expiration = *(long*)address;
                *(long*)address = 0L;  // To ensure zero-init
            }

            // Expiration will be either zero or a valid expiration, and we have not changed the info.HasExpiration flag
            address -= ETagSize;
            *(long*)address = expiration;       // will be 0 or a valid expiration
            address += expirationSize;          // repositions to fillerAddress if expirationSize is nonzero
            InfoRef.ClearHasETag();

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to zero and adjust address for its space.
            if (Info.RecordHasObjects)
            {
                *(long*)address = 0;
                address += ObjectLogPositionSize;
            }

            //  - Set the new (increased) FillerLength if there is space for it.
            if (fillerLen >= FillerLengthSize)
            {
                SetHasFiller(IndicatorAddress); // May already be set, but will definitely now be true since we opened up more than FillerLengthSize bytes
                *(int*)address = fillerLen;
            }
            return true;
        }

        /// <summary>
        /// Set the Expiration, checking for space for optionals.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetExpiration(long expiration)
        {
            if (expiration == 0)
                return RemoveExpiration();

            if (Info.HasExpiration)
            {
                *(long*)GetExpirationAddress() = expiration;
                return true;
            }

            // We're adding an Expiration where there wasn't one before.
            const int growth = ExpirationSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var fillerLen = GetFillerLength(fillerLenAddress) - growth;
            if (fillerLen < 0)
                return false;

            // Start at FillerLen address and back up, for speed

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            //      - We must do this here in case there is not enough room for filler after the growth.
            if (HasFiller(IndicatorByte))
                *(int*)fillerLenAddress = 0;

            // We don't preserve the ObjectLogPosition field; that's only for serialization. We'll set it to 0 below.
            if (Info.RecordHasObjects)
                fillerLenAddress -= ObjectLogPositionSize;

            //  - Set Expiration where filler space used to be
            InfoRef.SetHasExpiration();
            *(long*)fillerLenAddress = expiration;
            fillerLenAddress += ExpirationSize;

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to zero and adjust address for its space.
            if (Info.RecordHasObjects)
            {
                *(long*)fillerLenAddress = 0;
                fillerLenAddress += ObjectLogPositionSize;
            }

            //  - Set the new (reduced) FillerLength if there is still space for it.
            if (fillerLen >= FillerLengthSize)
                *(int*)fillerLenAddress = fillerLen;
            else
                ClearHasFiller(IndicatorAddress);
            return true;
        }

        /// <summary>
        /// Remove the expiration
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool RemoveExpiration()
        {
            if (!Info.HasExpiration)
                return true;

            const int growth = -ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var fillerLen = GetFillerLength(fillerLenAddress) - growth; // This will be negative, so adds ExpirationSize to it

            // Start at FillerLen address and back up, for speed
            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            if (HasFiller(IndicatorByte))
                *(int*)fillerLenAddress = 0;

            // We don't preserve the ObjectLogPosition field; that's only for serialization. Just set it to 0 here and remove the space.
            if (Info.RecordHasObjects)
            {
                fillerLenAddress -= ObjectLogPositionSize;
                *(long*)fillerLenAddress = 0;
            }

            //  - Remove Expiration and clear the Expiration bit; this will be the new fillerLenAddress
            fillerLenAddress -= ExpirationSize;
            *(long*)fillerLenAddress = 0;
            InfoRef.ClearHasExpiration();

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to zero and adjust address for its space.
            if (Info.RecordHasObjects)
            {
                *(long*)fillerLenAddress = 0;
                fillerLenAddress += ObjectLogPositionSize;
            }

            //  - Set the new (increased) FillerLength if there is space for it.
            if (fillerLen >= FillerLengthSize)
            {
                SetHasFiller(IndicatorAddress); // May already be set, but will definitely now be true since we opened up more than FillerLengthSize bytes
                *(int*)fillerLenAddress = fillerLen;
            }
            return true;
        }

        /// <summary>
        /// Copy the entire record values: Value and optionals (ETag, Expiration).
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryCopyFrom<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, in RecordSizeInfo sizeInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            // TODOnow: For RENAME, add a key param to this and copy it in. Reflect this in RecordFieldInfo's KeyLength etc. (including whether it's overflow)
            // For now, this assumes the Key has been set and is not changed
            if (srcLogRecord.Info.ValueIsInline)
            {
                if (!TrySetValueLength(in sizeInfo))
                    return false;
                srcLogRecord.ValueSpan.CopyTo(ValueSpan);
            }
            else
            {
                if (srcLogRecord.Info.ValueIsOverflow)
                {
                    Debug.Assert(Info.ValueIsOverflow, "Expected this.Info.ValueIsOverflow to be set already");
                    ValueOverflow = srcLogRecord.ValueOverflow;
                }
                else
                {
                    // TODOnow: make sure Object isn't disposed by the source, to avoid use-after-Dispose. Maybe this (and DiskLogRecord remapping to TransientOIDMap) needs Clone()
                    Debug.Assert(srcLogRecord.ValueObject is not null, "Expected srcLogRecord.ValueObject to be set (or deserialized) already");
                    if (!TrySetValueObject(srcLogRecord.ValueObject, in sizeInfo))
                        return false;
                }
            }
            return TryCopyOptionals(in srcLogRecord, in sizeInfo);
        }

        /// <summary>
        /// Copy the record optional values (ETag, Expiration)
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryCopyOptionals<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, in RecordSizeInfo sizeInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            var srcRecordInfo = srcLogRecord.Info;

            // If the source has optionals and the destination wants them, copy them over
            if (!srcRecordInfo.HasETag || !sizeInfo.FieldInfo.HasETag)
                _ = RemoveETag();
            else if (!TrySetETag(srcLogRecord.ETag))
                return false;

            if (!srcRecordInfo.HasExpiration || !sizeInfo.FieldInfo.HasExpiration)
                _ = RemoveExpiration();
            else if (!TrySetExpiration(srcLogRecord.Expiration))
                return false;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void ClearOptionals()
        {
            _ = RemoveExpiration();
            _ = RemoveETag();
        }

        /// <summary>
        /// Clears any heap-allocated Value: Object or Overflow. Does not clear key (if it is Overflow).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void ClearValueIfHeap(Action<IHeapObject> objectDisposer)
        {
            if (Info.ValueIsInline)
                return;

            if (!Info.KeyIsInline)
            {
                // The key is overflow and will remain that way, so we won't be changing ObjectLogPosition or filler.
                // Just call this directly.
                var (_ /*valueLength*/, valueAddress) = GetValueFieldInfo(IndicatorAddress);
                LogField.ClearObjectIdAndConvertToInline(ref InfoRef, valueAddress, objectIdMap, isKey: false, objectDisposer);
                return;
            }

            // The key is not overflow so we must remove ObjectLogPosition and update filler.
            ClearHeapFields(clearKey: false, objectDisposer);
        }

        /// <summary>
        /// Clears any heap-allocated field, Object or Overflow, in the Value and optionally the Key. If we go from 
        /// <see cref="RecordInfo.RecordIsInline"/> being false to true, then we need to adjust filler as well.
        /// </summary>
        public readonly void ClearHeapFields(bool clearKey, Action<IHeapObject> objectDisposer)
        {
            if (Info.RecordIsInline)
                return;
            var (valueLength, valueAddress) = GetValueFieldInfo(IndicatorAddress);

            // If we're not clearing the key and it's Heap, then we don't need the operations below to change ObjectLogPosition and filler.
            if (!clearKey && !Info.KeyIsInline)
            {
                if (!Info.ValueIsInline)
                    LogField.ClearObjectIdAndConvertToInline(ref InfoRef, valueAddress, objectIdMap, isKey: false, objectDisposer);
                return;
            }

            // First create a varbyte word that uses the value length to be the full space from end of key to end of record.
            var (keyLengthBytes, valueLengthBytes, hasFillerBit) = DeconstructIndicatorByte(*(byte*)IndicatorAddress);
            var allocatedSize = GetInlineRecordSizes().allocatedSize;
            var fullValueLength = (int)(physicalAddress + allocatedSize - valueAddress);
            *(long*)IndicatorAddress = CreateIgnoreOptionalsVarbyteWord(*(long*)IndicatorAddress, keyLengthBytes, valueLengthBytes, fullValueLength);

            // Clear the filler if we have it set. Note that this offset still includes ObjectLogPosition.
            var usedLength = (int)(valueAddress - physicalAddress + valueLength + ETagLen + ExpirationLen);
            if (hasFillerBit)
                *(int*)(physicalAddress + usedLength + ObjectLogPositionSize) = 0;

            // If we're here and the key is overflow we're clearing it.
            if (!Info.KeyIsInline)
            {
                var keyAddress = IndicatorAddress + NumIndicatorBytes + keyLengthBytes + valueLengthBytes;
                LogField.ClearObjectIdAndConvertToInline(ref InfoRef, keyAddress, objectIdMap, isKey: true);
            }
            if (!Info.ValueIsInline)
                LogField.ClearObjectIdAndConvertToInline(ref InfoRef, valueAddress, objectIdMap, isKey: false, objectDisposer);

            // We know we have cleared out the ObjectLogPosition field, so we also know we have at least sizeof(int) bytes of filler.
            // Set the current number of filler bytes.
            *(int*)(physicalAddress + usedLength) = allocatedSize - usedLength;

            // Set varbyte value length to the actual new value length, with filler bit if we have it. valueLength has not changed.
            // We'll be consistent internally and Scan will be able to navigate to the end of the record with GetInlineRecordSizes().allocatedSize.
            *(long*)IndicatorAddress = CreateUpdatedInlineVarbyteLengthWord(*(long*)IndicatorAddress, keyLengthBytes, valueLengthBytes, (int)valueLength, kHasFillerBitMask);
        }

        /// <summary>
        /// For revivification or reuse: prepare the current record to be passed to initial updaters, based upon the sizeInfo's key and value lengths.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PrepareForRevivification(ref RecordSizeInfo sizeInfo, int allocatedSize)
        {
            // We expect that the key and value are inline and there are no optionals, per LogRecord.InitalizeForReuse and LogField.ClearObjectIdAndConvertToInline.
            Debug.Assert(Info.KeyIsInline, "Expected Key to be inline in PrepareForRevivification");
            Debug.Assert(Info.ValueIsInline, "Expected Value to be inline in PrepareForRevivification");
            Debug.Assert(!Info.HasETag && !Info.HasExpiration, "Expected no optionals in PrepareForRevivification");
            Debug.Assert(allocatedSize >= sizeInfo.AllocatedInlineRecordSize, $"Expected allocatedSize {allocatedSize} >= sizeInfo.AllocatedInlineRecordSize {sizeInfo.AllocatedInlineRecordSize}");

            // We can now set the new Key and value lengths directly. This record may have come from revivification so we must use the actual allocated length,
            // not the requested sizeInfo.AllocatedInlineRecordSize, in the calculation of value length bytes. However, we will still use only the requested
            // length as the valueLength, with the rest as filler.

            // Get new record layout part 1: Key bytes
            var keyLength = sizeInfo.InlineKeySize;
            var valueLength = sizeInfo.InlineValueSize;
            Debug.Assert(keyLength + valueLength <= allocatedSize, "Insufficient new record size");   // Should not happen as we passed sizeInfo to BlockAllocate
            var keyLengthBytes = GetByteCount(keyLength);

            // Get new record layout part 2: Full value space, using 1 byte for the estimated value length bytes, to ensure we don't underestimate the value length by a byte or two
            // (there is thus a boundary case where we will thus overestimate the value space by a byte or two and allocate an extra length varbyte, but after 255 that's a tiny
            // percentage of the record size).
            var spaceAfterKey = allocatedSize - RecordInfo.Size - 1 /*indicatorByte*/ - keyLengthBytes - 1 /*valueLengthBytes*/;
            var valueLengthBytes = GetByteCount(spaceAfterKey);

            // Get new record layout part 3: Determine if we have a filler; that is the space after the value and any optionals (eTag, expiration).
            // If we do, set the value of that filler into the proper location before we set the new varbyte info.
            var fillerSpace = spaceAfterKey - valueLength - sizeInfo.OptionalSize;
            long hasFillerBit = 0;
            if (fillerSpace > sizeof(int))
            {
                hasFillerBit = kHasFillerBitMask;
                *(int*)(physicalAddress + allocatedSize - fillerSpace) = fillerSpace;
            }

            // Finally, set the new record layout and update sizeInfo.
            *(long*)IndicatorAddress = ConstructInlineVarbyteLengthWord(keyLengthBytes, keyLength, valueLengthBytes, valueLength, hasFillerBit);
            sizeInfo.AllocatedInlineRecordSize = allocatedSize;
        }

        /// <summary>
        /// Sets the lengths of Overflow Keys and Values and Object values into the disk-image copy of the log record before the main-log page is flushed.
        /// </summary>
        /// <remarks>
        /// IMPORTANT: This is only to be called in the disk image copy of the log record, not in the actual log record itself.
        /// </remarks>
        internal readonly void SetObjectLogRecordStartPositionAndLength(in ObjectLogFilePositionInfo objectLogFilePosition, ulong valueObjectLength)
        {
            if (Info.KeyIsOverflow)
            {
                var (keyLength, keyAddress) = GetKeyFieldInfo(IndicatorAddress);
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)keyAddress);
                *(int*)keyAddress = overflow.Length;
            }

            var (valueLength, valueAddress) = GetValueFieldInfo(IndicatorAddress);
            if (Info.ValueIsOverflow)
            {
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)valueAddress);
                *(int*)valueAddress = overflow.Length;
            }
            else if (Info.ValueIsObject)
            {
                *(uint*)valueAddress = (uint)(valueObjectLength & 0xFFFFFFFF);
                // Update the high byte in the value length metadata (it is combined with the length that is stored in the ObjectId field data of the record).
                UpdateVarbyteValueLengthByteInWord(IndicatorAddress, (byte)((valueObjectLength >> 32) & 0xFF));
            }
            else if (Info.RecordIsInline)   // ValueIsInline is true; if the record is fully inline, we should not be called here
            {
                Debug.Fail("Cannot call SetObjectLogRecordStartPositionAndLength for an inline record");
                return;
            }

            // If we've replaced the varbyte valueLength with the upper byte of valueObjectLength, the usual optional accessors (e.g.GetExpirationAddress())
            // won't work, so we add all the pieces together here.
            *(ulong*)(valueAddress + valueLength + ETagLen + ExpirationLen) = objectLogFilePosition.word;
        }

        /// <summary>
        /// Returns the object log position for the start of the key (if any) and value (if any).
        /// </summary>
        /// <param name="keyLength">Outputs key length; will always be for overflow</param>
        /// <param name="valueObjectLength">Outputs key length; will be for overflow or object</param>
        /// <returns>The object log position for this record</returns>
        internal readonly ulong GetObjectLogRecordStartPositionAndLengths(out int keyLength, out ulong valueObjectLength)
        {
            if (Info.KeyIsOverflow)
            {
                (keyLength, var keyAddress) = GetKeyFieldInfo(IndicatorAddress);
                keyLength = *(int*)keyAddress;
            }
            else // KeyIsInline is true; keyLength will be ignored
                keyLength = 0;

            var (valueLength, valueAddress) = GetValueFieldInfo(IndicatorAddress);
            if (Info.ValueIsOverflow)
                valueObjectLength = (ulong)*(int*)valueAddress;
            else if (Info.ValueIsObject)
            {
                valueObjectLength = *(uint*)valueAddress | (((ulong)valueLength & 0xFF) << 32);
                valueLength = ObjectIdMap.ObjectIdSize; // locally only, restore this. We will restore it for real later, when we read the objects.
            }
            else // ValueIsInline is true; valueLength will be ignored
            {
                valueObjectLength = 0;
                if (Info.RecordIsInline) // If the record is fully inline, we should not be called here
                {
                    Debug.Fail("Cannot call GetObjectLogRecordStartPositionAndLength for an inline record");
                    return 0;
                }
            }

            // If we've replaced the varbyte valueLength with the upper byte of valueObjectLength, the usual optional accessors (e.g.GetExpirationAddress())
            // won't work, so we add all the pieces together here.
            return *(ulong*)(valueAddress + valueLength + ETagLen + ExpirationLen);
        }

        public void Dispose(Action<IHeapObject> objectDisposer)
        {
            if (IsSet)
                ClearHeapFields(clearKey: true, objectDisposer);
        }

        public override readonly string ToString()
        {
            if (physicalAddress == 0)
                return "<empty>";
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = Info.ValueIsObject ? $"obj:{ValueObject}" : ValueSpan.ToShortString(20);
            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}