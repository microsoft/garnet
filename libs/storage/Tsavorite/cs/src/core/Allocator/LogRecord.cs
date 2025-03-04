// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.OverflowAllocator;
using static Tsavorite.core.Utility;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>A non-Generic form of the in-memory <see cref="LogRecord{TValue}"/> that provides access to <see cref="RecordInfo"/> and Key only.
    /// Useful in quick recordInfo-testing and key-matching operations</summary>
    public unsafe partial struct LogRecord(long physicalAddress)
    {
        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress = physicalAddress;

        /// <summary>Number of bytes required to store an ETag</summary>
        public const int ETagSize = sizeof(long);
        /// <summary>Number of bytes required to store an Expiration</summary>
        public const int ExpirationSize = sizeof(long);
        /// <summary>Number of bytes required to store the FillerLen</summary>
        internal const int FillerLengthSize = sizeof(int);

        /// <summary>A ref to the record header</summary>
        public readonly ref RecordInfo InfoRef => ref GetInfoRef(physicalAddress);
        /// <summary>Fast access returning a copy of the record header</summary>
        public readonly RecordInfo Info => GetInfoRef(physicalAddress);
        /// <summary>Fast access to the record Key</summary>
        public readonly SpanByte Key => GetKey(physicalAddress);

        /// <summary>A ref to the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);

        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;

        /// <summary>The address of the key</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetKeyAddress(long physicalAddress) => physicalAddress + RecordInfo.GetLength();

        /// <summary>The key Span</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SpanByte GetKey(long physicalAddress) => SpanField.AsSpanByte(GetKeyAddress(physicalAddress), GetInfo(physicalAddress).KeyIsInline);

        // All Key information is independent of TValue, which means we can navigate to the ValueAddress since we know KeyAddress and size.
        // However, any further Value operations requires knowledge of TValue, including valueIsObject.

        /// <summary>The address of the value.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetValueAddress(long physicalAddress)
        {
            var keyAddress = GetKeyAddress(physicalAddress);
            return keyAddress + SpanField.GetInlineTotalSizeOfKey(keyAddress, GetInfo(physicalAddress).KeyIsInline);
        }
    }

    /// <summary>The in-memory record on the log: header, key, value, and optional fields
    ///     until some other things have been done that will allow clean separation.
    /// </summary>
    /// <remarks>The space is laid out as:
    ///     <list>
    ///     <item>[RecordInfo][SpanByte key][Value (SpanByte or ObjectId)][ETag?][Expiration?][FillerLen?][Filler bytes]
    ///         <br>Where Value Span is one of:</br>
    ///         <list type="bullet">
    ///             <item>ObjectId: If this is a object record, this is the ID into the <see cref="ObjectIdMap"/></item>
    ///             <item>Span data: See <see cref="SpanField"/> for details</item>
    ///         </list>
    ///     </item>
    ///     </list>
    /// This lets us get to the key without intermediate computations to account for the optional fields.
    /// Some methods have both member and static versions for ease of access and possibly performance gains.
    /// </remarks>
    public unsafe struct LogRecord<TValue> : ISourceLogRecord<TValue>
    {
        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress;

        /// <summary>The overflow allocator for Keys and, if SpanByteAllocator, Values.</summary>
        internal readonly OverflowAllocator overflowAllocator;

        /// <summary>The ObjectIdMap if this is a record in the object log.</summary>
        internal readonly ObjectIdMap<TValue> objectIdMap;

        /// <summary>Address-only ctor. Must only be used for simple record parsing, including inline size calculations.
        /// In particular, if knowledge of whether this is a string or object record is required, or an overflow allocator is needed, this method cannot be used.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress) => this.physicalAddress = physicalAddress;

        /// <summary>This ctor is primarily used for internal record-creation operations for the ObjectAllocator, and is passed to IObjectSessionFunctions callbacks.</summary> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress, OverflowAllocator overflowAllocator, ObjectIdMap<TValue> objectIdMap)
            : this(physicalAddress)
        {
            this.overflowAllocator = overflowAllocator;
            this.objectIdMap = objectIdMap;
        }

        /// <summary>This ctor is primarily used for internal record-creation operations for the ObjectAllocator, and is passed to IObjectSessionFunctions callbacks.</summary> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress, OverflowAllocator overflowAllocator)
            : this(physicalAddress)
            => this.overflowAllocator = overflowAllocator;

        #region ISourceLogRecord
        /// <inheritdoc/>
        public readonly bool ValueIsObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(!Info.ValueIsObject || objectIdMap is not null, $"Mismatch between Info.ValueIsObject ({Info.ValueIsObject}) and objectIdMap != null {objectIdMap is not null}");
                return Info.ValueIsObject;
            }
        }

        /// <inheritdoc/>
        public readonly bool IsSet => physicalAddress != 0;
        /// <inheritdoc/>
        public readonly ref RecordInfo InfoRef => ref GetInfoRef(physicalAddress);
        /// <inheritdoc/>
        public readonly RecordInfo Info => GetInfoRef(physicalAddress);
        /// <inheritdoc/>
        public readonly SpanByte Key => GetKey(physicalAddress);

        /// <inheritdoc/>
        public readonly SpanByte ValueSpan
        {
            get
            {
                Debug.Assert(!ValueIsObject || Info.ValueIsInline, "ValueSpan is not valid for non-inline Object log records");
                return SpanField.AsSpanByte(ValueAddress, Info.ValueIsInline);
            }
        }

        /// <inheritdoc/>
        public readonly TValue ValueObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(ValueIsObject, "ValueObject is not valid for String log records");
                Debug.Assert(!Info.ValueIsInline, "ValueObject is not valid for inline Object values");
                return (Info.ValueIsInline || *ValueObjectIdAddress == ObjectIdMap.InvalidObjectId) ? default : objectIdMap.Get(ValueObjectId);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly TValue GetReadOnlyValue()
        {
            if (ValueIsObject)
                return objectIdMap.Get(ValueObjectId);

            var sb = ValueSpan;
            return Unsafe.As<SpanByte, TValue>(ref sb);
        }

        /// <inheritdoc/>
        public readonly long ETag => Info.HasETag ? *(long*)GetETagAddress() : 0;
        /// <inheritdoc/>
        public readonly long Expiration => Info.HasExpiration ? *(long*)GetExpirationAddress() : 0;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': not doing so because it modifies internal state
        public void ClearValueObject(Action<TValue> disposer)
#pragma warning restore IDE0251
        {
            Debug.Assert(ValueIsObject, "ClearValueObject() is not valid for String log records");
            if (ValueIsObject)
            {
                objectIdMap.ClearAt(ValueObjectId, disposer);
                if (!Info.ValueIsInline)
                    *ValueObjectIdAddress = ObjectIdMap.InvalidObjectId;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord<TValue> AsLogRecord() => this;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetRecordFieldInfo() => new()
        {
            KeyDataSize = SpanField.GetInlineDataSizeOfKey(KeyAddress, Info.KeyIsInline),
            ValueDataSize = SpanField.GetInlineDataSizeOfValue(ValueAddress, ValueIsObject, Info.ValueIsInline),
            ValueIsObject = ValueIsObject,
            HasETag = Info.HasETag,
            HasExpiration = Info.HasExpiration
        };
        #endregion // ISourceLogRecord

        /// <summary>A ref to the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref LogRecord.GetInfoRef(physicalAddress);

        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => LogRecord.GetInfo(physicalAddress);

        /// <summary>The address of the key</summary>
        internal readonly long KeyAddress => LogRecord.GetKeyAddress(physicalAddress);
        /// <summary>The address of the key</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetKeyAddress(long physicalAddress) => LogRecord.GetKeyAddress(physicalAddress);

        /// <summary>A <see cref="SpanField"/> representing the record Key</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SpanByte GetKey(long physicalAddress) => LogRecord.GetKey(physicalAddress);

        /// <summary>The address of the value</summary>
        internal readonly long ValueAddress => LogRecord.GetValueAddress(physicalAddress);

        internal readonly int* ValueObjectIdAddress => (int*)ValueAddress;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int* GetValueObjectIdAddress(long physicalAddress) => (int*)LogRecord.GetValueAddress(physicalAddress);

        /// <summary>The value object id (index into the object values array)</summary>
        internal readonly int ValueObjectId
        {
            get
            {
                Debug.Assert(ValueIsObject, "Cannot get ValueObjectId for String LogRecord");
                Debug.Assert(!Info.ValueIsInline, "Cannot get ValueObjectId for inline values");
                return Info.ValueIsInline ? ObjectIdMap.InvalidObjectId : *ValueObjectIdAddress;
            }
        }

        /// <summary>The actual size of the main-log (inline) portion of the record; for in-memory records it does not include filler length.</summary>
        public readonly int ActualRecordSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                var valueAddress = ValueAddress;
                var valueSize = SpanField.GetInlineTotalSizeOfValue(valueAddress, ValueIsObject, GetInfo(physicalAddress).ValueIsInline);
                return (int)(valueAddress - physicalAddress + valueSize + OptionalSize);
            }
        }

        /// <summary>
        /// Asserts that <paramref name="newValueSize"/> is the same size as the value data size in the <see cref="RecordSizeInfo"/> before setting the length.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueLength(int newValueSize, ref RecordSizeInfo sizeInfo)
        {
            Debug.Assert(newValueSize == sizeInfo.FieldInfo.ValueDataSize, $"Mismatched value size; expected {sizeInfo.FieldInfo.ValueDataSize}, actual {newValueSize}");
            return TrySetValueLength(ref sizeInfo);
        }

        /// <summary>
        /// Tries to set the length of the value field, with consideration to whether there is also space for the optionals (ETag and Expiration).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueLength(ref RecordSizeInfo sizeInfo)
        {
            var valueAddress = ValueAddress;
            var oldInlineValueSize = SpanField.GetInlineTotalSizeOfValue(valueAddress, ValueIsObject, Info.ValueIsInline);
            var newInlineValueSize = sizeInfo.InlineTotalValueSize;

            // Growth and fillerLen may be negative if shrinking.
            var inlineValueGrowth = newInlineValueSize - oldInlineValueSize;
            var newOptionalSize = sizeInfo.OptionalSize;
            var oldOptionalSize = OptionalSize;
            var inlineTotalGrowth = inlineValueGrowth + (newOptionalSize - oldOptionalSize);

            var optionalStartAddress = valueAddress + oldInlineValueSize;
            var fillerLenAddress = optionalStartAddress + oldOptionalSize;
            var fillerLen = GetFillerLength(fillerLenAddress);

            // See if we have enough room for the inline data. Note: We don't consider here things like moving inline data that is less than
            // overflow length into overflow to free up inline space; we calculate the inline size required for the new value (including whether
            // it is overflow) and optionals, and success is based on whether that can fit into the allocated record space.
            if (fillerLen < inlineTotalGrowth)
                return false;

            // We know we have enough space for the changed value *and* for whatever changes are coming in for the optionals. However, we aren't
            // changing the optionals here, so don't adjust fillerLen for them; only adjust it for value length change.
            fillerLen -= inlineValueGrowth;

            int newDataLength = sizeInfo.FieldInfo.ValueDataSize;

            // See if we need to shift/zero the optionals and Filler.
            var shiftOptionals = inlineValueGrowth != 0;

            // We have enough space to handle the changed value size, including changing between inline and out-of-line or vice-versa, and the new
            // optional space. But we do not count the change in optional space here; we just ensure there is enough, and a later operation will
            // actually add/remove/update the optional(s), including setting the flag. So only adjust offsets for valueGrowth, not totalGrowth.

            // Evaluate in order of most common (i.e. most perf-critical) cases first.
            if (Info.ValueIsInline && sizeInfo.ValueIsInline)
            {
                // Both are inline, so resize in place (and adjust filler) if needed.
                if (inlineValueGrowth != 0)
                {
                    var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                    _ = SpanField.AdjustInlineLength(ValueAddress, newDataLength);
                    optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                }
                goto Done;
            }
            else if (Info.ValueIsOverflow && sizeInfo.ValueIsOverflow)
            {
                // Both are out-of-line, so reallocate in place if needed. Object records will do what they need to after this call returns;
                // we're only here to set up inline lengths, and that hasn't changed here.
                if (newDataLength != BlockHeader.GetUserSize(SpanField.GetOverflowPointer(valueAddress)))
                    _ = SpanField.ReallocateOverflow(valueAddress, newDataLength, overflowAllocator);
                goto Done;
            }
            else if (Info.ValueIsObject && sizeInfo.ValueIsObject)
            {
                // Both are object records, so nothing to change.
                goto Done;
            }
            else
            {
                // Overflow/Object-ness differs and we've verified there is enough space for the change, so convert. The SpanField.ConvertTo* functions copy
                // existing data, as we are likely here for IPU or for the initial update going from inline to overflow with Value length == sizeof(IntPtr).
                if (Info.ValueIsInline)
                {
                    if (sizeInfo.ValueIsOverflow)
                    {
                        // Convert from inline to overflow.
                        Debug.Assert(inlineValueGrowth == SpanField.OverflowInlineSize - oldInlineValueSize,
                                    $"ValueGrowth {inlineValueGrowth} does not equal expected {oldInlineValueSize - SpanField.OverflowInlineSize}");

                        if (shiftOptionals)
                        {
                            var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                            _ = SpanField.ConvertInlineToOverflow(ref InfoRef, valueAddress, newDataLength, overflowAllocator);
                            optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                        }
                        else
                            _ = SpanField.ConvertInlineToOverflow(ref InfoRef, valueAddress, newDataLength, overflowAllocator);
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsObject, "Expected ValueIsObject to be set, pt 1");

                        // If there is no change in inline size (it is already the out-of-line inline-portion size), we don't need the optional-shift.
                        if (shiftOptionals)
                        {
                            var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                            _ = SpanField.ConvertInlineToObjectId(ref InfoRef, (long)ValueObjectIdAddress, objectIdMap);
                            optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                        }
                        else
                            _ = SpanField.ConvertInlineToObjectId(ref InfoRef, (long)ValueObjectIdAddress, objectIdMap);
                    }
                }
                else if (Info.ValueIsOverflow)
                {
                    if (sizeInfo.ValueIsInline)
                    {
                        // Convert from overflow to inline. We should already have handled the case where it won't fit.
                        Debug.Assert(fillerLen >= newDataLength + SpanField.FieldLengthPrefixSize - SpanField.OverflowInlineSize,
                                $"Inconsistent required Inline space: available {fillerLen}, required {newDataLength + SpanField.FieldLengthPrefixSize - SpanField.OverflowInlineSize}");

                        if (shiftOptionals)
                        {
                            var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                            _ = SpanField.ConvertOverflowToInline(ref InfoRef, valueAddress, newDataLength, overflowAllocator);
                            optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                        }
                        else
                            _ = SpanField.ConvertOverflowToInline(ref InfoRef, valueAddress, newDataLength, overflowAllocator);
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsObject, "Expected ValueIsObject to be set, pt 2");

                        if (shiftOptionals)
                        {
                            var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                            _ = SpanField.ConvertOverflowToObjectId(ref InfoRef, valueAddress, overflowAllocator, objectIdMap);
                            optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                        }
                        else
                            _ = SpanField.ConvertOverflowToObjectId(ref InfoRef, valueAddress, overflowAllocator, objectIdMap);
                    }
                }
                else
                {
                    Debug.Assert(Info.ValueIsObject, "Expected ValueIsObject to be set, pt 3");

                    if (sizeInfo.ValueIsInline)
                    {
                        if (shiftOptionals)
                        {
                            var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                            _ = SpanField.ConvertObjectIdToInline(ref InfoRef, valueAddress, newDataLength, objectIdMap);
                            optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                        }
                        else
                            _ = SpanField.ConvertObjectIdToInline(ref InfoRef, valueAddress, newDataLength, objectIdMap);
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsOverflow, "Expected ValueIsOverflow to be true");

                        if (shiftOptionals)
                        {
                            var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                            _ = SpanField.ConvertObjectIdToOverflow(ref InfoRef, valueAddress, newDataLength, overflowAllocator, objectIdMap);
                            optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                        }
                        else
                            _ = SpanField.ConvertObjectIdToOverflow(ref InfoRef, valueAddress, newDataLength, overflowAllocator, objectIdMap);
                    }
                }
            }

        Done:
            Debug.Assert(Info.ValueIsInline == sizeInfo.ValueIsInline, "Final ValueIsInline is inconsistent");
            Debug.Assert(!Info.ValueIsInline || ValueSpan.Length <= sizeInfo.MaxInlineValueSpanSize, $"Inline ValueSpan.Length {ValueSpan.Length} is greater than sizeInfo.MaxInlineValueSpanSize {sizeInfo.MaxInlineValueSpanSize}");
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueSpan(SpanByte value, ref RecordSizeInfo sizeInfo)
        {
            RecordSizeInfo.AssertValueDataLength(value.Length, ref sizeInfo);

            if (!TrySetValueLength(ref sizeInfo))
                return false;

            value.CopyTo(SpanField.AsSpanByte(ValueAddress, Info.ValueIsInline));
            return true;
        }

        /// <summary>
        /// Set the object, checking for conversion from inline and for space for optionals (ETag, Expiration).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': Not doing so because it modifies internal state
        public bool TrySetValueObject(TValue value, ref RecordSizeInfo sizeInfo)
#pragma warning restore IDE0251
        {
            return TrySetValueLength(ref sizeInfo) && TrySetValueObject(value);
        }


        /// <summary>
        /// This overload must be called only when it is known the LogRecord's Value is not inline, and there is no need to check
        /// optionals (ETag or Expiration). In that case it is faster to just set the object.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': Not doing so because it modifies internal state
        public bool TrySetValueObject(TValue value)
#pragma warning restore IDE0251
        {
            Debug.Assert(ValueIsObject, $"Cannot call this overload of {GetCurrentMethodName()} for non-object Value");

            if (Info.ValueIsInline)
            {
                Debug.Fail($"Cannot call {GetCurrentMethodName()} with no {nameof(RecordSizeInfo)} when the value is inline");
                return false;
            }

            var objectId = *ValueObjectIdAddress;
            if (objectId == ObjectIdMap.InvalidObjectId)
                objectId = *ValueObjectIdAddress = objectIdMap.Allocate();

            objectIdMap.Set(objectId, value);
            return true;
        }

        private readonly int ETagLen => Info.HasETag ? LogRecord.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? LogRecord.ExpirationSize : 0;

        /// <summary>A tuple of the total size of the main-log (inline) portion of the record, with and without filler length.</summary>
        public readonly (int actualSize, int allocatedSize) GetInlineRecordSizes()
        {
            var actualSize = ActualRecordSize;
            return (actualSize, actualSize + GetFillerLength());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetOptionalStartAddress()
        {
            var valueAddress = ValueAddress;
            return ValueAddress + SpanField.GetInlineTotalSizeOfValue(valueAddress, ValueIsObject, Info.ValueIsInline);
        }

        public readonly int OptionalSize => ETagLen + ExpirationLen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetETagAddress() => GetOptionalStartAddress();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetExpirationAddress() => GetETagAddress() + ETagLen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetFillerLengthAddress() => physicalAddress + ActualRecordSize;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLength() => GetFillerLength(GetFillerLengthAddress());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLength(long fillerLenAddress)
        {
            if (Info.HasFiller)
                return *(int*)fillerLenAddress;

            // Filler includes Filler space opened up by removing ETag or Expiration. If there is no Filler, we may still have a couple bytes (< Constants.FillerLenSize)
            // due to RoundUp of record size. Optimize the filler address to avoid additional "if" statements.
            var recSize = (int)(fillerLenAddress - physicalAddress);
            return RoundUp(recSize, Constants.kRecordAlignment) - recSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': Not doing so because it modifies the object list
        public void SetFillerLength(int allocatedSize)
#pragma warning restore IDE0251
        {
            // This assumes Key and Value lengths have been set. It is called when we have initialized a record, or reinitialized due to revivification etc.
            // Therefore optionals (ETag, Expiration) are not considered here.
            var valueAddress = ValueAddress;
            var fillerAddress = valueAddress + SpanField.GetInlineTotalSizeOfValue(valueAddress, ValueIsObject, Info.ValueIsInline);
            var usedSize = (int)(fillerAddress - physicalAddress);
            var fillerSize = allocatedSize - usedSize;

            if (fillerSize >= LogRecord.FillerLengthSize)
            {
                InfoRef.SetHasFiller(); // must do this first, for zero-init
                *(int*)fillerAddress = fillerSize;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitializeForReuse(ref RecordSizeInfo sizeInfo)
        {
            // This assumes Key and Value lengths have not been set; and further, that the record is zero'd.
            InfoRef.SetKeyIsInline();
            _ = SpanField.SetInlineDataLength(KeyAddress, sizeInfo.InlineTotalKeySize);
            InfoRef.SetValueIsInline();
            _ = SpanField.SetInlineDataLength(ValueAddress, sizeInfo.InlineTotalValueSize);

            // Anything remaining is filler.
            SetFillerLength(sizeInfo.AllocatedInlineRecordSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': Not doing so because it modifies internal state
        public bool TrySetETag(long eTag)
#pragma warning restore IDE0251
        {
            if (Info.HasETag)
            {
                *(long*)GetETagAddress() = eTag;
                return true;
            }

            // We're adding an ETag where there wasn't one before.
            const int growth = LogRecord.ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var fillerLen = GetFillerLength(fillerLenAddress) - growth;
            if (fillerLen < 0)
                return false;

            // Start at FillerLen address and back up, for speed
            var address = fillerLenAddress;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            //      - We must do this here in case there is not enough room for filler after the growth.
            if (Info.HasFiller)
                *(int*)address = 0;

            //  - Preserve Expiration if present; set ETag; re-enter Expiration if present
            var expiration = 0L;
            var expirationSize = 0;
            if (Info.HasExpiration)
            {
                expirationSize = LogRecord.ExpirationSize;
                address -= expirationSize;
                expiration = *(long*)address;
            }
            *(long*)address = eTag;
            InfoRef.SetHasETag();

            *(long*)address = expiration;   // will be 0 or a valid expiration
            address += expirationSize;      // repositions to fillerAddress if expirationSize is nonzero

            //  - Set the new (reduced) FillerLength if there is still space for it.
            if (fillerLen >= LogRecord.FillerLengthSize)
                *(int*)address = fillerLen;
            else
                InfoRef.ClearHasFiller();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': Not doing so because it modifies internal state
        public bool RemoveETag()
#pragma warning restore IDE0251
        {
            if (!Info.HasETag)
                return true;

            const int growth = -LogRecord.ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var fillerLen = GetFillerLength(fillerLenAddress) - growth; // This will be negative, so adds ETagSize to it

            // Start at FillerLen address and back up, for speed
            var address = fillerLenAddress;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            if (Info.HasFiller)
                *(int*)address = 0;
            //  - Move Expiration, if present, up to cover ETag; then clear the ETag bit
            var expiration = 0L;
            var expirationSize = 0;
            if (Info.HasExpiration)
            {
                expirationSize = LogRecord.ExpirationSize;
                address -= expirationSize;
                expiration = *(long*)address;
                *(long*)address = 0L;  // To ensure zero-init
            }
            if (Info.HasETag)
            {
                // Expiration will be either zero or a valid expiration, and we have not changed the info.HasExpiration flag
                address -= LogRecord.ETagSize;
                *(long*)address = expiration;   // will be 0 or a valid expiration
                address += expirationSize;      // repositions to fillerAddress if expirationSize is nonzero
                InfoRef.ClearHasETag();
            }

            //  - Set the new (increased) FillerLength if there is space for it.
            if (fillerLen >= LogRecord.FillerLengthSize)
            {
                InfoRef.SetHasFiller();         // May already be set, but will definitely now be true since we opened up more than FillerLengthSize bytes
                *(int*)fillerLenAddress = fillerLen;
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetExpiration(long expiration)
        {
            if (expiration == 0)
                return RemoveExpiration();

            if (Info.HasExpiration)
            {
                *(long*)GetExpirationAddress() = expiration;
                return true;
            }

            // We're adding an Expiration where there wasn't one before.
            const int growth = LogRecord.ExpirationSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var fillerLen = GetFillerLength(fillerLenAddress) - growth;
            if (fillerLen < 0)
                return false;

            // Start at FillerLen address and back up, for speed

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            //      - We must do this here in case there is not enough room for filler after the growth.
            if (Info.HasFiller)
                *(int*)fillerLenAddress = 0;

            //  - Set Expiration where filler space used to be
            InfoRef.SetHasExpiration();
            *(long*)fillerLenAddress = expiration;
            fillerLenAddress += LogRecord.ExpirationSize;

            //  - Set the new (reduced) FillerLength if there is still space for it.
            if (fillerLen >= LogRecord.FillerLengthSize)
                *(int*)fillerLenAddress = fillerLen;
            else
                InfoRef.ClearHasFiller();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': Not doing so because it modifies internal state
        public bool RemoveExpiration()
#pragma warning restore IDE0251
        {
            if (!Info.HasExpiration)
                return true;

            const int growth = -LogRecord.ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var fillerLen = GetFillerLength(fillerLenAddress) - growth; // This will be negative, so adds ExpirationSize to it

            // Start at FillerLen address and back up, for speed
            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            if (Info.HasFiller)
                *(int*)fillerLenAddress = 0;

            //  - Remove Expiration and clear the Expiration bit; this will be the new fillerLenAddress
            fillerLenAddress -= LogRecord.ExpirationSize;
            *(long*)fillerLenAddress = 0;
            InfoRef.ClearHasExpiration();

            //  - Set the new (increased) FillerLength if there is space for it.
            if (fillerLen >= LogRecord.FillerLengthSize)
            {
                InfoRef.SetHasFiller();         // May already be set, but will definitely now be true since we opened up more than FillerLengthSize bytes
                *(int*)fillerLenAddress = fillerLen;
            }
            return true;
        }

        /// <summary>
        /// Copy the entire record values: Value and optionals (ETag, Expiration)
        /// </summary>
        public bool TryCopyRecordValues<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RecordSizeInfo sizeInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>
        {
            // This assumes the Key has been set and is not changed
            var srcRecordInfo = srcLogRecord.Info;
            if (!srcLogRecord.ValueIsObject)
            {
                if (!TrySetValueLength(ref sizeInfo))
                    return false;
                srcLogRecord.ValueSpan.AsReadOnlySpan().CopyTo(ValueSpan.AsSpan());
            }
            else
            {
                if (!TrySetValueObject(srcLogRecord.ValueObject, ref sizeInfo))
                    return false;
            }

            return TryCopyRecordOptionals(ref srcLogRecord, ref sizeInfo);
        }

        /// <summary>
        /// Copy the record optional values (ETag, Expiration)
        /// </summary>
        public bool TryCopyRecordOptionals<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref RecordSizeInfo sizeInfo)
            where TSourceLogRecord : ISourceLogRecord<TValue>
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
        internal void ClearOptionals()
        {
            _ = RemoveExpiration();
            _ = RemoveETag();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool FreeKeyOverflow()
        {
            if (!Info.KeyIsOverflow)
                return false;
            SpanField.FreeOverflowAndConvertToInline(ref InfoRef, KeyAddress, overflowAllocator, isKey: true);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool FreeValueOverflow()
        {
            if (!Info.ValueIsOverflow)
                return false;
            SpanField.FreeOverflowAndConvertToInline(ref InfoRef, KeyAddress, overflowAllocator, isKey: false);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PrepareForRevivification(RecordSizeInfo sizeInfo)
        {
            // For revivification: prepare the current record to be passed to initial updaters.
            // The rules:
            //  Zero things out, moving backward in address for both speed and zero-init correctness.
            //      - However, retain any overflow allocations.
            var newKeySize = Info.KeyIsInline && sizeInfo.KeyIsInline ? sizeInfo.FieldInfo.KeyDataSize + SpanField.FieldLengthPrefixSize : SpanField.OverflowInlineSize;
            var newValueSize = Info.ValueIsInline && sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueDataSize + SpanField.FieldLengthPrefixSize : SpanField.OverflowInlineSize;

            // Zero out optionals, starting from highest addresses
            var address = GetFillerLengthAddress();
            var allocatedRecordSize = GetInlineRecordSizes().allocatedSize;
            if (Info.HasFiller)
            {
                *(int*)address = 0;
                InfoRef.ClearHasFiller();
            }

            if (Info.HasExpiration)
            {
                address -= LogRecord.ExpirationSize;
                *(long*)address = 0L;
                InfoRef.ClearHasExpiration();
            }
            if (Info.HasETag)
            {
                *(long*)address = 0L;
                address -= LogRecord.ETagSize;
                InfoRef.ClearHasETag();
            }

            // Value is going to take up whatever space is leftover after Key, so optimize zeroinit to be only from end of new Key length to end of old value, if this difference is > 0.
            var endOfNewKey = KeyAddress + newKeySize;
            var valueAddress = ValueAddress;
            var endOfCurrentValue = valueAddress + SpanField.GetInlineTotalSizeOfValue(valueAddress, ValueIsObject, Info.ValueIsInline);
            var zeroSize = (int)(endOfCurrentValue - endOfNewKey);
            if (zeroSize > 0)
                new Span<byte>((byte*)endOfNewKey, zeroSize).Clear();

            // Set new key size
            if (Info.KeyIsInline)
                *(int*)KeyAddress = newKeySize;

            // Re-get ValueAddress due to the new KeyAddress and assign it all available space, unless it is already overflow, in which case the space after the overflow allocation pointer is filler space.
            valueAddress = ValueAddress;
            var spaceToEndOfRecord = allocatedRecordSize - (int)(valueAddress - physicalAddress) + newValueSize;
            if (Info.ValueIsInline)
                *(int*)valueAddress = spaceToEndOfRecord;
            else if (spaceToEndOfRecord - SpanField.OverflowInlineSize > LogRecord.FillerLengthSize)
            {
                InfoRef.SetHasFiller(); // must do this first, for zero-init
                address = valueAddress + SpanField.OverflowInlineSize;  // FillerAddress
                *(int*)address = spaceToEndOfRecord - SpanField.OverflowInlineSize;
            }
        }

        public override readonly string ToString()
        {
            if (physicalAddress == 0)
                return "<empty>";
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = ValueIsObject ? "<obj>" : ValueSpan.ToShortString(20);
            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}