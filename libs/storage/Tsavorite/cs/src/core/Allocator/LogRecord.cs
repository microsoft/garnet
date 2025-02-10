// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net;
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
        public static SpanByte GetKey(long physicalAddress) => SpanField.AsSpanByte(GetKeyAddress(physicalAddress), GetInfo(physicalAddress).KeyIsOverflow);

        /// <summary>The size of the key at <paramref name="keyAddress"/> </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int InlineKeySizeAt(long keyAddress) => SpanField.GetInlineSize(keyAddress);

        // All Key information is independent of TValue, which means we can navigate to the ValueAddress since we know KeyAddress and size.
        // However, any further Value operations requires TValue

        /// <summary>The address of the value</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetValueAddress(long physicalAddress)
        {
            var keyAddress = GetKeyAddress(physicalAddress);
            return keyAddress + InlineKeySizeAt(keyAddress);
        }
    }

    /// <summary>The in-memory record on the log: header, key, value, and optional fields
    ///     until some other things have been done that will allow clean separation.
    /// </summary>
    /// <remarks>The space is laid out as:
    ///     <list>
    ///     <item>[RecordInfo][SpanByte key][Value][ETag?][Expiration?][FillerLen?]
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
        {
            this.overflowAllocator = overflowAllocator;
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public readonly bool IsObjectRecord => objectIdMap is not null;
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
                Debug.Assert(!IsObjectRecord, "ValueSpan is not valid for Object log records");
                return SpanField.AsSpanByte(ValueAddress, Info.ValueIsOverflow);
            }
        }

        /// <inheritdoc/>
        public readonly TValue ValueObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                Debug.Assert(IsObjectRecord, "ValueObject is not valid for String log records");
                return (*ValueObjectIdAddress == ObjectIdMap.InvalidObjectId) ? default : objectIdMap.GetRef(ValueObjectId);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly TValue GetReadOnlyValue()
        {
            if (IsObjectRecord)
                return objectIdMap.GetRef(ValueObjectId);

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
            Debug.Assert(IsObjectRecord, "ClearValueObject() is not valid for String log records");
            if (IsObjectRecord)
            {
                ref var valueObjectRef = ref objectIdMap.GetRef(ValueObjectId);
                disposer(valueObjectRef);
                valueObjectRef = default;
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly LogRecord<TValue> AsLogRecord() => this;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetRecordFieldInfo() => new()
            {
                KeySize = Key.TotalSize,
                ValueSize = IsObjectRecord ? ObjectIdMap.ObjectIdSize : ValueSpan.TotalSize,
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

        private readonly int InlineKeySize => LogRecord.InlineKeySizeAt(KeyAddress);

        private readonly int InlineValueSize => InlineValueSizeAt(ValueAddress);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly int InlineValueSizeAt(long valueAddress) => IsObjectRecord ? ObjectIdMap.ObjectIdSize : SpanField.GetInlineSize(valueAddress);

        internal readonly int* ValueObjectIdAddress => (int*)ValueAddress;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int* GetValueObjectIdAddress(long physicalAddress) => (int*)LogRecord.GetValueAddress(physicalAddress);

        /// <summary>The value object id (index into the object values array)</summary>
        internal readonly int ValueObjectId
        {
            get
            {
                Debug.Assert(IsObjectRecord, "Cannot get ValueObjectId for String LogRecord");
                return *ValueObjectIdAddress;
            }
        }

        internal readonly ref TValue ObjectRef => ref objectIdMap.GetRef(ValueObjectId);

        /// <summary>The actual size of the main-log (inline) portion of the record; for in-memory records it does not include filler length.</summary>
        public readonly int ActualRecordSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                // Step through the address to reduce redundant addition operations.
                var size = RecordInfo.GetLength();
                var address = KeyAddress;
                var fieldSize = LogRecord.InlineKeySizeAt(address);
                size += fieldSize;
                address += fieldSize;
                size += InlineValueSizeAt(address);
                return size + OptionalSize;
            }
        }

        /// <summary>
        /// Asserts that <paramref name="newValueSize"/> is the same size as the value data size in the <see cref="RecordSizeInfo"/> before setting the length.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueSpanLength(int newValueSize, ref RecordSizeInfo sizeInfo)
        { 
            Debug.Assert(newValueSize == sizeInfo.ValueDataSize, $"Mismatched value size; expected {sizeInfo.ValueDataSize}, actual {newValueSize}");
            return TrySetValueSpanLength(ref sizeInfo);
        }

        /// <summary>
        /// Tries to set the length of the value field, with consideration to whether there is also space for the optionals (ETag and Expiration).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueSpanLength(ref RecordSizeInfo sizeInfo)
        {
            Debug.Assert(!IsObjectRecord, "ValueSpan cannot be used with Object log records");

            var valueAddress = ValueAddress;
            var oldInlineValueSize = InlineValueSizeAt(valueAddress);
            var newInlineValueSize = sizeInfo.InlineValueSize;

            // Growth and fillerLen may be negative if shrinking.
            var inlineValueGrowth = newInlineValueSize - oldInlineValueSize;
            var newOptionalSize = sizeInfo.OptionalSize;
            var oldOptionalSize = OptionalSize;
            var inlineTotalGrowth = inlineValueGrowth + (newOptionalSize - oldOptionalSize);

            var optionalStartAddress = valueAddress + oldInlineValueSize;
            var fillerLenAddress = optionalStartAddress + oldOptionalSize;
            var usedRecordSize = (int)(fillerLenAddress - physicalAddress);
            var fillerLen = GetFillerLength(fillerLenAddress);

            // See if we have enough room for the inline data. Note: We don't consider here things like moving inline data that is less than
            // overflow length into overflow to free up inline space; we calculate the inline size required for the new value (including whether
            // it is overflow) and optionals, and success is based on whether that can fit into the allocated record space.
            if (fillerLen < inlineTotalGrowth)
                return false;

            // We know we have enough space for the changed value *and* for whatever changes are coming in for the optionals. However, we aren't
            // changing the optionals here, so don't adjust fillerLen for them; only adjust it for value length change.
            fillerLen -= inlineValueGrowth;

            var newValueDataLength = sizeInfo.ValueDataSize;

            // We have enough space to handle the changed value size, including changing between inline and overflow or vice-versa, and the new
            // optional space. But we do not count the change in optional space here; we just ensure there is enough, and a later operation will
            // actually add/remove/update the optional(s), including setting the flag. So only adjust offsets for valueGrowth, not totalGrowth.
            if (Info.ValueIsOverflow == sizeInfo.ValueIsOverflow)
            {
                if (Info.ValueIsOverflow)
                {
                    // Both are overflow, so reallocate in place if needed.
                    if (newValueDataLength != BlockHeader.GetUserSize(SpanField.GetOverflowPointer(valueAddress)))
                        _ = SpanField.ReallocateOverflow(valueAddress, newValueDataLength, overflowAllocator);
                }
                else
                {
                    // Both are inline, so resize in place (and adjust filler) if needed.
                    if (inlineValueGrowth != 0)
                    {
                        var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                        _ = SpanField.AdjustInlineLength(ValueAddress, newValueDataLength);
                        optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                    }
                }
            }
            else
            {
                // Overflow-ness differs and we've verified there is enough space for the change, so convert. The SpanField.ConvertTo* functions copy
                // existing data, as we are likely here for IPU or for the initial update going from inline to overflow with Value length == sizeof(IntPtr).
                if (Info.ValueIsOverflow)
                {
                    // Convert from overflow to inline.
                    Debug.Assert(fillerLen >= newValueDataLength - SpanField.OverflowDataPtrSize,
                                $"Inconsistent required Inline space: available {fillerLen}, required {newValueDataLength - SpanField.OverflowDataPtrSize}");

                    var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                    _ = SpanField.ConvertToInline(valueAddress, newValueDataLength, overflowAllocator);
                    InfoRef.ClearValueIsOverflow();
                    optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                }
                else
                {
                    // Convert from inline to overflow.
                    Debug.Assert(inlineValueGrowth == SpanField.OverflowInlineSize - oldInlineValueSize, 
                                $"ValueGrowth {inlineValueGrowth} does not equal expected {oldInlineValueSize - SpanField.OverflowInlineSize}");

                    // If this is IPU or CU then our inline size is already calculated to be SpanField.OverflowDataPtrSize, so we don't need the optional-shift.
                    if (inlineValueGrowth != 0)
                    {
                        var optionalFields = OptionalFieldsShift.SaveAndClear(optionalStartAddress, ref InfoRef);
                        _ = SpanField.ConvertToOverflow(valueAddress, newValueDataLength, overflowAllocator);   // zeroes any "extra" space if there was shrinkage
                        InfoRef.SetValueIsOverflow();
                        optionalFields.Restore(optionalStartAddress + inlineValueGrowth, ref InfoRef, fillerLen);
                    }
                    else
                    {
                        _ = SpanField.ConvertToOverflow(valueAddress, newValueDataLength, overflowAllocator);   // zeroes any "extra" space if there was shrinkage
                        InfoRef.SetValueIsOverflow();
                    }
                }
            }

            Debug.Assert(Info.ValueIsOverflow == sizeInfo.ValueIsOverflow, "Final ValueIsOverflow is inconsistent");
            Debug.Assert(Info.ValueIsOverflow || ValueSpan.Length <= sizeInfo.MaxInlineValueSpanSize, $"Inline ValueSpan.Length {ValueSpan.Length} is greater than sizeInfo.MaxInlineValueSpanSize {sizeInfo.MaxInlineValueSpanSize}");
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueSpan(SpanByte value, ref RecordSizeInfo sizeInfo)
        {
            Debug.Assert(!IsObjectRecord, "ValueSpan cannot be used with Object log records");
            sizeInfo.AssertValueDataLength(value.Length);

            if (!TrySetValueSpanLength(ref sizeInfo))
                return false;
            
            value.CopyTo(SpanField.AsSpanByte(ValueAddress, Info.ValueIsOverflow));
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': Not doing so because it modifies internal state
        public bool TrySetValueObject(TValue value, ref RecordSizeInfo sizeInfo)
#pragma warning restore IDE0251
        {
            // This never changes Value length; it's always ObjectIdMap.ObjectIdSize. Make sure there is enough room for the any optionals
            // growth before we potentially allocate an ObjectId.
            var valueAddress = ValueAddress;
            var newOptionalSize = sizeInfo.OptionalSize;
            var oldOptionalSize = OptionalSize;
            var totalGrowth = newOptionalSize - oldOptionalSize;

            var optionalStartAddress = valueAddress + ObjectIdMap.ObjectIdSize;
            var fillerLenAddress = optionalStartAddress + oldOptionalSize;
            var usedRecordSize = (int)(fillerLenAddress - physicalAddress);
            var allocatedRecordSize = usedRecordSize + GetFillerLength(fillerLenAddress);

            var fillerLen = allocatedRecordSize - usedRecordSize;
            if (fillerLen < totalGrowth)
                return false;

            var objectId = *ValueObjectIdAddress;
            if (objectId == ObjectIdMap.InvalidObjectId)
            {
                if (!objectIdMap.Allocate(out objectId))
                    return false;
                *ValueObjectIdAddress = objectId;
            }
            objectIdMap.GetRef(objectId) = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': Not doing so because it modifies internal state
        public bool TrySetValueObject(TValue value)
#pragma warning restore IDE0251
        {
            var objectId = *ValueObjectIdAddress;
            if (objectId == ObjectIdMap.InvalidObjectId)
            {
                if (!objectIdMap.Allocate(out objectId))
                    return false;
                *ValueObjectIdAddress = objectId;
            }
            objectIdMap.GetRef(objectId) = value;
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
            // Step through the address to reduce redundant addition operations.
            var address = KeyAddress;
            address += LogRecord.InlineKeySizeAt(address);
            return address + InlineValueSizeAt(address);
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
            // Optionals (ETag, Expiration) are not considered here.
            var valueAddress = ValueAddress;
            var fillerAddress = valueAddress + InlineValueSize;
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
            _ = SpanField.SetInlineDataLength(KeyAddress, sizeInfo.InlineKeySize);
            _ = SpanField.SetInlineDataLength(ValueAddress, sizeInfo.InlineValueSize);

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
            if (!srcLogRecord.IsObjectRecord)
            {
                if (!TrySetValueSpanLength(ref sizeInfo))
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
        internal void FreeKeyOverflow()
        {
            if (!Info.KeyIsOverflow)
                return;
            overflowAllocator.Free(SpanField.GetOverflowPointer(KeyAddress));
            InfoRef.ClearKeyIsOverflow();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void FreeValueOverflow()
        {
            if (!Info.ValueIsOverflow)
                return;
            overflowAllocator.Free(SpanField.GetOverflowPointer(ValueAddress));
            InfoRef.ClearValueIsOverflow();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsBigEnough(ref RecordSizeInfo sizeInfo)
        {
            // For revivification: see if the current record is big enough to handle sizeInfo.
            // The rules:
            //  If the record's key or value has an overflow alloc, then the inline size cost is SpanField.OverflowInlineSize (overriding sizeInfo).
            //      - This is consistent with LogRecord.TrySetValueSpan which shrinks in place if there is an overflow alloc.
            //  If key or value has no overflow alloc but sizeInfo.(Key|Value)IsOverflow is true, the cost is SpanField.OverflowInlineSize
            //  If after the above are checked the inline size cannot fit into the record size, return false.
            //      - This is so we don't waste a bunch of small allocs in the overflow allocator
            var keySize = Info.KeyIsOverflow || sizeInfo.KeyIsOverflow ? SpanField.OverflowInlineSize : sizeInfo.FieldInfo.KeySize;
            var valueSize = Info.ValueIsOverflow || sizeInfo.ValueIsOverflow ? SpanField.OverflowInlineSize : sizeInfo.FieldInfo.ValueSize;
            return keySize + valueSize + sizeInfo.OptionalSize <= GetInlineRecordSizes().allocatedSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PrepareForRevivification(RecordSizeInfo sizeInfo)
        {
            // For revivification: prepare the current record to be passed to initial updaters.
            // The rules:
            //  Zero things out, moving backward in address for both speed and zero-init correctness.
            //      - However, retain any overflow allocations.
            var newKeySize = Info.KeyIsOverflow || sizeInfo.KeyIsOverflow ? SpanField.OverflowInlineSize : sizeInfo.FieldInfo.KeySize;
            var newValueSize = Info.ValueIsOverflow || sizeInfo.ValueIsOverflow ? SpanField.OverflowInlineSize : sizeInfo.FieldInfo.ValueSize;

            // Zero out optionals
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
            var endOfCurrentValue = valueAddress + InlineValueSizeAt(valueAddress);
            var zeroSize = (int)(endOfCurrentValue - endOfNewKey);
            if (zeroSize > 0)
                new Span<byte>((byte*)endOfNewKey, zeroSize).Clear();

            // Set new key size
            if (!Info.KeyIsOverflow)
                *(int*)KeyAddress = newKeySize;
            
            // Re-get ValueAddress due to the new KeyAddress and assign it all available space, unless it is already overflow, in which case the space after the overflow allocation pointer is filler space.
            valueAddress = ValueAddress;
            var spaceToEndOfRecord = allocatedRecordSize - (int)(valueAddress - physicalAddress) + newValueSize;
            if (!Info.ValueIsOverflow)
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
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = IsObjectRecord ? "<obj>" : ValueSpan.ToShortString(20);
            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}