﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SpanByte GetKey(long physicalAddress) => *(SpanByte*)(physicalAddress + RecordInfo.GetLength());
    }

    /// <summary>The in-memory record on the log: header, key, value, and optional fields
    ///     until some other things have been done that will allow clean separation.
    /// </summary>
    /// <remarks>The space is laid out as:
    ///     <list>
    ///     <item>[RecordInfo][SpanByte key][Value Id or SpanByte][ETag?][Expiration?][FillerLen?]</item>
    ///     </list>
    /// This lets us get to the key without intermediate computations to account for the optional fields.
    /// Some methods have both member and static versions for ease of access and possibly performance gains.
    /// </remarks>
    public unsafe struct LogRecord<TValue> : ISourceLogRecord<TValue>
    {
        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress;

        /// <summary>The overflow allocator for Keys and, if SpanByteAllocator, Values.</summary>
        readonly OverflowAllocator overflowAllocator;

        /// <summary>The ObjectIdMap if this is a record in the object log.</summary>
        readonly ObjectIdMap<TValue> objectIdMap;

        /// <summary>Address-only ctor, usually used for simple record parsing.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress) => this.physicalAddress = physicalAddress;

        /// <summary>This ctor is primarily used for internal record-creation operations and is passed to IObjectSessionFunctions callbacks.</summary> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress, OverflowAllocator overflowAllocator, ObjectIdMap<TValue> objectIdMap = null)
            : this(physicalAddress)
        {
            this.overflowAllocator = overflowAllocator;
            this.objectIdMap = objectIdMap;
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
                return *(SpanByte*)ValueAddress;
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
        public readonly ref TValue GetReadOnlyValueRef()
        {
            if (IsObjectRecord)
                return ref objectIdMap.GetRef(ValueObjectId);
            return ref Unsafe.AsRef<TValue>((void*)ValueAddress);
        }

        /// <inheritdoc/>
        public readonly long ETag => Info.HasETag ? *(long*)GetETagAddress() : 0;
        /// <inheritdoc/>
        public readonly long Expiration => Info.HasExpiration ? *(long*)GetExpirationAddress() : 0;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': not doing so because it modifies the object list
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
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);

        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;

        /// <summary>The address of the key</summary>
        internal readonly long KeyAddress => GetKeyAddress(physicalAddress);
        /// <summary>The address of the key</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetKeyAddress(long physicalAddress) => physicalAddress + RecordInfo.GetLength();

        /// <summary>A <see cref="SpanField"/> representing the record Key</summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SpanByte GetKey(long physicalAddress) => *(SpanByte*)(physicalAddress + RecordInfo.GetLength());

        /// <summary>The address of the value</summary>
        internal readonly long ValueAddress => GetValueAddress(physicalAddress);
        /// <summary>The address of the value</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetValueAddress(long physicalAddress)
        {
            var keyAddress = GetKeyAddress(physicalAddress);
            return keyAddress + SpanField.InlineSize(GetKeyAddress(physicalAddress));
        }

        private readonly int InlineKeySize => SpanField.InlineSize(KeyAddress);

        private readonly int InlineValueSize => IsObjectRecord ? ObjectIdMap.ObjectIdSize : SpanField.InlineSize(ValueAddress);

        internal readonly int* ValueObjectIdAddress => (int*)ValueAddress;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int* GetValueObjectIdAddress(long physicalAddress) => (int*)GetValueAddress(physicalAddress);

        /// <summary>The value object id (index into the object values array)</summary>
        internal readonly int ValueObjectId
        {
            get
            {
                Debug.Assert(!IsObjectRecord, "Cannot get ValueObjectId for String LogRecord");
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
                var fieldSize = SpanField.InlineSize(address);      // Key
                size += fieldSize;
                address += fieldSize;
                size += SpanField.InlineSize(address);              // Value
                return size + OptionalLength;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueSpanLength(int newValueLength)
        {
            Debug.Assert(!IsObjectRecord, "ValueSpan cannot be used with Object log records");

            var address = ValueAddress;
            var currentDataSize = SpanField.DataSize(address);

            // Do nothing if no size change. Growth and fillerLen may be negative if shrinking.
            var growth = newValueLength - currentDataSize;
            if (growth == 0)
                return true;

            var currentInlineSize = SpanField.InlineSize(address);
            var isOverflow = Info.ValueIsOverflow;
            var valueAddress = ValueAddress;
            var optionalStartAddress = valueAddress + SpanField.InlineSize(valueAddress);

            var (usedRecordSize, allocatedRecordSize) = GetFullRecordSizes();
            var fillerLen = allocatedRecordSize - usedRecordSize;

            // Handle changed size, including the overflow-ness of this size changing.
            // Note: We don't need to know the allocator's maxInlineSize because the in-place growth is gated by the current record size (including any filler size).

            if (growth > 0)
            {
                // We are growing
                if (!isOverflow)
                {
                    if (growth <= fillerLen)
                    {
                        // There is enough space to stay inline and adjust filler.
                        var optionalFields = new OptionalFieldsShift(optionalStartAddress, ref InfoRef);
                        SpanField.LengthRef(ValueAddress) = newValueLength; // growing into zero-init'd space
                        optionalStartAddress += growth;
                        optionalFields.Restore(optionalStartAddress, ref InfoRef, fillerLen - growth);
                    }
                    else
                    {
                        // There is not enough room to grow stay inline. If we do not have enough space for the out-of-line pointer, we must RCU.
                        if (currentInlineSize + fillerLen < SpanField.OverflowInlineSize)
                            return false;

                        // Convert to overflow
                        var optionalFields = new OptionalFieldsShift(optionalStartAddress, ref InfoRef);
                        growth = currentInlineSize - SpanField.OverflowInlineSize;
                        SpanField.ConvertToOverflow(ValueAddress, newValueLength, overflowAllocator);   // zeroes any "extra" space if there was shrinkage
                        optionalStartAddress = valueAddress + SpanField.OverflowInlineSize;
                        optionalFields.Restore(optionalStartAddress, ref InfoRef, fillerLen - growth);
                    }
                    return true;
                }
                else    // currently isOverflow
                {
                    // Growing and already oversize. SpanField will handle this either in-place (shrinking, or growth within the already-allocated size) or by free/alloc.
                    SpanField.ReallocateOverflow(address, newValueLength, overflowAllocator);
                }
            }
            else
            {
                // We are shrinking
                if (!isOverflow)
                {
                    // Stay inline and adjust filler
                    var optionalFields = new OptionalFieldsShift(optionalStartAddress, ref InfoRef);
                    SpanField.ShrinkInline(address, newValueLength);
                    optionalStartAddress += growth;
                    optionalFields.Restore(optionalStartAddress, ref InfoRef, fillerLen - growth);
                }
                else
                {
                    // We are in overflow and shrinking. Do so in-place; we do not need to zero-init in overflow space.
                    SpanField.LengthRef(address) = newValueLength;
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueSpan(SpanByte value)
        {
            Debug.Assert(!IsObjectRecord, "ValueSpan cannot be used with Object log records");

            if (!TrySetValueSpanLength(value.Length))
                return false;
            value.CopyTo(SpanField.AsSpan(ValueAddress));
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#pragma warning disable IDE0251 // Make member 'readonly': Not doing so because it modifies the object list
        public bool TrySetValueObject(TValue value)
#pragma warning restore IDE0251
        {
            if (*ValueObjectIdAddress == ObjectIdMap.InvalidObjectId)
            {
                if (!objectIdMap.Allocate(out var objectId))
                    return false;
                *ValueObjectIdAddress = objectId;
            }
            objectIdMap.GetRef(*ValueObjectIdAddress) = value;
            return true;
        }

        private readonly int ETagLen => Info.HasETag ? LogRecord.ETagSize : 0;
        private readonly int ExpirationLen => Info.HasExpiration ? LogRecord.ExpirationSize : 0;

        /// <summary>A tuple of the total size of the main-log (inline) portion of the record, with and without filler length.</summary>
        public readonly (int actualSize, int allocatedSize) GetFullRecordSizes()
        {
            var actualSize = ActualRecordSize;
            return (actualSize, actualSize + GetFillerLength());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetOptionalStartAddress()
        {
            // Step through the address to reduce redundant addition operations.
            var address = KeyAddress + RecordInfo.GetLength();
            address += SpanField.InlineSize(address);           // Key
            return address + SpanField.InlineSize(address);     // Value
        }

        public readonly int OptionalLength => ETagLen + ExpirationLen;

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
        public bool TrySetETag(long eTag)
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
            var maxLen = recordLen + GetFillerLength(fillerLenAddress);
            var availableSpace = maxLen - recordLen;
            if (availableSpace < growth)
                return false;

            // Start at FillerLen address and back up, for speed
            var address = fillerLenAddress;
            var fillerLen = availableSpace - growth;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            //      - We must do this here in case there is not enough room for filler after the growth.
            if (Info.HasFiller)
                *(int*)address = 0;
            //  - Preserve Expiration if present; set ETag; re-enter Expiration if present
            var expiration = 0L;
            if (Info.HasExpiration)
            {
                address -= LogRecord.ExpirationSize;
                expiration = *(long*)address;
            }
            *(long*)address = eTag;
            InfoRef.SetHasETag();
            if (Info.HasExpiration)
            {
                address += LogRecord.ETagSize;
                *(long*)address = expiration;
                address += ExpirationLen;
            }
            //  - Set the new (reduced) FillerLength if there is still space for it.
            if (fillerLen >= LogRecord.FillerLengthSize)
                *(int*)address = fillerLen;
            else
                InfoRef.ClearHasFiller();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool RemoveETag()
        {
            if (!Info.HasETag)
                return true;

            const int growth = -LogRecord.ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var maxLen = recordLen + GetFillerLength(fillerLenAddress);
            var availableSpace = maxLen - recordLen;

            // Start at FillerLen address and back up, for speed
            var address = fillerLenAddress;
            var fillerLen = availableSpace + growth;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            if (Info.HasFiller)
                *(int*)address = 0;
            //  - Move Expiration, if present, up to cover ETag; then clear the ETag bit
            var expiration = 0L;
            if (Info.HasExpiration)
            {
                address -= LogRecord.ExpirationSize;
                expiration = *(long*)address;
                *(long*)address = 0L;  // To ensure zero-init
            }
            address -= LogRecord.ETagSize;
            if (Info.HasETag)
            {
                // Expiration will be either zero or a valid expiration, and we have not changed the info.HasExpiration flag
                *(long*)address = expiration;
                address += LogRecord.ExpirationSize; // repositions to fillerAddress
            }
            InfoRef.ClearHasETag();
            //  - Set the new (increased) FillerLength if there is space for it.
            if (fillerLen >= LogRecord.FillerLengthSize)
                *(int*)address = fillerLen;
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
            var maxLen = recordLen + GetFillerLength(fillerLenAddress);
            var availableSpace = maxLen - recordLen;
            if (availableSpace < growth)
                return false;

            // Start at FillerLen address and back up, for speed
            var address = fillerLenAddress;
            var fillerLen = availableSpace - growth;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            //      - We must do this here in case there is not enough room for filler after the growth.
            if (Info.HasFiller)
                *(int*)address = 0;
            //  - Set Expiration
            InfoRef.SetHasExpiration();
            *(long*)address = expiration;
            address += ExpirationLen;
            //  - Set the new (reduced) FillerLength if there is still space for it.
            if (fillerLen >= LogRecord.FillerLengthSize)
                *(int*)address = fillerLen;
            else
                InfoRef.ClearHasFiller();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool RemoveExpiration()
        {
            if (!Info.HasExpiration)
                return true;

            const int growth = -LogRecord.ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var maxLen = recordLen + GetFillerLength(fillerLenAddress);
            var availableSpace = maxLen - recordLen;

            // Start at FillerLen address and back up, for speed
            var address = fillerLenAddress;
            var fillerLen = availableSpace + growth;

            // Preserve zero-init by:
            //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
            if (Info.HasFiller)
                *(int*)address = 0;
            //  - Remove Expiration and clear the Expiration bit
            address -= LogRecord.ExpirationSize;
            *(long*)address = 0;
            InfoRef.ClearHasExpiration();
            //  - Set the new (increased) FillerLength if there is space for it.
            if (fillerLen >= LogRecord.FillerLengthSize)
                *(int*)address = fillerLen;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool HasEnoughSpace(int newValueLen, bool withETag, bool withExpiration)
        {
            // TODO: Consider overflow in this
            var growth = newValueLen - InlineValueSize;
            if (Info.HasETag != withETag)
                growth += withETag ? LogRecord.ETagSize : -LogRecord.ETagSize;
            if (Info.HasExpiration != withExpiration)
                growth += withExpiration ? LogRecord.ExpirationSize : -LogRecord.ExpirationSize;

            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var maxLen = recordLen + GetFillerLength(fillerLenAddress);
            var availableSpace = maxLen - recordLen;
            return availableSpace >= growth;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueSpan(SpanByte valueSpan, long? eTag, long? expiration)
        {
            if (!HasEnoughSpace(valueSpan.TotalSize, eTag.HasValue, expiration.HasValue))
                return false;
            _ = TrySetValueSpan(valueSpan);
            return SetOptionals(eTag, expiration);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueObject(TValue valueObject, long? eTag, long? expiration)
        {
            if (!HasEnoughSpace(ObjectIdMap.ObjectIdSize, eTag.HasValue, expiration.HasValue))
                return false;
            _ = TrySetValueObject(valueObject);
            return SetOptionals(eTag, expiration);
        }

        public bool TryCopyRecord<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord)
            where TSourceLogRecord : ISourceLogRecord<TValue>
        {
            var recordInfo = srcLogRecord.Info;
            if (!srcLogRecord.IsObjectRecord)
            {
                var valueSpan = srcLogRecord.ValueSpan;
                if (!HasEnoughSpace(valueSpan.TotalSize, recordInfo.HasETag, recordInfo.HasExpiration))
                    return false;
                if (!TrySetValueSpan(valueSpan))
                    return false;
            }
            else
            {
                if (!HasEnoughSpace(ObjectIdMap.ObjectIdSize, recordInfo.HasETag, recordInfo.HasExpiration))
                    return false;
                if (!TrySetValueObject(srcLogRecord.ValueObject))
                    return false;
            }

            // Copy optionals
            if (!recordInfo.HasETag)
                RemoveETag();
            else if (!TrySetETag(srcLogRecord.ETag))
                return false;

            if (!recordInfo.HasExpiration)
                RemoveExpiration();
            else if (!TrySetExpiration(srcLogRecord.Expiration))
                return false;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool SetOptionals(long? eTag, long? expiration)
        {
            if (!eTag.HasValue)
                RemoveETag();
            else if (!TrySetETag(eTag.Value))
                return false;

            if (!expiration.HasValue)
                RemoveExpiration();
            else if (!TrySetExpiration(expiration.Value))
                return false;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearOptionals()
        {
            RemoveExpiration();
            RemoveETag();
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
            return keySize + valueSize + sizeInfo.OptionalSize <= GetFullRecordSizes().allocatedSize;
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
            var allocatedRecordSize = GetFullRecordSizes().allocatedSize;
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
            var endOfCurrentValue = valueAddress + SpanField.InlineSize(valueAddress);
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
            var valueString = IsObjectRecord ? "<obj>" : ValueSpan.ToString();
            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}