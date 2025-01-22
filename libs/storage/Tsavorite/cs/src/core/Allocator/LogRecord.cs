// Copyright (c) Microsoft Corporation.
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
    public unsafe struct LogRecord(long physicalAddress)
    {
        /// <summary>The physicalAddress in the log.</summary>
        internal readonly long physicalAddress = physicalAddress;

        /// <summary>Number of bytes required to store an ETag</summary>
        public const int ETagSize = sizeof(long);
        /// <summary>Number of bytes required to store an Expiration</summary>
        public const int ExpirationSize = sizeof(long);
        /// <summary>Number of bytes required to store the FillerLen</summary>
        internal const int FillerLenSize = sizeof(int);

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

        /// <summary>The ObjectIdMap if this is a record in the object log.</summary>
        readonly ObjectIdMap<TValue> objectIdMap;

        /// <summary>Address-only ctor, usually used for simple record parsing.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress) => this.physicalAddress = physicalAddress;

        /// <summary>This ctor is primarily used for internal record-creation operations and is passed to IObjectSessionFunctions callbacks.</summary> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal LogRecord(long physicalAddress, ObjectIdMap<TValue> objectIdMap)
            : this(physicalAddress)
        {
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
            get
            {
                Debug.Assert(IsObjectRecord, "ValueObject is not valid for String log records");
                return (*ValueObjectIdAddress == ObjectIdMap.InvalidObjectId) ? default : objectIdMap.GetRef(ValueObjectId);
            }
        }

        /// <inheritdoc/>
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
        public void ClearValueObject(Action<TValue> disposer)
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

        /// <summary>The key:
        ///     <list type="bullet">
        ///     <item>If serialized, then the key is inline in this record (i.e. is below the overflow size).</item>
        ///     <item>If not serialized, then it is a pointer to the key in <see cref="OverflowAllocator"/>.</item>
        ///     </list>
        /// </summary>
        /// <remarks>Not a ref return as it cannot be changed</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static SpanByte GetKey(long physicalAddress) => *(SpanByte*)(physicalAddress + RecordInfo.GetLength());

        /// <summary>Get a reference to the key; used for initial key setting.</summary>
        public readonly ref SpanByte KeyRef => ref GetKeyRef(physicalAddress);
        /// <summary>Get a reference to the key; used for initial key setting.</summary>
        public static ref SpanByte GetKeyRef(long physicalAddress) => ref *(SpanByte*)(physicalAddress + RecordInfo.GetLength());

        /// <summary>The address of the value</summary>
        internal readonly long ValueAddress => GetValueAddress(physicalAddress);
        /// <summary>The address of the value</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetValueAddress(long physicalAddress) => physicalAddress + RecordInfo.GetLength() + GetKey(physicalAddress).TotalInlineSize;

        /// <summary>The span of the value; may be inline-serialized or out-of-line overflow</summary>
        public readonly ref SpanByte ValueSpanRef => ref *(SpanByte*)ValueAddress;

        private readonly int InlineValueLength => IsObjectRecord ? ObjectIdMap.ObjectIdSize : ValueSpan.TotalInlineSize;

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
        public readonly int ActualRecordSize => RecordInfo.GetLength() + Key.TotalInlineSize + InlineValueLength + OptionalLength;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueSpanLength(int newValueLen)
        {
            Debug.Assert(!IsObjectRecord, "ValueSpan cannot be used with Object log records");

            // Do nothing if no size change. Growth and fillerLen may be negative if shrinking.
            var growth = newValueLen - InlineValueLength;
            if (growth == 0)
                return true;

            // TODO: handle the overflow-ness of this size changing
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var maxLen = recordLen + GetFillerLen(fillerLenAddress);
            var availableSpace = maxLen - recordLen;

            var optLen = OptionalLength;
            var optStartAddress = GetOptionalStartAddress();
            var fillerLen = availableSpace - growth;

            if (growth > 0)
            {
                // We're growing. See if there is enough space for the requested growth of Value.
                if (growth > availableSpace)
                    return false;

                // Preserve zero-init by:
                //  - Zeroing out FillerLen (this will leave only zeroes all the way to the next record, as there is nothing past FillerLen in this record).
                //      - We must do this here in case there is not enough room for filler after the growth.
                if (Info.HasFiller)
                    *(int*)fillerLenAddress = 0;
                //  - Zeroing out optionals is *not* necessary as GetRecordSize bases its calculation on Info bits.
                //  - Update the value length
                ValueSpanRef.Length += growth;
                //  - Set the new (reduced) FillerLength if there is still space for it.
                if (fillerLen >= LogRecord.FillerLenSize)
                {
                    fillerLenAddress += growth;
                    *(int*)fillerLenAddress = fillerLen;
                }
                else
                    InfoRef.ClearHasFiller();

                // Put the optionals in their new locations (MemoryCopy handles overlap). Let the caller's value update overwrite the previous optional space.
                Buffer.MemoryCopy((byte*)optStartAddress, (byte*)(optStartAddress + growth), optLen, optLen);

                return true;
            }

            // We're shrinking. Preserve zero-init by:
            //  - Store off optionals. We don't need to store FillerLen because we have to recalculate it anyway.
            var saveBuf = stackalloc byte[optLen]; // Garanteed not to be large
            Buffer.MemoryCopy((byte*)optStartAddress, saveBuf, optLen, optLen);
            //  - Zeroing FillerLen and the optionals space
            if (Info.HasFiller)
                *(int*)fillerLenAddress = 0;
            Unsafe.InitBlockUnaligned((byte*)optStartAddress, 0, (uint)optLen);
            //  - Shrinking the value and zeroing unused space
            ValueSpanRef.ShrinkSerializedLength(newValueLen);
            //  - Set the new (increased) FillerLength first, then the optionals
            if (fillerLen >= LogRecord.FillerLenSize)
                *(int*)fillerLenAddress = fillerLen;
            Buffer.MemoryCopy((byte*)(optStartAddress + growth), saveBuf, optLen, optLen);

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueSpan(SpanByte value)
        {
            Debug.Assert(!IsObjectRecord, "ValueSpan cannot be used with Object log records");

            if (!TrySetValueSpanLength(value.Length))
                return false;
            value.CopyTo(ref ValueSpanRef);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetValueObject(TValue value)
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
            return (actualSize, actualSize + GetFillerLen());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetOptionalStartAddress() => physicalAddress + RecordInfo.GetLength() + Key.TotalInlineSize + InlineValueLength;

        public readonly int OptionalLength => ETagLen + ExpirationLen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetETagAddress() => GetOptionalStartAddress();
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetExpirationAddress() => GetETagAddress() + ETagLen;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly long GetFillerLenAddress() => physicalAddress + ActualRecordSize;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLen() => GetFillerLen(GetFillerLenAddress());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly int GetFillerLen(long fillerLenAddress)
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
            var maxLen = recordLen + GetFillerLen(fillerLenAddress);
            var availableSpace = maxLen - recordLen;
            if (availableSpace < growth)
                return false;

            // Start at FillerLen address
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
            if (fillerLen >= LogRecord.FillerLenSize)
                *(int*)address = fillerLen;
            else
                InfoRef.ClearHasFiller();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RemoveETag()
        {
            if (!Info.HasETag)
                return;

            const int growth = -LogRecord.ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var maxLen = recordLen + GetFillerLen(fillerLenAddress);
            var availableSpace = maxLen - recordLen;

            // Start at FillerLen address
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
            if (Info.HasExpiration)
            {
                *(long*)address = expiration;
                address += LogRecord.ExpirationSize;
            }
            InfoRef.ClearHasETag();
            //  - Set the new (increased) FillerLength if there is space for it.
            if (fillerLen >= LogRecord.FillerLenSize)
                *(int*)address = fillerLen;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TrySetExpiration(long expiration)
        {
            if (Info.HasExpiration)
            {
                *(long*)GetExpirationAddress() = expiration;
                return true;
            }

            // We're adding an Expiration where there wasn't one before.
            const int growth = LogRecord.ExpirationSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var maxLen = recordLen + GetFillerLen(fillerLenAddress);
            var availableSpace = maxLen - recordLen;
            if (availableSpace < growth)
                return false;

            // Start at FillerLen address
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
            if (fillerLen >= LogRecord.FillerLenSize)
                *(int*)address = fillerLen;
            else
                InfoRef.ClearHasFiller();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RemoveExpiration()
        {
            if (!Info.HasExpiration)
                return;

            const int growth = -LogRecord.ETagSize;
            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var maxLen = recordLen + GetFillerLen(fillerLenAddress);
            var availableSpace = maxLen - recordLen;

            // Start at FillerLen address
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
            if (fillerLen >= LogRecord.FillerLenSize)
                *(int*)address = fillerLen;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool HasEnoughSpace(int newValueLen, bool withETag, bool withExpiration)
        {
            // TODO: Consider overflow in this
            var growth = newValueLen - InlineValueLength;
            if (Info.HasETag != withETag)
                growth += withETag ? LogRecord.ETagSize : -LogRecord.ETagSize;
            if (Info.HasExpiration != withExpiration)
                growth += withExpiration ? LogRecord.ExpirationSize : -LogRecord.ExpirationSize;

            var recordLen = ActualRecordSize;
            var fillerLenAddress = physicalAddress + recordLen;
            var maxLen = recordLen + GetFillerLen(fillerLenAddress);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool SetOptionals(long? eTag, long? expiration)
        {
            if (eTag.HasValue)
                _ = TrySetETag(eTag.Value);
            else
                RemoveETag();

            if (expiration.HasValue)
                _ = TrySetExpiration(expiration.Value);
            else
                RemoveExpiration();
            return true;
        }

        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = IsObjectRecord ? "<obj>" : ValueSpan.ToString();
            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{ETag} | HasExpiration {bstr(Info.HasExpiration)}:{Expiration}";
        }
    }
}