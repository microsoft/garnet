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

    /// <summary>The in-memory record on the log. The space is laid out as:
    /// <list type="bullet">
    ///     <item><see cref="RecordInfo"/> header</item>
    ///     <item>RecordDataHeader bytes (including RecordType and Namespace) and lengths; see <see cref="RecordDataHeader"/> header comments for details</item>
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
        public const long NoETag = 0;
        /// <summary>Number of bytes required to store an Expiration</summary>
        public const int ExpirationSize = sizeof(long);
        /// <summary>Invalid Expiration</summary>
        public const long NoExpiration = 0;
        /// <summary>Number of bytes required to the object log position</summary>
        public const int ObjectLogPositionSize = sizeof(long);
        /// <summary>Number of bytes required to store the FillerLen</summary>
        internal const int FillerLengthSize = sizeof(int);

        /// <summary>Address-only ctor. Must only be used for simple record parsing, including inline size calculations.
        /// In particular, if knowledge of whether this is a string or object record is required, or an overflow allocator is needed, this method cannot be used.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LogRecord(long physicalAddress) => this.physicalAddress = physicalAddress;

        /// <summary>Address-only ctor. Must only be used for simple record parsing, including inline size calculations.
        /// In particular, if knowledge of whether this is a string or object record is required, or an overflow allocator is needed, this method cannot be used.</summary>
        public LogRecord(byte* recordPtr) => physicalAddress = (long)recordPtr;

        /// <summary>Address of the <see cref="RecordDataHeader"/></summary>
        internal readonly long DataHeaderAddress => physicalAddress + RecordInfo.Size;
        /// <summary>Address of the namespace indicator byte. If the bit at position <see cref="RecordDataHeader.ExtendedNamespaceIndicatorBit"/> is not set, then the <see cref="RecordDataHeader.NamespaceIndicatorMask"/> bits
        /// contain the full namespace as a single byte; otherwise those bits are the length of the extended namespace data preceding the key data.</summary>
        private readonly long NamespaceAddress => physicalAddress + RecordInfo.Size + RecordDataHeader.NamespaceOffsetInHeader;
        /// <summary>Address of the Record type indicator byte</summary>
        private readonly long RecordTypeAddress => physicalAddress + RecordInfo.Size + RecordDataHeader.RecordTypeOffsetInHeader;

        public readonly byte IndicatorByte => *(byte*)DataHeaderAddress;

        /// <summary>Returns a copy of the <see cref="core.RecordDataHeader"/> for reads.</summary>
        public readonly RecordDataHeader DataHeader => *(RecordDataHeader*)DataHeaderAddress;

        /// <summary>Returns a ref to the in-memory <see cref="core.RecordDataHeader"/> for mutations.</summary>
        public readonly ref RecordDataHeader DataHeaderRef => ref *(RecordDataHeader*)DataHeaderAddress;

        // MIGRATION: Old property. Will be removed in Phase 8.5.
        public readonly RecordDataHeader RecordDataHeader => *(RecordDataHeader*)DataHeaderAddress;

        /// <summary>
        /// Initialize both the RecordInfo and the RecordDataHeader for a newly-allocated record. This is the single entry point for resetting
        /// a record's headers before <see cref="InitializeRecord{TKey}(TKey, in RecordSizeInfo)"/> populates the actual data. Both headers must
        /// be reset together so neither retains stale bits (Sealed, KeyIsInline/ValueIsInline) from a previous occupant of the slot.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void InitializeHeadersForNewRecord(bool inNewVersion, long previousAddress)
        {
            InfoRef.WriteInfo(inNewVersion, previousAddress);
            DataHeaderRef.InitializeForNewRecord();
        }

        /// <summary>This ctor is primarily used for internal record-creation operations for the ObjectAllocator, and is passed to IObjectSessionFunctions callbacks.</summary> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LogRecord(long physicalAddress, ObjectIdMap objectIdMap)
            : this(physicalAddress)
        {
            this.objectIdMap = objectIdMap;
        }

        /// <summary>This ctor is primarily used for internal record-creation operations for the ObjectAllocator, and is passed to IObjectSessionFunctions callbacks.</summary> 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LogRecord(byte* recordPtr, ObjectIdMap objectIdMap)
            : this((long)recordPtr, objectIdMap)
        {
        }

        /// <summary>This ctor is used construct a transient copy of an in-memory LogRecord that remaps the object Ids in <paramref name="physicalAddress"/> to the transient map. 
        /// <paramref name="physicalAddress"/> is a pointer to transient memory that contains a copy of the in-memory allocator page's record span, including the objectIds
        /// in Key and Value data. This is used for iteration. Note that the objects are not removed from the allocator-page map, so for iteration they may temporarily be in both.
        /// </summary> 
        /// <remarks>This is ONLY to be done for transient log records, not records on the main log.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static LogRecord CreateRemappedOverPinnedTransientMemory(long physicalAddress, ObjectIdMap allocatorMap, ObjectIdMap transientMap)
        {
            var logRecord = new LogRecord(physicalAddress, transientMap);
            logRecord.RemapOverPinnedTransientMemory(allocatorMap, transientMap);
            return logRecord;
        }

        /// <summary>Remaps the object Ids to the transient map.</summary>
        /// <remarks>This is ONLY to be done for transient log records, not records on the main log.</remarks>
        public readonly void RemapOverPinnedTransientMemory(ObjectIdMap allocatorMap, ObjectIdMap transientMap)
        {
            if (ReferenceEquals(allocatorMap, transientMap))
                return;

            var dataHeader = DataHeader;

            if (DataHeader.KeyIsOverflow)
            {
                var (length, dataAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
                var overflow = allocatorMap.GetOverflowByteArray(*(int*)dataAddress);
                *(int*)dataAddress = transientMap.AllocateAndSet(overflow);
            }

            if (DataHeader.ValueIsOverflow)
            {
                var (length, dataAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
                var overflow = allocatorMap.GetOverflowByteArray(*(int*)dataAddress);
                *(int*)dataAddress = transientMap.AllocateAndSet(overflow);
            }
            else if (DataHeader.ValueIsObject)
            {
                var (length, dataAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
                var heapObj = allocatorMap.GetHeapObject(*(int*)dataAddress);
                *(int*)dataAddress = transientMap.AllocateAndSet(heapObj);
            }
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public readonly byte RecordType => *(byte*)RecordTypeAddress;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> Namespace
        {
            get
            {
                var indicator = *(byte*)NamespaceAddress;
                if ((indicator & (1 << RecordDataHeader.ExtendedNamespaceIndicatorBit)) == 0)
                {
                    // Single-byte namespace
                    return new ReadOnlySpan<byte>(ref *(byte*)NamespaceAddress);
                }
                else
                {
                    // Extended namespace
                    // var length = indicator & RecordDataHeader.NamespaceIndicatorMask;
                    // return new ReadOnlySpan<byte>((byte*)(ExtendedNamespaceAddress + 1), length);
                    ThrowTsavoriteException("Extended namespace not yet supported");
                    return default;
                }
            }
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
                var (length, dataAddress) = DataHeader.GetKeyFieldInfo(physicalAddress);
                return DataHeader.KeyIsInline ? new((byte*)dataAddress, length) : objectIdMap.GetOverflowByteArray(*(int*)dataAddress).ReadOnlySpan;
            }
        }

        /// <inheritdoc/>
        public readonly bool IsPinnedKey => DataHeader.KeyIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedKeyPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!IsPinnedKey)
                    ThrowTsavoriteException("PinnedKeyPointer is unavailable when Key is not pinned; use IsPinnedKey");
                (_ /*length*/, var dataAddress) = DataHeader.GetKeyFieldInfo(physicalAddress);
                return (byte*)dataAddress;
            }
        }

        /// <summary>Get and set the <see cref="OverflowByteArray"/> if this Key is not pinned; an exception is thrown if it is a pinned pointer (e.g. to a <see cref="SectorAlignedMemory"/>.</summary>
        public readonly OverflowByteArray KeyOverflow
        {
            get
            {
                if (DataHeader.KeyIsInline)
                    ThrowTsavoriteException("get_Overflow is unavailable when Key is inline");
                var (length, dataAddress) = DataHeader.GetKeyFieldInfo(physicalAddress);
                return objectIdMap.GetOverflowByteArray(*(int*)dataAddress);
            }
            set
            {
                var (length, dataAddress) = DataHeader.GetKeyFieldInfo(physicalAddress);
                if (!DataHeader.KeyIsOverflow || length != ObjectIdMap.ObjectIdSize)
                    ThrowTsavoriteException("set_KeyOverflow should only be called when transferring into a new record with KeyIsInline==false and key.Length==ObjectIdSize");
                *(int*)dataAddress = objectIdMap.AllocateAndSet(value);
            }
        }

        /// <inheritdoc/>
        public readonly Span<byte> ValueSpan
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (DataHeader.ValueIsObject)
                    ThrowTsavoriteException("ValueSpan is not valid for Object values");
                var (length, dataAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
                return DataHeader.ValueIsInline ? new((byte*)dataAddress, (int)length) : objectIdMap.GetOverflowByteArray(*(int*)dataAddress).Span;
            }
        }

        /// <inheritdoc/>
        public readonly IHeapObject ValueObject
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (DataHeader.ValueIsObject)
                {
                    var (_ /*valueLength*/, valueAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
                    return objectIdMap.GetHeapObject(*(int*)valueAddress);
                }
                ThrowTsavoriteException("ValueObject is not valid for Span values");
                return default;
            }
            internal set
            {
                if (DataHeader.ValueIsObject)
                {
                    var (valueLength, valueAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
                    Debug.Assert(valueLength == ObjectIdMap.ObjectIdSize, $"valueLength {valueLength} should be ObjectIdSize {ObjectIdMap.ObjectIdSize}");
                    *(int*)valueAddress = objectIdMap.AllocateAndSet(value);

                    // Clear the object log file position.
                    *(ulong*)GetObjectLogPositionAddress(GetOptionalStartAddress()) = ObjectLogFilePositionInfo.NotSet;
                    return;
                }
                ThrowTsavoriteException("SetValueObject should only be called by DiskLogRecord or Deserialization with ValueIsObject==true");
            }
        }

        /// <summary>Whether the value in this record is a valid IHeapObject; an exception is thrown if it is a Span, either inline or overflow byte[].</summary>
        public readonly bool ValueObjectIsSet
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (DataHeader.ValueIsObject)
                {
                    return *(int*)DataHeader.GetValueFieldInfo(physicalAddress).valueAddress != ObjectIdMap.InvalidObjectId;
                }
                ThrowTsavoriteException("ValueObjectIsSet is not valid for Span values");
                return default;
            }
        }

        /// <summary>
        /// We track the deserialized length of an object value in the ObjectLogPosition field after deserialization is complete. This allows
        /// flushes during recovery to both avoid re-serializing the object and know how to reset the ObjectLogPosition.
        /// </summary>
        /// <param name="heapObject">The deserialized object</param>
        /// <param name="deserializedLength">The deserialized length of the object</param>
        internal readonly void SetDeserializedValueObject(IHeapObject heapObject, ulong deserializedLength)
        {
            var (valueLength, valueAddress) = DataHeader.GetValueFieldInfo(physicalAddress);

            if (!DataHeader.ValueIsObject)
                ThrowTsavoriteException("SetDeserializedValueObject should only be called by Deserialization with ValueIsObject==true");
            Debug.Assert(valueLength == ObjectIdMap.ObjectIdSize, $"valueLength {valueLength} should be ObjectIdSize {ObjectIdMap.ObjectIdSize}");

            *(int*)valueAddress = objectIdMap.AllocateAndSet(heapObject);

            // Adding valueAddress and length is the same as GetOptionalStartAddress() but faster
            var objectLogPositionPtr = (ulong*)GetObjectLogPositionAddress(valueAddress + valueLength);
            *objectLogPositionPtr = deserializedLength;
        }

        /// <summary>The span of the entire record, including the ObjectId space if the record has objects.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly ReadOnlySpan<byte> AsReadOnlySpan() => new((byte*)physicalAddress, ActualSize);

        /// <inheritdoc/>
        public readonly bool IsPinnedValue => DataHeader.ValueIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedValuePointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!IsPinnedValue)
                    ThrowTsavoriteException("PinnedValuePointer is unavailable when Key is not pinned; use IsPinnedValue");
                return (byte*)DataHeader.GetValueFieldInfo(physicalAddress).valueAddress;
            }
        }

        /// <summary>
        /// Return the pinned value address and length, or throw if the value is not pinned
        /// </summary>
        public readonly (long address, int length) PinnedValueAddressAndLength
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (!IsPinnedValue)
                    ThrowTsavoriteException("PinnedValuePointer is unavailable when Key is not pinned; use IsPinnedValue");
                var (length, address) = DataHeader.GetValueFieldInfo(physicalAddress);
                return (address, (int)length);
            }
        }

        /// <summary>Get and set the <see cref="OverflowByteArray"/> if this Value is not pinned; an exception is thrown if it is a pinned pointer (e.g. to a <see cref="SectorAlignedMemory"/>.</summary>
        public readonly OverflowByteArray ValueOverflow
        {
            get
            {
                if (!DataHeader.ValueIsOverflow)
                    ThrowTsavoriteException("get_Overflow is unavailable when Value is not overflow");
                var (length, dataAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
                return objectIdMap.GetOverflowByteArray(*(int*)dataAddress);
            }
            set
            {
                var (length, dataAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
                if (!DataHeader.ValueIsOverflow || length != ObjectIdMap.ObjectIdSize)
                    ThrowTsavoriteException("set_ValueOverflow should only be called when trnasferring into a new record with ValueIsOverflow == true and value.Length==ObjectIdSize");
                *(int*)dataAddress = objectIdMap.AllocateAndSet(value);
            }
        }

        /// <inheritdoc/>
        public readonly SpanByteAndMemory ValueSpanByteAndMemory
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (DataHeader.ValueIsObject)
                    ThrowTsavoriteException("ValueSpanByteAndMemory is not valid for Object values");
                var (length, dataAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
                if (DataHeader.ValueIsInline)
                    return SpanByteAndMemory.FromPinnedPointer((byte*)dataAddress, (int)length);
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)dataAddress);
                return new SpanByteAndMemory(new BorrowedMemoryOwner(overflow.AsMemory()), overflow.Length);
            }
        }

        /// <inheritdoc/>
        public readonly long ETag => DataHeader.HasETag ? *(long*)GetETagAddress(GetOptionalStartAddress()) : NoETag;
        /// <inheritdoc/>
        public readonly long Expiration => DataHeader.HasExpiration ? *(long*)GetExpirationAddress(GetETagAddress(GetOptionalStartAddress())) : 0;

        /// <inheritdoc/>
        public readonly bool IsMemoryLogRecord => true;

        /// <inheritdoc/>
        public readonly ref LogRecord AsMemoryLogRecordRef() => ref Unsafe.AsRef(in this);

        /// <inheritdoc/>
        public readonly bool IsDiskLogRecord => false;

        /// <inheritdoc/>
        public readonly ref DiskLogRecord AsDiskLogRecordRef() => throw new InvalidOperationException("Cannot cast a memory LogRecord to a DiskLogRecord.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetRecordFieldInfo()
        {
            var dataHeader = DataHeader;
            return new()
            {
                KeySize = dataHeader.KeyLength,
                ValueSize = dataHeader.ValueLength,
                ValueIsObject = dataHeader.ValueIsObject,
                HasETag = dataHeader.HasETag,
                HasExpiration = dataHeader.HasExpiration
            };
        }

        /// <inheritdoc/>
        public readonly int AllocatedSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Info.IsNull ? RecordInfo.Size : DataHeader.GetRecordLength(physicalAddress);
        }

        public readonly int ActualSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Info.IsNull ? RecordInfo.Size : DataHeader.GetUnalignedComponentSum();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetAllocatedSize(long physicalAddress)
        {
            // Ensure this isn't called accidentally on a null record; it is used by revivification so that should never happen.
            Debug.Assert(!(*(RecordInfo*)physicalAddress).IsNull, "GetAllocatedSize should not be called on a null RecordInfo");
            return (*(RecordDataHeader*)(physicalAddress + RecordInfo.Size)).GetRecordLength(physicalAddress);
        }

        #endregion // ISourceLogRecord

        internal readonly void SetRecordAndFillerLength(int recordLength, int newFillerLen)
        {
            DataHeaderRef.SetFiller(physicalAddress, newFillerLen);
        }

        /// <summary>
        /// Initialize record for <see cref="ObjectAllocator{TStoreFunctions}"/>--includes Overflow option for Key and Overflow and Object option for Value
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeRecord<TKey>(TKey key, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            _ = DataHeaderRef.Initialize(in sizeInfo, out var keyAddress, out var namespaceAddress, out var valueAddress, physicalAddress);   // TODO: Pass in (possibly) namespace span

            // Serialize Key
            if (sizeInfo.KeyIsInline)
            {
                DataHeaderRef.SetKeyIsInline();
                key.KeyBytes.CopyTo(new Span<byte>((byte*)keyAddress, sizeInfo.InlineKeySize));
            }
            else
            {
                DataHeaderRef.SetKeyIsOverflow();
                var overflow = new OverflowByteArray(key.KeyBytes.Length, startOffset: 0, endOffset: 0, zeroInit: false);
                key.KeyBytes.CopyTo(overflow.Span);

                // This is record initialization so no object has been allocated for this field yet.
                var objectId = objectIdMap.Allocate();
                *(int*)keyAddress = objectId;
                objectIdMap.Set(objectId, overflow);
            }

            // Serialize namespace, if any
            //
            // Since TKey is generic, the hope is this whole branch gets elided when using a no-namespace key type
            if (key.HasNamespace)
            {
                var namespaceBytes = key.NamespaceBytes;
                Debug.Assert(namespaceBytes.Length == 1, "Should have exactly 1 namespace byte, variable length is not implemented");
                namespaceBytes.CopyTo(new Span<byte>((byte*)namespaceAddress, namespaceBytes.Length));
            }

            // Initialize Value metadata (but we don't have the value here to set yet; that's done in ISessionFunctions).
            if (sizeInfo.ValueIsInline)
            {
                DataHeaderRef.SetValueIsInline();
            }
            else
            {
                if (!sizeInfo.ValueIsObject)
                {
                    // We must have the space allocated for Overflow just like we do for inline, so we set the Overflow allocation and objectId here.
                    // We have no value data to copy yet.
                    DataHeaderRef.SetValueIsOverflow();
                    var overflow = new OverflowByteArray(sizeInfo.FieldInfo.ValueSize, startOffset: 0, endOffset: 0, zeroInit: false);

                    // This is record initialization so no object has been allocated for this field yet.
                    var objectId = objectIdMap.Allocate();
                    *(int*)valueAddress = objectId;
                    objectIdMap.Set(objectId, overflow);
                }
                else
                {
                    Debug.Assert(sizeInfo.FieldInfo.ValueSize == ObjectIdMap.ObjectIdSize, $"Expected object size ({ObjectIdMap.ObjectIdSize}) for Object ValueSize but was {sizeInfo.FieldInfo.ValueSize}");

                    // Unlike for Keys and Overflow values, we do not set the objectId here; we wait for the UMD operation to do that.
                    *(int*)valueAddress = ObjectIdMap.InvalidObjectId;
                    DataHeaderRef.SetValueIsObject();
                }
            }
        }

        /// <summary>
        /// Initialize record for <see cref="SpanByteAllocator{TStoreFunctions}"/>--does not include Overflow/Object options so is streamlined
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeRecord<TKey>(TKey key, in RecordSizeInfo sizeInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            _ = DataHeaderRef.Initialize(in sizeInfo, out var keyAddress, out var namespaceAddress, out _ /*valueAddress*/, physicalAddress);

            DataHeaderRef.SetKeyAndValueInline();

            // Serialize Key. Do nothing for the value; we've set it inline and the actual value setting is done in ISessionFunctions).
            key.KeyBytes.CopyTo(new Span<byte>((byte*)keyAddress, sizeInfo.InlineKeySize));

            // Serialize namespace, if any
            //
            // Since TKey is generic, the hope is this whole branch gets elided when using a no-namespace key type
            if (key.HasNamespace)
            {
                var namespaceBytes = key.NamespaceBytes;
                Debug.Assert(namespaceBytes.Length == 1, "Should have exactly 1 namespace byte, variable length is not implemented");
                namespaceBytes.CopyTo(new Span<byte>((byte*)namespaceAddress, namespaceBytes.Length));
            }
        }

        /// <summary>A ref to the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref *(RecordInfo*)physicalAddress;

        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;

        internal static ReadOnlySpan<byte> GetInlineKey(long physicalAddress)
        {
            var dataHeader = *(RecordDataHeader*)(physicalAddress + RecordInfo.Size);
            Debug.Assert(dataHeader.KeyIsInline, "Key must be inline");
            var (length, dataAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
            return new((byte*)dataAddress, length);
        }

        /// <summary>Get the span of the inline portion of the record. Following this, the caller should be sure the objectIds are remapped
        ///     to a transient ObjectIdMap if necessary.</summary>
        public readonly Span<byte> RecordSpan => new((byte*)physicalAddress, ActualSize);

        /// <summary>
        /// Tries to set the length of the value field, as well as verifying there is also space for the optionals (ETag, Expiration, ObjectLogPosition) as 
        /// specified by <paramref name="sizeInfo"/>, shifting the optional positions if necessary, and setting or clearing the appropriate optional RecordInfo flags.
        /// Asserts that <paramref name="newValueSize"/> is the same size as the value data size in the <see cref="RecordSizeInfo"/> before setting the length.
        /// </summary>
        /// <returns>If successful, returns true and the caller can proceed to set the value data.</returns>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetContentLengths(int newValueSize, in RecordSizeInfo sizeInfo, bool zeroInit = false)
        {
            Debug.Assert(newValueSize == sizeInfo.FieldInfo.ValueSize, $"Mismatched value size; expected {sizeInfo.FieldInfo.ValueSize}, actual {newValueSize}");
            return TrySetContentLengthsAndPrepareOptionals(in sizeInfo, zeroInit, out _ /*valueAddress*/);
        }

        /// <summary>
        /// Tries to set the length of the value field, as well as verifying there is also space for the optionals (ETag, Expiration, ObjectLogPosition) as 
        /// specified by <paramref name="sizeInfo"/>, shifting the optional positions if necessary, and setting or clearing the appropriate optional RecordInfo flags.
        /// </summary>
        /// <returns>If successful, returns true and the caller can proceed to set the value data.</returns>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetContentLengths(in RecordSizeInfo sizeInfo, bool zeroInit = false) => TrySetContentLengthsAndPrepareOptionals(in sizeInfo, zeroInit, out _ /*valueAddress*/);

        /// <summary>
        /// Tries to set the length of the value field, as well as verifying there is also space for the optionals (ETag, Expiration, ObjectLogPosition) as 
        /// specified by <paramref name="sizeInfo"/>, shifting the optional positions if necessary, and setting or clearing the appropriate optional RecordInfo flags.
        /// </summary>
        /// <returns>If successful, returns true and the caller can proceed to set the value data.</returns>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        private readonly bool TrySetContentLengthsAndPrepareOptionals(in RecordSizeInfo sizeInfo, bool zeroInit, out long valueAddress)
        {
            // Get the number of bytes in existing key and value lengths.
            var dataHeader = DataHeader;
            var (keyLength, oldInlineValueSize) = dataHeader.GetKVLengths(physicalAddress, out var oldETagLen, out var oldExpirationLen, out var oldObjectLogPositionLen, out var oldFillerLen, out valueAddress);
            var recordLength = dataHeader.GetRecordLength(physicalAddress);
            var oldOptionalSize = oldETagLen + oldExpirationLen + oldObjectLogPositionLen;
            var extendedNamespaceSize = dataHeader.ExtendedNamespaceLength;

            // Key does not change, so its size and size byte count remain the same. valueAddress does not change either, as everything before it is immutable.
            // optionalStartAddress will change if inline value size changes.
            var optionalStartAddress = valueAddress + oldInlineValueSize;

            // It is OK if the record is shrinking but we cannot grow past the old RecordLength. (If we are converting from inline to overflow that will
            // already be accounted for because sizeInfo will be set for the ObjectId length.)
            if (sizeInfo.ActualInlineRecordSize > recordLength)
                return false;

            // We don't need to change the size if values are both inline and their size hasn't changed and the optional specs are the same,
            // we can exit early with success.
            var newInlineValueSize = sizeInfo.InlineValueSize;
            var inlineValueGrowth = newInlineValueSize - oldInlineValueSize;
            if (dataHeader.RecordIsInline && sizeInfo.RecordIsInline && inlineValueGrowth == 0
                    && dataHeader.HasETag == sizeInfo.FieldInfo.HasETag && dataHeader.HasExpiration == sizeInfo.FieldInfo.HasExpiration)
                return true;

            // inlineValueGrowth and fillerLen may be negative if shrinking value or converting to Overflow/Object.
            // ETag and Expiration won't change, but optionalGrowth may be positive or negative if adding or removing ObjectLogPosition.
            var optionalGrowth = sizeInfo.OptionalSize - oldOptionalSize;

            // See if we have enough room for the change in Value inline data. Note: This includes things like moving inline data that is less than
            // overflow length into overflow, which frees up inline space > ObjectIdMap.ObjectIsSize. We calculate the inline size required for the
            // new value (including whether it is overflow) and the existing optionals, and success is based on whether that can fit into the allocated
            // record space. We also change the presence of optionals here, shift their positions, and adjust their RecordInfo flags as needed.
            // Subsequent operations must assign new ETag or Expiration if the flag was set in sizeInfo.
            if (oldFillerLen < inlineValueGrowth + optionalGrowth)  // Optional growth here includes ObjLogPositionSize changes
                return false;

            // Update record part 1: Save the optionals if shifting is needed. We can't just shift now because we may be e.g. converting from inline to
            // overflow and they'd overwrite needed data.
            var optionalFields = new OptionalFieldsShift();
            optionalFields.Save(optionalStartAddress, dataHeader);

            // Update record part 2: Atomically install a "dummy" DataHeader in the live record that absorbs the entire post-key span as a single inline
            // value (no optionals, no filler, no object/overflow flags). This keeps any concurrent reader that walks records by computing
            //   record_length = RecordInfo.Size + RDH.Size + ExtendedNamespaceLength + KeyLength + ValueLength + OptionalSize [+ explicit filler]
            // from seeing a torn state while we shuffle data around. The dummy preserves Namespace + RecordType + KeyLength.
            var dummyValueLength = recordLength - (RecordInfo.Size + RecordDataHeader.Size + extendedNamespaceSize + keyLength);
            Debug.Assert((uint)dummyValueLength <= LogSettings.ValueLengthSentinel,
                $"Dummy ValueLength {dummyValueLength} exceeds 24-bit field; reserve LogSettings.OptionalsReservedBytes is insufficient");
            RecordDataHeader dummyHeader = default;
            dummyHeader.SetKeyAndValueInline();                          // KeyIsInline | ValueIsInline; clears ValueIsObject
            dummyHeader.SetNamespaceByteRaw((byte)((dataHeader.word >> 56) & 0xFF));  // preserve namespace byte (raw, including extended-namespace indicator)
            dummyHeader.RecordType = dataHeader.RecordType;
            dummyHeader.KeyLength = keyLength;
            dummyHeader.ValueLength = dummyValueLength;
            DataHeaderRef = dummyHeader;  // atomic 8-byte install

            // Update record part 3: Do any necessary conversions between Inline, Overflow, and Object. This may allocate or free Heap Objects.
            // All bit-flag changes go into the LOCAL `local` header; data-area writes go to the LIVE record. The dummy header keeps any
            // concurrent record-walker safe throughout.
            // Evaluate in order of most common (i.e. most perf-critical) cases first.
            var local = dataHeader;
            if (dataHeader.ValueIsInline && sizeInfo.ValueIsInline)
            {
                // Both are inline. The conversion routines below would normally update the local's ValueLength; here we must do it explicitly
                // (previously implied by the now-removed RecordLength field).
                local.ValueLength = newInlineValueSize;
            }
            else if (dataHeader.ValueIsOverflow && sizeInfo.ValueIsOverflow)
            {
                // Both are out-of-line, so reallocate in place if needed; the caller will operate on that space after we return.
                _ = LogField.ReallocateValueOverflow(physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                // local already reflects ValueIsOverflow + ValueLength == ObjectIdSize
            }
            else if (dataHeader.ValueIsObject && sizeInfo.ValueIsObject)
            {
                // Both are object records, so nothing to change; the caller will operate on the object after we return.
            }
            else
            {
                // Overflow/Object-ness differs and we've verified there is enough space for the change, so convert. The LogField.ConvertTo* functions copy
                // existing data, as we are likely here for IPU or for the initial update going from inline to overflow with Value length == sizeof(IntPtr).
                if (dataHeader.ValueIsInline)
                {
                    if (sizeInfo.ValueIsOverflow)
                    {
                        Debug.Assert(inlineValueGrowth == ObjectIdMap.ObjectIdSize - oldInlineValueSize,
                                    $"ValueGrowth {inlineValueGrowth} does not equal expected {oldInlineValueSize - ObjectIdMap.ObjectIdSize}");
                        _ = LogField.ConvertInlineToOverflow(ref local, physicalAddress, valueAddress, oldInlineValueSize, in sizeInfo, objectIdMap);
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsObject, "Expected ValueIsObject to be set, pt 1");
                        _ = LogField.ConvertInlineToValueObject(ref local, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                    }
                    // After conversion away from inline, the local's ValueLength is the ObjectIdSize (already set in the original local since ValueIsInline);
                    // we need to set it explicitly to ObjectIdSize to match the post-conversion state.
                    local.ValueLength = ObjectIdMap.ObjectIdSize;
                }
                else if (dataHeader.ValueIsOverflow)
                {
                    if (sizeInfo.ValueIsInline)
                    {
                        _ = LogField.ConvertOverflowToInline(ref local, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                        local.ValueLength = newInlineValueSize;
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsObject, "Expected ValueIsObject to be set, pt 2");
                        _ = LogField.ConvertOverflowToValueObject(ref local, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                        // ObjectId-sized; local.ValueLength remains ObjectIdSize
                    }
                }
                else
                {
                    Debug.Assert(dataHeader.ValueIsObject, "Expected ValueIsObject to be set, pt 3");

                    if (sizeInfo.ValueIsInline)
                    {
                        _ = LogField.ConvertValueObjectToInline(ref local, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                        local.ValueLength = newInlineValueSize;
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsOverflow, "Expected ValueIsOverflow to be true");
                        _ = LogField.ConvertValueObjectToOverflow(ref local, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                        // ObjectId-sized; local.ValueLength remains ObjectIdSize
                    }
                }
            }

            // Update record part 4: Restore optionals to their new location. If we have some optionals in sizeInfo that weren't in the record previously, they'll get
            // their default values; subsequently, the caller should set them to the actual values. We have to do this even if not sizeInfo.HasOptionalFields because
            // this also sets or clears optional flags.
            optionalStartAddress += inlineValueGrowth;
            optionalFields.Restore(optionalStartAddress, in sizeInfo, ref local);

            // Update Filler length on the local DataHeader. Optional data size for ETag/Expiration is unchanged even if newOptionalSize != oldOptionalSize,
            // because we are not updating those optionals here, so don't adjust fillerLen for that. However, a change in the presence or absence of the pseudo-optional
            // ObjectLogPosition must be accounted for if we have changed whether the record is inline or has objects. Note that we don't have a valueLength to update;
            // it is a calculated value, which depends (in part) upon FillerLength.
            var newFillerLen = oldFillerLen - inlineValueGrowth - optionalGrowth;
            if (newFillerLen != oldFillerLen)
                local.SetFiller(physicalAddress, newFillerLen > 0 ? newFillerLen : 0);
            if (zeroInit && inlineValueGrowth > 0)
            {
                // Zeroinit any extra space we grew the value by. For example, if we grew by one byte we might have a stale fillerLength in that byte.
                new Span<byte>((byte*)(valueAddress + oldInlineValueSize), newInlineValueSize - oldInlineValueSize).Clear();
            }

            // Update Record Part 5: All data movement is complete; atomically replace the dummy header with the real (fully-updated) one.
            DataHeaderRef = local;

            Debug.Assert(local.ValueIsInline == sizeInfo.ValueIsInline, "Final ValueIsInline is inconsistent");
            Debug.Assert(!local.ValueIsInline || ValueSpan.Length <= sizeInfo.MaxInlineValueSize, $"Inline ValueSpan.Length {ValueSpan.Length} is greater than sizeInfo.MaxInlineValueSpanSize {sizeInfo.MaxInlineValueSize}");
            return true;
        }

        /// <summary>
        /// Tries to set the length of the value field, including shifting optionals as needed. Does NOT change the presence of optionals,
        /// and only works on Inline values. Used for in-place updates and preceded by calling <see cref="PinnedValueAddressAndLength"/>
        /// which is usually necessary to evaluate the current value data, e.g. for INCRBY.
        /// </summary>
        /// <param name="newValueSize">The new size of the value.</param>
        /// <param name="valueAddress">The address of the value, obtained from <see cref="PinnedValueAddressAndLength"/></param>
        /// <param name="valueLength">The current length of the value; on input obtained from <see cref="PinnedValueAddressAndLength"/>; set on output to newValueSize</param>
        /// <param name="zeroInit">If true, set any value space "exposed" by increasing <paramref name="valueLength"/></param>
        /// <returns>If successful, returns true and the caller can proceed to set the value data.</returns>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        public readonly bool TrySetPinnedValueLength(in int newValueSize, long valueAddress, ref int valueLength, bool zeroInit = false)
        {
            if (!DataHeader.ValueIsInline)
            {
                Debug.Fail($"{nameof(TrySetPinnedValueLength)} should only be called when Value is known to be inline, such as INCRBY");
                return false;
            }

            // If we're not changing value size, there's nothing to do.
            var inlineValueGrowth = newValueSize - valueLength;
            if (inlineValueGrowth == 0)
                return true;

            // Snapshot DataHeader and existing lengths.
            var dataHeader = DataHeader;
            int oldFillerLen;
            int oldOptionalSize = 0;
            int oldETagLen = 0;
            int oldExpirationLen = 0;
            int oldObjectLogPositionLen = 0;
            int keyLength = dataHeader.KeyLength;
            int extendedNamespaceSize = dataHeader.ExtendedNamespaceLength;
            int recordLength;
            if (!dataHeader.HasOptionalOrObjectFields)
            {
                oldFillerLen = dataHeader.GetTotalFillerLength(physicalAddress);
                if (oldFillerLen < inlineValueGrowth)
                    return false;
                recordLength = dataHeader.GetRecordLength(physicalAddress);
            }
            else
            {
                _ = dataHeader.GetKVLengths(physicalAddress, out oldETagLen, out oldExpirationLen, out oldObjectLogPositionLen, out oldFillerLen, out _ /*valueAddress*/);
                if (oldFillerLen < inlineValueGrowth)
                    return false;
                oldOptionalSize = oldETagLen + oldExpirationLen + oldObjectLogPositionLen;
                recordLength = dataHeader.GetRecordLength(physicalAddress);
            }

            // Install a dummy DataHeader in the live record that absorbs the entire post-key span as opaque inline value bytes,
            // keeping concurrent record-walkers safe while we shift optionals and update filler.
            var dummyValueLength = recordLength - (RecordInfo.Size + RecordDataHeader.Size + extendedNamespaceSize + keyLength);
            Debug.Assert((uint)dummyValueLength <= LogSettings.ValueLengthSentinel,
                $"Dummy ValueLength {dummyValueLength} exceeds 24-bit field; reserve LogSettings.OptionalsReservedBytes is insufficient");
            RecordDataHeader dummyHeader = default;
            dummyHeader.SetKeyAndValueInline();
            dummyHeader.SetNamespaceByteRaw((byte)((dataHeader.word >> 56) & 0xFF));
            dummyHeader.RecordType = dataHeader.RecordType;
            dummyHeader.KeyLength = keyLength;
            dummyHeader.ValueLength = dummyValueLength;
            DataHeaderRef = dummyHeader;  // atomic install

            // Shift optionals in the data area if needed.
            if (oldOptionalSize != 0)
            {
                var optionalStartAddress = valueAddress + valueLength;
                Buffer.MemoryCopy((void*)optionalStartAddress, (void*)(optionalStartAddress + inlineValueGrowth), oldOptionalSize, oldOptionalSize);
            }

            // Zeroinit any extra space we grew the value by. For example, if we grew by one byte we might have a stale fillerLength in that byte.
            if (zeroInit && inlineValueGrowth > 0)
                new Span<byte>((byte*)(valueAddress + valueLength), inlineValueGrowth).Clear();

            // Build the real DataHeader on a local: update ValueLength and Filler. Optional flags and Key/Namespace/RecordType are unchanged from dataHeader.
            var local = dataHeader;
            local.ValueLength = newValueSize;
            local.SetFiller(physicalAddress, oldFillerLen - inlineValueGrowth);

            // Atomic install of the real DataHeader.
            DataHeaderRef = local;

            // Key does not change, so its size and size byte count remain the same. valueAddress does not change either, as everything before it is immutable.
            // So the only things that change are FillerLength and ValueLength.
            valueLength += inlineValueGrowth;
            return true;
        }

        /// <summary>
        /// Tries to set the length of the value field, including shifting optionals as needed. Does NOT change the presence of optionals,
        /// and only works on Inline values. Used for in-place updates and preceded by calling <see cref="PinnedValueAddressAndLength"/>
        /// which is usually necessary to evaluate the current value data, e.g. for INCRBY.
        /// </summary>
        /// <param name="newValue">The new value to set into the record.</param>
        /// <param name="valueAddress">The address of the value, obtained from <see cref="PinnedValueAddressAndLength"/></param>
        /// <param name="valueLength">The current length of the value; on input obtained from <see cref="PinnedValueAddressAndLength"/>; set on output to newValueSize</param>
        /// <param name="zeroInit">If true, set any value space "exposed" by increasing <paramref name="valueLength"/></param>
        /// <returns>If successful, returns true and the caller can proceed to set the value data.</returns>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetPinnedValueSpan(ReadOnlySpan<byte> newValue, long valueAddress, ref int valueLength, bool zeroInit = false)
        {
            if (!TrySetPinnedValueLength(newValue.Length, valueAddress, ref valueLength, zeroInit))
                return false;
            newValue.CopyTo(new Span<byte>((byte*)valueAddress, newValue.Length));
            return true;
        }

        /// <summary>
        /// Set the value span, checking for conversion to/from inline as well as verifying there is also space for the optionals (ETag, Expiration, ObjectLogPosition) as 
        /// specified by <paramref name="sizeInfo"/>, shifting the optional positions if necessary, and setting or clearing the appropriate optional RecordInfo flags.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValueSpanAndPrepareOptionals(ReadOnlySpan<byte> value, in RecordSizeInfo sizeInfo, bool zeroInit = false)
        {
            RecordSizeInfo.AssertValueDataLength(value.Length, in sizeInfo);
            if (!TrySetContentLengthsAndPrepareOptionals(in sizeInfo, zeroInit, out var valueAddress))
                return false;

            var valueSpan = sizeInfo.ValueIsInline ? new((byte*)valueAddress, sizeInfo.FieldInfo.ValueSize) : objectIdMap.GetOverflowByteArray(*(int*)valueAddress).Span;
            value.CopyTo(valueSpan);
            return true;
        }

        internal readonly bool TryReinitializeValueLength(in RecordSizeInfo sizeInfo)
        {
            // This is called when reinitializing a record for InitialUpdater or InitialWriter; we don't want to them to see initial state with optionals set.
            // Because it is for (re)initialization, we don't zero-initialize; the caller should assume they have to do that if they only copy partial data in.
            ClearOptionals();
            var dataHeader = DataHeader;
            var (_ /*valueLength*/, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
            var recordLength = dataHeader.GetRecordLength(physicalAddress);
            var fillerLength = (int)(physicalAddress + recordLength - (valueAddress + sizeInfo.InlineValueSize));
            if (fillerLength < 0)
                return false;
            DataHeaderRef.SetFiller(physicalAddress, fillerLength);
            return true;
        }

        /// <summary>
        /// Set the value span, checking for conversion to/from inline as well as verifying there is also space for the optionals (ETag, Expiration, ObjectLogPosition) as 
        /// specified by <paramref name="sizeInfo"/>, shifting the optional positions if necessary, and setting or clearing the appropriate optional RecordInfo flags.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValueObjectAndPrepareOptionals(IHeapObject value, in RecordSizeInfo sizeInfo) => TrySetContentLengths(in sizeInfo) && TrySetValueObject(value);

        /// <summary>
        /// This overload must be called only when it is known the LogRecord's Value is not inline, and there is no need to check
        /// optionals (ETag or Expiration). In that case it is faster to just set the object.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetValueObject(IHeapObject value)
        {
            Debug.Assert(DataHeader.ValueIsObject, $"Cannot call this overload of {GetCurrentMethodName()} for non-object Value");

            if (!DataHeader.ValueIsObject)
            {
                Debug.Fail($"Cannot call {GetCurrentMethodName()} with no {nameof(RecordSizeInfo)} when !ValueIsObject");
                return false;
            }

            var (valueLength, valueAddress) = DataHeader.GetValueFieldInfo(physicalAddress);

            // If there is no object there yet, allocate a slot
            var objectId = *(int*)valueAddress;
            if (objectId == ObjectIdMap.InvalidObjectId)
                objectId = *(int*)valueAddress = objectIdMap.Allocate();

            // Set the new object into the slot
            objectIdMap.Set(objectId, value);
            return true;
        }

        public readonly int ETagLen => DataHeader.HasETag ? ETagSize : 0;
        public readonly int ExpirationLen => DataHeader.HasExpiration ? ExpirationSize : 0;
        internal readonly int ObjectLogPositionLen => DataHeader.RecordHasObjects ? ObjectLogPositionSize : 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetOptionalStartAddress()
        {
            var (valueLength, valueAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
            return valueAddress + valueLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly ReadOnlySpan<byte> GetOptionalFieldsSpan() => new((byte*)GetOptionalStartAddress(), OptionalLength);

        public readonly int OptionalLength => ETagLen + ExpirationLen + ObjectLogPositionLen;

        #region IKey
        /// <inheritdoc/>
        public readonly bool IsPinned => IsPinnedKey;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> KeyBytes => Key;

        /// <inheritdoc/>
        public readonly bool HasNamespace
        {
            get
            {
                // A 1-byte 0 values namespace is the "default" and should be ignored.
                // Any non-zero value (including the ExtendedNamespaceIndicatorBit being set) means we have a namespace.
                var indicator = *(byte*)NamespaceAddress;
                return indicator != 0;
            }
        }

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> NamespaceBytes
        {
            get
            {
                Debug.Assert(HasNamespace, "Shouldn't call if !HasNamespace");
                return Namespace;
            }
        }
        #endregion

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetETagAddress(long optionalStartAddress) => optionalStartAddress;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetExpirationAddress(long optionalStartAddress) => optionalStartAddress + ETagLen;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly long GetObjectLogPositionAddress(long optionalStartAddress) => optionalStartAddress + ETagLen + ExpirationLen;

        /// <summary>
        /// Called during cleanup of a record allocation, before the key was copied.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly void InitializeForReuse(in RecordSizeInfo sizeInfo)
        {
            Debug.Assert(!DataHeader.HasETag && !DataHeader.HasExpiration, "Record should not have ETag or Expiration here");

            // This is called after the record has been allocated, so it's at the tail (or very close to it), and before it is returned to the TryBlockAllocate
            // caller. So all we need to do is initialize it to a consistent RecordLength state. We could make this a little leaner for this case but this is
            // called only on recovery from a failed TryAllocate (e.g. HeadAddress moved up so we couldn't complete the allocation), so it's not perf-critical.
            InfoRef = RecordInfo.InitialValid;

            // Reuse records must be heap-free; preserve their allocation size via filler.
            var reuseSizeInfo = new RecordSizeInfo { AllocatedInlineRecordSize = sizeInfo.AllocatedInlineRecordSize };
            reuseSizeInfo.SetKeyIsInline();
            reuseSizeInfo.SetValueIsInline();
            DataHeaderRef.InitializeForNewRecord();
            _ = DataHeaderRef.Initialize(in reuseSizeInfo, out _ /*keyAddress*/, out _ /*namespaceAddress*/, out _ /*valueAddress*/, physicalAddress);
        }

        /// <summary>
        /// Set the ETag, checking for space for optionals.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetETag(long eTag)
        {
            var optionalStartAddress = GetOptionalStartAddress();
            if (DataHeader.HasETag)
            {
                *(long*)GetETagAddress(optionalStartAddress) = eTag;
                return true;
            }

            var dataHeader = DataHeader;
            var recordLength = dataHeader.GetRecordLength(physicalAddress);

            // We're adding an ETag where there wasn't one before.
            var fillerLen = dataHeader.GetTotalFillerLength(physicalAddress);
            // We'll keep the original FillerLen address and back up, for speed.
            var address = physicalAddress + recordLength - fillerLen;
            fillerLen -= ETagSize;
            if (fillerLen < 0)
                return false;

            // We don't preserve the ObjectLogPosition field; that's only for serialization.
            if (DataHeader.RecordHasObjects)
                address -= ObjectLogPositionSize;

            // Preserve Expiration if present; set ETag; re-enter Expiration if present
            var expiration = 0L;
            if (DataHeader.HasExpiration)
            {
                address -= ExpirationSize;
                expiration = *(long*)address;
            }

            // Set the eTag
            *(long*)address = eTag;
            DataHeaderRef.SetHasETag();
            address += ETagSize;

            // Restore expiration, if any
            if (DataHeader.HasExpiration)
            {
                *(long*)address = expiration;   // will be 0 or a valid expiration
                address += ExpirationSize;      // repositions to ObjectLogPosition address
            }

            // ObjectLogPosition is not preserved (it's only for serialization) so set it to NotSet.
            if (DataHeader.RecordHasObjects)
                *(ulong*)address = ObjectLogFilePositionInfo.NotSet;

            DataHeaderRef.SetFiller(physicalAddress, fillerLen);
            return true;
        }

        /// <summary>
        /// Remove the ETag.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool RemoveETag()
        {
            if (!DataHeader.HasETag)
                return true;

            var dataHeader = DataHeader;
            var recordLength = dataHeader.GetRecordLength(physicalAddress);

            // We're adding an ETag where there wasn't one before.
            var fillerLen = dataHeader.GetTotalFillerLength(physicalAddress);
            // We'll keep the original FillerLen address and back up, for speed.
            var address = physicalAddress + recordLength - fillerLen;
            fillerLen += ETagSize;

            // We don't preserve the ObjectLogPosition field; that's only for serialization. Just set it to 0 here.
            if (DataHeader.RecordHasObjects)
            {
                address -= ObjectLogPositionSize;
                *(ulong*)address = 0;
            }

            // Move Expiration, if present, up to cover ETag; then clear the ETag bit
            var expiration = 0L;
            var expirationSize = 0;
            if (DataHeader.HasExpiration)
            {
                expirationSize = ExpirationSize;
                address -= expirationSize;
                expiration = *(long*)address;
                *(long*)address = 0L;  // To ensure zero-init
            }

            // Expiration will be either zero or a valid expiration, and we have not changed the dataHeader.HasExpiration flag
            address -= ETagSize;
            *(long*)address = expiration;       // will be 0 or a valid expiration
            address += expirationSize;          // repositions to fillerAddress if expirationSize is nonzero
            DataHeaderRef.ClearHasETag();

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to NotSet.
            if (DataHeader.RecordHasObjects)
                *(ulong*)address = ObjectLogFilePositionInfo.NotSet;

            DataHeaderRef.SetFiller(physicalAddress, fillerLen);
            return true;
        }

        /// <summary>
        /// Set the Expiration, checking for space for optionals.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetExpiration(long expiration)
        {
            if (expiration == NoExpiration)
                return RemoveExpiration();

            var optionalStartAddress = GetOptionalStartAddress();
            if (DataHeader.HasExpiration)
            {
                *(long*)GetExpirationAddress(optionalStartAddress) = expiration;
                return true;
            }

            var dataHeader = DataHeader;
            var recordLength = dataHeader.GetRecordLength(physicalAddress);

            // We're adding an Expiration where there wasn't one before.
            var fillerLen = dataHeader.GetTotalFillerLength(physicalAddress);
            // We'll keep the original FillerLen address and back up, for speed.
            var address = physicalAddress + recordLength - fillerLen;
            fillerLen -= ExpirationSize;
            if (fillerLen < 0)
                return false;

            // We don't preserve the ObjectLogPosition field; that's only for serialization.
            if (DataHeader.RecordHasObjects)
                address -= ObjectLogPositionSize;

            // Set the Expiration
            DataHeaderRef.SetHasExpiration();
            *(long*)address = expiration;
            address += ExpirationSize;

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to NotSet.
            if (DataHeader.RecordHasObjects)
                *(ulong*)address = ObjectLogFilePositionInfo.NotSet;

            DataHeaderRef.SetFiller(physicalAddress, fillerLen);
            return true;
        }

        /// <summary>
        /// Remove the expiration
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool RemoveExpiration()
        {
            if (!DataHeader.HasExpiration)
                return true;

            var dataHeader = DataHeader;
            var recordLength = dataHeader.GetRecordLength(physicalAddress);

            // We're adding an ETag where there wasn't one before.
            var fillerLen = dataHeader.GetTotalFillerLength(physicalAddress);
            // We'll keep the original FillerLen address and back up, for speed.
            var address = physicalAddress + recordLength - fillerLen;
            fillerLen += ExpirationSize;

            // We don't preserve the ObjectLogPosition field; that's only for serialization. Just set it to 0 here.
            if (DataHeader.RecordHasObjects)
            {
                address -= ObjectLogPositionSize;
                *(ulong*)address = 0;
            }

            // Remove Expiration and clear the Expiration bit; this will be the new fillerLenAddress
            address -= ExpirationSize;
            *(long*)address = 0;
            DataHeaderRef.ClearHasExpiration();

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to NotSet.
            if (DataHeader.RecordHasObjects)
                *(ulong*)address = ObjectLogFilePositionInfo.NotSet;

            DataHeaderRef.SetFiller(physicalAddress, fillerLen);
            return true;
        }

        /// <summary>
        /// Copy the entire record values: Value and optionals (ETag, Expiration). Key is not copied as it has already been set into 'this'.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryCopyFrom<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, in RecordSizeInfo sizeInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.DataHeader.ValueIsInline)
            {
                if (!TrySetContentLengths(in sizeInfo))
                    return false;
                srcLogRecord.ValueSpan.CopyTo(ValueSpan);
            }
            else
            {
                if (srcLogRecord.DataHeader.ValueIsOverflow)
                {
                    Debug.Assert(DataHeader.ValueIsOverflow, "Expected this.DataHeader.ValueIsOverflow to be set already");
                    ValueOverflow = srcLogRecord.ValueOverflow;
                }
                else
                {
                    // TODO: Clone the value object here so source and destination have independent
                    // HeapMemorySize fields. Currently both records share the same IHeapObject instance,
                    // which means mutations on the destination affect the source's reported heap size
                    // at eviction time, causing accounting drift in logSizeTracker. A naive Clone()
                    // here causes CanDoHashExpireLTM to crash — needs investigation in a follow-up.
                    Debug.Assert(srcLogRecord.ValueObject is not null, "Expected srcLogRecord.ValueObject to be set (or deserialized) already");
                    if (!TrySetValueObjectAndPrepareOptionals(srcLogRecord.ValueObject, in sizeInfo))
                        return false;
                }
            }
            return TryCopyOptionals(in srcLogRecord, in sizeInfo);
        }

        /// <summary>
        /// Check if there is sufficient space to store an ETag in the log record
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PopulateRecordSizeInfoForIPU(ref RecordSizeInfo sizeInfo)
        {
            Debug.Assert(sizeInfo.word == 0, "RecordSizeInfo should not be resused");

            var dataHeader = DataHeader;
            var (keyLength, existingValueLength) = dataHeader.GetKVLengths(physicalAddress, out var eTagLen, out var expirationLen, out var objectLogPositionLen, out var fillerLen, out _ /*valueAddress*/);

            // The sizeInfo's FieldInfo has already been populated. Key size won't change in IPU.
            var keyOverflowInlineSize = 0;
            if (DataHeader.KeyIsInline)
                sizeInfo.SetKeyIsInline();
            else
                keyOverflowInlineSize = ObjectLogPositionSize;

            // Because this is IPU we are limited in inline value size by the record length less any optional length growth in the sizeInfo.
            // We don't allow non-inline if we have a null objectIdMap. TODO: Need better awareness of actual inline value max length.
            var existingOptionalSize = eTagLen + expirationLen + objectLogPositionLen;

            // sizeInfo.OptionalSize will be nonzero because we've not yet set ValueIsInline so calculate the sizeInfo OptionalSize values directly
            // from its FieldInfo with keyOverflowInlineSize as a proxy for ObjectLogPosition.
            sizeInfo.MaxInlineValueSize = existingValueLength + fillerLen - (sizeInfo.FieldInfo.eTagSize + sizeInfo.FieldInfo.expirationSize + keyOverflowInlineSize - existingOptionalSize);

            if (objectIdMap is null || (!sizeInfo.ValueIsObject && sizeInfo.FieldInfo.ValueSize <= sizeInfo.MaxInlineValueSize))
                sizeInfo.SetValueIsInline();
            var valueSize = sizeInfo.ValueIsInline ? sizeInfo.FieldInfo.ValueSize : ObjectIdMap.ObjectIdSize;

            // Record
            sizeInfo.CalculateSizes(keyLength, valueSize);
        }

        /// <summary>
        /// Check if there is sufficient space to store an ETag in the log record
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long GetValueHeapMemorySize()
        {
            if (DataHeader.ValueIsInline)
                return 0;

            if (DataHeader.ValueIsObject)
                return ValueObject.HeapMemorySize;

            var (_ /*length*/, dataAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
            return objectIdMap.GetOverflowByteArray(*(int*)dataAddress).HeapMemorySize;
        }

        /// <summary>
        /// Check if there is sufficient space to grow by the additional space, both the value and whether we want to have optionals when done.
        /// </summary>
        /// <param name="newValueLength">The inline length of the new value</param>
        /// <param name="newETagLen">If we are going to set the ETag this is <see cref="ETagSize"/>; if we are removing the ETag this is 0; if we're not changing the ETag it's <see cref="ETagLen"/></param>
        /// <param name="newExpirationLen">If we are going to set the Expiration this is <see cref="ExpirationSize"/>; if we are removing the Expiration this is 0; if we're not changing the Expiration it's <see cref="ExpirationLen"/></param>
        /// <param name="valueAddress">The address of the pinned value</param>
        /// <param name="valueLength">The current length of the value</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool CanGrowPinnedValue(int newValueLength, int newETagLen, int newExpirationLen, out long valueAddress, out int valueLength)
        {
            if (!DataHeader.ValueIsInline)
                ThrowTsavoriteException("Cannot call CanGrowInline when !ValueIsInline");

            var dataHeader = DataHeader;
            (_ /*keyLength*/, valueLength) = dataHeader.GetKVLengths(physicalAddress, out var eTagLen, out var expirationLen, out var objectLogPositionLen, out var fillerLen, out valueAddress);

            var growth = (newValueLength - valueLength) + (newETagLen - eTagLen) + (newExpirationLen - expirationLen);
            return growth <= fillerLen;
        }

        /// <summary>
        /// Copy the record optional values (ETag, Expiration)
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TryCopyOptionals<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, in RecordSizeInfo sizeInfo)
            where TSourceLogRecord : ISourceLogRecord
        {
            var srcDataHeader = srcLogRecord.DataHeader;

            // If the source has optionals and the destination wants them, copy them over
            if (!srcDataHeader.HasETag || !sizeInfo.FieldInfo.HasETag)
                _ = RemoveETag();
            else if (!TrySetETag(srcLogRecord.ETag))
                return false;

            if (!srcDataHeader.HasExpiration || !sizeInfo.FieldInfo.HasExpiration)
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
        public readonly void ClearValueIfHeap()
        {
            if (DataHeader.ValueIsInline)
                return;

            // If the key is Heap and we're not clearing it then we don't want to to change ObjectLogPosition and Filler, so just clear the value and return.
            if (!DataHeader.KeyIsInline)
            {
                var dataHeader = DataHeader;
                var (_ /*valueLength*/, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
                LogField.ClearObjectIdAndConvertToInline(ref DataHeaderRef, valueAddress, objectIdMap, isKey: false);
                return;
            }

            // The key is not overflow so we must remove ObjectLogPosition and update filler.
            ClearHeapFields(clearKey: false);
        }

        /// <summary>
        /// Clears any heap-allocated field, Object or Overflow, in the Value and optionally the Key. If we go from 
        /// <see cref="RecordDataHeader.RecordIsInline"/> being false to true, then we need to adjust filler as well.
        /// </summary>
        public readonly void ClearHeapFields(bool clearKey)
        {
            if (DataHeader.RecordIsInline)
                return;

            var dataHeader = DataHeader;
            var fillerLen = dataHeader.GetTotalFillerLength(physicalAddress);

            var (keyLength, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
            var valueAddress = keyAddress + keyLength;

            // If the key is Heap and we're not clearing it then we don't want to to change ObjectLogPosition and Filler, so just clear the value and return.
            if (!clearKey && !DataHeader.KeyIsInline)
            {
                if (!DataHeader.ValueIsInline)
                    LogField.ClearObjectIdAndConvertToInline(ref DataHeaderRef, valueAddress, objectIdMap, isKey: false);
                return;
            }

            // If we're here and the key is overflow we're clearing it.
            if (!DataHeader.KeyIsInline)
            {
                LogField.ClearObjectIdAndConvertToInline(ref DataHeaderRef, keyAddress, objectIdMap, isKey: true);
            }
            if (!DataHeader.ValueIsInline)
                LogField.ClearObjectIdAndConvertToInline(ref DataHeaderRef, valueAddress, objectIdMap, isKey: false);

            // Now update filler to account for removal of ObjectLogPosition
            DataHeaderRef.SetFiller(physicalAddress, fillerLen + ObjectLogPositionSize);
        }

        /// <summary>
        /// For revivification or reuse: the record space has been retrieved from revivification or PendingContext, so prepare it to be passed to initial updaters,
        /// based upon the sizeInfo's key and value lengths.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void PrepareForRevivification(ref RecordSizeInfo sizeInfo)
            => DataHeaderRef.InitializeForRevivification(ref sizeInfo, physicalAddress);

        /// <summary>
        /// Sets the lengths of Overflow Keys and Values and Object values into the disk-image copy of the log record before the main-log page is flushed.
        /// </summary>
        /// <param name="objectLogFilePosition">The starting position of the serialized key and value data in the object log.</param>
        /// <param name="valueObjectLength">The serialized length of the value object if it is an object and not inline or overflow. Overflow
        ///     fields have their length known from the <see cref="OverflowByteArray.Length"/> property.</param>
        /// <remarks>
        /// IMPORTANT: This is only to be called in the disk image copy of the log record, not in the actual log record itself.
        /// </remarks>
        internal readonly void SetObjectLogRecordStartPositionAndLength(in ObjectLogFilePositionInfo objectLogFilePosition, ulong valueObjectLength)
        {
            if (DataHeader.RecordIsInline)   // ValueIsInline is true; if the record is fully inline, we should not be called here
            {
                Debug.Fail("Cannot call SetObjectLogRecordStartPositionAndLength for an inline record");
                return;
            }

            var dataHeader = DataHeader;

            var (valueLength, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);

            // Write ObjectLogPosition (clean, without high-byte trick)
            var objectLogPositionPtr = (ulong*)GetObjectLogPositionAddress(valueAddress + valueLength);
            *objectLogPositionPtr = objectLogFilePosition.word;

            // Write overflow key length to DataHeader (or sentinel if exceeds limit)
            if (DataHeader.KeyIsOverflow)
            {
                var (_, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)keyAddress);
                dataHeader.KeyLength = (LogSettings.KeySizeExceedsHeaderLimit(overflow.Length)
                    ? LogSettings.KeyLengthSentinel
                    : overflow.Length);
            }

            // Write overflow/object value length to DataHeader (or sentinel if exceeds limit)
            if (DataHeader.ValueIsOverflow)
            {
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)valueAddress);
                dataHeader.ValueLength = (LogSettings.ValueSizeExceedsHeaderLimit(overflow.Length)
                    ? LogSettings.ValueLengthSentinel
                    : overflow.Length);
            }
            else if (DataHeader.ValueIsObject)
            {
                dataHeader.ValueLength = (LogSettings.ValueSizeExceedsHeaderLimit((long)valueObjectLength)
                    ? LogSettings.ValueLengthSentinel
                    : (int)valueObjectLength);
            }

            // Atomic write back
            DataHeaderRef = dataHeader;
        }

        /// <summary>
        /// Returns the object log position for the start of the key (if any) and value (if any).
        /// </summary>
        /// <param name="keyLength">Outputs key length; will always be for overflow</param>
        /// <param name="valueObjectLength">Outputs key length; will be for overflow or object</param>
        /// <returns>The object log position for this record</returns>
        internal readonly ulong GetObjectLogRecordStartPositionAndLengths(out int keyLength, out ulong valueObjectLength)
        {
            var dataHeader = DataHeader;
            if (DataHeader.KeyIsOverflow)
                keyLength = dataHeader.GetKeyLengthRaw();  // actual length or sentinel (NOT the effective length)
            else // KeyIsInline is true; keyLength will be ignored
                keyLength = 0;

            if (DataHeader.ValueIsOverflow || DataHeader.ValueIsObject)
                valueObjectLength = (ulong)dataHeader.GetValueLengthRaw();  // actual length or sentinel (NOT the effective length)
            else // ValueIsInline is true; valueLength will be ignored
            {
                valueObjectLength = 0;
                if (DataHeader.RecordIsInline) // If the record is fully inline, we should not be called here
                {
                    Debug.Fail("Cannot call GetObjectLogRecordStartPositionAndLengths for an inline record");
                    return 0;
                }
            }

            return *(ulong*)GetObjectLogPositionAddress(GetOptionalStartAddress());
        }

        /// <summary>
        /// For recovery, we have already deserialized all objects and know their lengths: Overflow is in the Key or Value field,
        /// and Object is in the ObjectLogPosition field. So we can set up the pagePositionInfo for this record directly rather than
        /// re-serializing, which also keeps the objectLogTail consistent.
        /// </summary>
        /// <param name="pagePositionInfo">The cumulative position on the page (starting from the PageHeader)</param>
        /// <remarks>
        /// IMPORTANT: This is only to be called in the disk image copy of the log record, not in the actual log record itself.
        /// </remarks>
        /// <returns>The total "serialized" lengths from this LogRecord; will be 0 for inline records. Caller will adjust for
        ///     segment boundaries.</returns>
        internal readonly ulong SetRecoveredObjectLogRecordStartPosition(ObjectLogFilePositionInfo pagePositionInfo)
        {
            if (DataHeader.RecordIsInline)
            {
                Debug.Fail("Cannot call SetRecoveredObjectLogRecordStartPositionAndLengths for an inline record");
                return 0;
            }

            var dataHeader = DataHeader;
            var (valueLength, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
            var objectLogPositionPtr = (ulong*)GetObjectLogPositionAddress(valueAddress + valueLength);
            ulong objectLengths = 0;

            // In case we're a ValueObject, store off the deserialized length at objectLogPositionPtr before overwriting it with the position in the log file.
            var valueObjectLength = *objectLogPositionPtr;
            *objectLogPositionPtr = pagePositionInfo.word;

            if (DataHeader.KeyIsOverflow)
            {
                var (_ /*keyLength*/, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)keyAddress);
                objectLengths += (uint)overflow.Length;
                if (LogSettings.KeySizeExceedsHeaderLimit(overflow.Length))
                {
                    dataHeader.KeyLength = (LogSettings.KeyLengthSentinel);
                    objectLengths += LogSettings.KeyOverflowPrefixSize;  // account for prefix in object log
                }
                else
                    dataHeader.KeyLength = (overflow.Length);
            }

            if (DataHeader.ValueIsOverflow)
            {
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)valueAddress);
                objectLengths += (uint)overflow.Length;
                if (LogSettings.ValueSizeExceedsHeaderLimit(overflow.Length))
                {
                    dataHeader.ValueLength = (LogSettings.ValueLengthSentinel);
                    objectLengths += LogSettings.ValueOverflowPrefixSize;  // account for prefix in object log
                }
                else
                    dataHeader.ValueLength = (overflow.Length);
            }
            else if (DataHeader.ValueIsObject)
            {
                objectLengths += valueObjectLength;
                // No prefix for value objects; auto-extension handles large objects on the read side
                dataHeader.ValueLength = (LogSettings.ValueSizeExceedsHeaderLimit((long)valueObjectLength)
                    ? LogSettings.ValueLengthSentinel
                    : (int)valueObjectLength);
            }

            // Atomic write back
            DataHeaderRef = dataHeader;
            return objectLengths;
        }

        /// <summary>
        /// Called after <see cref="ObjectLogReader{TStoreFunctions}"/> completes deserialization of a record's objects.
        /// Resets non-inline length fields in <see cref="RecordDataHeader"/> back to <see cref="ObjectIdMap.ObjectIdSize"/>
        /// so that in-memory record length calculations work correctly.
        /// </summary>
        internal readonly void OnObjectReadComplete()
        {
            var dataHeader = DataHeader;
            var modified = false;
            if (!dataHeader.KeyIsInline)
            {
                dataHeader.KeyLength = (ObjectIdMap.ObjectIdSize);
                modified = true;
            }
            if (!dataHeader.ValueIsInline)
            {
                dataHeader.ValueLength = (ObjectIdMap.ObjectIdSize);
                modified = true;
            }
            if (modified)
                DataHeaderRef = dataHeader;
        }

        internal readonly void OnDeserializationError(bool keyWasSet)
        {
            // If the key was set, clear it. Then set things as inline so we don't try to release objects on Dispose().
            // This is a transient logRecord, so it is no problem to clear these fields.
            var dataHeader = DataHeader;
            var (keyLength, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
            if (keyWasSet)
                LogField.ClearObjectIdAndConvertToInline(ref DataHeaderRef, keyAddress, objectIdMap, isKey: true);
            else if (!DataHeader.KeyIsInline)
            {
                DataHeaderRef.SetKeyIsInline();
            }

            // Value length may not be ObjectIdSize.
            if (!DataHeader.ValueIsInline)
            {
                var valueAddress = keyAddress + keyLength;
                *(int*)valueAddress = ObjectIdMap.InvalidObjectId;
                LogField.ClearObjectIdAndConvertToInline(ref DataHeaderRef, valueAddress, objectIdMap, isKey: false);
            }
        }

        /// <summary>
        /// Return the serialized size of the contained logRecord.
        /// </summary>
        public readonly int GetSerializedSize()
        {
            var recordSize = AllocatedSize;
            if (DataHeader.RecordIsInline)
                return recordSize;

            _ = GetObjectLogRecordStartPositionAndLengths(out var keyLength, out var valueLength);
            return recordSize + keyLength + (int)valueLength;
        }

        public readonly long CalculateHeapMemorySize()
        {
            long size = 0;
            if (!Info.Tombstone)
            {
                if (DataHeader.KeyIsOverflow)
                    size += KeyOverflow.HeapMemorySize;

                if (DataHeader.ValueIsOverflow)
                    size += ValueOverflow.HeapMemorySize;
                else if (DataHeader.ValueIsObject)
                {
                    var (_ /*valueLength*/, valueAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
                    var objectId = *(int*)valueAddress;
                    if (objectId != ObjectIdMap.InvalidObjectId)
                    {
                        var valueObject = objectIdMap.GetHeapObject(objectId);
                        if (valueObject is not null)    // ignore deleted values being evicted (they are accounted for by InPlaceDeleter)
                            size += valueObject.HeapMemorySize;
                    }
                }
            }
            return size;
        }

        public readonly void Dispose()
        {
            if (IsSet)
                ClearHeapFields(clearKey: true);
        }

        public override readonly string ToString()
        {
            if (physicalAddress == 0)
                return "<empty>";

            string keyString, valueString;
            try { keyString = SpanByte.ToShortString(Key, 12); }
            catch (Exception ex) { keyString = $"<exception: {ex.Message}>"; }
            try { valueString = DataHeader.ValueIsObject ? "obj" : ValueSpan.ToShortString(20); }
            catch (Exception ex) { valueString = $"<exception: {ex.Message}>"; }

            var dataHeader = DataHeader;
            var keyOid = DataHeader.KeyIsInline ? "na" : (*(int*)dataHeader.GetKeyFieldInfo(physicalAddress).keyAddress).ToString();
            var valOid = DataHeader.ValueIsInline ? "na" : (*(int*)dataHeader.GetValueFieldInfo(physicalAddress).valueAddress).ToString();

            var eTagStr = DataHeader.HasETag ? ETag.ToString() : "na";
            var expirStr = DataHeader.HasExpiration ? Expiration.ToString() : "na";
            return $"ri {Info} | hdr: {dataHeader.ToString(keyString, valueString, physicalAddress)} | OIDs k:{keyOid} v:{valOid} | ETag {eTagStr} Expir {expirStr}";
        }
    }
}