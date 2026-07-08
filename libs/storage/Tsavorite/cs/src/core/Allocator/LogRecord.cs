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
    ///     <item><see cref="RecordDataHeader"/> bytes (including RecordType and Namespace) and lengths; see <see cref="DataHeader"/> header comments for details</item>
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
        /// <remarks>Private as of R9: external callers (and even most LogRecord methods) must not assign directly through
        /// this ref. Build multi-field mutations on a local <see cref="RecordDataHeader"/> snapshot, then publish via
        /// <see cref="SetDataHeader"/>, which guarantees a single atomic 8-byte word write that scanners cannot observe
        /// in a half-updated state.</remarks>
        private readonly ref RecordDataHeader DataHeaderRef => ref *(RecordDataHeader*)DataHeaderAddress;

        /// <summary>
        /// Atomically publish a new <see cref="core.RecordDataHeader"/> via a single 8-byte word write to
        /// <see cref="DataHeaderAddress"/>. This is the supported way to update an RDH after composing multiple
        /// field changes on a local copy; concurrent scanners never observe a half-updated header. For safety,
        /// we do not expose <see cref="DataHeaderRef"/> publicly; <see cref="SetDataHeader(RecordDataHeader)"/>
        /// is the only supported method for updating the header directly, and should be done only when it will not
        /// introduce inconsistency.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void SetDataHeader(RecordDataHeader newHeader) => *(RecordDataHeader*)DataHeaderAddress = newHeader;

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
                    var (length, dataAddress) = DataHeader.GetExtendedNamespaceInfo(physicalAddress);
                    return new ReadOnlySpan<byte>((byte*)dataAddress, length);
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
        /// <remarks>The setter restores <see cref="RecordDataHeader.KeyLength"/> to <see cref="ObjectIdMap.ObjectIdSize"/> if needed
        /// (e.g. when called on a deserialized record whose KeyLength holds the actual overflow length or sentinel from the disk image).
        /// The restoration uses a local + <see cref="SetDataHeader"/> for atomicity. No-op for the common in-memory case where
        /// the raw KeyLength field is already <see cref="ObjectIdMap.ObjectIdSize"/>.</remarks>
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

                // Restore KeyLength to ObjectIdSize for the in-memory invariant (atomic single-write via local + SetDataHeader).
                // No-op when already ObjectIdSize (the common in-memory path); only writes when called on a deserialized record
                // whose KeyLength held the actual overflow length or sentinel from the disk image.
                var localDataHeader = DataHeader;
                if (localDataHeader.GetKeyLengthRaw() != ObjectIdMap.ObjectIdSize)
                {
                    localDataHeader.KeyLength = ObjectIdMap.ObjectIdSize;
                    SetDataHeader(localDataHeader);
                }
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
        /// <remarks>Also restores <see cref="RecordDataHeader.ValueLength"/> to <see cref="ObjectIdMap.ObjectIdSize"/> if the raw stored
        /// length is not already ObjectIdSize (deserialization-time invariant restoration; atomic via local + <see cref="SetDataHeader"/>).</remarks>
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

            // Restore raw ValueLength to ObjectIdSize for the in-memory invariant if needed (atomic via local + SetDataHeader).
            var localDataHeader = DataHeader;
            if (localDataHeader.GetValueLengthRaw() != ObjectIdMap.ObjectIdSize)
            {
                localDataHeader.ValueLength = ObjectIdMap.ObjectIdSize;
                SetDataHeader(localDataHeader);
            }
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
        /// <remarks>The setter restores <see cref="RecordDataHeader.ValueLength"/> to <see cref="ObjectIdMap.ObjectIdSize"/> if needed
        /// (e.g. when called on a deserialized record whose ValueLength holds the actual overflow length or sentinel from the disk image).
        /// The restoration uses a local + <see cref="SetDataHeader"/> for atomicity. No-op for the common in-memory case where
        /// the raw ValueLength field is already <see cref="ObjectIdMap.ObjectIdSize"/>.</remarks>
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
                    ThrowTsavoriteException("set_ValueOverflow should only be called when transferring into a new record with ValueIsOverflow == true and value.Length==ObjectIdSize");
                *(int*)dataAddress = objectIdMap.AllocateAndSet(value);

                // Restore ValueLength to ObjectIdSize for the in-memory invariant (atomic single-write via local + SetDataHeader).
                // No-op when already ObjectIdSize (the common in-memory path); only writes when called on a deserialized record
                // whose ValueLength held the actual overflow length or sentinel from the disk image.
                var localDataHeader = DataHeader;
                if (localDataHeader.GetValueLengthRaw() != ObjectIdMap.ObjectIdSize)
                {
                    localDataHeader.ValueLength = ObjectIdMap.ObjectIdSize;
                    SetDataHeader(localDataHeader);
                }
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
                KeySize = DataHeader.KeyIsInline ? dataHeader.GetKeyLengthRaw() : KeyOverflow.Length,
                ValueSize = DataHeader.ValueIsInline ? dataHeader.GetValueLengthRaw() : (dataHeader.ValueIsObject ? ObjectIdMap.ObjectIdSize : ValueOverflow.Length),
                ExtendedNamespaceSize = dataHeader.ExtendedNamespaceLength,
                ValueIsObject = dataHeader.ValueIsObject,
                HasETag = dataHeader.HasETag,
                HasExpiration = dataHeader.HasExpiration,
                RecordType = dataHeader.RecordType,
            };
        }

        /// <inheritdoc/>
        public readonly int AllocatedSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Info.IsNull ? RecordInfo.Size : DataHeader.GetRecordLength();
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
            return (*(RecordDataHeader*)(physicalAddress + RecordInfo.Size)).GetRecordLength();
        }

        #endregion // ISourceLogRecord

        /// <summary>
        /// Computes the expected allocated record size from component lengths and optional-field expectations, without requiring
        /// an actual record in memory. This uses the same layout math as <see cref="RecordSizeInfo.CalculateSizes"/>.
        /// </summary>
        /// <param name="keyDataLength">Length of the key data in bytes.</param>
        /// <param name="valueDataLength">Length of the value data in bytes. For object values, use <see cref="ObjectIdMap.ObjectIdSize"/>.</param>
        /// <param name="extendedNamespaceLength">Length of any extended namespace data preceding the key (0 if none).</param>
        /// <param name="expectETag">Whether the record is expected to have an ETag optional field.</param>
        /// <param name="expectExpiration">Whether the record is expected to have an Expiration optional field.</param>
        /// <param name="expectObject">Whether the record is expected to have an object (key overflow, value overflow, or value object),
        ///     which requires an <see cref="ObjectLogPositionSize"/>-byte object-log position field.</param>
        /// <returns>The expected allocated (record-aligned) size in bytes, including <see cref="RecordInfo"/> header.</returns>
        public static int GetExpectedIORecordSize(int keyDataLength, int valueDataLength, int extendedNamespaceLength = 0,
            bool expectETag = false, bool expectExpiration = false, bool expectObject = false)
        {
            var optionalSize = (expectETag ? ETagSize : 0) + (expectExpiration ? ExpirationSize : 0) + (expectObject ? ObjectLogPositionSize : 0);
            var actualSize = Constants.FixedHeaderSize + extendedNamespaceLength + keyDataLength + valueDataLength + optionalSize;
            return Utility.RoundUp(actualSize, Constants.kRecordAlignment);
        }

        /// <summary>Set the filler length on the RDH for a record. Used only by <see cref="DiskLogRecord.DirectCopyInlinePortionOfRecord"/>
        /// when constructing a LogRecord over a transient output buffer (NOT a live log record), so no concurrent scanner exists.
        /// <see cref="RecordDataHeader.SetFiller"/> performs its own atomic word update.</summary>
        internal readonly void SetFillerLength(int newFillerLen)
        {
            DataHeaderRef.SetFiller(physicalAddress, newFillerLen);
        }

        /// <summary>
        /// Initialize record for <see cref="ObjectAllocator{TStoreFunctions}"/>--includes Overflow option for Key and Overflow and Object option for Value
        /// </summary>
        /// <remarks>
        /// Atomicity: builds the new RDH in a local via <see cref="RecordDataHeader.Initialize"/>, performs all data writes
        /// (key copy, namespace copy, overflow allocation, object slot init) into the record body, then publishes the RDH
        /// via <see cref="SetDataHeader"/> in a single atomic 8-byte word write. A concurrent scanner sees either the
        /// pre-Initialize zero RDH (16-byte min-record advance via the <see cref="RecordDataHeader.GetRecordLength"/> guard)
        /// or the fully-formed post-Initialize state.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeRecord<TKey>(TKey key, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            // Build the full RDH in a local (Initialize folds indicator bits + lengths + namespace + recordType + FillerWords
            // into a SINGLE atomic word write at the end). We pass `physicalAddress` as baseAddress and the returned addresses
            // are within it (NOT addresses based on the stack variable). Any record-split work for over-large filler writes the
            // new invalid record before this RDH is published.
            var localDataHeader = default(RecordDataHeader);
            _ = localDataHeader.Initialize(in sizeInfo, out var keyAddress, out var namespaceAddress, out var valueAddress, physicalAddress);

            // Publish the RDH atomically — single 8-byte write makes the fully-formed record visible to scanners. Do this before serializing the key,
            // so that we have the proper record length set rather than stepping over zeros and encountering key bytes instead.
            SetDataHeader(localDataHeader);

            if (sizeInfo.KeyIsInline)
            {
                Debug.Assert(key.KeyBytes.Length == sizeInfo.InlineKeySize, $"Key length {key.KeyBytes.Length} != sizeInfo.InlineKeySize {sizeInfo.InlineKeySize}");
                key.KeyBytes.CopyTo(new Span<byte>((byte*)keyAddress, sizeInfo.InlineKeySize));
            }
            else
            {
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

                namespaceBytes.CopyTo(new Span<byte>((byte*)namespaceAddress, namespaceBytes.Length));
            }

            // Initialize Value metadata (no value data yet; that's done in ISessionFunctions).
            if (!sizeInfo.ValueIsInline)
            {
                if (!sizeInfo.ValueIsObject)
                {
                    // Overflow: allocate the OverflowByteArray slot now (no value data yet).
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
                }
            }
        }

        /// <summary>
        /// Initialize record for <see cref="SpanByteAllocator{TStoreFunctions}"/>--does not include Overflow/Object options so is streamlined
        /// </summary>
        /// <remarks>
        /// Atomicity: same pattern as the ObjectAllocator overload — build RDH in a local, write key/namespace data,
        /// then publish via <see cref="SetDataHeader"/>.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void InitializeRecord<TKey>(TKey key, in RecordSizeInfo sizeInfo)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
        {
            // Build the full RDH in a local; SpanByteAllocator records are always inline keys + inline values, so
            // sizeInfo.KeyIsInline and sizeInfo.ValueIsInline must both be true (Initialize encodes both). We pass
            // `physicalAddress` as baseAddress and the returned addresses are within it (NOT addresses based on the stack variable).
            // Any record-split work for over-large filler writes the new invalid record before this RDH is published.
            var localDataHeader = default(RecordDataHeader);
            _ = localDataHeader.Initialize(in sizeInfo, out var keyAddress, out var namespaceAddress, out _ /*valueAddress*/, physicalAddress);

            // Publish the RDH atomically — single 8-byte write makes the fully-formed record visible to scanners. Do this before serializing the key,
            // so that we have the proper record length set rather than stepping over zeros and encountering key bytes instead.
            SetDataHeader(localDataHeader);

            // Serialize Key. Do nothing for the value; we've set it inline and the actual value setting is done in ISessionFunctions).
            key.KeyBytes.CopyTo(new Span<byte>((byte*)keyAddress, sizeInfo.InlineKeySize));

            // Serialize namespace, if any
            //
            // Since TKey is generic, the hope is this whole branch gets elided when using a no-namespace key type
            if (key.HasNamespace)
            {
                var namespaceBytes = key.NamespaceBytes;
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
            var localDataHeader = DataHeader;
            var (keyLength, oldInlineValueSize) = localDataHeader.GetKVLengths(physicalAddress, out var oldETagLen, out var oldExpirationLen, out var oldObjectLogPositionLen, out var oldFillerLen, out valueAddress);
            var recordLength = localDataHeader.GetRecordLength();
            var oldOptionalSize = oldETagLen + oldExpirationLen + oldObjectLogPositionLen;
            var extendedNamespaceSize = localDataHeader.ExtendedNamespaceLength;

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
            if (localDataHeader.RecordIsInline && sizeInfo.RecordIsInline && inlineValueGrowth == 0
                    && localDataHeader.HasETag == sizeInfo.FieldInfo.HasETag && localDataHeader.HasExpiration == sizeInfo.FieldInfo.HasExpiration)
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
            optionalFields.Save(optionalStartAddress, localDataHeader);

            // Update record part 2: Do any necessary conversions between Inline, Overflow, and Object. This may allocate or free Heap Objects.
            // All bit-flag changes go into localDataHeader; data-area writes go to the LIVE record. We will install localDataHeader atomically
            // at the end as a single 8-byte write, so the live RDH is always self-consistent for scanners that walk records by computing
            //   record_length = RecordInfo.Size + RDH.Size + ExtendedNamespaceLength + KeyLength + ValueLength + OptionalSize + (FillerWords << kRecordAlignmentShift)
            // The total allocated record length is preserved across the update (the change in ValueLength + optional sizes is offset by an
            // equal change in FillerWords*kRecordAlignment), so a concurrent scanner sees a stable record extent throughout the operation.
            //
            // NOTE: the if/else-if chain below reads the inline/overflow/object bits of localDataHeader BEFORE any mutation; once a branch is
            // entered, the conversion calls and ValueLength assignment may mutate localDataHeader, but we exit the chain immediately and no
            // subsequent branch condition is evaluated, so reads of original state remain accurate within each branch.
            // Evaluate in order of most common (i.e. most perf-critical) cases first.
            if (localDataHeader.ValueIsInline && sizeInfo.ValueIsInline)
            {
                // Both are inline. The conversion routines below would normally update the local's ValueLength; here we must do it explicitly
                // (previously implied by the now-removed RecordLength field).
                localDataHeader.ValueLength = newInlineValueSize;
            }
            else if (localDataHeader.ValueIsOverflow && sizeInfo.ValueIsOverflow)
            {
                // Both are out-of-line, so reallocate in place if needed; the caller will operate on that space after we return.
                _ = LogField.ReallocateValueOverflow(physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                // localDataHeader already reflects ValueIsOverflow + ValueLength == ObjectIdSize
            }
            else if (localDataHeader.ValueIsObject && sizeInfo.ValueIsObject)
            {
                // Both are object records, so nothing to change; the caller will operate on the object after we return.
            }
            else
            {
                // Overflow/Object-ness differs and we've verified there is enough space for the change, so convert. The LogField.ConvertTo* functions copy
                // existing data, as we are likely here for IPU or for the initial update going from inline to overflow with Value length == sizeof(IntPtr).
                if (localDataHeader.ValueIsInline)
                {
                    if (sizeInfo.ValueIsOverflow)
                    {
                        Debug.Assert(inlineValueGrowth == ObjectIdMap.ObjectIdSize - oldInlineValueSize,
                                    $"ValueGrowth {inlineValueGrowth} does not equal expected {oldInlineValueSize - ObjectIdMap.ObjectIdSize}");
                        _ = LogField.ConvertInlineToOverflow(ref localDataHeader, physicalAddress, valueAddress, oldInlineValueSize, in sizeInfo, objectIdMap);
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsObject, "Expected ValueIsObject to be set, pt 1");
                        _ = LogField.ConvertInlineToValueObject(ref localDataHeader, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                    }
                    // After conversion away from inline, set ValueLength to ObjectIdSize to match the post-conversion state.
                    localDataHeader.ValueLength = ObjectIdMap.ObjectIdSize;
                }
                else if (localDataHeader.ValueIsOverflow)
                {
                    if (sizeInfo.ValueIsInline)
                    {
                        _ = LogField.ConvertOverflowToInline(ref localDataHeader, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                        localDataHeader.ValueLength = newInlineValueSize;
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsObject, "Expected ValueIsObject to be set, pt 2");
                        _ = LogField.ConvertOverflowToValueObject(ref localDataHeader, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                        // ObjectId-sized; localDataHeader.ValueLength remains ObjectIdSize
                    }
                }
                else
                {
                    Debug.Assert(localDataHeader.ValueIsObject, "Expected ValueIsObject to be set, pt 3");

                    if (sizeInfo.ValueIsInline)
                    {
                        _ = LogField.ConvertValueObjectToInline(ref localDataHeader, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                        localDataHeader.ValueLength = newInlineValueSize;
                    }
                    else
                    {
                        Debug.Assert(sizeInfo.ValueIsOverflow, "Expected ValueIsOverflow to be true");
                        _ = LogField.ConvertValueObjectToOverflow(ref localDataHeader, physicalAddress, valueAddress, in sizeInfo, objectIdMap);
                        // ObjectId-sized; localDataHeader.ValueLength remains ObjectIdSize
                    }
                }
            }

            // Update record part 3: Restore optionals to their new location. If we have some optionals in sizeInfo that weren't in the record previously, they'll get
            // their default values; subsequently, the caller should set them to the actual values. We have to do this even if not sizeInfo.HasOptionalFields because
            // this also sets or clears optional flags.
            optionalStartAddress += inlineValueGrowth;
            optionalFields.Restore(optionalStartAddress, in sizeInfo, ref localDataHeader);

            // Update Filler length on the local DataHeader. Optional data size for ETag/Expiration is unchanged even if newOptionalSize != oldOptionalSize,
            // because we are not updating those optionals here, so don't adjust fillerLen for that. However, a change in the presence or absence of the pseudo-optional
            // ObjectLogPosition must be accounted for if we have changed whether the record is inline or has objects. The new total filler offsets the inline-value
            // growth and optional growth so the overall allocated record length is preserved.
            var newFillerLen = oldFillerLen - inlineValueGrowth - optionalGrowth;
            if (newFillerLen != oldFillerLen)
                localDataHeader.SetFiller(physicalAddress, newFillerLen > 0 ? newFillerLen : 0);
            if (zeroInit && inlineValueGrowth > 0)
            {
                // Zeroinit any extra space we grew the value by. For example, if we grew by one byte we might have a stale fillerLength in that byte.
                new Span<byte>((byte*)(valueAddress + oldInlineValueSize), newInlineValueSize - oldInlineValueSize).Clear();
            }

            // Update record part 4: All data movement is complete; atomically install the fully-updated header in a single 8-byte write.
            // This is the moment at which all the changes (ValueLength, optional flags, FillerWords, inline/object/overflow bits) become visible
            // to concurrent readers. The new RDH is self-consistent (its KeyLength + ValueLength + optionals + filler all sum to the same allocated
            // record length the old RDH represented), so a scanner reading either the pre- or post-install RDH gets a consistent record extent.
            SetDataHeader(localDataHeader);

            Debug.Assert(localDataHeader.ValueIsInline == sizeInfo.ValueIsInline, "Final ValueIsInline is inconsistent");
            Debug.Assert(!localDataHeader.ValueIsInline || ValueSpan.Length <= sizeInfo.MaxInlineValueSize, $"Inline ValueSpan.Length {ValueSpan.Length} is greater than sizeInfo.MaxInlineValueSpanSize {sizeInfo.MaxInlineValueSize}");
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

            // Snapshot DataHeader and existing filler.
            var localDataHeader = DataHeader;
            int oldFillerLen;
            if (!localDataHeader.HasOptionalOrObjectFields)
            {
                oldFillerLen = localDataHeader.GetTotalFillerLength();
                if (oldFillerLen < inlineValueGrowth)
                    return false;
            }
            else
            {
                _ = localDataHeader.GetKVLengths(physicalAddress, out var oldETagLen, out var oldExpirationLen, out var oldObjectLogPositionLen, out oldFillerLen, out _ /*valueAddress*/);
                if (oldFillerLen < inlineValueGrowth)
                    return false;
                var oldOptionalSize = oldETagLen + oldExpirationLen + oldObjectLogPositionLen;

                // Shift optionals in the data area to make room for the value-length change. This is only needed when there
                // ARE optionals in the live record; the no-optionals branch above already verified we can grow without moving anything.
                if (oldOptionalSize != 0)
                {
                    var optionalStartAddress = valueAddress + valueLength;
                    Buffer.MemoryCopy((void*)optionalStartAddress, (void*)(optionalStartAddress + inlineValueGrowth), oldOptionalSize, oldOptionalSize);
                }
            }

            // Zeroinit any extra space we grew the value by. For example, if we grew by one byte we might have a stale fillerLength in that byte.
            if (zeroInit && inlineValueGrowth > 0)
                new Span<byte>((byte*)(valueAddress + valueLength), inlineValueGrowth).Clear();

            // Update the local DataHeader: new ValueLength and Filler. Optional flags and Key/Namespace/RecordType are unchanged.
            // The single atomic install below publishes all changes (ValueLength + FillerWords) consistently; the change in ValueLength is exactly offset
            // by the change in FillerWords*kRecordAlignment so the derived recordLength is preserved for any concurrent scanner.
            localDataHeader.ValueLength = newValueSize;
            localDataHeader.SetFiller(physicalAddress, oldFillerLen - inlineValueGrowth);

            // Atomic install of the real DataHeader.
            SetDataHeader(localDataHeader);

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
            // This is called when reinitializing a record for InitialUpdater or InitialWriter; we don't want them to see initial state with optionals set.
            // Because it is for (re)initialization, we don't zero-initialize; the caller should assume they have to do that if they only copy partial data in.
            //
            // Atomicity: snapshot DataHeader into a local; compute new layout (clear ETag/Expiration flags, set ValueLength, set FillerWords);
            // publish the full new RDH via SetDataHeader in a single 8-byte word write.
            var localDataHeader = DataHeader;

            // Compute new value address using the current key/namespace layout (which is unchanged).
            var (_ /*oldValueLength*/, valueAddress) = localDataHeader.GetValueFieldInfo(physicalAddress);
            var recordLength = localDataHeader.GetRecordLength();
            var fillerLength = (int)(physicalAddress + recordLength - (valueAddress + sizeInfo.InlineValueSize));
            if (fillerLength < 0)
                return false;

            // Clear optional flags + update ValueLength on the local; SetFiller computes FillerWords from the local's new
            // unalignedSum and writes them into the local's word.
            localDataHeader.ClearHasETag();
            localDataHeader.ClearHasExpiration();
            localDataHeader.ValueLength = sizeInfo.InlineValueSize;
            localDataHeader.SetFiller(physicalAddress, fillerLength);

            // Single atomic publish.
            SetDataHeader(localDataHeader);
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
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.
        /// <para>Atomicity: snapshot DataHeader into a local; shift in-record optional data BEFORE mutating the local; mutate
        /// the local (SetHasETag + SetFiller); publish via SetDataHeader. The in-record data shifting must precede the RDH
        /// publish so a concurrent scanner that observes the new RDH (HasETag set) sees consistent ETag bytes.</para></remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool TrySetETag(long eTag)
        {
            var optionalStartAddress = GetOptionalStartAddress();
            if (DataHeader.HasETag)
            {
                *(long*)GetETagAddress(optionalStartAddress) = eTag;
                return true;
            }

            var localDataHeader = DataHeader;
            var recordLength = localDataHeader.GetRecordLength();

            // We're adding an ETag where there wasn't one before.
            var fillerLen = localDataHeader.GetTotalFillerLength();
            // We'll keep the original FillerLen address and back up, for speed.
            var address = physicalAddress + recordLength - fillerLen;
            fillerLen -= ETagSize;
            if (fillerLen < 0)
                return false;

            // We don't preserve the ObjectLogPosition field; that's only for serialization.
            if (localDataHeader.RecordHasObjects)
                address -= ObjectLogPositionSize;

            // Preserve Expiration if present; set ETag; re-enter Expiration if present
            var expiration = 0L;
            if (localDataHeader.HasExpiration)
            {
                address -= ExpirationSize;
                expiration = *(long*)address;
            }

            // Set the eTag in-record (BEFORE publishing the new RDH).
            *(long*)address = eTag;
            address += ETagSize;

            // Restore expiration, if any
            if (localDataHeader.HasExpiration)
            {
                *(long*)address = expiration;   // will be 0 or a valid expiration
                address += ExpirationSize;      // repositions to ObjectLogPosition address
            }

            // ObjectLogPosition is not preserved (it's only for serialization) so set it to NotSet.
            if (localDataHeader.RecordHasObjects)
                *(ulong*)address = ObjectLogFilePositionInfo.NotSet;

            // Mutate the local (SetHasETag + SetFiller), then publish atomically.
            localDataHeader.SetHasETag();
            localDataHeader.SetFiller(physicalAddress, fillerLen);
            SetDataHeader(localDataHeader);
            return true;
        }

        /// <summary>
        /// Remove the ETag.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.
        /// <para>Atomicity: snapshot DataHeader into a local; shift in-record optional data first; mutate the local
        /// (ClearHasETag + SetFiller); publish via SetDataHeader.</para></remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool RemoveETag()
        {
            if (!DataHeader.HasETag)
                return true;

            var localDataHeader = DataHeader;
            var recordLength = localDataHeader.GetRecordLength();

            // We're removing an ETag.
            var fillerLen = localDataHeader.GetTotalFillerLength();
            // We'll keep the original FillerLen address and back up, for speed.
            var address = physicalAddress + recordLength - fillerLen;
            fillerLen += ETagSize;

            // We don't preserve the ObjectLogPosition field; that's only for serialization. Just set it to 0 here.
            if (localDataHeader.RecordHasObjects)
            {
                address -= ObjectLogPositionSize;
                *(ulong*)address = 0;
            }

            // Move Expiration, if present, up to cover ETag; then clear the ETag bit
            var expiration = 0L;
            var expirationSize = 0;
            if (localDataHeader.HasExpiration)
            {
                expirationSize = ExpirationSize;
                address -= expirationSize;
                expiration = *(long*)address;
                *(long*)address = 0L;  // To ensure zero-init
            }

            // Expiration will be either zero or a valid expiration, and we have not changed the localDataHeader.HasExpiration flag
            address -= ETagSize;
            *(long*)address = expiration;       // will be 0 or a valid expiration
            address += expirationSize;          // repositions to fillerAddress if expirationSize is nonzero

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to NotSet.
            if (localDataHeader.RecordHasObjects)
                *(ulong*)address = ObjectLogFilePositionInfo.NotSet;

            // Mutate the local (ClearHasETag + SetFiller), then publish atomically.
            localDataHeader.ClearHasETag();
            localDataHeader.SetFiller(physicalAddress, fillerLen);
            SetDataHeader(localDataHeader);
            return true;
        }

        /// <summary>
        /// Set the Expiration, checking for space for optionals.
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.
        /// <para>Atomicity: snapshot DataHeader into a local; shift in-record optional data first; mutate the local
        /// (SetHasExpiration + SetFiller); publish via SetDataHeader.</para></remarks>
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

            var localDataHeader = DataHeader;
            var recordLength = localDataHeader.GetRecordLength();

            // We're adding an Expiration where there wasn't one before.
            var fillerLen = localDataHeader.GetTotalFillerLength();
            // We'll keep the original FillerLen address and back up, for speed.
            var address = physicalAddress + recordLength - fillerLen;
            fillerLen -= ExpirationSize;
            if (fillerLen < 0)
                return false;

            // We don't preserve the ObjectLogPosition field; that's only for serialization.
            if (localDataHeader.RecordHasObjects)
                address -= ObjectLogPositionSize;

            // Set the Expiration in-record (BEFORE publishing the new RDH).
            *(long*)address = expiration;
            address += ExpirationSize;

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to NotSet.
            if (localDataHeader.RecordHasObjects)
                *(ulong*)address = ObjectLogFilePositionInfo.NotSet;

            // Mutate the local (SetHasExpiration + SetFiller), then publish atomically.
            localDataHeader.SetHasExpiration();
            localDataHeader.SetFiller(physicalAddress, fillerLen);
            SetDataHeader(localDataHeader);
            return true;
        }

        /// <summary>
        /// Remove the expiration
        /// </summary>
        /// <remarks>This is 'readonly' because it does not alter the fields of this object, only what they point to.
        /// <para>Atomicity: snapshot DataHeader into a local; shift in-record optional data first; mutate the local
        /// (ClearHasExpiration + SetFiller); publish via SetDataHeader.</para></remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool RemoveExpiration()
        {
            if (!DataHeader.HasExpiration)
                return true;

            var localDataHeader = DataHeader;
            var recordLength = localDataHeader.GetRecordLength();

            // We're removing an Expiration.
            var fillerLen = localDataHeader.GetTotalFillerLength();
            // We'll keep the original FillerLen address and back up, for speed.
            var address = physicalAddress + recordLength - fillerLen;
            fillerLen += ExpirationSize;

            // We don't preserve the ObjectLogPosition field; that's only for serialization. Just set it to 0 here.
            if (localDataHeader.RecordHasObjects)
            {
                address -= ObjectLogPositionSize;
                *(ulong*)address = 0;
            }

            // Remove Expiration; this will be the new fillerLenAddress
            address -= ExpirationSize;
            *(long*)address = 0;

            // ObjectLogPosition is not preserved (it's only for serialization) but we set it to NotSet.
            if (localDataHeader.RecordHasObjects)
                *(ulong*)address = ObjectLogFilePositionInfo.NotSet;

            // Mutate the local (ClearHasExpiration + SetFiller), then publish atomically.
            localDataHeader.ClearHasExpiration();
            localDataHeader.SetFiller(physicalAddress, fillerLen);
            SetDataHeader(localDataHeader);
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
        /// <remarks>Atomicity: snapshot DataHeader into a local; pass ref to LogField helpers that mutate the local;
        /// publish via SetDataHeader.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void ClearValueIfHeap()
        {
            if (DataHeader.ValueIsInline)
                return;

            // If the key is Heap and we're not clearing it then we don't want to to change ObjectLogPosition and Filler, so just clear the value and return.
            if (!DataHeader.KeyIsInline)
            {
                var localDataHeader = DataHeader;
                var (_ /*valueLength*/, valueAddress) = localDataHeader.GetValueFieldInfo(physicalAddress);
                LogField.ClearObjectIdAndConvertToInline(ref localDataHeader, valueAddress, objectIdMap, isKey: false);
                SetDataHeader(localDataHeader);
                return;
            }

            // The key is not overflow so we must remove ObjectLogPosition and update filler.
            ClearHeapFields(clearKey: false);
        }

        /// <summary>
        /// Clears any heap-allocated field, Object or Overflow, in the Value and optionally the Key. If we go from 
        /// <see cref="RecordDataHeader.RecordIsInline"/> being false to true, then we need to adjust filler as well.
        /// </summary>
        /// <remarks>Atomicity: snapshot DataHeader into a local; pass ref to LogField helpers that mutate the local;
        /// adjust filler on the local for ObjectLogPosition removal; publish via SetDataHeader.</remarks>
        public readonly void ClearHeapFields(bool clearKey)
        {
            if (DataHeader.RecordIsInline)
                return;

            var localDataHeader = DataHeader;

            var (keyLength, keyAddress) = localDataHeader.GetKeyFieldInfo(physicalAddress);
            var valueAddress = keyAddress + keyLength;

            // If the key is Heap and we're not clearing it then we don't want to to change ObjectLogPosition and Filler, so just clear the value and return.
            if (!clearKey && !localDataHeader.KeyIsInline)
            {
                if (!localDataHeader.ValueIsInline)
                    LogField.ClearObjectIdAndConvertToInline(ref localDataHeader, valueAddress, objectIdMap, isKey: false);
                SetDataHeader(localDataHeader);
                return;
            }

            // If we're here and the key is overflow we're clearing it.
            if (!localDataHeader.KeyIsInline)
            {
                LogField.ClearObjectIdAndConvertToInline(ref localDataHeader, keyAddress, objectIdMap, isKey: true);
            }
            if (!localDataHeader.ValueIsInline)
                LogField.ClearObjectIdAndConvertToInline(ref localDataHeader, valueAddress, objectIdMap, isKey: false);

            // Now update filler to account for removal of ObjectLogPosition (mutates local; published below).
            localDataHeader.SetFiller(physicalAddress, localDataHeader.GetTotalFillerLength() + ObjectLogPositionSize);
            SetDataHeader(localDataHeader);
        }

        /// <summary>
        /// For revivification or reuse: the record space has been retrieved from revivification or OperationState, so prepare it to be passed to initial updaters,
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
        /// <para>R11 encoding: for overflow keys, low 12 bits of actual length go into the RDH KeyLength field and the next 32 bits
        /// overwrite the int* at keyAddress (which previously held the objectId) — total 44 bits → 16 TB max key. For overflow values
        /// or object values, low 22 bits go into the RDH ValueLength field and the next 32 bits overwrite the int* at valueAddress —
        /// total 54 bits → 16 PB max. The ObjectLogPosition word has the <see cref="ObjectLogFilePositionInfo.kReuseObjectIdForSizeBit"/>
        /// flag set so the reader knows the encoding to expect.</para>
        /// <para>IMPORTANT: Overwrites the int* slots that held the in-memory objectIds, so this is only safe to call in the disk-image
        /// copy of the log record (srcBuffer), not in the live main-log record.</para>
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

            // Write ObjectLogPosition with the ReuseObjectIdForSize flag set so the reader knows the on-disk encoding.
            var objectLogPositionPtr = (ulong*)GetObjectLogPositionAddress(valueAddress + valueLength);
            *objectLogPositionPtr = objectLogFilePosition.word | ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask;

            // Overflow key: low 12 bits → RDH KeyLength; next 32 bits → int* slot at keyAddress (overwriting the in-memory objectId).
            if (dataHeader.KeyIsOverflow)
            {
                var (_, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)keyAddress);
                var actualKeyLength = (ulong)overflow.Length;
                dataHeader.KeyLength = (int)(actualKeyLength & RecordDataHeader.kKeyLengthLowBitsMask);
                *(int*)keyAddress = (int)(actualKeyLength >> RecordDataHeader.kKeyLengthBits);
            }

            // Overflow value or Object value: low 22 bits → RDH ValueLength; next 32 bits → int* slot at valueAddress.
            if (dataHeader.ValueIsOverflow)
            {
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)valueAddress);
                var actualValueLength = (ulong)overflow.Length;
                dataHeader.ValueLength = (int)(actualValueLength & RecordDataHeader.kValueLengthLowBitsMask);
                *(int*)valueAddress = (int)(actualValueLength >> RecordDataHeader.kValueLengthBits);
            }
            else if (dataHeader.ValueIsObject)
            {
                dataHeader.ValueLength = (int)(valueObjectLength & RecordDataHeader.kValueLengthLowBitsMask);
                *(uint*)valueAddress = (uint)(valueObjectLength >> RecordDataHeader.kValueLengthBits);
            }

            // Atomic publish via SetDataHeader.
            SetDataHeader(dataHeader);
        }

        /// <summary>
        /// Repoints this record's object-log position word to <paramref name="objectLogFilePosition"/> without touching the R11-encoded
        /// key/value lengths (in the RDH fields and the int* slots at keyAddress/valueAddress) or the <see cref="ObjectIdMap"/>.
        /// </summary>
        /// <param name="objectLogFilePosition">The new object-log position (e.g. the main object-log position a snapshot record's bytes were copied to).</param>
        /// <remarks>
        /// Used by the snapshot-recovery flush, which copies a record's object bytes from the snapshot object-log to the main object-log and must
        /// repoint the disk-image record to the main position. The record's objects are NOT deserialized at this point (objectIdMap is empty and the
        /// int* slots still hold the on-disk R11 length high-bits), so unlike <see cref="SetObjectLogRecordStartPositionAndLength"/> and
        /// <see cref="SetRecoveredObjectLogRecordStartPosition"/> this must not read the lengths from objectIdMap. The existing R11 length encoding
        /// is preserved as-is, since the copied lengths are unchanged.
        /// <para>IMPORTANT: Like the other position setters, this is only safe to call on the disk-image copy of the record (srcBuffer).</para>
        /// </remarks>
        internal readonly void RepointObjectLogPosition(in ObjectLogFilePositionInfo objectLogFilePosition)
        {
            if (DataHeader.RecordIsInline)
            {
                Debug.Fail("Cannot call RepointObjectLogPosition for an inline record");
                return;
            }

            var (valueLength, valueAddress) = DataHeader.GetValueFieldInfo(physicalAddress);
            var objectLogPositionPtr = (ulong*)GetObjectLogPositionAddress(valueAddress + valueLength);
            *objectLogPositionPtr = objectLogFilePosition.word | ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask;
        }

        /// <summary>
        /// Returns the object log position for the start of the key (if any) and value (if any), with the length encoded per R11:
        /// (low N bits from RDH KeyLength/ValueLength) + (next 32 bits from int* slot at keyAddress/valueAddress).
        /// </summary>
        /// <param name="keyLength">Outputs key length; will always be for overflow</param>
        /// <param name="valueObjectLength">Outputs value length; will be for overflow or object</param>
        /// <returns>The object log position word for this record, with flag bits masked off (segment+offset only).</returns>
        internal readonly ulong GetObjectLogRecordStartPositionAndLengths(out int keyLength, out ulong valueObjectLength)
        {
            var dataHeader = DataHeader;
            if (dataHeader.KeyIsOverflow)
            {
                var (_ /*kLen*/, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
                // Combine low 12 bits (RDH) with next 32 bits (int* slot at keyAddress).
                var keyHighBits = (ulong)(uint)*(int*)keyAddress;
                var combinedKeyLength = (keyHighBits << RecordDataHeader.kKeyLengthBits) | (ulong)(uint)dataHeader.GetKeyLengthRaw();
                Debug.Assert(combinedKeyLength <= int.MaxValue, $"Key length {combinedKeyLength} exceeds int.MaxValue");
                keyLength = (int)combinedKeyLength;
            }
            else // KeyIsInline is true; keyLength will be ignored
                keyLength = 0;

            var (valueLength, valueAddress) = dataHeader.GetValueFieldInfo(physicalAddress);
            if (dataHeader.ValueIsOverflow || dataHeader.ValueIsObject)
            {
                var valueHighBits = (ulong)(uint)*(int*)valueAddress;
                valueObjectLength = (valueHighBits << RecordDataHeader.kValueLengthBits) | (ulong)(uint)dataHeader.GetValueLengthRaw();
            }
            else // ValueIsInline is true; valueLength will be ignored
            {
                valueObjectLength = 0;
                if (dataHeader.RecordIsInline) // If the record is fully inline, we should not be called here
                {
                    Debug.Fail("Cannot call GetObjectLogRecordStartPositionAndLengths for an inline record");
                    return 0;
                }
            }

            // Read the position word; mask off flag bits to return just segment+offset.
            var word = *(ulong*)GetObjectLogPositionAddress(GetOptionalStartAddress());
            return word & ObjectLogFilePositionInfo.SegmentAndOffsetMask;
        }

        /// <summary>
        /// For recovery, we have already deserialized all objects and know their lengths: Overflow is in the Key or Value field,
        /// and Object is in the ObjectLogPosition field. So we can set up the pagePositionInfo for this record directly rather than
        /// re-serializing, which also keeps the objectLogTail consistent.
        /// </summary>
        /// <param name="pagePositionInfo">The cumulative position on the page (starting from the PageHeader)</param>
        /// <remarks>
        /// IMPORTANT: This is only to be called in the disk image copy of the log record, not in the actual log record itself.
        /// See <see cref="SetObjectLogRecordStartPositionAndLength"/> for encoding details.
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

            // For ValueObject, the deserialized length was stored at objectLogPositionPtr by SetDeserializedValueObject; save it.
            var valueObjectLength = *objectLogPositionPtr;
            *objectLogPositionPtr = pagePositionInfo.word | ObjectLogFilePositionInfo.kReuseObjectIdForSizeMask;

            ulong objectLengths = 0;
            if (dataHeader.KeyIsOverflow)
            {
                var (_ /*kLen*/, keyAddress) = dataHeader.GetKeyFieldInfo(physicalAddress);
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)keyAddress);
                objectLengths += (uint)overflow.Length;
                var actualKeyLength = (ulong)overflow.Length;
                dataHeader.KeyLength = (int)(actualKeyLength & RecordDataHeader.kKeyLengthLowBitsMask);
                *(int*)keyAddress = (int)(actualKeyLength >> RecordDataHeader.kKeyLengthBits);
            }

            if (dataHeader.ValueIsOverflow)
            {
                var overflow = objectIdMap.GetOverflowByteArray(*(int*)valueAddress);
                objectLengths += (uint)overflow.Length;
                var actualValueLength = (ulong)overflow.Length;
                dataHeader.ValueLength = (int)(actualValueLength & RecordDataHeader.kValueLengthLowBitsMask);
                *(int*)valueAddress = (int)(actualValueLength >> RecordDataHeader.kValueLengthBits);
            }
            else if (dataHeader.ValueIsObject)
            {
                objectLengths += valueObjectLength;
                dataHeader.ValueLength = (int)(valueObjectLength & RecordDataHeader.kValueLengthLowBitsMask);
                *(uint*)valueAddress = (uint)(valueObjectLength >> RecordDataHeader.kValueLengthBits);
            }

            // Atomic publish via SetDataHeader.
            SetDataHeader(dataHeader);
            return objectLengths;
        }

        /// <summary>Whether the <c>ReuseObjectIdForSize</c> flag is set on this record's ObjectLogPosition slot. The flag indicates
        /// that overflow/object lengths are encoded as (RDH KeyLength/ValueLength low bits) + (objectId slot high 32 bits), and the
        /// object-log stream contains NO length prefix.</summary>
        internal readonly bool HasReuseObjectIdForSize
            => ObjectLogFilePositionInfo.GetReuseObjectIdForSize((ulong*)GetObjectLogPositionAddress(GetOptionalStartAddress()));

        /// <summary>Set the <c>ReuseObjectIdForSize</c> flag on this record's ObjectLogPosition slot. Must be called before
        /// <see cref="ObjectLogWriter{TStoreFunctions}.WriteRecordObjects"/> to honor the encoding contract.</summary>
        internal readonly void SetReuseObjectIdForSize()
            => ObjectLogFilePositionInfo.SetReuseObjectIdForSize((ulong*)GetObjectLogPositionAddress(GetOptionalStartAddress()));

        /// <summary>
        /// Called after <see cref="ObjectLogReader{TStoreFunctions}"/> completes deserialization of a record's objects.
        /// Restores the raw <see cref="RecordDataHeader.KeyLength"/> / <see cref="RecordDataHeader.ValueLength"/> fields back to
        /// <see cref="ObjectIdMap.ObjectIdSize"/> for non-inline keys/values so the in-memory record length calculations work
        /// correctly. (During disk read, these raw fields hold the low 12/22 bits of the on-disk overflow/object length per the
        /// R11 encoding; runtime code expects them to be ObjectIdSize so the property getter returns ObjectIdSize for non-inline.)
        /// </summary>
        internal readonly void OnObjectReadComplete()
        {
            // Simply assert Key and Value lengths. We should always have the check for !InlineKey/Value returning ObjectIdSize.
            Debug.Assert(DataHeader.KeyIsInline || DataHeader.KeyLength == ObjectIdMap.ObjectIdSize, "Expected KeyLength to always be ObjectIdSize for non-inline key");
            Debug.Assert(DataHeader.ValueIsInline || DataHeader.ValueLength == ObjectIdMap.ObjectIdSize, "Expected ValueLength to always be ObjectIdSize for non-inline value");
#if false
            var dataHeader = DataHeader;
            var modified = false;
            if (!dataHeader.KeyIsInline && dataHeader.GetKeyLengthRaw() != ObjectIdMap.ObjectIdSize)
            {
                dataHeader.KeyLength = ObjectIdMap.ObjectIdSize;
                modified = true;
            }
            if (!dataHeader.ValueIsInline && dataHeader.GetValueLengthRaw() != ObjectIdMap.ObjectIdSize)
            {
                dataHeader.ValueLength = ObjectIdMap.ObjectIdSize;
                modified = true;
            }
            if (modified)
                SetDataHeader(dataHeader);
#endif
        }

        internal readonly void OnDeserializationError(bool keyWasSet)
        {
            // If the key was set, clear it. Then set things as inline so we don't try to release objects on Dispose().
            // This is a transient logRecord, so it is no problem to clear these fields.
            //
            // Atomicity: snapshot DataHeader, mutate local (LogField.ClearObjectIdAndConvertToInline updates local;
            // SetKeyIsInline updates local), then SetDataHeader publishes the new RDH in a single 8-byte write.
            var localDataHeader = DataHeader;
            var (keyLength, keyAddress) = localDataHeader.GetKeyFieldInfo(physicalAddress);

            if (keyWasSet)
            {
                LogField.ClearObjectIdAndConvertToInline(ref localDataHeader, keyAddress, objectIdMap, isKey: true);
            }
            else if (!localDataHeader.KeyIsInline)
            {
                localDataHeader.SetKeyIsInline();
            }

            // Value length may not be ObjectIdSize.
            if (!localDataHeader.ValueIsInline)
            {
                var valueAddress = keyAddress + keyLength;
                *(int*)valueAddress = ObjectIdMap.InvalidObjectId;
                LogField.ClearObjectIdAndConvertToInline(ref localDataHeader, valueAddress, objectIdMap, isKey: false);
            }

            SetDataHeader(localDataHeader);
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
            return $"ri {Info} | hdr: {dataHeader.ToString(keyString, valueString)} | OIDs k:{keyOid} v:{valOid} | ETag {eTagStr} Expir {expirStr}";
        }
    }
}