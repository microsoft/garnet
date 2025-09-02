// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
#pragma warning disable IDE0032 // Use auto property

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static Utility;
    using static VarbyteLengthUtility;

    /// <summary>A record from the disk or network buffer. Because we read in pieces, we isolate the individual pieces so we don't rely on having the full record in memory.</summary>
    /// <remarks>This struct is used as a temporary holding space for pending operations (both holding onto keys on the query side, and the results on the completion side) and iteration,
    /// including for Compaction. This is part of the reason for the separate fields, in particular Overflow; this allows fast transfer of <see cref="OverflowByteArray"/> for Keys and Values
    /// directly to the <see cref="LogRecord"/>, rather than copying the data from a buffer to an Overflow allocation.</remarks>
    public unsafe struct DiskLogRecord : ISourceLogRecord, IDisposable
    {
        /// <summary>If non-null, this is the physicalAddress of the full record data from an externally-held buffer (possibly a <see cref="SectorAlignedMemory"/> or an iterator frame).</summary>
        internal long physicalAddress;

        /// <summary>The record header.</summary>
        RecordInfo recordInfo;

        /// <summary>If not empty, the key is an overflow key. Held directly; does not use <see cref="ObjectIdMap"/>.</summary>
        OverflowByteArray keyOverflow;
        /// <summary>If not empty, the key is part of either <see cref="keyBuffer"/> or <see cref="recordOrValueBuffer"/>, or relative to <see cref="physicalAddress"/>.</summary>
        PinnedSpanByte keySpan;
        /// <summary>We may have had an initial read of just the key, but not the full value. In that case we hung onto the key memory here (and it must be Return()ed).
        /// We may also have allocated <see cref="recordOrValueBuffer"/>.</summary>
        /// <remarks>If Info.KeyIsInline and keyBuffer is null, the key is in recordOrValueBuffer</remarks>
        SectorAlignedMemory keyBuffer;

        /// <summary>If non-null, this is the deserialized ValueObject if Info.ValueIsObject, else the overflow byte[] for the object. Held directly; does not use <see cref="ObjectIdMap"/>.</summary>
        object valueOverflowOrObject;
        /// <summary>If not empty, the value is part of <see cref="recordOrValueBuffer"/>, or relative to <see cref="physicalAddress"/>.</summary>
        PinnedSpanByte valueSpan;
        /// <summary>If this is non-null, it is either the full record (Key and Value) or the Value only. Either way, it must be Return()ed on <see cref="Dispose()"/>.</summary>
        /// <remarks>If this is the full record it includes RecordInfo and Optionals, but those are copied to separate fields as we may not have them in buffers if, for example, there is a value object.</remarks>
        SectorAlignedMemory recordOrValueBuffer;

        // Note: We keep eTag and expiration separately because we can't access them directly in the IHeapObject, and possibly not in the valueOverflow.
        // It would be nice to save this space and the additional RecordInfo and Key/Value Span fields, all of which would add up in iterator frames with
        // large buffers. However, this is used only for ObjectAllocatorIterator frames, not SpanByteAllocator, and we control the size of the read buffer
        // for that, so there is a limit to the number of records at once (which may be large).

        /// <summary>If Info.HasETag, this is the ETag.</summary>
        long eTag;
        /// <summary>If Info.HasExpiration, this is the Expiration.</summary>
        long expiration;

        /// <summary>
        /// If this record was read from the disk, this is its length; used to move to the next record. Note that this may have been filled in by object
        /// deserialization from disk, and thus may not be reflected in <see cref="recordOrValueBuffer"/>.
        /// </summary>
        internal long diskReadLength;

        /// <summary>
        /// Constructor that takes a physical address, which may come from a <see cref="SectorAlignedMemory"/> or some other allocation
        /// that will have at least the lifetime of this <see cref="DiskLogRecord"/>. This <see cref="DiskLogRecord"/> does not own the memory
        /// allocation so will not free it on <see cref="Dispose()"/>.
        /// </summary>
        /// <remarks>This factory function wraps the private ctor so that all calls use this self-documenting method name identifying it as taking a pinned pointer.</remarks>
        /// <param name="physicalAddress">Physical address of the record; may have come from a <see cref="SectorAlignedMemory"/> buffer</param>
        /// <param name="availableBytes">Length of data available at <paramref name="physicalAddress"/></param>
        /// <param name="fieldInfo">Information, such as lengths, about the fields in this record</param>
        /// <param name="valueSerializer">Object serializer; optional, and used only if this record has an object</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static DiskLogRecord CreateFromPinnedPointer(long physicalAddress, int availableBytes, in SerializedFieldInfo fieldInfo, IObjectSerializer<IHeapObject> valueSerializer = null)
        {
            Debug.Assert(fieldInfo.SerializedLength <= availableBytes, $"SerializedLength {fieldInfo.SerializedLength} exceeds availableBytes {availableBytes}");
            Debug.Assert(fieldInfo.isComplete, "caller should have verified isComplete is true");
            var diskLogRecord = new DiskLogRecord(physicalAddress, in fieldInfo);
            if (diskLogRecord.Info.ValueIsObject && valueSerializer is not null)
                _ = diskLogRecord.DeserializeValueObject(valueSerializer);
            diskLogRecord.diskReadLength = fieldInfo.SerializedLength;
            return diskLogRecord;
        }

        /// <summary>
        /// Constructor that takes a physical address, which may come from a <see cref="SectorAlignedMemory"/> or some other allocation
        /// that will have at least the lifetime of this <see cref="DiskLogRecord"/>. This <see cref="DiskLogRecord"/> does not own the memory
        /// allocation so will not free it on <see cref="Dispose()"/>.
        /// </summary>
        /// <remarks>This factory function wraps the private ctor so that all calls use this self-documenting method name identifying it as taking a pinned pointer.</remarks>
        /// <param name="physicalAddress">Physical address of the record; may have come from a <see cref="SectorAlignedMemory"/> buffer</param>
        /// <param name="availableBytes">Length of data available at <paramref name="physicalAddress"/></param>
        /// <param name="diskLogRecord">The resultant DiskLogRecord</param>
        /// <param name="valueSerializer">Object serializer; optional, and used only if this record has an object</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool TryCreateFromPinnedPointer(long physicalAddress, int availableBytes, out DiskLogRecord diskLogRecord, IObjectSerializer<IHeapObject> valueSerializer = null)
        {
            if (!GetSerializedLength(physicalAddress, availableBytes, out var fieldInfo))
            {
                diskLogRecord = default;
                return false;
            }
            diskLogRecord = CreateFromPinnedPointer(physicalAddress, availableBytes, in fieldInfo, valueSerializer);
            return true;
        }

        /// <summary>
        /// Private constructor that takes a physical address, which may come from a <see cref="SectorAlignedMemory"/> or some other allocation
        /// that will have at least the lifetime of this <see cref="DiskLogRecord"/> (it may be <see cref="recordOrValueBuffer"/>).
        /// This <see cref="DiskLogRecord"/> does not own the memory allocation so will not free it on <see cref="Dispose()"/>
        /// (unless it is <see cref="recordOrValueBuffer"/>).
        /// </summary>
        /// <remarks>This is private and called only from the self-documenting method <see cref="CreateFromPinnedPointer"/>.</remarks>
        /// <param name="physicalAddress">Physical address of the record; may have come from a <see cref="SectorAlignedMemory"/> buffer</param>
        /// <param name="fieldInfo">Information, such as lengths, about the fields in this record</param>
        /// <param name="valueObject">The deserialized value object, if any; the record bytestream at <paramref name="physicalAddress"/> may contain the serialized value bytes as well</param>
        private DiskLogRecord(long physicalAddress, in SerializedFieldInfo fieldInfo, IHeapObject valueObject = null)
        {
            CopyFrom(physicalAddress, in fieldInfo, valueObject);
        }

        /// <summary>
        /// Tests whether the entire record is available at <paramref name="physicalAddress"/>, and returns the serialized length if <paramref name="availableBytes"/> is sufficient to read the varbytes.
        /// </summary>
        /// <param name="physicalAddress">Physical address of the record; may have come from a <see cref="SectorAlignedMemory"/> buffer</param>
        /// <param name="availableBytes">Length of data available at <paramref name="physicalAddress"/></param>
        /// <param name="fieldInfo">Output information about the serialized fields, such as lengths, as much as <paramref name="availableBytes"/> is sufficient to read them</param>
        /// <returns>True if the record is or may be valid (<see cref="SerializedFieldInfo.isComplete"/> indicates whether it is complete which includes whether the full RecordInfo was available), else false</returns>
        internal static bool GetSerializedLength(long physicalAddress, int availableBytes, out SerializedFieldInfo fieldInfo)
        {
            fieldInfo = default;
            if (availableBytes >= RecordInfo.GetLength() + 1 + 2) // + 1 for indicator byte + the minimum of 2 1-byte lengths for key and value
            {
                var ptr = (byte*)physicalAddress;
                (var keyLengthBytes, var valueLengthBytes, fieldInfo.isChunkedValue) = DeconstructIndicatorByte(*(ptr + RecordInfo.GetLength()));
                Debug.Assert(!fieldInfo.isChunkedValue, "This should not be called in contexts where isChunkedValue may be true (which is just the disk IO processing in DiskStreamReadBuffer.Read())");
                fieldInfo.recordInfo = *(RecordInfo*)ptr;
                if (fieldInfo.recordInfo.Invalid)
                    return false;
                fieldInfo.optionalLength = LogRecord.GetOptionalLength(fieldInfo.recordInfo);

                fieldInfo.offsetToKeyStart = RecordInfo.GetLength() + 1 + keyLengthBytes + valueLengthBytes;
                if (availableBytes >= fieldInfo.offsetToKeyStart)
                {
                    fieldInfo.keyLength = GetKeyLength(keyLengthBytes, ptr + RecordInfo.GetLength() + 1);
                    fieldInfo.valueLength = GetValueLength(valueLengthBytes, ptr + RecordInfo.GetLength() + 1 + keyLengthBytes);
                    fieldInfo.isComplete = !fieldInfo.isChunkedValue && fieldInfo.SerializedLength <= availableBytes;
                }
            }
            return true;
        }

        /// <summary>
        /// Gets the length if we are a single complete stream of bytes. We may have only <see cref="physicalAddress"/>, and do not carry a field
        /// for serializedLength, so we assume in that case that we are complete, and we get the lengths from the indicator bytes without checking 
        /// for sufficient space.
        /// </summary>
        /// <param name="fieldInfo">Output information about the serialized fields, such as lengths</param>
        /// <returns>True if the record is valid, with (<see cref="SerializedFieldInfo.isComplete"/> set true</returns>
        internal readonly bool GetCompleteSingleStreamSerializedFieldInfo(out SerializedFieldInfo fieldInfo)
        {
            fieldInfo = default;
            if (!IsSingleCompleteStream)
                return false;
            var ptr = (byte*)physicalAddress;
            (var keyLengthBytes, var valueLengthBytes, fieldInfo.isChunkedValue) = DeconstructIndicatorByte(*(ptr + RecordInfo.GetLength()));
            Debug.Assert(!fieldInfo.isChunkedValue, "This should not be called in contexts where isChunkedValue may be true (which is just the disk IO processing in DiskStreamReadBuffer.Read())");
            fieldInfo.recordInfo = *(RecordInfo*)ptr;
            if (fieldInfo.recordInfo.Invalid)
                return false;
            fieldInfo.optionalLength = LogRecord.GetOptionalLength(fieldInfo.recordInfo);

            fieldInfo.offsetToKeyStart = RecordInfo.GetLength() + 1 + keyLengthBytes + valueLengthBytes;
            fieldInfo.keyLength = GetKeyLength(keyLengthBytes, ptr + RecordInfo.GetLength() + 1);
            fieldInfo.valueLength = GetValueLength(valueLengthBytes, ptr + RecordInfo.GetLength() + 1 + keyLengthBytes);
            fieldInfo.isComplete = true;
            return true;
        }

        /// <summary>
        /// Static factory that takes a <see cref="SectorAlignedMemory"/> from which it obtains the physical address.
        /// This <see cref="DiskLogRecord"/> owns the memory allocation and must free it on <see cref="Dispose()"/>.
        /// </summary>
        internal static DiskLogRecord Transfer(ref SectorAlignedMemory allocatedRecord, int offsetToKeyStart, int keyLength, int valueLength, IHeapObject valueObject = null)
        {
            if (allocatedRecord is null)
                throw new ArgumentNullException(nameof(allocatedRecord), "Cannot transfer a null SectorAlignedMemory to DiskLogRecord");
            var fieldInfo = new SerializedFieldInfo()
            {
                recordInfo = *(RecordInfo*)allocatedRecord.GetValidPointer(),
                offsetToKeyStart = offsetToKeyStart,
                keyLength = keyLength,
                valueLength = valueLength,
                optionalLength = LogRecord.GetOptionalLength(*(RecordInfo*)allocatedRecord.GetValidPointer()),
                isChunkedValue = false, // if there's an object it'll already be in valueObject
                isComplete = true
            };

            // Create the new DiskLogRecord, transferring our buffer to it.
            var diskLogRecord = new DiskLogRecord((long)allocatedRecord.GetValidPointer(), in fieldInfo, valueObject)
                { recordOrValueBuffer = allocatedRecord };
            allocatedRecord = default;
            return diskLogRecord;
        }

        internal DiskLogRecord(RecordInfo recordInfo, ref SectorAlignedMemory keyBuffer, OverflowByteArray keyOverflow, ref SectorAlignedMemory valueBuffer,
            OverflowByteArray valueOverflow, RecordOptionals optionals, IHeapObject valueObject) : this()
        {
            this.recordInfo = recordInfo;
            
            if (keyBuffer is not null)
            {
                Debug.Assert(keyOverflow.IsEmpty, "Must have only one of keyBuffer or keyOverflow");
                this.keyBuffer = keyBuffer;     // Transfer ownership to us
                keyBuffer = default;
                keySpan = PinnedSpanByte.FromPinnedSpan(this.keyBuffer.RequiredValidSpan);
                recordInfo.SetKeyIsInline();
            }
            else
            {
                Debug.Assert(!keyOverflow.IsEmpty, "Must have either keyBuffer or keyOverflow");
                Debug.Assert(keyBuffer is null, "Must have only one of keyBuffer or keyOverflow");
                this.keyOverflow = keyOverflow;
                recordInfo.SetKeyIsOverflow();
            }

            if (valueBuffer is not null)
            {
                Debug.Assert(valueObject is null && valueOverflow.IsEmpty, "Must have only one of valueBuffer, valueOverflow, or valueObject");
                recordOrValueBuffer = valueBuffer;     // Transfer ownership to us
                valueBuffer = default;
                valueSpan = PinnedSpanByte.FromPinnedSpan(recordOrValueBuffer.RequiredValidSpan);
                recordInfo.SetValueIsInline();
            }
            else if (valueObject is not null)
            {
                Debug.Assert(valueBuffer is null && valueOverflow.IsEmpty, "Must have only one of valueBuffer, valueOverflow, or valueObject");
                valueOverflowOrObject = valueObject;
                recordInfo.SetValueIsObject();
            }
            else
            {
                Debug.Assert(!valueOverflow.IsEmpty, "Must have valueBuffer, valueObject, or valueOverflow");
                Debug.Assert(valueObject is null && valueBuffer is null, "Must have only one of valueBuffer, valueOverflow, or valueObject");
                valueOverflowOrObject = valueOverflow.Data;     // *not* the OverflowByteArray
                recordInfo.SetValueIsOverflow();
            }
            eTag = Info.HasETag ? optionals.eTag : LogRecord.NoETag;
            expiration = Info.HasExpiration ? optionals.expiration : 0;
        }

        /// <summary>A ref to the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref RecordInfo GetInfoRef(long physicalAddress) => ref Unsafe.AsRef<RecordInfo>((byte*)physicalAddress);

        /// <summary>Fast access returning a copy of the record header</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static RecordInfo GetInfo(long physicalAddress) => *(RecordInfo*)physicalAddress;

        /// <summary>Returns whether this is a single complete serialized stream of bytes, which may or may not be owned by <see cref="recordOrValueBuffer"/>. It returns
        /// false if we have Overflow, a value Object, or both keyBuffer and valueBuffer.</summary>
        public readonly bool IsSingleCompleteStream
            => Info.RecordIsInline && physicalAddress != 0 && keyBuffer is null;

        /// <summary>If true, this DiskLogRecord owns the buffer and must free it on <see cref="Dispose"/></summary>
        public readonly bool OwnsRecordBuffer => keyBuffer is not null || recordOrValueBuffer is not null;

        public void Dispose()
        {
            keyBuffer?.Return();
            keyBuffer = default;
            keySpan = default;
            keyOverflow = default;

            recordOrValueBuffer?.Return();
            recordOrValueBuffer = default;
            valueSpan = default;
            valueOverflowOrObject = default;

            eTag = LogRecord.NoETag;
            expiration = 0;
        }

        #region ISourceLogRecord
        /// <inheritdoc/>
        public readonly bool IsPinnedKey => Info.KeyIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedKeyPointer => IsPinnedKey ? keySpan.ToPointer() : null;

        /// <inheritdoc/>
        public OverflowByteArray KeyOverflow
        {
            readonly get => !Info.KeyIsInline ? keyOverflow : throw new TsavoriteException("get_Overflow is unavailable when Key is inline");
            set
            {
                keyOverflow = value;
                InfoRef.SetKeyIsOverflow();
            }
        }

        /// <inheritdoc/>
        public readonly bool IsPinnedValue => Info.ValueIsInline;

        /// <inheritdoc/>
        public readonly byte* PinnedValuePointer => IsPinnedValue ? valueSpan.ToPointer() : null;

        /// <inheritdoc/>
        public OverflowByteArray ValueOverflow
        {
            readonly get => Info.ValueIsOverflow ? new((byte[])valueOverflowOrObject) : throw new TsavoriteException("get_Overflow is unavailable when Value is inline or object");
            set
            {
                valueOverflowOrObject = value;
                InfoRef.SetValueIsOverflow();
            }
        }

        /// <inheritdoc/>
        public readonly bool IsSet => physicalAddress != 0 || keyBuffer is not null || !keyOverflow.IsEmpty;

        /// <inheritdoc/>
        public ref RecordInfo InfoRef => ref Unsafe.AsRef(ref recordInfo);
        /// <inheritdoc/>
        public readonly RecordInfo Info => recordInfo;

        /// <inheritdoc/>
        public readonly ReadOnlySpan<byte> Key => keyOverflow.IsEmpty ? keySpan : keyOverflow.ReadOnlySpan;

        /// <inheritdoc/>
        public readonly Span<byte> ValueSpan 
            => !Info.ValueIsObject
                ? (!Info.ValueIsOverflow ? valueSpan.Span : OverflowByteArray.AsSpan(valueOverflowOrObject))
                : throw new TsavoriteException("DiskLogRecord with Info.ValueIsObject does not support Span<byte> values");

        /// <inheritdoc/>
        public readonly IHeapObject ValueObject
            => Info.ValueIsObject
                ? Unsafe.As<IHeapObject>(valueOverflowOrObject)
                : throw new TsavoriteException("DiskLogRecord without Info.ValueIsObject does not allow ValueObject");

        /// <inheritdoc/>
        public readonly long ETag => eTag;

        /// <inheritdoc/>
        public readonly long Expiration => expiration;

        /// <inheritdoc/>
        public readonly bool IsMemoryLogRecord => false;

        /// <inheritdoc/>
        public readonly unsafe ref LogRecord AsMemoryLogRecordRef() => throw new InvalidOperationException("Cannot cast a DiskLogRecord to a memory LogRecord.");

        /// <inheritdoc/>
        public readonly bool IsDiskLogRecord => true;

        /// <inheritdoc/>
        public readonly unsafe ref DiskLogRecord AsDiskLogRecordRef() => ref Unsafe.AsRef(in this);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly RecordFieldInfo GetRecordFieldInfo() => new()
        {
            KeySize = Key.Length,
            ValueSize = Info.ValueIsObject ? ObjectIdMap.ObjectIdSize : ValueSpan.Length,
            ValueIsObject = Info.ValueIsObject,
            HasETag = Info.HasETag,
            HasExpiration = Info.HasExpiration
        };
        #endregion //ISourceLogRecord

        /// <summary>
        /// Copy key for Read or RMW operations, called by PendingContext; these have no Value (only Upsert has that) but have TInput, TOutput, and TContext,
        /// which are stored separately by PendingContext. Optionals (ETag and Expiration) are not considered here; those are specified in the TInput.
        /// </summary>
        /// <param name="key">Record key</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CopyKey(ReadOnlySpan<byte> key, SectorAlignedBufferPool bufferPool)
        {
            if (keyBuffer is not null)
                keyBuffer.pool.EnsureSize(ref keyBuffer, key.Length);
            else
                keyBuffer = bufferPool.Get(key.Length);
            key.CopyTo(keyBuffer.RequiredValidSpan);
            keyOverflow = default;
            keySpan = PinnedSpanByte.FromPinnedSpan(keyBuffer.RequiredValidSpan);
            recordInfo.SetKeyIsInline();
        }

        /// <summary>
        /// Copy value for record copying.
        /// </summary>
        /// <param name="srcValueSpan">Record value Span</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CopyValue(ReadOnlySpan<byte> srcValueSpan, SectorAlignedBufferPool bufferPool)
        {
            if (recordOrValueBuffer is not null)
                recordOrValueBuffer.pool.EnsureSize(ref keyBuffer, srcValueSpan.Length);
            else
                recordOrValueBuffer = bufferPool.Get(srcValueSpan.Length);
            srcValueSpan.CopyTo(keyBuffer.RequiredValidSpan);
            valueOverflowOrObject = default;
            valueSpan = PinnedSpanByte.FromPinnedSpan(recordOrValueBuffer.RequiredValidSpan);
            recordInfo.SetValueIsInline();
        }

        /// <summary>
        /// Copy method that takes a physical address, which may come from a <see cref="SectorAlignedMemory"/> or some other allocation
        /// that will have at least the lifetime of this <see cref="DiskLogRecord"/> (it may be <see cref="recordOrValueBuffer"/>).
        /// This <see cref="DiskLogRecord"/> does not own the memory allocation so will not free it on <see cref="Dispose()"/>
        /// (unless it is <see cref="recordOrValueBuffer"/>).
        /// </summary>
        private void CopyFrom(long physicalAddress, in SerializedFieldInfo fieldInfo, IHeapObject valueObject)
        {
            this.physicalAddress = physicalAddress;

            var ptr = (byte*)physicalAddress;
            recordInfo = *(RecordInfo*)ptr;
            recordInfo.ClearBitsForDiskImages();

            ptr += fieldInfo.offsetToKeyStart;
            keySpan = PinnedSpanByte.FromPinnedPointer(ptr, fieldInfo.keyLength);
            ptr += fieldInfo.keyLength;

            if (valueObject is null)
            {
                Debug.Assert(fieldInfo.valueLength < int.MaxValue, "fieldInfo.ValueLength should not exceed int.MaxValue; that should have been an object, which would not come here.");
                valueSpan = PinnedSpanByte.FromPinnedPointer(ptr, (int)fieldInfo.valueLength);
                ptr += fieldInfo.valueLength;
            }
            else
                valueOverflowOrObject = valueObject;

            if (recordInfo.HasETag)
            {
                eTag = *(long*)ptr;
                ptr += LogRecord.ETagSize;
            }
            else
                eTag = LogRecord.NoETag;

            expiration = recordInfo.HasExpiration ? *(long*)ptr : 0;
        }

        /// <summary>
        /// Serialize for Compact, Scan, Conditional Pending Operations, Migration, Replication, etc. The logRecord comes from the in-memory log;
        /// there is no associated TInput, TOutput, TContext, and any object is just reference-copied to the destination.
        /// </summary>
        /// <param name="logRecord">The record from the main log.</param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyFrom(in LogRecord logRecord, SectorAlignedBufferPool bufferPool)
        {
            if (logRecord.Info.RecordIsInline)
            {
                keyBuffer?.Return();
                keyBuffer = default;
                DirectCopyRecordBytes(logRecord.physicalAddress, logRecord.ActualRecordSize, bufferPool, ref recordOrValueBuffer);

                var (keyLength, valueLength, offsetToKeyStart) = GetInlineKeyAndValueSizes(logRecord.IndicatorAddress);
                var fieldInfo = new SerializedFieldInfo()
                {
                    recordInfo = logRecord.Info,
                    offsetToKeyStart = offsetToKeyStart,
                    keyLength = keyLength,
                    valueLength = valueLength,
                    optionalLength = logRecord.OptionalLength,
                    isChunkedValue = false, // It's inline, so there's no object
                    isComplete = true
                };

                CopyFrom((long)recordOrValueBuffer.GetValidPointer(), in fieldInfo, valueObject: null);
                return;
            }

            // Record is not inline so we have either key or value (or both) as Overflow or (for value) Object.
            recordInfo = logRecord.Info;

            if (logRecord.Info.KeyIsInline)
                CopyKey(logRecord.Key, bufferPool);
            else
            {
                keyBuffer = default;
                keyOverflow = logRecord.GetKeyOverflow();
                recordInfo.ClearKeyIsInline();
            }

            // Set the value
            if (logRecord.Info.ValueIsInline)
                CopyValue(logRecord.ValueSpan, bufferPool);
            else if (logRecord.Info.ValueIsObject)
            {
                valueOverflowOrObject = logRecord.ValueObject;
                recordInfo.SetValueIsObject();
            }
            else
            {
                valueOverflowOrObject = logRecord.GetValueOverflow().Data;  // *not* the OverflowByteArray
                recordInfo.SetValueIsOverflow();
            }

            // Set optionals; recordInfo already has the flags set correctly.
            eTag = logRecord.ETag;
            expiration = logRecord.Expiration;
        }

        /// <summary>
        /// Clone from a temporary <see cref="DiskLogRecord"/> to this one, e.g. for Compaction.
        /// </summary>
        /// <param name="inputDiskLogRecord"></param>
        /// <param name="bufferPool">Allocator for backing storage</param>
        public void CopyFrom(ref DiskLogRecord inputDiskLogRecord, SectorAlignedBufferPool bufferPool)
        {
            Debug.Assert(inputDiskLogRecord.IsSet, "inputDiskLogRecord is not set");

            // If the inputDiskLogRecord is inline it may be split between keyBuffer and recordOrValueBuffer, in recordOrValueBuffer only, or just physicalAddress.
            // We optimize the second and third cases here to a direct copy.
            if (GetCompleteSingleStreamSerializedFieldInfo(out var fieldInfo))
            {
                DirectCopyRecordBytes(inputDiskLogRecord.physicalAddress, fieldInfo.SerializedLength, bufferPool, ref recordOrValueBuffer);
                CopyFrom((long)recordOrValueBuffer.GetValidPointer(), in fieldInfo, valueObject: null);
                return;
            }

            // Record is not inline so we have either key or value (or both) as Overflow or (for value) Object.
            recordInfo = inputDiskLogRecord.Info;

            if (inputDiskLogRecord.Info.KeyIsInline)
                CopyKey(inputDiskLogRecord.Key, bufferPool);
            else
            {
                keyBuffer = default;
                keyOverflow = inputDiskLogRecord.keyOverflow;
                recordInfo.ClearKeyIsInline();
            }

            // Set the value
            if (inputDiskLogRecord.Info.ValueIsInline)
                CopyValue(inputDiskLogRecord.ValueSpan, bufferPool);
            else if (inputDiskLogRecord.Info.ValueIsObject)
            {
                valueOverflowOrObject = inputDiskLogRecord.ValueObject;
                recordInfo.SetValueIsObject();
            }
            else
            {
                valueOverflowOrObject = inputDiskLogRecord.valueOverflowOrObject;
                recordInfo.SetValueIsOverflow();
            }

            // Set optionals; recordInfo already has the flags set correctly.
            eTag = inputDiskLogRecord.ETag;
            expiration = inputDiskLogRecord.Expiration;
        }

        /// <summary>
        /// Directly copies a record in varbyte format to the <paramref name="allocatedRecord"/>, allocating it from <paramref name="bufferPool"/> if needed.
        /// <see cref="CopyFrom(long, in SerializedFieldInfo, IHeapObject)"/> should be called after this to populate the fields.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DirectCopyRecordBytes(long srcPhysicalAddress, long srcRecordSize, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            EnsureAllocation(srcRecordSize, bufferPool, ref allocatedRecord);
            Buffer.MemoryCopy((byte*)srcPhysicalAddress, (byte*)physicalAddress, srcRecordSize, srcRecordSize);
        }

        /// <summary>
        /// Directly copies a record in varbyte format to the SpanByteAndMemory. Allocates <see cref="SpanByteAndMemory.Memory"/> if needed.
        /// <see cref="CopyFrom(long, in SerializedFieldInfo, IHeapObject)"/> should be called after this to populate the fields.
        /// </summary>
        /// <remarks>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void DirectCopyRecordBytes(long srcPhysicalAddress, long srcRecordSize, MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
        {
            // TotalSize includes the length prefix, which is included in the output stream if we can write to the SpanByte.
            if (output.IsSpanByte && output.SpanByte.TotalSize >= (int)srcRecordSize)     // TODO: long value sizes
            {
                var outPtr = output.SpanByte.ToPointer();
                *(int*)outPtr = (int)srcRecordSize;
                Buffer.MemoryCopy((byte*)srcPhysicalAddress, outPtr + sizeof(int), output.SpanByte.Length, srcRecordSize);
                output.SpanByte.Length = (int)srcRecordSize;
                return;
            }

            // Do not include the length prefix in the output stream; this is done by the caller before writing the stream, from the SpanByte.Length we set here.
            output.EnsureHeapMemorySize((int)srcRecordSize, memoryPool);
            fixed (byte* outPtr = output.MemorySpan)
            {
                Buffer.MemoryCopy((byte*)srcPhysicalAddress, outPtr, srcRecordSize, srcRecordSize);
                output.Length = (int)srcRecordSize;
            }
        }

        /// <summary>
        /// Deserialize the current value span to a <see cref="valueOverflowOrObject"/> valueObject.
        /// </summary>
        /// <param name="valueSerializer">Serializer for the value object; if null, do not serialize (carry the valueObject (if any) through from the logRecord instead)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IHeapObject DeserializeValueObject(IObjectSerializer<IHeapObject> valueSerializer)
        {
            if (valueOverflowOrObject is not null)
                return (IHeapObject)valueOverflowOrObject;
            if (!Info.ValueIsObject || valueSpan.IsEmpty)
                return (IHeapObject)(valueOverflowOrObject = default);

            var stream = new UnmanagedMemoryStream(valueSpan.ToPointer(), valueSpan.Length);
            valueSerializer.BeginDeserialize(stream);
            valueSerializer.Deserialize(out var valueObject);
            valueSerializer.EndDeserialize();
            valueOverflowOrObject = valueObject;
            return valueObject;
        }

        /// <summary>
        /// Serializes a record in varbyte format to the SpanByteAndMemory. Allocates <see cref="SpanByteAndMemory.Memory"/> if needed.
        /// </summary>
        /// <remarks>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long SerializeRecordToVarbyteFormat<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, long srcPhysicalAddress, IObjectSerializer<IHeapObject> valueSerializer,
                MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
            where TSourceLogRecord : ISourceLogRecord
        {
            // If we have an object and we don't serialize it, we don't need to allocate space for it.
            // Value length prefix on the disk is a long, as it may be an object.
            long valueLength;
            if (!srcLogRecord.Info.ValueIsObject)
            {
                valueLength = srcLogRecord.ValueSpan.Length;
            }
            else
            {
                // TODO: Large-object chunking, including chaining. For now we assume an object small enough to allocate inline
                if (!srcLogRecord.ValueObject.SerializedSizeIsExact)
                    throw new TsavoriteException("Currently we do not support in-memory serialization of objects that do not support SerializedSizeIsExact");
                valueLength = srcLogRecord.ValueObject.SerializedSize;
            }

            var indicatorByte = ConstructIndicatorByte(srcLogRecord.Key.Length, valueLength, out var keyLengthByteCount, out var valueLengthByteCount);

            var recordSize = RecordInfo.GetLength()
                + 1 // indicator byte
                + keyLengthByteCount + srcLogRecord.Key.Length
                + valueLengthByteCount + valueLength
                + (srcLogRecord.Info.HasETag ? LogRecord.ETagSize : 0)
                + (srcLogRecord.Info.HasExpiration ? LogRecord.ExpirationSize : 0);

            // Copy in varbyte format.
            var serializedSize = 0L;

            // TotalSize includes the length prefix, which is included in the output stream if we can write to the SpanByte.
            if (output.IsSpanByte && output.SpanByte.TotalSize >= (int)recordSize)
            {
                var outPtr = output.SpanByte.ToPointer();
                *(int*)outPtr = (int)recordSize;
                serializedSize = SerializeVarbyteRecordToPinnedPointer(in srcLogRecord, outPtr + sizeof(int), indicatorByte, keyLengthByteCount, valueLength, valueLengthByteCount, valueSerializer);
                output.SpanByte.Length = (int)serializedSize;
            }
            else
            {
                // Do not include the length prefix in the output stream; this is done by the caller before writing the stream, from the SpanByte.Length we set here.
                output.EnsureHeapMemorySize((int)recordSize, memoryPool);
                fixed (byte* outPtr = output.MemorySpan)
                {
                    serializedSize = SerializeVarbyteRecordToPinnedPointer(in srcLogRecord, outPtr, indicatorByte, keyLengthByteCount, valueLength, valueLengthByteCount, valueSerializer);
                    output.Length = (int)recordSize;
                }
            }
            Debug.Assert(serializedSize == recordSize, $"Serialized size {serializedSize} does not match expected size {recordSize}");
            return serializedSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long SerializeVarbyteRecordToPinnedPointer<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, byte* ptr, byte indicatorByte,
                int keyLengthByteCount, long valueLength, int valueLengthByteCount, IObjectSerializer<IHeapObject> valueSerializer)
            where TSourceLogRecord : ISourceLogRecord
        {
            var dstRecordInfoPtr = (RecordInfo*)ptr;
            ptr = SerializeVarbyteLengthsAndKey(srcLogRecord.Info, indicatorByte, srcLogRecord.Key, keyLengthByteCount, valueLength, valueLengthByteCount, ptr);

            // If the srcLogRecord has an object already serialized to its ValueSpan, then we will not be here; LogRecord does not serialize values to the ValueSpan,
            // and the DiskLogRecord case where valueInfo.length > 0 has already been tested for in the caller.
            if (!srcLogRecord.Info.ValueIsObject)
                srcLogRecord.ValueSpan.CopyTo(new Span<byte>(ptr, (int)valueLength));
            else
            {
                var stream = new UnmanagedMemoryStream(ptr, srcLogRecord.ValueObject.SerializedSize, srcLogRecord.ValueObject.SerializedSize, FileAccess.ReadWrite);
                valueSerializer.BeginSerialize(stream);
                valueSerializer.Serialize(srcLogRecord.ValueObject);
                valueSerializer.EndSerialize();
            }
            ptr += valueLength;

            if (srcLogRecord.Info.HasETag)
            {
                dstRecordInfoPtr->SetHasETag();
                *(long*)ptr = srcLogRecord.ETag;
                ptr += LogRecord.ETagSize;
            }

            if (srcLogRecord.Info.HasExpiration)
            {
                dstRecordInfoPtr->SetHasExpiration();
                *(long*)ptr = srcLogRecord.Expiration;
                ptr += LogRecord.ExpirationSize;
            }

            return ptr - (byte*)dstRecordInfoPtr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe byte* SerializeVarbyteLengthsAndKey(RecordInfo recordInfo, byte indicatorByte, ReadOnlySpan<byte> key, int keyLengthByteCount,
                long valueLength, int valueLengthByteCount, byte* ptr)
        {
            // RecordInfo is a stack copy so we can modify it here. Write the key "inline" status; keys are always inline in DiskLogRecord, as they cannot
            // be object. Value, however, does not set ValueIsInline for varbyte records, because then Info.RecordIsInline would be true. Instead we clear
            // ValueIsInline and use ValueIsObject to determine whether the serialized bytes are string or object, and whether DeserializeValueObject can
            // be used. WARNING: ToString() may AV when stepping through here in the debugger, until we have the lengths correctly set.
            recordInfo.SetKeyIsInline();
            recordInfo.ClearValueIsInline();
            *(RecordInfo*)ptr = recordInfo;
            ptr += RecordInfo.GetLength();

            // Set the indicator and lengths
            *ptr++ = indicatorByte;
            WriteVarbyteLength(key.Length, keyLengthByteCount, ptr);
            ptr += keyLengthByteCount;
            WriteVarbyteLength(valueLength, valueLengthByteCount, ptr);
            ptr += valueLengthByteCount;

            // Copy the key but not the value; the caller does that.
            key.CopyTo(new Span<byte>(ptr, key.Length));

            // Return the pointer to the value data space (immediately following the key data space; the value length was already written above).
            return ptr + key.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureAllocation(long recordSize, SectorAlignedBufferPool bufferPool, ref SectorAlignedMemory allocatedRecord)
        {
            var allocatedSize = RoundUp((int)recordSize, Constants.kRecordAlignment);
            if (allocatedRecord is not null)
                allocatedRecord.pool.EnsureSize(ref allocatedRecord, allocatedSize);
            else
                allocatedRecord = bufferPool.Get(allocatedSize);
            Debug.Assert(allocatedRecord.required_bytes == recordSize, $"allocatedRecord.required_bytes {allocatedRecord.required_bytes} should have been set to recordSize {recordSize}");
            physicalAddress = (long)allocatedRecord.GetValidPointer();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly void CopyOptionals<TSourceLogRecord>(ref readonly TSourceLogRecord logRecord, ref byte* ptr)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (logRecord.Info.HasETag)
            {
                recordInfo.SetHasETag();
                *(long*)ptr = logRecord.ETag;
                ptr += LogRecord.ETagSize;
            }

            if (logRecord.Info.HasExpiration)
            {
                recordInfo.SetHasExpiration();
                *(long*)ptr = logRecord.Expiration;
                ptr += LogRecord.ExpirationSize;
            }
        }

        /// <summary>
        /// Transfer memory ownership from a temporary <see cref="DiskLogRecord"/> to a longer-lasting one.
        /// </summary>
        /// <remarks>This is separate from <see cref="CopyFrom(ref DiskLogRecord, SectorAlignedBufferPool)"/> to ensure the caller
        /// is prepared to handle the implications of the transfer</remarks>
        /// <param name="inputDiskLogRecord"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TransferFrom(ref DiskLogRecord inputDiskLogRecord)
        {
            Debug.Assert(inputDiskLogRecord.IsSet, "inputDiskLogRecord is not set");
            Debug.Assert(inputDiskLogRecord.recordOrValueBuffer is not null, "inputDiskLogRecord does not own its memory");

            Dispose();

            physicalAddress = inputDiskLogRecord.physicalAddress;
            recordInfo = inputDiskLogRecord.recordInfo;

            keyOverflow = inputDiskLogRecord.keyOverflow;
            inputDiskLogRecord.keyOverflow = default;
            keyBuffer = inputDiskLogRecord.keyBuffer;
            keyBuffer = default;
            keySpan = inputDiskLogRecord.keySpan;
            inputDiskLogRecord.keySpan = default;

            valueOverflowOrObject = inputDiskLogRecord.valueOverflowOrObject;
            inputDiskLogRecord.valueOverflowOrObject = default;
            recordOrValueBuffer = inputDiskLogRecord.recordOrValueBuffer;
            inputDiskLogRecord.recordOrValueBuffer = default;
            valueSpan = inputDiskLogRecord.valueSpan;
            inputDiskLogRecord.valueSpan = default;

            eTag = inputDiskLogRecord.eTag;
            expiration = inputDiskLogRecord.expiration;
        }

        /// <summary>
        /// Serialize a log record to the <see cref="SpanByteAndMemory"/> <paramref name="output"/> in DiskLogRecord format, with the objectValue
        /// serialized to the Value span if it is not already there in the source (there will be no ValueObject in the result).
        /// Allocates <see cref="SpanByteAndMemory.Memory"/> if needed. This is used for migration.
        /// </summary>
        /// <remarks>If <paramref name="output"/>.<see cref="SpanByteAndMemory.IsSpanByte"/>, it points directly to the network buffer so we include the length prefix in the output.</remarks>
        public static void Serialize<TSourceLogRecord>(in TSourceLogRecord srcLogRecord, IObjectSerializer<IHeapObject> valueSerializer, MemoryPool<byte> memoryPool, ref SpanByteAndMemory output)
            where TSourceLogRecord : ISourceLogRecord
        {
            if (srcLogRecord.IsMemoryLogRecord)
            {
                ref var inMemoryLogRecord = ref srcLogRecord.AsMemoryLogRecordRef();
                if (inMemoryLogRecord.Info.RecordIsInline)
                    DirectCopyRecordBytes(inMemoryLogRecord.physicalAddress, inMemoryLogRecord.ActualRecordSize, memoryPool, ref output);
                else
                    _ = SerializeRecordToVarbyteFormat(in inMemoryLogRecord, inMemoryLogRecord.physicalAddress, valueSerializer, memoryPool, ref output);
                return;
            }

            if (!srcLogRecord.IsDiskLogRecord)
                throw new TsavoriteException("Unknown TSourceLogRecord type");
            ref var diskLogRecord = ref srcLogRecord.AsDiskLogRecordRef();

            if (diskLogRecord.GetCompleteSingleStreamSerializedFieldInfo(out var fieldInfo))
                DirectCopyRecordBytes(diskLogRecord.physicalAddress, fieldInfo.SerializedLength, memoryPool, ref output);
            else
                _ = SerializeRecordToVarbyteFormat(in diskLogRecord, diskLogRecord.physicalAddress, valueSerializer, memoryPool, ref output);
        }

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            static string bstr(bool value) => value ? "T" : "F";
            var valueString = Info.ValueIsObject ? $"obj:{ValueObject}" : ValueSpan.ToString();
            var eTag = Info.HasETag ? ETag.ToString() : "-";
            var expiration = Info.HasExpiration ? Expiration.ToString() : "-";

            return $"ri {Info} | key {Key.ToShortString(20)} | val {valueString} | HasETag {bstr(Info.HasETag)}:{eTag} | HasExpiration {bstr(Info.HasExpiration)}:{expiration}";
        }
    }
}