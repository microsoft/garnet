// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>
    /// Struct encoding a Span field (Key or Value) at a certain address. Since (small) Objects can be represented as inline spans,
    /// this applies to those forms as well as the inline component of the Object, which is the ObjectId. The layout is:
    /// <list type="bullet">
    ///     <item>Inline: [int Length][data bytes]</item>
    ///     <item>Overflow: an int ObjectId for a byte[] that is held in <see cref="ObjectIdMap"/></item>
    ///     <item>Object: an int ObjectId for an IHeapObject that is held in <see cref="ObjectIdMap"/></item>
    /// </list>
    /// </summary>
    /// <remarks>Considerations regarding variable field sizes:
    /// <list type="bullet">
    ///     <item>Keys are immutable (unless revivification is happening), so the inline size of a Key field does not change</item>
    ///     <item>When Values change size the Filler length and offsets to optional ETag and Extension are adjusted. Converting between inline and out-of-line
    ///         due to size changes altering whether the Value overflows is handled as part of normal Value-sizechange operations</item>
    /// </list>
    /// </remarks>
    public unsafe struct SpanField
    {
        /// <summary>This is the size of the length prefix on Span field.</summary>
        public const int FieldLengthPrefixSize = sizeof(int);

        /// <summary>For an inline field, get a reference to the length field of the data.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref int GetInlineLengthRef(long fieldAddress) => ref *(int*)fieldAddress;

        /// <summary>For an inline field, get the address of the actual data (past the length prefix); this is the start of the stream of bytes.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetInlineDataAddress(long fieldAddress) => fieldAddress + FieldLengthPrefixSize;

        /// <summary>
        /// This is the inline size of an overflow (out-of-line) objectId.. There is no length prefix for
        /// this field. A field's inline size must be at least this to be able to convert it to an overflow byte[].
        /// For an Object record, this should not be used; use <see cref="ObjectIdMap.ObjectIdSize"/> instead.
        /// </summary>
        internal const int OverflowInlineSize = sizeof(int);

        /// <summary>Gets a referemce to the ObjectId at address (which is ValueAddress). There is no length prefix.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref int GetObjectIdRef(long fieldAddress) => ref *(int*)fieldAddress;

        /// <summary>For an inline field, get the total inline size of the field: The length prefix plus the length of the byte stream</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetTotalSizeOfInlineField(long fieldAddress) => FieldLengthPrefixSize + GetInlineLengthRef(fieldAddress);

        /// <summary>Get a field's inline length, depending on whether it is actually inline or whether it is an out-of-line objectId.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetInlineDataSizeOfKey(long fieldAddress, bool isInline) => isInline ? GetInlineLengthRef(fieldAddress) : OverflowInlineSize;

        /// <summary>Get a field's inline length, depending on whether it is actually inline or whether it is an out-of-line objectId.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetInlineTotalSizeOfKey(long fieldAddress, bool isInline) => isInline ? FieldLengthPrefixSize + GetInlineLengthRef(fieldAddress) : OverflowInlineSize;

        /// <summary>The inline length of the value without any length prefix.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetInlineDataSizeOfValue(long valueAddress, bool valueIsObject, bool valueIsInline)
            => valueIsInline ? GetInlineLengthRef(valueAddress) : GetInlineSizeOfOutOfLineValue(valueIsObject);

        /// <summary>The inline length of the out-of-line Object Id.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetInlineSizeOfOutOfLineValue(bool valueIsObject) => valueIsObject ? ObjectIdMap.ObjectIdSize : OverflowInlineSize;

        /// <summary>The inline length of the value including any length prefix.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetInlineTotalSizeOfValue(long valueAddress, bool valueIsObject, bool valueIsInline)
            => valueIsInline ? GetTotalSizeOfInlineField(valueAddress) : GetInlineSizeOfOutOfLineValue(valueIsObject);

        /// <summary>
        /// Initialize an inline field with the length necessary for the eventual overflow objectId, but don't set the field to overflow yet to avoid needing nullref checks.
        /// <see cref="LogRecord.TrySetValueLength(ref RecordSizeInfo)"/> will handle converting to overflow and allocating.
        /// </summary>
        /// <param name="fieldAddress"></param>
        internal static void InitializeInlineForOverflowField(long fieldAddress) => GetInlineLengthRef(fieldAddress) = OverflowInlineSize - FieldLengthPrefixSize;

        /// <summary>
        /// Obtain a <see cref="Span{_byte_}"/> referencing the inline or overflow data and the datasize for this field.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> AsSpan(long fieldAddress, bool isInline, ObjectIdMap objectIdMap)
        {
            if (isInline)
                return new((byte*)GetInlineDataAddress(fieldAddress), GetInlineLengthRef(fieldAddress));
            var objectId = GetObjectIdRef(fieldAddress);
            if (objectId != ObjectIdMap.InvalidObjectId)
            {
                var byteArrayObj = objectIdMap.Get(objectId);
                return new Span<byte>(Unsafe.As<object, byte[]>(ref byteArrayObj));
            }
            return [];
        }

        /// <summary>
        /// Obtain a <see cref="Span{_byte_}"/> referencing the inline data and the datasize for this field; MUST be an inline field.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> AsInlineSpan(long fieldAddress) => new((byte*)GetInlineDataAddress(fieldAddress), GetInlineLengthRef(fieldAddress));

        /// <summary>
        /// Set all data within a portion of a field to zero.
        /// </summary>
        /// <param name="address">Address of the field</param>
        /// <param name="dataOffset">Starting position in the field to zero</param>
        /// <param name="clearLength">Length of the data from <paramref name="dataOffset"/> to zero</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ZeroInlineData(long address, int dataOffset, int clearLength)
            => ZeroData(GetInlineDataAddress(address) + dataOffset, clearLength);

        /// <summary>
        /// Set all data within a portion of a field to zero.
        /// </summary>
        /// <param name="clearStartAddress">Address to start clearing at</param>
        /// <param name="clearLength">Length of the data from <paramref name="clearStartAddress"/> to zero</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ZeroData(long clearStartAddress, int clearLength)
            => new Span<byte>((byte*)clearStartAddress, clearLength).Clear();

        /// <summary>
        /// Convert a Span field from inline to overflow.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field does not currently contain an overflow allocation. Applies to Keys as well during freelist revivification.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ConvertInlineToOverflow(ref RecordInfo recordInfo, long fieldAddress, int newLength, ObjectIdMap objectIdMap)
        {
            // First copy the data
            var array = GC.AllocateUninitializedArray<byte>(newLength);
            var oldLength = GetInlineLengthRef(fieldAddress);
            var copyLength = oldLength < newLength ? oldLength : newLength;

            if (copyLength > 0)
            {
                var oldSpan = new ReadOnlySpan<byte>((byte*)GetInlineDataAddress(fieldAddress), copyLength);
                oldSpan.CopyTo(array);
            }

            // If "shrinking" the allocation because the overflow objectId size is less than the current inline size, we must zeroinit the extra space.
            // Note: We don't zeroinit data in the overflow allocation, just like we don't zeroinit data in the inline value within the length.
            var clearLength = oldLength - OverflowInlineSize;
            if (clearLength > 0)
                ZeroInlineData(fieldAddress, OverflowInlineSize, clearLength);

            recordInfo.SetValueIsOverflow();
            var objectId = objectIdMap.Allocate();
            GetObjectIdRef(fieldAddress) = objectId;
            objectIdMap.Set(objectId, array);
            return array;
        }

        /// <summary>
        /// Convert a Span field from inline to overflow.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field does not currently contain an overflow allocation. Here we do not copy the data; we assume the caller will have already
        /// prepared to convert from Object format to inline format.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ConvertObjectIdToOverflow(ref RecordInfo recordInfo, long fieldAddress, int newLength, ObjectIdMap objectIdMap)
        {
            var array = GC.AllocateUninitializedArray<byte>(newLength);

            var objectId = GetObjectIdRef(fieldAddress);
            if (objectId == ObjectIdMap.InvalidObjectId)
            {
                objectId = objectIdMap.Allocate();
                GetObjectIdRef(fieldAddress) = objectId;
            }
            objectIdMap.Set(objectId, array);

            // OverflowInlineSize is >= ObjectIdSize so we will not be "shrinking" the allocation and therefore have no new extra space to zeroinit.
            Debug.Assert(OverflowInlineSize > ObjectIdMap.ObjectIdSize);

            recordInfo.SetValueIsOverflow();
            return array;
        }

        /// <summary>
        /// Convert a Span field from inline to ObjectId.
        /// </summary>
        /// <remarks>
        /// Applies to Value during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field does not currently contain a valid ObjectId. Here we do not copy the data; we assume the caller will have already
        /// created an object that has converted from inline format to object format.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int ConvertInlineToObjectId(ref RecordInfo recordInfo, long fieldAddress, ObjectIdMap objectIdMap)
        {
            // Here we do not copy the data; we assume the caller will have already created an object that has converted from inline format to object format.
            var objectId = objectIdMap.Allocate();
            var oldLength = GetInlineLengthRef(fieldAddress);

            // If "shrinking" the allocation because ObjectIdSize is less than the current inline size, we must zeroinit the extra space.
            var clearLength = oldLength - ObjectIdMap.ObjectIdSize;
            if (clearLength > 0)
                ZeroInlineData(fieldAddress, ObjectIdMap.ObjectIdSize, clearLength);

            recordInfo.SetValueIsObject();
            GetObjectIdRef(fieldAddress) = objectId;
            return objectId;
        }

        /// <summary>
        /// Convert a Span field from an out-of-line overflow allocation to ObjectId.
        /// </summary>
        /// <remarks>
        /// Applies to Value during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field does not currently contain a valid ObjectId. Here we do not copy the data; we assume the caller will have already
        /// created an object that has converted from inline format to object format.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int ConvertOverflowToObjectId(ref RecordInfo recordInfo, long fieldAddress, ObjectIdMap objectIdMap)
        {
            var objectId = GetObjectIdRef(fieldAddress);
            if (objectId != ObjectIdMap.InvalidObjectId)
                objectIdMap.Set(objectId, null);
            else
            {
                objectId = objectIdMap.Allocate();
                GetObjectIdRef(fieldAddress) = objectId;
            }

            recordInfo.SetValueIsObject();
            return objectId;
        }

        /// <summary>
        /// Utility function to set the overflow allocation at the given Span field's address. Assumes caller has ensured no existing overflow
        /// allocation is there; e.g. SerializeKey and InitializeValue.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> SetOverflowAllocation(long fieldAddress, int newLength, ObjectIdMap objectIdMap)
        {
            var objectId = objectIdMap.Allocate();
            GetObjectIdRef(fieldAddress) = objectId;

            var newArray = GC.AllocateUninitializedArray<byte>(newLength);
            objectIdMap.Set(objectId, newArray);
            return new Span<byte>(newArray);
        }

        /// <summary>
        /// Convert a Span field from overflow to inline.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an overflow allocation. Applies to Keys as well during freelist revivification.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ConvertOverflowToInline(ref RecordInfo recordInfo, long fieldAddress, int newLength, ObjectIdMap objectIdMap)
        {
            // First copy the data
            var objectId = GetObjectIdRef(fieldAddress);
            if (objectId != ObjectIdMap.InvalidObjectId)
            {
                var oldSpan = new Span<byte>((byte[])objectIdMap.Get(objectId));
                objectIdMap.Set(objectId, null);

                // Sequencing here is important for zeroinit correctness
                var copyLength = oldSpan.Length < newLength ? oldSpan.Length : newLength;
                var newSpan = SetInlineDataLength(fieldAddress, newLength);
                recordInfo.SetValueIsInline();
                oldSpan.Slice(0, copyLength).CopyTo(newSpan);
                return newSpan;
            }
            return SetInlineDataLength(fieldAddress, newLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void FreeOverflowAndConvertToInline(ref RecordInfo recordInfo, long fieldAddress, ObjectIdMap objectIdMap, bool isKey)
        {
            var objectId = GetObjectIdRef(fieldAddress);
            if (objectId != ObjectIdMap.InvalidObjectId)
                objectIdMap.Set(objectId, null);

            // Set this as inline with length equal to the size difference.
            int newLength = OverflowInlineSize - FieldLengthPrefixSize;
            Debug.Assert(newLength >= 0, "newLength must be non-negative");

            // Sequencing here is important for zeroinit correctness
            GetInlineLengthRef(fieldAddress) = newLength;
            if (isKey)
                recordInfo.SetKeyIsInline();
            else
                recordInfo.SetValueIsInline();
        }

        /// <summary>
        /// Convert a Value field from ObjectId to inline.
        /// </summary>
        /// <remarks>
        /// Applies to Value during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an ObjectId (which may be ObjectIdMap.InvalidObjectId). Here we do not copy the data; we assume
        /// the caller will have already prepared to convert from Object format to inline format.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ConvertObjectIdToInline(ref RecordInfo recordInfo, long fieldAddress, int newLength, ObjectIdMap objectIdMap)
        {
            ref int objIdRef = ref GetObjectIdRef(fieldAddress);
            objectIdMap.Free(objIdRef);
            objIdRef = 0;

            // Sequencing here is important for zeroinit correctness
            var newSpan = SetInlineDataLength(fieldAddress, newLength);
            recordInfo.SetValueIsInline();
            return newSpan;
        }

        /// <summary>
        /// Utility function to set the inline length of a Span field and return a <see cref="Span{_byte_}"/> to the data start (which may be an inline byte stream or a byte[]).
        /// </summary>
        internal static Span<byte> SetInlineDataLength(long fieldAddress, int newLength)
        {
            GetInlineLengthRef(fieldAddress) = newLength;             // actual length (i.e. the inline data space used by this field)
            return new Span<byte>((byte*)GetInlineDataAddress(fieldAddress), newLength);
        }

        /// <summary>
        /// Shrink an inline Span field in place.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an overflow allocation. Applies to Keys as well during freelist revivification.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* AdjustInlineLength(long fieldAddress, int newLength)
        {
            // Zeroinit the extra space. Here we are concerned about shrinkage leaving nonzero leftovers, so we clear those.
            var clearLength = GetInlineLengthRef(fieldAddress) - newLength;
            if (clearLength > 0)
                ZeroInlineData(fieldAddress, newLength, clearLength);
            GetInlineLengthRef(fieldAddress) = newLength;
            return (byte*)GetInlineDataAddress(fieldAddress);
        }

        /// <summary>
        /// Reallocate a Span field that is overflow, e.g. to make the overflow allocation larger. Shrinkage is done in-place (the caller decides if the
        /// shrinkage is sufficient (given available space in the record) to convert the field in-place to inline.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an overflow allocation. Applies to Keys as well during freelist revivification.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ReallocateOverflow(long fieldAddress, int newLength, ObjectIdMap objectIdMap)
        {
            byte[] newArray;

            var objectId = GetObjectIdRef(fieldAddress);
            if (objectId != ObjectIdMap.InvalidObjectId)
            {
                var oldArray = (byte[])objectIdMap.Get(objectId);
                if (oldArray.Length == newLength)
                    return new Span<byte>(oldArray);

                // Allocate and copy
                newArray = new byte[newLength];
                var copyLength = oldArray.Length < newLength ? oldArray.Length : newLength;
                Array.Copy(oldArray, newArray, copyLength);
                if (copyLength < newLength)
                    Array.Clear(newArray, copyLength, newLength - copyLength);
            }
            else
            {
                // Allocate; nothing to copy
                newArray = new byte[newLength];
                objectId = objectIdMap.Allocate();
                GetObjectIdRef(fieldAddress) = objectId;
            }
            objectIdMap.Set(objectId, newArray);
            return new Span<byte>(newArray);
        }
    }
}