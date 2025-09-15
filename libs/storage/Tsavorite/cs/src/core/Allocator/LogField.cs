// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static VarbyteLengthUtility;

    /// <summary>
    /// Static class providing functions to operate on a Log field (Key Span, or Value Span or Object) at a certain address. Since (small) Objects can be represented
    /// as inline spans, this applies to those forms as well as the inline component of the Object, which is the ObjectId. The layout is:
    /// <list type="bullet">
    ///     <item>Varbyte indicator byte and lengths; see <see cref="VarbyteLengthUtility"/> header comments for details</item>
    ///     <item>Key data: either the inline data or an int ObjectId for a byte[] that is held in <see cref="ObjectIdMap"/></item>
    ///     <item>Value data: either the inline data or an int ObjectId for a byte[] that is held in <see cref="ObjectIdMap"/></item>
    /// </list>
    /// </summary>
    /// <remarks>Considerations regarding variable field sizes:
    /// <list type="bullet">
    ///     <item>Keys are immutable (unless revivification is happening), so the inline size of a Key field does not change</item>
    ///     <item>When Values change size the Filler length and offsets to optional ETag and Extension are adjusted. Converting between inline and out-of-line
    ///         due to size changes altering whether the Value overflows is handled as part of normal Value-sizechange operations</item>
    /// </list>
    /// </remarks>
    public static unsafe class LogField
    {
        /// <summary>
        /// Convert a Span field from inline to overflow.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field does not currently contain an overflow allocation.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ConvertInlineToOverflow(ref RecordInfo recordInfo, long physicalAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsInline);

            // First copy the data
            var oldLength = sizeInfo.GetValueInlineLength(physicalAddress);
            var newLength = sizeInfo.FieldInfo.ValueSize;
            var overflow = new OverflowByteArray(newLength, startOffset: 0, endOffset: 0, zeroInit: false);
            var copyLength = oldLength < newLength ? oldLength : newLength;
            var fieldAddress = sizeInfo.GetValueAddress(physicalAddress);

            if (copyLength > 0)
            {
                var oldSpan = new ReadOnlySpan<byte>((byte*)fieldAddress, copyLength);
                oldSpan.CopyTo(overflow.Span);
            }

            var objectId = objectIdMap.Allocate();
            *(int*)fieldAddress = objectId;
            objectIdMap.Set(objectId, overflow);
            recordInfo.SetValueIsOverflow();
            return overflow.Span;
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
        internal static Span<byte> ConvertHeapObjectToOverflow(ref RecordInfo recordInfo, long physicalAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsObject);
            var overflow = new OverflowByteArray(sizeInfo.FieldInfo.ValueSize, startOffset: 0, endOffset: 0, zeroInit: false);

            var fieldAddress = sizeInfo.GetValueAddress(physicalAddress);
            var objectId = *(int*)fieldAddress;
            if (objectId == ObjectIdMap.InvalidObjectId)
                *(int*)fieldAddress = objectId = objectIdMap.Allocate();
            objectIdMap.Set(objectId, overflow);
            recordInfo.SetValueIsOverflow();
            return overflow.Span;
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
        internal static int ConvertInlineToHeapObject(ref RecordInfo recordInfo, long physicalAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsInline);
            var objectId = objectIdMap.Allocate();
            var oldLength = sizeInfo.GetValueInlineLength(physicalAddress);
            recordInfo.SetValueIsObject();

            var fieldAddress = sizeInfo.GetValueAddress(physicalAddress);
            *(int*)fieldAddress = objectId;
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
        internal static int ConvertOverflowToHeapObject(ref RecordInfo recordInfo, long physicalAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsOverflow);
            var fieldAddress = sizeInfo.GetValueAddress(physicalAddress);

            var objectId = *(int*)fieldAddress;
            if (objectId != ObjectIdMap.InvalidObjectId)
                objectIdMap.Set(objectId, null);    // Clear the byte[] from the existing slot but do not free the slot; caller will put the HeapObject into the slot.
            else
                *(int*)fieldAddress = objectId = objectIdMap.Allocate();

            recordInfo.SetValueIsObject();
            return objectId;
        }

        /// <summary>
        /// Utility function to set the overflow allocation at the given Span field's address. Assumes caller has ensured no existing overflow
        /// allocation is there; e.g. SerializeKey and InitializeValue.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetOverflowAllocation(long physicalAddress, OverflowByteArray overflow, ObjectIdMap objectIdMap, bool isKey)
        {
            var objIdPtr = (int*)GetFieldPtr(physicalAddress + RecordInfo.Size, isKey, out var lengthPtr, out var lengthBytes, out _ /*length*/);

            // Assumes no object allocated for this field yet.
            var objectId = objectIdMap.Allocate();
            WriteVarbyteLength(ObjectIdMap.ObjectIdSize, lengthBytes, lengthPtr);
            *objIdPtr = objectId;
            objectIdMap.Set(objectId, overflow);
        }

        /// <summary>
        /// Convert a Span field from overflow to inline.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an overflow allocation.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ConvertOverflowToInline(ref RecordInfo recordInfo, long physicalAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsOverflow);

            // First copy the data
            var fieldAddress = sizeInfo.GetValueAddress(physicalAddress);
            var objectId = *(int*)fieldAddress;

            var newLength = sizeInfo.FieldInfo.ValueSize;
            var newSpan = new Span<byte>((byte*)fieldAddress, newLength);

            if (objectId != ObjectIdMap.InvalidObjectId)
            {
                var overflow = objectIdMap.GetOverflowByteArray(objectId);
                var oldSpan = overflow.Span;

                var copyLength = oldSpan.Length < newLength ? oldSpan.Length : newLength;
                recordInfo.SetValueIsInline();
                oldSpan.Slice(0, copyLength).CopyTo(newSpan);
                objectIdMap.Free(objectId);
            }
            return newSpan;
        }

        /// <summary>
        /// Called when disposing a record, to free an Object or Overflow allocation and convert to inline so the lengths are set for record scanning or revivification.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ClearObjectIdAndConvertToInline(ref RecordInfo recordInfo, long physicalAddress, ObjectIdMap objectIdMap, bool isKey, Action<IHeapObject> objectDisposer = null)
        {
            Debug.Assert(isKey ? !recordInfo.KeyIsInline : !recordInfo.ValueIsInline);

            // We don't have to adjust the filler length, since the field size here isn't changing; we'll just have int-sized "data". This method is called by record disposal, which
            // also clears the optionals, which may adjust filler length. Consistency Note: LogRecord.InitializeForReuse also sets field lengths to zero and sets the filler length.
            var fieldAddress = GetFieldPtr(physicalAddress + RecordInfo.Size, isKey, out _ /*lengthPtr*/, out _ /*lengthBytes*/, out _ /*length*/);
            var objectId = *(int*)fieldAddress;
            if (objectId != ObjectIdMap.InvalidObjectId)
                objectIdMap.Free(objectId, objectDisposer);
            *(int*)fieldAddress = 0;

            // We don't need to change the length; we'll keep the current length and just convert to inline.
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
        internal static Span<byte> ConvertHeapObjectToInline(ref RecordInfo recordInfo, long physicalAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            var fieldAddress = sizeInfo.GetValueAddress(physicalAddress);
            var objIdPtr = (int*)fieldAddress;
            var objectId = *objIdPtr;
            if (objectId != ObjectIdMap.InvalidObjectId)
                objectIdMap.Free(objectId);
            *objIdPtr = 0;

            recordInfo.SetValueIsInline();
            return new((byte*)fieldAddress, sizeInfo.FieldInfo.ValueSize);
        }

        /// <summary>
        /// Utility function to get the inline length of a Span field; this is either the datalength if the field is inline, or <see cref="ObjectIdMap.ObjectIdSize"/>
        /// for Overflow or Object.
        /// </summary>
        internal static int GetInlineDataLength(long physicalAddress, bool isKey)
        {
            _ = GetFieldPtr(physicalAddress + RecordInfo.Size, isKey, out _ /*lengthPtr*/, out var _ /*lengthBytes*/, out var length);
            return (int)length;
        }

        /// <summary>
        /// Reallocate a Span field that is overflow, e.g. to make the overflow allocation larger. Shrinkage is done in-place (the caller decides if the
        /// shrinkage is sufficient (given available space in the record) to convert the field in-place to inline.
        /// </summary>
        /// <remarks>
        /// Applies to Value only, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an overflow allocation.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ReallocateValueOverflow(long physicalAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            OverflowByteArray newOverflow;
            var fieldAddress = sizeInfo.GetValueAddress(physicalAddress);
            var newLength = sizeInfo.FieldInfo.ValueSize;

            var objectId = *(int*)fieldAddress;
            if (objectId != ObjectIdMap.InvalidObjectId)
            {
                var oldOverflow = objectIdMap.GetOverflowByteArray(objectId);
                var oldSpan = oldOverflow.Span;
                if (oldSpan.Length == newLength)
                    return oldSpan;

                // AllocateUninitialized and copy, and zeroinit any remainder
                newOverflow = new(newLength, startOffset: 0, endOffset: 0, zeroInit: false);
                var copyLength = oldSpan.Length < newLength ? oldSpan.Length : newLength;

                oldOverflow.Span.Slice(0, copyLength).CopyTo(newOverflow.Span);
                if (copyLength < newLength)
                    newOverflow.Span.Slice(copyLength, newLength - copyLength).Clear();
            }
            else
            {
                // Allocate; nothing to copy, so allocate with zero initialization
                newOverflow = new(newLength, startOffset: 0, endOffset: 0, zeroInit: false);
                objectId = objectIdMap.Allocate();
                *(int*)fieldAddress = objectId;
            }
            objectIdMap.Set(objectId, newOverflow);
            return newOverflow.Span;
        }
    }
}