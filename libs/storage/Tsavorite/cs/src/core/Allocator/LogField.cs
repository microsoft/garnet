// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Static class providing functions to operate on a Log field (Key Span, or Value Span or Object) at a certain address. Since (small) Objects can be represented
    /// as inline spans, this applies to those forms as well as the inline component of the Object, which is the ObjectId. The layout is:
    /// <list type="bullet">
    ///     <item>RecordDataHeader indicator byte and lengths; see <see cref="VarbyteLengthUtility"/> header comments for details</item>
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
    internal static unsafe class LogField
    {
        /// <summary>
        /// Convert a Span field from inline to overflow.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field does not currently contain an overflow allocation.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ConvertInlineToOverflow(ref RecordInfo recordInfo, long physicalAddress, long valueAddress, long oldValueLength, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsInline);

            // First copy the data. We are converting to overflow so the length is limited to int.
            var newLength = sizeInfo.FieldInfo.ValueSize;
            var overflow = new OverflowByteArray(newLength, startOffset: 0, endOffset: 0, zeroInit: false);
            var copyLength = oldValueLength < newLength ? oldValueLength : newLength;

            if (copyLength > 0)
            {
                var oldSpan = new ReadOnlySpan<byte>((byte*)valueAddress, (int)copyLength);
                oldSpan.CopyTo(overflow.Span);
            }

            var objectId = objectIdMap.Allocate();
            *(int*)valueAddress = objectId;
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
        internal static Span<byte> ConvertValueObjectToOverflow(ref RecordInfo recordInfo, long physicalAddress, long valueAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsObject);
            var overflow = new OverflowByteArray(sizeInfo.FieldInfo.ValueSize, startOffset: 0, endOffset: 0, zeroInit: false);

            var objectId = *(int*)valueAddress;
            if (objectId == ObjectIdMap.InvalidObjectId)
                *(int*)valueAddress = objectId = objectIdMap.Allocate();
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
        internal static int ConvertInlineToValueObject(ref RecordInfo recordInfo, long physicalAddress, long valueAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsInline);
            var objectId = objectIdMap.Allocate();
            recordInfo.SetValueIsObject();

            *(int*)valueAddress = objectId;
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
        internal static int ConvertOverflowToValueObject(ref RecordInfo recordInfo, long physicalAddress, long valueAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsOverflow);

            var objectId = *(int*)valueAddress;
            if (objectId != ObjectIdMap.InvalidObjectId)
                objectIdMap.Set(objectId, null);    // Clear the byte[] from the existing slot but do not free the slot; caller will put the HeapObject into the slot.
            else
                *(int*)valueAddress = objectId = objectIdMap.Allocate();

            recordInfo.SetValueIsObject();
            return objectId;
        }

        /// <summary>
        /// Convert a Span field from overflow to inline.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an overflow allocation.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> ConvertOverflowToInline(ref RecordInfo recordInfo, long physicalAddress, long valueAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            Debug.Assert(recordInfo.ValueIsOverflow);

            // First copy the data
            var objectId = *(int*)valueAddress;

            var newLength = sizeInfo.FieldInfo.ValueSize;
            var newSpan = new Span<byte>((byte*)valueAddress, newLength);

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
        internal static void ClearObjectIdAndConvertToInline(ref RecordInfo recordInfo, long fieldAddress, ObjectIdMap objectIdMap, bool isKey, Action<IHeapObject> objectDisposer = null)
        {
            Debug.Assert(isKey ? !recordInfo.KeyIsInline : !recordInfo.ValueIsInline);

            // We don't have to adjust the filler length, since the field size here isn't changing; we'll just have int-sized "data". This method is called by record disposal, which
            // also clears the optionals, which may adjust filler length. Consistency Note: LogRecord.InitializeForReuse also sets field lengths to zero and sets the filler length.
            // However, here we may be called after setting the IgnoreOptionals word, so we don't want to decode the indicator.
            var objectId = *(int*)fieldAddress;
            if (objectId != ObjectIdMap.InvalidObjectId)
            {
                objectIdMap.Free(objectId, objectDisposer);
                *(int*)fieldAddress = ObjectIdMap.InvalidObjectId;
            }

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
        internal static Span<byte> ConvertValueObjectToInline(ref RecordInfo recordInfo, long physicalAddress, long valueAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            var objIdPtr = (int*)valueAddress;
            var objectId = *objIdPtr;
            if (objectId != ObjectIdMap.InvalidObjectId)
                objectIdMap.Free(objectId);
            *objIdPtr = 0;

            recordInfo.SetValueIsInline();
            return new((byte*)valueAddress, sizeInfo.FieldInfo.ValueSize);
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
        internal static Span<byte> ReallocateValueOverflow(long physicalAddress, long valueAddress, in RecordSizeInfo sizeInfo, ObjectIdMap objectIdMap)
        {
            OverflowByteArray newOverflow;
            var newLength = sizeInfo.FieldInfo.ValueSize;

            var objectId = *(int*)valueAddress;
            if (objectId != ObjectIdMap.InvalidObjectId)
            {
                var oldOverflow = objectIdMap.GetOverflowByteArray(objectId);
                var oldSpan = oldOverflow.Span;
                if (oldSpan.Length == newLength)
                    return oldSpan;

                // AllocateUninitialized and copy, and zeroinit any remainder
                newOverflow = new(newLength, startOffset: 0, endOffset: 0, zeroInit: false);
                var copyLength = oldSpan.Length < newLength ? oldSpan.Length : newLength;

                oldOverflow.AsReadOnlySpan(0, copyLength).CopyTo(newOverflow.Span);
                if (copyLength < newLength)
                    newOverflow.AsSpan(copyLength, newLength - copyLength).Clear();
            }
            else
            {
                // Allocate; nothing to copy, so allocate with zero initialization
                newOverflow = new(newLength, startOffset: 0, endOffset: 0, zeroInit: false);
                objectId = objectIdMap.Allocate();
                *(int*)valueAddress = objectId;
            }
            objectIdMap.Set(objectId, newOverflow);
            return newOverflow.Span;
        }
    }
}