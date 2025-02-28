// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using static Tsavorite.core.OverflowAllocator;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>
    /// Struct encoding a Span field (Key or Value) at a certain address. The layout is:
    /// <list type="bullet">
    ///     <item>Inline: [int Length][data bytes]</item>
    ///     <item>Overflow: [<see cref="IntPtr"/> Length][<see cref="IntPtr"/> to overflow allocation containing data bytes]</item>
    ///     <br>The data bytes are laid out as in the <see cref="BlockHeader"/> description:</br>
    ///     <list type="bullet">
    ///         <item>[int allocatedSize][int userSize] for fixed-length data (less than or equal to <see cref="FixedSizePages.MaxExternalBlockSize"/>)</item>
    ///         <item>[int allocatedSize][int nextFreeSlot] for oversize data (greater than <see cref="FixedSizePages.MaxExternalBlockSize"/>)</item>
    ///     </list>
    /// </list>
    /// The [<see cref="IntPtr"/> size] prefix for Overflow is necessary to ensure proper zero-initialization layout of the record if a checkpoint is happening 
    /// at the same time we are adjusting the Value length, <see cref="RecordInfo.ValueIsInline"/> bit, and optional field offsets.
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
        /// This is the inline size of an overflow (out-of-line) Key or Span Value; the size of the overflow pointer. There is no length prefix for
        /// this field. A field's inline size must be at least this to be able to convert it to an overflow pointer.
        /// For an Object record, this should not be used; use <see cref="ObjectIdMap.ObjectIdSize"/> instead.
        /// </summary>
        internal const int OverflowInlineSize = sizeof(long);   // (sizeof(IntPtr) or (nuint) is not a constant expression so use (long)

        /// <summary>Gets the out-of-line pointer at address (which is KeyAddress or ValueAddress). There is no length prefix.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr GetOverflowPointer(long fieldAddress) => *(IntPtr*)fieldAddress;

        /// <summary>Sets the out-of-line pointer at address (which is KeyAddress or ValueAddress).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetOverflowPointer(long fieldAddress, IntPtr pointer) => *(IntPtr*)fieldAddress = pointer;

        /// <summary>Gets a pointer to the ObjectId at address (which is ValueAddress). There is no length prefix.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int* GetObjectIdPointer(long fieldAddress) => (int*)fieldAddress;

        /// <summary>For an inline field, get the total inline size of the field: The length prefix plus the length of the byte stream</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetTotalSizeOfInlineField(long fieldAddress) => FieldLengthPrefixSize + GetInlineLengthRef(fieldAddress);

        /// <summary>Get a field's inline length, depending on whether it is actually inline or whether it is an out-of-line pointer.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetInlineTotalSizeOfField(long fieldAddress, bool isInline) => isInline ? FieldLengthPrefixSize + GetInlineLengthRef(fieldAddress) : OverflowInlineSize;

        /// <summary>
        /// Obtain a <see cref="Span{_byte_}"/> referencing the inline or overflow data and the datasize for this field.
        /// </summary>
        /// <remarks>Note: SpanByte.CopyTo(<see cref="Span{_byte_}"/> destSpan) will assume that destSpan is a serialized SpanByte space, and will write length to the first sizeof(int) bytes of the span data</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> AsSpan(long fieldAddress, bool isInline)
        {
            if (isInline)
                return new((byte*)GetInlineDataAddress(fieldAddress), GetInlineLengthRef(fieldAddress));
            var dataAddress = (byte*)GetOverflowPointer(fieldAddress);
            return new(dataAddress, BlockHeader.GetUserSize((long)dataAddress));
        }

        /// <summary>
        /// Obtain a <see cref="SpanByte"/> referencing the inline or overflow data and the datasize for this field.
        /// </summary>
        /// <remarks>Note: returns non-serialized as it is not passed by ref</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static SpanByte AsSpanByte(long fieldAddress, bool isInline)
        {
            if (isInline)
                return new(GetInlineLengthRef(fieldAddress), (IntPtr)GetInlineDataAddress(fieldAddress));
            var dataAddress = (byte*)GetOverflowPointer(fieldAddress);
            return new(BlockHeader.GetUserSize((long)dataAddress), (IntPtr)dataAddress);
        }

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
        internal static byte* ConvertToOverflow(long fieldAddress, int newLength, OverflowAllocator allocator)
        {
            // First copy the data
            byte* newPtr = allocator.Allocate(newLength, zeroInit: false);
            var oldLength = GetInlineLengthRef(fieldAddress);
            var copyLength = oldLength < newLength ? oldLength : newLength;
            var oldPtr = (byte*)GetInlineDataAddress(fieldAddress);
            Buffer.MemoryCopy(oldPtr, newPtr, newLength, copyLength);

            // If "shrinking" the allocation because the overflow pointer size is less than the current inline size, we must zeroinit the extra space.
            var clearLength = oldLength - OverflowInlineSize;
            if (clearLength > 0)
                ZeroInlineData(fieldAddress, OverflowInlineSize, clearLength);

            // Now clear any extra space in the new allocation beyond what we copied from the old data.
            clearLength = newLength - copyLength;
            if (clearLength > 0)
                ZeroData((long)newPtr + copyLength, clearLength);

            SetOverflowPointer(fieldAddress, (IntPtr)newPtr);
            return newPtr;
        }

        /// <summary>
        /// Convert a Span field from inline to ObjectId.
        /// </summary>
        /// <remarks>
        /// Applies to Value during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field does not currently contain a valid ObjectId.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int ConvertToObjectId<TValue>(long fieldAddress, int newLength, ObjectIdMap<TValue> objectIdMap)
        {
            // Here we do not copy the data; we assume the caller will have already created an object that has converted from inline format to object format.
            var objectId = objectIdMap.Allocate();
            var oldLength = GetInlineLengthRef(fieldAddress);

            // If "shrinking" the allocation because ObjectIdSize is less than the current inline size, we must zeroinit the extra space.
            var clearLength = oldLength - ObjectIdMap.ObjectIdSize;
            if (clearLength > 0)
                ZeroInlineData(fieldAddress, ObjectIdMap.ObjectIdSize, clearLength);

            *GetObjectIdPointer(fieldAddress) = objectId;
            return objectId;
        }

        /// <summary>
        /// Utility function to set the overflow allocation at the given Span field's address. Assumes caller has ensured no existing overflow
        /// allocation is there; e.g. SerializeKey and InitializeValue.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* SetOverflowAllocation(long fieldAddress, int newLength, OverflowAllocator allocator)
        {
            var ptr = allocator.Allocate(newLength, zeroInit: false);
            SetOverflowPointer(fieldAddress, (IntPtr)ptr);
            return ptr;
        }

        /// <summary>
        /// Convert a Span field from overflow to inline.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an overflow allocation. Applies to Keys as well during freelist revivification.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* ConvertToInline(long fieldAddress, int newLength, OverflowAllocator allocator)
        {
            // First copy the data
            var oldPtr = (byte*)GetOverflowPointer(fieldAddress);
            var oldLength = BlockHeader.GetUserSize((long)oldPtr);
            var copyLength = oldLength < newLength ? oldLength : newLength;
            var newPtr = SetInlineDataLength(fieldAddress, newLength);
            Buffer.MemoryCopy(oldPtr, newPtr, newLength, copyLength);
            allocator.Free((long)oldPtr);

            // We are either making an inline allocation >= old inline length, which was OverflowDataPtrSize, or we are "shrinking" the inline allocation
            // because the new inline size is less than the inline size of the pointer. In the latter case we must zeroinit the extra space. If we grew the
            // inline size, we must have already zero-init'd the extra space.
            var clearLength = OverflowInlineSize - newLength;
            if (clearLength > 0)
                ZeroInlineData(fieldAddress, OverflowInlineSize - clearLength, clearLength);

            return newPtr;
        }

        /// <summary>
        /// Convert a Value field from ObjectId to inline.
        /// </summary>
        /// <remarks>
        /// Applies to Value during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an ObjectId (which may be ObjectIdMap.InvalidObjectId).
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* ConvertToInline<TValue>(long fieldAddress, int newLength, ObjectIdMap<TValue> objectIdMap)
        {
            // Here we do not copy the data; we assume the caller will have already prepared to convert from Object format to inline format.
            objectIdMap.Free(*GetObjectIdPointer(fieldAddress));
            var newPtr = SetInlineDataLength(fieldAddress, newLength);

            // We are either making an inline allocation >= old inline length, which was ObjectIdMap.ObjectIdSize, or we are "shrinking" the inline allocation
            // because the new inline size is less than the inline size of the pointer. In the latter case we must zeroinit the extra space. If we grew the
            // inline size, we must have already zero-init'd the extra space.
            var clearLength = ObjectIdMap.ObjectIdSize - newLength;
            if (clearLength > 0)
                ZeroInlineData(fieldAddress, ObjectIdMap.ObjectIdSize - clearLength, clearLength);

            return newPtr;
        }

        /// <summary>
        /// Utility function to set the inline length of a Span field and return a pointer to the data start (which may be a byte stream or a pointer to overflow data).
        /// </summary>
        internal static byte* SetInlineDataLength(long fieldAddress, int newLength)
        {
            GetInlineLengthRef(fieldAddress) = newLength;             // actual length (i.e. the inline data space used by this field)
            return (byte*)GetInlineDataAddress(fieldAddress);
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
        internal static byte* ReallocateOverflow(long fieldAddress, int newLength, OverflowAllocator allocator)
        {
            // First see if the existing allocation is large enough. If we are shrinking we don't need to zeroinit in the oversize allocations
            // because there is no "log scan to next record" there.
            var oldPtr = (byte*)GetOverflowPointer(fieldAddress);
            var oldLength = BlockHeader.GetUserSize((long)oldPtr);
            if (allocator.TryRealloc((long)oldPtr, newLength, out byte* newPtr))
            {
                SetOverflowPointer(fieldAddress, (IntPtr)newPtr);    // Currently this will probably be the same pointer
                var clearLength = newLength - oldLength;
                if (clearLength > 0)
                    ZeroData((long)newPtr + oldLength, clearLength);
                return newPtr;
            }

            // Allocate and insert a new block, copy to it, then free the current allocation
            newPtr = SetOverflowAllocation(fieldAddress, newLength, allocator);
            var copyLength = oldLength < newLength ? oldLength : newLength;
            Buffer.MemoryCopy(oldPtr, newPtr, newLength, copyLength);
            if (copyLength < newLength)
                ZeroData((long)newPtr + copyLength, newLength - copyLength);
            allocator.Free((long)oldPtr);
            return newPtr;
        }
    }
}