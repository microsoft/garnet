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
    /// at the same time we are adjusting the Value length, <see cref="RecordInfo.ValueIsOverflow"/> bit, and optional field offsets.
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
        /// <summary>
        /// This is the size of the length prefix on Span field.
        /// </summary>
        public const int FieldLengthPrefixSize = sizeof(int);

        /// <summary>
        /// This is the size of the overflow pointer if <see cref="RecordInfo.KeyIsOverflow"/> or if <see cref="RecordInfo.ValueIsOverflow"/>.
        /// </summary>
        internal const int OverflowDataPtrSize = sizeof(long);  // (sizeof(IntPtr) or (nuint) is not a constant expression so use (long)

        /// <summary>
        /// This is the inline size of an overflow (out-of-line, non-serialized) Key or Value; a field's inline size must be at least this to be able to convert it to an overflow pointer.
        /// </summary>
        internal const int OverflowInlineSize = FieldLengthPrefixSize + OverflowDataPtrSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref int GetLengthRef(long address) => ref *(int*)address;

        /// <summary>
        /// Address of the actual data (past the length prefix). This may be either the start of the stream of bytes, or a pointer to an overflow allocation.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetDataAddress(long address) => address + FieldLengthPrefixSize;

        /// <summary>
        /// Gets the out-of-line pointer at address (which is KeyAddress or ValueAddress).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static IntPtr GetOverflowPointer(long address) => *(IntPtr*)GetDataAddress(address);

        /// <summary>
        /// Sets the out-of-line pointer at address (which is KeyAddress or ValueAddress).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SetOverflowPointer(long address, IntPtr pointer) => *(IntPtr*)GetDataAddress(address) = pointer;

        /// <summary>
        /// Size of the actual data (not including the length prefix).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetDataSize(long address, bool isOverflow) => !isOverflow ? GetLengthRef(address) : BlockHeader.GetUserSize(GetOverflowPointer(address));

        /// <summary>
        /// Total inline size of the field: The length prefix plus: length of byte stream if not overflow, else length of the pointer to overflow data.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetInlineSize(long address) => FieldLengthPrefixSize + GetLengthRef(address);

        /// <summary>
        /// Obtain a <see cref="Span{_byte_}"/> referencing the inline or overflow data and the datasize for this field.
        /// </summary>
        /// <remarks>Note: SpanByte.CopyTo(<see cref="Span{_byte_}"/> x) will assume that x is a serialized SpanByte space, and will write length to the first sizeof(int) bytes of the span data</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> AsSpan(long address, bool isOverflow)
        {
            var dataAddress = (byte*)GetDataAddress(address);
            if (!isOverflow)
                return new(dataAddress, GetLengthRef(address));
            dataAddress = *(byte**)dataAddress;     // dataAddress contains the pointer
            return new(dataAddress, BlockHeader.GetUserSize((long)dataAddress));
        }

        /// <summary>
        /// Obtain a <see cref="SpanByte"/> referencing the inline or overflow data and the datasize for this field.
        /// </summary>
        /// <remarks>Note: returns non-serialized as it is not passed by ref</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static SpanByte AsSpanByte(long address, bool isOverflow)
        {
            var dataAddress = (byte*)GetDataAddress(address);
            if (!isOverflow)
                return new(GetLengthRef(address), (IntPtr)dataAddress);
            dataAddress = *(byte**)dataAddress;     // dataAddress contains the pointer
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
            => ZeroData(GetDataAddress(address) + dataOffset, clearLength);

        /// <summary>
        /// Set all data within a portion of a field to zero.
        /// </summary>
        /// <param name="clearStart">Address to start clearing at</param>
        /// <param name="clearLength">Length of the data from <paramref name="clearStart"/> to zero</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ZeroData(long clearStart, int clearLength)
            => new Span<byte>((byte*)clearStart, clearLength).Clear();

        /// <summary>
        /// Convert a Span field from inline to overflow.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field does not currently contain an overflow allocation. Applies to Keys as well during freelist revivification.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* ConvertToOverflow(long address, int newLength, OverflowAllocator allocator)
        {
            // First copy the data
            byte* newPtr = allocator.Allocate(newLength, zeroInit: false);
            var oldLength = GetLengthRef(address);
            var copyLength = oldLength < newLength ? oldLength : newLength;
            var oldPtr = (byte*)GetDataAddress(address);
            Buffer.MemoryCopy(oldPtr, newPtr, newLength, copyLength);

            // If "shrinking" the allocation because the overflow pointer size is less than the current inline size, we must zeroinit the extra space.
            var clearLength = GetLengthRef(address) - OverflowDataPtrSize;
            if (clearLength > 0)
                ZeroInlineData(address, OverflowDataPtrSize, clearLength);

            // Now clear any extra space in the new allocation beyond what we copied from the old data.
            clearLength = newLength - copyLength;
            if (clearLength > 0)
                ZeroData((long)newPtr + copyLength, clearLength);

            return SetOverflowAllocation(address, newPtr);
        }

        /// <summary>
        /// Utility function to set the overflow allocation at the given Span field's address. Assumes caller has ensured no existing overflow
        /// allocation is there; e.g. SerializeKey and InitializeValue.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* SetOverflowAllocation(long address, int newLength, OverflowAllocator allocator)
            => SetOverflowAllocation(address, allocator.Allocate(newLength, zeroInit: false));

        /// <summary>
        /// Utility function to set the overflow allocation at the given Span field's address. Assumes caller has ensured no existing overflow
        /// allocation is there; e.g. SerializeKey and InitializeValue.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* SetOverflowAllocation(long address, byte* ptr)
        {
            GetLengthRef(address) = sizeof(IntPtr);                         // actual length (i.e. the size of the out-of-line allocation)
            SetOverflowPointer(address, (IntPtr)ptr);                       // out-of-line data pointer
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
        internal static byte* ConvertToInline(long address, int newLength, OverflowAllocator allocator)
        {
            // First copy the data
            var oldPtr = (byte*)GetOverflowPointer(address);
            var oldLength = BlockHeader.GetUserSize((long)oldPtr);
            var copyLength = oldLength < newLength ? oldLength : newLength;
            var newPtr = SetInlineDataLength(address, newLength);
            Buffer.MemoryCopy(oldPtr, newPtr, newLength, copyLength);
            allocator.Free((long)oldPtr);

            // We are either making an inline allocation >= old inline length, which was OverflowDataPtrSize, or we are "shrinking" the inline allocation
            // because the new inline size is less than the inline size of the pointer. In the latter case we must zeroinit the extra space. If we grew the
            // inline size, we must have already zero-init'd the extra space.
            var clearLength = OverflowDataPtrSize - newLength;
            if (clearLength > 0)
                ZeroInlineData(address, OverflowDataPtrSize - clearLength, clearLength);

            return newPtr;
        }

        /// <summary>
        /// Utility function to set the inline length of a Span field and return a pointer to the data start (which may be a byte stream or a pointer to overflow data).
        /// </summary>
        internal static byte* SetInlineDataLength(long address, int newLength)
        {
            GetLengthRef(address) = newLength;             // actual length (i.e. the inline data space used by this field)
            return (byte*)GetDataAddress(address);
        }

        /// <summary>
        /// Shrink an inline Span field in place.
        /// </summary>
        /// <remarks>
        /// Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
        /// and that the field currently contains an overflow allocation. Applies to Keys as well during freelist revivification.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* AdjustInlineLength(long address, int newLength)
        {
            // Zeroinit the extra space. Here we are concerned about shrinkage leaving nonzero leftovers, so we clear those.
            var clearLength = GetLengthRef(address) - newLength;
            if (clearLength > 0)
                ZeroInlineData(address, newLength, clearLength);
            GetLengthRef(address) = newLength;
            return (byte*)GetDataAddress(address);
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
        internal static byte* ReallocateOverflow(long address, int newLength, OverflowAllocator allocator)
        {
            // First see if the existing allocation is large enough. If we are shrinking we don't need to zeroinit in the oversize allocations
            // because there is no "log scan to next record" there.
            var oldPtr = (byte*)GetOverflowPointer(address);
            var oldLength = BlockHeader.GetUserSize((long)oldPtr);
            if (allocator.TryRealloc((long)oldPtr, newLength, out byte* newPtr))
            {
                SetOverflowPointer(address, (IntPtr)newPtr);
                var clearLength = newLength - oldLength;
                if (clearLength > 0)
                    ZeroData((long)newPtr + oldLength, clearLength);
                return newPtr;
            }

            // Allocate and insert a new block, copy to it, then free the current allocation
            newPtr = SetOverflowAllocation(address, newLength, allocator);
            var copyLength = oldLength < newLength ? oldLength : newLength;
            Buffer.MemoryCopy(oldPtr, newPtr, newLength, copyLength);
            if (copyLength < newLength)
                ZeroData((long)newPtr + copyLength, newLength - copyLength);
            allocator.Free((long)oldPtr);
            return newPtr;
        }
    }
}