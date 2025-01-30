// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>
    /// Struct encoding a Span field (Key or Value) at a certain address. The layout is:
    /// <list type="bullet">
    ///     <item>Inline: [int size = # of data bytes][inline data bytes]</item>
    ///     <item>Overflow: [int size = sizeof(IntPtr)][Intptr overflow_pointer] (see <see cref="OverflowInlineSize"/>)</item>
    /// </list>
    /// The [int size] prefix for Overflow is necessary to ensure proper zero-initialization layout of the record if a checkpoint is happening 
    /// at the same time we are adjusting the Value length, <see cref="RecordInfo.ValueIsOverflow"/> bit, and optional field offsets.
    /// </summary>
    /// <remarks>Considerations regarding variable field sizes:
    /// <list type="bullet">
    ///     <item>Keys are immutable (unless revivification is happening), so the inline size of a Key field does not change</item>
    ///     <item>When Values change size the Filler length and offsets to optional ETag and Extension are adjusted. Converting between inline and out-of-line
    ///         due to size changes altering whether the Value overflows is handled as part of normal Value-sizechange operations</item>
    /// </list>
    /// </remarks>
    internal unsafe struct SpanField
    {
        /// <summary>
        /// This is the size of the length prefix on Span field.
        /// </summary>
        internal const int FieldLengthPrefixSize = sizeof(int);

        /// <summary>
        /// This is the size of the overflow pointer if <see cref="RecordInfo.KeyIsOverflow"/> or if <see cref="RecordInfo.ValueIsOverflow"/>.
        /// </summary>
        internal const int OverflowDataPtrSize = sizeof(long);  // (sizeof(IntPtr) or (nuint) is not a constant expression so use (long)

        /// <summary>
        /// This is the inline size of an overflow (out-of-line, non-serialized) Key or Value; a field's inline size must be at least this to be able to convert it to an overflow pointer.
        /// </summary>
        internal const int OverflowInlineSize = FieldLengthPrefixSize + OverflowDataPtrSize;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref int LengthRef(long address) => ref *(int*)address;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long DataAddress(long address) => (IntPtr)(address + FieldLengthPrefixSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int InlineSize(long address) => FieldLengthPrefixSize + LengthRef(address);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int DataSize(long address) => LengthRef(address);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static Span<byte> AsSpan(long address) => new((byte*)(address + FieldLengthPrefixSize), LengthRef(address));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ZeroInlineData(long address, int dataOffset, int clearLength) 
            => new Span<byte>((byte*)(DataAddress(address) + dataOffset), clearLength).Clear();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* ConvertToOverflow(long address, int newLength, OverflowAllocator allocator)
        {
            // Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
            // and that the field does not currently contain an overflow allocation. Applies to Keys as well during freelist revivification.

            // If "shrinking" the allocation because the overflow pointer size is less than the current inline size, we must zeroinit the extra space.
            var clearLength = LengthRef(address) - OverflowDataPtrSize;
            if (clearLength > 0)
                ZeroInlineData(address, OverflowDataPtrSize, clearLength);

            return SetOverflowAllocation(address, newLength, allocator);
        }

        internal static byte* SetOverflowAllocation(long address, int newLength, OverflowAllocator allocator)
        {
            LengthRef(address) = sizeof(IntPtr);                        // actual length (i.e. the size of the out-of-line allocation)
            byte* ptr = allocator.Allocate(newLength, zeroInit: false);
            *(IntPtr*)(address + FieldLengthPrefixSize) = (IntPtr)ptr;  // out-of-line data pointer
            return ptr;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* ConvertToInline(long address, int newLength, OverflowAllocator allocator)
        {
            // Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
            // and that the field currently contains an overflow allocation. Applies to Keys as well during freelist revivification.

            // If "shrinking" the allocation because the new inline size is less than the inline size, we must zeroinit the extra space.
            var clearLength = OverflowDataPtrSize - newLength;
            if (clearLength > 0)
                ZeroInlineData(address, OverflowDataPtrSize - clearLength, clearLength);

            allocator.Free(address);
            return SetInlineLength(address, newLength);
        }

        internal static byte* SetInlineLength(long address, int newLength)
        {
            LengthRef(address) = newLength;                             // actual length (i.e. the inline data space used by this field)
            return (byte*)(address + FieldLengthPrefixSize);            // Data pointer
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* ShrinkInline(long address, int newLength)
        {
            // Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
            // and that the field currently contains an overflow allocation. Applies to Keys as well during freelist revivification.

            // Zeroinit the extra space.
            var clearLength = LengthRef(address) - newLength;
            if (clearLength > 0)
                ZeroInlineData(address, newLength, clearLength);
            return (byte*)(address + FieldLengthPrefixSize);            // Data pointer
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static byte* ReallocateOverflow(long address, int newLength, OverflowAllocator allocator)
        {
            // Applies to Value-only during normal ops, and assumes any record size adjustment due to Value growth/shrinkage has already been handled
            // and that the field currently contains an overflow allocation. Applies to Keys as well during freelist revivification.
            
            // First see if the existing allocation is large enough. If we are shrinking we don't need to zeroinit in the oversize allocations
            // because there is no "log scan to next record" there.
            if (allocator.GetAllocatedSize(address) >= newLength)
            {
                LengthRef(address) = newLength;                          // actual length (i.e. the inline data space used by this field)
                return (byte*)address;
            }

            // Free the current allocation then insert a new one
            allocator.Free(address);
            return ConvertToOverflow(address, newLength, allocator);
        }
    }
}