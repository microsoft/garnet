// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    internal unsafe partial class OverflowAllocator
    {
        /// <summary>
        /// A header for an individual block of memory (a single allocation)
        /// </summary>

        [StructLayout(LayoutKind.Explicit, Size = sizeof(int) * 2)]
        internal struct BlockHeader
        {
            /// <summary>Full size of this block. For FixedSizePages this is the next-highest power of 2 and keys freelist operations; for OversizePages it is the size of the single allocation per page.</summary>
            /// <remarks>If this size is &lt;= <see cref="FixedSizePages.MaxBlockSize"/>, this allocation is from the fixed-size region, else it is from the oversize region.</remarks>
            [FieldOffset(0)]
            internal int AllocatedSize;

            #region Union of UserSize and Slot
            /// <summary>Union element 1: For FixedSizePages, the user request size. For OversizePages, there is only one allocation per page, 
            /// so it is always the same size as the user request size and thus need not be repeated.</summary>
            [FieldOffset(sizeof(int))]
            internal int UserSize;

            /// <summary>Union element 2: For OversizePages, the block's slot; used to track free slots in the page vector freelist.</summary>
            [FieldOffset(sizeof(int))]
            internal int Slot;
            #endregion // Union of UserSize and NextSlot

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static byte* InitializeFixedSize(BlockHeader* blockPtr, int blockSize, int userSize, bool zeroInit)
            {
                // This is for OversizePages for which UserSize is <= BlockSize and a slot is not present
                return Initialize(blockPtr, blockSize, userSize, zeroInit);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static byte* InitializeOversize(BlockHeader* blockPtr, int allocatedSize, int slot, bool zeroInit)
            {
                // This is for OversizePages, for which UserSize is the same as allocatedSize but a slot is present
                return Initialize(blockPtr, allocatedSize, slot, zeroInit);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static byte* Initialize(BlockHeader* blockPtr, int allocatedSize, int userSizeOrSlot, bool zeroInit)
            {
                blockPtr->AllocatedSize = allocatedSize;
                allocatedSize -= sizeof(BlockHeader);

                // Update the union; both elements are int, so just pick one for the lhs
                blockPtr->UserSize = userSizeOrSlot;

                ++blockPtr;     // Move to the data space
                if (zeroInit)
                    NativeMemory.Clear(blockPtr, (nuint)(allocatedSize));
                return (byte*)blockPtr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static BlockHeader* FromUserAddress(long address) => ((BlockHeader*)address - 1);

            internal bool IsOversize => AllocatedSize > FixedSizePages.MaxBlockSize;

            /// <summary>Get the user size of the allocation.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int GetUserSize(long address)
            {
                // UserSize is the same as AllocatedSize for Oversize, so we don't carry a separate UserSize field for that (we union it with the FreeList slots instead).
                var blockPtr = FromUserAddress(address);
                return !blockPtr->IsOversize ? blockPtr->UserSize : blockPtr->AllocatedSize;
            }

            /// <inheritdoc/>
            public override readonly string ToString()
                => $"allocatedSize {AllocatedSize}, UserSizeOrSlot {UserSize}";
        }
    }
}
