// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Tsavorite.core.Utility;

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
            internal int BlockSize;

            #region Union of UserSize and NextSlot
            /// <summary>Union element 1: For FixedSizePages, the user request size. For OversizePages, there is only one allocation per page, 
            /// so it is always the same size as the user request size and thus need not be repeated.</summary>
            [FieldOffset(sizeof(int))]
            internal int UserSize;

            /// <summary>Union element 2: For OversizePages, the next slot in the page vector freelist.</summary>
            [FieldOffset(sizeof(int))]
            internal int NextSlot;
            #endregion // Union of UserSize and NextSlot

            /// <summary>Include blockHeader in the allocation size. For FixedSizeBlocks, this is included in the internal request size, so blocks remain power-of-2.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int PromoteSize(int size) => (int)NextPowerOf2(size + sizeof(BlockHeader));

            /// <summary>Get the user size of the allocation.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int GetUserSize(long address) => ((BlockHeader*)address - 1)->UserSize;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static byte* InitializeFixedSize(BlockHeader* blockPtr, int blockSize, int userSize, bool zeroInit)
            {
                // This is for OversizePages for which UserSize is <= BlockSize and a slot is not present
                return Initialize(blockPtr, blockSize, userSize, zeroInit);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static byte* InitializeOversize(BlockHeader* blockPtr, int blockSize, int slot, bool zeroInit)
            {
                // This is for OversizePages for which UserSize is the same as BlockSize but a slot is present
                return Initialize(blockPtr, blockSize, slot, zeroInit);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static byte* Initialize(BlockHeader* blockPtr, int blockSize, int userSizeOrSlot, bool zeroInit)
            {
                blockSize -= sizeof(BlockHeader);
                blockPtr->BlockSize = blockSize;

                // Update the union; both elements are int
                blockPtr->UserSize = userSizeOrSlot;

                ++blockPtr;
                if (zeroInit)
                    NativeMemory.Clear(blockPtr + 1, (nuint)(blockSize - sizeof(BlockHeader)));
                return (byte*)blockPtr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static BlockHeader* FromUserAddress(long address) => ((BlockHeader*)address - 1);
        }
    }
}
