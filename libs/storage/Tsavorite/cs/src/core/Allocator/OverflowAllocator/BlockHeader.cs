// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
        internal struct BlockHeader
        {
            /// <summary>Full size of this block. Actual used size is kept by LogRecord{TValue} in the key or value Span length.</summary>
            /// <remarks>If this size is &lt;= <see cref="FixedSizePages.MaxBlockSize"/>, this allocation is from the fixed-size region, else it is from the oversize region.</remarks>
            internal int Size;

            /// <summary>Include blockHeader in the allocation size. For FixedSizeBlocks, this is included in the internal request size, so blocks remain power-of-2.</summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int PromoteSize(int size) => (int)NextPowerOf2(size + sizeof(BlockHeader));

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static byte* Initialize(BlockHeader* blockPtr, int size, bool zeroInit)
            {
                size -= sizeof(BlockHeader);
                blockPtr->Size = size;
                ++blockPtr;
                if (zeroInit)
                    NativeMemory.Clear(blockPtr, (nuint)size);
                return (byte*)blockPtr;
            }
        }
    }
}
