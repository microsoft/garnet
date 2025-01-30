// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal unsafe partial class OverflowAllocator
    {
        /// <summary>
        /// A header for an individual oversize page (a single allocation)
        /// </summary>
        struct OversizePageHeader
        {
            /// <summary>The usual block header. Must be the first field, so casting between OversizePageHeader and BlockHeader works.</summary>
            internal BlockHeader blockHeader;

            /// <summary>The slot in the page vector. Used in the freelist.</summary>
            internal int slot;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static int PromoteSize(int size) => BlockHeader.PromoteSize(size) + sizeof(int);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal static byte* Initialize(OversizePageHeader* pageHeader, int size, bool zeroInit, int pageSlot)
            {
                BlockHeader.Initialize(&pageHeader->blockHeader, size, zeroInit);
                pageHeader->slot = pageSlot;
                return (byte*)(pageHeader + 1);
            }
        }
    }
}
