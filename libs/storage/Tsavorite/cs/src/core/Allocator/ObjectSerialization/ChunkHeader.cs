// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    public struct ChunkHeader
    {
        public const int TotalSize = sizeof(uint) * 2;

        /// <summary>The length of the current chunk if an object chunk; else the length of a greater-than-sentinel
        /// or padded Overflow. For object chunks, it may have <see cref="ChunkedRecordConstants.ContinuationFlag"/>
        /// to indicate another chunk follows; if not, this is the last chunk.</summary>
        [FieldOffset(0)]
        internal uint currentLength;

        /// <summary>For Objects, the length of the following chunk; 0 if we have no further chunks. Unioned with <see cref="alignmentPadding"/>.</summary>
        [FieldOffset(sizeof(uint))]
        internal uint nextLength;

        /// <summary>For Overflow, the padding for alignment if it is a DMA. Unioned with <see cref="nextLength"/>.</summary>
        [FieldOffset(sizeof(uint))]
        internal uint alignmentPadding;
    }
}