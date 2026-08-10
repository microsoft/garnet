// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    public struct ChunkHeader
    {
        public const int TotalSize = sizeof(uint) * 2;

        /// <summary>For overflow, the complete payload length. For an object chunk, the low bits are this chunk's data length and
        /// <see cref="ChunkedRecordConstants.ContinuationFlag"/> indicates that another header follows after the data.</summary>
        [FieldOffset(0)]
        internal uint currentLength;

        /// <summary>For overflow, the number of padding bytes between this header and the payload so a large payload can begin at the
        /// sector residue required for direct IO. Zero for object chunks and buffered overflow writes.</summary>
        [FieldOffset(sizeof(uint))]
        internal uint alignmentPadding;
    }
}