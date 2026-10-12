// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    /// <summary>Framing header for an out-of-line component's object-log bytes.</summary>
    /// <remarks>
    /// Object chunk headers are written at an 8-byte-aligned object-log offset, and the header is itself 8 bytes. Because every
    /// container the bytes pass through -- the 4 KB flush page, the 512-byte sector, and the write/read buffer -- has a size that is a
    /// multiple of 8, an 8-aligned header always lies wholly within one of them and can never straddle a boundary.
    /// <para>That matters most to the writer: <c>currentLength</c> is not known until the chunk's data has been produced, so the header
    /// is written as a placeholder and <b>back-filled</b> afterwards. Back-filling requires the header to still be addressable as eight
    /// contiguous bytes in a single buffer; a header split across two buffers could have its first half already flushed and therefore no
    /// longer patchable. The same property lets the reader consume a header as a unit.</para>
    /// <para>The alignment is absolute within the object log, not relative to the record, which is why a record's start offset modulo 8
    /// is carried through the read path and why a verbatim snapshot copy pads to preserve the source's modulo-8 offset.</para>
    /// </remarks>
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