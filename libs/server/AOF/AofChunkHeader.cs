// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Header for a chunk of an object in the append-only file (AOF).
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    public struct AofChunkHeader
    {
        public const int TotalSize = ChunkHeader.TotalSize + sizeof(uint);

        /// <summary>The basic chunk information</summary>
        [FieldOffset(0)]
        internal ChunkHeader header;

        /// <summary>The identifier of the object being reconstructed; this is the logicalAddress of the first chunk encountered
        /// during AOF iterator traversal.</summary>
        [FieldOffset(ChunkHeader.TotalSize)]
        internal uint objectId;
    }
}
