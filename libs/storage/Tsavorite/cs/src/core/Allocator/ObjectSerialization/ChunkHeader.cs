// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    public struct ChunkHeader
    {
        public const int TotalSize = (sizeof(int) * 2) + (sizeof(ushort) * 4);

        /// <summary>The length of the current chunk</summary>
        [FieldOffset(0)]
        internal uint length;

        /// <summary>The length of the following chunk; 0 if we have no further chunks</summary>
        [FieldOffset(sizeof(uint))]
        internal uint nextChunkLength;

        /// <summary>The 0-based sequence number of the current chunk</summary>
        [FieldOffset(sizeof(uint) * 2)]
        internal ushort chunkNumber;
    }
}