// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Garnet.server
{
    /// <summary>
    /// Header for a chunk of an object in the append-only file (AOF).
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = TotalSize)]
    public struct AofChunkHeader
    {
        public const int TotalSize = (3 * sizeof(uint)) + sizeof(ulong) + sizeof(long);

        /// <summary>Full length of the record's (inline/overflow span) key. The key length is always known up front at
        /// WriteLog* time, so the reader pre-allocates one byte[] of this size and copies the key chunks into it.</summary>
        [FieldOffset(0)]
        internal uint overflowKeyLength;

        /// <summary>Full length of the record's (inline/overflow span) value. Known up front for span values, so the reader
        /// pre-allocates one byte[] of this size. Left 0 for streamed object values (their length is not known up front); the
        /// reader accumulates those instead, keyed off the op type.</summary>
        [FieldOffset(sizeof(uint))]
        internal uint overflowValueLength;

        /// <summary>Full length of the record's serialized input. Known up front, so the reader pre-allocates one byte[] of this
        /// size. 0 when the op carries no input.</summary>
        [FieldOffset(2 * sizeof(uint))]
        internal uint inputLength;

        /// <summary>The identifier of the logical record being reconstructed; the logicalAddress of its first chunk, set
        /// identically on every chunk so the reader can group them. This is the only field patched per-chunk at write time.</summary>
        [FieldOffset(ObjectIdOffset)]
        internal ulong objectId;

        /// <summary>The 64-bit hash of the logical record's key (<c>GarnetLog.HASH</c>), set identically on every chunk. Used
        /// during parallel/sharded replay to route all of a record's chunks to the same replay task (they cannot expose the key
        /// directly, as it is itself spread across the chunk data).</summary>
        [FieldOffset(ObjectIdOffset + sizeof(ulong))]
        internal long keyHash;

        /// <summary>Byte offset of <see cref="objectId"/> within this struct.</summary>
        internal const int ObjectIdOffset = 3 * sizeof(uint);
    }
}