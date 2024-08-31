// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    internal unsafe struct InternalHashTable
    {
        public long size;       // Number of buckets
        public long size_mask;
        public int size_bits;
        public HashBucket[] tableRaw;
        public HashBucket* tableAligned;
    }

    // The spine - basic bucket array - of the hash table.
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    internal unsafe struct HashTableSpine
    {
        internal const int Size = Constants.IntPtrSize + ResizeInfo.Size;

        // An array of size two, that contains the old and new versions of the hash-table
        [FieldOffset(0)]
        internal InternalHashTable[] state = new InternalHashTable[2];

        [FieldOffset(Constants.IntPtrSize)]
        internal ResizeInfo resizeInfo;

        public HashTableSpine() { }
    }
}
