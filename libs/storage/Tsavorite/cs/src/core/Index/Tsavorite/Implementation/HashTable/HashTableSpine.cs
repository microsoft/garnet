// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
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
