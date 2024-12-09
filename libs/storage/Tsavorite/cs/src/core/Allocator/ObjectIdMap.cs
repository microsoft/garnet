// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    public unsafe class ObjectIdMap
    {
        internal IHeapObject[] objectVector;
        int tail = 0;

        internal ObjectIdMap(int entriesPerPage, ILogger logger = null)
        {
            // entriesPerPage comes from ObjectAllocator's minimum pagesize / expected record size so is the maximum possible number of records.
            // Records may be larger due to key size but we have limits on that so it is unlikely we will waste very much of this allocation.
            objectVector = new IHeapObject[entriesPerPage];
        }

        // We will never return a negative index from Allocate
        public const int InvalidObjectId = -1;

        public const int ObjectIdSize = sizeof(int);

        // Reserve a slot and return its ID.
        public int Allocate()
        {
            Debug.Assert(tail < objectVector.Length - 1, "ObjectgIdMap overflow detected");
            return Interlocked.Increment(ref tail) - 1;
        }

        // Returns a reference to the slot's object.
        public ref IHeapObject GetRef(int objectId)
        {
            Debug.Assert(objectId > 0 && objectId < tail, "Invalid objectId");
            return ref objectVector[objectId];
        }
        public void Clear() => Array.Clear(objectVector, 0, tail);
    }
}
