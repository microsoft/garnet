// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;

namespace Tsavorite.core
{
    public unsafe class ObjectIdMap
    {
        internal IHeapObject[] objectVector;
        int tail = 0;

        internal ObjectIdMap(int recordsPerPage)
        {
            // entriesPerPage comes from ObjectAllocator's minimum pagesize / expected record size so is the maximum possible number of records.
            // Records may be larger due to key size but we have limits on that so it is unlikely we will waste very much of this allocation.
            objectVector = new IHeapObject[recordsPerPage];
        }

        // We will never return a negative index from Allocate
        public const int InvalidObjectId = -1;

        public const int ObjectIdSize = sizeof(int);

        // Reserve a slot and return its ID.
        public bool Allocate(out int objectId)
        {
            if (tail >= objectVector.Length)
            { 
                Debug.Fail("ObjectIdMap overflow detected");
                objectId = 0;
                return false;
            }
            objectId = Interlocked.Increment(ref tail) - 1;
            return true;
        }

        // Returns a reference to the slot's object.
        internal ref IHeapObject GetRef(int objectId)
        {
            Debug.Assert(objectId > 0 && objectId < tail, "Invalid objectId");
            return ref objectVector[objectId];
        }
        public void Clear() => Array.Clear(objectVector, 0, tail);
    }
}
