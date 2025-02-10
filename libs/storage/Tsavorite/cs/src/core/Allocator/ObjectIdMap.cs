// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Constants for <see cref="ObjectIdMap{TValue}"/>
    /// </summary>
    public struct ObjectIdMap
    {
        /// <summary>We will never return a negative index from Allocate</summary>
        public const int InvalidObjectId = -1;

        /// <summary>Size of the object Id</summary>
        public const int ObjectIdSize = sizeof(int);
    }

    /// <summary>
    /// Maps the ObjectId in the ObjectAllocator's Value field to the actual object in the object vector
    /// </summary>
    public unsafe class ObjectIdMap<TValue>
    {
        internal TValue[] objectVector;
        TailAndLatch tailAndLatch;
        SimpleConcurrentStack<int> freeList;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectIdMap(int recordsPerPage)
        {
            // entriesPerPage comes from ObjectAllocator's minimum pagesize / expected record size so is the maximum possible number of records.
            // Records may be larger due to key size but we have limits on that so it is unlikely we will waste very much of this allocation.
            objectVector = new TValue[recordsPerPage];
            tailAndLatch = new();
            freeList = new();
        }

        /// <summary>Reserve a slot and return its ID.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Allocate(out int objectId)
        {
            if (freeList.TryPop(out objectId))
                return true;

            tailAndLatch.AcquireForPush(ref objectVector);
            objectId = tailAndLatch.Tail;
            _ = tailAndLatch.ClearLatch();
            return true;
        }

        /// <summary>Free a slot for reuse by another record on this page (e.g. when sending a record to the revivification freelist, or on a failed CAS, etc.).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free(int objectId) => freeList.TryPush(objectId);

        /// <summary>Returns a reference to the slot's object.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref TValue GetRef(int objectId)
        {
            Debug.Assert(objectId >= 0 && objectId <= tailAndLatch.Tail, "Invalid objectId");
            return ref objectVector[objectId];
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear() => Array.Clear(objectVector, 0, tailAndLatch.Tail);

        /// <inheritdoc/>
        public override string ToString() => $"{tailAndLatch.Tail} | capacity {objectVector.Length}";
    }
}
