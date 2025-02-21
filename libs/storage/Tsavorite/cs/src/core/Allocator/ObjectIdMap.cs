// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        // For this class, the "page" is a TValue.
        internal MultiLevelPageArray<TValue> objectArray;

        internal SimpleConcurrentStack<int> freeList;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectIdMap()
        {
            // entriesPerPage comes from ObjectAllocator's minimum pagesize / expected record size so is the maximum possible number of records.
            // Records may be larger due to key size but we have limits on that so it is unlikely we will waste very much of this allocation.
            objectArray = new();
            freeList = new();
        }

        internal int Count => objectArray.Count;

        /// <summary>Reserve a slot and return its ID.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Allocate(out int objectId)
        {
            if (!freeList.TryPop(out objectId))
                objectId = objectArray.Allocate();
        }

        /// <summary>Free a slot for reuse by another record on this page (e.g. when sending a record to the revivification freelist, or on a failed CAS, etc.).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free(int objectId)
        {
            Set(objectId, default);
            freeList.Push(objectId);
        }

        /// <summary>Returns the slot's object.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal TValue Get(int objectId) => objectArray.Get(objectId);

        /// <summary>Returns the slot's object.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(int objectId, TValue element) => objectArray.Set(objectId, element);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear() => objectArray?.Clear();    // TODO reduce allocated chapter count also?
            
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearAt(int objectId, Action<TValue> disposer)
        {
            var element = Get(objectId);
            disposer(element);
            Set(objectId, default);
        }

        /// <inheritdoc/>
        public override string ToString() => $"tail: {(objectArray is not null ? objectArray.tail.ToString() : "<null>")}";
    }
}
