// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Maps the ObjectId in the ObjectAllocator's Value field to the actual object in the object multi-level array.
    /// This may be either a byte[] Span-overflow allocation, or an IHeapObject.
    /// </summary>
    public class ObjectIdMap
    {
        /// <summary>We will never return a negative index from Allocate</summary>
        public const int InvalidObjectId = -1;

        /// <summary>Size of the object Id</summary>
        public const int ObjectIdSize = sizeof(int);

        // For this class, the "page" is an object.
        internal MultiLevelPageArray<object> objectArray;

        internal SimpleConcurrentStack<int> freeSlots;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ObjectIdMap()
        {
            // entriesPerPage comes from ObjectAllocator's minimum pagesize / expected record size so is the maximum possible number of records.
            // Records may be larger due to key size but we have limits on that so it is unlikely we will waste very much of this allocation.
            objectArray = new();
            freeSlots = new();
        }

        internal int Count => objectArray.Count;

        /// <summary>Reserve a slot and return its ID.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Allocate()
            => freeSlots.TryPop(out var objectId) ? objectId : objectArray.Allocate();

        /// <summary>Free a slot for reuse by another record on this page (e.g. when sending a record to the revivification freelist, or on a failed CAS, etc.).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free(int objectId)
        {
            if (objectId != InvalidObjectId)
            {
                objectArray.Set(objectId, default);
                freeSlots.Push(objectId);
            }
        }

        /// <summary>Clear a specific slot of the array.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free(int objectId, Action<IHeapObject> disposer)
        {
            if (objectId != InvalidObjectId)
            {
                if (disposer is not null)
                {
                    var element = objectArray.Get(objectId);
                    disposer(Unsafe.As<object, IHeapObject>(ref element));
                }
                objectArray.Set(objectId, default);
                freeSlots.Push(objectId);
            }
        }

        /// <summary>Returns the slot's object as an IHeapObject.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IHeapObject GetHeapObject(int objectId) => Unsafe.As<IHeapObject>(objectArray.Get(objectId));

        /// <summary>Returns the slot's object as an <see cref="OverflowByteArray"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OverflowByteArray GetOverflowByteArray(int objectId) => new(Unsafe.As<byte[]>(objectArray.Get(objectId)));

        /// <summary>Sets the slot's object.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(int objectId, IHeapObject element) => objectArray.Set(objectId, element);

        /// <summary>Sets the slot's object.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(int objectId, OverflowByteArray element) => objectArray.Set(objectId, element.Array);

        /// <summary>Clear the array.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear() => objectArray?.Clear();    // TODO reduce allocated chapter count also?

        /// <inheritdoc/>
        public override string ToString() => $"tail: {(objectArray is not null ? objectArray.tail.ToString() : "<null>")}";
    }
}