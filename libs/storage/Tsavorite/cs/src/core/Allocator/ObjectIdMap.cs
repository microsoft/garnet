// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
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

        // ── objectId slot bit layout ─────────────────────────────────────────────────────────────────────────────────
        // The 32-bit objectId slot at keyAddress/valueAddress holds the ObjectIdMap index in its low bits. A 128 MB page with
        // a 32-byte minimum record holds at most 2^22 records, so the index needs at most 22 bits; the top 9 bits are
        // reclaimable to carry an out-of-line component's read-size HINT on the flushed/on-disk record. The hint's meaning is
        // selected by the per-component ObjectLogFilePositionInfo.Key/ValueIsExactSize flag:
        //   flag SET   -> the 9 bits are the EXACT byte length (0..511) of the out-of-line component; NO leading ChunkHeader.
        //   flag CLEAR -> for a VALUE, the 9 bits are a 4 KB-page count whose max value is the discovery sentinel; for a KEY with
        //                 KeyHasExtendedSizeHint set, they are the low 9 bits of an exact page count whose high 10 bits are in raw RDH
        //                 KeyLength. Earlier keys without that flag use the value-style page-count/sentinel interpretation.
        //                 A leading ChunkHeader carries the exact logical payload length.
        // Either way the hint lets IO for Read/RMW and recovery size the initial object-log read without an RDH length field
        // (those are reserved for hybrid values). The stamp is applied to the disk image (and, on the no-copy live-page flush,
        // to the live slot), so all in-memory reads of the slot as an index MUST go through GetIndex to mask off the hint bits.

        /// <summary>Number of low bits of the objectId slot used for the ObjectIdMap index (23 bits &gt;&gt; the 22 bits a max-size page needs).</summary>
        internal const int ObjectIdIndexBits = 23;

        /// <summary>Mask selecting the ObjectIdMap index bits of an objectId slot value.</summary>
        internal const int ObjectIdIndexMask = (1 << ObjectIdIndexBits) - 1;      // 0x7FFFFF

        /// <summary>Bit position of the out-of-line read-size hint stamped into the top of an objectId slot.</summary>
        internal const int ObjectIdSizeHintShift = ObjectIdIndexBits;             // 23

        /// <summary>Number of high bits of an objectId slot used for the out-of-line read-size hint.</summary>
        internal const int ObjectIdSizeHintBits = (sizeof(int) * 8) - ObjectIdIndexBits;  // 9

        /// <summary>Mask (right-aligned) of the out-of-line read-size hint stored in the top of an objectId slot.</summary>
        internal const int ObjectIdSizeHintMask = (1 << ObjectIdSizeHintBits) - 1;  // 0x1FF

        /// <summary>Largest value the 9-bit objectId read-size hint can hold. When the record's exact-size flag is SET this is the
        /// largest out-of-line byte length encodable as an exact size (one below the 512-byte sector size, so an exact-size component
        /// never needs a leading ChunkHeader). For a non-exact value or earlier-format key this is the page-count sentinel; for a non-exact
        /// key with <see cref="ObjectLogFilePositionInfo.kKeyHasExtendedSizeHintMask"/> it is an ordinary low-9-bit page-count value.</summary>
        internal const int MaxObjectIdSizeHint = ObjectIdSizeHintMask;            // 511

        /// <summary>Extract the ObjectIdMap index from a (possibly size-hint-stamped) objectId slot value; passes <see cref="InvalidObjectId"/> through unchanged.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetIndex(int slot) => slot == InvalidObjectId ? InvalidObjectId : (slot & ObjectIdIndexMask);

        /// <summary>Extract the out-of-line read-size hint from the top bits of an objectId slot. Its meaning depends on the record's
        /// <see cref="ObjectLogFilePositionInfo.kKeyIsExactSizeMask"/> / <see cref="ObjectLogFilePositionInfo.kValueIsExactSizeMask"/> flag:
        /// flag set -&gt; exact byte length; flag clear -&gt; page count/sentinel, or the low bits of a key page count when
        /// <see cref="ObjectLogFilePositionInfo.kKeyHasExtendedSizeHintMask"/> is set.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetSizeHint(int slot) => (slot >> ObjectIdSizeHintShift) & ObjectIdSizeHintMask;

        /// <summary>Stamp an out-of-line read-size hint (0..<see cref="MaxObjectIdSizeHint"/>) into the top bits of an objectId slot,
        /// preserving the ObjectIdMap index in the low bits. The result is never <see cref="InvalidObjectId"/> for an in-range index
        /// (a max-size page's index never reaches the all-ones low-bit pattern that a size hint would complete to -1).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int StampSizeHint(int slot, int sizeHint)
        {
            Debug.Assert(slot != InvalidObjectId, "Cannot stamp a size hint onto an invalid objectId slot");
            Debug.Assert((uint)sizeHint <= ObjectIdSizeHintMask, $"sizeHint {sizeHint} exceeds {ObjectIdSizeHintBits}-bit max {ObjectIdSizeHintMask}");
            Debug.Assert((slot & ~ObjectIdIndexMask) == 0, $"objectId slot {slot} has bits set above the {ObjectIdIndexBits}-bit index range");
            var stamped = (slot & ObjectIdIndexMask) | (sizeHint << ObjectIdSizeHintShift);
            Debug.Assert(stamped != InvalidObjectId, $"stamped objectId slot collides with InvalidObjectId (index {slot & ObjectIdIndexMask}, sizeHint {sizeHint})");
            return stamped;
        }

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

        internal bool IsEmpty => objectArray.Count == 0;

        /// <summary>Reserve a slot and return its ID.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Allocate()
        {
            if (freeSlots.TryPop(out var objectId))
            {
                // Cache Count in a local so the assertion check and its message see the same value. (Count is monotonically non-decreasing
                // in the non-OOM path, so a freshly-returned freelist id is always < Count, but a concurrent Allocate on another thread can
                // advance Count between the two reads we would otherwise do, producing a misleading message.)
                var countSnapshot = objectArray.Count;
                Debug.Assert(objectId < countSnapshot, $"objectId {objectId} retrieved from freelist must be less than Count {countSnapshot}");
                return objectId;
            }
            return objectArray.Allocate();
        }

        /// <summary>Reserve a slot, place the Overflow into it, and return the slot's ID.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AllocateAndSet(OverflowByteArray element)
        {
            var id = Allocate();
            Set(id, element);
            return id;
        }

        /// <summary>Reserve a slot, place the Object into it, and return the slot's ID.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AllocateAndSet(IHeapObject element)
        {
            var id = Allocate();
            Set(id, element);
            return id;
        }

        /// <summary>Free a slot for reuse by another record on this page (e.g. when sending a record to the revivification freelist, on a failed CAS, on record disposal, etc.).
        /// The slot is cleared so its previous occupant (byte[] overflow or IHeapObject) becomes unreachable via the map and eligible for GC. If the application needs
        /// to run <see cref="IDisposable.Dispose"/> on an IHeapObject (e.g. to release external resources), it should do so in <see cref="IRecordTriggers.OnDispose"/>
        /// before the containing record is cleared.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free(int objectId)
        {
            objectId = GetIndex(objectId);
            if (objectId != InvalidObjectId)
            {
                objectArray.Set(objectId, default);
                freeSlots.Push(objectId);
            }
        }

        /// <summary>Returns the slot's object as an IHeapObject.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IHeapObject GetHeapObject(int objectId) => Unsafe.As<IHeapObject>(objectArray.Get(GetIndex(objectId)));

        /// <summary>Returns the slot's object as an <see cref="OverflowByteArray"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OverflowByteArray GetOverflowByteArray(int objectId) => new(Unsafe.As<byte[]>(objectArray.Get(GetIndex(objectId))));

        /// <summary>Sets the slot's object.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(int objectId, IHeapObject element) => objectArray.Set(GetIndex(objectId), element);

        /// <summary>Sets the slot's object.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(int objectId, OverflowByteArray element) => objectArray.Set(GetIndex(objectId), element.Array);

        /// <summary>Clear the array.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            objectArray?.Clear(1 << MultiLevelPageArray.PrimaryClearRetainedPageSizeBits);
            freeSlots.Clear(1 << MultiLevelPageArray.FreeListClearRetainedPageSizeBits);
        }

        /// <inheritdoc/>
        public override string ToString() => $"objectArray: {(objectArray is not null ? objectArray.ToString() : "<null>")}; freeSlots: {(freeSlots is not null ? freeSlots.ToString() : "<null>")}";
    }
}