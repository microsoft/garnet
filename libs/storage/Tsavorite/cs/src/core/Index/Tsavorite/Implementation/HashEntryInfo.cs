// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>Hash table entry information for a key</summary>
    public unsafe struct HashEntryInfo
    {
        /// <summary>The first bucket in this chain for this hash bucket</summary>
        internal HashBucket* firstBucket;

        /// <summary>The hash bucket for this key (may be an overflow bucket)</summary>
        internal HashBucket* bucket;

        /// <summary>The hash bucket entry slot for this key</summary>
        internal int slot;

        /// <summary>The hash bucket entry for this key</summary>
        internal HashBucketEntry entry;

        /// <summary>The hash code for this key</summary>
        internal readonly long hash;

        /// <summary>The hash tag for this key</summary>
        internal readonly ushort tag;

        internal long bucketIndex;

        enum TransientLockState
        {
            None = 0,
            TransientSLock,
            TransientXLock
        }

        private TransientLockState transientLockState;

        /// <summary>The id of the Tsavorite partition to search for this key in.</summary>
        public readonly ushort partitionId;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal HashEntryInfo(long hash, ushort partitionId)
        {
            firstBucket = default;
            bucket = default;
            slot = default;
            entry = default;
            this.hash = hash;
            this.partitionId = partitionId;
            tag = (ushort)((ulong)this.hash >> HashBucketEntry.kHashTagShift);
        }

        internal void Reset() => Reset(partitionId);

        internal void Reset(ushort partitionId)
        {
            var lockState = transientLockState;
            this = new(hash, partitionId);
            transientLockState = lockState;
        }

        /// <summary>
        /// The original address of this hash entry (at the time of FindTag, etc.)
        /// </summary>
        internal readonly long Address => entry.Address;
        internal readonly long AbsoluteAddress => Utility.AbsoluteAddress(Address);

        /// <summary>
        /// The current address of this hash entry (which may have been updated (via CAS) in the bucket after FindTag, etc.)
        /// </summary>
        internal readonly long CurrentAddress
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return new HashBucketEntry() { word = bucket->bucket_entries[slot] }.Address; }
        }

        internal readonly long AbsoluteCurrentAddress => Utility.AbsoluteAddress(CurrentAddress);

        /// <summary>
        /// Return whether the <see cref="HashBucketEntry"/> has been updated
        /// </summary>
        internal readonly bool IsCurrent => CurrentAddress == Address;

        /// <summary>
        /// Whether the original address for this hash entry (at the time of FindTag, etc.) is a readcache address.
        /// </summary>
        internal readonly bool IsReadCache => entry.ReadCache;

        /// <summary>
        /// Whether the current address for this hash entry (possibly modified after FindTag, etc.) is a readcache address.
        /// </summary>
        internal readonly bool IsCurrentReadCache => IsReadCache(CurrentAddress);

        /// <summary>
        /// Set members to the current entry (which may have been updated (via CAS) in the bucket after FindTag, etc.)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetToCurrent() => entry = new() { word = bucket->bucket_entries[slot] };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryCAS(long newLogicalAddress, ushort partition)
        {
            // Insert as the first record in the hash chain.
            HashBucketEntry updatedEntry = new()
            {
                Tag = tag,
                PartitionId = partition,
                Address = newLogicalAddress & HashBucketEntry.kAddressMask,
                Tentative = false
                // .ReadCache is included in newLogicalAddress
            };

            if (entry.word == Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, entry.word))
            {
                entry.word = updatedEntry.word;
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryElide()
        {
            if (entry.word != Interlocked.CompareExchange(ref bucket->bucket_entries[slot], 0L, entry.word))
                return false;
            entry.word = 0L;
            return true;
        }

        /// <summary>
        /// Set (and cleared) by caller to indicate whether we have a LockTable-based Transient Shared lock (does not include Manual locks; this is per-operation only).
        /// </summary>
        internal readonly bool HasTransientSLock => transientLockState == TransientLockState.TransientSLock;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasTransientSLock()
        {
            Debug.Assert(transientLockState == TransientLockState.None, $"Cannot set TransientSLock while holding {TransientLockStateString()}");
            transientLockState = TransientLockState.TransientSLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasTransientSLock() => transientLockState = TransientLockState.None;

        /// <summary>
        /// Set (and cleared) by caller to indicate whether we have a LockTable-based Transient Exclusive lock (does not include Manual locks; this is per-operation only).
        /// </summary>
        internal readonly bool HasTransientXLock => transientLockState == TransientLockState.TransientXLock;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHasTransientXLock()
        {
            Debug.Assert(transientLockState == TransientLockState.None, $"Cannot set TransientXLock while holding {TransientLockStateString()}");
            transientLockState = TransientLockState.TransientXLock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearHasTransientXLock() => transientLockState = TransientLockState.None;

        /// <summary>
        /// Indicates whether we have any type of non-Manual lock.
        /// </summary>
        internal readonly bool HasTransientLock => transientLockState != TransientLockState.None;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal readonly string TransientLockStateString() => transientLockState.ToString();

        public override string ToString()
        {
            var hashStr = GetHashString(hash);

            if (bucket == null)
                return $"hash {hashStr} <no bucket>";

            var isRC = "(rc)";
            var addrRC = IsReadCache ? isRC : string.Empty;
            var currAddrRC = IsCurrentReadCache ? isRC : string.Empty;
            var isNotCurr = Address == CurrentAddress ? string.Empty : "*";

            var result = $"addr {AbsoluteAddress}{addrRC}, currAddr {AbsoluteCurrentAddress}{currAddrRC}{isNotCurr}, hash {hashStr}, tag {tag}, slot {slot}, partId {partitionId}, transientLocks {TransientLockStateString()},";
            result += $" Bkt1 [index {bucketIndex}, {HashBucket.ToString(firstBucket)}]";
            return result;
        }
    }
}