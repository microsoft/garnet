// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    /// <summary>Hash table entry information for a key</summary>
    internal unsafe struct HashEntryInfo
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
        internal ushort tag;

        internal long bucketIndex;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal HashEntryInfo(long hash)
        {
            firstBucket = default;
            bucket = default;
            slot = default;
            entry = default;
            this.hash = hash;
            tag = (ushort)((ulong)this.hash >> Constants.kHashTagShift);
        }

        /// <summary>
        /// The original address of this hash entry (at the time of FindTag, etc.)
        /// </summary>
        internal readonly long Address => entry.Address;
        internal readonly long AbsoluteAddress => Utility.AbsoluteAddress(Address);

        /// <summary>
        /// The current address of this hash entry (which may have been updated (via CAS) in the bucket after FindTag, etc.)
        /// </summary>
        internal readonly long CurrentAddress => new HashBucketEntry() { word = bucket->bucket_entries[slot] }.Address;
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
        internal void SetToCurrent() => entry = new HashBucketEntry() { word = bucket->bucket_entries[slot] };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryCAS(long newLogicalAddress)
        {
            // Insert as the first record in the hash chain.
            HashBucketEntry updatedEntry = new()
            {
                Tag = tag,
                Address = newLogicalAddress & Constants.kAddressMask,
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

        public override string ToString()
        {
            var hashStr = GetHashString(hash);

            if (bucket == null)
                return $"hash {hashStr} <no bucket>";

            var isRC = "(rc)";
            var addrRC = IsReadCache ? isRC : string.Empty;
            var currAddrRC = IsCurrentReadCache ? isRC : string.Empty;
            var isNotCurr = Address == CurrentAddress ? string.Empty : "*";

            var result = $"addr {AbsoluteAddress}{addrRC}, currAddr {AbsoluteCurrentAddress}{currAddrRC}{isNotCurr}, hash {hashStr}, tag {tag}, slot {slot},";
            result += $" Bkt1 [index {bucketIndex}, {HashBucket.ToString(firstBucket)}]";
            return result;
        }
    }
}