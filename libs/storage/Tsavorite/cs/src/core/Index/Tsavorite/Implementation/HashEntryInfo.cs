// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    using static LogAddress;
    using static Utility;

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
            tag = HashBucketEntry.GetTag(hash);
        }

        /// <summary>
        /// The original address of this hash entry (at the time of FindTag, etc.)
        /// </summary>
        internal readonly long Address => entry.Address;

        /// <summary>
        /// The current address of this hash entry (which may have been updated (via CAS) in the bucket after FindTag, etc.)
        /// </summary>
        internal readonly long CurrentAddress
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return new HashBucketEntry() { word = bucket->bucket_entries[slot] }.Address; }
        }

        /// <summary>
        /// Whether the original address for this hash entry (at the time of FindTag, etc.) is a readcache address.
        /// </summary>
        internal readonly bool IsReadCache
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => entry.IsReadCache;
        }

        /// <summary>
        /// Set members to the current entry (which may have been updated (via CAS) in the bucket after FindTag, etc.)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetToCurrent() => entry.word = bucket->bucket_entries[slot];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryCAS(long newLogicalAddress)
        {
            // Insert as the first record in the hash chain.
            HashBucketEntry updatedEntry = new()
            {
                Tag = tag,
                Address = newLogicalAddress & kAddressBitMask,
                Tentative = false
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
            if (entry.word != Interlocked.CompareExchange(ref bucket->bucket_entries[slot], kInvalidAddress, entry.word))
                return false;

            // We have successfully updated the actual bucket; set the local copy of the entry to match.
            entry.word = kInvalidAddress;
            return true;
        }

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            var hashStr = GetHashString(hash);

            if (bucket == null)
                return $"hash {hashStr} <no bucket>";

            var isNotCurr = Address == CurrentAddress ? string.Empty : "*";

            return $"addr {AddressString(Address)}, currAddr {AddressString(CurrentAddress)}{isNotCurr}, hash {hashStr}, tag {tag}, slot {slot},"
                 + $" Bkt1 [index {bucketIndex}, {HashBucket.ToString(firstBucket)}]";
        }
    }
}