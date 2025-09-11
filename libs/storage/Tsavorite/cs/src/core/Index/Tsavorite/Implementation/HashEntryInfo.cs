// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
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

        /// <summary>
        /// Update this entry's Address to point on-disk if it matches <paramref name="targetAddress"/>
        /// </summary>
        /// <param name="targetAddress">The address to change</param>
        /// <param name="diskAddress">The on-disk address to replace PreviousAddress</param>
        /// <return>True if the address was updated in the <see cref="HashBucketEntry"/>, else false and the caller will traverse the chain
        ///     to find <paramref name="targetAddress"/>.</return>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool UpdateToOnDiskAddress(long targetAddress, long diskAddress)
        {
            while (true)
            {
                HashBucketEntry currentEntry = new() { word = bucket->bucket_entries[slot] };

                // If the address is not in the HashBucketEntry, do some sanity checks then return false.
                if (currentEntry.Address != targetAddress)
                {
                    if (diskAddress == currentEntry.Address)
                    {
                        Debug.Fail("Unexpected re-setting of same address to OnDisk; should only be done once");
                        return true;
                    }
                    Debug.Assert(currentEntry.Address >= targetAddress, "Unexpected miss of targetAddress; should have found it while looking to update");
                    return false;
                }

                // Try to update the HashBucketEntry
                HashBucketEntry updatedEntry = new(tag, diskAddress);
                if (currentEntry.word == Interlocked.CompareExchange(ref bucket->bucket_entries[slot], updatedEntry.word, currentEntry.word))
                {
                    entry.word = updatedEntry.word;
                    return true;
                }

                // CAS failed which likely means a conflict with insertion or elision; yield and retry
                _ = Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryCAS(long newLogicalAddress)
        {
            // Insert as the first record in the hash chain.
            HashBucketEntry updatedEntry = new(tag, newLogicalAddress & kAddressBitMask);

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

        public override string ToString()
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