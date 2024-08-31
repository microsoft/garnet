// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tsavorite.core
{
    [StructLayout(LayoutKind.Explicit)]
    internal struct HashBucketLockTable : ILockTable 
    {
        // Unioned field in 'struct HashTable'
        [FieldOffset(0)]
        HashTableSpine spine;

        // No additional fields are allowed; this would mess up the union in 'struct HashTable'.

        internal readonly long NumBuckets => spine.state[spine.resizeInfo.version].size_mask + 1;

        public HashBucketLockTable() => throw new TsavoriteException("HashBucketLockTable is part of a union in TsavoriteKernel and must not be instantiated directly");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetBucketIndex(long keyHash, long size_mask)
            => keyHash & size_mask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetBucketIndex(long keyHash)
            => GetBucketIndex(keyHash, spine.state[spine.resizeInfo.version].size_mask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe HashBucket* GetBucket(long keyHash)
            => spine.state[spine.resizeInfo.version].tableAligned + GetBucketIndex(keyHash);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockShared(ref HashEntryInfo hei)
            => HashBucket.TryAcquireSharedLatch(hei.firstBucket);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockExclusive(ref HashEntryInfo hei)
            => HashBucket.TryAcquireExclusiveLatch(hei.firstBucket);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryPromoteLock(ref HashEntryInfo hei)
            => HashBucket.TryPromoteLatch(hei.firstBucket);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockShared(ref HashEntryInfo hei)
            => HashBucket.ReleaseSharedLatch(ref hei);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockExclusive(ref HashEntryInfo hei)
            => HashBucket.ReleaseExclusiveLatch(ref hei);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLockedShared(ref HashEntryInfo hei)
            => HashBucket.NumLatchedShared(hei.firstBucket) > 0;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLockedExclusive(ref HashEntryInfo hei)
            => HashBucket.IsLatchedExclusive(hei.firstBucket);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLocked(ref HashEntryInfo hei)
            => HashBucket.IsLatched(hei.firstBucket);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe LockState GetLockState(ref HashEntryInfo hei)
            => new()
            {
                IsFound = true, // Always true for OverflowBucketLockTable
                NumLockedShared = HashBucket.NumLatchedShared(hei.firstBucket),
                IsLockedExclusive = HashBucket.IsLatchedExclusive(hei.firstBucket)
            };

        private static int KeyHashComparer<TLockableKey>(TLockableKey key1, TLockableKey key2, long size_mask)
            where TLockableKey : ILockableKey
        {
            var idx1 = GetBucketIndex(key1.KeyHash, size_mask);
            var idx2 = GetBucketIndex(key2.KeyHash, size_mask);
            return (idx1 != idx2) ? idx1.CompareTo(idx2) : ((byte)key1.LockType).CompareTo((byte)key2.LockType);
        }

        /// <inheritdoc/>
        internal int CompareKeyHashes<TLockableKey>(TLockableKey key1, TLockableKey key2)
            where TLockableKey : ILockableKey
            => KeyHashComparer(key1, key2, spine.state[spine.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal int CompareKeyHashes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2)
            where TLockableKey : ILockableKey
            => KeyHashComparer(key1, key2, spine.state[spine.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal void SortKeyHashes<TLockableKey>(TLockableKey[] keys)
            where TLockableKey : ILockableKey
            => Array.Sort(keys, new KeyComparer<TLockableKey>(spine.state[spine.resizeInfo.version].size_mask));

        /// <inheritdoc/>
        internal void SortKeyHashes<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey
            => Array.Sort(keys, start, count, new KeyComparer<TLockableKey>(spine.state[spine.resizeInfo.version].size_mask));

        /// <summary>
        /// Need this struct because the Comparison{T} form of Array.Sort is not available with start and length arguments.
        /// </summary>
        struct KeyComparer<TLockableKey> : IComparer<TLockableKey>
            where TLockableKey : ILockableKey
        {
            readonly long size_mask;

            internal KeyComparer(long s) => size_mask = s;

            public int Compare(TLockableKey key1, TLockableKey key2)
            {
                // This sorts by partitionId, then calls Tsavorite to sort by lock code and then by lockType.
                var cmp = key1.PartitionId.CompareTo(key2.PartitionId);
                if (cmp != 0)
                    return cmp;
                return KeyHashComparer(key1, key2, size_mask);
            }
        }

        /// <inheritdoc/>
        public void Dispose() { }
    }
}