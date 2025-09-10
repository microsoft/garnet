// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal struct OverflowBucketLockTable<TKey, TValue, TStoreFunctions, TAllocator> : ILockTable<TKey>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        private readonly TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store;

        internal readonly long NumBuckets => store.state[store.resizeInfo.version].size_mask + 1;

        public readonly bool IsEnabled => store is not null;

        internal OverflowBucketLockTable(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store) => this.store = store;

        internal readonly long GetSize() => store.state[store.resizeInfo.version].size_mask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetBucketIndex(long keyHash, long size_mask)
            => keyHash & size_mask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetBucketIndex(long keyHash)
            => GetBucketIndex(keyHash, store.state[store.resizeInfo.version].size_mask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe HashBucket* GetBucket(long keyHash)
            => store.state[store.resizeInfo.version].tableAligned + GetBucketIndex(keyHash);

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

        private static int KeyHashComparer<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2, long size_mask)
            where TLockableKey : ILockableKey
        {
            var idx1 = GetBucketIndex(key1.KeyHash, size_mask);
            var idx2 = GetBucketIndex(key2.KeyHash, size_mask);
            return (idx1 != idx2) ? idx1.CompareTo(idx2) : ((byte)key1.LockType).CompareTo((byte)key2.LockType);
        }

        /// <inheritdoc/>
        internal readonly int CompareKeyHashes<TLockableKey>(TLockableKey key1, TLockableKey key2)
            where TLockableKey : ILockableKey
            => KeyHashComparer(key1, key2, store.state[store.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal readonly int CompareKeyHashes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2)
            where TLockableKey : ILockableKey
            => KeyHashComparer(ref key1, ref key2, store.state[store.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal readonly void SortKeyHashes<TLockableKey>(Span<TLockableKey> keys)
            where TLockableKey : ILockableKey
            => keys.Sort(new KeyComparer<TLockableKey>(store.state[store.resizeInfo.version].size_mask));

        /// <summary>
        /// Need this struct because the Comparison{T} form of Array.Sort is not available with start and length arguments.
        /// </summary>
        struct KeyComparer<TLockableKey> : IComparer<TLockableKey>
            where TLockableKey : ILockableKey
        {
            readonly long size_mask;

            internal KeyComparer(long s) => size_mask = s;

            public int Compare(TLockableKey key1, TLockableKey key2) => KeyHashComparer(key1, key2, size_mask);
        }

        /// <inheritdoc/>
        public void Dispose() { }
    }
}