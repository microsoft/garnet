// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal struct OverflowBucketLockTable<TStoreFunctions, TAllocator> : ILockTable
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        private readonly TsavoriteKV<TStoreFunctions, TAllocator> store;

        internal readonly long NumBuckets => store.state[store.resizeInfo.version].size_mask + 1;

        public readonly bool IsEnabled => store is not null;

        internal OverflowBucketLockTable(TsavoriteKV<TStoreFunctions, TAllocator> store) => this.store = store;

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

        private static int KeyHashComparer<TTransactionalKey>(TTransactionalKey key1, TTransactionalKey key2, long size_mask)
            where TTransactionalKey : ITransactionalKey
        {
            var idx1 = GetBucketIndex(key1.KeyHash, size_mask);
            var idx2 = GetBucketIndex(key2.KeyHash, size_mask);
            return (idx1 != idx2) ? idx1.CompareTo(idx2) : ((byte)key1.LockType).CompareTo((byte)key2.LockType);
        }

        private static int KeyHashComparer<TTransactionalKey>(ref TTransactionalKey key1, ref TTransactionalKey key2, long size_mask)
            where TTransactionalKey : ITransactionalKey
        {
            var idx1 = GetBucketIndex(key1.KeyHash, size_mask);
            var idx2 = GetBucketIndex(key2.KeyHash, size_mask);
            return (idx1 != idx2) ? idx1.CompareTo(idx2) : ((byte)key1.LockType).CompareTo((byte)key2.LockType);
        }

        /// <inheritdoc/>
        internal readonly int CompareKeyHashes<TTransactionalKey>(TTransactionalKey key1, TTransactionalKey key2)
            where TTransactionalKey : ITransactionalKey
            => KeyHashComparer(key1, key2, store.state[store.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal readonly int CompareKeyHashes<TTransactionalKey>(ref TTransactionalKey key1, ref TTransactionalKey key2)
            where TTransactionalKey : ITransactionalKey
            => KeyHashComparer(ref key1, ref key2, store.state[store.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal readonly void SortKeyHashes<TTransactionalKey>(Span<TTransactionalKey> keys)
            where TTransactionalKey : ITransactionalKey
            => keys.Sort(new KeyComparer<TTransactionalKey>(store.state[store.resizeInfo.version].size_mask));

        /// <summary>
        /// Need this struct because the Comparison{T} form of Array.Sort is not available with start and length arguments.
        /// </summary>
        struct KeyComparer<TTransactionalKey> : IComparer<TTransactionalKey>
            where TTransactionalKey : ITransactionalKey
        {
            readonly long size_mask;

            internal KeyComparer(long s) => size_mask = s;

            public int Compare(TTransactionalKey key1, TTransactionalKey key2) => KeyHashComparer(key1, key2, size_mask);
        }

        /// <inheritdoc/>
        public void Dispose() { }
    }
}