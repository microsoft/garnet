// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal struct OverflowBucketLockTable<TKey, TValue> : ILockTable<TKey>
    {
        private readonly TsavoriteKV<TKey, TValue> store;

        internal readonly long NumBuckets => IsEnabled ? store.state[store.resizeInfo.version].size_mask + 1 : 0;

        internal readonly bool IsEnabled => store is not null;

        internal OverflowBucketLockTable(TsavoriteKV<TKey, TValue> f) => store = f;

        [Conditional("DEBUG")]
        private readonly void AssertLockAllowed() => Debug.Assert(IsEnabled, $"Attempt to do Manual locking when not using {nameof(ConcurrencyControlMode)}.{ConcurrencyControlMode.LockTable}");

        [Conditional("DEBUG")]
        private readonly void AssertUnlockAllowed() => Debug.Assert(IsEnabled, $"Attempt to do Manual unlocking when not using {nameof(ConcurrencyControlMode)}.{ConcurrencyControlMode.LockTable}");

        [Conditional("DEBUG")]
        private readonly void AssertQueryAllowed() => Debug.Assert(IsEnabled, $"Attempt to do Manual locking query when not using {nameof(ConcurrencyControlMode)}.{ConcurrencyControlMode.LockTable}");

        internal readonly long GetSize() => store.state[store.resizeInfo.version].size_mask;

        public readonly bool NeedKeyHash => IsEnabled;

        static OverflowBucketLockTable()
        {
            Debug.Assert(LockType.Exclusive < LockType.Shared, "LockType.Exclusive must be < LockType.Shared, or KeyHashComparer must be changed accordingly");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetBucketIndex(long keyHash, long size_mask)
            => keyHash & size_mask;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static long GetBucketIndex(long keyHash, TsavoriteKV<TKey, TValue> store)
            => GetBucketIndex(keyHash, store.state[store.resizeInfo.version].size_mask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetBucketIndex(long keyHash)
            => GetBucketIndex(keyHash, store.state[store.resizeInfo.version].size_mask);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe HashBucket* GetBucket(long keyHash)
            => store.state[store.resizeInfo.version].tableAligned + GetBucketIndex(keyHash);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockManual(ref TKey key, ref HashEntryInfo hei, LockType lockType)
            => TryLockManual(hei.firstBucket, lockType);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockManual(long keyHash, LockType lockType)
            => TryLockManual(GetBucket(keyHash), lockType);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool TryLockManual(HashBucket* bucket, LockType lockType)
        {
            AssertLockAllowed();
            return lockType switch
            {
                LockType.Shared => HashBucket.TryAcquireSharedLatch(bucket),
                LockType.Exclusive => HashBucket.TryAcquireExclusiveLatch(bucket),
                _ => throw new TsavoriteException("Attempt to lock with unknown LockType")
            };
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryPromoteLockManual(long keyHash)
        {
            AssertLockAllowed();
            return HashBucket.TryPromoteLatch(GetBucket(keyHash));
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockTransient(ref TKey key, ref HashEntryInfo hei, LockType lockType)
            => lockType == LockType.Shared ? TryLockTransientShared(ref key, ref hei) : TryLockTransientExclusive(ref key, ref hei);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockTransientShared(ref TKey key, ref HashEntryInfo hei)
        {
            AssertLockAllowed();
            return HashBucket.TryAcquireSharedLatch(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryLockTransientExclusive(ref TKey key, ref HashEntryInfo hei)
        {
            AssertLockAllowed();
            return HashBucket.TryAcquireExclusiveLatch(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Unlock(ref TKey key, ref HashEntryInfo hei, LockType lockType)
        {
            AssertUnlockAllowed();
            if (lockType == LockType.Shared)
                UnlockShared(ref key, ref hei);
            else
            {
                Debug.Assert(lockType == LockType.Exclusive, "Attempt to unlock with unknown LockType");
                UnlockExclusive(ref key, ref hei);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Unlock(long keyHash, LockType lockType)
        {
            AssertUnlockAllowed();
            HashBucket* bucket = GetBucket(keyHash);
            if (lockType == LockType.Shared)
                HashBucket.ReleaseSharedLatch(bucket);
            else
            {
                Debug.Assert(lockType == LockType.Exclusive, "Attempt to unlock with unknown LockType");
                HashBucket.ReleaseExclusiveLatch(bucket);
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockShared(ref TKey key, ref HashEntryInfo hei)
        {
            AssertUnlockAllowed();
            HashBucket.ReleaseSharedLatch(ref hei);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void UnlockExclusive(ref TKey key, ref HashEntryInfo hei)
        {
            AssertUnlockAllowed();
            HashBucket.ReleaseExclusiveLatch(ref hei);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLockedShared(ref TKey key, ref HashEntryInfo hei)
        {
            AssertQueryAllowed();
            return HashBucket.NumLatchedShared(hei.firstBucket) > 0;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLockedExclusive(ref TKey key, ref HashEntryInfo hei)
        {
            AssertQueryAllowed();
            return HashBucket.IsLatchedExclusive(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool IsLocked(ref TKey key, ref HashEntryInfo hei)
        {
            AssertQueryAllowed();
            return HashBucket.IsLatched(hei.firstBucket);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe LockState GetLockState(ref TKey key, ref HashEntryInfo hei)
        {
            AssertQueryAllowed();
            return new()
            {
                IsFound = true, // Always true for OverflowBucketLockTable
                NumLockedShared = HashBucket.NumLatchedShared(hei.firstBucket),
                IsLockedExclusive = HashBucket.IsLatchedExclusive(hei.firstBucket)
            };
        }

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
            => KeyHashComparer(key1, key2, store.state[store.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal int CompareKeyHashes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2)
            where TLockableKey : ILockableKey
            => KeyHashComparer(key1, key2, store.state[store.resizeInfo.version].size_mask);

        /// <inheritdoc/>
        internal void SortKeyHashes<TLockableKey>(TLockableKey[] keys)
            where TLockableKey : ILockableKey
            => Array.Sort(keys, new KeyComparer<TLockableKey>(store.state[store.resizeInfo.version].size_mask));

        /// <inheritdoc/>
        internal void SortKeyHashes<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey
            => Array.Sort(keys, start, count, new KeyComparer<TLockableKey>(store.state[store.resizeInfo.version].size_mask));

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