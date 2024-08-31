// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace Tsavorite.core
{
    /// <summary>
    /// The core data structures of the core, used for dual Tsavorite operations
    /// </summary>
    public partial class TsavoriteKernel
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareKeyHashes<TLockableKey>(TLockableKey key1, TLockableKey key2) where TLockableKey : ILockableKey => lockTable.CompareKeyHashes(key1, key2);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareKeyHashes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2) where TLockableKey : ILockableKey => lockTable.CompareKeyHashes(ref key1, ref key2);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SortKeyHashes<TLockableKey>(TLockableKey[] keys) where TLockableKey : ILockableKey => lockTable.SortKeyHashes(keys);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SortKeyHashes<TLockableKey>(TLockableKey[] keys, int start, int count) where TLockableKey : ILockableKey => lockTable.SortKeyHashes(keys, start, count);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool DoManualLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, int start, int count)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code,
            // which of course allows the session to do shared operations as well, so we take the first occurrence of each key code.
            // This is the same as DoManualTryLock but without timeout; it will keep trying until it acquires all locks or the hardcoded retry limit is reached.
            var end = start + count - 1;

            var retryCount = 0;
        Retry:
            long prevBucketIndex = -1;

            for (var keyIdx = start; keyIdx <= end; ++keyIdx)
            {
                ref var key = ref keys[keyIdx];
                var currBucketIndex = lockTable.GetBucketIndex(key.KeyHash);
                if (currBucketIndex != prevBucketIndex)
                {
                    prevBucketIndex = currBucketIndex;
                    var status = DoManualLock(ref kernelSession, key);
                    if (status == OperationStatus.SUCCESS)
                        continue;   // Success; continue to the next key.

                    // Lock failure before we've completed all keys, and we did not lock the current key. Unlock anything we've locked.
                    DoManualUnlock(ref kernelSession, keys, start, keyIdx - 1);

                    // We've released our locks so this refresh will let other threads advance and release their locks, and we will retry with a full timeout.
                    kernelSession.HandleImmediateNonPendingRetryStatus(status == OperationStatus.RETRY_LATER);
                    retryCount++;
                    if (retryCount >= KeyLockMaxRetryAttempts)
                        return false;
                    goto Retry;
                }
            }

            // We reached the end of the list, possibly after a duplicate keyhash; all locks were successful.
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool DoManualTryLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, int start, int count, TimeSpan timeout, CancellationToken cancellationToken)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code,
            // which of course allows the session to do shared operations as well, so we take the first occurrence of each key code.
            // This is the same as DoManualLock but with timeout.
            var end = start + count - 1;

            // We can't start each retry with a full timeout because we might always fail if someone is not unlocking (e.g. another thread hangs
            // somehow while holding a lock, or the current thread has issued two lock calls on two key sets and the second tries to lock one in
            // the first, and so on). So set the timeout high enough to accommodate as many retries as you want.
            var startTime = DateTime.UtcNow;

        Retry:
            long prevBucketIndex = -1;

            for (var keyIdx = start; keyIdx <= end; ++keyIdx)
            {
                ref var key = ref keys[keyIdx];
                var currBucketIndex = lockTable.GetBucketIndex(key.KeyHash);
                if (currBucketIndex != prevBucketIndex)
                {
                    prevBucketIndex = currBucketIndex;

                    OperationStatus status;
                    if (cancellationToken.IsCancellationRequested)
                        status = OperationStatus.CANCELED;
                    else
                    {
                        status = DoManualLock(ref kernelSession, key);
                        if (status == OperationStatus.SUCCESS)
                            continue;   // Success; continue to the next key.
                    }

                    // Cancellation or lock failure before we've completed all keys; we have not locked the current key. Unlock anything we've locked.
                    DoManualUnlock(ref kernelSession, keys, start, keyIdx - 1);

                    // Lock failure is the only place we check the timeout. If we've exceeded that, or if we've had a cancellation, return false.
                    if (cancellationToken.IsCancellationRequested || DateTime.UtcNow.Ticks - startTime.Ticks > timeout.Ticks)
                        return false;

                    // No cancellation and we're within the timeout. We've released our locks so this refresh will let other threads advance
                    // and release their locks, and we will retry with a full timeout.
                    kernelSession.HandleImmediateNonPendingRetryStatus(status == OperationStatus.RETRY_LATER);
                    goto Retry;
                }
            }

            // All locks were successful.
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool DoManualTryPromoteLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey key, TimeSpan timeout, CancellationToken cancellationToken)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
        {
            var startTime = DateTime.UtcNow;
            while (true)
            {
                if (InternalPromoteLock(ref key))
                {
                    ++kernelSession.ExclusiveTxnLockCount;
                    --kernelSession.SharedTxnLockCount;

                    // Success; the caller should update the ILockableKey.LockType so the unlock has the right type
                    return true;
                }

                // CancellationToken can accompany either of the other two mechanisms
                if (cancellationToken.IsCancellationRequested || DateTime.UtcNow.Ticks - startTime.Ticks > timeout.Ticks)
                    break;  // out of the retry loop

                // Lock failed, must retry
                kernelSession.HandleImmediateNonPendingRetryStatus(refresh: true);
            }

            // Failed to promote
            return false;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus DoManualLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey key)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
        {
            if (key.LockType == LockType.Shared)
            {
                if (!InternalTryLockShared(ref key))
                    return OperationStatus.RETRY_LATER;
                ++kernelSession.SharedTxnLockCount;
            }
            else
            {
                if (!InternalTryLockExclusive(ref key))
                    return OperationStatus.RETRY_LATER;
                ++kernelSession.ExclusiveTxnLockCount;
            }
            return OperationStatus.SUCCESS;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DoManualUnlock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, int start, int keyIdx)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code.
            // Unlock has to be done in the reverse order of locking, so we take the *last* occurrence of each key there, and keyIdx moves backward.
            for (; keyIdx >= start; --keyIdx)
            {
                ref var key = ref keys[keyIdx];
                if (keyIdx == start || lockTable.GetBucketIndex(key.KeyHash) != lockTable.GetBucketIndex(keys[keyIdx - 1].KeyHash))
                {
                    if (key.LockType == LockType.Shared)
                    {
                        InternalUnlockShared(ref key);
                        --kernelSession.SharedTxnLockCount;
                    }
                    else
                    {
                        InternalUnlockExclusive(ref key);
                        --kernelSession.ExclusiveTxnLockCount;
                    }
                }
            }
        }

        public void Lock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey 
            => Lock(ref kernelSession, keys, 0, keys.Length);

        public void Lock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, int start, int count)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
        {
            kernelSession.CheckTransactionIsStarted();
            Debug.Assert(kernelSession.IsEpochAcquired(), "Expected Epoch to be acquired for Lock");

            while (true)
            {
                if (!DoManualLock(ref kernelSession, keys, start, count))
                {
                    // Pulse epoch protection to give others a fair chance to progress
                    kernelSession.EndUnsafe();
                    kernelSession.BeginUnsafe();
                }
            }
        }

        public bool TryLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => TryLock(ref kernelSession, keys, 0, keys.Length, Timeout.InfiniteTimeSpan, cancellationToken: default);

        public bool TryLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, TimeSpan timeout)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => TryLock(ref kernelSession, keys, 0, keys.Length, timeout, cancellationToken: default);

        public bool TryLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, int start, int count, TimeSpan timeout)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => TryLock(ref kernelSession, keys, start, count, timeout, cancellationToken: default);

        public bool TryLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, CancellationToken cancellationToken)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => TryLock(ref kernelSession, keys, 0, keys.Length, Timeout.InfiniteTimeSpan, cancellationToken);

        public bool TryLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, int start, int count, CancellationToken cancellationToken)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => TryLock(ref kernelSession, keys, start, count, Timeout.InfiniteTimeSpan, cancellationToken);

        public bool TryLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, TimeSpan timeout, CancellationToken cancellationToken)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => TryLock(ref kernelSession, keys, 0, keys.Length, timeout, cancellationToken);

        public bool TryLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, int start, int count, TimeSpan timeout, CancellationToken cancellationToken)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
        {
            kernelSession.CheckTransactionIsStarted();
            Debug.Assert(kernelSession.IsEpochAcquired(), "Expected Epoch to be acquired for TryLock");
            return DoManualTryLock(ref kernelSession, keys, start, count, timeout, cancellationToken);
        }

        public bool TryPromoteLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey key)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => TryPromoteLock(ref kernelSession, key, Timeout.InfiniteTimeSpan, cancellationToken: default);

        public bool TryPromoteLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey key, TimeSpan timeout)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => TryPromoteLock(ref kernelSession, key, timeout, cancellationToken: default);

        public bool TryPromoteLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey key, CancellationToken cancellationToken)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => TryPromoteLock(ref kernelSession, key, Timeout.InfiniteTimeSpan, cancellationToken);

        public bool TryPromoteLock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey key, TimeSpan timeout, CancellationToken cancellationToken)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
        {
            kernelSession.CheckTransactionIsStarted();
            Debug.Assert(kernelSession.IsEpochAcquired(), "Expected Epoch to be acquired for TryPromoteLock");
            return DoManualTryPromoteLock(ref kernelSession, key, timeout, cancellationToken);
        }

        public void Unlock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
            => Unlock(ref kernelSession, keys, 0, keys.Length);

        public void Unlock<TKernelSession, TLockableKey>(ref TKernelSession kernelSession, TLockableKey[] keys, int start, int count)
            where TKernelSession : IKernelSession
            where TLockableKey : ILockableKey
        {
            kernelSession.CheckTransactionIsStarted();
            Debug.Assert(kernelSession.IsEpochAcquired(), "Expected Epoch to be acquired for Unlock");

            DoManualUnlock(ref kernelSession, keys, start, start + count - 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InternalTryLockShared<TLockableKey>(ref TLockableKey key)
            where TLockableKey : ILockableKey
        {
            Debug.Assert(Epoch.ThisInstanceProtected(), "InternalLockShared must have protected epoch");
            HashEntryInfo hei = new(key.KeyHash, key.PartitionId);
            _ = hashTable.FindTag(ref hei);
            return lockTable.TryLockShared(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InternalTryLockExclusive<TLockableKey>(ref TLockableKey key)
            where TLockableKey : ILockableKey
        {
            Debug.Assert(Epoch.ThisInstanceProtected(), "InternalLockExclusive must have protected epoch");
            HashEntryInfo hei = new(key.KeyHash, key.PartitionId);
            _ = hashTable.FindTag(ref hei);
            return lockTable.TryLockExclusive(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalUnlockShared<TLockableKey>(ref TLockableKey key)
            where TLockableKey : ILockableKey
        {
            Debug.Assert(Epoch.ThisInstanceProtected(), "InternalUnlockShared must have protected epoch");
            HashEntryInfo hei = new(key.KeyHash, key.PartitionId);
            _ = hashTable.FindTag(ref hei);
            lockTable.UnlockShared(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalUnlockExclusive<TLockableKey>(ref TLockableKey key)
            where TLockableKey : ILockableKey
        {
            Debug.Assert(Epoch.ThisInstanceProtected(), "InternalUnlockExclusive must have protected epoch");
            HashEntryInfo hei = new(key.KeyHash, key.PartitionId);
            _ = hashTable.FindTag(ref hei);
            lockTable.UnlockExclusive(ref hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InternalPromoteLock<TLockableKey>(ref TLockableKey key)
            where TLockableKey : ILockableKey
        {
            Debug.Assert(Epoch.ThisInstanceProtected(), "InternalLock must have protected epoch");
            HashEntryInfo hei = new(key.KeyHash, key.PartitionId);
            _ = hashTable.FindTag(ref hei);
            return lockTable.TryPromoteLock(ref hei);
        }
    }
}
