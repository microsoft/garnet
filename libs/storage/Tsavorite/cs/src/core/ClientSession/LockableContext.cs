// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Tsavorite Context implementation that allows manual control of record locking and epoch management. For advanced use only.
    /// </summary>
    public readonly struct LockableContext<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> : ITsavoriteContext<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>, ILockableContext<TKey>
        where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession;
        readonly SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, LockableSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator> sessionFunctions;

        /// <inheritdoc/>
        public bool IsNull => clientSession is null;

        const int KeyLockMaxRetryAttempts = 1000;

        internal LockableContext(ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            this.clientSession = clientSession;
            sessionFunctions = new(clientSession);
        }

        #region Begin/EndLockable

        /// <inheritdoc/>
        public void BeginLockable() => clientSession.AcquireLockable(sessionFunctions);

        /// <inheritdoc/>
        public void LocksAcquired(long txnVersion) => clientSession.LocksAcquired(sessionFunctions, txnVersion);

        /// <inheritdoc/>
        public void EndLockable() => clientSession.ReleaseLockable(sessionFunctions);

        #endregion Begin/EndLockable

        #region Key Locking

        /// <inheritdoc/>
        public int CompareKeyHashes<TLockableKey>(TLockableKey key1, TLockableKey key2) where TLockableKey : ILockableKey => clientSession.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public int CompareKeyHashes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2) where TLockableKey : ILockableKey => clientSession.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortKeyHashes<TLockableKey>(Span<TLockableKey> keys) where TLockableKey : ILockableKey => clientSession.SortKeyHashes(keys);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool DoManualLock<TSessionFunctionsWrapper, TLockableKey>(TSessionFunctionsWrapper sessionFunctions, ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession,
                                                                   ReadOnlySpan<TLockableKey> keys)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code,
            // which of course allows the session to do shared operations as well, so we take the first occurrence of each key code.
            // This is the same as DoManualTryLock but without timeout; it will keep trying until it acquires all locks or the hardcoded retry limit is reached.

            var retryCount = 0;
        Retry:
            var prevBucketIndex = -1L;

            for (var keyIdx = 0; keyIdx < keys.Length; ++keyIdx)
            {
                ref readonly var key = ref keys[keyIdx];
                var currBucketIndex = clientSession.store.LockTable.GetBucketIndex(key.KeyHash);
                if (currBucketIndex != prevBucketIndex)
                {
                    prevBucketIndex = currBucketIndex;
                    OperationStatus status = DoManualLock(clientSession, key);
                    if (status == OperationStatus.SUCCESS)
                        continue;   // Success; continue to the next key.

                    // Lock failure before we've completed all keys, and we did not lock the current key. Unlock anything we've locked.
                    DoManualUnlock(clientSession, keys[..keyIdx]);

                    // We've released our locks so this refresh will let other threads advance and release their locks, and we will retry with a full timeout.
                    _ = clientSession.store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions);
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
        internal static bool DoManualTryLock<TSessionFunctionsWrapper, TLockableKey>(TSessionFunctionsWrapper sessionFunctions, ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession,
                                                                   ReadOnlySpan<TLockableKey> keys, TimeSpan timeout, CancellationToken cancellationToken)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code,
            // which of course allows the session to do shared operations as well, so we take the first occurrence of each key code.
            // This is the same as DoManualLock but with timeout.

            // We can't start each retry with a full timeout because we might always fail if someone is not unlocking (e.g. another thread hangs
            // somehow while holding a lock, or the current thread has issued two lock calls on two key sets and the second tries to lock one in
            // the first, and so on). So set the timeout high enough to accommodate as many retries as you want.
            var startTime = DateTime.UtcNow;

        Retry:
            var prevBucketIndex = -1L;

            for (var keyIdx = 0; keyIdx < keys.Length; ++keyIdx)
            {
                ref readonly var key = ref keys[keyIdx];
                var currBucketIndex = clientSession.store.LockTable.GetBucketIndex(key.KeyHash);
                if (currBucketIndex != prevBucketIndex)
                {
                    prevBucketIndex = currBucketIndex;

                    OperationStatus status;
                    if (cancellationToken.IsCancellationRequested)
                        status = OperationStatus.CANCELED;
                    else
                    {
                        status = DoManualLock(clientSession, key);
                        if (status == OperationStatus.SUCCESS)
                            continue;   // Success; continue to the next key.
                    }

                    // Cancellation or lock failure before we've completed all keys; we have not locked the current key. Unlock anything we've locked.
                    DoManualUnlock(clientSession, keys[..keyIdx]);

                    // Lock failure is the only place we check the timeout. If we've exceeded that, or if we've had a cancellation, return false.
                    if (cancellationToken.IsCancellationRequested || DateTime.UtcNow.Ticks - startTime.Ticks > timeout.Ticks)
                        return false;

                    // No cancellation and we're within the timeout. We've released our locks so this refresh will let other threads advance
                    // and release their locks, and we will retry with a full timeout.
                    _ = clientSession.store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions);
                    goto Retry;
                }
            }

            // All locks were successful.
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool DoManualTryPromoteLock<TSessionFunctionsWrapper, TLockableKey>(TSessionFunctionsWrapper sessionFunctions, ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession,
                                                                   TLockableKey key, TimeSpan timeout, CancellationToken cancellationToken)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TLockableKey : ILockableKey
        {
            var startTime = DateTime.UtcNow;
            while (true)
            {
                if (clientSession.store.InternalPromoteLock(key.KeyHash))
                {
                    ++clientSession.exclusiveLockCount;
                    --clientSession.sharedLockCount;

                    // Success; the caller should update the ILockableKey.LockType so the unlock has the right type
                    return true;
                }

                // CancellationToken can accompany either of the other two mechanisms
                if (cancellationToken.IsCancellationRequested || DateTime.UtcNow.Ticks - startTime.Ticks > timeout.Ticks)
                    break;  // out of the retry loop

                // Lock failed, must retry
                _ = clientSession.store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(OperationStatus.RETRY_LATER, sessionFunctions);
            }

            // Failed to promote
            return false;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static OperationStatus DoManualLock<TLockableKey>(ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession, TLockableKey key)
            where TLockableKey : ILockableKey
        {
            if (key.LockType == LockType.Shared)
            {
                if (!clientSession.store.InternalTryLockShared(key.KeyHash))
                    return OperationStatus.RETRY_LATER;
                ++clientSession.sharedLockCount;
            }
            else
            {
                if (!clientSession.store.InternalTryLockExclusive(key.KeyHash))
                    return OperationStatus.RETRY_LATER;
                ++clientSession.exclusiveLockCount;
            }
            return OperationStatus.SUCCESS;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void DoManualUnlock<TLockableKey>(ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession,
                                                                   ReadOnlySpan<TLockableKey> keys)
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code.
            // Unlock has to be done in the reverse order of locking, so we take the *last* occurrence of each key there, and keyIdx moves backward.
            for (var keyIdx = keys.Length - 1; keyIdx >= 0; --keyIdx)
            {
                ref readonly var key = ref keys[keyIdx];
                if (keyIdx == 0 || clientSession.store.LockTable.GetBucketIndex(key.KeyHash) != clientSession.store.LockTable.GetBucketIndex(keys[keyIdx - 1].KeyHash))
                {
                    if (key.LockType == LockType.Shared)
                    {
                        clientSession.store.InternalUnlockShared(key.KeyHash);
                        --clientSession.sharedLockCount;
                    }
                    else
                    {
                        clientSession.store.InternalUnlockExclusive(key.KeyHash);
                        --clientSession.exclusiveLockCount;
                    }
                }
            }
        }

        /// <inheritdoc/>
        public void Lock<TLockableKey>(ReadOnlySpan<TLockableKey> keys)
            where TLockableKey : ILockableKey
        {
            clientSession.CheckIsAcquiredLockable(sessionFunctions);
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for LockableUnsafeContext.Lock()");
            var lockAcquired = false;
            while (!lockAcquired)
            {
                clientSession.UnsafeResumeThread(sessionFunctions);
                try
                {
                    lockAcquired = DoManualLock(sessionFunctions, clientSession, keys);
                }
                finally
                {
                    clientSession.UnsafeSuspendThread();
                }
            }
        }

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(ReadOnlySpan<TLockableKey> keys)
            where TLockableKey : ILockableKey
            => TryLock(keys, Timeout.InfiniteTimeSpan, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(ReadOnlySpan<TLockableKey> keys, TimeSpan timeout)
            where TLockableKey : ILockableKey
            => TryLock(keys, timeout, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(ReadOnlySpan<TLockableKey> keys, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey
            => TryLock(keys, Timeout.InfiniteTimeSpan, cancellationToken);

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(ReadOnlySpan<TLockableKey> keys, TimeSpan timeout, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey
        {
            clientSession.CheckIsAcquiredLockable(sessionFunctions);
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for LockableUnsafeContext.Lock()");

            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return DoManualTryLock(sessionFunctions, clientSession, keys, timeout, cancellationToken);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public bool TryPromoteLock<TLockableKey>(TLockableKey key)
            where TLockableKey : ILockableKey
            => TryPromoteLock(key, Timeout.InfiniteTimeSpan, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryPromoteLock<TLockableKey>(TLockableKey key, TimeSpan timeout)
            where TLockableKey : ILockableKey
            => TryPromoteLock(key, timeout, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryPromoteLock<TLockableKey>(TLockableKey key, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey
            => TryPromoteLock(key, Timeout.InfiniteTimeSpan, cancellationToken);

        /// <inheritdoc/>
        public bool TryPromoteLock<TLockableKey>(TLockableKey key, TimeSpan timeout, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey
        {
            clientSession.CheckIsAcquiredLockable(sessionFunctions);
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for LockableUnsafeContext.Lock()");

            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return DoManualTryPromoteLock(sessionFunctions, clientSession, key, timeout, cancellationToken);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public void Unlock<TLockableKey>(ReadOnlySpan<TLockableKey> keys)
            where TLockableKey : ILockableKey
        {
            clientSession.CheckIsAcquiredLockable(sessionFunctions);
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for LockableUnsafeContext.Unlock()");

            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                DoManualUnlock(clientSession, keys);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// The id of the current Tsavorite Session
        /// </summary>
        public int SessionID { get { return clientSession.ctx.sessionID; } }

        #endregion Key Locking

        #region ITsavoriteContext

        /// <inheritdoc/>
        public ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session => clientSession;

        /// <inheritdoc/>
        public long GetKeyHash(TKey key) => clientSession.store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref TKey key) => clientSession.store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.UnsafeCompletePending(sessionFunctions, false, wait, spinWaitForCommit);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.UnsafeCompletePendingWithOutputs(sessionFunctions, out completedOutputs, wait, spinWaitForCommit);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingAsync(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingWithOutputsAsync(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextRead(ref key, ref input, ref output, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            => Read(ref key, ref input, ref output, ref readOptions, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, TInput input, out TOutput output, TContext userContext = default)
        {
            output = default;
            return Read(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, TInput input, out TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            output = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref TKey key, ref TOutput output, TContext userContext = default)
        {
            TInput input = default;
            return Read(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref TKey key, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, out TOutput output, TContext userContext = default)
        {
            TInput input = default;
            output = default;
            return Read(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, out TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            output = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(TKey key, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(ref key, ref input, ref output, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(TKey key, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(ref key, ref input, ref output, ref readOptions, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextRead(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadWithPrefetch<TBatch>(ref TBatch batch, TContext userContext = default)
            where TBatch : IReadArgBatch<TKey, TInput, TOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                clientSession.store.ContextReadWithPrefetch<TBatch, TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, LockableSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(ref batch, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextReadAtAddress(address, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TValue desiredValue, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(ref key, clientSession.store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TValue desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, TContext userContext = default)
            => Upsert(ref key, clientSession.store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref TKey key, long keyHash, ref TInput input, ref TValue desiredValue, ref TOutput output, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => Upsert(ref key, clientSession.store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref TKey key, long keyHash, ref TInput input, ref TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(TKey key, TValue desiredValue, TContext userContext = default)
            => Upsert(ref key, ref desiredValue, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(TKey key, TValue desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            => Upsert(ref key, ref desiredValue, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(TKey key, TInput input, TValue desiredValue, ref TOutput output, TContext userContext = default)
            => Upsert(ref key, ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(TKey key, TInput input, TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => Upsert(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
            => RMW(ref key, ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            => RMW(ref key, rmwOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref TKey key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => RMW(ref key, clientSession.store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => RMW(ref key, rmwOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status RMW(ref TKey key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextRMW(ref key, keyHash, ref input, ref output, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(TKey key, TInput input, out TOutput output, TContext userContext = default)
        {
            output = default;
            return RMW(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(TKey key, TInput input, out TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
        {
            output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref TKey key, ref TInput input, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref TKey key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(TKey key, TInput input, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(TKey key, TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref TKey key, TContext userContext = default)
            => Delete(ref key, clientSession.store.storeFunctions.GetKeyHashCode64(ref key), userContext);

        /// <inheritdoc/>
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref TKey key, ref DeleteOptions deleteOptions, TContext userContext = default)
            => Delete(ref key, deleteOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(ref key), userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Delete(ref TKey key, long keyHash, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextDelete<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, LockableSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(
                    ref key, keyHash, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(TKey key, TContext userContext = default)
            => Delete(ref key, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(TKey key, ref DeleteOptions deleteOptions, TContext userContext = default)
            => Delete(ref key, ref deleteOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(ref TKey key)
            => clientSession.ResetModified(sessionFunctions, ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified(TKey key)
            => clientSession.IsModified(sessionFunctions, ref key);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                clientSession.store.InternalRefresh<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, LockableSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        #endregion ITsavoriteContext
    }
}