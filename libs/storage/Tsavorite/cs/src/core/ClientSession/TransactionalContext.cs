﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Tsavorite Context implementation that allows Transactional control of locking and automatic epoch management. For advanced use only.
    /// </summary>
    public readonly struct TransactionalContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> : ITsavoriteContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>, ITransactionalContext
        where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        readonly ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession;
        readonly SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator> sessionFunctions;

        /// <inheritdoc/>
        public bool IsNull => clientSession is null;

        const int KeyLockMaxRetryAttempts = 1000;

        internal TransactionalContext(ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            this.clientSession = clientSession;
            sessionFunctions = new(clientSession);
        }

        #region Begin/EndTransaction

        /// <inheritdoc/>
        public void BeginTransaction() => clientSession.AcquireTransactional(sessionFunctions);

        /// <inheritdoc/>
        public void LocksAcquired(long txnVersion) => clientSession.LocksAcquired(sessionFunctions, txnVersion);

        /// <inheritdoc/>
        public void EndTransaction() => clientSession.ReleaseTransactional(sessionFunctions);

        #endregion Begin/EndTransaction

        #region Key Locking

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(TTransactionalKey key1, TTransactionalKey key2) where TTransactionalKey : ITransactionalKey => clientSession.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(ref TTransactionalKey key1, ref TTransactionalKey key2) where TTransactionalKey : ITransactionalKey => clientSession.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortKeyHashes<TTransactionalKey>(TTransactionalKey[] keys) where TTransactionalKey : ITransactionalKey => clientSession.SortKeyHashes(keys);

        /// <inheritdoc/>
        public void SortKeyHashes<TTransactionalKey>(TTransactionalKey[] keys, int start, int count) where TTransactionalKey : ITransactionalKey => clientSession.SortKeyHashes(keys, start, count);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool DoTransactionalLock<TSessionFunctionsWrapper, TTransactionalKey>(TSessionFunctionsWrapper sessionFunctions, ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession,
                                                                   TTransactionalKey[] keys, int start, int count)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TTransactionalKey : ITransactionalKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code,
            // which of course allows the session to do shared operations as well, so we take the first occurrence of each key code.
            // This is the same as DoManualTryLock but without timeout; it will keep trying until it acquires all locks or the hardcoded retry limit is reached.
            var end = start + count - 1;

            int retryCount = 0;
        Retry:
            long prevBucketIndex = -1;

            for (int keyIdx = start; keyIdx <= end; keyIdx++)
            {
                ref var key = ref keys[keyIdx];
                long currBucketIndex = clientSession.store.LockTable.GetBucketIndex(key.KeyHash);
                if (currBucketIndex != prevBucketIndex)
                {
                    prevBucketIndex = currBucketIndex;
                    OperationStatus status = DoTransactionalLock(clientSession, key);
                    if (status == OperationStatus.SUCCESS)
                        continue;   // Success; continue to the next key.

                    // Lock failure before we've completed all keys, and we did not lock the current key. Unlock anything we've locked.
                    DoManualUnlock(clientSession, keys, start, keyIdx - 1);

                    // We've released our locks so this refresh will let other threads advance and release their locks, and we will retry with a full timeout.
                    clientSession.store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions);
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
        internal static bool DoTransactionalTryLock<TSessionFunctionsWrapper, TTransactionalKey>(TSessionFunctionsWrapper sessionFunctions, ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession,
                                                                   TTransactionalKey[] keys, int start, int count, TimeSpan timeout, CancellationToken cancellationToken)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TTransactionalKey : ITransactionalKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code,
            // which of course allows the session to do shared operations as well, so we take the first occurrence of each key code.
            // This is the same as DoTransactionalLock but with timeout.
            var end = start + count - 1;

            // We can't start each retry with a full timeout because we might always fail if someone is not unlocking (e.g. another thread hangs
            // somehow while holding a lock, or the current thread has issued two lock calls on two key sets and the second tries to lock one in
            // the first, and so on). So set the timeout high enough to accommodate as many retries as you want.
            var startTime = DateTime.UtcNow;

        Retry:
            long prevBucketIndex = -1;

            for (int keyIdx = start; keyIdx <= end; keyIdx++)
            {
                ref var key = ref keys[keyIdx];
                long currBucketIndex = clientSession.store.LockTable.GetBucketIndex(key.KeyHash);
                if (currBucketIndex != prevBucketIndex)
                {
                    prevBucketIndex = currBucketIndex;

                    OperationStatus status;
                    if (cancellationToken.IsCancellationRequested)
                        status = OperationStatus.CANCELED;
                    else
                    {
                        status = DoTransactionalLock(clientSession, key);
                        if (status == OperationStatus.SUCCESS)
                            continue;   // Success; continue to the next key.
                    }

                    // Cancellation or lock failure before we've completed all keys; we have not locked the current key. Unlock anything we've locked.
                    DoManualUnlock(clientSession, keys, start, keyIdx - 1);

                    // Lock failure is the only place we check the timeout. If we've exceeded that, or if we've had a cancellation, return false.
                    if (cancellationToken.IsCancellationRequested || DateTime.UtcNow.Ticks - startTime.Ticks > timeout.Ticks)
                        return false;

                    // No cancellation and we're within the timeout. We've released our locks so this refresh will let other threads advance
                    // and release their locks, and we will retry with a full timeout.
                    clientSession.store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions);
                    goto Retry;
                }
            }

            // All locks were successful.
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool DoManualTryPromoteLock<TSessionFunctionsWrapper, TTransactionalKey>(TSessionFunctionsWrapper sessionFunctions, ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession,
                                                                   TTransactionalKey key, TimeSpan timeout, CancellationToken cancellationToken)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TTransactionalKey : ITransactionalKey
        {
            var startTime = DateTime.UtcNow;
            while (true)
            {
                if (clientSession.store.InternalPromoteLock(key.KeyHash))
                {
                    ++clientSession.exclusiveLockCount;
                    --clientSession.sharedLockCount;

                    // Success; the caller should update the ITransactionalKey.LockType so the unlock has the right type
                    return true;
                }

                // CancellationToken can accompany either of the other two mechanisms
                if (cancellationToken.IsCancellationRequested || DateTime.UtcNow.Ticks - startTime.Ticks > timeout.Ticks)
                    break;  // out of the retry loop

                // Lock failed, must retry
                clientSession.store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(OperationStatus.RETRY_LATER, sessionFunctions);
            }

            // Failed to promote
            return false;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static OperationStatus DoTransactionalLock<TTransactionalKey>(ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession, TTransactionalKey key)
            where TTransactionalKey : ITransactionalKey
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
        internal static void DoManualUnlock<TTransactionalKey>(ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession,
                                                                   TTransactionalKey[] keys, int start, int keyIdx)
            where TTransactionalKey : ITransactionalKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code.
            // Unlock has to be done in the reverse order of locking, so we take the *last* occurrence of each key there, and keyIdx moves backward.
            for (; keyIdx >= start; --keyIdx)
            {
                ref var key = ref keys[keyIdx];
                if (keyIdx == start || clientSession.store.LockTable.GetBucketIndex(key.KeyHash) != clientSession.store.LockTable.GetBucketIndex(keys[keyIdx - 1].KeyHash))
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
        public void Lock<TTransactionalKey>(TTransactionalKey[] keys) where TTransactionalKey : ITransactionalKey => Lock(keys, 0, keys.Length);

        /// <inheritdoc/>
        public void Lock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count)
            where TTransactionalKey : ITransactionalKey
        {
            clientSession.CheckIsAcquiredTransactional(sessionFunctions);
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for TransactionalUnsafeContext.Lock()");
            bool lockAquired = false;
            while (!lockAquired)
            {
                clientSession.UnsafeResumeThread(sessionFunctions);
                try
                {
                    lockAquired = DoTransactionalLock(sessionFunctions, clientSession, keys, start, count);
                }
                finally
                {
                    clientSession.UnsafeSuspendThread();
                }
            }
        }

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(TTransactionalKey[] keys)
            where TTransactionalKey : ITransactionalKey
            => TryLock(keys, 0, keys.Length, Timeout.InfiniteTimeSpan, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, TimeSpan timeout)
            where TTransactionalKey : ITransactionalKey
            => TryLock(keys, 0, keys.Length, timeout, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count, TimeSpan timeout)
            where TTransactionalKey : ITransactionalKey
            => TryLock(keys, start, count, timeout, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey
            => TryLock(keys, 0, keys.Length, Timeout.InfiniteTimeSpan, cancellationToken);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey
            => TryLock(keys, start, count, Timeout.InfiniteTimeSpan, cancellationToken);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, TimeSpan timeout, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey
            => TryLock(keys, 0, keys.Length, timeout, cancellationToken);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count, TimeSpan timeout, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey
        {
            clientSession.CheckIsAcquiredTransactional(sessionFunctions);
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for TransactionalUnsafeContext.TryLock()");

            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return DoTransactionalTryLock(sessionFunctions, clientSession, keys, start, count, timeout, cancellationToken);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key)
            where TTransactionalKey : ITransactionalKey
            => TryPromoteLock(key, Timeout.InfiniteTimeSpan, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key, TimeSpan timeout)
            where TTransactionalKey : ITransactionalKey
            => TryPromoteLock(key, timeout, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey
            => TryPromoteLock(key, Timeout.InfiniteTimeSpan, cancellationToken);

        /// <inheritdoc/>
        public bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key, TimeSpan timeout, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey
        {
            clientSession.CheckIsAcquiredTransactional(sessionFunctions);
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for TransactionalUnsafeContext.TryPromoteLock()");

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
        public void Unlock<TTransactionalKey>(TTransactionalKey[] keys) where TTransactionalKey : ITransactionalKey => Unlock(keys, 0, keys.Length);

        /// <inheritdoc/>
        public void Unlock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count)
            where TTransactionalKey : ITransactionalKey
        {
            clientSession.CheckIsAcquiredTransactional(sessionFunctions);
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for TransactionalUnsafeContext.Unlock()");

            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                DoManualUnlock(clientSession, keys, start, start + count - 1);
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
        public ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session => clientSession;

        /// <inheritdoc/>
        public long GetKeyHash(ReadOnlySpan<byte> key) => clientSession.store.GetKeyHash(key);

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
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
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
        public ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingWithOutputsAsync(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextRead(key, ref input, ref output, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            => Read(key, ref input, ref output, ref readOptions, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ReadOnlySpan<byte> key, ref TOutput output, TContext userContext = default)
        {
            TInput input = default;
            return Read(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ReadOnlySpan<byte> key, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            return Read(key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(ReadOnlySpan<byte> key, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(ReadOnlySpan<byte> key, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, ref readOptions, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextRead(key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
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
        public Status ReadAtAddress(long address, ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextReadAtAddress(address, key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> desiredValue, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, TContext userContext = default)
            => Upsert(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => Upsert(key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ReadOnlySpan<byte> key, long keyHash, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextUpsert(key, keyHash, ref input, srcStringValue: desiredValue, ref output, out _, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            var keyHash = upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key);
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextUpsert(key, keyHash, ref input, srcStringValue: desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, IHeapObject desiredValue, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, IHeapObject desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, TContext userContext = default)
            => Upsert(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => Upsert(key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ReadOnlySpan<byte> key, long keyHash, ref TInput input, IHeapObject desiredValue, ref TOutput output, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextUpsert(key, keyHash, ref input, srcObjectValue: desiredValue, ref output, out _, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            var keyHash = upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key);
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextUpsert(key, keyHash, ref input, srcObjectValue: desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(in TSourceLogRecord diskLogRecord)
            where TSourceLogRecord : ISourceLogRecord
            => Upsert(diskLogRecord.Key, in diskLogRecord);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord diskLogRecord)
            where TSourceLogRecord : ISourceLogRecord
        {
            TInput input = default;
            TOutput output = default;
            UpsertOptions upsertOptions = default;
            return Upsert(key, ref input, in diskLogRecord, ref output, ref upsertOptions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TSourceLogRecord : ISourceLogRecord
            => Upsert(inputLogRecord.Key, ref input, in inputLogRecord, ref output, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            var keyHash = upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key);

            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextUpsert(key, keyHash, ref input, inputLogRecord: in inputLogRecord, ref output, out _, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext = default)
            => RMW(key, ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            => RMW(key, rmwOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => RMW(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => RMW(key, rmwOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status RMW(ReadOnlySpan<byte> key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextRMW(key, keyHash, ref input, ref output, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ReadOnlySpan<byte> key, TContext userContext = default)
            => Delete(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), userContext);

        /// <inheritdoc/>
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ReadOnlySpan<byte> key, ref DeleteOptions deleteOptions, TContext userContext = default)
            => Delete(key, deleteOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Delete(ReadOnlySpan<byte> key, long keyHash, TContext userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextDelete<TInput, TOutput, TContext, SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(
                    key, keyHash, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(ReadOnlySpan<byte> key)
            => clientSession.ResetModified(sessionFunctions, key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified(ReadOnlySpan<byte> key)
            => clientSession.IsModified(sessionFunctions, key);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                clientSession.store.InternalRefresh<TInput, TOutput, TContext, SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        #endregion ITsavoriteContext
    }
}