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
    /// Tsavorite Context implementation that allows manual control of record locking and epoch management. For advanced use only.
    /// </summary>
    public readonly struct LockableContext<Key, Value, Input, Output, Context, Functions> : ITsavoriteContext<Key, Value, Input, Output, Context>, ILockableContext<Key>
        where Functions : ISessionFunctions<Key, Value, Input, Output, Context>
    {
        readonly ClientSession<Key, Value, Input, Output, Context, Functions> clientSession;
        readonly SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, LockableSessionLocker<Key, Value>> sessionFunctions;

        /// <summary>Indicates whether this struct has been initialized</summary>
        public bool IsNull => clientSession is null;

        const int KeyLockMaxRetryAttempts = 1000;

        internal LockableContext(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            this.clientSession = clientSession;
            sessionFunctions = new(clientSession);
        }

        #region Begin/EndLockable

        /// <inheritdoc/>
        public void BeginLockable() => clientSession.AcquireLockable(sessionFunctions);

        /// <inheritdoc/>
        public void EndLockable() => clientSession.ReleaseLockable();

        #endregion Begin/EndLockable

        #region Key Locking

        /// <inheritdoc/>
        public bool NeedKeyHash => clientSession.NeedKeyHash;

        /// <inheritdoc/>
        public int CompareKeyHashes<TLockableKey>(TLockableKey key1, TLockableKey key2) where TLockableKey : ILockableKey => clientSession.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public int CompareKeyHashes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2) where TLockableKey : ILockableKey => clientSession.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortKeyHashes<TLockableKey>(TLockableKey[] keys) where TLockableKey : ILockableKey => clientSession.SortKeyHashes(keys);

        /// <inheritdoc/>
        public void SortKeyHashes<TLockableKey>(TLockableKey[] keys, int start, int count) where TLockableKey : ILockableKey => clientSession.SortKeyHashes(keys, start, count);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool DoInternalLock<TsavoriteSession, TLockableKey>(TsavoriteSession tsavoriteSession, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                                                   TLockableKey[] keys, int start, int count)
            where TsavoriteSession : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code,
            // which of course allows the session to do shared operations as well, so we take the first occurrence of each key code.
            // This is the same as DoInternalTryLock but without timeout; it will keep trying until it acquires all locks.
            var end = start + count - 1;

            int retryCount = 0;
        Retry:
            long prevBucketIndex = -1;

            for (int keyIdx = start; keyIdx <= end; ++keyIdx)
            {
                ref var key = ref keys[keyIdx];
                long currBucketIndex = clientSession.store.LockTable.GetBucketIndex(key.KeyHash);
                if (currBucketIndex != prevBucketIndex)
                {
                    prevBucketIndex = currBucketIndex;
                    OperationStatus status = DoInternalLock(clientSession, key);
                    if (status == OperationStatus.SUCCESS)
                        continue;   // Success; continue to the next key.

                    // Lock failure before we've completed all keys, and we did not lock the current key. Unlock anything we've locked.
                    DoInternalUnlock(clientSession, keys, start, keyIdx - 1);

                    // We've released our locks so this refresh will let other threads advance and release their locks, and we will retry with a full timeout.
                    clientSession.store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, TsavoriteSession>(status, tsavoriteSession);
                    retryCount++;
                    if (retryCount >= KeyLockMaxRetryAttempts)
                    {
                        return false;
                    }
                    goto Retry;
                }
            }

            // We reached the end of the list, possibly after a duplicate keyhash; all locks were successful.
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool DoInternalTryLock<TsavoriteSession, TLockableKey>(TsavoriteSession tsavoriteSession, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                                                   TLockableKey[] keys, int start, int count, TimeSpan timeout, CancellationToken cancellationToken)
            where TsavoriteSession : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code,
            // which of course allows the session to do shared operations as well, so we take the first occurrence of each key code.
            // This is the same as DoInternalLock but with timeout.
            var end = start + count - 1;

            // We can't start each retry with a full timeout because we might always fail if someone is not unlocking (e.g. another thread hangs
            // somehow while holding a lock, or the current thread has issued two lock calls on two key sets and the second tries to lock one in
            // the first, and so on). So set the timeout high enough to accommodate as many retries as you want.
            var startTime = DateTime.UtcNow;

        Retry:
            long prevBucketIndex = -1;

            for (int keyIdx = start; keyIdx <= end; ++keyIdx)
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
                        status = DoInternalLock(clientSession, key);
                        if (status == OperationStatus.SUCCESS)
                            continue;   // Success; continue to the next key.
                    }

                    // Cancellation or lock failure before we've completed all keys; we have not locked the current key. Unlock anything we've locked.
                    DoInternalUnlock(clientSession, keys, start, keyIdx - 1);

                    // Lock failure is the only place we check the timeout. If we've exceeded that, or if we've had a cancellation, return false.
                    if (cancellationToken.IsCancellationRequested || DateTime.UtcNow.Ticks - startTime.Ticks > timeout.Ticks)
                        return false;

                    // No cancellation and we're within the timeout. We've released our locks so this refresh will let other threads advance
                    // and release their locks, and we will retry with a full timeout.
                    clientSession.store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, TsavoriteSession>(status, tsavoriteSession);
                    goto Retry;
                }
            }

            // All locks were successful.
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool DoInternalTryPromoteLock<TsavoriteSession, TLockableKey>(TsavoriteSession tsavoriteSession, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                                                   TLockableKey key, TimeSpan timeout, CancellationToken cancellationToken)
            where TsavoriteSession : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
            where TLockableKey : ILockableKey
        {
            var startTime = DateTime.UtcNow;
            while (true)
            {
                OperationStatus status = clientSession.store.InternalPromoteLock(key.KeyHash);
                if (status == OperationStatus.SUCCESS)
                {
                    ++clientSession.exclusiveLockCount;
                    --clientSession.sharedLockCount;

                    // Success; the caller should update the ILockableKey.LockType so the unlock has the right type
                    return true;
                }

                // CancellationToken can accompany either of the other two mechanisms
                if (cancellationToken.IsCancellationRequested || DateTime.UtcNow.Ticks - startTime.Ticks > timeout.Ticks)
                    break;  // out of the retry loop

                clientSession.store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, TsavoriteSession>(status, tsavoriteSession);
            }

            // Failed to promote
            return false;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static OperationStatus DoInternalLock<TLockableKey>(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession, TLockableKey key)
            where TLockableKey : ILockableKey
        {
            OperationStatus status = clientSession.store.InternalLock(key.KeyHash, key.LockType);
            if (status == OperationStatus.SUCCESS)
            {
                if (key.LockType == LockType.Exclusive)
                    ++clientSession.exclusiveLockCount;
                else if (key.LockType == LockType.Shared)
                    ++clientSession.sharedLockCount;
            }
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void DoInternalUnlock<TLockableKey>(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                                                   TLockableKey[] keys, int start, int keyIdx)
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code.
            // Unlock has to be done in the reverse order of locking, so we take the *last* occurrence of each key there, and keyIdx moves backward.
            for (; keyIdx >= start; --keyIdx)
            {
                ref var key = ref keys[keyIdx];
                if (keyIdx == start || clientSession.store.LockTable.GetBucketIndex(key.KeyHash) != clientSession.store.LockTable.GetBucketIndex(keys[keyIdx - 1].KeyHash))
                {
                    clientSession.store.InternalUnlock(key.KeyHash, key.LockType);
                    if (key.LockType == LockType.Exclusive)
                        --clientSession.exclusiveLockCount;
                    else if (key.LockType == LockType.Shared)
                        --clientSession.sharedLockCount;
                }
            }
        }

        /// <inheritdoc/>
        public void Lock<TLockableKey>(TLockableKey[] keys) where TLockableKey : ILockableKey => Lock(keys, 0, keys.Length);

        /// <inheritdoc/>
        public void Lock<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey
        {
            clientSession.CheckIsAcquiredLockable();
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for LockableUnsafeContext.Lock()");
            bool lockAquired = false;
            while (!lockAquired)
            {
                clientSession.UnsafeResumeThread(sessionFunctions);
                try
                {
                    lockAquired = DoInternalLock(sessionFunctions, clientSession, keys, start, count);
                }
                finally
                {
                    clientSession.UnsafeSuspendThread();
                }
            }
        }

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(TLockableKey[] keys)
            where TLockableKey : ILockableKey
            => TryLock(keys, 0, keys.Length, Timeout.InfiniteTimeSpan, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(TLockableKey[] keys, TimeSpan timeout)
            where TLockableKey : ILockableKey
            => TryLock(keys, 0, keys.Length, timeout, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(TLockableKey[] keys, int start, int count, TimeSpan timeout)
            where TLockableKey : ILockableKey
            => TryLock(keys, start, count, timeout, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(TLockableKey[] keys, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey
            => TryLock(keys, 0, keys.Length, Timeout.InfiniteTimeSpan, cancellationToken);

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(TLockableKey[] keys, int start, int count, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey
            => TryLock(keys, start, count, Timeout.InfiniteTimeSpan, cancellationToken);

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(TLockableKey[] keys, TimeSpan timeout, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey
            => TryLock(keys, 0, keys.Length, timeout, cancellationToken);

        /// <inheritdoc/>
        public bool TryLock<TLockableKey>(TLockableKey[] keys, int start, int count, TimeSpan timeout, CancellationToken cancellationToken)
            where TLockableKey : ILockableKey
        {
            clientSession.CheckIsAcquiredLockable();
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for LockableUnsafeContext.Lock()");

            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return DoInternalTryLock(sessionFunctions, clientSession, keys, start, count, timeout, cancellationToken);
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
            clientSession.CheckIsAcquiredLockable();
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for LockableUnsafeContext.Lock()");

            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return DoInternalTryPromoteLock(sessionFunctions, clientSession, key, timeout, cancellationToken);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public void Unlock<TLockableKey>(TLockableKey[] keys) where TLockableKey : ILockableKey => Unlock(keys, 0, keys.Length);

        /// <inheritdoc/>
        public void Unlock<TLockableKey>(TLockableKey[] keys, int start, int count)
            where TLockableKey : ILockableKey
        {
            clientSession.CheckIsAcquiredLockable();
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected(), "Trying to protect an already-protected epoch for LockableUnsafeContext.Unlock()");

            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                DoInternalUnlock(clientSession, keys, start, start + count - 1);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// The session id of TsavoriteSession
        /// </summary>
        public int SessionID { get { return clientSession.ctx.sessionID; } }

        #endregion Key Locking

        #region ITsavoriteContext

        /// <inheritdoc/>
        public long GetKeyHash(Key key) => clientSession.store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref Key key) => clientSession.store.GetKeyHash(ref key);

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
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
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
        public ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingWithOutputsAsync(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default)
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
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default)
            => Read(ref key, ref input, ref output, ref readOptions, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, Context userContext = default)
        {
            output = default;
            return Read(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, ref ReadOptions readOptions, Context userContext = default)
        {
            output = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, Context userContext = default)
        {
            Input input = default;
            return Read(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, ref ReadOptions readOptions, Context userContext = default)
        {
            Input input = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, Context userContext = default)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, ref ReadOptions readOptions, Context userContext = default)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, Context userContext = default)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, ref ReadOptions readOptions, Context userContext = default)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, ref readOptions, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
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
        public Status ReadAtAddress(long address, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
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
        public Status ReadAtAddress(long address, ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
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
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, CancellationToken cancellationToken = default)
        {
            ReadOptions readOptions = default;
            return ReadAsync(ref key, ref input, ref readOptions, userContext, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, ref ReadOptions readOptions, Context userContext = default, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ReadAsync(sessionFunctions, ref key, ref input, ref readOptions, userContext, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, CancellationToken token = default)
        {
            ReadOptions readOptions = default;
            return ReadAsync(ref key, ref input, ref readOptions, context, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, ref ReadOptions readOptions, Context context = default, CancellationToken token = default)
            => ReadAsync(ref key, ref input, ref readOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, CancellationToken token = default)
        {
            ReadOptions readOptions = default;
            return ReadAsync(ref key, ref readOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref ReadOptions readOptions, Context userContext = default, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            Input input = default;
            return clientSession.store.ReadAsync(sessionFunctions, ref key, ref input, ref readOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, CancellationToken token = default)
            => ReadAsync(ref key, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, ref ReadOptions readOptions, Context context = default, CancellationToken token = default)
            => ReadAsync(ref key, ref readOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            Key key = default;
            return clientSession.store.ReadAtAddressAsync(sessionFunctions, address, ref key, ref input, ref readOptions, userContext, cancellationToken, noKey: true);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Key key, ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ReadAtAddressAsync(sessionFunctions, address, ref key, ref input, ref readOptions, userContext, cancellationToken, noKey: false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default)
            => Upsert(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default)
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
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
            => Upsert(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
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
        public Status Upsert(Key key, Value desiredValue, Context userContext = default)
            => Upsert(ref key, ref desiredValue, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default)
            => Upsert(ref key, ref desiredValue, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, Context userContext = default)
            => Upsert(ref key, ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default)
            => Upsert(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, Context userContext = default, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, CancellationToken token = default)
        {
            UpsertOptions upsertOptions = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.UpsertAsync<Input, Output, Context, SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, LockableSessionLocker<Key, Value>>>
                (sessionFunctions, ref key, ref input, ref desiredValue, ref upsertOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, Context userContext = default, CancellationToken token = default)
            => UpsertAsync(ref key, ref desiredValue, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
            => UpsertAsync(ref key, ref desiredValue, ref upsertOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, Context userContext = default, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default)
            => RMW(ref key, ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, Context userContext = default)
            => RMW(ref key, rmwOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
            => RMW(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => RMW(ref key, rmwOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status RMW(ref Key key, long keyHash, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
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
        public Status RMW(Key key, Input input, out Output output, Context userContext = default)
        {
            output = default;
            return RMW(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, out Output output, ref RMWOptions rmwOptions, Context userContext = default)
        {
            output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, Context userContext = default)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context userContext = default)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, Context userContext = default)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, ref RMWOptions rmwOptions, Context userContext = default)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, CancellationToken token = default)
        {
            RMWOptions rmwOptions = default;
            return RMWAsync(ref key, ref input, ref rmwOptions, context, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context context = default, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.RmwAsync<Input, Output, Context, SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, LockableSessionLocker<Key, Value>>>(
                sessionFunctions, ref key, ref input, ref rmwOptions, context, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, CancellationToken token = default)
            => RMWAsync(ref key, ref input, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, ref RMWOptions rmwOptions, Context context = default, CancellationToken token = default)
            => RMWAsync(ref key, ref input, ref rmwOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default)
            => Delete(ref key, clientSession.store.comparer.GetHashCode64(ref key), userContext);

        /// <inheritdoc/>
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default)
            => Delete(ref key, deleteOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), userContext);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Delete(ref Key key, long keyHash, Context userContext = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                return clientSession.store.ContextDelete<Input, Output, Context, SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, LockableSessionLocker<Key, Value>>>(
                    ref key, keyHash, userContext, sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, Context userContext = default)
            => Delete(ref key, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, ref DeleteOptions deleteOptions, Context userContext = default)
            => Delete(ref key, ref deleteOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, CancellationToken token = default)
        {
            DeleteOptions deleteOptions = default;
            return DeleteAsync(ref key, ref deleteOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.DeleteAsync<Input, Output, Context, SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, LockableSessionLocker<Key, Value>>>(
                sessionFunctions, ref key, ref deleteOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, CancellationToken token = default)
            => DeleteAsync(ref key, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, ref DeleteOptions deleteOptions, Context userContext = default, CancellationToken token = default)
            => DeleteAsync(ref key, ref deleteOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(ref Key key)
            => clientSession.ResetModified(sessionFunctions, ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified(Key key)
            => clientSession.IsModified(sessionFunctions, ref key);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(sessionFunctions);
            try
            {
                clientSession.store.InternalRefresh<Input, Output, Context, SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, LockableSessionLocker<Key, Value>>>(sessionFunctions);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        #endregion ITsavoriteContext

    }
}