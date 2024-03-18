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
    public readonly struct LockableContext<Key, Value, Input, Output, Context, Functions> : ITsavoriteContext<Key, Value, Input, Output, Context>, ILockableContext<Key>
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        readonly ClientSession<Key, Value, Input, Output, Context, Functions> clientSession;
        readonly InternalTsavoriteSession TsavoriteSession;

        /// <summary>Indicates whether this struct has been initialized</summary>
        public bool IsNull => clientSession is null;

        internal LockableContext(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            this.clientSession = clientSession;
            TsavoriteSession = new InternalTsavoriteSession(clientSession);
        }

        #region Begin/EndLockable

        /// <inheritdoc/>
        public void BeginLockable() => clientSession.AcquireLockable();

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
        internal static void DoInternalLock<TsavoriteSession, TLockableKey>(TsavoriteSession tsavoriteSession, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                                                   TLockableKey[] keys, int start, int count)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
            where TLockableKey : ILockableKey
        {
            // The key codes are sorted, but there may be duplicates; the sorting is such that exclusive locks come first for each key code,
            // which of course allows the session to do shared operations as well, so we take the first occurrence of each key code.
            // This is the same as DoInternalTryLock but without timeout; it will keep trying until it acquires all locks.
            var end = start + count - 1;

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
                    goto Retry;
                }
            }

            // We reached the end of the list, possibly after a duplicate keyhash; all locks were successful.
            return;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool DoInternalTryLock<TsavoriteSession, TLockableKey>(TsavoriteSession tsavoriteSession, ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                                                                   TLockableKey[] keys, int start, int count, TimeSpan timeout, CancellationToken cancellationToken)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
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

                    // Cancellation or lock failure before we've completed all keys; we may or may not have locked the current key. Unlock anything we've locked.
                    var unlockIdx = keyIdx - (status == OperationStatus.SUCCESS ? 0 : 1);
                    DoInternalUnlock(clientSession, keys, start, unlockIdx);

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
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
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

            clientSession.UnsafeResumeThread();
            try
            {
                DoInternalLock(TsavoriteSession, clientSession, keys, start, count);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
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

            clientSession.UnsafeResumeThread();
            try
            {
                return DoInternalTryLock(TsavoriteSession, clientSession, keys, start, count, timeout, cancellationToken);
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

            clientSession.UnsafeResumeThread();
            try
            {
                return DoInternalTryPromoteLock(TsavoriteSession, clientSession, key, timeout, cancellationToken);
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

            clientSession.UnsafeResumeThread();
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
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.UnsafeCompletePending(TsavoriteSession, false, wait, spinWaitForCommit);
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
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.UnsafeCompletePendingWithOutputs(TsavoriteSession, out completedOutputs, wait, spinWaitForCommit);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingAsync(waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingWithOutputsAsync(waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.store.ContextRead(ref key, ref input, ref output, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
            => Read(ref key, ref input, ref output, ref readOptions, out _, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, userContext, serialNo), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, ref readOptions, userContext, serialNo), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.store.ContextRead(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.store.ContextReadAtAddress(address, ref input, ref output, ref readOptions, out recordMetadata, userContext, serialNo, TsavoriteSession);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.store.ContextReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, serialNo, TsavoriteSession);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            ReadOptions readOptions = default;
            return ReadAsync(ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ReadAsync(TsavoriteSession, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            ReadOptions readOptions = default;
            return ReadAsync(ref key, ref input, ref readOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, ref ReadOptions readOptions, Context context = default, long serialNo = 0, CancellationToken token = default)
            => ReadAsync(ref key, ref input, ref readOptions, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            ReadOptions readOptions = default;
            return ReadAsync(ref key, ref readOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            Input input = default;
            return clientSession.store.ReadAsync(TsavoriteSession, ref key, ref input, ref readOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default)
            => ReadAsync(ref key, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, ref ReadOptions readOptions, Context context = default, long serialNo = 0, CancellationToken token = default)
            => ReadAsync(ref key, ref readOptions, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            Key key = default;
            return clientSession.store.ReadAtAddressAsync(TsavoriteSession, address, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken, noKey: true);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Key key, ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ReadAtAddressAsync(TsavoriteSession, address, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken, noKey: false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, out recordMetadata, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref desiredValue, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref desiredValue, ref upsertOptions, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref input, ref desiredValue, ref output, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            UpsertOptions upsertOptions = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.UpsertAsync<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, ref key, ref input, ref desiredValue, ref upsertOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref desiredValue, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref desiredValue, ref upsertOptions, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
            => RMW(ref key, ref input, ref output, out _, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, Context userContext = default, long serialNo = 0)
            => RMW(ref key, rmwOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref output, out _, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
            => RMW(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref output, out recordMetadata, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
            => RMW(ref key, rmwOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref output, out recordMetadata, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, long keyHash, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.store.ContextRMW(ref key, keyHash, ref input, ref output, out recordMetadata, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, out Output output, ref RMWOptions rmwOptions, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, ref RMWOptions rmwOptions, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            RMWOptions rmwOptions = default;
            return RMWAsync(ref key, ref input, ref rmwOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.RmwAsync<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, ref key, ref input, ref rmwOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
            => RMWAsync(ref key, ref input, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, ref RMWOptions rmwOptions, Context context = default, long serialNo = 0, CancellationToken token = default)
            => RMWAsync(ref key, ref input, ref rmwOptions, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default, long serialNo = 0)
            => Delete(ref key, clientSession.store.comparer.GetHashCode64(ref key), userContext, serialNo);

        /// <inheritdoc/>
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default, long serialNo = 0)
            => Delete(ref key, deleteOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), userContext, serialNo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Delete(ref Key key, long keyHash, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.store.ContextDelete<Input, Output, Context, InternalTsavoriteSession>(ref key, keyHash, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, Context userContext = default, long serialNo = 0)
            => Delete(ref key, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, ref DeleteOptions deleteOptions, Context userContext = default, long serialNo = 0)
            => Delete(ref key, ref deleteOptions, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            DeleteOptions deleteOptions = default;
            return DeleteAsync(ref key, ref deleteOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.DeleteAsync<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, ref key, ref deleteOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => DeleteAsync(ref key, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, ref DeleteOptions deleteOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => DeleteAsync(ref key, ref deleteOptions, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(ref Key key)
            => clientSession.ResetModified(ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified(Key key)
            => clientSession.IsModified(ref key);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                clientSession.store.InternalRefresh<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        #endregion ITsavoriteContext

        #region ITsavoriteSession

        // This is a struct to allow JIT to inline calls (and bypass default interface call mechanism)
        internal readonly struct InternalTsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            private readonly ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;

            public InternalTsavoriteSession(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
            {
                _clientSession = clientSession;
            }

            public bool IsManualLocking => true;
            public TsavoriteKV<Key, Value> Store => _clientSession.store;

            #region IFunctions - Reads
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
                => _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            {
                // Note: RecordIsolation is not used with Lockable contexts
                return _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo, ref recordInfo);
            }

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion IFunctions - Reads

            #region IFunctions - Upserts
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
                => _clientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentWriter(long physicalAddress, ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                // Note: RecordIsolation is not used with Lockable contexts
                (upsertInfo.UsedValueLength, upsertInfo.FullValueLength, _) = _clientSession.store.GetRecordLengths(physicalAddress, ref dst, ref recordInfo);
                if (!_clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo))
                    return false;
                _clientSession.store.SetExtraValueLength(ref dst, ref recordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);
                recordInfo.SetDirtyAndModified();
                return true;
            }
            #endregion IFunctions - Upserts

            #region IFunctions - RMWs
            #region InitialUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
                => _clientSession.functions.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            }
            #endregion InitialUpdater

            #region CopyUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output, ref rmwInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
                => _clientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            }
            #endregion CopyUpdater

            #region InPlaceUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InPlaceUpdater(long physicalAddress, ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, out OperationStatus status, ref RecordInfo recordInfo)
            {
                // Note: RecordIsolation is not used with Lockable contexts
                (rmwInfo.UsedValueLength, rmwInfo.FullValueLength, _) = _clientSession.store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);
                if (!_clientSession.InPlaceUpdater(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo, out status))
                    return false;
                _clientSession.store.SetExtraValueLength(ref value, ref recordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                recordInfo.SetDirtyAndModified();
                return true;
            }
            #endregion InPlaceUpdater

            public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion IFunctions - RMWs

            #region IFunctions - Deletes
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
                => _clientSession.functions.SingleDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostSingleDeleter(ref key, ref deleteInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentDeleter(long physicalAddress, ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo, out int allocatedSize)
            {
                // Note: RecordIsolation is not used with Lockable contexts
                (deleteInfo.UsedValueLength, deleteInfo.FullValueLength, allocatedSize) = _clientSession.store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);
                if (!_clientSession.functions.ConcurrentDeleter(ref key, ref value, ref deleteInfo, ref recordInfo))
                    return false;
                _clientSession.store.SetTombstoneAndExtraValueLength(ref value, ref recordInfo, deleteInfo.UsedValueLength, deleteInfo.FullValueLength);
                recordInfo.SetDirtyAndModified();
                return true;
            }
            #endregion IFunctions - Deletes

            #region IFunctions - Dispose
            public void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
                => _clientSession.functions.DisposeSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
            public void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.DisposeCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            public void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.DisposeInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            public void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo)
                => _clientSession.functions.DisposeSingleDeleter(ref key, ref value, ref deleteInfo);
            public void DisposeDeserializedFromDisk(ref Key key, ref Value value, ref RecordInfo recordInfo)
                => _clientSession.functions.DisposeDeserializedFromDisk(ref key, ref value);
            public void DisposeForRevivification(ref Key key, ref Value value, int newKeySize, ref RecordInfo recordInfo)
                => _clientSession.functions.DisposeForRevivification(ref key, ref value, newKeySize);
            #endregion IFunctions - Dispose

            #region IFunctions - Checkpointing
            public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
            {
                _clientSession.functions.CheckpointCompletionCallback(sessionID, sessionName, commitPoint);
                _clientSession.LatestCommitPoint = commitPoint;
            }
            #endregion IFunctions - Checkpointing

            #region Transient locking
            public bool TryLockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                Debug.Assert(Store.LockTable.IsLockedExclusive(ref key, ref stackCtx.hei),
                            $"Attempting to use a non-XLocked key in a Lockable context (requesting XLock):"
                            + $" XLocked {_clientSession.store.LockTable.IsLockedExclusive(ref key, ref stackCtx.hei)},"
                            + $" Slocked {_clientSession.store.LockTable.IsLockedShared(ref key, ref stackCtx.hei)}");
                return true;
            }

            public bool TryLockTransientShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                Debug.Assert(Store.LockTable.IsLocked(ref key, ref stackCtx.hei),
                            $"Attempting to use a non-Locked (S or X) key in a Lockable context (requesting SLock):"
                            + $" XLocked {_clientSession.store.LockTable.IsLockedExclusive(ref key, ref stackCtx.hei)},"
                            + $" Slocked {_clientSession.store.LockTable.IsLockedShared(ref key, ref stackCtx.hei)}");
                return true;
            }

            public void UnlockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                Debug.Assert(Store.LockTable.IsLockedExclusive(ref key, ref stackCtx.hei),
                            $"Attempting to unlock a non-XLocked key in a Lockable context (requesting XLock):"
                            + $" XLocked {_clientSession.store.LockTable.IsLockedExclusive(ref key, ref stackCtx.hei)},"
                            + $" Slocked {_clientSession.store.LockTable.IsLockedShared(ref key, ref stackCtx.hei)}");
            }

            public void UnlockTransientShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                Debug.Assert(Store.LockTable.IsLockedShared(ref key, ref stackCtx.hei),
                            $"Attempting to use a non-XLocked key in a Lockable context (requesting XLock):"
                            + $" XLocked {_clientSession.store.LockTable.IsLockedExclusive(ref key, ref stackCtx.hei)},"
                            + $" Slocked {_clientSession.store.LockTable.IsLockedShared(ref key, ref stackCtx.hei)}");
            }
            #endregion

            #region Internal utilities
            public int GetRMWInitialValueLength(ref Input input) => _clientSession.functions.GetRMWInitialValueLength(ref input);

            public int GetRMWModifiedValueLength(ref Value value, ref Input input) => _clientSession.functions.GetRMWModifiedValueLength(ref value, ref input);

            public IHeapContainer<Input> GetHeapContainer(ref Input input)
            {
                if (typeof(Input) == typeof(SpanByte))
                    return new SpanByteHeapContainer(ref Unsafe.As<Input, SpanByte>(ref input), _clientSession.store.hlog.bufferPool) as IHeapContainer<Input>;
                return new StandardHeapContainer<Input>(ref input);
            }

            public void UnsafeResumeThread() => _clientSession.UnsafeResumeThread();

            public void UnsafeSuspendThread() => _clientSession.UnsafeSuspendThread();

            public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
                => _clientSession.CompletePendingWithOutputs(out completedOutputs, wait, spinWaitForCommit);

            public TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> Ctx => _clientSession.ctx;
            #endregion Internal utilities
        }
        #endregion ITsavoriteSession
    }
}