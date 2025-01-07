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
    /// Tsavorite Context implementation that allows Transactional control of locking and manual epoch management. For advanced use only.
    /// </summary>
    public readonly struct TransactionalUnsafeContext<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        : ITsavoriteContext<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>, ITransactionalContext, IUnsafeContext
        where TFunctions : ISessionFunctions<TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        readonly ClientSession<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession;
        readonly SessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator> sessionFunctions;

        /// <inheritdoc/>
        public bool IsNull => clientSession is null;

        internal TransactionalUnsafeContext(ClientSession<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            this.clientSession = clientSession;
            sessionFunctions = new(clientSession);
        }

        #region Begin/EndUnsafe

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BeginUnsafe() => clientSession.UnsafeResumeThread(sessionFunctions);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EndUnsafe() => clientSession.UnsafeSuspendThread();

        #endregion Begin/EndUnsafe

        #region Begin/EndTransaction

        /// <inheritdoc/>
        public void BeginTransaction() => clientSession.AcquireTransactional(sessionFunctions);

        /// <inheritdoc/>
        public void EndTransaction() => clientSession.ReleaseTransactional();
        #endregion Begin/EndTransaction

        #region Key Locking

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(TTransactionalKey key1, TTransactionalKey key2) where TTransactionalKey : ITransactionalKey => clientSession.CompareKeyHashes(key1, key2);

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(ref TTransactionalKey key1, ref TTransactionalKey key2) where TTransactionalKey : ITransactionalKey => clientSession.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortKeyHashes<TTransactionalKey>(TTransactionalKey[] keys) where TTransactionalKey : ITransactionalKey => clientSession.SortKeyHashes(keys);

        /// <inheritdoc/>
        public void SortKeyHashes<TTransactionalKey>(TTransactionalKey[] keys, int start, int count) where TTransactionalKey : ITransactionalKey => clientSession.SortKeyHashes(keys, start, count);

        /// <inheritdoc/>
        public void Lock<TTransactionalKey>(TTransactionalKey[] keys) where TTransactionalKey : ITransactionalKey => Lock(keys, 0, keys.Length);

        /// <inheritdoc/>
        public void Lock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count)
            where TTransactionalKey : ITransactionalKey
        {
            clientSession.CheckIsAcquiredTransactional();
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected(), "Epoch protection required for TransactionalUnsafeContext.Lock()");
            while (true)
            {
                if (TransactionalContext<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>.DoTransactionalLock(sessionFunctions, clientSession, keys, start, count))
                {
                    break;
                }
                // Suspend and resume epoch protection to give others a fair chance to progress
                clientSession.store.epoch.Suspend();
                clientSession.store.epoch.Resume();
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
            clientSession.CheckIsAcquiredTransactional();
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected(), "Epoch protection required for TransactionalUnsafeContext.Lock()");

            return TransactionalContext<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>.DoTransactionalTryLock(sessionFunctions, clientSession, keys, start, count, timeout, cancellationToken);
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
            clientSession.CheckIsAcquiredTransactional();
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected(), "Epoch protection required for TransactionalUnsafeContext.Lock()");

            return TransactionalContext<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>.DoManualTryPromoteLock(sessionFunctions, clientSession, key, timeout, cancellationToken);
        }

        /// <inheritdoc/>
        public void Unlock<TTransactionalKey>(TTransactionalKey[] keys) where TTransactionalKey : ITransactionalKey => Unlock(keys, 0, keys.Length);

        /// <inheritdoc/>
        public void Unlock<TTransactionalKey>(TTransactionalKey[] keys, int start, int count)
            where TTransactionalKey : ITransactionalKey
        {
            clientSession.CheckIsAcquiredTransactional();
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected(), "Epoch protection required for TransactionalUnsafeContext.Unlock()");

            TransactionalContext<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>.DoManualUnlock(clientSession, keys, start, start + count - 1);
        }

        /// <summary>
        /// The id of the current Tsavorite Session
        /// </summary>
        public int SessionID { get { return clientSession.ctx.sessionID; } }

        #endregion Key Locking

        #region ITsavoriteContext

        /// <inheritdoc/>
        public ClientSession<TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session => clientSession;

        /// <inheritdoc/>
        public long GetKeyHash(SpanByte key) => clientSession.store.GetKeyHash(key);

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePending(sessionFunctions, false, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePendingWithOutputs(sessionFunctions, out completedOutputs, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingAsync(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<TValue, TInput, TOutput, TContext>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingWithOutputsAsync(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(SpanByte key, ref TInput input, ref TOutput output, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(key, ref input, ref output, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(SpanByte key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(key, ref input, ref output, ref readOptions, out _, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(SpanByte key, TInput input, out TOutput output, TContext userContext = default)
        {
            output = default;
            return Read(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(SpanByte key, TInput input, out TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            output = default;
            return Read(key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(SpanByte key, ref TOutput output, TContext userContext = default)
        {
            TInput input = default;
            return Read(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(SpanByte key, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            return Read(key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(SpanByte key, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(SpanByte key, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, ref readOptions, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(SpanByte key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextReadAtAddress(address, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, SpanByte key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextReadAtAddress(address, key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(SpanByte key, TValue desiredValue, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(SpanByte key, TValue desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(SpanByte key, ref TInput input, TValue desiredValue, ref TOutput output, TContext userContext = default)
            => Upsert(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(SpanByte key, ref TInput input, TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => Upsert(key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(SpanByte key, long keyHash, ref TInput input, TValue desiredValue, ref TOutput output, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextUpsert(key, keyHash, ref input, desiredValue, ref output, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(SpanByte key, ref TInput input, TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => Upsert(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(SpanByte key, ref TInput input, TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => Upsert(key, upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(SpanByte key, long keyHash, ref TInput input, TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextUpsert(key, keyHash, ref input, desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(SpanByte key, TInput input, TValue desiredValue, ref TOutput output, TContext userContext = default)
            => Upsert(key, ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(SpanByte key, TInput input, TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => Upsert(key, ref input, desiredValue, ref output, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, ref TInput input, ref TOutput output, TContext userContext = default)
            => RMW(key, ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            => RMW(key, rmwOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => RMW(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => RMW(key, rmwOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRMW(key, keyHash, ref input, ref output, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, TInput input, out TOutput output, TContext userContext = default)
        {
            output = default;
            return RMW(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, TInput input, out TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
        {
            output = default;
            return RMW(key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, ref TInput input, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, TInput input, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(SpanByte key, TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
        {
            TOutput output = default;
            return RMW(key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(SpanByte key, TContext userContext = default)
            => Delete(key, clientSession.store.storeFunctions.GetKeyHashCode64(key), userContext);

        /// <inheritdoc/>
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(SpanByte key, ref DeleteOptions deleteOptions, TContext userContext = default)
            => Delete(key, deleteOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key), userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(SpanByte key, long keyHash, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextDelete<TInput, TOutput, TContext, SessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(
                    key, keyHash, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(SpanByte key)
            => clientSession.UnsafeResetModified(sessionFunctions, key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified(SpanByte key)
            => clientSession.UnsafeIsModified(sessionFunctions, key);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            clientSession.store.InternalRefresh<TInput, TOutput, TContext, SessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(sessionFunctions);
        }

        #endregion ITsavoriteContext
    }
}