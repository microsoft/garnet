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
    public readonly struct TransactionalUnsafeContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        : ITsavoriteContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>, ITransactionalContext, IUnsafeContext
        where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        readonly ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession;
        readonly SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator> sessionFunctions;

        /// <inheritdoc/>
        public bool IsNull => clientSession is null;

        internal TransactionalUnsafeContext(ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
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
        public void LocksAcquired(long txnVersion) => clientSession.LocksAcquired(sessionFunctions, txnVersion);

        /// <inheritdoc/>
        public void EndTransaction() => clientSession.ReleaseTransactional(sessionFunctions);
        #endregion Begin/EndTransaction

        #region Key Locking

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(TTransactionalKey key1, TTransactionalKey key2) where TTransactionalKey : ITransactionalKey => clientSession.CompareKeyHashes(key1, key2);

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(ref TTransactionalKey key1, ref TTransactionalKey key2) where TTransactionalKey : ITransactionalKey => clientSession.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortKeyHashes<TTransactionalKey>(Span<TTransactionalKey> keys) where TTransactionalKey : ITransactionalKey => clientSession.SortKeyHashes(keys);

        /// <inheritdoc/>
        public void Lock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys)
            where TTransactionalKey : ITransactionalKey
        {
            clientSession.CheckIsAcquiredTransactional(sessionFunctions);
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected(), "Epoch protection required for TransactionalUnsafeContext.Lock()");
            while (true)
            {
                if (TransactionalContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>.DoTransactionalLock(sessionFunctions, clientSession, keys))
                {
                    break;
                }
                // Suspend and resume epoch protection to give others a fair chance to progress
                clientSession.store.epoch.Suspend();
                clientSession.store.epoch.Resume();
            }
        }

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys)
            where TTransactionalKey : ITransactionalKey
            => TryLock(keys, Timeout.InfiniteTimeSpan, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys, TimeSpan timeout)
            where TTransactionalKey : ITransactionalKey
            => TryLock(keys, timeout, cancellationToken: default);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey
            => TryLock(keys, Timeout.InfiniteTimeSpan, cancellationToken);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys, TimeSpan timeout, CancellationToken cancellationToken)
            where TTransactionalKey : ITransactionalKey
        {
            clientSession.CheckIsAcquiredTransactional(sessionFunctions);
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected(), "Epoch protection required for TransactionalUnsafeContext.TryLock()");

            return TransactionalContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>.DoTransactionalTryLock(sessionFunctions, clientSession, keys, timeout, cancellationToken);
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
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected(), "Epoch protection required for TransactionalUnsafeContext.TryPromoteLock()");

            return TransactionalContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>.DoManualTryPromoteLock(sessionFunctions, clientSession, key, timeout, cancellationToken);
        }

        /// <inheritdoc/>
        public void Unlock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys)
            where TTransactionalKey : ITransactionalKey
        {
            clientSession.CheckIsAcquiredTransactional(sessionFunctions);
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected(), "Epoch protection required for TransactionalUnsafeContext.Unlock()");

            TransactionalContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>.DoTransactionalUnlock(clientSession, keys);
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
        public long GetKeyHash<TKey>(TKey key)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => clientSession.store.GetKeyHash(key);

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePending(sessionFunctions, false, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePendingWithOutputs(sessionFunctions, out completedOutputs, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingAsync(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingWithOutputsAsync(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKey>(TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(key, ref input, ref output, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKey>(TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(key, ref input, ref output, ref readOptions, out _, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKey>(TKey key, ref TOutput output, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TInput input = default;
            return Read(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKey>(TKey key, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TInput input = default;
            return Read(key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read<TKey>(TKey key, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read<TKey>(TKey key, ref ReadOptions readOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, ref readOptions, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKey>(TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
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
        public void ReadWithPrefetch<TKey, TBatch>(ref TBatch batch, TContext userContext)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TBatch : IReadArgBatch<TKey, TInput, TOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            clientSession.store.ContextReadWithPrefetch<TKey, TBatch, TInput, TOutput, TContext, SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(ref batch, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress<TKey>(long address, TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextReadAtAddress(address, key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, ReadOnlySpan<byte> desiredValue, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, key.GetKeyHashCode64(), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, ReadOnlySpan<byte> desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, upsertOptions.KeyHash ?? key.GetKeyHashCode64(), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => Upsert(key, key.GetKeyHashCode64(), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => Upsert(key, upsertOptions.KeyHash ?? key.GetKeyHashCode64(), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert<TKey>(TKey key, long keyHash, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextUpsert(key, keyHash, ref input, srcStringValue: desiredValue, ref output, out _, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            var keyHash = upsertOptions.KeyHash ?? key.GetKeyHashCode64();
            return clientSession.store.ContextUpsert(key, keyHash, ref input, srcStringValue: desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, IHeapObject desiredValue, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, key.GetKeyHashCode64(), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, IHeapObject desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(key, upsertOptions.KeyHash ?? key.GetKeyHashCode64(), ref input, desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, ref TInput input, IHeapObject desiredValue, ref TOutput output, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => Upsert(key, key.GetKeyHashCode64(), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => Upsert(key, upsertOptions.KeyHash ?? key.GetKeyHashCode64(), ref input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert<TKey>(TKey key, long keyHash, ref TInput input, IHeapObject desiredValue, ref TOutput output, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextUpsert(key, keyHash, ref input, srcObjectValue: desiredValue, ref output, out _, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey>(TKey key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            var keyHash = upsertOptions.KeyHash ?? key.GetKeyHashCode64();
            return clientSession.store.ContextUpsert(key, keyHash, ref input, srcObjectValue: desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(in TSourceLogRecord diskLogRecord)
            where TSourceLogRecord : ISourceLogRecord
#if NET9_0_OR_GREATER
            => Upsert(new SpanByteKey(diskLogRecord.Key), in diskLogRecord);
#else
            => Upsert(PinnedSpanByte.FromPinnedSpan(diskLogRecord.Key), in diskLogRecord);
#endif

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey, TSourceLogRecord>(TKey key, in TSourceLogRecord diskLogRecord)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord
        {
            TInput input = default;
            TOutput output = default;
            UpsertOptions upsertOptions = default;
            return Upsert(key, ref input, in diskLogRecord, ref output, ref upsertOptions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey, TSourceLogRecord>(TKey key, ref TInput input, in TSourceLogRecord diskLogRecord)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord
        {
            TOutput output = default;
            UpsertOptions upsertOptions = default;
            return Upsert(key, ref input, in diskLogRecord, ref output, ref upsertOptions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TSourceLogRecord : ISourceLogRecord
#if NET9_0_OR_GREATER
            => Upsert(new SpanByteKey(inputLogRecord.Key), ref input, in inputLogRecord, ref output, ref upsertOptions, userContext);
#else
            => Upsert(PinnedSpanByte.FromPinnedSpan(inputLogRecord.Key), ref input, in inputLogRecord, ref output, ref upsertOptions, userContext);
#endif

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKey, TSourceLogRecord>(TKey key, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            var keyHash = upsertOptions.KeyHash ?? key.GetKeyHashCode64();
            return clientSession.store.ContextUpsert(key, keyHash, ref input, inputLogRecord: in inputLogRecord, ref output, out _, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKey>(TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => RMW(key, ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKey>(TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => RMW(key, rmwOptions.KeyHash ?? key.GetKeyHashCode64(), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKey>(TKey key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => RMW(key, key.GetKeyHashCode64(), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKey>(TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => RMW(key, rmwOptions.KeyHash ?? key.GetKeyHashCode64(), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKey>(TKey key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRMW(key, keyHash, ref input, ref output, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKey>(TKey key, ref TInput input, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TOutput output = default;
            return RMW(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKey>(TKey key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            TOutput output = default;
            return RMW(key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKey>(TKey key, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => Delete(key, key.GetKeyHashCode64(), userContext);

        /// <inheritdoc/>
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKey>(TKey key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
            => Delete(key, deleteOptions.KeyHash ?? key.GetKeyHashCode64(), userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKey>(TKey key, long keyHash, TContext userContext = default)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextDelete<TKey, TInput, TOutput, TContext, SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(
                    key, keyHash, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified<TKey>(TKey key)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
            => clientSession.UnsafeResetModified(sessionFunctions, new SpanByteKey(key.KeyBytes));
#else
        {
            clientSession.UnsafeResetModified(sessionFunctions, PinnedSpanByte.FromPinnedSpan(key.KeyBytes));
        }
#endif

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified<TKey>(TKey key)
            where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
            => clientSession.UnsafeIsModified(sessionFunctions, new SpanByteKey(key.KeyBytes));
#else
        {
            return clientSession.UnsafeIsModified(sessionFunctions, PinnedSpanByte.FromPinnedSpan(key.KeyBytes));
        }
#endif

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            clientSession.store.InternalRefresh<TInput, TOutput, TContext, SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, TransactionalSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(sessionFunctions);
        }

        #endregion ITsavoriteContext
    }
}