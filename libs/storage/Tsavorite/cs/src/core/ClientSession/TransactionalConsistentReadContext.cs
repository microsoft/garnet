// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Transactional consistent read context that extends transactionalContext functionality with consistent read protocols.
    /// </summary>
    public readonly struct TransactionalConsistentReadContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        : ITsavoriteContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>, ITransactionalContext
        where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public readonly TransactionalContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> TransactionalContext { get; }

        /// <inheritdoc/>
        public bool IsNull => TransactionalContext.IsNull;

        internal TransactionalConsistentReadContext(ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            TransactionalContext = new TransactionalContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>(clientSession);
        }

        #region Begin/EndTransaction

        /// <inheritdoc/>
        public void BeginTransaction() => TransactionalContext.BeginTransaction();

        /// <inheritdoc/>
        public void LocksAcquired(long txnVersion) => TransactionalContext.LocksAcquired(txnVersion);

        /// <inheritdoc/>
        public void EndTransaction() => TransactionalContext.EndTransaction();

        #endregion Begin/EndTransaction

        #region Key Locking

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(TTransactionalKey key1, TTransactionalKey key2) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.CompareKeyHashes(key1, key2);

        /// <inheritdoc/>
        public int CompareKeyHashes<TTransactionalKey>(ref TTransactionalKey key1, ref TTransactionalKey key2) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortKeyHashes<TTransactionalKey>(Span<TTransactionalKey> keys) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.SortKeyHashes(keys);

        /// <inheritdoc/>
        public void Lock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.Lock(keys);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.TryLock(keys);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys, TimeSpan timeout) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.TryLock(keys, timeout);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys, CancellationToken cancellationToken) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.TryLock(keys, cancellationToken);

        /// <inheritdoc/>
        public bool TryLock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys, TimeSpan timeout, CancellationToken cancellationToken) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.TryLock(keys, timeout, cancellationToken);

        /// <inheritdoc/>
        public bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.TryPromoteLock(key);

        /// <inheritdoc/>
        public bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key, TimeSpan timeout) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.TryPromoteLock(key, timeout);

        /// <inheritdoc/>
        public bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key, CancellationToken cancellationToken) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.TryPromoteLock(key, cancellationToken);

        /// <inheritdoc/>
        public bool TryPromoteLock<TTransactionalKey>(TTransactionalKey key, TimeSpan timeout, CancellationToken cancellationToken) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.TryPromoteLock(key, timeout, cancellationToken);

        /// <inheritdoc/>
        public void Unlock<TTransactionalKey>(ReadOnlySpan<TTransactionalKey> keys) where TTransactionalKey : ITransactionalKey
            => TransactionalContext.Unlock(keys);

        /// <summary>
        /// The id of the current Tsavorite Session
        /// </summary>
        public int SessionID => TransactionalContext.SessionID;

        #endregion Key Locking

        #region ITsavoriteContext/Read

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext = default)
        {
            var callbacks = Session.functions.GetContextCallbacks();
            callbacks.validateKeySequenceNumber.Invoke(PinnedSpanByte.FromPinnedSpan(key));
            var status = TransactionalContext.Read(key, ref input, ref output, userContext);
            if (status.Found)
                callbacks.updateKeySequenceNumber.Invoke();
            return status;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            => Read(key, ref input, ref output, ref readOptions, userContext);

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
            var callbacks = Session.functions.GetContextCallbacks();
            callbacks.validateKeySequenceNumber.Invoke(PinnedSpanByte.FromPinnedSpan(key));
            var status = TransactionalContext.Read(key, ref input, ref output, ref readOptions, out recordMetadata, userContext);
            if (status.Found)
                callbacks.updateKeySequenceNumber.Invoke();
            return status;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow reads from address!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow reads from address!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadWithPrefetch<TBatch>(ref TBatch batch, TContext userContext = default)
            where TBatch : IReadArgBatch<TInput, TOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            // TODO: implement
            throw new NotImplementedException();
        }

        #endregion Read Methods (To be overridden with custom logic)

        #region ITsavoriteContext

        /// <inheritdoc/>
        public ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session => TransactionalContext.Session;

        /// <inheritdoc/>
        public long GetKeyHash(ReadOnlySpan<byte> key) => TransactionalContext.GetKeyHash(key);

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
            => TransactionalContext.CompletePending(wait, spinWaitForCommit);

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => TransactionalContext.CompletePendingWithOutputs(out completedOutputs, wait, spinWaitForCommit);

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => TransactionalContext.CompletePendingAsync(waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => TransactionalContext.CompletePendingWithOutputsAsync(waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> desiredValue, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, IHeapObject desiredValue, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, IHeapObject desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, ref TInput input, in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ReadOnlySpan<byte> key, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ReadOnlySpan<byte> key, ref DeleteOptions deleteOptions, TContext userContext = default)
            => throw new TsavoriteException("Transactional consistent read context does not allow writes!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(ReadOnlySpan<byte> key)
            => throw new TsavoriteException("Transactional consistent read context does not reset ResetModified!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified(ReadOnlySpan<byte> key)
            => throw new TsavoriteException("Transactional consistent read context does not allow IsModified!");

        /// <inheritdoc/>
        public void Refresh()
            => throw new TsavoriteException("Transactional consistent read context does not Refresh!");

        #endregion ITsavoriteContext
    }
}
