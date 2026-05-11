// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Consistent read context that extends basicContext functionality with consistent read protocols.
    /// </summary>
    public readonly struct ConsistentReadContext<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        : ITsavoriteContext<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        where TKey : IKey
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public readonly BasicContext<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> BasicContext { get; }

        /// <inheritdoc/>
        public long GetKeyHash<TOpKey>(TOpKey key)
            where TOpKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            => Session.store.GetKeyHash(key);

        internal ConsistentReadContext(ClientSession<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            BasicContext = new BasicContext<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>(clientSession);
        }

        /// <inheritdoc/>
        public bool IsNull => BasicContext.IsNull;

        /// <inheritdoc/>
        public ClientSession<TKey, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session => BasicContext.Session;

        #region ITsavoriteContext/Read

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
        {
            var hash = GetKeyHash(key);
            Session.functions.BeforeConsistentReadCallback(hash);
            var status = BasicContext.Read(key, ref input, ref output, userContext);
            Session.functions.AfterConsistentReadKeyCallback();
            return status;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            => Read(key, ref input, ref output, ref readOptions, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TOutput output, TContext userContext = default)
        {
            TInput input = default;
            return Read(key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            return Read(key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(TKey key, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read(TKey key, ref ReadOptions readOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return (Read(key, ref input, ref output, ref readOptions, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            var hash = GetKeyHash(key);
            Session.functions.BeforeConsistentReadCallback(hash);
            var status = BasicContext.Read(key, ref input, ref output, ref readOptions, out recordMetadata, userContext);
            Session.functions.AfterConsistentReadKeyCallback();
            return status;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow reads from address!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow reads from address!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadWithPrefetch<TBatch>(ref TBatch batch, TContext userContext = default)
            where TBatch : IReadArgBatch<TKey, TInput, TOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            do
            {
                Thread.Yield();
                Session.functions.BeforeConsistentReadKeyBatchCallback(batch.Parameters);
                BasicContext.ReadWithPrefetch(ref batch, userContext);
            } while (!Session.functions.AfterConsistentReadKeyBatchCallback(batch.Count));
        }

        #endregion

        #region ITsavoriteContext

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
        {
            var status = BasicContext.CompletePending(wait, spinWaitForCommit);
            Session.functions.AfterConsistentReadKeyCallback();
            return status;
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            var status = BasicContext.CompletePendingWithOutputs(out completedOutputs, wait, spinWaitForCommit);
            Session.functions.AfterConsistentReadKeyCallback();
            return status;
        }

        /// <inheritdoc/>
        public async ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            await BasicContext.CompletePendingAsync(waitForCommit, token).ConfigureAwait(false);
            Session.functions.AfterConsistentReadKeyCallback();
        }

        /// <inheritdoc/>
        public async ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            var status = BasicContext.CompletePendingWithOutputsAsync(waitForCommit, token);
            Session.functions.AfterConsistentReadKeyCallback();
            return await status.ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public Status Upsert(TKey key, ReadOnlySpan<byte> desiredValue, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ReadOnlySpan<byte> desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, IHeapObject desiredValue, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, IHeapObject desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, IHeapObject desiredValue, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(TKey key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => BasicContext.Upsert(diskLogRecord);

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(TKey key, in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(TKey key, ref TInput input, in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(ref TInput input, in TSourceLogRecord diskLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(TKey key, ref TInput input, in TSourceLogRecord diskLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        public Status Upsert<TOpKey, TSourceLogRecord>(TOpKey key, in TSourceLogRecord diskLogRecord)
            where TOpKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord => throw new TsavoriteException("Consistent read context does not allow writes!");
        public Status Upsert<TOpKey, TSourceLogRecord>(TOpKey key, ref TInput input, in TSourceLogRecord diskLogRecord)
            where TOpKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord => throw new TsavoriteException("Consistent read context does not allow writes!");
        public Status Upsert<TOpKey, TSourceLogRecord>(TOpKey key, ref TInput input, in TSourceLogRecord diskLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TOpKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSourceLogRecord : ISourceLogRecord => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(TKey key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Delete(TKey key, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Delete(TKey key, ref DeleteOptions deleteOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public void ResetModified(TKey key)
            => throw new TsavoriteException("Consistent read context does not reset ResetModified!");

        /// <inheritdoc/>
        public void Refresh()
            => throw new TsavoriteException("Consistent read context does not reset Refresh!");
        #endregion
    }
}