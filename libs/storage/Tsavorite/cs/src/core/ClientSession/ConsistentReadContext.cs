// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Context callbacks
    /// </summary>
    /// <param name="ValidateKeySequenceNumber">Callback used to implement prepare phase of the consistent read protocol</param>
    /// <param name="UpdateKeySequenceNumber">Callback used to implement update phase of the consistent read protocol</param>
    public class ConsistentReadContextCallbacks(Action<PinnedSpanByte> ValidateKeySequenceNumber, Action UpdateKeySequenceNumber)
    {
        public readonly Action<PinnedSpanByte> validateKeySequenceNumber = ValidateKeySequenceNumber;
        public readonly Action updateKeySequenceNumber = UpdateKeySequenceNumber;
    }

    /// <summary>
    /// Consistent read context that extends basicContext functionality with consistent read protocols.
    /// </summary>
    public readonly struct ConsistentReadContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        : ITsavoriteContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public readonly BasicContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> BasicContext { get; }

        internal ConsistentReadContext(ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            BasicContext = new BasicContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>(clientSession);
        }

        /// <inheritdoc/>
        public bool IsNull => BasicContext.IsNull;

        /// <inheritdoc/>
        public ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session => BasicContext.Session;

        /// <inheritdoc/>
        public long GetKeyHash(ReadOnlySpan<byte> key) => BasicContext.GetKeyHash(key);

        #region ITsavoriteContext/Read

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext = default)
        {
            var callbacks = Session.functions.GetContextCallbacks();
            callbacks.validateKeySequenceNumber.Invoke(PinnedSpanByte.FromPinnedSpan(key));
            var status = BasicContext.Read(key, ref input, ref output, userContext);
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
            var status = BasicContext.Read(key, ref input, ref output, ref readOptions, out recordMetadata, userContext);
            if (status.Found)
                callbacks.updateKeySequenceNumber.Invoke();
            return status;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow reads from address!");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow reads from address!");

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

        #endregion

        #region ITsavoriteContext

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
        {
            var callbacks = Session.functions.GetContextCallbacks();
            var status = BasicContext.CompletePending(wait, spinWaitForCommit);
            callbacks.updateKeySequenceNumber.Invoke();
            return status;
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            var callbacks = Session.functions.GetContextCallbacks();
            var status = BasicContext.CompletePendingWithOutputs(out completedOutputs, wait, spinWaitForCommit);
            callbacks.updateKeySequenceNumber.Invoke();
            return status;
        }

        /// <inheritdoc/>
        public async ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            var callbacks = Session.functions.GetContextCallbacks();
            await BasicContext.CompletePendingAsync(waitForCommit, token);
            callbacks.updateKeySequenceNumber.Invoke();
        }

        /// <inheritdoc/>
        public async ValueTask<CompletedOutputIterator<TInput, TOutput, TContext>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            var callbacks = Session.functions.GetContextCallbacks();
            var status = BasicContext.CompletePendingWithOutputsAsync(waitForCommit, token);
            callbacks.updateKeySequenceNumber.Invoke();
            return await status;
        }

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> desiredValue, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, ReadOnlySpan<byte> desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, IHeapObject desiredValue, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, IHeapObject desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => BasicContext.Upsert(diskLogRecord);

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, ref TInput input, in TSourceLogRecord diskLogRecord) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(ref TInput input, in TSourceLogRecord diskLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, ref TInput input, in TSourceLogRecord diskLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default) where TSourceLogRecord : ISourceLogRecord
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status RMW(ReadOnlySpan<byte> key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Delete(ReadOnlySpan<byte> key, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public Status Delete(ReadOnlySpan<byte> key, ref DeleteOptions deleteOptions, TContext userContext = default)
            => throw new TsavoriteException("Consistent read context does not allow writes!");

        /// <inheritdoc/>
        public void ResetModified(ReadOnlySpan<byte> key)
            => throw new TsavoriteException("Consistent read context does not reset ResetModified!");

        /// <inheritdoc/>
        public void Refresh()
            => throw new TsavoriteException("Consistent read context does not reset Refresh!");
        #endregion
    }
}
