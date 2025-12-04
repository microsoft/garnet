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
    /// Tsavorite Operations implementation that allows manual control of epoch management. For advanced use only.
    /// </summary>
    public readonly struct UnsafeContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        : ITsavoriteContext<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>, IUnsafeContext
        where TFunctions : ISessionFunctions<TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        readonly ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession;
        internal readonly SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator> sessionFunctions;

        /// <inheritdoc/>
        public bool IsNull => clientSession is null;

        internal UnsafeContext(ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
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

        #region ITsavoriteContext

        /// <inheritdoc/>
        public ClientSession<TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session => clientSession;

        /// <inheritdoc/>
        public long GetKeyHash(ReadOnlySpan<byte> key) => clientSession.store.GetKeyHash(key);

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
        public Status Read(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(key, ref input, ref output, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(key, ref input, ref output, ref readOptions, out _, userContext, sessionFunctions);
        }

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
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReadWithPrefetch<TBatch>(ref TBatch batch, TContext userContext)
            where TBatch : IReadArgBatch<TInput, TOutput>
#if NET9_0_OR_GREATER
            , allows ref struct
#endif
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            clientSession.store.ContextReadWithPrefetch<TBatch, TInput, TOutput, TContext, SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(ref batch, userContext, sessionFunctions);
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
        public Status ReadAtAddress(long address, ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextReadAtAddress(address, key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
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
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextUpsert(key, keyHash, ref input, srcStringValue: desiredValue, ref output, out _, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, ReadOnlySpan<byte> desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            var keyHash = upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key);
            return clientSession.store.ContextUpsert(key, keyHash, ref input, srcStringValue: desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
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
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextUpsert(key, keyHash, ref input, srcObjectValue: desiredValue, ref output, out _, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ReadOnlySpan<byte> key, ref TInput input, IHeapObject desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            var keyHash = upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key);
            return clientSession.store.ContextUpsert(key, keyHash, ref input, srcObjectValue: desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
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
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, ref TInput input, in TSourceLogRecord diskLogRecord)
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
            => Upsert(inputLogRecord.Key, ref input, in inputLogRecord, ref output, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TSourceLogRecord>(ReadOnlySpan<byte> key, ref TInput input, in TSourceLogRecord inputLogRecord, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            var keyHash = upsertOptions.KeyHash ?? clientSession.store.storeFunctions.GetKeyHashCode64(key);
            return clientSession.store.ContextUpsert(key, keyHash, ref input, inputLogRecord: in inputLogRecord, ref output, out _, userContext, sessionFunctions);
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
        public Status RMW(ReadOnlySpan<byte> key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRMW(key, keyHash, ref input, ref output, out recordMetadata, userContext, sessionFunctions);
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

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ReadOnlySpan<byte> key, long keyHash, TContext userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextDelete<TInput, TOutput, TContext, SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(
                    key, keyHash, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(ReadOnlySpan<byte> key)
            => clientSession.UnsafeResetModified(sessionFunctions, key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified(ReadOnlySpan<byte> key)
            => clientSession.UnsafeIsModified(sessionFunctions, key);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            clientSession.store.InternalRefresh<TInput, TOutput, TContext, SessionFunctionsWrapper<TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(sessionFunctions);
        }
        #endregion ITsavoriteContext
    }
}