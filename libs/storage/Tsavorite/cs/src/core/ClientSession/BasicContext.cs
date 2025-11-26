// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Basic Tsavorite Context implementation.
    /// </summary>
    public readonly struct BasicContext<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        : ITsavoriteContext<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
        where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession;
        internal readonly SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator> sessionFunctions;

        /// <inheritdoc/>
        public bool IsNull => clientSession is null;

        private TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store => clientSession.store;

        internal BasicContext(ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            this.clientSession = clientSession;
            sessionFunctions = new(clientSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread()
            => clientSession.UnsafeResumeThread(sessionFunctions);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeSuspendThread()
            => clientSession.UnsafeSuspendThread();

        #region ITsavoriteContext

        /// <inheritdoc/>
        public ClientSession<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator> Session => clientSession;

        /// <inheritdoc/>
        public long GetKeyHash(TKey key) => clientSession.store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref TKey key) => clientSession.store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
            => clientSession.CompletePending(sessionFunctions, wait, spinWaitForCommit);

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => clientSession.CompletePendingWithOutputs(sessionFunctions, out completedOutputs, wait, spinWaitForCommit);

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
            UnsafeResumeThread();
            try
            {
                return clientSession.store.ContextRead(ref key, ref input, ref output, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
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
            UnsafeResumeThread();
            try
            {
                clientSession.store.ContextReadWithPrefetch<TBatch, TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(ref batch, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
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
            UnsafeResumeThread();
            try
            {
                return store.ContextRead(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextReadAtAddress(address, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TValue desiredValue, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(ref key, store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TValue desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
        {
            TInput input = default;
            TOutput output = default;
            return Upsert(ref key, upsertOptions.KeyHash ?? store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, TContext userContext = default)
            => Upsert(ref key, store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref TKey key, long keyHash, ref TInput input, ref TValue desiredValue, ref TOutput output, TContext userContext = default)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => Upsert(ref key, store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref TKey key, long keyHash, ref TInput input, ref TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
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
            => RMW(ref key, store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            => RMW(ref key, rmwOptions.KeyHash ?? store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        public Status RMW(ref TKey key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            => RMW(ref key, store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        public Status RMW(ref TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            var keyHash = rmwOptions.KeyHash ?? store.storeFunctions.GetKeyHashCode64(ref key);
            return RMW(ref key, keyHash, ref input, ref output, out recordMetadata, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status RMW(ref TKey key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextRMW(ref key, keyHash, ref input, ref output, out recordMetadata, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
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
            => RMW(ref key, ref input, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(TKey key, TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
            => RMW(ref key, ref input, ref rmwOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref TKey key, TContext userContext = default)
            => Delete(ref key, store.storeFunctions.GetKeyHashCode64(ref key), userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref TKey key, ref DeleteOptions deleteOptions, TContext userContext = default)
            => Delete(ref key, deleteOptions.KeyHash ?? store.storeFunctions.GetKeyHashCode64(ref key), userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Delete(ref TKey key, long keyHash, TContext userContext = default)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextDelete<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(ref key, keyHash, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
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
        public void ResetModified(TKey key)
            => clientSession.ResetModified(sessionFunctions, ref key);

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
            => clientSession.Refresh(sessionFunctions);

        #endregion ITsavoriteContext

        /// <summary>
        /// Copy key and value to tail, succeed only if key is known to not exist in between expectedLogicalAddress and tail.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="value"></param>
        /// <param name="currentAddress">LogicalAddress of the record to be copied</param>
        /// <param name="untilAddress">Lower-bound address (addresses are searched from tail (high) to head (low); do not search for "future records" earlier than this)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status CompactionCopyToTail(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, long currentAddress, long untilAddress)
        {
            UnsafeResumeThread();
            try
            {
                return store.CompactionConditionalCopyToTail<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(
                        sessionFunctions, ref key, ref input, ref value, ref output, currentAddress, untilAddress);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Push a scan record to client if key is known to not exist in between expectedLogicalAddress and tail.
        /// </summary>
        /// <param name="scanCursorState">Scan cursor tracking state, from the session on which this scan was initiated</param>
        /// <param name="recordInfo"></param>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="currentAddress">LogicalAddress of the record to be copied</param>
        /// <param name="untilAddress">Lower-bound address (addresses are searched from tail (high) to head (low); do not search for "future records" earlier than this)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ConditionalScanPush(ScanCursorState<TKey, TValue> scanCursorState, RecordInfo recordInfo, ref TKey key, ref TValue value, long currentAddress, long untilAddress, long maxAddress)
        {
            UnsafeResumeThread();
            try
            {
                return store.hlogBase.ConditionalScanPush<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(
                        sessionFunctions, scanCursorState, recordInfo, ref key, ref value, currentAddress, untilAddress, maxAddress);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Checks whether specified record is present in memory (between max(fromAddress, HeadAddress) and tail), including tombstones.
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="logicalAddress">Logical address of record, if found</param>
        /// <param name="fromAddress">Look until this address; if less than HeadAddress, then HeadAddress is used</param>
        /// <returns>Status</returns>
        public Status ContainsKeyInMemory(ref TKey key, out long logicalAddress, long fromAddress = -1)
        {
            UnsafeResumeThread();
            try
            {
                return store.InternalContainsKeyInMemory<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TFunctions, BasicSessionLocker<TKey, TValue, TStoreFunctions, TAllocator>, TStoreFunctions, TAllocator>>(
                        ref key, sessionFunctions, out logicalAddress, fromAddress);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }
    }
}