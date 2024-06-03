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
    public readonly struct BasicContext<Key, Value, Input, Output, Context, Functions> : ITsavoriteContext<Key, Value, Input, Output, Context>
        where Functions : ISessionFunctions<Key, Value, Input, Output, Context>
    {
        readonly ClientSession<Key, Value, Input, Output, Context, Functions> clientSession;
        internal readonly SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, BasicSessionLocker<Key, Value>> sessionFunctions;

        /// <summary>Indicates whether this struct has been initialized</summary>
        public bool IsNull => clientSession is null;

        private TsavoriteKV<Key, Value> store => clientSession.store;

        internal BasicContext(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
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
        public long GetKeyHash(Key key) => clientSession.store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref Key key) => clientSession.store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
            => clientSession.CompletePending(sessionFunctions, wait, spinWaitForCommit);

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => clientSession.CompletePendingWithOutputs(sessionFunctions, out completedOutputs, wait, spinWaitForCommit);

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
        public Status ReadAtAddress(long address, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
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
        public Status ReadAtAddress(long address, ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
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
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, upsertOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default)
            => Upsert(ref key, store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default)
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
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
            => Upsert(ref key, store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
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
        public Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default)
            => RMW(ref key, store.comparer.GetHashCode64(ref key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, Context userContext = default)
            => RMW(ref key, rmwOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        public Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
            => RMW(ref key, store.comparer.GetHashCode64(ref key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, Context userContext = default)
        {
            var keyHash = rmwOptions.KeyHash ?? store.comparer.GetHashCode64(ref key);
            return RMW(ref key, keyHash, ref input, ref output, out recordMetadata, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status RMW(ref Key key, long keyHash, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
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
            => RMW(ref key, ref input, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, ref RMWOptions rmwOptions, Context userContext = default)
            => RMW(ref key, ref input, ref rmwOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default)
            => Delete(ref key, store.comparer.GetHashCode64(ref key), userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default)
            => Delete(ref key, deleteOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Delete(ref Key key, long keyHash, Context userContext = default)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextDelete<Input, Output, Context, SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, BasicSessionLocker<Key, Value>>>(ref key, keyHash, userContext, sessionFunctions);
            }
            finally
            {
                UnsafeSuspendThread();
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
        public void ResetModified(Key key)
            => clientSession.ResetModified(sessionFunctions, ref key);

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
            => clientSession.Refresh(sessionFunctions);

        #endregion ITsavoriteContext

        /// <summary>
        /// Copy key and value to tail, succeed only if key is known to not exist in between expectedLogicalAddress and tail.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="value"></param>
        /// <param name="untilAddress">Lower-bound address (addresses are searched from tail (high) to head (low); do not search for "future records" earlier than this)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status CompactionCopyToTail(ref Key key, ref Input input, ref Value value, ref Output output, long untilAddress)
        {
            UnsafeResumeThread();
            try
            {
                return store.CompactionConditionalCopyToTail<Input, Output, Context, SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, BasicSessionLocker<Key, Value>>>(
                        sessionFunctions, ref key, ref input, ref value, ref output, untilAddress);
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
        /// <param name="untilAddress">Lower-bound address (addresses are searched from tail (high) to head (low); do not search for "future records" earlier than this)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ConditionalScanPush(ScanCursorState<Key, Value> scanCursorState, RecordInfo recordInfo, ref Key key, ref Value value, long untilAddress)
        {
            UnsafeResumeThread();
            try
            {
                return store.hlog.ConditionalScanPush<Input, Output, Context, SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, BasicSessionLocker<Key, Value>>>(
                        sessionFunctions, scanCursorState, recordInfo, ref key, ref value, untilAddress);
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
        internal Status ContainsKeyInMemory(ref Key key, out long logicalAddress, long fromAddress = -1)
        {
            UnsafeResumeThread();
            try
            {
                return store.InternalContainsKeyInMemory<Input, Output, Context, SessionFunctionsWrapper<Key, Value, Input, Output, Context, Functions, BasicSessionLocker<Key, Value>>>(
                        ref key, sessionFunctions, out logicalAddress, fromAddress);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }
    }
}