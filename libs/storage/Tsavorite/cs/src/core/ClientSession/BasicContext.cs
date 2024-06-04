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
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        readonly ClientSession<Key, Value, Input, Output, Context, Functions> clientSession;

        /// <summary>Indicates whether this struct has been initialized</summary>
        public bool IsNull => clientSession is null;

        internal BasicContext(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            this.clientSession = clientSession;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread()
            => clientSession.UnsafeResumeThread();

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
            => clientSession.CompletePending(wait, spinWaitForCommit);

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => clientSession.CompletePendingWithOutputs(out completedOutputs, wait, spinWaitForCommit);

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingAsync(waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingWithOutputsAsync(waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default)
            => clientSession.Read(ref key, ref input, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default)
            => clientSession.Read(ref key, ref input, ref output, ref readOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, Context userContext = default)
            => clientSession.Read(key, input, out output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, ref ReadOptions readOptions, Context userContext = default)
            => clientSession.Read(key, input, out output, ref readOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, Context userContext = default)
            => clientSession.Read(ref key, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, ref ReadOptions readOptions, Context userContext = default)
            => clientSession.Read(ref key, ref output, ref readOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, Context userContext = default)
            => clientSession.Read(key, out output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, ref ReadOptions readOptions, Context userContext = default)
            => clientSession.Read(key, out output, ref readOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, Context userContext = default)
            => clientSession.Read(key, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, ref ReadOptions readOptions, Context userContext = default)
            => clientSession.Read(key, ref readOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => clientSession.Read(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => clientSession.ReadAtAddress(address, ref input, ref output, ref readOptions, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => clientSession.ReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, CancellationToken cancellationToken = default)
            => clientSession.ReadAsync(ref key, ref input, userContext, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, CancellationToken token = default)
            => clientSession.ReadAsync(key, input, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, ref ReadOptions readOptions, Context context = default, CancellationToken token = default)
            => clientSession.ReadAsync(key, input, ref readOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, CancellationToken token = default)
            => clientSession.ReadAsync(ref key, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref ReadOptions readOptions, Context userContext = default, CancellationToken token = default)
            => clientSession.ReadAsync(ref key, ref readOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, CancellationToken token = default)
            => clientSession.ReadAsync(key, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, ref ReadOptions readOptions, Context context = default, CancellationToken token = default)
            => clientSession.ReadAsync(key, ref readOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, ref ReadOptions readOptions, Context userContext = default, CancellationToken cancellationToken = default)
            => clientSession.ReadAsync(ref key, ref input, ref readOptions, userContext, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Input input, ref ReadOptions readOptions, Context userContext = default, CancellationToken cancellationToken = default)
            => clientSession.ReadAtAddressAsync(address, ref input, ref readOptions, userContext, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Key key, ref Input input, ref ReadOptions readOptions, Context userContext = default, CancellationToken cancellationToken = default)
            => clientSession.ReadAtAddressAsync(address, ref key, ref input, ref readOptions, userContext, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default)
            => clientSession.Upsert(ref key, ref desiredValue, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default)
            => clientSession.Upsert(ref key, ref desiredValue, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default)
            => clientSession.Upsert(ref key, ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default)
            => clientSession.Upsert(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
            => clientSession.Upsert(ref key, ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => clientSession.Upsert(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, Context userContext = default)
            => clientSession.Upsert(key, desiredValue, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default)
            => clientSession.Upsert(key, desiredValue, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, Context userContext = default)
            => clientSession.Upsert(key, input, desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default)
            => clientSession.Upsert(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, Context userContext = default, CancellationToken token = default)
            => clientSession.UpsertAsync(ref key, ref desiredValue, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
            => clientSession.UpsertAsync(ref key, ref desiredValue, ref upsertOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
            => clientSession.UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, CancellationToken token = default)
            => clientSession.UpsertAsync(ref key, ref input, ref desiredValue, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, Context userContext = default, CancellationToken token = default)
            => clientSession.UpsertAsync(key, desiredValue, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
            => clientSession.UpsertAsync(key, desiredValue, ref upsertOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, Context userContext = default, CancellationToken token = default)
            => clientSession.UpsertAsync(key, input, desiredValue, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
            => clientSession.UpsertAsync(key, input, desiredValue, ref upsertOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default)
            => clientSession.RMW(ref key, ref input, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, Context userContext = default)
            => clientSession.RMW(ref key, ref input, ref output, ref rmwOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => clientSession.RMW(ref key, ref input, ref output, ref rmwOptions, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
            => clientSession.RMW(ref key, ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, out Output output, Context userContext = default)
            => clientSession.RMW(key, input, out output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, out Output output, ref RMWOptions rmwOptions, Context userContext = default)
            => clientSession.RMW(key, input, out output, ref rmwOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, Context userContext = default)
            => clientSession.RMW(ref key, ref input, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context userContext = default)
            => clientSession.RMW(ref key, ref input, ref rmwOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, Context userContext = default)
            => clientSession.RMW(key, input, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, ref RMWOptions rmwOptions, Context userContext = default)
            => clientSession.RMW(key, input, ref rmwOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, CancellationToken token = default)
            => clientSession.RMWAsync(ref key, ref input, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context context = default, CancellationToken token = default)
            => clientSession.RMWAsync(ref key, ref input, ref rmwOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, CancellationToken token = default)
            => clientSession.RMWAsync(key, input, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, ref RMWOptions rmwOptions, Context context = default, CancellationToken token = default)
            => clientSession.RMWAsync(key, input, ref rmwOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default)
            => clientSession.Delete(ref key, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default)
            => clientSession.Delete(ref key, ref deleteOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, Context userContext = default)
            => clientSession.Delete(key, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, ref DeleteOptions deleteOptions, Context userContext = default)
            => clientSession.Delete(key, ref deleteOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, CancellationToken token = default)
            => clientSession.DeleteAsync(ref key, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default, CancellationToken token = default)
            => clientSession.DeleteAsync(ref key, ref deleteOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, CancellationToken token = default)
            => clientSession.DeleteAsync(key, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, ref DeleteOptions deleteOptions, Context userContext = default, CancellationToken token = default)
            => clientSession.DeleteAsync(key, ref deleteOptions, userContext, token);

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
            => clientSession.Refresh();

        #endregion ITsavoriteContext
    }
}