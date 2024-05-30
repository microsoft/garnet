﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Tsavorite.core
{
    /// <summary>
    /// Tsavorite Operations implementation that allows manual control of record epoch management. For advanced use only.
    /// </summary>
    public readonly struct UnsafeContext<Key, Value, Input, Output, Context, Functions> : ITsavoriteContext<Key, Value, Input, Output, Context>, IUnsafeContext
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        readonly ClientSession<Key, Value, Input, Output, Context, Functions> clientSession;
        internal readonly ClientSession<Key, Value, Input, Output, Context, Functions>.InternalTsavoriteSession TsavoriteSession;

        /// <summary>Indicates whether this struct has been initialized</summary>
        public bool IsNull => clientSession is null;

        internal UnsafeContext(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            this.clientSession = clientSession;
            TsavoriteSession = new ClientSession<Key, Value, Input, Output, Context, Functions>.InternalTsavoriteSession(clientSession);
        }

        #region Begin/EndUnsafe

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BeginUnsafe() => clientSession.UnsafeResumeThread();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EndUnsafe() => clientSession.UnsafeSuspendThread();

        #endregion Begin/EndUnsafe

        #region ITsavoriteContext

        /// <inheritdoc/>
        public long GetKeyHash(Key key) => clientSession.store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref Key key) => clientSession.store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePending(TsavoriteSession, false, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePendingWithOutputs(TsavoriteSession, out completedOutputs, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingAsync(waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => clientSession.CompletePendingWithOutputsAsync(waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(ref key, ref input, ref output, userContext, TsavoriteSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(ref key, ref input, ref output, ref readOptions, out _, userContext, TsavoriteSession);
        }

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
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRead(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, TsavoriteSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextReadAtAddress(address, ref input, ref output, ref readOptions, out recordMetadata, userContext, TsavoriteSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, TsavoriteSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            ReadOptions readOptions = default;
            return clientSession.store.ReadAsync(TsavoriteSession, ref key, ref input, ref readOptions, userContext, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, CancellationToken token = default)
            => ReadAsync(ref key, ref input, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, ref ReadOptions readOptions, Context context = default, CancellationToken token = default)
            => ReadAsync(ref key, ref input, ref readOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, CancellationToken token = default)
        {
            ReadOptions readOptions = default;
            return ReadAsync(ref key, ref readOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref ReadOptions readOptions, Context userContext = default, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            Input input = default;
            return clientSession.store.ReadAsync(TsavoriteSession, ref key, ref input, ref readOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, CancellationToken token = default) =>
            ReadAsync(ref key, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, ref ReadOptions readOptions, Context context = default, CancellationToken token = default)
            => ReadAsync(ref key, ref readOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, ref ReadOptions readOptions,
                                                                                                 Context userContext = default, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ReadAsync(TsavoriteSession, ref key, ref input, ref readOptions, userContext, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            Key key = default;
            return clientSession.store.ReadAtAddressAsync(TsavoriteSession, address, ref key, ref input, ref readOptions, userContext, cancellationToken, noKey: true);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Key key, ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ReadAtAddressAsync(TsavoriteSession, address, ref key, ref input, ref readOptions, userContext, cancellationToken, noKey: false);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out _, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default)
            => Upsert(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, userContext, TsavoriteSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
            => Upsert(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => Upsert(ref key, upsertOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, out recordMetadata, userContext, TsavoriteSession);
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
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default)
            => Upsert(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, Context userContext = default)
            => Upsert(ref key, ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, Context userContext = default, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, CancellationToken token = default)
        {
            UpsertOptions upsertOptions = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.UpsertAsync<Input, Output, Context, ClientSession<Key, Value, Input, Output, Context, Functions>.InternalTsavoriteSession>(
                    TsavoriteSession, ref key, ref input, ref desiredValue, ref upsertOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, Context userContext = default, CancellationToken token = default)
            => UpsertAsync(ref key, ref desiredValue, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
            => UpsertAsync(ref key, ref desiredValue, ref upsertOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, Context userContext = default, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default)
            => RMW(ref key, ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, Context userContext = default)
            => RMW(ref key, rmwOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
            => RMW(ref key, clientSession.store.comparer.GetHashCode64(ref key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, Context userContext = default)
            => RMW(ref key, rmwOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, long keyHash, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextRMW(ref key, keyHash, ref input, ref output, out recordMetadata, userContext, TsavoriteSession);
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
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, CancellationToken token = default)
        {
            RMWOptions rmwOptions = default;
            return RMWAsync(ref key, ref input, ref rmwOptions, context, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context context = default, CancellationToken token = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.RmwAsync<Input, Output, Context, ClientSession<Key, Value, Input, Output, Context, Functions>.InternalTsavoriteSession>(
                    TsavoriteSession, ref key, ref input, ref rmwOptions, context, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, CancellationToken token = default)
            => RMWAsync(ref key, ref input, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, ref RMWOptions rmwOptions, Context context = default, CancellationToken token = default)
            => RMWAsync(ref key, ref input, ref rmwOptions, context, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default)
            => Delete(ref key, clientSession.store.comparer.GetHashCode64(ref key), userContext);

        /// <inheritdoc/>
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default)
            => Delete(ref key, deleteOptions.KeyHash ?? clientSession.store.comparer.GetHashCode64(ref key), userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, long keyHash, Context userContext = default)
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.ContextDelete<Input, Output, Context, ClientSession<Key, Value, Input, Output, Context, Functions>.InternalTsavoriteSession>(
                    ref key, keyHash, userContext, TsavoriteSession);
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
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, CancellationToken token = default)
        {
            DeleteOptions deleteOptions = default;
            return DeleteAsync(ref key, ref deleteOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.store.epoch.ThisInstanceProtected());
            return clientSession.store.DeleteAsync<Input, Output, Context, ClientSession<Key, Value, Input, Output, Context, Functions>.InternalTsavoriteSession>(
                    TsavoriteSession, ref key, ref deleteOptions, userContext, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, CancellationToken token = default)
            => DeleteAsync(ref key, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, ref DeleteOptions deleteOptions, Context userContext = default, CancellationToken token = default)
            => DeleteAsync(ref key, ref deleteOptions, userContext, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(ref Key key)
            => clientSession.UnsafeResetModified(ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified(Key key)
            => clientSession.UnsafeIsModified(ref key);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(clientSession.store.epoch.ThisInstanceProtected());
            clientSession.store.InternalRefresh<Input, Output, Context, ClientSession<Key, Value, Input, Output, Context, Functions>.InternalTsavoriteSession>(TsavoriteSession);
        }
        #endregion ITsavoriteContext
    }
}