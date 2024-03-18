// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Thread-independent session interface to Tsavorite
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    public sealed class ClientSession<Key, Value, Input, Output, Context, Functions> : IClientSession, ITsavoriteContext<Key, Value, Input, Output, Context>, IDisposable
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal readonly TsavoriteKV<Key, Value> store;

        internal readonly TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx;
        internal CommitPoint LatestCommitPoint;

        internal readonly Functions functions;

        internal CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs;

        internal readonly InternalTsavoriteSession TsavoriteSession;

        readonly UnsafeContext<Key, Value, Input, Output, Context, Functions> uContext;
        readonly LockableUnsafeContext<Key, Value, Input, Output, Context, Functions> luContext;
        readonly LockableContext<Key, Value, Input, Output, Context, Functions> lContext;
        readonly BasicContext<Key, Value, Input, Output, Context, Functions> bContext;

        internal const string NotAsyncSessionErr = "Session does not support async operations";

        readonly ILoggerFactory loggerFactory;
        readonly ILogger logger;

        internal ulong TotalLockCount => sharedLockCount + exclusiveLockCount;
        internal ulong sharedLockCount;
        internal ulong exclusiveLockCount;

        bool isAcquiredLockable;

        ScanCursorState<Key, Value> scanCursorState;

        internal void AcquireLockable()
        {
            CheckIsNotAcquiredLockable();

            while (true)
            {
                // Checkpoints cannot complete while we have active locking sessions.
                while (IsInPreparePhase())
                {
                    if (store.epoch.ThisInstanceProtected())
                        store.InternalRefresh<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession);
                    Thread.Yield();
                }

                store.IncrementNumLockingSessions();
                isAcquiredLockable = true;

                if (!IsInPreparePhase())
                    break;
                InternalReleaseLockable();
                Thread.Yield();
            }
        }

        internal void ReleaseLockable()
        {
            CheckIsAcquiredLockable();
            if (TotalLockCount > 0)
                throw new TsavoriteException($"EndLockable called with locks held: {sharedLockCount} shared locks, {exclusiveLockCount} exclusive locks");
            InternalReleaseLockable();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InternalReleaseLockable()
        {
            isAcquiredLockable = false;
            store.DecrementNumLockingSessions();
        }

        internal void CheckIsAcquiredLockable()
        {
            if (!isAcquiredLockable)
                throw new TsavoriteException("Lockable method call when BeginLockable has not been called");
        }

        void CheckIsNotAcquiredLockable()
        {
            if (isAcquiredLockable)
                throw new TsavoriteException("BeginLockable cannot be called twice (call EndLockable first)");
        }

        internal ClientSession(
            TsavoriteKV<Key, Value> store,
            TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> ctx,
            Functions functions,
            ILoggerFactory loggerFactory = null)
        {
            bContext = new(this);
            uContext = new(this);

            if (store.LockTable.IsEnabled)
            {
                lContext = new(this);
                luContext = new(this);
            }

            this.loggerFactory = loggerFactory;
            logger = loggerFactory?.CreateLogger($"ClientSession-{GetHashCode():X8}");
            this.store = store;
            this.ctx = ctx;
            this.functions = functions;
            LatestCommitPoint = new CommitPoint { UntilSerialNo = -1, ExcludedSerialNos = null };
            TsavoriteSession = new InternalTsavoriteSession(this);
        }

        /// <summary>
        /// Get session ID
        /// </summary>
        public int ID { get { return ctx.sessionID; } }

        /// <summary>
        /// Get session name
        /// </summary>
        public string Name { get { return ctx.sessionName; } }

        /// <summary>
        /// Next sequential serial no for session (current serial no + 1)
        /// </summary>
        public long NextSerialNo => ctx.serialNum + 1;

        /// <summary>
        /// Current serial no for session
        /// </summary>
        public long SerialNo => ctx.serialNum;

        /// <summary>
        /// Current version number of the session
        /// </summary>
        public long Version => ctx.version;

        /// <summary>
        /// Dispose session
        /// </summary>
        public void Dispose()
        {
            completedOutputs?.Dispose();
            CompletePending(true);
            store.DisposeClientSession(ID, ctx.phase);
        }

        /// <summary>
        /// Return a new interface to Tsavorite operations that supports manual epoch control.
        /// </summary>
        public UnsafeContext<Key, Value, Input, Output, Context, Functions> UnsafeContext => uContext;

        /// <summary>
        /// Return a new interface to Tsavorite operations that supports manual locking and epoch control.
        /// </summary>
        public LockableUnsafeContext<Key, Value, Input, Output, Context, Functions> LockableUnsafeContext
        {
            get
            {
                if (!store.LockTable.IsEnabled)
                    throw new TsavoriteException($"LockableUnsafeContext requires {nameof(ConcurrencyControlMode.LockTable)}");
                return luContext;
            }
        }

        /// <summary>
        /// Return a session wrapper that supports manual locking.
        /// </summary>
        public LockableContext<Key, Value, Input, Output, Context, Functions> LockableContext
        {
            get
            {
                if (!store.LockTable.IsEnabled)
                    throw new TsavoriteException($"LockableContext requires {nameof(ConcurrencyControlMode.LockTable)}");
                return lContext;
            }
        }

        /// <summary>
        /// Return a session wrapper struct that passes through to client session
        /// </summary>
        public BasicContext<Key, Value, Input, Output, Context, Functions> BasicContext => bContext;

        #region ITsavoriteContext

        /// <inheritdoc/>
        public long GetKeyHash(Key key) => store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref Key key) => store.GetKeyHash(ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextRead(ref key, ref input, ref output, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
            => Read(ref key, ref input, ref output, ref readOptions, out _, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, ref readOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, userContext, serialNo), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, ref readOptions, userContext, serialNo), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextRead(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextReadAtAddress(address, ref input, ref output, ref readOptions, out recordMetadata, userContext, serialNo, TsavoriteSession);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextReadAtAddress(address, ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, serialNo, TsavoriteSession);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            ReadOptions readOptions = default;
            return store.ReadAsync(TsavoriteSession, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
            => store.ReadAsync(TsavoriteSession, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
            => ReadAsync(ref key, ref input, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, ref ReadOptions readOptions, Context context = default, long serialNo = 0, CancellationToken token = default)
            => store.ReadAsync(TsavoriteSession, ref key, ref input, ref readOptions, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            ReadOptions readOptions = default;
            return ReadAsync(ref key, ref readOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return store.ReadAsync(TsavoriteSession, ref key, ref input, ref readOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default)
            => ReadAsync(ref key, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, ref ReadOptions readOptions, Context context = default, long serialNo = 0, CancellationToken token = default)
            => ReadAsync(ref key, ref readOptions, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Key key = default;
            return store.ReadAtAddressAsync(TsavoriteSession, address, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken, noKey: true);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Key key, ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
            => store.ReadAtAddressAsync(TsavoriteSession, address, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken, noKey: false);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, upsertOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, upsertOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, upsertOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert(ref Key key, long keyHash, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextUpsert(ref key, keyHash, ref input, ref desiredValue, ref output, out recordMetadata, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref desiredValue, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref desiredValue, ref upsertOptions, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref input, ref desiredValue, ref output, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            UpsertOptions upsertOptions = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => store.UpsertAsync<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, ref key, ref input, ref desiredValue, ref upsertOptions, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref desiredValue, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref desiredValue, ref upsertOptions, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, ref UpsertOptions upsertOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, ref upsertOptions, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
            => RMW(ref key, store.comparer.GetHashCode64(ref key), ref input, ref output, out _, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, Context userContext = default, long serialNo = 0)
            => RMW(ref key, rmwOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), ref input, ref output, out _, userContext, serialNo);

        /// <inheritdoc/>
        public Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
            => RMW(ref key, store.comparer.GetHashCode64(ref key), ref input, ref output, out recordMetadata, userContext, serialNo);

        /// <inheritdoc/>
        public Status RMW(ref Key key, ref Input input, ref Output output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            var keyHash = rmwOptions.KeyHash ?? store.comparer.GetHashCode64(ref key);
            return RMW(ref key, keyHash, ref input, ref output, out recordMetadata, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status RMW(ref Key key, long keyHash, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextRMW(ref key, keyHash, ref input, ref output, out recordMetadata, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, out Output output, ref RMWOptions rmwOptions, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, ref rmwOptions, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, Context userContext = default, long serialNo = 0)
            => RMW(ref key, ref input, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, ref RMWOptions rmwOptions, Context userContext = default, long serialNo = 0)
            => RMW(ref key, ref input, ref rmwOptions, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            RMWOptions rmwOptions = default;
            return store.RmwAsync<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, ref key, ref input, ref rmwOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, ref RMWOptions rmwOptions, Context context = default, long serialNo = 0, CancellationToken token = default)
            => store.RmwAsync<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, ref key, ref input, ref rmwOptions, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
            => RMWAsync(ref key, ref input, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, ref RMWOptions rmwOptions, Context context = default, long serialNo = 0, CancellationToken token = default)
            => RMWAsync(ref key, ref input, ref rmwOptions, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default, long serialNo = 0)
            => Delete(ref key, store.comparer.GetHashCode64(ref key), userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default, long serialNo = 0)
            => Delete(ref key, deleteOptions.KeyHash ?? store.comparer.GetHashCode64(ref key), userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, long keyHash, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return store.ContextDelete<Input, Output, Context, InternalTsavoriteSession>(ref key, keyHash, userContext, TsavoriteSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, Context userContext = default, long serialNo = 0)
            => Delete(ref key, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, ref DeleteOptions deleteOptions, Context userContext = default, long serialNo = 0)
            => Delete(ref key, ref deleteOptions, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            DeleteOptions deleteOptions = default;
            return store.DeleteAsync<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, ref key, ref deleteOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, ref DeleteOptions deleteOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => store.DeleteAsync<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, ref key, ref deleteOptions, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => DeleteAsync(ref key, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<TsavoriteKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, ref DeleteOptions deleteOptions, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => DeleteAsync(ref key, ref deleteOptions, userContext, serialNo, token);

        /// <inheritdoc/>
        public void Refresh()
        {
            UnsafeResumeThread();
            store.InternalRefresh<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession);
            UnsafeSuspendThread();
        }

        /// <inheritdoc/>
        public void ResetModified(ref Key key)
        {
            UnsafeResumeThread();
            try
            {
                UnsafeResetModified(ref key);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public bool NeedKeyHash => store.LockTable.IsEnabled && store.LockTable.NeedKeyHash;

        /// <inheritdoc/>
        public int CompareKeyHashes<TLockableKey>(TLockableKey key1, TLockableKey key2) where TLockableKey : ILockableKey => store.LockTable.CompareKeyHashes(key1, key2);

        /// <inheritdoc/>
        public int CompareKeyHashes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2) where TLockableKey : ILockableKey => store.LockTable.CompareKeyHashes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortKeyHashes<TLockableKey>(TLockableKey[] keys) where TLockableKey : ILockableKey => store.LockTable.SortKeyHashes(keys);

        /// <inheritdoc/>
        public void SortKeyHashes<TLockableKey>(TLockableKey[] keys, int start, int count) where TLockableKey : ILockableKey => store.LockTable.SortKeyHashes(keys, start, count);

        #endregion ITsavoriteContext

        #region Pending Operations

        /// <summary>
        /// Get list of pending requests (for current session)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> GetPendingRequests()
        {
            foreach (var kvp in ctx.prevCtx?.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var kvp in ctx.ioPendingRequests)
                yield return kvp.Value.serialNum;
        }

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
            => CompletePending(false, wait, spinWaitForCommit);

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            InitializeCompletedOutputs();
            var result = CompletePending(true, wait, spinWaitForCommit);
            completedOutputs = this.completedOutputs;
            return result;
        }

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations, returning outputs for the completed operations.
        /// Assumes epoch protection is managed by user. Async operations must be completed individually.
        /// </summary>
        internal bool UnsafeCompletePendingWithOutputs<TsavoriteSession>(TsavoriteSession tsavoriteSession, out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            InitializeCompletedOutputs();
            var result = UnsafeCompletePending(tsavoriteSession, true, wait, spinWaitForCommit);
            completedOutputs = this.completedOutputs;
            return result;
        }

        private void InitializeCompletedOutputs()
        {
            if (completedOutputs is null)
                completedOutputs = new CompletedOutputIterator<Key, Value, Input, Output, Context>();
            else
                completedOutputs.Dispose();
        }

        internal bool CompletePending(bool getOutputs, bool wait, bool spinWaitForCommit)
        {
            UnsafeResumeThread();
            try
            {
                return UnsafeCompletePending(TsavoriteSession, getOutputs, wait, spinWaitForCommit);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        internal bool UnsafeCompletePending<TsavoriteSession>(TsavoriteSession tsavoriteSession, bool getOutputs, bool wait, bool spinWaitForCommit)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            var requestedOutputs = getOutputs ? completedOutputs : default;
            var result = store.InternalCompletePending(tsavoriteSession, wait, requestedOutputs);
            if (spinWaitForCommit)
            {
                if (!wait)
                    throw new TsavoriteException("Can spin-wait for commit (checkpoint completion) only if wait is true");
                do
                {
                    store.InternalCompletePending(tsavoriteSession, wait, requestedOutputs);
                    if (store.InRestPhase())
                    {
                        store.InternalCompletePending(tsavoriteSession, wait, requestedOutputs);
                        return true;
                    }
                } while (wait);
            }
            return result;
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => CompletePendingAsync(false, waitForCommit, token);

        /// <inheritdoc/>
        public async ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            InitializeCompletedOutputs();
            await CompletePendingAsync(true, waitForCommit, token).ConfigureAwait(false);
            return completedOutputs;
        }

        private async ValueTask CompletePendingAsync(bool getOutputs, bool waitForCommit = false, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (store.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            // Complete all pending operations on session
            await store.CompletePendingAsync(TsavoriteSession, token, getOutputs ? completedOutputs : null).ConfigureAwait(false);

            // Wait for commit if necessary
            if (waitForCommit)
                await WaitForCommitAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Check if at least one synchronous request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding synchronous requests
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask ReadyToCompletePendingAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (store.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            await TsavoriteKV<Key, Value>.ReadyToCompletePendingAsync(ctx, token).ConfigureAwait(false);
        }

        #endregion Pending Operations

        #region Other Operations

        internal void UnsafeResetModified(ref Key key)
        {
            OperationStatus status;
            do
                status = store.InternalModifiedBitOperation(ref key, out _);
            while (store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, InternalTsavoriteSession>(status, TsavoriteSession));
        }

        /// <inheritdoc/>
        public unsafe void ResetModified(Key key) => ResetModified(ref key);

        /// <inheritdoc/>
        internal bool IsModified(ref Key key)
        {
            UnsafeResumeThread();
            try
            {
                return UnsafeIsModified(ref key);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        internal bool UnsafeIsModified(ref Key key)
        {
            RecordInfo modifiedInfo;
            OperationStatus status;
            do
                status = store.InternalModifiedBitOperation(ref key, out modifiedInfo, false);
            while (store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, InternalTsavoriteSession>(status, TsavoriteSession));
            return modifiedInfo.Modified;
        }

        /// <inheritdoc/>
        internal unsafe bool IsModified(Key key) => IsModified(ref key);

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (!ctx.prevCtx.pendingReads.IsEmpty || !ctx.pendingReads.IsEmpty)
                throw new TsavoriteException("Make sure all async operations issued on this session are awaited and completed first");

            // Complete all pending sync operations on session
            await CompletePendingAsync(token: token).ConfigureAwait(false);

            var task = store.CheckpointTask;
            CommitPoint localCommitPoint = LatestCommitPoint;
            if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                return;

            while (true)
            {
                await task.WithCancellationAsync(token).ConfigureAwait(false);
                Refresh();

                task = store.CheckpointTask;
                localCommitPoint = LatestCommitPoint;
                if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                    break;
            }
        }

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="compactUntilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact(long compactUntilAddress, CompactionType compactionType = CompactionType.Scan)
            => Compact(compactUntilAddress, compactionType, default(DefaultCompactionFunctions<Key, Value>));

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="compactUntilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact(ref Input input, ref Output output, long compactUntilAddress, CompactionType compactionType = CompactionType.Scan)
            => Compact(ref input, ref output, compactUntilAddress, compactionType, default(DefaultCompactionFunctions<Key, Value>));

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<CompactionFunctions>(long untilAddress, CompactionType compactionType, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            Input input = default;
            Output output = default;
            return store.Compact<Input, Output, Context, Functions, CompactionFunctions>(functions, compactionFunctions, ref input, ref output, untilAddress, compactionType);
        }

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<CompactionFunctions>(ref Input input, ref Output output, long untilAddress, CompactionType compactionType, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            return store.Compact<Input, Output, Context, Functions, CompactionFunctions>(functions, compactionFunctions, ref input, ref output, untilAddress, compactionType);
        }

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
                return store.CompactionConditionalCopyToTail<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, ref key, ref input, ref value, ref output, untilAddress);
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
                return store.hlog.ConditionalScanPush<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession, scanCursorState, recordInfo, ref key, ref value, untilAddress);
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
                return store.InternalContainsKeyInMemory<Input, Output, Context, InternalTsavoriteSession>(ref key, TsavoriteSession, out logicalAddress, fromAddress);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Pull iterator for all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>Tsavorite iterator</returns>
        public ITsavoriteScanIterator<Key, Value> Iterate(long untilAddress = -1)
            => store.Iterate<Input, Output, Context, Functions>(functions, untilAddress);

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in Tsavorite
        /// </summary>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>True if Iteration completed; false if Iteration ended early due to one of the TScanIterator reader functions returning false</returns>
        public bool Iterate<TScanFunctions>(ref TScanFunctions scanFunctions, long untilAddress = -1)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
            => store.Iterate<Input, Output, Context, Functions, TScanFunctions>(functions, ref scanFunctions, untilAddress);

        /// <summary>
        /// Push-scan the log from <paramref name="cursor"/> (which should be a valid address) and push up to <paramref name="count"/> records
        /// to the caller via <paramref name="scanFunctions"/> for each Key that is not found at a higher address.
        /// </summary>
        /// <param name="cursor">The cursor of the scan. If 0, start at BeginAddress, else this should be the value this was updated with on the previous call,
        ///     which is the next address to return. If this is some other value, then for variable length records it must be validated by iterating from the 
        ///     start of the page, and iteration will start from the first valid logical address &lt;= this value. This is expensive.</param>
        /// <param name="count">The number of records to push.</param>
        /// <param name="scanFunctions">Caller functions called to push records. For this variant of Scan, this is not a ref param, because it will likely be copied through
        ///     the pending IO process.</param>
        /// <param name="endAddress">A specific end address; otherwise we scan until we hit the current TailAddress, which may yield duplicates in the event of RCUs.
        ///     This may be set to the TailAddress at the start of the scan, which may lose records that are RCU'd during the scan (because they are moved above the starting
        ///     TailAddress). A snapshot can be taken by calling ShiftReadOnlyToTail() and then using that TailAddress as endAddress.</param>
        /// <param name="validateCursor">If true, validate that the cursor is on a valid address boundary, and snap it to the highest lower address if it is not.</param>
        /// <returns>True if Scan completed and pushed <paramref name="count"/> records; false if Scan ended early due to finding less than <paramref name="count"/> records
        /// or one of the TScanIterator reader functions returning false</returns>
        public bool ScanCursor<TScanFunctions>(ref long cursor, long count, TScanFunctions scanFunctions, long endAddress = long.MaxValue, bool validateCursor = false)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
            => store.hlog.ScanCursor(store, scanCursorState ??= new(), ref cursor, count, scanFunctions, endAddress, validateCursor);

        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call SuspendThread before any async op.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread()
        {
            // We do not track any "acquired" state here; if someone mixes calls between safe and unsafe contexts, they will 
            // get the "trying to acquire already-acquired epoch" error.
            store.epoch.Resume();
            store.InternalRefresh<Input, Output, Context, InternalTsavoriteSession>(TsavoriteSession);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeSuspendThread()
        {
            Debug.Assert(store.epoch.ThisInstanceProtected());
            store.epoch.Suspend();
        }

        void IClientSession.AtomicSwitch(long version)
        {
            store.AtomicSwitch(ctx, ctx.prevCtx, version, store._hybridLogCheckpoint.info.checkpointTokens);
        }

        /// <inheritdoc/>
        public void MergeRevivificationStatsTo(ref RevivificationStats to, bool reset) => ctx.MergeRevivificationStatsTo(ref to, reset);

        /// <inheritdoc/>
        public void ResetRevivificationStats() => ctx.ResetRevivificationStats();

        /// <summary>
        /// Return true if Tsavorite State Machine is in PREPARE state
        /// </summary>
        internal bool IsInPreparePhase()
        {
            return store.SystemState.Phase == Phase.PREPARE || store.SystemState.Phase == Phase.PREPARE_GROW;
        }

        #endregion Other Operations

        #region ITsavoriteSession

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo, out OperationStatus status)
        {
            // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
            if (functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo))
            {
                rmwInfo.Action = RMWAction.Default;
                // MarkPage is done in InternalRMW
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                return true;
            }
            if (rmwInfo.Action == RMWAction.CancelOperation)
            {
                status = OperationStatus.CANCELED;
                return false;
            }
            if (rmwInfo.Action == RMWAction.ExpireAndResume)
            {
                // This inserts the tombstone if appropriate
                return store.ReinitializeExpiredRecord<Input, Output, Context, InternalTsavoriteSession>(ref key, ref input, ref value, ref output, ref recordInfo,
                                                    ref rmwInfo, rmwInfo.Address, TsavoriteSession, isIpu: true, out status);
            }
            if (rmwInfo.Action == RMWAction.ExpireAndStop)
            {
                recordInfo.Tombstone = true;
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord | StatusCode.Expired);
                return false;
            }

            status = OperationStatus.SUCCESS;
            return false;
        }

        // This is a struct to allow JIT to inline calls (and bypass default interface call mechanism)
        internal readonly struct InternalTsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            private readonly ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;

            public InternalTsavoriteSession(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
            {
                _clientSession = clientSession;
            }

            public bool IsManualLocking => false;
            public TsavoriteKV<Key, Value> Store => _clientSession.store;

            #region IFunctions - Reads
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo)
                => _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo)
            {
                // RecordIsolation locking is done at Transient scope, in stackCtx. For reads either an SLock or XLock is valid.
                Debug.Assert(!_clientSession.store.DoRecordIsolation || readInfo.RecordInfo.IsLocked, "Expected Lock in ConcurrentReader");

                return _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo, ref recordInfo);
            }

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion IFunctions - Reads

            #region IFunctions - Upserts
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
                => _clientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                if (_clientSession.store.DoRecordIsolation)
                    PostSingleWriterRecordIsolation(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);
                else
                    PostSingleWriterNoLock(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleWriterNoLock(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
            }

            public void PostSingleWriterRecordIsolation(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo)
            {
                // Note: RecordIsolation XLock was taken by CASRecordIntoChain() (if not readcache), separate from the Transient-scope stackCtx.recSrc lock.
                try
                {
                    PostSingleWriterNoLock(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason, ref recordInfo);
                }
                finally
                {
                    if (reason != WriteReason.CopyToReadCache)  // readcache records are readonly so are not XLocked
                        recordInfo.UnlockExclusive();
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentWriter(long physicalAddress, ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo)
            {
                // RecordIsolation locking is done at Transient scope, in stackCtx.
                Debug.Assert(!_clientSession.store.DoRecordIsolation || upsertInfo.RecordInfo.IsLockedExclusive, "Expected XLock in ConcurrentWriter");

                (upsertInfo.UsedValueLength, upsertInfo.FullValueLength, _) = _clientSession.store.GetRecordLengths(physicalAddress, ref dst, ref recordInfo);
                if (!_clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, ref recordInfo))
                    return false;
                _clientSession.store.SetExtraValueLength(ref dst, ref recordInfo, upsertInfo.UsedValueLength, upsertInfo.FullValueLength);
                recordInfo.SetDirtyAndModified();
                return true;
            }
            #endregion IFunctions - Upserts

            #region IFunctions - RMWs
            #region InitialUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
                => _clientSession.functions.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                if (_clientSession.store.DoRecordIsolation)
                    PostInitialUpdaterRecordIsolation(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
                else
                    PostInitialUpdaterNoLock(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PostInitialUpdaterNoLock(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void PostInitialUpdaterRecordIsolation(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                // Note: RecordIsolation XLock was taken by CASRecordIntoChain(), separate from the Transient-scope stackCtx.recSrc lock.
                try
                {
                    PostInitialUpdaterNoLock(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
            }
            #endregion InitialUpdater

            #region CopyUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output, ref rmwInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
                => _clientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                if (_clientSession.store.DoRecordIsolation)
                    PostCopyUpdaterRecordIsolation(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);
                else
                    PostCopyUpdaterNoLock(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostCopyUpdaterNoLock(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostCopyUpdaterRecordIsolation(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo)
            {
                // Note: RecordIsolation XLock was taken by CASRecordIntoChain(), separate from the Transient-scope stackCtx.recSrc lock.
                try
                {
                    PostCopyUpdaterNoLock(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo, ref recordInfo);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
            }
            #endregion CopyUpdater

            #region InPlaceUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InPlaceUpdater(long physicalAddress, ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, out OperationStatus status, ref RecordInfo recordInfo)
            {
                // RecordIsolation locking is done at Transient scope, in stackCtx.
                Debug.Assert(!_clientSession.store.DoRecordIsolation || rmwInfo.RecordInfo.IsLockedExclusive, "Expected XLock in InPlaceUpdater");

                (rmwInfo.UsedValueLength, rmwInfo.FullValueLength, _) = _clientSession.store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);
                if (!_clientSession.InPlaceUpdater(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo, out status))
                    return false;
                _clientSession.store.SetExtraValueLength(ref value, ref recordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                recordInfo.SetDirtyAndModified();
                return true;
            }
            #endregion InPlaceUpdater

            public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion IFunctions - RMWs

            #region IFunctions - Deletes
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
                => _clientSession.functions.SingleDeleter(ref key, ref value, ref deleteInfo, ref recordInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            {
                if (_clientSession.store.DoRecordIsolation)
                    PostSingleDeleterRecordIsolation(ref key, ref deleteInfo, ref recordInfo);
                else
                    PostSingleDeleterNoLock(ref key, ref deleteInfo, ref recordInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleterNoLock(ref Key key, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostSingleDeleter(ref key, ref deleteInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleterRecordIsolation(ref Key key, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo)
            {
                // Note: RecordIsolation XLock was taken by CASRecordIntoChain(), separate from the Transient-scope stackCtx.recSrc lock.
                try
                {
                    PostSingleDeleterNoLock(ref key, ref deleteInfo, ref recordInfo);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentDeleter(long physicalAddress, ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo, out int allocatedSize)
            {
                // RecordIsolation locking is done at Transient scope, in stackCtx.
                Debug.Assert(!_clientSession.store.DoRecordIsolation || deleteInfo.RecordInfo.IsLockedExclusive, "Expected XLock in ConcurrentDeleter");

                (deleteInfo.UsedValueLength, deleteInfo.FullValueLength, allocatedSize) = _clientSession.store.GetRecordLengths(physicalAddress, ref value, ref recordInfo);
                if (!_clientSession.functions.ConcurrentDeleter(ref key, ref value, ref deleteInfo, ref recordInfo))
                    return false;
                _clientSession.store.SetTombstoneAndExtraValueLength(ref value, ref recordInfo, deleteInfo.UsedValueLength, deleteInfo.FullValueLength);
                recordInfo.SetDirtyAndModified();
                return true;
            }
            #endregion IFunctions - Deletes

            #region IFunctions - Dispose
            public void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason)
                => _clientSession.functions.DisposeSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
            public void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.DisposeCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            public void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.DisposeInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            public void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo)
                => _clientSession.functions.DisposeSingleDeleter(ref key, ref value, ref deleteInfo);
            public void DisposeDeserializedFromDisk(ref Key key, ref Value value, ref RecordInfo recordInfo)
                => _clientSession.functions.DisposeDeserializedFromDisk(ref key, ref value);
            public void DisposeForRevivification(ref Key key, ref Value value, int newKeySize, ref RecordInfo recordInfo)
                => _clientSession.functions.DisposeForRevivification(ref key, ref value, newKeySize);
            #endregion IFunctions - Dispose

            #region IFunctions - Checkpointing
            public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
            {
                _clientSession.functions.CheckpointCompletionCallback(sessionID, sessionName, commitPoint);
                _clientSession.LatestCommitPoint = commitPoint;
            }
            #endregion IFunctions - Checkpointing

            #region Transient locking
            public bool TryLockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                if (!Store.DoTransientLocking)
                    return true;
                if (!Store.LockTable.TryLockTransientExclusive(ref key, ref stackCtx.hei))
                    return false;
                stackCtx.recSrc.SetHasTransientXLock();
                return true;
            }

            public bool TryLockTransientShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                if (!Store.DoTransientLocking)
                    return true;
                if (!Store.LockTable.TryLockTransientShared(ref key, ref stackCtx.hei))
                    return false;
                stackCtx.recSrc.SetHasTransientSLock();
                return true;
            }

            public void UnlockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                if (!Store.DoTransientLocking)
                    return;
                Store.LockTable.UnlockExclusive(ref key, ref stackCtx.hei);
                stackCtx.recSrc.ClearHasTransientXLock();
            }

            public void UnlockTransientShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                if (!Store.DoTransientLocking)
                    return;
                Store.LockTable.UnlockShared(ref key, ref stackCtx.hei);
                stackCtx.recSrc.ClearHasTransientSLock();
            }
            #endregion Transient locking

            #region Internal utilities
            public int GetRMWInitialValueLength(ref Input input) => _clientSession.functions.GetRMWInitialValueLength(ref input);

            public int GetRMWModifiedValueLength(ref Value t, ref Input input) => _clientSession.functions.GetRMWModifiedValueLength(ref t, ref input);

            public IHeapContainer<Input> GetHeapContainer(ref Input input)
            {
                if (typeof(Input) == typeof(SpanByte))
                    return new SpanByteHeapContainer(ref Unsafe.As<Input, SpanByte>(ref input), _clientSession.store.hlog.bufferPool) as IHeapContainer<Input>;
                return new StandardHeapContainer<Input>(ref input);
            }

            public void UnsafeResumeThread() => _clientSession.UnsafeResumeThread();

            public void UnsafeSuspendThread() => _clientSession.UnsafeSuspendThread();

            public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
                => _clientSession.CompletePendingWithOutputs(out completedOutputs, wait, spinWaitForCommit);

            public TsavoriteKV<Key, Value>.TsavoriteExecutionContext<Input, Output, Context> Ctx => _clientSession.ctx;
            #endregion Internal utilities
        }
        #endregion ITsavoriteSession
    }
}