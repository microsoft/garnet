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
    public readonly struct BasicContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        internal readonly SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> sessionFunctions;

        /// <inheritdoc/>
        private TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> Store => KernelSession.Store;

        internal BasicContext(ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            sessionFunctions = new(clientSession, isDual: false);
        }

        #region ITsavoriteContext

        /// <inheritdoc/>
        public ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> Session => KernelSession.ClientSession;

        private ref BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> KernelSession => ref Session.BasicKernelSession;

        /// <inheritdoc/>
        public long GetKeyHash(TKey key) => KernelSession.Store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref TKey key) => KernelSession.Store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public bool CompletePending<TKeyLocker>(bool wait = false, bool spinWaitForCommit = false)
            where TKeyLocker : struct, IKeyLocker
            => KernelSession.ClientSession.CompletePending<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(sessionFunctions, wait, spinWaitForCommit);

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs<TKeyLocker>(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TKeyLocker : struct, IKeyLocker
            => KernelSession.ClientSession.CompletePendingWithOutputs<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(sessionFunctions, out completedOutputs, wait, spinWaitForCommit);

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync<TKeyLocker>(bool waitForCommit = false, CancellationToken token = default)
            where TKeyLocker : struct, IKeyLocker
            => KernelSession.ClientSession.CompletePendingAsync<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext>> CompletePendingWithOutputsAsync<TKeyLocker>(bool waitForCommit = false, CancellationToken token = default)
            where TKeyLocker : struct, IKeyLocker
            => KernelSession.ClientSession.CompletePendingWithOutputsAsync<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            ReadOptions readOptions = new();
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, recordMetadata: out _, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey key, TInput input, out TOutput output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            output = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey key, TInput input, out TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            output = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey key, ref TOutput output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TInput input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(ref TKey key, ref TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TInput input = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey key, out TOutput output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TInput input = default;
            output = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker, TEpochGuard>(TKey key, out TOutput output, ref ReadOptions readOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TInput input = default;
            output = default;
            return Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read<TKeyLocker, TEpochGuard>(TKey key, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TInput input = default;
            TOutput output = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, TOutput output) Read<TKeyLocker, TEpochGuard>(TKey key, ref ReadOptions readOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TInput input = default;
            TOutput output = default;
            return (Read<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref readOptions, userContext), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly Status Read<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            return KernelSession.Store.ContextRead<TInput, TOutput, TContext,
                    SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    TKeyLocker, TEpochGuard>
                (ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions, ref KernelSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress<TKeyLocker, TEpochGuard>(long address, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            KernelSession.BeginUnsafe();
            try
            {
                TKey key = default;
                return Store.ContextReadAtAddress<TInput, TOutput, TContext,
                    SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    TKeyLocker, TEpochGuard>(address, ref key, isNoKey: true, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions, ref KernelSession);
            }
            finally
            {
                KernelSession.EndUnsafe();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress<TKeyLocker, TEpochGuard>(long address, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            KernelSession.BeginUnsafe();
            try
            {
                return Store.ContextReadAtAddress<TInput, TOutput, TContext,
                    SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    TKeyLocker, TEpochGuard>(address, ref key, isNoKey: false, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions, ref KernelSession);
            }
            finally
            {
                KernelSession.EndUnsafe();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey key, ref TValue desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TInput input = default;
            TOutput output = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, Store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey key, ref TValue desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TInput input = default;
            TOutput output = default;
            return Upsert<TKeyLocker, TEpochGuard>(ref key, upsertOptions.KeyHash ?? Store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, Store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, upsertOptions.KeyHash ?? Store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert<TKeyLocker, TEpochGuard>(ref TKey key, long keyHash, ref TInput input, ref TValue desiredValue, ref TOutput output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, keyHash, ref input, ref desiredValue, ref output, recordMetadata: out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, Store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, upsertOptions.KeyHash ?? Store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref desiredValue, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Upsert<TKeyLocker, TEpochGuard>(ref TKey key, long keyHash, ref TInput input, ref TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            KernelSession.BeginUnsafe();
            try
            {
                return Store.ContextUpsert<TInput, TOutput, TContext,
                    SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    TKeyLocker, TEpochGuard>(ref key, keyHash, ref input, ref desiredValue, ref output, out recordMetadata, userContext, sessionFunctions, ref KernelSession);
            }
            finally
            {
                KernelSession.EndUnsafe();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey key, TValue desiredValue, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref desiredValue, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey key, TValue desiredValue, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref desiredValue, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey key, TInput input, TValue desiredValue, ref TOutput output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker, TEpochGuard>(TKey key, TInput input, TValue desiredValue, ref TOutput output, ref UpsertOptions upsertOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Upsert<TKeyLocker, TEpochGuard>(ref key, ref input, ref desiredValue, ref output, ref upsertOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TOutput output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => RMW<TKeyLocker, TEpochGuard>(ref key, Store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => RMW<TKeyLocker, TEpochGuard>(ref key, rmwOptions.KeyHash ?? Store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref output, out _, userContext);

        /// <inheritdoc/>
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => RMW<TKeyLocker, TEpochGuard>(ref key, Store.storeFunctions.GetKeyHashCode64(ref key), ref input, ref output, out recordMetadata, userContext);

        /// <inheritdoc/>
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TOutput output, ref RMWOptions rmwOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            var keyHash = rmwOptions.KeyHash ?? Store.storeFunctions.GetKeyHashCode64(ref key);
            return RMW<TKeyLocker, TEpochGuard>(ref key, keyHash, ref input, ref output, out recordMetadata, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status RMW<TKeyLocker, TEpochGuard>(ref TKey key, long keyHash, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            KernelSession.BeginUnsafe();
            try
            {
                return Store.ContextRMW<TInput, TOutput, TContext,
                    SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    TKeyLocker, TEpochGuard>(ref key, keyHash, ref input, ref output, out recordMetadata, userContext, sessionFunctions, ref KernelSession);
            }
            finally
            {
                KernelSession.EndUnsafe();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey key, TInput input, out TOutput output, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            output = default;
            return RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey key, TInput input, out TOutput output, ref RMWOptions rmwOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            output = default;
            return RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TOutput output = default;
            return RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TOutput output = default;
            return RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref output, ref rmwOptions, userContext);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey key, TInput input, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker, TEpochGuard>(TKey key, TInput input, ref RMWOptions rmwOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => RMW<TKeyLocker, TEpochGuard>(ref key, ref input, ref rmwOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey key, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Delete<TKeyLocker, TEpochGuard>(ref key, Store.storeFunctions.GetKeyHashCode64(ref key), userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(ref TKey key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Delete<TKeyLocker, TEpochGuard>(ref key, deleteOptions.KeyHash ?? Store.storeFunctions.GetKeyHashCode64(ref key), userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Status Delete<TKeyLocker, TEpochGuard>(ref TKey key, long keyHash, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            KernelSession.BeginUnsafe();
            try
            {
                return Store.ContextDelete<TInput, TOutput, TContext,
                    SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>,
                    TKeyLocker, TEpochGuard>(ref key, keyHash, userContext, sessionFunctions, ref KernelSession);
            }
            finally
            {
                KernelSession.EndUnsafe();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey key, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Delete<TKeyLocker, TEpochGuard>(ref key, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker, TEpochGuard>(TKey key, ref DeleteOptions deleteOptions, TContext userContext = default)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => Delete<TKeyLocker, TEpochGuard>(ref key, ref deleteOptions, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified<TKeyLocker, TEpochGuard>(TKey key)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
            => ResetModified<TKeyLocker, TEpochGuard>(ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified<TKeyLocker, TEpochGuard>(ref TKey key)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TEpochGuard.BeginUnsafe(ref KernelSession);
            try
            { 
                HashEntryInfo hei = new(GetKeyHash(ref key), Store.PartitionId);
                if (Store.Kernel.hashTable.FindTag(ref hei))
                    KernelSession.ClientSession.UnsafeResetModified<TKeyLocker>(ref hei, sessionFunctions.ExecutionCtx, ref key);
            }
            finally
            {
                TEpochGuard.EndUnsafe(ref KernelSession);
            }
        }

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
        internal Status CompactionCopyToTail<TKeyLocker, TEpochGuard>(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, long currentAddress, long untilAddress)
            where TKeyLocker : struct, IKeyLocker
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TEpochGuard.BeginUnsafe(ref KernelSession);
            try
            {
                return Store.CompactionConditionalCopyToTail<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(
                        sessionFunctions, ref key, ref input, ref value, ref output, currentAddress, untilAddress);
            }
            finally
            {
                TEpochGuard.EndUnsafe(ref KernelSession);
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
        /// <remarks>No TKeyLocker or TEpochGuard are passed because we always acquire the epoch (we're within Scan()/Iterate() rather than the normal RUMD operations) and do not lock for this operation</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ConditionalScanPush(ScanCursorState<TKey, TValue> scanCursorState, RecordInfo recordInfo, ref TKey key, ref TValue value, long currentAddress, long untilAddress)
        {
            KernelSession.BeginUnsafe();
            try
            {
                return Store.hlogBase.ConditionalScanPush<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>>(
                        sessionFunctions, scanCursorState, recordInfo, ref key, ref value, currentAddress, untilAddress);
            }
            finally
            {
                KernelSession.EndUnsafe();
            }
        }

        /// <summary>
        /// Checks whether specified record is present in memory (between max(fromAddress, HeadAddress) and tail), including tombstones.
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="logicalAddress">Logical address of record, if found</param>
        /// <param name="fromAddress">Look until this address; if less than HeadAddress, then HeadAddress is used</param>
        /// <returns>Status</returns>
        /// <remarks>No TKeyLocker is passed because we do not lock for this operation</remarks>
        internal Status ContainsKeyInMemory<TEpochGuard>(ref TKey key, out long logicalAddress, long fromAddress = -1)
            where TEpochGuard : struct, IBasicEpochGuard<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        {
            TEpochGuard.BeginUnsafe(ref KernelSession);
            try
            {
                return Store.InternalContainsKeyInMemory<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>>(
                        ref key, sessionFunctions, out logicalAddress, fromAddress);
            }
            finally
            {
                TEpochGuard.EndUnsafe(ref KernelSession);
            }
        }
    }
}