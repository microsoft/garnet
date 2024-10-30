// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Tsavorite Operations implementation for dual-store configuration. Taken from <see cref="UnsafeContext{TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator}"/>
    /// with Dual-specific locking considerations. Requires manual control of epoch management.
    /// </summary>
    public readonly struct DualItemContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession;
        internal readonly SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> sessionFunctions;

        /// <inheritdoc/>
        public bool IsNull => clientSession is null;

        internal DualItemContext(ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            this.clientSession = clientSession;
            sessionFunctions = new(clientSession, isDual: true);
        }

        /// <inheritdoc/>
        public ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> Session => clientSession;

        /// <inheritdoc/>
        public long GetKeyHash(TKey key) => clientSession.Store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref TKey key) => clientSession.Store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public bool CompletePending<TKeyLocker>(bool wait = false, bool spinWaitForCommit = false)
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePending<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(
                    sessionFunctions, false, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs<TKeyLocker>(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePendingWithOutputs<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(
                    sessionFunctions, out completedOutputs, wait, spinWaitForCommit);
        }

        /// <summary>
        /// Utility function to return the single pending result immediately after detecting status.IsPending
        /// </summary>
        public (Status, TOutput output) GetSinglePendingResult<TKeyLocker>()
            where TKeyLocker : struct, ISessionLocker
        {
            _ = CompletePendingWithOutputs<TKeyLocker>(out var completedOutputs, wait: true);
            var hasNext = completedOutputs.Next();
            Debug.Assert(hasNext, "hasNext should be true");
            var (status, output) = (completedOutputs.Current.Status, completedOutputs.Current.Output);
            hasNext = completedOutputs.Next();
            Debug.Assert(!hasNext, "hasNext should be false");
            completedOutputs.Dispose();
            return (status, output);
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync<TKeyLocker>(bool waitForCommit = false, CancellationToken token = default)
            where TKeyLocker : struct, ISessionLocker
            => clientSession.CompletePendingAsync<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext>> CompletePendingWithOutputsAsync<TKeyLocker>(bool waitForCommit = false, CancellationToken token = default)
            where TKeyLocker : struct, ISessionLocker
            => clientSession.CompletePendingWithOutputsAsync<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKeyLocker>(ref HashEntryInfo hei, ref TKey key, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.Store.ContextRead<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(
                    ref hei, ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress<TKeyLocker>(ref HashEntryInfo hei, ref TKey key, bool isNoKey, ref TInput input, ref TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.Store.ContextReadAtAddress<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(
                    ref hei, ref key, isNoKey, ref input, ref output, ref readOptions, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKeyLocker>(ref HashEntryInfo hei, ref TKey key, ref TInput input, ref TValue desiredValue, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.Store.ContextUpsert<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(
                    ref hei, ref key, ref input, ref desiredValue, ref output, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKeyLocker>(ref HashEntryInfo hei, ref TKey key, ref TInput input, ref TOutput output, out RecordMetadata recordMetadata, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.Store.ContextRMW<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(
                    ref hei, ref key, ref input, ref output, out recordMetadata, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker>(ref HashEntryInfo hei, TKey key, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
            => Delete<TKeyLocker>(ref hei, ref key, userContext);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKeyLocker>(ref HashEntryInfo hei, ref TKey key, TContext userContext = default)
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.Store.ContextDelete<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(
                    ref hei, ref key, userContext, sessionFunctions);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified<TKeyLocker>(ref HashEntryInfo hei, ref TKey key)
            where TKeyLocker : struct, ISessionLocker
            => clientSession.UnsafeResetModified<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, TKeyLocker>(ref hei, sessionFunctions, ref key);
    }
}