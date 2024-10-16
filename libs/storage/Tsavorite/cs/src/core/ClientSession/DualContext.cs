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
    public readonly struct DualContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession;
        internal readonly SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, DualSessionLocker, TStoreFunctions, TAllocator> sessionFunctions;

        /// <inheritdoc/>
        public bool IsNull => clientSession is null;

        internal DualContext(ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession)
        {
            this.clientSession = clientSession;
            sessionFunctions = new(clientSession, new DualSessionLocker(dualRole));
        }

        /// <inheritdoc/>
        public ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> Session => clientSession;

        /// <inheritdoc/>
        public long GetKeyHash(TKey key) => clientSession.Store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public long GetKeyHash(ref TKey key) => clientSession.Store.GetKeyHash(ref key);

        /// <inheritdoc/>
        public bool CompletePending<TKernelSession, TKeyLocker>(bool wait = false, bool spinWaitForCommit = false)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePending<SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, DualSessionLocker, TStoreFunctions, TAllocator>, TKernelSession, TKeyLocker>(
                    sessionFunctions, false, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs<TKernelSession, TKeyLocker>(out CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.UnsafeCompletePendingWithOutputs<TKernelSession, TKeyLocker>(sessionFunctions, out completedOutputs, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync<TKernelSession, TKeyLocker>(bool waitForCommit = false, CancellationToken token = default)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            => clientSession.CompletePendingAsync<TKernelSession, TKeyLocker>(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext>> CompletePendingWithOutputsAsync<TKernelSession, TKeyLocker>(bool waitForCommit = false, CancellationToken token = default)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            => clientSession.CompletePendingWithOutputsAsync<TKernelSession, TKeyLocker>(sessionFunctions, waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read<TKernelSession, TKeyLocker>(ref HashEntryInfo hei, ref TKey key, ref TInput input, out TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata,
                TContext userContext, ref TKernelSession kernelSession)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.Store.ContextRead<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, DualSessionLocker, TStoreFunctions, TAllocator>, TKernelSession, TKeyLocker>(
                    ref hei, ref key, ref input, out output, ref readOptions, out recordMetadata, userContext, sessionFunctions, ref kernelSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress<TKernelSession, TKeyLocker, TEpochGuard>(ref HashEntryInfo hei, long address, ref TKey key, ref TInput input, out TOutput output, ref ReadOptions readOptions, out RecordMetadata recordMetadata,
                TContext userContext, ref TKernelSession kernelSession, TKeyLocker keyLocker)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            where TEpochGuard : struct, IEpochGuard<TKernelSession>
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());

            // TODO: Need DualContextPair.ReadAtAddress with/without key
            return clientSession.Store.ContextReadAtAddress<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, DualSessionLocker, TStoreFunctions, TAllocator>, TKernelSession, TKeyLocker, TEpochGuard>(
                    address, ref key, ref input, out output, ref readOptions, out recordMetadata, userContext, sessionFunctions, ref kernelSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert<TKernelSession, TKeyLocker>(ref HashEntryInfo hei, ref TKey key, ref TInput input, ref TValue desiredValue, out TOutput output, out RecordMetadata recordMetadata, TContext userContext, ref TKernelSession kernelSession)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.Store.ContextUpsert<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, DualSessionLocker, TStoreFunctions, TAllocator>, TKernelSession, TKeyLocker>(
                    ref hei, ref key, ref input, ref desiredValue, out output, out recordMetadata, userContext, sessionFunctions, ref kernelSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW<TKernelSession, TKeyLocker>(ref HashEntryInfo hei, ref TKey key, ref TInput input, out TOutput output, out RecordMetadata recordMetadata, TContext userContext, ref TKernelSession kernelSession)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.Store.ContextRMW<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, DualSessionLocker, TStoreFunctions, TAllocator>, TKernelSession, TKeyLocker>(
                    ref hei, ref key, ref input, out output, out recordMetadata, userContext, sessionFunctions, ref kernelSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete<TKernelSession, TKeyLocker>(ref HashEntryInfo hei, ref TKey key, TContext userContext, ref TKernelSession kernelSession)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            return clientSession.Store.ContextDelete<TInput, TOutput, TContext, SessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, DualSessionLocker, TStoreFunctions, TAllocator>, TKernelSession, TKeyLocker>(
                    ref hei, ref key, userContext, sessionFunctions, ref kernelSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified<TKernelSession, TKeyLocker>(ref TKey key)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            => clientSession.UnsafeResetModified(sessionFunctions, ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified<TKernelSession, TKeyLocker>(ref TKey key)
            where TKernelSession : IKernelSession
            where TKeyLocker : struct, ISessionLocker
            => clientSession.UnsafeIsModified(sessionFunctions, ref key);
    }
}