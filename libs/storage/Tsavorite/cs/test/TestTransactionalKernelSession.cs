// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Tsavorite.test
{
    internal struct TestTransactionalKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>: IKernelSession
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession;

        internal TestTransactionalKernelSession(ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession) 
            => this.clientSession = clientSession;

        /// <inheritdoc/>
        public ulong SharedTxnLockCount { get; set; }

        /// <inheritdoc/>
        public ulong ExclusiveTxnLockCount { get; set; }

        /// <inheritdoc/>
        readonly ulong TotalTxnLockCount => SharedTxnLockCount + ExclusiveTxnLockCount;

        bool isTxnStarted;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void BeginTransaction()
        {
            CheckTransactionIsNotStarted();
            isTxnStarted = true;

            // These must use session to be aware of per-session SystemState.
            clientSession.BeginTransaction();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void EndTransaction()
        {
            CheckTransactionIsStarted();

            if (TotalTxnLockCount > 0)
                throw new TsavoriteException($"EndTransaction called with locks held: {SharedTxnLockCount} shared locks, {ExclusiveTxnLockCount} exclusive locks");

            clientSession.EndTransaction();
            isTxnStarted = false;
        }

        internal void ClearCountsOnError()
        {
            // If we already have an exception, clear these counts so "Run" will not report them spuriously.
            SharedTxnLockCount = 0;
            ExclusiveTxnLockCount = 0;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void CheckTransactionIsStarted()
        {
            if (!isTxnStarted)
                throw new TsavoriteException("Transactional Locking method call when BeginTransaction has not been called");
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void CheckTransactionIsNotStarted()
        {
            if (isTxnStarted)
                throw new TsavoriteException("BeginTransaction cannot be called twice (call EndTransaction first)");
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Refresh() => clientSession.Refresh();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void HandleImmediateNonPendingRetryStatus(bool refresh) => clientSession.HandleImmediateNonPendingRetryStatus(refresh);

        /// <inheritdoc/>
        public void BeginUnsafe()
        {
            clientSession.Store.Kernel.Epoch.Resume();
            clientSession.DoThreadStateMachineStep();
        }

        /// <inheritdoc/>
        public void EndUnsafe()
        {
            Debug.Assert(clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            clientSession.Store.Kernel.Epoch.Suspend();
        }

        /// <inheritdoc/>
        public bool IsEpochAcquired() => clientSession.Store.Kernel.Epoch.ThisInstanceProtected();
    }
}
