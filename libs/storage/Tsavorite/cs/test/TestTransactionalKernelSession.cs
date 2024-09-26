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
        readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> _clientSession;

        internal TestTransactionalKernelSession(ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession) => _clientSession = clientSession;

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
            _clientSession.BeginTransaction();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void EndTransaction()
        {
            CheckTransactionIsStarted();

            if (TotalTxnLockCount > 0)
                throw new TsavoriteException($"EndTransaction called with locks held: {SharedTxnLockCount} shared locks, {ExclusiveTxnLockCount} exclusive locks");

            _clientSession.EndTransaction();
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
        public void Refresh() => _clientSession.Refresh();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void HandleImmediateNonPendingRetryStatus(bool refresh) => _clientSession.HandleImmediateNonPendingRetryStatus(refresh);

        /// <inheritdoc/>
        public void BeginUnsafe()
        {
            _clientSession.Store.Kernel.Epoch.Resume();
            _clientSession.DoThreadStateMachineStep();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool EnsureBeginUnsafe()
        {
            if (IsEpochAcquired)
                return false;
            BeginUnsafe();
            return true;
        }

        /// <inheritdoc/>
        public void EndUnsafe()
        {
            Debug.Assert(_clientSession.Store.Kernel.Epoch.ThisInstanceProtected());
            _clientSession.Store.Kernel.Epoch.Suspend();
        }

        /// <inheritdoc/>
        public bool IsEpochAcquired => _clientSession.Store.Kernel.Epoch.ThisInstanceProtected();
    }
}
