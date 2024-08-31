// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Tsavorite.test
{
    internal struct TestTransactionalKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator, TSessionContext>: IKernelSession
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        where TSessionContext : ITsavoriteContext<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator>, ILockableContext<TKey>
    {
        readonly TSessionContext context;

        internal TestTransactionalKernelSession(TSessionContext context) => this.context = context;

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
            context.BeginTransaction();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void EndTransaction()
        {
            CheckTransactionIsStarted();

            if (TotalTxnLockCount > 0)
                throw new TsavoriteException($"EndTransaction called with locks held: {SharedTxnLockCount} shared locks, {ExclusiveTxnLockCount} exclusive locks");

            context.EndTransaction();
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
        public void Refresh() => context.Refresh();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void HandleImmediateNonPendingRetryStatus(bool refresh) => context.HandleImmediateNonPendingRetryStatus(refresh);

        /// <inheritdoc/>
        public void BeginUnsafe()
        {
            context.Session.Store.Kernel.Epoch.Resume();
            context.DoThreadStateMachineStep();
        }

        /// <inheritdoc/>
        public void EndUnsafe()
        {
            Debug.Assert(context.Session.Store.Kernel.Epoch.ThisInstanceProtected());
            context.Session.Store.Kernel.Epoch.Suspend();
        }

        /// <inheritdoc/>
        public bool IsEpochAcquired() => context.Session.Store.Kernel.Epoch.ThisInstanceProtected();
    }
}
