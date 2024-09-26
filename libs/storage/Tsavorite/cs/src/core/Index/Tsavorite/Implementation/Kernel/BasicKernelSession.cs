// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal struct BasicKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> : IKernelSession
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        private readonly ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> _clientSession;

        public ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> ClientSession => _clientSession;
        public TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> Store => _clientSession.Store;

        internal BasicKernelSession(ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession) => _clientSession = clientSession;

        /// <inheritdoc/>
        public ulong SharedTxnLockCount { get => throw new NotImplementedException("SharedTxnLockCount.get"); set => throw new NotImplementedException("SharedTxnLockCount.set"); }

        /// <inheritdoc/>
        public ulong ExclusiveTxnLockCount { get => throw new NotImplementedException("ExclusiveTxnLockCount.get"); set => throw new NotImplementedException("ExclusiveTxnLockCount.set"); }

        /// <inheritdoc/>
        public void CheckTransactionIsStarted() => throw new NotImplementedException("CheckTransactionIsStarted()");

        /// <inheritdoc/>
        public void CheckTransactionIsNotStarted() => throw new NotImplementedException("CheckTransactionIsNotStarted()");

        /// <inheritdoc/>
        public void Refresh() => _clientSession.Refresh();

        /// <inheritdoc/>
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
        public readonly bool IsEpochAcquired => _clientSession.Store.Kernel.Epoch.ThisInstanceProtected();
    }
}
