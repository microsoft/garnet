// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using NUnit.Framework;
using Tsavorite.core;

namespace Tsavorite.test
{
    internal struct TestTransientKernelSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> : IKernelSession
        where TSessionFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession;

        internal TestTransientKernelSession(ClientSession<TKey, TValue, TInput, TOutput, TContext, TSessionFunctions, TStoreFunctions, TAllocator> clientSession)
            => this.clientSession = clientSession;

        /// <inheritdoc/>
        public ulong SharedTxnLockCount { get; set; }

        /// <inheritdoc/>
        public ulong ExclusiveTxnLockCount { get; set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void BeginTransaction() 
            => Assert.Fail($"Should not be doing transactional operations in {this.GetType().Name}");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void EndTransaction()
            => Assert.Fail($"Should not be doing transactional operations in {this.GetType().Name}");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void CheckTransactionIsStarted()
            => Assert.Fail($"Should not be doing transactional operations in {this.GetType().Name}");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void CheckTransactionIsNotStarted()
            => Assert.Fail($"Should not be doing transactional operations in {this.GetType().Name}");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Refresh()
        {
            if (clientSession.Store.Kernel.Epoch.ThisInstanceProtected())
                clientSession.UnsafeRefresh();
            else
                clientSession.Refresh();
        }

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
