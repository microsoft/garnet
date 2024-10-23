// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public struct DualKernelSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1,
                                    TKey2, TValue2, TInput2, TOutput2, TSessionFunctions2, TStoreFunctions2, TAllocator2> : IKernelSession
        where TSessionFunctions1 : ISessionFunctions<TKey1, TValue1, TInput1, TOutput1, TContext>
        where TSessionFunctions2 : ISessionFunctions<TKey2, TValue2, TInput2, TOutput2, TContext>
        where TStoreFunctions1 : IStoreFunctions<TKey1, TValue1>
        where TStoreFunctions2 : IStoreFunctions<TKey2, TValue2>
        where TAllocator1 : IAllocator<TKey1, TValue1, TStoreFunctions1>
        where TAllocator2 : IAllocator<TKey2, TValue2, TStoreFunctions2>
    {
        internal readonly TsavoriteKernel Kernel => clientSession1.Store.Kernel;

        internal readonly ClientSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1> clientSession1;
        internal readonly ClientSession<TKey2, TValue2, TInput2, TOutput2, TContext, TSessionFunctions2, TStoreFunctions2, TAllocator2> clientSession2;

        internal readonly DualItemContext<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1> itemContext1;
        internal readonly DualItemContext<TKey2, TValue2, TInput2, TOutput2, TContext, TSessionFunctions2, TStoreFunctions2, TAllocator2> itemContext2;

        /// <inheritdoc/>
        public ulong SharedTxnLockCount { get; set; }   // TODO do these still need to be public?

        /// <inheritdoc/>
        public ulong ExclusiveTxnLockCount { get; set; }

        /// <inheritdoc/>
        public readonly ulong TotalTxnLockCount => SharedTxnLockCount + ExclusiveTxnLockCount;

        bool isTxnStarted;

        /// <summary>ctor</summary>
        public DualKernelSession(ClientSession<TKey1, TValue1, TInput1, TOutput1, TContext, TSessionFunctions1, TStoreFunctions1, TAllocator1> clientSession1,
                                 ClientSession<TKey2, TValue2, TInput2, TOutput2, TContext, TSessionFunctions2, TStoreFunctions2, TAllocator2> clientSession2) : this()
        {
            this.clientSession1 = clientSession1;
            itemContext1 = clientSession1.DualContext;

            this.clientSession2 = clientSession2;
            itemContext2 = clientSession2 is null ? default : clientSession2.DualContext;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void BeginTransaction()
        {
            CheckTransactionIsNotStarted();
            isTxnStarted = true;

            // These must use session to be aware of per-session SystemState.
            clientSession1.BeginTransaction();
            clientSession2?.BeginTransaction();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void EndTransaction()
        {
            CheckTransactionIsStarted();

            if (TotalTxnLockCount > 0)
                throw new TsavoriteException($"EndTransaction called with locks held: {SharedTxnLockCount} shared locks, {ExclusiveTxnLockCount} exclusive locks");

            clientSession1.EndTransaction();
            clientSession2?.EndTransaction();
            isTxnStarted = false;
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
        public void Refresh<TKeyLocker>(ref HashEntryInfo hei)
            where TKeyLocker : struct, ISessionLocker
        {
            Kernel.Epoch.ProtectAndDrain();

            // These must use session to be aware of per-session SystemState.
            clientSession1.Refresh<TKeyLocker>(ref hei);
            clientSession2?.Refresh<TKeyLocker>(ref hei);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void HandleImmediateNonPendingRetryStatus(bool refresh)
        {
            // These must use session to be aware of per-session SystemState.
            clientSession1.HandleImmediateNonPendingRetryStatus(refresh);
            clientSession2?.HandleImmediateNonPendingRetryStatus(refresh);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BeginUnsafe()
        {
            // We do not track any "acquired" state here; if someone mixes calls between safe and unsafe contexts, they will 
            // get the "trying to acquire already-acquired epoch" error.
            Kernel.Epoch.Resume();

            clientSession1.DoThreadStateMachineStep();
            clientSession2?.DoThreadStateMachineStep();
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void EndUnsafe()
        {
            Debug.Assert(Kernel.Epoch.ThisInstanceProtected());
            Kernel.Epoch.Suspend();
        }

        /// <inheritdoc/>
        public readonly bool IsEpochAcquired
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Kernel.Epoch.ThisInstanceProtected(); }
        }
    }
}
