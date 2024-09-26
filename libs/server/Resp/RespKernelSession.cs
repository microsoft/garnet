// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Garnet.common;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tsavorite.core;

namespace Garnet.server
{
    internal struct RespKernelSession(StorageSession session) : IKernelSession
    {
        internal readonly StorageSession storageSession = session;

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
            storageSession.lockableContext.BeginTransaction();
            if (!storageSession.objectStoreLockableContext.IsNull)
                storageSession.objectStoreLockableContext.BeginTransaction();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void EndTransaction()
        {
            CheckTransactionIsStarted();

            if (TotalTxnLockCount > 0)
                throw new TsavoriteException($"EndTransaction called with locks held: {SharedTxnLockCount} shared locks, {ExclusiveTxnLockCount} exclusive locks");

            storageSession.lockableContext.EndTransaction();
            if (!storageSession.objectStoreLockableContext.IsNull)
                storageSession.objectStoreLockableContext.EndTransaction();
            isTxnStarted = false;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void CheckTransactionIsStarted()
        {
            if (!isTxnStarted)
                throw new GarnetException("Transactional Locking method call when BeginTransaction has not been called");
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly void CheckTransactionIsNotStarted()
        {
            if (isTxnStarted)
                throw new GarnetException("BeginTransaction cannot be called twice (call EndTransaction first)");
        }


        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Refresh()
        {
            storageSession.TsavoriteKernel.Epoch.ProtectAndDrain();

            // These must use session to be aware of per-session SystemState.
            storageSession.session.Refresh();
            storageSession.objectStoreSession?.Refresh();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void HandleImmediateNonPendingRetryStatus(bool refresh)
        {
            // These must use session to be aware of per-session SystemState.
            storageSession.session.HandleImmediateNonPendingRetryStatus(refresh);
            storageSession.objectStoreSession?.HandleImmediateNonPendingRetryStatus(refresh);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void BeginUnsafe()
        {
            // We do not track any "acquired" state here; if someone mixes calls between safe and unsafe contexts, they will 
            // get the "trying to acquire already-acquired epoch" error.
            storageSession.TsavoriteKernel.Epoch.Resume();

            storageSession.session.DoThreadStateMachineStep();
            storageSession.objectStoreSession?.DoThreadStateMachineStep();
        }

        // Overload for single-context operation
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void BeginUnsafe<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator, TSessionContext>(StorageSession storageSession, TSessionContext context)
            where TSessionContext : ITsavoriteContext<TKey, TValue, TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator>
            where TFunctions : ISessionFunctions<TKey, TValue, TInput, TOutput, TContext>
            where TStoreFunctions : IStoreFunctions<TKey, TValue>
            where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
        {
            // We do not track any "acquired" state here; if someone mixes calls between safe and unsafe contexts, they will 
            // get the "trying to acquire already-acquired epoch" error.
            storageSession.TsavoriteKernel.Epoch.Resume();

            context.Session.DoThreadStateMachineStep();
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
        public readonly void EndUnsafe() => EndUnsafe(storageSession);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void EndUnsafe(StorageSession storageSession)
        {
            Debug.Assert(storageSession.TsavoriteKernel.Epoch.ThisInstanceProtected());
            storageSession.TsavoriteKernel.Epoch.Suspend();
        }

        /// <inheritdoc/>
        public bool IsEpochAcquired
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return storageSession.TsavoriteKernel.Epoch.ThisInstanceProtected(); }
        }
    }
}
