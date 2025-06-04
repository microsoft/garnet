// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    /// <summary>
    /// Provides thread management and all callbacks. A wrapper for ISessionFunctions and additional methods called by TsavoriteImpl; the wrapped
    /// ISessionFunctions methods provide additional parameters to support the wrapper functionality, then call through to the user implementations. 
    /// </summary>
    public interface ISessionLocker<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        bool IsTransactionalLocking { get; }

        bool TryLockEphemeralExclusive(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx);
        bool TryLockEphemeralShared(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx);
        void UnlockEphemeralExclusive(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx);
        void UnlockEphemeralShared(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx);
    }

    /// <summary>
    /// Basic (non-transactional) sessions must do Ephemeral locking.
    /// </summary>
    /// <remarks>
    /// This struct contains no data fields; SessionFunctionsWrapper redirects with its ClientSession.
    /// </remarks>
    internal struct BasicSessionLocker<TStoreFunctions, TAllocator> : ISessionLocker<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public bool IsTransactionalLocking => false;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockEphemeralExclusive(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            if (!store.LockTable.TryLockExclusive(ref stackCtx.hei))
                return false;
            stackCtx.recSrc.SetHasEphemeralXLock();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockEphemeralShared(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            if (!store.LockTable.TryLockShared(ref stackCtx.hei))
                return false;
            stackCtx.recSrc.SetHasEphemeralSLock();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralExclusive(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            store.LockTable.UnlockExclusive(ref stackCtx.hei);
            stackCtx.recSrc.ClearHasEphemeralXLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralShared(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            store.LockTable.UnlockShared(ref stackCtx.hei);
            stackCtx.recSrc.ClearHasEphemeralSLock();
        }
    }

    /// <summary>
    /// Transactional sessions must have already locked the record prior to an operation on it, so assert that.
    /// </summary>
    internal struct TransactionalSessionLocker<TStoreFunctions, TAllocator> : ISessionLocker<TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        public bool IsTransactionalLocking => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockEphemeralExclusive(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            Debug.Assert(store.LockTable.IsLockedExclusive(ref stackCtx.hei),
                        $"Attempting to use a non-XLocked key in a Transactional context (requesting XLock):"
                        + $" XLocked {store.LockTable.IsLockedExclusive(ref stackCtx.hei)},"
                        + $" Slocked {store.LockTable.IsLockedShared(ref stackCtx.hei)}");
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockEphemeralShared(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            Debug.Assert(store.LockTable.IsLocked(ref stackCtx.hei),
                        $"Attempting to use a non-Locked (S or X) key in a Transactional context (requesting SLock):"
                        + $" XLocked {store.LockTable.IsLockedExclusive(ref stackCtx.hei)},"
                        + $" Slocked {store.LockTable.IsLockedShared(ref stackCtx.hei)}");
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralExclusive(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            Debug.Assert(store.LockTable.IsLockedExclusive(ref stackCtx.hei),
                        $"Attempting to unlock a non-XLocked key in a Transactional context (requesting XLock):"
                        + $" XLocked {store.LockTable.IsLockedExclusive(ref stackCtx.hei)},"
                        + $" Slocked {store.LockTable.IsLockedShared(ref stackCtx.hei)}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockEphemeralShared(TsavoriteKV<TStoreFunctions, TAllocator> store, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            Debug.Assert(store.LockTable.IsLockedShared(ref stackCtx.hei),
                        $"Attempting to use a non-XLocked key in a Transactional context (requesting XLock):"
                        + $" XLocked {store.LockTable.IsLockedExclusive(ref stackCtx.hei)},"
                        + $" Slocked {store.LockTable.IsLockedShared(ref stackCtx.hei)}");
        }
    }
}