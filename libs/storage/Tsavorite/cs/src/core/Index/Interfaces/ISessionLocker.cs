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
    public interface ISessionLocker<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        bool IsManualLocking { get; }

        bool TryLockTransientExclusive(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx);
        bool TryLockTransientShared(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx);
        void UnlockTransientExclusive(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx);
        void UnlockTransientShared(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx);
    }

    /// <summary>
    /// Basic (non-lockable) sessions must do transient locking.
    /// </summary>
    /// <remarks>
    /// This struct contains no data fields; SessionFunctionsWrapper redirects with its ClientSession.
    /// </remarks>
    internal struct BasicSessionLocker<TKey, TValue, TStoreFunctions, TAllocator> : ISessionLocker<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        public bool IsManualLocking => false;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientExclusive(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            if (!store.LockTable.TryLockExclusive(ref stackCtx.hei))
                return false;
            stackCtx.recSrc.SetHasTransientXLock();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientShared(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            if (!store.LockTable.TryLockShared(ref stackCtx.hei))
                return false;
            stackCtx.recSrc.SetHasTransientSLock();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientExclusive(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            store.LockTable.UnlockExclusive(ref stackCtx.hei);
            stackCtx.recSrc.ClearHasTransientXLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientShared(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            store.LockTable.UnlockShared(ref stackCtx.hei);
            stackCtx.recSrc.ClearHasTransientSLock();
        }
    }

    /// <summary>
    /// Lockable sessions are manual locking and thus must have already locked the record prior to an operation on it, so assert that.
    /// </summary>
    internal struct LockableSessionLocker<TKey, TValue, TStoreFunctions, TAllocator> : ISessionLocker<TKey, TValue, TStoreFunctions, TAllocator>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        public bool IsManualLocking => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientExclusive(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            Debug.Assert(store.LockTable.IsLockedExclusive(ref stackCtx.hei),
                        $"Attempting to use a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {store.LockTable.IsLockedExclusive(ref stackCtx.hei)},"
                        + $" Slocked {store.LockTable.IsLockedShared(ref stackCtx.hei)}");
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientShared(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            Debug.Assert(store.LockTable.IsLocked(ref stackCtx.hei),
                        $"Attempting to use a non-Locked (S or X) key in a Lockable context (requesting SLock):"
                        + $" XLocked {store.LockTable.IsLockedExclusive(ref stackCtx.hei)},"
                        + $" Slocked {store.LockTable.IsLockedShared(ref stackCtx.hei)}");
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientExclusive(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            Debug.Assert(store.LockTable.IsLockedExclusive(ref stackCtx.hei),
                        $"Attempting to unlock a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {store.LockTable.IsLockedExclusive(ref stackCtx.hei)},"
                        + $" Slocked {store.LockTable.IsLockedShared(ref stackCtx.hei)}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientShared(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            Debug.Assert(store.LockTable.IsLockedShared(ref stackCtx.hei),
                        $"Attempting to use a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {store.LockTable.IsLockedExclusive(ref stackCtx.hei)},"
                        + $" Slocked {store.LockTable.IsLockedShared(ref stackCtx.hei)}");
        }
    }
}