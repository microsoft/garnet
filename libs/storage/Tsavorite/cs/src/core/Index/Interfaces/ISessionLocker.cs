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
    public interface ISessionLocker
    {
        bool IsManualLocking { get; }

        bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei);
        bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei);
        void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei);
        void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei);
    }
     
    /// <summary>
    /// Basic (non-lockable) sessions must do transient locking.
    /// </summary>
    /// <remarks>
    /// This struct contains no data fields; SessionFunctionsWrapper redirects with its ClientSession.
    /// </remarks>
    internal struct BasicSessionLocker : ISessionLocker
    {
        public bool IsManualLocking => false;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            if (!kernel.lockTable.TryLockExclusive(ref hei))
                return false;
            hei.SetHasTransientXLock();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            if (!kernel.lockTable.TryLockShared(ref hei))
                return false;
            hei.SetHasTransientSLock();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            kernel.lockTable.UnlockExclusive(ref hei);
            hei.ClearHasTransientXLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            kernel.lockTable.UnlockShared(ref hei);
            hei.ClearHasTransientSLock();
        }
    }

    /// <summary>
    /// Lockable sessions are manual locking and thus must have already locked the record prior to an operation on it, so assert that.
    /// </summary>
    internal struct LockableSessionLocker : ISessionLocker
    {
        public bool IsManualLocking => true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(kernel.lockTable.IsLockedExclusive(ref hei),
                        $"Attempting to use a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(kernel.lockTable.IsLocked(ref hei),
                        $"Attempting to use a non-Locked (S or X) key in a Lockable context (requesting SLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(kernel.lockTable.IsLockedExclusive(ref hei),
                        $"Attempting to unlock a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(kernel.lockTable.IsLockedShared(ref hei),
                        $"Attempting to use a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
        }
    }
}