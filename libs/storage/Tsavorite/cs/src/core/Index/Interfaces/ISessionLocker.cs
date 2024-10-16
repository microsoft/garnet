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
        static abstract bool IsTransactional { get; }

        static abstract bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei);
        static abstract bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei);
        static abstract void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei);
        static abstract void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei);
    }
     
    /// <summary>
    /// Basic (non-lockable) sessions must do transient locking.
    /// </summary>
    /// <remarks>
    /// This struct contains no data fields; SessionFunctionsWrapper redirects with its ClientSession.
    /// </remarks>
    internal struct TransientSessionLocker : ISessionLocker
    {
        public static bool IsTransactional => false;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            // Check for existing lock due to RETRY and Dual
            if (!hei.HasTransientXLock)
            {
                if (!kernel.lockTable.TryLockExclusive(ref hei))
                    return false;
                hei.SetHasTransientXLock();
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            // Check for existing lock due to RETRY and Dual
            if (!hei.HasTransientSLock)
            {
                if (!kernel.lockTable.TryLockShared(ref hei))
                    return false;
                hei.SetHasTransientSLock();
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(!hei.HasTransientSLock, "Cannot XUnlock a session SLock");
            if (hei.HasTransientXLock)
            {
                kernel.lockTable.UnlockExclusive(ref hei);
                hei.ClearHasTransientXLock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(!hei.HasTransientXLock, "Cannot SUnlock a session XLock");
            if (hei.HasTransientSLock)
            {
                kernel.lockTable.UnlockShared(ref hei);
                hei.ClearHasTransientSLock();
            }
        }
    }

    /// <summary>
    /// Lockable sessions are manual locking and thus must have already locked the record prior to an operation on it, so assert that.
    /// </summary>
    internal struct TransactionalSessionLocker : ISessionLocker
    {
        public static bool IsTransactional => true;

        public static bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(kernel.lockTable.IsLockedExclusive(ref hei),
                        $"Attempting to use a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
            return true;
        }

        public static bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(kernel.lockTable.IsLocked(ref hei),
                        $"Attempting to use a non-Locked (S or X) key in a Lockable context (requesting SLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
            return true;
        }

        public static void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(kernel.lockTable.IsLockedExclusive(ref hei),
                        $"Attempting to unlock a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
        }

        public static void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
        {
            Debug.Assert(kernel.lockTable.IsLocked(ref hei),
                        $"Attempting to use a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
        }
    }

    /// <summary>
    /// Does no locking
    /// </summary>
    internal struct NoKeyLocker : ISessionLocker
    {
        public static bool IsTransactional => false;

        public static bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei) => true;

        public static bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei) => true;

        public static void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei) { }

        public static void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei) { }
    }
}