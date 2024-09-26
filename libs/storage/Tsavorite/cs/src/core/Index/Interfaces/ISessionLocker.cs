// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    // Whether an instance is a Dual context and if so which role
    public enum DualRole { None, First, Second };

    /// <summary>
    /// Provides thread management and all callbacks. A wrapper for ISessionFunctions and additional methods called by TsavoriteImpl; the wrapped
    /// ISessionFunctions methods provide additional parameters to support the wrapper functionality, then call through to the user implementations. 
    /// </summary>
    public interface ISessionLocker
    {
        bool IsTransactionalLocking { get; }
        DualRole DualRole { get; }

        bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei);
        bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei);
        void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei, bool isRetry);
        void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei, bool isRetry);
    }
     
    /// <summary>
    /// Basic (non-lockable) sessions must do transient locking.
    /// </summary>
    /// <remarks>
    /// This struct contains no data fields; SessionFunctionsWrapper redirects with its ClientSession.
    /// </remarks>
    internal struct TransientSessionLocker : ISessionLocker
    {
        public readonly bool IsTransactionalLocking => false;
        public readonly DualRole DualRole => DualRole.None;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
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
        public bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
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
        public void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei, bool isRetry)
        {
            Debug.Assert(!hei.HasTransientSLock, "Cannot XUnlock a session SLock");
            if (hei.HasTransientXLock)
            {
                kernel.lockTable.UnlockExclusive(ref hei);
                hei.ClearHasTransientXLock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei, bool isRetry)
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
        public readonly bool IsTransactionalLocking => true;
        public readonly DualRole DualRole => DualRole.None;

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
        public void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei, bool isRetry)
        {
            Debug.Assert(kernel.lockTable.IsLockedExclusive(ref hei),
                        $"Attempting to unlock a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei, bool isRetry)
        {
            Debug.Assert(kernel.lockTable.IsLocked(ref hei),
                        $"Attempting to use a non-XLocked key in a Lockable context (requesting XLock):"
                        + $" XLocked {kernel.lockTable.IsLockedExclusive(ref hei)},"
                        + $" Slocked {kernel.lockTable.IsLockedShared(ref hei)}");
        }
    }

    /// <summary>
    /// Basic (non-lockable) sessions must do transient locking.
    /// </summary>
    /// <remarks>
    /// This struct contains no data fields; SessionFunctionsWrapper redirects with its ClientSession.
    /// </remarks>
    internal struct DualSessionLocker : ISessionLocker
    {
        public readonly bool IsTransactionalLocking => false;
        public DualRole DualRole { get; private set; }

        internal DualSessionLocker(DualRole dualRole) => DualRole = dualRole;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei)
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
        public bool TryLockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei)
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
        public void UnlockTransientExclusive(TsavoriteKernel kernel, ref HashEntryInfo hei, bool isRetry)
        {
            Debug.Assert(!hei.HasTransientSLock, "Cannot XUnlock a session SLock");

            // Do no unlock if this is the first of a dual Tsavorite and we are not retrying; we want the lock to persist then.
            if (hei.HasTransientXLock && (isRetry || DualRole != DualRole.First))
            {
                kernel.lockTable.UnlockExclusive(ref hei);
                hei.ClearHasTransientXLock();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockTransientShared(TsavoriteKernel kernel, ref HashEntryInfo hei, bool isRetry)
        {
            Debug.Assert(!hei.HasTransientXLock, "Cannot SUnlock a session XLock");

            // Do no unlock if this is the first of a dual Tsavorite and we are not retrying; we want the lock to persist then.
            if (hei.HasTransientSLock && (isRetry || DualRole != DualRole.First))
            {
                kernel.lockTable.UnlockShared(ref hei);
                hei.ClearHasTransientSLock();
            }
        }
    }
}