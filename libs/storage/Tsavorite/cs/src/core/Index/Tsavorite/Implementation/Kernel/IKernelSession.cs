// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Defines a session on the <see cref="TsavoriteKernel"/>, which is used for both transactional lock counts and for 
    /// managing ancillary epoch operations such as calling ThreadStateMachineStep for a possibly-Dual Tsavorite configuration.
    /// </summary>
    public interface IKernelSession
    {
        /// <summary>Number of shared key locks for this kernel session.</summary>
        ulong SharedTxnLockCount { get; set; }

        /// <summary>Number of exclusive key locks for this kernel session.</summary>
        ulong ExclusiveTxnLockCount { get; set; }

        void CheckTransactionIsStarted();

        void CheckTransactionIsNotStarted();

        /// <summary>Internal refresh the store(s) when a lock may be held.</summary>
        void Refresh<TKeyLocker>(ref HashEntryInfo hei) where TKeyLocker : struct, IKeyLocker;

        /// <summary>Internally handle a retry in the store(s).</summary>
        void HandleImmediateNonPendingRetryStatus(bool refresh);

        /// <summary>Register the current thread with the epoch, and step the stores' state machines.</summary>
        void BeginUnsafe();

        /// <summary>If the current thread is not already registered with the epoch, register it, and step the stores' state machines, and return true.</summary>
        bool EnsureBeginUnsafe();

        /// <summary>Unregister the current thread from the epoch.</summary>
        void EndUnsafe();

        /// <summary>Whether the current thread is registered with the epoch.</summary>
        bool IsEpochAcquired { get; }
    }
}
