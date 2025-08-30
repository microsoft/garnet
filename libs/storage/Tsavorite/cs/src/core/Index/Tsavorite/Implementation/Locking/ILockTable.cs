// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

namespace Tsavorite.core
{
    /// <summary>
    /// LockTable interface definition (for both Transactional and Ephemeral)
    /// </summary>
    public interface ILockTable : IDisposable
    {
        /// <summary>
        /// Whether the lock table is enabled.
        /// </summary>
        public bool IsEnabled { get; }

        /// <summary>
        /// Try to acquire a shared lock for <paramref name="hei"/>. 
        /// </summary>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        public bool TryLockShared(ref HashEntryInfo hei);

        /// <summary>
        /// Try to acquire an exclusive lock for <paramref name="hei"/>.
        /// </summary>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        public bool TryLockExclusive(ref HashEntryInfo hei);

        /// <summary>
        /// Release a shared lock on the <paramref name="hei"/>.
        /// </summary>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        public void UnlockShared(ref HashEntryInfo hei);

        /// <summary>
        /// Release an exclusive lock on <paramref name="hei"/>.
        /// </summary>
        /// <param name="hei">The hash table entry info of the key to lock</param>
        public void UnlockExclusive(ref HashEntryInfo hei);

        /// <summary>
        /// Return whether the <paramref name="hei"/> is S locked
        /// </summary>
        public bool IsLockedShared(ref HashEntryInfo hei);

        /// <summary>
        /// Return whether the <paramref name="hei"/> is X locked
        /// </summary>
        public bool IsLockedExclusive(ref HashEntryInfo hei);

        /// <summary>
        /// Return whether an the key is S or X locked
        /// </summary>
        public bool IsLocked(ref HashEntryInfo hei);

        /// <summary>
        /// Return the Lock state of the key.
        /// </summary>
        public LockState GetLockState(ref HashEntryInfo hei);
    }
}