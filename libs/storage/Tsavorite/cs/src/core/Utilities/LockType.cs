// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

namespace Tsavorite.core
{
    /// <summary>
    /// Type of lock taken by Tsavorite on Read, Upsert, RMW, or Delete operations, either directly or within concurrent callback operations
    /// </summary>
    public enum LockType : byte
    {
        /// <summary>
        /// No lock
        /// </summary>
        None,

        /// <summary>
        /// Exclusive lock, taken on Upsert, RMW, or Delete
        /// </summary>
        Exclusive,

        /// <summary>
        /// Shared lock, taken on Read
        /// </summary>
        Shared
    }

    /// <summary>
    /// Interface that must be implemented to participate in keyHash-based locking.
    /// </summary>
    public interface ITransactionalKey
    {
        /// <summary>
        /// The hash code for a specific key, obtained from <see cref="ITsavoriteContext.GetKeyHash{TKey}(TKey)"/>
        /// </summary>
        public long KeyHash { get; }

        /// <summary>
        /// The lock type for a specific key
        /// </summary>
        public LockType LockType { get; }
    }

    /// <summary>
    /// Lock state of a record
    /// </summary>
    public struct LockState
    {
        internal bool IsLockedExclusive;
        internal bool IsFound;
        internal ushort NumLockedShared;
        internal bool IsLockedShared => NumLockedShared > 0;

        internal bool IsLocked => IsLockedExclusive || NumLockedShared > 0;

        public override string ToString()
        {
            var locks = $"{(IsLockedExclusive ? "x" : string.Empty)}{NumLockedShared}";
            return $"found {IsFound}, locks {locks}";
        }
    }
}