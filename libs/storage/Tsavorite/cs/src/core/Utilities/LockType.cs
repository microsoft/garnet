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
    public interface ILockableKey
    {
        /// <summary>
        /// The hash code for a specific key, obtained from <see cref="ITsavoriteContext{TKey}.GetKeyHash(ref TKey)"/>
        /// </summary>
        public long KeyHash { get; }

        /// <summary>
        /// The PartitionId of the containing Tsavorite instance
        /// </summary>
        public ushort PartitionId { get; }

        /// <summary>
        /// The lock type for a specific key
        /// </summary>
        public LockType LockType { get; }
    }

    /// <summary>
    /// A utility class to carry a fixed-length key (blittable or object type) and its assciated info for Locking
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public struct FixedLengthLockableKeyStruct<TKey> : ILockableKey
    {
        /// <summary>
        /// The key that is acquiring or releasing a lock
        /// </summary>
        public TKey Key;

        #region ILockableKey
        /// <inheritdoc/>
        public long KeyHash { get; set; }

        /// <inheritdoc/>
        public ushort PartitionId { get; set; }

        /// <inheritdoc/>
        public LockType LockType { get; set; }
        #endregion ILockableKey

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthLockableKeyStruct(TKey key, long keyHash, LockType lockType, ushort partitionId = 0) : this(ref key, keyHash, lockType, partitionId) { }

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthLockableKeyStruct(ref TKey key, long keyHash, LockType lockType, ushort partitionId = 0)
        {
            Key = key;
            KeyHash = keyHash;
            LockType = lockType;
            PartitionId = partitionId;
        }

        /// <summary>
        /// Sort the passed key array for use in <see cref="TsavoriteKernel.Lock{TKernelSession, TLockableKey}(ref TKernelSession, TLockableKey[])"/>
        /// and <see cref="TsavoriteKernel.Unlock{TKernelSession, TLockableKey}(ref TKernelSession, TLockableKey[])"/>
        /// </summary>
        public static void Sort(FixedLengthLockableKeyStruct<TKey>[] keys, TsavoriteKernel kernel) => kernel.lockTable.SortKeyHashes(keys);

        /// <inheritdoc/>
        public override string ToString()
        {
            var hashStr = Utility.GetHashString(KeyHash);
            return $"key {Key}, hash {hashStr}, {LockType}";
        }
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