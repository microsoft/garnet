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
        /// The hash code for a specific key, obtained from <see cref="ITsavoriteContext{TKey}.GetKeyHash(ref TKey)"/>
        /// </summary>
        public long KeyHash { get; }

        /// <summary>
        /// The lock type for a specific key
        /// </summary>
        public LockType LockType { get; }
    }

    /// <summary>
    /// A utility class to carry a fixed-length key (blittable or object type) and its assciated info for Locking
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public struct FixedLengthTransactionalKeyStruct<TKey> : ITransactionalKey
    {
        /// <summary>
        /// The key that is acquiring or releasing a lock
        /// </summary>
        public TKey Key;

        #region ITransactionalKey
        /// <inheritdoc/>
        public long KeyHash { get; set; }

        /// <inheritdoc/>
        public LockType LockType { get; set; }
        #endregion ITransactionalKey

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthTransactionalKeyStruct(TKey key, LockType lockType, ITsavoriteContext<TKey> context) : this(ref key, lockType, context) { }

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthTransactionalKeyStruct(ref TKey key, LockType lockType, ITsavoriteContext<TKey> context)
        {
            Key = key;
            LockType = lockType;
            KeyHash = context.GetKeyHash(ref key);
        }
        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthTransactionalKeyStruct(TKey key, long keyHash, LockType lockType, ITransactionalContext<TKey> context) : this(ref key, keyHash, lockType, context) { }

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthTransactionalKeyStruct(ref TKey key, long keyHash, LockType lockType, ITransactionalContext<TKey> context)
        {
            Key = key;
            KeyHash = keyHash;
            LockType = lockType;
        }

        /// <summary>
        /// Sort the passed key array for use in <see cref="ITransactionalContext{TKey}.Lock{TTransactionalKey}(TTransactionalKey[])"/>
        /// and <see cref="ITransactionalContext{TKey}.Unlock{TTransactionalKey}(TTransactionalKey[])"/>
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="context"></param>
        public static void Sort(FixedLengthTransactionalKeyStruct<TKey>[] keys, ITransactionalContext<TKey> context) => context.SortKeyHashes(keys);

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