// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;

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
        /// The hash code for a specific key, obtained from <see cref="ITsavoriteContext.GetKeyHash(ReadOnlySpan{byte})"/>
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
    public struct FixedLengthTransactionalKeyStruct : ITransactionalKey
    {
        /// <summary>
        /// The key that is acquiring or releasing a lock
        /// </summary>
        public PinnedSpanByte Key;

        #region ITransactionalKey
        /// <inheritdoc/>
        public long KeyHash { get; set; }

        /// <inheritdoc/>
        public LockType LockType { get; set; }
        #endregion ITransactionalKey

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthTransactionalKeyStruct(ReadOnlySpan<byte> key, LockType lockType, ITsavoriteContext context)
        {
            Key = PinnedSpanByte.FromPinnedSpan(key);
            LockType = lockType;
            KeyHash = context.GetKeyHash(key);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthTransactionalKeyStruct(ReadOnlySpan<byte> key, long keyHash, LockType lockType, ITransactionalContext context)
        {
            Key = PinnedSpanByte.FromPinnedSpan(key);
            KeyHash = keyHash;
            LockType = lockType;
        }

        /// <summary>
        /// Sort the passed key array for use in <see cref="ITransactionalContext.Lock"/>
        /// and <see cref="ITransactionalContext.Unlock"/>
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="context"></param>
        public static void Sort(FixedLengthTransactionalKeyStruct[] keys, ITransactionalContext context) => context.SortKeyHashes<FixedLengthTransactionalKeyStruct>(keys);

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