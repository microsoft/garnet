// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Tsavorite.core;

namespace Tsavorite.test
{
    /// <summary>
    /// A utility class to carry a fixed-length key (blittable or object type) and its assciated info for Locking
    /// </summary>
    public struct FixedLengthTransactionalKeyStruct : ITransactionalKey
    {
        /// <summary>
        /// The key that is acquiring or releasing a lock
        /// </summary>
        public TestSpanByteKey Key;

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
            Key = TestSpanByteKey.FromPinnedSpan(key);
            LockType = lockType;
            KeyHash = context.GetKeyHash(Key);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthTransactionalKeyStruct(ReadOnlySpan<byte> key, long keyHash, LockType lockType, ITransactionalContext context)
        {
            Key = TestSpanByteKey.FromPinnedSpan(key);
            KeyHash = keyHash;
            LockType = lockType;
        }

        /// <summary>
        /// Sort the passed key array for use in <see cref="ITransactionalContext.Lock"/>
        /// and <see cref="ITransactionalContext.Unlock"/>
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="context"></param>
        public static void Sort(FixedLengthTransactionalKeyStruct[] keys, ITransactionalContext context) => context.SortKeyHashes(keys);

        /// <inheritdoc/>
        public override string ToString()
        {
            var hashStr = Utility.GetHashString(KeyHash);
            return $"key {Key}, hash {hashStr}, {LockType}";
        }
    }
}