// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tsavorite.core;

namespace Tsavorite.test
{

    public readonly unsafe struct TestSpanByteKey : IKey
    {
        private readonly void* ptr;
        private readonly int len;

        public readonly bool IsPinned => true;

        public readonly ReadOnlySpan<byte> KeyBytes => new(ptr, len);

        private TestSpanByteKey(void* ptr, int len)
        {
            this.ptr = ptr;
            this.len = len;
        }

        public static TestSpanByteKey FromPinnedSpan(ReadOnlySpan<byte> key)
        {
            var ptr = Unsafe.AsPointer(ref MemoryMarshal.GetReference(key));
            var len = key.Length;

            return new(ptr, len);
        }
    }

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
