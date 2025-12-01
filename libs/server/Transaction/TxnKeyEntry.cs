// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Text;
using Tsavorite.core;

namespace Garnet.server
{
    using StoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using StoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Entry for a key to lock and unlock in transactions
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 9)]
    struct TxnKeyEntry : ITransactionalKey
    {
        [FieldOffset(0)]
        internal long keyHash;

        [FieldOffset(8)]
        internal LockType lockType;

        #region ITransactionalKey
        /// <inheritdoc/>
        public readonly long KeyHash => keyHash;

        /// <inheritdoc/>
        public readonly LockType LockType => lockType;
        #endregion ITransactionalKey

        /// <inheritdoc />
        public override readonly string ToString()
        {
            // The debugger often can't call the Globalization NegativeSign property so ToString() would just display the class name
            var keyHashSign = keyHash < 0 ? "-" : string.Empty;
            var absKeyHash = keyHash >= 0 ? keyHash : -keyHash;
            return $"{keyHashSign}{absKeyHash}:{(lockType == LockType.None ? "-" : (lockType == LockType.Shared ? "s" : "x"))}";
        }
    }

    internal sealed class TxnKeyEntries
    {
        // Basic keys
        int keyCount;
        TxnKeyEntry[] keys;

        bool unifiedStoreKeyLocked;

        readonly TxnKeyComparison comparison;

        public int phase;

        internal TxnKeyEntries(int initialCount,
                TransactionalContext<UnifiedInput, UnifiedOutput, long, UnifiedSessionFunctions, StoreFunctions, StoreAllocator> unifiedTransactionalContext)
        {
            keys = GC.AllocateArray<TxnKeyEntry>(initialCount, pinned: true);
            // We sort a single array for speed, and the sessions use the same sorting logic,
            comparison = new(unifiedTransactionalContext);
        }

        public bool IsReadOnly
        {
            get
            {
                var readOnly = true;
                for (var i = 0; i < keyCount; i++)
                {
                    if (keys[i].lockType == LockType.Exclusive)
                    {
                        readOnly = false;
                        break;
                    }
                }
                return readOnly;
            }
        }

        public void AddKey(PinnedSpanByte keyArgSlice, LockType type)
        {
            var keyHash = comparison.UnifiedTransactionalContext.GetKeyHash(keyArgSlice.ReadOnlySpan);

            // Grow the buffer if needed
            if (keyCount >= keys.Length)
            {
                var oldKeys = keys;
                keys = new TxnKeyEntry[keys.Length * 2];
                Array.Copy(oldKeys, keys, oldKeys.Length);
            }

            // Populate the new key slot.
            keys[keyCount].keyHash = keyHash;
            keys[keyCount].lockType = type;
            ++keyCount;
        }

        internal void LockAllKeys()
        {
            phase = 1;

            // Note: We've moved Sort inside the transaction because we must have a stable Tsavorite hash table (not in GROW phase)
            // during sorting as well as during locking itself. This should not be a significant impact on performance.

            // This does not call Tsavorite's SortKeyHashes because we need to consider isObject as well.
            MemoryExtensions.Sort(keys.AsSpan().Slice(0, keyCount), comparison.comparisonDelegate);

            // Issue unified store locks
            if (keyCount > 0)
            {
                comparison.UnifiedTransactionalContext.Lock<TxnKeyEntry>(keys.AsSpan().Slice(0, keyCount));
                unifiedStoreKeyLocked = true;
            }

            phase = 0;
        }

        internal bool TryLockAllKeys(TimeSpan lock_timeout)
        {
            phase = 1;

            // Note: We've moved Sort inside the transaction because we must have a stable Tsavorite hash table (not in GROW phase)
            // during sorting as well as during locking itself. This should not be a significant impact on performance.

            // This does not call Tsavorite's SortKeyHashes because we need to consider isObject as well.
            MemoryExtensions.Sort(keys.AsSpan().Slice(0, keyCount), comparison.comparisonDelegate);

            // Issue unified store locks
            // TryLock will unlock automatically in case of partial failure
            if (keyCount > 0)
            {
                unifiedStoreKeyLocked = comparison.UnifiedTransactionalContext.TryLock<TxnKeyEntry>(keys.AsSpan().Slice(0, keyCount), lock_timeout);
                if (!unifiedStoreKeyLocked)
                {
                    phase = 0;
                    return false;
                }
            }

            phase = 0;
            return true;
        }

        internal void UnlockAllKeys()
        {
            phase = 2;
            if (unifiedStoreKeyLocked && keyCount > 0)
                comparison.UnifiedTransactionalContext.Unlock<TxnKeyEntry>(keys.AsSpan().Slice(0, keyCount));
            keyCount = 0;
            unifiedStoreKeyLocked = false;
            phase = 0;
        }

        internal string GetLockset()
        {
            StringBuilder sb = new();

            var delimiter = string.Empty;
            for (int ii = 0; ii < keyCount; ii++)
            {
                ref var entry = ref keys[ii];
                _ = sb.Append(delimiter);
                _ = sb.Append(entry.ToString());
            }

            if (sb.Length > 0)
                _ = sb.Append($" (phase: {(phase == 0 ? "none" : (phase == 1 ? "lock" : "unlock"))}))");
            return sb.ToString();
        }
    }
}