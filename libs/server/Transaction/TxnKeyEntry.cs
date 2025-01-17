﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Text;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = ObjectAllocator<IGarnetObject, StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<IGarnetObject, SpanByteComparer, DefaultRecordDisposer<IGarnetObject>>;

    /// <summary>
    /// Entry for a key to lock and unlock in transactions
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 10)]
    struct TxnKeyEntry : ITransactionalKey
    {
        [FieldOffset(0)]
        internal long keyHash;

        [FieldOffset(8)]
        internal bool isObject;

        [FieldOffset(9)]
        internal LockType lockType;

        #region ITransactionalKey
        /// <inheritdoc/>
        public readonly long KeyHash { get => keyHash; }

        /// <inheritdoc/>
        public readonly LockType LockType { get => lockType; }
        #endregion ITransactionalKey

        /// <inheritdoc />
        public override readonly string ToString()
        {
            // The debugger often can't call the Globalization NegativeSign property so ToString() would just display the class name
            var keyHashSign = keyHash < 0 ? "-" : string.Empty;
            var absKeyHash = keyHash >= 0 ? keyHash : -keyHash;
            return $"{keyHashSign}{absKeyHash}:{(isObject ? "obj" : "raw")}:{(lockType == LockType.None ? "-" : (lockType == LockType.Shared ? "s" : "x"))}";
        }
    }

    internal sealed class TxnKeyEntries
    {
        // Basic keys
        int keyCount;
        int mainKeyCount;
        TxnKeyEntry[] keys;

        bool mainStoreKeyLocked;
        bool objectStoreKeyLocked;

        readonly TxnKeyComparison comparison;

        public int phase;

        internal TxnKeyEntries(int initialCount, TransactionalContext<SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> transactionalContext,
                TransactionalContext<IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreTransactionalContext)
        {
            keys = GC.AllocateArray<TxnKeyEntry>(initialCount, pinned: true);
            // We sort a single array for speed, and the sessions use the same sorting logic,
            comparison = new(transactionalContext, objectStoreTransactionalContext);
        }

        public bool IsReadOnly
        {
            get
            {
                var readOnly = true;
                for (int i = 0; i < keyCount; i++)
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

        public void AddKey(ArgSlice keyArgSlice, bool isObject, LockType type)
        {
            var keyHash = !isObject
                ? comparison.transactionalContext.GetKeyHash(keyArgSlice.SpanByte)
                : comparison.objectStoreTransactionalContext.GetKeyHash(keyArgSlice.SpanByte);

            // Grow the buffer if needed
            if (keyCount >= keys.Length)
            {
                var oldKeys = keys;
                keys = new TxnKeyEntry[keys.Length * 2];
                Array.Copy(oldKeys, keys, oldKeys.Length);
            }

            // Populate the new key slot.
            keys[keyCount].keyHash = keyHash;
            keys[keyCount].isObject = isObject;
            keys[keyCount].lockType = type;
            ++keyCount;
            if (!isObject)
                ++mainKeyCount;
        }

        internal void LockAllKeys()
        {
            phase = 1;

            // Note: We've moved Sort inside the transaction because we must have a stable Tsavorite hash table (not in GROW phase)
            // during sorting as well as during locking itself. This should not be a significant impact on performance.

            // This does not call Tsavorite's SortKeyHashes because we need to consider isObject as well.
            MemoryExtensions.Sort(keys.AsSpan().Slice(0, keyCount), comparison.comparisonDelegate);

            // Issue main store locks
            if (mainKeyCount > 0)
            {
                comparison.transactionalContext.Lock(keys, 0, mainKeyCount);
                mainStoreKeyLocked = true;
            }

            // Issue object store locks
            if (mainKeyCount < keyCount)
            {
                comparison.objectStoreTransactionalContext.Lock(keys, mainKeyCount, keyCount - mainKeyCount);
                objectStoreKeyLocked = true;
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

            // Issue main store locks
            // TryLock will unlock automatically in case of partial failure
            if (mainKeyCount > 0)
            {
                mainStoreKeyLocked = comparison.transactionalContext.TryLock(keys, 0, mainKeyCount, lock_timeout);
                if (!mainStoreKeyLocked)
                {
                    phase = 0;
                    return false;
                }
            }

            // Issue object store locks
            // TryLock will unlock automatically in case of partial failure
            if (mainKeyCount < keyCount)
            {
                objectStoreKeyLocked = comparison.objectStoreTransactionalContext.TryLock(keys, mainKeyCount, keyCount - mainKeyCount, lock_timeout);
                if (!objectStoreKeyLocked)
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
            if (mainStoreKeyLocked && mainKeyCount > 0)
                comparison.transactionalContext.Unlock(keys, 0, mainKeyCount);
            if (objectStoreKeyLocked && mainKeyCount < keyCount)
                comparison.objectStoreTransactionalContext.Unlock(keys, mainKeyCount, keyCount - mainKeyCount);
            mainKeyCount = 0;
            keyCount = 0;
            mainStoreKeyLocked = false;
            objectStoreKeyLocked = false;
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