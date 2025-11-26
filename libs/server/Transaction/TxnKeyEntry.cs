// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Text;
using Tsavorite.core;

namespace Garnet.server
{
    using MainStoreAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
    using MainStoreFunctions = StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>;

    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Entry for a key to lock and unlock in transactions
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 10)]
    struct TxnKeyEntry : ILockableKey
    {
        [FieldOffset(0)]
        internal long keyHash;

        [FieldOffset(8)]
        internal bool isObject;

        [FieldOffset(9)]
        internal LockType lockType;

        #region ILockableKey
        /// <inheritdoc/>
        public long KeyHash { get => keyHash; }

        /// <inheritdoc/>
        public LockType LockType { get => lockType; }
        #endregion ILockableKey

        /// <inheritdoc />
        public override string ToString()
        {
            // The debugger often can't call the Globalization NegativeSign property so ToString() would just display the class name
            var keyHashSign = keyHash < 0 ? "-" : string.Empty;
            var absKeyHash = this.keyHash >= 0 ? this.keyHash : -this.keyHash;
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

        internal TxnKeyEntries(int initialCount, LockableContext<SpanByte, SpanByte, RawStringInput, SpanByteAndMemory, long, MainSessionFunctions, MainStoreFunctions, MainStoreAllocator> lockableContext,
                LockableContext<byte[], IGarnetObject, ObjectInput, GarnetObjectStoreOutput, long, ObjectSessionFunctions, ObjectStoreFunctions, ObjectStoreAllocator> objectStoreLockableContext)
        {
            keys = GC.AllocateArray<TxnKeyEntry>(initialCount, pinned: true);
            // We sort a single array for speed, and the sessions use the same sorting logic,
            comparison = new(lockableContext, objectStoreLockableContext);
        }

        public bool IsReadOnly
        {
            get
            {
                bool readOnly = true;
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
                ? comparison.lockableContext.GetKeyHash(keyArgSlice.SpanByte)
                : comparison.objectStoreLockableContext.GetKeyHash(keyArgSlice.ToArray());

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
                comparison.lockableContext.Lock<TxnKeyEntry>(keys.AsSpan()[..mainKeyCount]);
                mainStoreKeyLocked = true;
            }

            // Issue object store locks
            if (mainKeyCount < keyCount)
            {
                comparison.objectStoreLockableContext.Lock<TxnKeyEntry>(keys.AsSpan().Slice(mainKeyCount, keyCount - mainKeyCount));
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
                mainStoreKeyLocked = comparison.lockableContext.TryLock<TxnKeyEntry>(keys.AsSpan()[..mainKeyCount], lock_timeout);
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
                objectStoreKeyLocked = comparison.objectStoreLockableContext.TryLock<TxnKeyEntry>(keys.AsSpan().Slice(mainKeyCount, keyCount - mainKeyCount), lock_timeout);
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
                comparison.lockableContext.Unlock<TxnKeyEntry>(keys.AsSpan()[..mainKeyCount]);
            if (objectStoreKeyLocked && mainKeyCount < keyCount)
                comparison.objectStoreLockableContext.Unlock<TxnKeyEntry>(keys.AsSpan().Slice(mainKeyCount, keyCount - mainKeyCount));
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
                sb.Append(delimiter);
                sb.Append(entry.ToString());
            }

            if (sb.Length > 0)
                sb.Append($" (phase: {(phase == 0 ? "none" : (phase == 1 ? "lock" : "unlock"))}))");
            return sb.ToString();
        }
    }
}