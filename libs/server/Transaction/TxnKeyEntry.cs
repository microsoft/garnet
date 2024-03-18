// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.InteropServices;
using System.Text;
using Tsavorite.core;

namespace Garnet.server
{
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

    internal class TxnKeyEntries
    {
        // Basic keys
        int keyCount;
        int mainKeyCount;
        TxnKeyEntry[] keys;

        bool mainStoreKeyLocked;
        bool objectStoreKeyLocked;

        readonly TxnKeyEntryComparer comparer;

        public int phase;

        internal TxnKeyEntries(int initialCount, LockableContext<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, long, MainStoreFunctions> lockableContext, LockableContext<byte[], IGarnetObject, SpanByte, GarnetObjectStoreOutput, long, ObjectStoreFunctions> objectStoreLockableContext)
        {
            keys = new TxnKeyEntry[initialCount];
            // We sort a single array for speed, and the sessions use the same sorting logic,
            this.comparer = new(lockableContext, objectStoreLockableContext);
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
                ? comparer.lockableContext.GetKeyHash(keyArgSlice.SpanByte)
                : comparer.objectStoreLockableContext.GetKeyHash(keyArgSlice.ReadOnlySpan.ToArray());

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
            Array.Sort(keys, 0, keyCount, comparer);

            // Issue main store locks
            if (mainKeyCount > 0)
            {
                comparer.lockableContext.Lock(keys, 0, mainKeyCount);
                mainStoreKeyLocked = true;
            }

            // Issue object store locks
            if (mainKeyCount < keyCount)
            {
                comparer.objectStoreLockableContext.Lock(keys, mainKeyCount, keyCount - mainKeyCount);
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
            Array.Sort(keys, 0, keyCount, comparer);

            // Issue main store locks
            // TryLock will unlock automatically in case of partial failure
            if (mainKeyCount > 0)
            {
                mainStoreKeyLocked = comparer.lockableContext.TryLock(keys, 0, mainKeyCount, lock_timeout);
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
                objectStoreKeyLocked = comparer.objectStoreLockableContext.TryLock(keys, mainKeyCount, keyCount - mainKeyCount, lock_timeout);
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
                comparer.lockableContext.Unlock(keys, 0, mainKeyCount);
            if (objectStoreKeyLocked && mainKeyCount < keyCount)
                comparer.objectStoreLockableContext.Unlock(keys, mainKeyCount, keyCount - mainKeyCount);
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