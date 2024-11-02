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
    [StructLayout(LayoutKind.Explicit, Size = 11)]
    struct TxnKeyEntry : ILockableKey
    {
        [FieldOffset(0)]
        internal long keyHash;

        [FieldOffset(8)]
        internal ushort partitionId;

        [FieldOffset(10)]
        internal LockType lockType;

        #region ILockableKey
        /// <inheritdoc/>
        public readonly long KeyHash => keyHash;

        /// <inheritdoc/>
        public readonly ushort PartitionId => partitionId;

        /// <inheritdoc/>
        public readonly LockType LockType => lockType;
        #endregion ILockableKey

        /// <inheritdoc />
        public override readonly string ToString()
        {
            // The debugger often can't call the Globalization NegativeSign property so ToString() would just display the class name
            var keyHashSign = keyHash < 0 ? "-" : string.Empty;
            var absKeyHash = keyHash >= 0 ? keyHash : -keyHash;
            return $"{keyHashSign}{absKeyHash} : PartitionId {partitionId} : LockType {lockType}";
        }
    }

    internal sealed class TxnKeyEntries
    {
        // Basic keys
        int keyCount;
        TxnKeyEntry[] keys;

        internal TsavoriteKernel kernel;

        enum LockPhase
        {
            /// <summary>No lock operation is in progress</summary>
            Rest,

            /// <summary>Lock() operation is in progress</summary>
            Locking,

            /// <summary>Unlock() operation is in progress</summary>
            Unlocking
        };
        LockPhase lockPhase;

        internal TxnKeyEntries(TsavoriteKernel kernel, int capacity)
        {
            keys = new TxnKeyEntry[capacity];
            this.kernel = kernel;
        }

        public bool IsReadOnly
        {
            get
            {
                for (var i = 0; i < keyCount; i++)
                {
                    if (keys[i].lockType == LockType.Exclusive)
                        return false;
                }
                return true;
            }
        }
        public long AddKey(StoreWrapper storeWrapper, ArgSlice keyArgSlice, bool isObject, LockType type)
        {
            var keyHash = !isObject
                ? storeWrapper.store.GetKeyHash(keyArgSlice.SpanByte)
                : storeWrapper.objectStore.GetKeyHash(keyArgSlice.ToArray());

            // Grow the buffer if needed
            if (keyCount >= keys.Length)
            {
                var oldKeys = keys;
                keys = new TxnKeyEntry[keys.Length * 2];
                Array.Copy(oldKeys, keys, oldKeys.Length);
            }

            // Populate the new key slot.
            keys[keyCount].keyHash = keyHash;
            keys[keyCount].partitionId = !isObject ? storeWrapper.store.PartitionId : storeWrapper.objectStore.PartitionId;
            keys[keyCount].lockType = type;
            ++keyCount;
            return keyHash;
        }

        internal void LockAllKeys(ref KernelSession kernelSession)
        {
            lockPhase = LockPhase.Locking;

            // Note: Sort happens inside the transaction because we must have a stable Tsavorite hash table (not in GROW phase)
            // during sorting as well as during locking itself. This should not be a significant impact on performance.

            kernel.SortKeyHashes(keys, 0, keyCount);
            kernel.Lock(ref kernelSession, keys, 0, keyCount);

            lockPhase = LockPhase.Rest;
        }

        internal bool TryLockAllKeys(ref KernelSession kernelSession, TimeSpan lock_timeout)
        {
            lockPhase = LockPhase.Locking;

            // Note: Sort happens inside the transaction because we must have a stable Tsavorite hash table (not in GROW phase)
            // during sorting as well as during locking itself. This should not be a significant impact on performance.

            kernel.SortKeyHashes(keys, 0, keyCount);

            var success = kernel.TryLock(ref kernelSession, keys, 0, keyCount, lock_timeout);
            lockPhase = LockPhase.Rest;
            return success;
        }

        internal void UnlockAllKeys(ref KernelSession kernelSession)
        {
            lockPhase = LockPhase.Unlocking;
            kernel.Unlock(ref kernelSession, keys, 0, keyCount);
            keyCount = 0;
            lockPhase = LockPhase.Rest;
        }

        internal string GetLockset()
        {
            StringBuilder sb = new();

            var delimiter = string.Empty;
            for (var ii = 0; ii < keyCount; ii++)
            {
                ref var entry = ref keys[ii];
                _ = sb.Append(delimiter);
                _ = sb.Append(entry.ToString());
                delimiter = ", ";
            }

            if (sb.Length > 0)
                _ = sb.Append($" (phase: {lockPhase})");
            return sb.ToString();
        }
    }
}