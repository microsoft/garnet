// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        /// <summary>
        /// Manual Lock operation for key-based locking. Locks the record corresponding to 'key'.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="lockType">Whether the lock is shared or exclusive</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalLock(ref Key key, LockType lockType)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLock must have protected epoch");
            Debug.Assert(LockTable.IsEnabled, "ManualLockTable must be enabled for InternalLock");

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));
            FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlog);

            if (!LockTable.TryLockManual(ref key, ref stackCtx.hei, lockType))
                return OperationStatus.RETRY_LATER;
            return OperationStatus.SUCCESS;
        }

        /// <summary>
        /// Manual Lock operation for key-based locking. Unlocks the record corresponding to 'key'.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="lockType">Whether the lock is shared or exclusive</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalUnlock(ref Key key, LockType lockType)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLock must have protected epoch");
            Debug.Assert(LockTable.IsEnabled, "ManualLockTable must be enabled for InternalLock");

            OperationStackContext<Key, Value> stackCtx = new(comparer.GetHashCode64(ref key));
            FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlog);

            LockTable.Unlock(ref key, ref stackCtx.hei, lockType);
        }

        /// <summary>
        /// Manual Lock operation for <see cref="HashBucket"/> locking. Locks the buckets corresponding to 'keys'.
        /// </summary>
        /// <param name="keyHash">Hash code of the key to be locked or unlocked.</param>
        /// <param name="lockType">Whether the lock is shared or exclusive</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalLock(long keyHash, LockType lockType)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLock must have protected epoch");
            Debug.Assert(LockTable.IsEnabled, "ManualLockTable must be enabled for InternalLock");

            if (!LockTable.TryLockManual(keyHash, lockType))
                return OperationStatus.RETRY_LATER;
            return OperationStatus.SUCCESS;
        }

        /// <summary>
        /// Manual Lock operation for <see cref="HashBucket"/> locking. Locks the buckets corresponding to 'keys'.
        /// </summary>
        /// <param name="keyHash">Hash code of the key to be locked or unlocked.</param>
        /// <param name="lockType">Whether the lock is shared or exclusive</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InternalUnlock(long keyHash, LockType lockType)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLock must have protected epoch");
            Debug.Assert(LockTable.IsEnabled, "ManualLockTable must be enabled for InternalLock");

            LockTable.Unlock(keyHash, lockType);
        }

        /// <summary>
        /// Manual Lock promotion for <see cref="HashBucket"/> locking. Promotes the lock for 'key' from Shared to Exclusive.
        /// </summary>
        /// <param name="keyHash">Hash code of the key to be locked or unlocked.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalPromoteLock(long keyHash)
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "InternalLock must have protected epoch");
            Debug.Assert(LockTable.IsEnabled, "ManualLockTable must be enabled for InternalLock");

            if (!LockTable.TryPromoteLockManual(keyHash))
                return OperationStatus.RETRY_LATER;
            return OperationStatus.SUCCESS;
        }
    }
}