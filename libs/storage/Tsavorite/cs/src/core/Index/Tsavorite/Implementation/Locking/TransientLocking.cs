// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryTransientXLock<TInput, TOutput, TContext, TKeyLocker>(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, out OperationStatus status)
            where TKeyLocker : struct, IKeyLocker
        {
            if (TKeyLocker.TryLockTransientExclusive(Kernel, ref stackCtx.hei))
            {
                stackCtx.ResetTransientLockTimeout();   // In case of retry
                status = OperationStatus.SUCCESS;
                return true;
            }
            if (stackCtx.IsTransientLockTimeout(TransientLockTimeout))
                throw new TsavoriteException($"Transient XLock exceeded timeout of {TransientLockTimeout}");
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TransientXUnlock<TKeyLocker>(ref HashEntryInfo hei)
            where TKeyLocker : struct, IKeyLocker
            => TKeyLocker.UnlockTransientExclusive(Kernel, ref hei);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryTransientSLock<TInput, TOutput, TContext, TKeyLocker>(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, out OperationStatus status)
            where TKeyLocker : struct, IKeyLocker
        {
            if (TKeyLocker.TryLockTransientShared(Kernel, ref stackCtx.hei))
            {
                stackCtx.ResetTransientLockTimeout();   // In case of retry
                status = OperationStatus.SUCCESS;
                return true;
            }
            if (stackCtx.IsTransientLockTimeout(TransientLockTimeout))
                throw new TsavoriteException($"Transient SLock exceeded timeout of {TransientLockTimeout}");
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TransientSUnlock<TKeyLocker>(ref HashEntryInfo hei)
            where TKeyLocker : struct, IKeyLocker
            => TKeyLocker.UnlockTransientShared(Kernel, ref hei);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockForScan(out HashEntryInfo hei, ref TKey key)
        {
            Debug.Assert(Kernel.Epoch.ThisInstanceProtected(), "Epoch should be protected by LockForScan caller");

            // This will always be a transient lock as it is not session-based
            hei = new(storeFunctions.GetKeyHashCode64(ref key), partitionId);
            _ = Kernel.hashTable.FindTag(ref hei);

            while (!LockTable.TryLockShared(ref hei))
                Kernel.Epoch.ProtectAndDrain();
            hei.SetHasTransientSLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockForScan(ref HashEntryInfo hei)
        {
            if (hei.HasTransientSLock)
            {
                LockTable.UnlockShared(ref hei);
                hei.ClearHasTransientSLock();
            }
        }
    }
}