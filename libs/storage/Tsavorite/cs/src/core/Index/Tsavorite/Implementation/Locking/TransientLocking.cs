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
        private bool TryTransientXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (sessionFunctions.TryLockTransientExclusive(Kernel, ref stackCtx.hei))
            {
                status = OperationStatus.SUCCESS;
                return true;
            }
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TransientXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (stackCtx.hei.HasTransientXLock)
                sessionFunctions.UnlockTransientExclusive(Kernel, ref stackCtx.hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryTransientSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (sessionFunctions.TryLockTransientShared(Kernel, ref stackCtx.hei))
            {
                status = OperationStatus.SUCCESS;
                return true;
            }
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TransientSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (stackCtx.hei.HasTransientSLock)
                sessionFunctions.UnlockTransientShared(Kernel, ref stackCtx.hei);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockForScan(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref TKey key)
        {
            Debug.Assert(!stackCtx.hei.HasTransientLock, $"Should not call LockForScan if HashEntryInfo already has a lock ({stackCtx.hei.TransientLockStateString()})");

            // This will always be a transient lock as it is not session-based
            stackCtx = new(storeFunctions.GetKeyHashCode64(ref key), partitionId);
            _ = Kernel.hashTable.FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

            while (!LockTable.TryLockShared(ref stackCtx.hei))
                Kernel.Epoch.ProtectAndDrain();
            stackCtx.hei.SetHasTransientSLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockForScan(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            if (stackCtx.hei.HasTransientSLock)
            {
                LockTable.UnlockShared(ref stackCtx.hei);
                stackCtx.hei.ClearHasTransientSLock();
            }
        }
    }
}