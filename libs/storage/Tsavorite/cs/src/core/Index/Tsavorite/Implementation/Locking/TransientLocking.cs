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
        private bool TryTransientXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (sessionFunctions.TryLockTransientExclusive(ref key, ref stackCtx))
            {
                status = OperationStatus.SUCCESS;
                return true;
            }
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void TransientXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (stackCtx.recSrc.HasTransientXLock)
                sessionFunctions.UnlockTransientExclusive(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryTransientSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (sessionFunctions.TryLockTransientShared(ref key, ref stackCtx))
            {
                status = OperationStatus.SUCCESS;
                return true;
            }
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void TransientSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (stackCtx.recSrc.HasTransientSLock)
                sessionFunctions.UnlockTransientShared(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockForScan(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref TKey key)
        {
            Debug.Assert(!stackCtx.recSrc.HasLock, $"Should not call LockForScan if recSrc already has a lock ({stackCtx.recSrc.LockStateString()})");

            // This will always be a transient lock as it is not session-based
            stackCtx = new(storeFunctions.GetKeyHashCode64(ref key), partitionId);
            _ = FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

            while (!LockTable.TryLockShared(ref stackCtx.hei))
                kernel.epoch.ProtectAndDrain();
            stackCtx.recSrc.SetHasTransientSLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockForScan(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            if (stackCtx.recSrc.HasTransientSLock)
            {
                LockTable.UnlockShared(ref stackCtx.hei);
                stackCtx.recSrc.ClearHasTransientSLock();
            }
        }
    }
}