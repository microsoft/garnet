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
        private bool TryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (sessionFunctions.TryLockEphemeralExclusive(ref key, ref stackCtx))
            {
                status = OperationStatus.SUCCESS;
                return true;
            }
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void EphemeralXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (stackCtx.recSrc.HasEphemeralXLock)
                sessionFunctions.UnlockEphemeralExclusive(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (sessionFunctions.TryLockEphemeralShared(ref key, ref stackCtx))
            {
                status = OperationStatus.SUCCESS;
                return true;
            }
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void EphemeralSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                                    ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (stackCtx.recSrc.HasEphemeralSLock)
                sessionFunctions.UnlockEphemeralShared(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockForScan(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref TKey key)
        {
            Debug.Assert(!stackCtx.recSrc.HasLock, $"Should not call LockForScan if recSrc already has a lock ({stackCtx.recSrc.LockStateString()})");

            // This will always be a Ephemeral lock as it is not session-based
            stackCtx = new(storeFunctions.GetKeyHashCode64(ref key));
            _ = FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

            while (!LockTable.TryLockShared(ref stackCtx.hei))
                epoch.ProtectAndDrain();
            stackCtx.recSrc.SetHasEphemeralSLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockForScan(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            if (stackCtx.recSrc.HasEphemeralSLock)
            {
                LockTable.UnlockShared(ref stackCtx.hei);
                stackCtx.recSrc.ClearHasEphemeralSLock();
            }
        }
    }
}