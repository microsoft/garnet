// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                    ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (sessionFunctions.TryLockEphemeralExclusive(ref stackCtx))
            {
                status = OperationStatus.SUCCESS;
                return true;
            }
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void EphemeralXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                    ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (stackCtx.recSrc.HasEphemeralXLock)
                sessionFunctions.UnlockEphemeralExclusive(ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                    ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (sessionFunctions.TryLockEphemeralShared(ref stackCtx))
            {
                status = OperationStatus.SUCCESS;
                return true;
            }
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void EphemeralSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                    ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (stackCtx.recSrc.HasEphemeralSLock)
                sessionFunctions.UnlockEphemeralShared(ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockForScan(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ReadOnlySpan<byte> key)
        {
            Debug.Assert(!stackCtx.recSrc.HasLock, $"Should not call LockForScan if recSrc already has a lock ({stackCtx.recSrc.LockStateString()})");

            // This will always be an Ephemeral lock as it is not session-based
            stackCtx = new(storeFunctions.GetKeyHashCode64(key));
            _ = FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

            while (!LockTable.TryLockShared(ref stackCtx.hei))
                epoch.ProtectAndDrain();
            stackCtx.recSrc.SetHasEphemeralSLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockForScan(ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
        {
            if (stackCtx.recSrc.HasEphemeralSLock)
            {
                LockTable.UnlockShared(ref stackCtx.hei);
                stackCtx.recSrc.ClearHasEphemeralSLock();
            }
        }
    }
}