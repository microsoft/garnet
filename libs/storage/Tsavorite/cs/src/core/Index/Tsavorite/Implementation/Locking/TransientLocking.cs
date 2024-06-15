// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<Key, Value>
        where TAllocator : IAllocator<Key, Value, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryTransientXLock<Input, Output, Context, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref Key key, 
                                    ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context, TStoreFunctions, TAllocator>
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
        private static void TransientXUnlock<Input, Output, Context, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref Key key,
                                    ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context, TStoreFunctions, TAllocator>
        {
            if (stackCtx.recSrc.HasTransientXLock)
                sessionFunctions.UnlockTransientExclusive(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryTransientSLock<Input, Output, Context, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref Key key, 
                                    ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx,
                                    out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context, TStoreFunctions, TAllocator>
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
        internal static void TransientSUnlock<Input, Output, Context, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref Key key, 
                                    ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context, TStoreFunctions, TAllocator>
        {
            if (stackCtx.recSrc.HasTransientSLock)
                sessionFunctions.UnlockTransientShared(ref key, ref stackCtx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockForScan(ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx, ref Key key)
        {
            Debug.Assert(!stackCtx.recSrc.HasLock, $"Should not call LockForScan if recSrc already has a lock ({stackCtx.recSrc.LockStateString()})");

            // This will always be a transient lock as it is not session-based
            stackCtx = new(storeFunctions.GetKeyHashCode64(ref key));
            _ = FindTag(ref stackCtx.hei);
            stackCtx.SetRecordSourceToHashEntry(hlogBase);

            while (!LockTable.TryLockShared(ref stackCtx.hei))
                epoch.ProtectAndDrain();
            stackCtx.recSrc.SetHasTransientSLock();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockForScan(ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx)
        {
            if (stackCtx.recSrc.HasTransientSLock)
            {
                LockTable.UnlockShared(ref stackCtx.hei);
                stackCtx.recSrc.ClearHasTransientSLock();
            }
        }
    }
}