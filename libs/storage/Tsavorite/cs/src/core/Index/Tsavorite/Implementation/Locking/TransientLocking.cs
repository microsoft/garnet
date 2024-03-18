// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryTransientXLock<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                                    out OperationStatus status)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (!LockTable.IsEnabled || tsavoriteSession.TryLockTransientExclusive(ref key, ref stackCtx))
                return true;
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TransientXUnlock<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            if (!stackCtx.recSrc.HasTransientXLock)
                return false;
            tsavoriteSession.UnlockTransientExclusive(ref key, ref stackCtx);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryTransientSLock<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                                    out OperationStatus status)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;
            if (!LockTable.IsEnabled || tsavoriteSession.TryLockTransientShared(ref key, ref stackCtx))
                return true;
            status = OperationStatus.RETRY_LATER;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TransientSUnlock<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession, ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            if (!stackCtx.recSrc.HasTransientSLock)
                return false;
            tsavoriteSession.UnlockTransientShared(ref key, ref stackCtx);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void LockForScan(ref OperationStackContext<Key, Value> stackCtx, ref Key key, ref RecordInfo recordInfo)
        {
            Debug.Assert(!stackCtx.recSrc.HasLock, $"Should not call LockForScan if recSrc already has a lock ({stackCtx.recSrc.LockStateString()})");
            if (DoTransientLocking)
            {
                stackCtx = new(comparer.GetHashCode64(ref key));
                FindTag(ref stackCtx.hei);
                stackCtx.SetRecordSourceToHashEntry(hlog);

                while (!LockTable.TryLockTransientShared(ref key, ref stackCtx.hei))
                    epoch.ProtectAndDrain();
                stackCtx.recSrc.SetHasTransientSLock();
            }
            else if (DoRecordIsolation)
            {
                while (!stackCtx.recSrc.TryLockShared(ref recordInfo))
                    epoch.ProtectAndDrain();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockForScan(ref OperationStackContext<Key, Value> stackCtx, ref Key key, ref RecordInfo recordInfo)
        {
            if (stackCtx.recSrc.HasTransientSLock)
            {
                LockTable.UnlockShared(ref key, ref stackCtx.hei);
                stackCtx.recSrc.ClearHasTransientSLock();
            }
            else
            {
                stackCtx.recSrc.UnlockShared(ref recordInfo, headAddress: 0);
            }
        }
    }
}