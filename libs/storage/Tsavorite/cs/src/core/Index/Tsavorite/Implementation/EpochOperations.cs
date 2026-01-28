// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void SynchronizeEpoch<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
            TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx,
            ref PendingContext<TInput, TOutput, TContext> pendingContext,
            TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var version = sessionCtx.version;
            Debug.Assert(sessionCtx.version == version, $"sessionCtx.version ({sessionCtx.version}) should == version ({version})");
            Debug.Assert(sessionCtx.phase == Phase.PREPARE, $"sessionCtx.phase ({sessionCtx.phase}) should == Phase.PREPARE");
            InternalRefresh<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions);
            Debug.Assert(sessionCtx.version > version, $"sessionCtx.version ({sessionCtx.version}) should be > version ({version})");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SpinWaitUntilClosed(long address)
        {
            // Unlike HeadAddress, ClosedUntilAddress is a high-water mark; a record that is == to ClosedUntilAddress has *not* been closed yet.
            while (address >= hlogBase.ClosedUntilAddress)
            {
                Debug.Assert(address < hlogBase.HeadAddress, "expected address < hlog.HeadAddress");
                epoch.ProtectAndDrain();
                Thread.Yield();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SpinWaitUntilRecordIsClosed(long logicalAddress, AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> log)
        {
            Debug.Assert(logicalAddress < log.HeadAddress, "SpinWaitUntilRecordIsClosed should not be called for addresses above HeadAddress");

            // The caller checks for logicalAddress < HeadAddress, so we must ProtectAndDrain at least once, even if logicalAddress >= ClosedUntilAddress.
            // Otherwise, let's say we have two OnPagesClosed operations in the epoch drainlist:
            //      old epoch: old HeadAddress -> [ClosedUntilAddress is somewhere in here] -> intermediate HeadAddress
            //      middle epoch: we hold this
            //      newer epoch: intermediate HeadAddress -> current HeadAddress
            // If we find the record above ClosedUntilAddress and return immediately, the caller will retry -- but we haven't given the second
            // OnPagesClosed a chance to run and we're holding an epoch below it, so the caller will again see logicalAddress < HeadAddress... forever.
            // However, we don't want the caller to check for logicalAddress < ClosedUntilAddress instead of HeadAddress; this allows races where
            // lock are split between the record and the LockTable:
            //      lock -> eviction to lock table -> unlock -> ClosedUntilAddress updated
            // So the caller has to check for logicalAddress < HeadAddress and we have to run this loop at least once.
            while (true)
            {
                epoch.ProtectAndDrain();
                Thread.Yield();

                // Unlike HeadAddress, ClosedUntilAddress is a high-water mark; a record that is == to ClosedUntilAddress has *not* been closed yet.
                if (logicalAddress < log.ClosedUntilAddress)
                    break;

                // Note: We cannot jump out here if the Lock Table contains the key, because it may be an older version of the record.
            }
        }
    }
}