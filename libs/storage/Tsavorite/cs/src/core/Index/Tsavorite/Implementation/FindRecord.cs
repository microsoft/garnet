// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<Key, Value>
        where TAllocator : IAllocator<Key, Value, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMemory(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx, long minAddress, bool stopAtHeadAddress = true)
        {
            if (UseReadCache && FindInReadCache(ref key, ref stackCtx, minAddress: Constants.kInvalidAddress))
                return true;
            if (minAddress < hlogBase.HeadAddress && stopAtHeadAddress)
                minAddress = hlogBase.HeadAddress;
            return TryFindRecordInMainLog(ref key, ref stackCtx, minAddress: minAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMemory<Input, Output, Context>(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx,
                                                                   ref PendingContext<Input, Output, Context> pendingContext)
        {
            // Add 1 to the pendingContext minAddresses because we don't want an inclusive search; we're looking to see if it was added *after*.
            if (UseReadCache)
            {
                var minRC = IsReadCache(pendingContext.InitialEntryAddress) ? pendingContext.InitialEntryAddress + 1 : Constants.kInvalidAddress;
                if (FindInReadCache(ref key, ref stackCtx, minAddress: minRC))
                    return true;
            }
            var minLog = pendingContext.InitialLatestLogicalAddress < hlogBase.HeadAddress ? hlogBase.HeadAddress : pendingContext.InitialLatestLogicalAddress + 1;
            return TryFindRecordInMainLog(ref key, ref stackCtx, minAddress: minLog);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryFindRecordInMainLog(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx, long minAddress)
        {
            Debug.Assert(!stackCtx.recSrc.HasInMemorySrc, "Should not have found record before this call");
            if (stackCtx.recSrc.LogicalAddress >= minAddress)
            {
                stackCtx.recSrc.SetPhysicalAddress();
                TraceBackForKeyMatch(ref key, ref stackCtx.recSrc, minAddress);
            }
            return stackCtx.recSrc.HasInMemorySrc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryFindRecordInMainLogForConditionalOperation<Input, Output, Context, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx, long minAddress, out OperationStatus internalStatus, out bool needIO)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context, TStoreFunctions, TAllocator>
        {
            internalStatus = OperationStatus.SUCCESS;
            if (RevivificationManager.UseFreeRecordPool)
            {
                // The TransientSLock here is necessary only for the tag chain to avoid record elision/revivification during traceback.
                if (!FindTagAndTryTransientSLock<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx, out internalStatus))
                    return needIO = false;
            }
            else
            {
                if (!FindTag(ref stackCtx.hei))
                {
                    internalStatus = OperationStatus.NOTFOUND;
                    return needIO = false;
                }
                stackCtx.SetRecordSourceToHashEntry(hlogBase);
            }

            try
            {
                // minAddress is inclusive
                if (!stackCtx.hei.IsReadCache)
                {
                    if (stackCtx.hei.Address < minAddress)
                        return needIO = false;
                    if (stackCtx.hei.Address < hlogBase.HeadAddress)
                    {
                        needIO = stackCtx.hei.Address >= hlogBase.BeginAddress;
                        return false;
                    }
                }

                if (UseReadCache)
                    SkipReadCache(ref stackCtx, out _); // Where this is called, we have no dependency on source addresses so we don't care if it Refreshed

                // We don't have a pendingContext here, so pass the minAddress directly.
                needIO = false;
                if (TryFindRecordInMainLogForPendingOperation(ref key, ref stackCtx, minAddress < hlogBase.HeadAddress ? hlogBase.HeadAddress : minAddress, out internalStatus))
                    return true;

                needIO = stackCtx.recSrc.LogicalAddress >= minAddress && stackCtx.recSrc.LogicalAddress < hlogBase.HeadAddress && stackCtx.recSrc.LogicalAddress >= hlogBase.BeginAddress;
                return false;
            }
            finally
            {
                TransientSUnlock<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx);
            }
        }

        // We want to return non-Invalid records or Invalid records that are Sealed, because Sealed is part of our "need RETRY" handling.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsValidTracebackRecord(RecordInfo recordInfo) => !recordInfo.Invalid || recordInfo.IsSealed;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, ref RecordSource<Key, Value, TStoreFunctions, TAllocator> recSrc, long minAddress)
        {
            // PhysicalAddress must already be populated by callers.
            ref var recordInfo = ref recSrc.GetInfo();
            if (IsValidTracebackRecord(recordInfo) && storeFunctions.KeysEqual(ref key, ref recSrc.GetKey()))
            {
                recSrc.SetHasMainLogSrc();
                return true;
            }

            recSrc.LogicalAddress = recordInfo.PreviousAddress;
            if (TraceBackForKeyMatch(ref key, recSrc.LogicalAddress, minAddress, out recSrc.LogicalAddress, out recSrc.PhysicalAddress))
            {
                recSrc.SetHasMainLogSrc();
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, long fromLogicalAddress, long minAddress, out long foundLogicalAddress, out long foundPhysicalAddress)
        {
            // This overload is called when the record at the "current" logical address does not match 'key'; fromLogicalAddress is its .PreviousAddress.
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minAddress)
            {
                foundPhysicalAddress = hlog.GetPhysicalAddress(foundLogicalAddress);

                ref var recordInfo = ref hlog.GetInfo(foundPhysicalAddress);
                if (IsValidTracebackRecord(recordInfo) && storeFunctions.KeysEqual(ref key, ref hlog.GetKey(foundPhysicalAddress)))
                    return true;

                foundLogicalAddress = recordInfo.PreviousAddress;
            }
            foundPhysicalAddress = Constants.kInvalidAddress;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordForUpdate(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx, long minAddress, out OperationStatus internalStatus)
        {
            // This routine returns true if we should proceed with the InternalXxx operation (whether the record was found or not),
            // else false (including false if we need a RETRY). If it returns true with recSrc.HasInMemorySrc, caller must set srcRecordInfo.

            // We are not here from Read() so have not processed readcache; search that as well as the in-memory log.
            if (TryFindRecordInMemory(ref key, ref stackCtx, minAddress))
            {
                if (stackCtx.recSrc.GetInfo().IsClosed)
                {
                    internalStatus = OperationStatus.RETRY_LATER;
                    return false;
                }
            }
            internalStatus = OperationStatus.SUCCESS;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordForRead(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx, long minAddress, out OperationStatus internalStatus)
        {
            // This routine returns true if we should proceed with the InternalXxx operation (whether the record was found or not),
            // else false (including false if we need a RETRY). If it returns true with recSrc.HasInMemorySrc, caller must set srcRecordInfo.

            // We are here for Read() so we have already processed readcache and are just here for the traceback in the main log.
            if (TryFindRecordInMainLog(ref key, ref stackCtx, minAddress))
            {
                if (stackCtx.recSrc.GetInfo().IsClosed)
                {
                    internalStatus = OperationStatus.RETRY_LATER;
                    return false;
                }
            }
            internalStatus = OperationStatus.SUCCESS;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordForPendingOperation<Input, Output, Context>(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx, long minAddress, out OperationStatus internalStatus,
                                                      ref PendingContext<Input, Output, Context> pendingContext)
        {
            // This routine returns true if we find the key, else false.
            internalStatus = OperationStatus.SUCCESS;

            if (!TryFindRecordInMemory(ref key, ref stackCtx, ref pendingContext))
                return false;
            if (stackCtx.recSrc.GetInfo().IsClosed)
                internalStatus = OperationStatus.RETRY_LATER;

            // We do not lock here; this is just to see if the key is found
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMainLogForPendingOperation(ref Key key, ref OperationStackContext<Key, Value, TStoreFunctions, TAllocator> stackCtx, long minAddress, out OperationStatus internalStatus)
        {
            // This overload is called when we do not have a PendingContext to get minAddress from, and we've skipped the readcache if present.

            // This routine returns true if we find the key, else false.
            internalStatus = OperationStatus.SUCCESS;

            if (!TryFindRecordInMainLog(ref key, ref stackCtx, minAddress))
                return false;
            if (stackCtx.recSrc.GetInfo().IsClosed)
                internalStatus = OperationStatus.RETRY_LATER;

            // We do not lock here; this is just to see if the key is found
            return true;
        }
    }
}