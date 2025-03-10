// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMemory(SpanByte key, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress, bool stopAtHeadAddress = true)
        {
            if (UseReadCache && FindInReadCache(key, ref stackCtx, minAddress: Constants.kInvalidAddress))
                return true;
            if (minAddress < hlogBase.HeadAddress && stopAtHeadAddress)
                minAddress = hlogBase.HeadAddress;
            return TryFindRecordInMainLog(key, ref stackCtx, minAddress: minAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMemory<TInput, TOutput, TContext>(SpanByte key, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx,
                                                                   ref PendingContext<TInput, TOutput, TContext> pendingContext)
        {
            // Add 1 to the pendingContext minAddresses because we don't want an inclusive search; we're looking to see if it was added *after*.
            if (UseReadCache)
            {
                var minRC = IsReadCache(pendingContext.InitialEntryAddress) ? pendingContext.InitialEntryAddress + 1 : Constants.kInvalidAddress;
                if (FindInReadCache(key, ref stackCtx, minAddress: minRC))
                    return true;
            }
            var minLog = pendingContext.InitialLatestLogicalAddress < hlogBase.HeadAddress ? hlogBase.HeadAddress : pendingContext.InitialLatestLogicalAddress + 1;
            return TryFindRecordInMainLog(key, ref stackCtx, minAddress: minLog);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryFindRecordInMainLog(SpanByte key, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress)
        {
            Debug.Assert(!stackCtx.recSrc.HasInMemorySrc, "Should not have found record before this call");
            if (stackCtx.recSrc.LogicalAddress >= minAddress)
            {
                stackCtx.recSrc.SetPhysicalAddress();
                TraceBackForKeyMatch(key, ref stackCtx.recSrc, minAddress);
            }
            return stackCtx.recSrc.HasInMemorySrc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryFindRecordInMainLog(SpanByte key, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress, long maxAddress)
        {
            Debug.Assert(!stackCtx.recSrc.HasInMemorySrc, "Should not have found record before this call");
            if (stackCtx.recSrc.LogicalAddress >= minAddress)
            {
                stackCtx.recSrc.SetPhysicalAddress();
                TraceBackForKeyMatch(key, ref stackCtx.recSrc, minAddress, maxAddress);
            }
            return stackCtx.recSrc.HasInMemorySrc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        // Return true if the record is found in the log, else false and an indication of whether we need to do IO to continue the search
        internal bool TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                SpanByte key, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long currentAddress, long minAddress, long maxAddress, out OperationStatus internalStatus, out bool needIO)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (!FindTag(ref stackCtx.hei))
            {
                internalStatus = OperationStatus.NOTFOUND;
                return needIO = false;
            }

            internalStatus = OperationStatus.SUCCESS;
            if (!stackCtx.hei.IsReadCache)
            {
                // If the address in the HashBucketEntry is the current address, there'll be no record above it, so return false (not found).
                // If there are no valid records in the HashBucketEntry (minAddress is inclusive), return false (not found).
                if (stackCtx.hei.Address == currentAddress || stackCtx.hei.Address < minAddress || stackCtx.hei.Address < hlogBase.BeginAddress)
                {
                    stackCtx.SetRecordSourceToHashEntry(hlogBase);
                    return needIO = false;
                }
                if (stackCtx.hei.Address < hlogBase.HeadAddress)
                {
                    stackCtx.SetRecordSourceToHashEntry(hlogBase);
                    needIO = true;
                    return false;
                }
            }

            if (RevivificationManager.UseFreeRecordPool)
            {
                // The EphemeralSLock here is necessary only for the tag chain to avoid record elision/revivification during traceback.
                if (!TryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                    return needIO = false;
            }
            else
                stackCtx.SetRecordSourceToHashEntry(hlogBase);

            try
            {
                if (UseReadCache)
                    SkipReadCache(ref stackCtx, out _); // Where this is called, we have no dependency on source addresses so we don't care if it Refreshed

                // We don't have a pendingContext here, so pass the minAddress directly.
                needIO = false;
                if (TryFindRecordInMainLogForPendingOperation(key, ref stackCtx, minAddress < hlogBase.HeadAddress ? hlogBase.HeadAddress : minAddress, maxAddress, out internalStatus))
                    return true;

                needIO = stackCtx.recSrc.LogicalAddress >= minAddress && stackCtx.recSrc.LogicalAddress < hlogBase.HeadAddress && stackCtx.recSrc.LogicalAddress >= hlogBase.BeginAddress;
                return false;
            }
            finally
            {
                EphemeralSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
            }
        }

        // We want to return non-Invalid records or Invalid records that are Sealed, because Sealed is part of our "need RETRY" handling.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool IsValidTracebackRecord(RecordInfo recordInfo) => !recordInfo.Invalid || recordInfo.IsSealed;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(SpanByte key, ref RecordSource<TValue, TStoreFunctions, TAllocator> recSrc, long minAddress)
        {
            // recSrc.PhysicalAddress must already be populated by callers.
            var recordInfo = LogRecord.GetInfo(recSrc.PhysicalAddress);

            if (IsValidTracebackRecord(recordInfo) && storeFunctions.KeysEqual(key, LogRecord.GetKey(recSrc.PhysicalAddress)))
            {
                recSrc.SetHasMainLogSrc();
                return true;
            }

            recSrc.LogicalAddress = recordInfo.PreviousAddress;
            if (TraceBackForKeyMatch(key, recSrc.LogicalAddress, minAddress, out recSrc.LogicalAddress, out recSrc.PhysicalAddress))
            {
                recSrc.SetHasMainLogSrc();
                return true;
            }
            return false;
        }

        // Overload with maxAddress to avoid the extra condition - TODO: check that this duplication saves on IL/perf
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(SpanByte key, ref RecordSource<TValue, TStoreFunctions, TAllocator> recSrc, long minAddress, long maxAddress)
        {
            // PhysicalAddress must already be populated by callers.
            var recordInfo = LogRecord.GetInfo(recSrc.PhysicalAddress);

            if (IsValidTracebackRecord(recordInfo) && recSrc.LogicalAddress < maxAddress && storeFunctions.KeysEqual(key, LogRecord.GetKey(recSrc.PhysicalAddress)))
            {
                recSrc.SetHasMainLogSrc();
                return true;
            }

            recSrc.LogicalAddress = recordInfo.PreviousAddress;
            if (TraceBackForKeyMatch(key, recSrc.LogicalAddress, minAddress, maxAddress, out recSrc.LogicalAddress, out recSrc.PhysicalAddress))
            {
                recSrc.SetHasMainLogSrc();
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(SpanByte key, long fromLogicalAddress, long minAddress, out long foundLogicalAddress, out long foundPhysicalAddress)
        {
            // This overload is called when the record at the "current" logical address does not match 'key'; fromLogicalAddress is its .PreviousAddress.
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minAddress)
            {
                foundPhysicalAddress = hlog.GetPhysicalAddress(foundLogicalAddress);

                ref var recordInfo = ref LogRecord.GetInfoRef(foundPhysicalAddress);
                if (IsValidTracebackRecord(recordInfo) && storeFunctions.KeysEqual(key, LogRecord.GetKey(foundPhysicalAddress)))
                    return true;

                foundLogicalAddress = recordInfo.PreviousAddress;
            }
            foundPhysicalAddress = Constants.kInvalidAddress;
            return false;
        }

        // Overload with maxAddress to avoid the extra condition - TODO: check that this duplication saves on IL/perf
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(SpanByte key, long fromLogicalAddress, long minAddress, long maxAddress, out long foundLogicalAddress, out long foundPhysicalAddress)
        {
            // This overload is called when the record at the "current" logical address does not match 'key'; fromLogicalAddress is its .PreviousAddress.
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minAddress)
            {
                foundPhysicalAddress = hlog.GetPhysicalAddress(foundLogicalAddress);

                var recordInfo = LogRecord.GetInfo(foundPhysicalAddress);
                if (IsValidTracebackRecord(recordInfo) && foundLogicalAddress < maxAddress && storeFunctions.KeysEqual(key, LogRecord.GetKey(foundPhysicalAddress)))
                    return true;

                foundLogicalAddress = recordInfo.PreviousAddress;
            }
            foundPhysicalAddress = Constants.kInvalidAddress;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordForUpdate(SpanByte key, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress, out OperationStatus internalStatus)
        {
            // This routine returns true if we should proceed with the InternalXxx operation (whether the record was found or not),
            // else false (including false if we need a RETRY). If it returns true with recSrc.HasInMemorySrc, caller must set srcRecordInfo.

            // We are not here from Read() so have not processed readcache; search that as well as the in-memory log.
            if (TryFindRecordInMemory(key, ref stackCtx, minAddress))
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
        private bool TryFindRecordForRead(SpanByte key, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress, out OperationStatus internalStatus)
        {
            // This routine returns true if we should proceed with the InternalXxx operation (whether the record was found or not),
            // else false (including false if we need a RETRY). If it returns true with recSrc.HasInMemorySrc, caller must set srcRecordInfo.

            // We are here for Read() so we have already processed readcache and are just here for the traceback in the main log.
            if (TryFindRecordInMainLog(key, ref stackCtx, minAddress))
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
        private bool TryFindRecordForPendingOperation<TInput, TOutput, TContext>(SpanByte key, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress, out OperationStatus internalStatus,
                                                      ref PendingContext<TInput, TOutput, TContext> pendingContext)
        {
            // This routine returns true if we find the key, else false.
            internalStatus = OperationStatus.SUCCESS;

            if (!TryFindRecordInMemory(key, ref stackCtx, ref pendingContext))
                return false;
            if (stackCtx.recSrc.GetInfo().IsClosed)
                internalStatus = OperationStatus.RETRY_LATER;

            // We do not lock here; this is just to see if the key is found
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMainLogForPendingOperation(SpanByte key, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress, long maxAddress, out OperationStatus internalStatus)
        {
            // This overload is called when we do not have a PendingContext to get minAddress from, and we've skipped the readcache if present.

            // This routine returns true if we find the key, else false.
            internalStatus = OperationStatus.SUCCESS;

            if (!TryFindRecordInMainLog(key, ref stackCtx, minAddress, maxAddress))
                return false;
            if (stackCtx.recSrc.GetInfo().IsClosed)
                internalStatus = OperationStatus.RETRY_LATER;

            // We do not lock here; this is just to see if the key is found
            return true;
        }
    }
}