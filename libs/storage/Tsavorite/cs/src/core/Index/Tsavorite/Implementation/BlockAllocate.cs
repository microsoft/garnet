// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryBlockAllocate<TInput, TOutput, TContext>(
                AllocatorBase<TValue, TStoreFunctions, TAllocator> allocator,
                int recordSize,
                out long logicalAddress,
                ref PendingContext<TInput, TOutput, TContext> pendingContext,
                out OperationStatus internalStatus)
        {
            pendingContext.flushEvent = allocator.FlushEvent;
            logicalAddress = allocator.TryAllocateRetryNow(recordSize);
            if (logicalAddress > 0)
            {
                pendingContext.flushEvent = default;
                internalStatus = OperationStatus.SUCCESS;
                return true;
            }

            // logicalAddress less than 0 (RETRY_NOW) should already have been handled
            Debug.Assert(logicalAddress == 0);
            // We expect flushEvent to be signaled.
            internalStatus = OperationStatus.ALLOCATE_FAILED;
            return false;
        }

        /// <summary>Options for TryAllocateRecord.</summary>
        internal struct AllocateOptions
        {
            /// <summary>If true, use the non-revivification recycling of records that failed to CAS and are carried in PendingContext through RETRY.</summary>
            internal bool recycle;

            /// <summary>If true, the source record is elidable so we can try to elide from the tag chain (and transfer it to the FreeList if we're doing Revivification).</summary>
            internal bool elideSourceRecord;
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryAllocateRecord<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                                       ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, ref RecordSizeInfo sizeInfo, AllocateOptions options,
                                                       out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            status = OperationStatus.SUCCESS;

            // MinRevivAddress is also needed for pendingContext-based record reuse.
            var minMutableAddress = GetMinRevivifiableAddress();
            var minRevivAddress = minMutableAddress;

            if (options.recycle && pendingContext.retryNewLogicalAddress != Constants.kInvalidAddress
                    && GetAllocationForRetry(sessionFunctions, ref pendingContext, minRevivAddress, ref sizeInfo, out newLogicalAddress, out newPhysicalAddress))
                return true;
            if (RevivificationManager.UseFreeRecordPool)
            {
                if (!options.elideSourceRecord && stackCtx.hei.Address >= minMutableAddress)
                    minRevivAddress = stackCtx.hei.Address;
                if (sessionFunctions.Ctx.IsInV1)
                {
                    var fuzzyStartAddress = _hybridLogCheckpoint.info.startLogicalAddress;
                    if (fuzzyStartAddress > minRevivAddress)
                        minRevivAddress = fuzzyStartAddress;
                }
                // TODO: This must handle out-of-line allocations for key and value (get overflow allocator for logicalAddress' page
                if (TryTakeFreeRecord<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref sizeInfo, minRevivAddress, out newLogicalAddress, out newPhysicalAddress))
                    return true;
            }

            // Spin to make sure newLogicalAddress is > recSrc.LatestLogicalAddress (the .PreviousAddress and CAS comparison value).
            for (; ; _ = Thread.Yield())
            {
                if (!TryBlockAllocate(hlogBase, sizeInfo.AllocatedInlineRecordSize, out newLogicalAddress, ref pendingContext, out status))
                    break;

                newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                if (VerifyInMemoryAddresses(ref stackCtx))
                {
                    if (newLogicalAddress > stackCtx.recSrc.LatestLogicalAddress)
                        return true;

                    // This allocation is below the necessary address so put it on the free list or abandon it, then repeat the loop.
                    if (!RevivificationManager.UseFreeRecordPool || !RevivificationManager.TryAdd(newLogicalAddress, newPhysicalAddress, sizeInfo.AllocatedInlineRecordSize, ref sessionFunctions.Ctx.RevivificationStats))
                    {
                        // TODO: Return overflow allocations and DisposeRecord
                        LogRecord.GetInfo(newPhysicalAddress).SetInvalid();  // Skip on log scan
                    }
                    continue;
                }

                // In-memory source dropped below HeadAddress during BlockAllocate. Save the record for retry if we can.
                ref var newRecordInfo = ref LogRecord.GetInfoRef(newPhysicalAddress);
                if (options.recycle)
                {
                    // TODO: Return overflow allocations and DisposeRecord

                    // FillerLength is not needed now; the Value is initialized correctly, and space for optionals is keyed by the RecordInfo header
                    SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress);
                }
                else
                    newRecordInfo.SetInvalid();  // Skip on log scan
                status = OperationStatus.RETRY_LATER;
                break;
            }

            newPhysicalAddress = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryAllocateRecordReadCache<TInput, TOutput, TContext>(ref PendingContext<TInput, TOutput, TContext> pendingContext, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx,
                                                       ref RecordSizeInfo recordSizeInfo, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status)
        {
            // Spin to make sure the start of the tag chain is not readcache, or that newLogicalAddress is > the first address in the tag chain.
            for (; ; Thread.Yield())
            {
                if (!TryBlockAllocate(readCacheBase, recordSizeInfo.ActualInlineRecordSize, out newLogicalAddress, ref pendingContext, out status))
                    break;

                newPhysicalAddress = readcache.GetPhysicalAddress(newLogicalAddress);
                if (VerifyInMemoryAddresses(ref stackCtx))
                {
                    if (!stackCtx.hei.IsReadCache || newLogicalAddress > stackCtx.hei.AbsoluteAddress)
                        return true;

                    // This allocation is below the necessary address so abandon it and repeat the loop.
                    ReadCacheAbandonRecord(newPhysicalAddress);
                    continue;
                }

                // In-memory source dropped below HeadAddress during BlockAllocate.
                ReadCacheAbandonRecord(newPhysicalAddress);
                status = OperationStatus.RETRY_LATER;
                break;
            }

            newPhysicalAddress = 0;
            return false;
        }

        // Do not inline, to keep CreateNewRecord* lean
        void SaveAllocationForRetry<TInput, TOutput, TContext>(ref PendingContext<TInput, TOutput, TContext> pendingContext, long logicalAddress, long physicalAddress)
        {
            ref var recordInfo = ref hlog.GetInfoRef(physicalAddress);

            // TryAllocateRecord may stash this before WriteRecordInfo is called, leaving .PreviousAddress set to kInvalidAddress.
            // This is zero, and setting Invalid will result in recordInfo.IsNull being true, which will cause log-scan problems.
            // We don't need whatever .PreviousAddress was there, so set it to kTempInvalidAddress (which is nonzero).
            recordInfo.PreviousAddress = Constants.kTempInvalidAddress;
            recordInfo.SetInvalid();    // Skip on log scan

            pendingContext.retryNewLogicalAddress = logicalAddress < hlogBase.HeadAddress ? Constants.kInvalidAddress : logicalAddress;
        }

        // Do not inline, to keep TryAllocateRecord lean
        bool GetAllocationForRetry<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref PendingContext<TInput, TOutput, TContext> pendingContext, long minAddress,
                ref RecordSizeInfo sizeInfo, out long newLogicalAddress, out long newPhysicalAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Use an earlier allocation from a failed operation, if possible.
            newLogicalAddress = pendingContext.retryNewLogicalAddress;
            pendingContext.retryNewLogicalAddress = 0;

            if (newLogicalAddress < hlogBase.HeadAddress)
            {
                // The record dropped below headAddress. If it needs DisposeRecord, it will be done on eviction.
                newPhysicalAddress = 0;
                return false;
            }

            newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
            var newLogRecord = new LogRecord<TValue>(newPhysicalAddress);

            if (newLogicalAddress <= minAddress || newLogRecord.GetFullRecordSizes().allocatedSize < sizeInfo.AllocatedInlineRecordSize)
            {
                // Can't reuse, so abandon it.
                newPhysicalAddress = 0;
                return false;
            }
            return true;
        }
    }
}