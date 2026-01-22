// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    using static LogAddress;

    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryBlockAllocate<TInput, TOutput, TContext>(
                AllocatorBase<TStoreFunctions, TAllocator> allocator,
                int recordSize,
                out long logicalAddress,
                ref PendingContext<TInput, TOutput, TContext> pendingContext,
                out OperationStatus internalStatus)
        {
            pendingContext.flushEvent = allocator.flushEvent;
            if (allocator.TryAllocateRetryNow(recordSize, out logicalAddress))
            {
                pendingContext.flushEvent = default;
                internalStatus = OperationStatus.SUCCESS;
                return true;
            }

            // logicalAddress less than 0 (RETRY_NOW) should already have been handled. We expect flushEvent to be signaled.
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
                                                       ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref RecordSizeInfo sizeInfo, AllocateOptions options,
                                                       out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            status = OperationStatus.SUCCESS;

            // MinRevivAddress is also needed for pendingContext-based record reuse.
            var minRevivAddress = GetMinRevivifiableAddress();

            // If are eliding the first record in the tag chain, then there can be no others; we are removing the only record in the chain. Otherwise, the new record must be above hei.Address.
            if ((minRevivAddress <= stackCtx.hei.Address) && (!options.elideSourceRecord || stackCtx.hei.Address != stackCtx.recSrc.LogicalAddress))
                minRevivAddress = stackCtx.hei.Address;

            if (options.recycle && pendingContext.retryNewLogicalAddress != kInvalidAddress
                    && GetAllocationForRetry(sessionFunctions, ref pendingContext, minRevivAddress, in sizeInfo, out newLogicalAddress, out newPhysicalAddress))
            {
                new LogRecord(newPhysicalAddress).PrepareForRevivification(ref sizeInfo);
                return true;
            }
            if (RevivificationManager.UseFreeRecordPool)
            {
                if (sessionFunctions.Ctx.IsInV1)
                {
                    var fuzzyStartAddress = _hybridLogCheckpoint.info.startLogicalAddress;
                    if (fuzzyStartAddress > minRevivAddress)
                        minRevivAddress = fuzzyStartAddress;
                }
                if (TryTakeFreeRecord<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, in sizeInfo, minRevivAddress, out newLogicalAddress, out newPhysicalAddress))
                {
                    new LogRecord(newPhysicalAddress).PrepareForRevivification(ref sizeInfo);
                    return true;
                }
            }

            // Spin to make sure newLogicalAddress is > recSrc.LatestLogicalAddress (the .PreviousAddress and CAS comparison value).
            while (true)
            {
                if (!TryBlockAllocate(hlogBase, sizeInfo.AllocatedInlineRecordSize, out newLogicalAddress, ref pendingContext, out status))
                    break;

                newPhysicalAddress = hlogBase.GetPhysicalAddress(newLogicalAddress);

                // If allocation had to flush and did it inline, then the epoch was refreshed and we need to check for address safety.
                if (VerifyInMemoryAddresses(ref stackCtx))
                {
                    if (newLogicalAddress > stackCtx.recSrc.LatestLogicalAddress)
                        return true;

                    // This allocation is below the necessary address so put it on the free list or abandon it, then repeat the loop.
                    if (RevivificationManager.UseFreeRecordPool)
                    {
                        // Set up a simple LogRecord with specified key size and value size taking the entire non-key space (we don't have optionals now)
                        // so revivification can read the record size.
                        var logRecord = hlog.CreateLogRecord(newLogicalAddress, newPhysicalAddress);
                        logRecord.InitializeForReuse(in sizeInfo);

                        // Call RevivificationManager.TryAdd() directly, as here we've done InitializeForReuse of a new record so don't want DisposeRecord.
                        if (RevivificationManager.TryAdd(newLogicalAddress, ref logRecord, ref sessionFunctions.Ctx.RevivificationStats))
                            continue;
                    }
                    LogRecord.GetInfo(newPhysicalAddress).SetInvalid();  // Skip on log scan
                    _ = Thread.Yield();
                    continue;
                }

                // The in-memory source dropped below HeadAddress during BlockAllocate. Save the record for retry if we can and return RETRY_LATER.
                if (options.recycle)
                {
                    var logRecord = new LogRecord(newPhysicalAddress);
                    logRecord.InitializeForReuse(in sizeInfo);
                    SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress);
                }
                else
                    LogRecord.GetInfoRef(newPhysicalAddress).SetInvalid();      // Skip on log scan
                status = OperationStatus.RETRY_LATER;
                break;
            }

            newPhysicalAddress = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryAllocateRecordReadCache<TInput, TOutput, TContext>(ref PendingContext<TInput, TOutput, TContext> pendingContext, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx,
                                                       in RecordSizeInfo recordSizeInfo, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status)
        {
            // Spin to make sure the start of the tag chain is not readcache, or that newLogicalAddress is > the first address in the tag chain.
            while (true)
            {
                if (!TryBlockAllocate(readcacheBase, recordSizeInfo.AllocatedInlineRecordSize, out newLogicalAddress, ref pendingContext, out status))
                    break;
                newPhysicalAddress = readcacheBase.GetPhysicalAddress(newLogicalAddress);

                if (VerifyInMemoryAddresses(ref stackCtx))
                {
                    if (!stackCtx.hei.IsReadCache || newLogicalAddress > AbsoluteAddress(stackCtx.hei.Address))
                        return true;

                    // This allocation is below the necessary address so abandon it and repeat the loop.
                    TsavoriteKV<TStoreFunctions, TAllocator>.ReadCacheAbandonRecord(newPhysicalAddress);
                    _ = Thread.Yield();
                    continue;
                }

                // The in-memory source dropped below HeadAddress during BlockAllocate. Abandon the record (TODO: reuse readcache records) and return RETRY_LATER.
                TsavoriteKV<TStoreFunctions, TAllocator>.ReadCacheAbandonRecord(newPhysicalAddress);
                status = OperationStatus.RETRY_LATER;
                break;
            }

            newPhysicalAddress = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]  // Do not inline, to keep CreateNewRecord* lean
        void SaveAllocationForRetry<TInput, TOutput, TContext>(ref PendingContext<TInput, TOutput, TContext> pendingContext, long logicalAddress, long physicalAddress)
        {
            ref var recordInfo = ref LogRecord.GetInfoRef(physicalAddress);

            // TryAllocateRecord may stash this before WriteRecordInfo is called, leaving .PreviousAddress set to kInvalidAddress.
            // This is zero, and setting Invalid will result in recordInfo.IsNull being true, which will cause log-scan problems.
            // We don't need whatever .PreviousAddress was there, so set it to kTempInvalidAddress (which is nonzero).
            recordInfo.PreviousAddress = kTempInvalidAddress;
            recordInfo.SetInvalid();    // Skip on log scan

            pendingContext.retryNewLogicalAddress = logicalAddress < hlogBase.HeadAddress ? kInvalidAddress : logicalAddress;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]  // Do not inline, to keep TryAllocateRecord lean
        bool GetAllocationForRetry<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref PendingContext<TInput, TOutput, TContext> pendingContext, long minAddress,
                in RecordSizeInfo sizeInfo, out long newLogicalAddress, out long newPhysicalAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Use an earlier allocation from a failed operation, if possible.
            newLogicalAddress = pendingContext.retryNewLogicalAddress;
            pendingContext.retryNewLogicalAddress = 0;

            if (newLogicalAddress <= minAddress || newLogicalAddress < hlogBase.HeadAddress)
            {
                // The record is too small or dropped below headAddress.
                goto Fail;
            }

            newPhysicalAddress = hlogBase.GetPhysicalAddress(newLogicalAddress);
            var newLogRecord = new LogRecord(newPhysicalAddress);

            if (newLogRecord.AllocatedSize < sizeInfo.AllocatedInlineRecordSize)
                goto Fail;
            return true;

        Fail:
            var logRecord = hlog.CreateLogRecord(newLogicalAddress);
            DisposeRecord(ref logRecord, DisposeReason.CASAndRetryFailed);
            newPhysicalAddress = 0;
            return false;
        }
    }
}