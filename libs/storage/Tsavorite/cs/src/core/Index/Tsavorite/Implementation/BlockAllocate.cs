// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryBlockAllocate<Input, Output, Context>(
                AllocatorBase<Key, Value> allocator,
                int recordSize,
                out long logicalAddress,
                ref PendingContext<Input, Output, Context> pendingContext,
                out OperationStatus internalStatus)
        {
            pendingContext.flushEvent = allocator.FlushEvent;
            logicalAddress = allocator.TryAllocate(recordSize);
            if (logicalAddress > 0)
            {
                pendingContext.flushEvent = default;
                internalStatus = OperationStatus.SUCCESS;
                return true;
            }

            if (logicalAddress == 0)
            {
                // We expect flushEvent to be signaled.
                internalStatus = OperationStatus.ALLOCATE_FAILED;
                return false;
            }

            // logicalAddress is < 0 so we do not expect flushEvent to be signaled; return RETRY_LATER to refresh the epoch.
            pendingContext.flushEvent = default;
            allocator.TryComplete();
            internalStatus = OperationStatus.RETRY_LATER;
            return false;
        }

        internal struct AllocateOptions
        {
            internal bool Recycle;
            internal bool IgnoreHeiAddress;
        };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool TryAllocateRecord<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession, ref PendingContext<Input, Output, Context> pendingContext,
                                                       ref OperationStackContext<Key, Value> stackCtx, int actualSize, ref int allocatedSize, int newKeySize, AllocateOptions options,
                                                       out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            status = OperationStatus.SUCCESS;

            // MinRevivAddress is also needed for pendingContext-based record reuse.
            var minMutableAddress = GetMinRevivifiableAddress();
            var minRevivAddress = minMutableAddress;

            if (options.Recycle && pendingContext.retryNewLogicalAddress != Constants.kInvalidAddress && GetAllocationForRetry(tsavoriteSession, ref pendingContext, minRevivAddress, ref allocatedSize, newKeySize, out newLogicalAddress, out newPhysicalAddress))
                return true;
            if (RevivificationManager.UseFreeRecordPool)
            {
                if (!options.IgnoreHeiAddress && stackCtx.hei.Address >= minMutableAddress)
                    minRevivAddress = stackCtx.hei.Address;
                if (tsavoriteSession.Ctx.IsInV1)
                {
                    var fuzzyStartAddress = _hybridLogCheckpoint.info.startLogicalAddress;
                    if (fuzzyStartAddress > minRevivAddress)
                        minRevivAddress = fuzzyStartAddress;
                }
                if (TryTakeFreeRecord<Input, Output, Context, TsavoriteSession>(tsavoriteSession, actualSize, ref allocatedSize, newKeySize, minRevivAddress, out newLogicalAddress, out newPhysicalAddress))
                    return true;
            }

            // Spin to make sure newLogicalAddress is > recSrc.LatestLogicalAddress (the .PreviousAddress and CAS comparison value).
            for (; ; Thread.Yield())
            {
                if (!TryBlockAllocate(hlog, allocatedSize, out newLogicalAddress, ref pendingContext, out status))
                    break;

                newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
                if (VerifyInMemoryAddresses(ref stackCtx))
                {
                    if (newLogicalAddress > stackCtx.recSrc.LatestLogicalAddress)
                        return true;

                    // This allocation is below the necessary address so put it on the free list or abandon it, then repeat the loop.
                    if (!RevivificationManager.UseFreeRecordPool || !RevivificationManager.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize, ref tsavoriteSession.Ctx.RevivificationStats))
                        hlog.GetInfo(newPhysicalAddress).SetInvalid();  // Skip on log scan
                    continue;
                }

                // In-memory source dropped below HeadAddress during BlockAllocate. Save the record for retry if we can.
                ref var newRecordInfo = ref hlog.GetInfo(newPhysicalAddress);
                if (options.Recycle)
                {
                    ref var newValue = ref hlog.GetValue(newPhysicalAddress);
                    hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
                    int valueOffset = (int)((long)Unsafe.AsPointer(ref newValue) - newPhysicalAddress);
                    SetExtraValueLength(ref hlog.GetValue(newPhysicalAddress), ref newRecordInfo, actualSize - valueOffset, allocatedSize - valueOffset);
                    SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
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
        bool TryAllocateRecordReadCache<Input, Output, Context>(ref PendingContext<Input, Output, Context> pendingContext, ref OperationStackContext<Key, Value> stackCtx,
                                                       int allocatedSize, out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status)
        {
            // Spin to make sure the start of the tag chain is not readcache, or that newLogicalAddress is > the first address in the tag chain.
            for (; ; Thread.Yield())
            {
                if (!TryBlockAllocate(readcache, allocatedSize, out newLogicalAddress, ref pendingContext, out status))
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void SaveAllocationForRetry<Input, Output, Context>(ref PendingContext<Input, Output, Context> pendingContext, long logicalAddress, long physicalAddress, int allocatedSize)
        {
            ref var recordInfo = ref hlog.GetInfo(physicalAddress);

            // TryAllocateRecord may stash this before WriteRecordInfo is called, leaving .PreviousAddress set to kInvalidAddress.
            // This is zero, and setting Invalid will result in recordInfo.IsNull being true, which will cause log-scan problems.
            // We don't need whatever .PreviousAddress was there, so set it to kTempInvalidAddress (which is nonzero).
            recordInfo.PreviousAddress = Constants.kTempInvalidAddress;
            recordInfo.SetInvalid();    // Skip on log scan

            // ExtraValueLength has been set by caller.
            pendingContext.retryNewLogicalAddress = logicalAddress < hlog.HeadAddress ? Constants.kInvalidAddress : logicalAddress;
        }

        // Do not inline, to keep TryAllocateRecord lean
        bool GetAllocationForRetry<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession, ref PendingContext<Input, Output, Context> pendingContext, long minAddress,
                ref int allocatedSize, int newKeySize, out long newLogicalAddress, out long newPhysicalAddress)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            // Use an earlier allocation from a failed operation, if possible.
            newLogicalAddress = pendingContext.retryNewLogicalAddress;
            pendingContext.retryNewLogicalAddress = 0;

            if (newLogicalAddress < hlog.HeadAddress)
            {
                // The record dropped below headAddress. If it needs DisposeForRevivification, it will be done on eviction.
                newPhysicalAddress = 0;
                return false;
            }

            newPhysicalAddress = hlog.GetPhysicalAddress(newLogicalAddress);
            ref var recordInfo = ref hlog.GetInfo(newPhysicalAddress);
            Debug.Assert(!recordInfo.IsNull(), "RecordInfo should not be IsNull");
            ref var recordValue = ref hlog.GetValue(newPhysicalAddress);
            (int usedValueLength, int fullValueLength, int fullRecordLength) = GetRecordLengths(newPhysicalAddress, ref recordValue, ref recordInfo);

            // Dispose the record for either reuse or abandonment.
            ClearExtraValueSpace(ref recordInfo, ref recordValue, usedValueLength, fullValueLength);
            tsavoriteSession.DisposeForRevivification(ref hlog.GetKey(newPhysicalAddress), ref recordValue, newKeySize, ref recordInfo);

            if (newLogicalAddress <= minAddress || fullRecordLength < allocatedSize)
            {
                // Can't reuse, so abandon it.
                newPhysicalAddress = 0;
                return false;
            }

            allocatedSize = fullRecordLength;
            return true;
        }
    }
}