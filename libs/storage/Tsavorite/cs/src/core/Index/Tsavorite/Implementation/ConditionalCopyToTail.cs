// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    using static LogAddress;

    public unsafe partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>
        /// Copy a record to the tail of the log after caller has verified it does not exist within a specified range.
        /// </summary>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <param name="pendingContext">pending context created when the operation goes pending.</param>
        /// <param name="srcLogRecord">key of the record.</param>
        /// <param name="stackCtx">Contains information about the call context, record metadata, and so on</param>
        /// <param name="wantIO">Whether to do IO if the search must go below HeadAddress. ReadFromImmutable, for example,
        ///     is just an optimization to avoid future IOs, so if we need an IO here we just defer them to the next Read().</param>
        /// <param name="maxAddress">Maximum address for determining liveness, records after this address are not considered when checking validity.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationStatus ConditionalCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(TSessionFunctionsWrapper sessionFunctions,
                ref PendingContext<TInput, TOutput, TContext> pendingContext, in TSourceLogRecord srcLogRecord,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, bool wantIO = true, long maxAddress = long.MaxValue)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var callerHasEphemeralLock = stackCtx.recSrc.HasEphemeralSLock;

            // We are called by one of ReadFromImmutable, CompactionConditionalCopyToTail, or ContinuePendingConditionalCopyToTail;
            // these have already searched to see if the record is present above minAddress, and stackCtx is set up for the first try.
            // minAddress is the stackCtx.recSrc.LatestLogicalAddress; by the time we get here, any IO below that has been done due to
            // PrepareConditionalCopyToTailIO, which then went to ContinuePendingConditionalCopyToTail, which evaluated whether the
            // record was found at that level.
            while (true)
            {
                // ConditionalCopyToTail is different from the usual procedures, in that if we find a source record we don't lock--we exit with success.
                // So we only lock for tag-chain stability during search.
                if (callerHasEphemeralLock || TryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out var status))
                {
                    try
                    {
                        status = TryCopyToTail(in srcLogRecord, sessionFunctions, ref pendingContext, ref stackCtx);
                    }
                    finally
                    {
                        stackCtx.HandleNewRecordOnException(this);
                        if (!callerHasEphemeralLock)
                            EphemeralSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
                    }
                }

                // If we're here we failed TryCopyToTail, probably a failed CAS due to another record insertion.
                if (!HandleImmediateRetryStatus(status, sessionFunctions, ref pendingContext))
                    return status;

                // HandleImmediateRetryStatus may have refreshed the epoch which means HeadAddress etc. may have changed. Re-traverse from the tail to the highest
                // point we just searched (which may have gone below HeadAddress). +1 to LatestLogicalAddress because we have examined that already. Use stackCtx2 to
                // preserve stacKCtx, both for retrying the Insert if needed and for preserving the caller's lock status, etc.
                var minAddress = stackCtx.recSrc.LatestLogicalAddress + 1;
                OperationStackContext<TStoreFunctions, TAllocator> stackCtx2 = new(stackCtx.hei.hash);
                bool needIO;
                do
                {
                    if (TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
                            sessionFunctions, srcLogRecord.Key, srcLogRecord.Namespace, ref stackCtx2, stackCtx.recSrc.LogicalAddress, minAddress, maxAddress, out status, out needIO))
                        return OperationStatus.SUCCESS;
                }
                while (HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions));

                // Issue IO if necessary and desired (for ReadFromImmutable, it isn't; just exit in that case), else loop back up and retry the insert.
                if (!wantIO)
                {
                    // Caller (e.g. ReadFromImmutable) called this to keep read-hot records in memory. That's already failed, so give up and we'll read it when we have to.
                    if (stackCtx.recSrc.LogicalAddress < hlogBase.HeadAddress)
                        return OperationStatus.SUCCESS;
                }
                else if (needIO)
                    return PrepareIOForConditionalOperation(ref pendingContext, in srcLogRecord, ref stackCtx2, minAddress, maxAddress);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status CompactionConditionalCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(
                TSessionFunctionsWrapper sessionFunctions, in TSourceLogRecord srcLogRecord, long currentAddress, long minAddress, long maxAddress = long.MaxValue)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is called only from Compaction so the epoch should be protected");
            PendingContext<TInput, TOutput, TContext> pendingContext = new();

            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(storeFunctions.GetKeyHashCode64(srcLogRecord.Key, srcLogRecord.Namespace));
            OperationStatus status;
            bool needIO;
            do
            {
                if (TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, srcLogRecord.Key, srcLogRecord.Namespace, ref stackCtx, currentAddress, minAddress, maxAddress, out status, out needIO))
                    return Status.CreateFound();
            }
            while (sessionFunctions.Store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions));

            if (needIO)
                status = PrepareIOForConditionalOperation(ref pendingContext, in srcLogRecord, ref stackCtx, minAddress, maxAddress);
            else
                status = ConditionalCopyToTail(sessionFunctions, ref pendingContext, in srcLogRecord, ref stackCtx, maxAddress: maxAddress);
            return HandleOperationStatus(sessionFunctions.Ctx, ref pendingContext, status, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PrepareIOForConditionalOperation<TInput, TOutput, TContext, TSourceLogRecord>(
                                        ref PendingContext<TInput, TOutput, TContext> pendingContext, in TSourceLogRecord srcLogRecord,
                                        ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long minAddress, long maxAddress, OperationType opType = OperationType.CONDITIONAL_INSERT)
            where TSourceLogRecord : ISourceLogRecord
        {
            pendingContext.CopyKey(srcLogRecord.Key, hlogBase.bufferPool);
            pendingContext.type = opType;
            pendingContext.minAddress = minAddress;
            pendingContext.maxAddress = maxAddress;
            pendingContext.initialEntryAddress = kInvalidAddress;
            pendingContext.initialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

            // Transfer the log record to the pending context. We do not want to dispose memory log records; those objects are still alive in the log.
            if (!pendingContext.diskLogRecord.IsSet)
            {
                pendingContext.CopyFrom(in srcLogRecord, hlogBase.bufferPool, hlogBase.transientObjectIdMap,
                    srcLogRecord.IsMemoryLogRecord
                        ? obj => { }
                : obj => storeFunctions.DisposeValueObject(obj, DisposeReason.DeserializedFromDisk));
            }
            return OperationStatus.RECORD_ON_DISK;
        }
    }
}