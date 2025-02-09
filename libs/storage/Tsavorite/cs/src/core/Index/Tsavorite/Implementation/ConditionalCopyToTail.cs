// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        /// <summary>
        /// Copy a record to the tail of the log after caller has verified it does not exist within a specified range.
        /// </summary>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <param name="pendingContext">pending context created when the operation goes pending.</param>
        /// <param name="srcLogRecord">key of the record.</param>
        /// <param name="input">input passed through.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="userContext">user context corresponding to operation used during completion callback.</param>
        /// <param name="stackCtx">Contains information about the call context, record metadata, and so on</param>
        /// <param name="writeReason">The reason the CopyToTail is being done</param>
        /// <param name="wantIO">Whether to do IO if the search must go below HeadAddress. ReadFromImmutable, for example,
        ///     is just an optimization to avoid future IOs, so if we need an IO here we just defer them to the next Read().</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationStatus ConditionalCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(TSessionFunctionsWrapper sessionFunctions,
                ref PendingContext<TInput, TOutput, TContext> pendingContext, ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, TContext userContext,
                ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, WriteReason writeReason, bool wantIO = true)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord<TValue>
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
                        RecordInfo dummyRecordInfo = default;   // TryCopyToTail only needs this for readcache record invalidation.
                        status = TryCopyToTail(ref pendingContext, ref srcLogRecord, ref input, ref output, ref stackCtx, ref dummyRecordInfo, sessionFunctions, writeReason);
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
                OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx2 = new(stackCtx.hei.hash);
                bool needIO;
                do
                {
                    if (TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(
                            sessionFunctions, srcLogRecord.Key, ref stackCtx2, stackCtx.recSrc.LogicalAddress, minAddress, out status, out needIO))
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
                    return PrepareIOForConditionalOperation(sessionFunctions, ref pendingContext, ref srcLogRecord, ref input, ref output, userContext,
                                                      ref stackCtx2, minAddress, WriteReason.Compaction);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status CompactionConditionalCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(
                TSessionFunctionsWrapper sessionFunctions, ref TSourceLogRecord srcLogRecord, ref TInput input,
                ref TOutput output, long currentAddress, long minAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord<TValue>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is called only from Compaction so the epoch should be protected");
            PendingContext<TInput, TOutput, TContext> pendingContext = new();

            OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx = new(storeFunctions.GetKeyHashCode64(srcLogRecord.Key));
            OperationStatus status;
            bool needIO;
            do
            {
                if (TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, srcLogRecord.Key, ref stackCtx, currentAddress, minAddress, out status, out needIO))
                    return Status.CreateFound();
            }
            while (sessionFunctions.Store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(status, sessionFunctions));

            if (needIO)
                status = PrepareIOForConditionalOperation(sessionFunctions, ref pendingContext, ref srcLogRecord, ref input, ref output, default, ref stackCtx, minAddress, WriteReason.Compaction);
            else
                status = ConditionalCopyToTail(sessionFunctions, ref pendingContext, ref srcLogRecord, ref input, ref output, default, ref stackCtx, WriteReason.Compaction);
            return HandleOperationStatus(sessionFunctions.Ctx, ref pendingContext, status, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PrepareIOForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(
                                        TSessionFunctionsWrapper sessionFunctions, ref PendingContext<TInput, TOutput, TContext> pendingContext,
                                        ref TSourceLogRecord srcLogRecord, ref TInput input, ref TOutput output, TContext userContext,
                                        ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress, WriteReason writeReason,
                                        OperationType opType = OperationType.CONDITIONAL_INSERT)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord<TValue>
        {
            pendingContext.type = opType;
            pendingContext.minAddress = minAddress;
            pendingContext.writeReason = writeReason;
            pendingContext.InitialEntryAddress = Constants.kInvalidAddress;
            pendingContext.InitialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;

            // if pendingContext.IsSet, then we are reissuing IO after previously doing so, and srcLogRecord is probably pendingContext.
            if (!pendingContext.IsSet)
            {
                pendingContext.recordInfo = srcLogRecord.Info;
                Debug.Assert(pendingContext.key == default, "Key unexpectedly set");
                pendingContext.key = hlog.GetKeyContainer(srcLogRecord.Key);
                Debug.Assert(pendingContext.input == default, "Input unexpectedly set");
                pendingContext.input = sessionFunctions.GetHeapContainer(ref input);
                Debug.Assert(pendingContext.value == default, "Value unexpectedly set");
                pendingContext.value = hlog.GetValueContainer(srcLogRecord.GetReadOnlyValue());

                pendingContext.output = output;
                sessionFunctions.ConvertOutputToHeap(ref input, ref pendingContext.output);

                pendingContext.eTag = srcLogRecord.ETag;
                pendingContext.expiration = srcLogRecord.Expiration;
            }

            pendingContext.userContext = userContext;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

            return OperationStatus.RECORD_ON_DISK;
        }
    }
}