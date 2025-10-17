// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    internal enum OperationType
    {
        NONE = 0,
        READ,
        RMW,
        UPSERT,
        DELETE,
        CONDITIONAL_INSERT,
        CONDITIONAL_SCAN_PUSH,
    }

    [Flags]
    internal enum OperationStatus
    {
        // Completed Status codes

        /// <summary>
        /// Operation completed successfully, and a record with the specified key was found.
        /// </summary>
        SUCCESS = StatusCode.Found,

        /// <summary>
        /// Operation completed successfully, and a record with the specified key was not found; the operation may have created a new one.
        /// </summary>
        NOTFOUND = StatusCode.NotFound,

        /// <summary>
        /// Operation was canceled by the client.
        /// </summary>
        CANCELED = StatusCode.Canceled,

        /// <summary>
        /// The maximum range that directly maps to the <see cref="StatusCode"/> enumeration; the operation completed. 
        /// This is an internal code to reserve ranges in the <see cref="OperationStatus"/> enumeration.
        /// </summary>
        MAX_MAP_TO_COMPLETED_STATUSCODE = CANCELED,

        // Not-completed Status codes

        /// <summary>
        /// Retry operation immediately, within the current epoch. This is only used in situations where another thread does not need to do another operation 
        /// to bring things into a consistent state.
        /// </summary>
        RETRY_NOW,

        /// <summary>
        /// Retry operation immediately, after refreshing the epoch. This is used in situations where another thread may have done an operation that requires it
        /// to do a subsequent operation to bring things into a consistent state; that subsequent operation may require <see cref="LightEpoch.BumpCurrentEpoch()"/>.
        /// </summary>
        RETRY_LATER,

        /// <summary>
        /// I/O has been enqueued and the caller must go through <see cref="ITsavoriteContext{TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator}.CompletePending(bool, bool)"/> or
        /// <see cref="ITsavoriteContext{TInput, TOutput, TContext, TFunctions, TStoreFunctions, TAllocator}.CompletePendingWithOutputs(out CompletedOutputIterator{TInput, TOutput, TContext}, bool, bool)"/>,
        /// or one of the Async forms.
        /// </summary>
        RECORD_ON_DISK,

        /// <summary>
        /// A checkpoint is in progress so the operation must be retried internally after refreshing the epoch and updating the session context version.
        /// </summary>
        CPR_SHIFT_DETECTED,

        /// <summary>
        /// Allocation failed, due to a need to flush pages. Clients do not see this status directly; they see <see cref="Status.IsPending"/>.
        /// <list type="bullet">
        ///   <item>For Sync operations we retry this as part of <see cref="TsavoriteKV{TStoreFunctions, TAllocator}.HandleImmediateRetryStatus{TInput, TOutput, TContext, TSessionFunctionsWrapper}(OperationStatus, TSessionFunctionsWrapper, ref TsavoriteKV{TStoreFunctions, TAllocator}.PendingContext{TInput, TOutput, TContext})"/>.</item>
        ///   <item>For Async operations we retry this as part of the ".Complete(...)" or ".CompleteAsync(...)" operation on the appropriate "*AsyncResult{}" object.</item>
        /// </list>
        /// </summary>
        ALLOCATE_FAILED,

        /// <summary>
        /// An internal code to reserve ranges in the <see cref="OperationStatus"/> enumeration.
        /// </summary>
        BASIC_MASK = 0xFF,      // Leave plenty of space for future expansion

        ADVANCED_MASK = 0x700,  // Coordinate any changes with OperationStatusUtils.OpStatusToStatusCodeShif
        CREATED_RECORD = StatusCode.CreatedRecord << OperationStatusUtils.OpStatusToStatusCodeShift,
        INPLACE_UPDATED_RECORD = StatusCode.InPlaceUpdatedRecord << OperationStatusUtils.OpStatusToStatusCodeShift,
        COPY_UPDATED_RECORD = StatusCode.CopyUpdatedRecord << OperationStatusUtils.OpStatusToStatusCodeShift,
        COPIED_RECORD = StatusCode.CopiedRecord << OperationStatusUtils.OpStatusToStatusCodeShift,
        COPIED_RECORD_TO_READ_CACHE = StatusCode.CopiedRecordToReadCache << OperationStatusUtils.OpStatusToStatusCodeShift,
        // unused (StatusCode)0x60,
        // unused (StatusCode)0x70,
        EXPIRED = StatusCode.Expired << OperationStatusUtils.OpStatusToStatusCodeShift
    }

    internal static class OperationStatusUtils
    {
        // StatusCode has this in the high nybble of the first (only) byte; put it in the low nybble of the second byte here).
        // Coordinate any changes with OperationStatus.ADVANCED_MASK.
        internal const int OpStatusToStatusCodeShift = 4;

        internal static OperationStatus BasicOpCode(OperationStatus status) => status & OperationStatus.BASIC_MASK;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static OperationStatus AdvancedOpCode(OperationStatus status, StatusCode advancedStatusCode) => status | (OperationStatus)((int)advancedStatusCode << OpStatusToStatusCodeShift);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool TryConvertToCompletedStatusCode(OperationStatus advInternalStatus, out Status statusCode)
        {
            var internalStatus = BasicOpCode(advInternalStatus);
            if (internalStatus <= OperationStatus.MAX_MAP_TO_COMPLETED_STATUSCODE)
            {
                statusCode = new(advInternalStatus);
                return true;
            }
            statusCode = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsAppend(OperationStatus internalStatus)
        {
            var advInternalStatus = internalStatus & OperationStatus.ADVANCED_MASK;
            return advInternalStatus == OperationStatus.CREATED_RECORD || advInternalStatus == OperationStatus.COPY_UPDATED_RECORD;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsRetry(OperationStatus internalStatus) => internalStatus == OperationStatus.RETRY_NOW || internalStatus == OperationStatus.RETRY_LATER;
    }
}