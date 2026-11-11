// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private OperationStatus ReadCacheCopyToTail<TInput, TOutput, TContext, TSessionFunctionsWrapper>(in LogRecord srcLogRecord,
                TSessionFunctionsWrapper sessionFunctions, ref OperationState<TInput, TOutput, TContext> operationState,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Only hits in the read-only, second-chance region need a fresh copy. The hash-entry CAS
            // rejects a copy if an update or another promotion changed the chain after this lookup.
            if (AbsoluteAddress(stackCtx.recSrc.LogicalAddress) < readcacheBase.SafeReadOnlyAddress
                    && TryCopyToReadCache(in srcLogRecord, sessionFunctions, ref operationState, ref stackCtx))
                return OperationStatus.SUCCESS | OperationStatus.COPIED_RECORD_TO_READ_CACHE;
            return OperationStatus.SUCCESS;
        }

        /// <summary>
        /// Copy a record from disk, the immutable main log, or the read cache to the read-cache tail.
        /// </summary>
        /// <param name="operationState"></param>
        /// <param name="inputLogRecord">Source log record.</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{TStoreFunctions, TAllocator}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="sessionFunctions"></param>
        /// <returns>True if copied to readcache, else false; readcache is "best effort", and we don't fail the read process, or slow it down by retrying.
        /// </returns>
        internal bool TryCopyToReadCache<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(in TSourceLogRecord inputLogRecord, TSessionFunctionsWrapper sessionFunctions,
                                        ref OperationState<TInput, TOutput, TContext> operationState, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var sizeInfo = new RecordSizeInfo() { FieldInfo = inputLogRecord.GetRecordFieldInfo() };
            hlog.PopulateRecordSizeInfo(ref sizeInfo);

            if (!TryAllocateRecordReadCache(ref operationState, ref stackCtx, in sizeInfo, out var newLogicalAddress, out var newPhysicalAddress, out _ /*status*/))
                return false;
            var newLogRecord = WriteNewRecordInfo(inputLogRecord, readcacheBase, newLogicalAddress, newPhysicalAddress, in sizeInfo, inNewVersion: false, previousAddress: stackCtx.hei.Address);

            stackCtx.SetNewRecord(newLogicalAddress | RecordInfo.kIsReadCacheBitMask);
            _ = newLogRecord.TryCopyFrom(in inputLogRecord, in sizeInfo);

            // Insert the new record by CAS'ing it into the hash entry (read-cache records are always head-inserted).
            // The head CAS is the single linearization point: a newer main-log record for this key can only be published
            // via a head CAS (a value-changing update detaching the prefix, a CTT, or another promotion) - which would
            // have changed hei.entry and failed this CAS - or it already existed when the pending read completed, in
            // which case the pending-read pre-check / re-issue (see InternalContinuePendingRead) would have used it
            // instead of promoting. So a successful CAS here cannot shadow a newer value.
            if (!stackCtx.hei.TryCAS(newLogicalAddress | RecordInfo.kIsReadCacheBitMask))
            {
                // Another insert won the head; abandon this copy (read-cache population is best-effort).
                stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
                OnDispose(ref newLogRecord, DisposeReason.InitialWriterCASFailed);
                newLogRecord.InfoRef.PreviousAddress = kTempInvalidAddress;     // Necessary for ReadCacheEvict, but cannot be kInvalidAddress or we have recordInfo.IsNull
                return false;
            }

            // We don't call PostInitialWriter here so we must do the size tracking separately.
            readcacheBase.logSizeTracker?.UpdateSize(in newLogRecord, add: true);

            newLogRecord.InfoRef.UnsealAndValidate();
            // Do not clear operationState.logicalAddress; we've already set it to the requested address, which is valid. We don't expose readcache
            // addresses, but here we found it in the main log address space, so retain that address.
            stackCtx.ClearNewRecord();
            return true;
        }
    }
}