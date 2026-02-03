// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
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
        /// Delete operation. Replaces the value corresponding to 'key' with tombstone.
        /// If at head, tries to remove item from hash chain
        /// </summary>
        /// <param name="key">Key of the record to be deleted.</param>
        /// <param name="keyHash"></param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully deleted</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, long keyHash, ref TContext userContext,
                            ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var latchOperation = LatchOperation.None;

            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;
            pendingContext.logicalAddress = kInvalidAddress;
            pendingContext.eTag = LogRecord.NoETag;

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out OperationStatus status))
                return status;

            LogRecord srcLogRecord = default;

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                // Search the entire in-memory region; this lets us find a tombstoned record in the immutable region, avoiding unnecessarily adding one.
                if (!TryFindRecordForUpdate(key, ref stackCtx, hlogBase.HeadAddress, out status))
                    return status;

                // Note: Delete does not track pendingContext.InitialAddress because we don't have an InternalContinuePendingDelete

                DeleteInfo deleteInfo = new()
                {
                    Version = sessionFunctions.Ctx.version,
                    SessionID = sessionFunctions.Ctx.sessionID,
                    Address = stackCtx.recSrc.LogicalAddress,
                    KeyHash = stackCtx.hei.hash
                };

                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    // Use the readcache record as the CopyUpdater source.
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    goto CreateNewRecord;
                }

                // Check for CPR consistency after checking if source is readcache.
                if (sessionFunctions.Ctx.phase != Phase.REST)
                {
                    var latchDestination = CheckCPRConsistencyDelete(sessionFunctions.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
                    switch (latchDestination)
                    {
                        case LatchDestination.Retry:
                            goto LatchRelease;
                        case LatchDestination.CreateNewRecord:
                            if (stackCtx.recSrc.HasMainLogSrc)
                                srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                            goto CreateNewRecord;
                        default:
                            Debug.Assert(latchDestination == LatchDestination.NormalProcessing, "Unknown latchDestination value; expected NormalProcessing");
                            break;
                    }
                }

                if (stackCtx.recSrc.LogicalAddress >= hlogBase.ReadOnlyAddress)
                {
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();

                    pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                    pendingContext.eTag = srcLogRecord.ETag;

                    // If we already have a deleted record, there's nothing to do.
                    if (srcLogRecord.Info.Tombstone)
                        return OperationStatus.NOTFOUND;

                    // Mutable Region: Update the record in-place

                    // DeleteInfo's lengths are filled in and GetRecordLengths and SetDeletedValueLength are called inside InPlaceDeleter.
                    if (sessionFunctions.InPlaceDeleter(ref srcLogRecord, ref deleteInfo))
                    {
                        MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);

                        // Try to transfer the record from the tag chain to the free record pool iff previous address points to invalid address.
                        // Otherwise an earlier record for this key could be reachable again.
                        if (CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, srcLogRecord.Info))
                            HandleRecordElision<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcLogRecord);
                        else
                            DisposeRecord(ref srcLogRecord, DisposeReason.Deleted);

                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                        goto LatchRelease;
                    }

                    if (deleteInfo.Action == DeleteAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        goto LatchRelease;
                    }

                    // Could not delete in place for some reason - create new record.
                    goto CreateNewRecord;
                }
                else if (stackCtx.recSrc.HasMainLogSrc)
                {
                    // If we already have a deleted record, there's nothing to do.
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    if (srcLogRecord.Info.Tombstone)
                        return OperationStatus.NOTFOUND;
                    goto CreateNewRecord;
                }

                // Either on-disk or no record exists - drop through to create new record.
                Debug.Assert(!sessionFunctions.IsTransactionalLocking || LockTable.IsLockedExclusive(ref stackCtx.hei), "A Transactional-session Delete() of an on-disk or non-existent key requires a LockTable lock");

            CreateNewRecord:
                // Immutable region or new record
                status = CreateNewRecordDelete(key, ref srcLogRecord, ref pendingContext, sessionFunctions, ref stackCtx);

                // We should never return "SUCCESS" for a new record operation: it returns NOTFOUND on success.
                Debug.Assert(OperationStatusUtils.IsAppend(status) || OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS);
                goto LatchRelease;
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                EphemeralXUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
            }

        LatchRelease:
            {
                switch (latchOperation)
                {
                    case LatchOperation.Shared:
                        HashBucket.ReleaseSharedLatch(ref stackCtx.hei);
                        break;
                    case LatchOperation.Exclusive:
                        HashBucket.ReleaseExclusiveLatch(ref stackCtx.hei);
                        break;
                    default:
                        break;
                }
            }
            return status;
        }

        private LatchDestination CheckCPRConsistencyDelete(Phase phase, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // This is the same logic as Upsert; neither goes pending.
            return CheckCPRConsistencyUpsert(phase, ref stackCtx, ref status, ref latchOperation);
        }

        /// <summary>
        /// Create a new tombstoned record for Delete
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="srcLogRecord">The source record, if <paramref name="stackCtx"/>.<see cref="RecordSource{TStoreFunctions, TAllocator}.HasInMemorySrc"/> and
        /// it is either too small or is in readonly region, or is in raadcache</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="sessionFunctions">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{TStoreFunctions, TAllocator}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        private OperationStatus CreateNewRecordDelete<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, ref LogRecord srcLogRecord, ref PendingContext<TInput, TOutput, TContext> pendingContext,
                TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            var sizeInfo = hlog.GetDeleteRecordSize(key);
            AllocateOptions allocOptions = new()
            {
                recycle = true,
                elideSourceRecord = stackCtx.recSrc.HasMainLogSrc && CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, srcLogRecord.Info)
            };

            // We know the existing record cannot be elided; it must point to a valid record; otherwise InternalDelete would have returned NOTFOUND.
            if (!TryAllocateRecord(sessionFunctions, ref pendingContext, ref stackCtx, ref sizeInfo, allocOptions, out var newLogicalAddress, out var newPhysicalAddress, out var status))
                return status;

            var newLogRecord = WriteNewRecordInfo(key, hlogBase, newLogicalAddress, newPhysicalAddress, in sizeInfo, sessionFunctions.Ctx.InNewVersion, previousAddress: stackCtx.recSrc.LatestLogicalAddress);
            newLogRecord.InfoRef.SetTombstone();
            stackCtx.SetNewRecord(newLogicalAddress);

            DeleteInfo deleteInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                SessionID = sessionFunctions.Ctx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
            };

            if (!sessionFunctions.InitialDeleter(ref newLogRecord, ref deleteInfo))
            {
                // This record was allocated with a minimal Value size (unless it was a revivified larger record) so there's no room for a Filler,
                // but we may want it for a later Delete, or for insert with a smaller Key.
                stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
                if (!RevivificationManager.UseFreeRecordPool || !TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, newLogicalAddress, ref newLogRecord))
                    DisposeRecord(ref newLogRecord, DisposeReason.InsertAbandoned);

                if (deleteInfo.Action == DeleteAction.CancelOperation)
                    return OperationStatus.CANCELED;
                return OperationStatus.NOTFOUND;    // But not CreatedRecord
            }

            newLogRecord.InfoRef.SetTombstone();

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            var success = CASRecordIntoChain(newLogicalAddress, ref newLogRecord, ref stackCtx);
            if (success)
            {
                PostCopyToTail(in srcLogRecord, ref stackCtx);

                // Note that this is the new logicalAddress; we have not retrieved the old one if it was below HeadAddress, and thus
                // we do not know whether 'logicalAddress' belongs to 'key' or is a collision.
                sessionFunctions.PostInitialDeleter(ref newLogRecord, ref deleteInfo);

                // Success should always Seal the old record. This may be readcache or readonly, which is OK.
                if (stackCtx.recSrc.HasMainLogSrc)
                    srcLogRecord.InfoRef.Seal();    // Not elided so Seal without invalidate

                stackCtx.ClearNewRecord();
                pendingContext.logicalAddress = newLogicalAddress;
                pendingContext.eTag = newLogRecord.ETag;
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newLogRecord.InfoRef);
            DisposeRecord(ref newLogRecord, DisposeReason.InitialDeleterCASFailed);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}