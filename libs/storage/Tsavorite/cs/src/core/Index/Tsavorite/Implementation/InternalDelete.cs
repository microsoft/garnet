// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        /// <summary>
        /// Delete operation. Replaces the value corresponding to 'key' with tombstone.
        /// If at head, tries to remove item from hash chain
        /// </summary>
        /// <param name="key">Key of the record to be deleted.</param>
        /// <param name="keyHash"></param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <param name="tsavoriteSession">Callback functions.</param>
        /// <param name="lsn">Operation serial number</param>
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
        internal OperationStatus InternalDelete<Input, Output, Context, TsavoriteSession>(ref Key key, long keyHash, ref Context userContext,
                            ref PendingContext<Input, Output, Context> pendingContext, TsavoriteSession tsavoriteSession, long lsn)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            var latchOperation = LatchOperation.None;

            OperationStackContext<Key, Value> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;

            if (tsavoriteSession.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindTagAndTryTransientXLock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx, out OperationStatus status))
                return status;

            RecordInfo dummyRecordInfo = RecordInfo.InitialValid;
            ref RecordInfo srcRecordInfo = ref dummyRecordInfo;

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                // Search the entire in-memory region; this lets us find a tombstoned record in the immutable region, avoiding unnecessarily adding one.
                if (!TryFindRecordForUpdate(ref key, ref stackCtx, hlog.HeadAddress, out status, wantLock: DoRecordIsolation))
                    return status;

                // Need the extra 'if' to set this here as there are several places it would have to be set otherwise if it has a RecordIsolation lock.
                if (stackCtx.recSrc.HasInMemorySrc)
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();

                // Note: Delete does not track pendingContext.InitialAddress because we don't have an InternalContinuePendingDelete

                DeleteInfo deleteInfo = new()
                {
                    Version = tsavoriteSession.Ctx.version,
                    SessionID = tsavoriteSession.Ctx.sessionID,
                    Address = stackCtx.recSrc.LogicalAddress,
                    KeyHash = stackCtx.hei.hash
                };

                // If we already have a deleted record, there's nothing to do.
                if (srcRecordInfo.Tombstone)
                    return OperationStatus.NOTFOUND;

                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    // Use the readcache record as the CopyUpdater source.
                    goto CreateNewRecord;
                }

                // Check for CPR consistency after checking if source is readcache.
                if (tsavoriteSession.Ctx.phase != Phase.REST)
                {
                    var latchDestination = CheckCPRConsistencyDelete(tsavoriteSession.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
                    switch (latchDestination)
                    {
                        case LatchDestination.Retry:
                            goto LatchRelease;
                        case LatchDestination.CreateNewRecord:
                            goto CreateNewRecord;
                        case LatchDestination.CreatePendingContext:
                            CreatePendingDeleteContext(ref key, userContext, ref pendingContext, tsavoriteSession, lsn, ref stackCtx);
                            goto LatchRelease;
                        default:
                            Debug.Assert(latchDestination == LatchDestination.NormalProcessing, "Unknown latchDestination value; expected NormalProcessing");
                            break;
                    }
                }

                if (stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress)
                {
                    // Mutable Region: Update the record in-place
                    deleteInfo.SetRecordInfo(ref srcRecordInfo);
                    ref Value recordValue = ref stackCtx.recSrc.GetValue();

                    // DeleteInfo's lengths are filled in and GetRecordLengths and SetDeletedValueLength are called inside ConcurrentDeleter, in RecordIsolation if needed
                    if (tsavoriteSession.ConcurrentDeleter(stackCtx.recSrc.PhysicalAddress, ref stackCtx.recSrc.GetKey(), ref recordValue, ref deleteInfo, ref srcRecordInfo, out int fullRecordLength))
                    {
                        MarkPage(stackCtx.recSrc.LogicalAddress, tsavoriteSession.Ctx);

                        // Try to transfer the record from the tag chain to the free record pool iff previous address points to invalid address.
                        // Otherwise an earlier record for this key could be reachable again.
                        // Note: We do not currently consider this reuse for mid-chain records (records past the HashBucket), because TracebackForKeyMatch would need
                        //  to return the next-higher record whose .PreviousAddress points to this one, *and* we'd need to make sure that record was not revivified out.
                        //  Also, we do not consider this in-chain reuse for records with different keys, because we don't get here if the keys don't match.
                        if (CanElide<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref stackCtx, ref srcRecordInfo))
                        {
                            if (!RevivificationManager.IsEnabled)
                            {
                                // If we succeed, we need to SealAndInvalidate. It's fine if we don't succeed here; this is just tidying up the HashBucket. 
                                if (stackCtx.hei.TryElide())
                                    srcRecordInfo.SealAndInvalidate();
                            }
                            else
                            {
                                // Always Seal here, even if we're using the LockTable, because the Sealed state must survive this Delete() call.
                                // We invalidate it also for checkpoint/recovery consistency (this removes Sealed bit so Scan would enumerate records that are not in any
                                // tag chain--they would be in the freelist if the freelist survived Recovery), but we restore the Valid bit if it is returned to the chain,
                                // which due to epoch protection is guaranteed to be done before the record can be written to disk and violate the "No Invalid records in
                                // tag chain" invariant.
                                stackCtx.recSrc.UnlockExclusiveAndSealInvalidate(ref srcRecordInfo);

                                bool isElided = false, isAdded = false;
                                if (RevivificationManager.UseFreeRecordPool)
                                {
                                    Debug.Assert(srcRecordInfo.Tombstone, $"Unexpected loss of Tombstone; Record should have been XLocked or SealInvalidated. RecordInfo: {srcRecordInfo.ToString()}");
                                    (isElided, isAdded) = TryElideAndTransferToFreeList<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref stackCtx, ref srcRecordInfo,
                                                            (deleteInfo.UsedValueLength, deleteInfo.FullValueLength, fullRecordLength));
                                }

                                if (!isElided)
                                {
                                    // Leave this in the chain as a normal Tombstone; we aren't going to add a new record so we can't leave this one sealed.
                                    srcRecordInfo.Unseal(makeValid: true);
                                }
                                else if (!isAdded && RevivificationManager.restoreDeletedRecordsIfBinIsFull)
                                {
                                    // The record was not added to the freelist, but was elided. See if we can put it back in as a normal Tombstone. Since we just
                                    // elided it and the elision criteria is that it is the only above-BeginAddress record in the chain, and elision sets the
                                    // HashBucketEntry.word to 0, it means we do not expect any records for this key's tag to exist after the elision. Therefore,
                                    // we can re-insert the record iff the HashBucketEntry's address is <= kTempInvalidAddress.
                                    stackCtx.hei = new(stackCtx.hei.hash);
                                    FindOrCreateTag(ref stackCtx.hei, hlog.BeginAddress);

                                    if (stackCtx.hei.entry.Address <= Constants.kTempInvalidAddress && stackCtx.hei.TryCAS(stackCtx.recSrc.LogicalAddress))
                                        srcRecordInfo.Unseal(makeValid: true);
                                }
                            }
                        }

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
                if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                {
                    goto CreateNewRecord;
                }

                // Either on-disk or no record exists - drop through to create new record. RecordIsolation does not lock in ReadOnly as we cannot modify the record in-place.
                Debug.Assert(!tsavoriteSession.IsManualLocking || LockTable.IsLockedExclusive(ref key, ref stackCtx.hei), "A Lockable-session Delete() of an on-disk or non-existent key requires a LockTable lock");

            CreateNewRecord:
                // Immutable region or new record
                status = CreateNewRecordDelete(ref key, ref pendingContext, tsavoriteSession, ref stackCtx, ref srcRecordInfo);
                if (!OperationStatusUtils.IsAppend(status))
                {
                    // We should never return "SUCCESS" for a new record operation: it returns NOTFOUND on success.
                    Debug.Assert(OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS);
                    if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync)
                        CreatePendingDeleteContext(ref key, userContext, ref pendingContext, tsavoriteSession, lsn, ref stackCtx);
                }
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                if (!TransientXUnlock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx))
                    stackCtx.recSrc.UnlockExclusive(ref srcRecordInfo, hlog.HeadAddress);
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

        private void CreatePendingDeleteContext<Input, Output, Context, TsavoriteSession>(ref Key key, Context userContext,
                ref PendingContext<Input, Output, Context> pendingContext, TsavoriteSession tsavoriteSession, long lsn, ref OperationStackContext<Key, Value> stackCtx)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            pendingContext.type = OperationType.DELETE;
            if (pendingContext.key == default) pendingContext.key = hlog.GetKeyContainer(ref key);
            pendingContext.userContext = userContext;
            pendingContext.entry.word = stackCtx.recSrc.LatestLogicalAddress;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
            pendingContext.version = tsavoriteSession.Ctx.version;
            pendingContext.serialNum = lsn;
        }

        private LatchDestination CheckCPRConsistencyDelete(Phase phase, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // This is the same logic as Upsert; neither goes pending.
            return CheckCPRConsistencyUpsert(phase, ref stackCtx, ref status, ref latchOperation);
        }

        /// <summary>
        /// Create a new tombstoned record for Delete
        /// </summary>
        /// <param name="key">The record Key</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="tsavoriteSession">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value}.LogicalAddress"/></param>
        private OperationStatus CreateNewRecordDelete<Input, Output, Context, TsavoriteSession>(ref Key key, ref PendingContext<Input, Output, Context> pendingContext,
                                                                                             TsavoriteSession tsavoriteSession, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            var value = default(Value);
            var (actualSize, allocatedSize, keySize) = hlog.GetRecordSize(ref key, ref value);

            // We know the existing record cannot be elided; it must point to a valid record; otherwise InternalDelete would have returned NOTFOUND.
            if (!TryAllocateRecord(tsavoriteSession, ref pendingContext, ref stackCtx, actualSize, ref allocatedSize, keySize, new AllocateOptions() { Recycle = true },
                    out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;

            ref RecordInfo newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: tsavoriteSession.Ctx.InNewVersion, tombstone: true, stackCtx.recSrc.LatestLogicalAddress);
            stackCtx.SetNewRecord(newLogicalAddress);

            DeleteInfo deleteInfo = new()
            {
                Version = tsavoriteSession.Ctx.version,
                SessionID = tsavoriteSession.Ctx.sessionID,
                Address = newLogicalAddress,
                KeyHash = stackCtx.hei.hash,
            };
            deleteInfo.SetRecordInfo(ref newRecordInfo);

            ref Value newRecordValue = ref hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            (deleteInfo.UsedValueLength, deleteInfo.FullValueLength) = GetNewValueLengths(actualSize, allocatedSize, newPhysicalAddress, ref newRecordValue);

            if (!tsavoriteSession.SingleDeleter(ref key, ref newRecordValue, ref deleteInfo, ref newRecordInfo))
            {
                // This record was allocated with a minimal Value size (unless it was a revivified larger record) so there's no room for a Filler,
                // but we may want it for a later Delete, or for insert with a smaller Key.
                if (RevivificationManager.UseFreeRecordPool && RevivificationManager.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize, ref tsavoriteSession.Ctx.RevivificationStats))
                    stackCtx.ClearNewRecord();
                else
                    stackCtx.SetNewRecordInvalid(ref newRecordInfo);

                if (deleteInfo.Action == DeleteAction.CancelOperation)
                    return OperationStatus.CANCELED;
                return OperationStatus.NOTFOUND;    // But not CreatedRecord
            }

            SetTombstoneAndExtraValueLength(ref newRecordValue, ref newRecordInfo, deleteInfo.UsedValueLength, deleteInfo.FullValueLength);

            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            bool success = CASRecordIntoChain(ref key, ref stackCtx, newLogicalAddress, ref newRecordInfo);
            if (success)
            {
                PostCopyToTail(ref key, ref stackCtx, ref srcRecordInfo);

                // Note that this is the new logicalAddress; we have not retrieved the old one if it was below HeadAddress, and thus
                // we do not know whether 'logicalAddress' belongs to 'key' or is a collision.
                tsavoriteSession.PostSingleDeleter(ref key, ref deleteInfo, ref newRecordInfo);

                // Success should always Seal the old record. This may be readcache, readonly, or the temporary recordInfo, which is OK and saves the cost of an "if".
                stackCtx.recSrc.UnlockExclusiveAndSeal(ref srcRecordInfo);    // Not elided so Seal without invalidate

                stackCtx.ClearNewRecord();
                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = newLogicalAddress;
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            ref Value insertedValue = ref hlog.GetValue(newPhysicalAddress);
            ref Key insertedKey = ref hlog.GetKey(newPhysicalAddress);
            tsavoriteSession.DisposeSingleDeleter(ref insertedKey, ref insertedValue, ref deleteInfo);

            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }
    }
}