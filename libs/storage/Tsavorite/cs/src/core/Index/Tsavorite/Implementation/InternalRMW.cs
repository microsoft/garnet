﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        /// <summary>
        /// Read-Modify-Write Operation. Updates value of 'key' using 'input' and current value.
        /// Pending operations are processed either using InternalRetryPendingRMW or 
        /// InternalContinuePendingRMW.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="keyHash">the hash of <parameref name="key"/></param>
        /// <param name="input">input used to update the value.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="userContext">user context corresponding to operation used during completion callback.</param>
        /// <param name="pendingContext">pending context created when the operation goes pending.</param>
        /// <param name="sessionFunctions">Callback functions.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated (or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalRMW<Input, Output, Context, TSessionFunctionsWrapper>(ref Key key, long keyHash, ref Input input, ref Output output, ref Context userContext,
                                    ref PendingContext<Input, Output, Context> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            var latchOperation = LatchOperation.None;

            OperationStackContext<Key, Value> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindOrCreateTagAndTryTransientXLock<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx, out OperationStatus status))
                return status;

            RecordInfo dummyRecordInfo = RecordInfo.InitialValid;
            ref RecordInfo srcRecordInfo = ref dummyRecordInfo;

            // We must use try/finally to ensure unlocking even in the presence of exceptions.
            try
            {
                // Search the entire in-memory region.
                if (!TryFindRecordForUpdate(ref key, ref stackCtx, hlog.HeadAddress, out status))
                    return status;

                // These track the latest main-log address in the tag chain; InternalContinuePendingRMW uses them to check for new inserts.
                pendingContext.InitialEntryAddress = stackCtx.hei.Address;
                pendingContext.InitialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;

                // If there is a readcache record, use it as the CopyUpdater source.
                if (stackCtx.recSrc.HasReadCacheSrc)
                {
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                    goto CreateNewRecord;
                }

                // Check for CPR consistency after checking if source is readcache.
                if (sessionFunctions.Ctx.phase != Phase.REST)
                {
                    var latchDestination = CheckCPRConsistencyRMW(sessionFunctions.Ctx.phase, ref stackCtx, ref status, ref latchOperation);
                    switch (latchDestination)
                    {
                        case LatchDestination.Retry:
                            goto LatchRelease;
                        case LatchDestination.CreateNewRecord:
                            goto CreateNewRecord;
                        default:
                            Debug.Assert(latchDestination == LatchDestination.NormalProcessing, "Unknown latchDestination value; expected NormalProcessing");
                            break;
                    }
                }

                if (stackCtx.recSrc.LogicalAddress >= hlog.ReadOnlyAddress)
                {
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();

                    // Mutable Region: Update the record in-place. We perform mutable updates only if we are in normal processing phase of checkpointing
                    RMWInfo rmwInfo = new()
                    {
                        Version = sessionFunctions.Ctx.version,
                        SessionID = sessionFunctions.Ctx.sessionID,
                        Address = stackCtx.recSrc.LogicalAddress,
                        KeyHash = stackCtx.hei.hash,
                        IsFromPending = pendingContext.type != OperationType.NONE,
                    };

                    rmwInfo.SetRecordInfo(ref srcRecordInfo);
                    ref Value recordValue = ref stackCtx.recSrc.GetValue();

                    if (srcRecordInfo.Tombstone)
                    {
                        // If we're doing revivification and this is in the revivifiable range, try to revivify--otherwise we'll create a new record.
                        if (RevivificationManager.IsEnabled && stackCtx.recSrc.LogicalAddress >= GetMinRevivifiableAddress())
                        {
                            if (!sessionFunctions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo))
                            {
                                status = OperationStatus.NOTFOUND;
                                goto LatchRelease;
                            }

                            if (TryRevivifyInChain(ref key, ref input, ref output, ref pendingContext, sessionFunctions, ref stackCtx, ref srcRecordInfo, ref rmwInfo, out status, ref recordValue)
                                    || status != OperationStatus.SUCCESS)
                                goto LatchRelease;
                        }
                        goto CreateNewRecord;
                    }

                    // rmwInfo's lengths are filled in and GetValueLengths and SetLength are called inside InPlaceUpdater.
                    if (sessionFunctions.InPlaceUpdater(stackCtx.recSrc.PhysicalAddress, ref key, ref input, ref recordValue, ref output, ref rmwInfo, out status, ref srcRecordInfo)
                        || (rmwInfo.Action == RMWAction.ExpireAndStop))
                    {
                        MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);

                        // ExpireAndStop means to override default Delete handling (which is to go to InitialUpdater) by leaving the tombstoned record as current.
                        // Our SessionFunctionsWrapper.InPlaceUpdater implementation has already reinitialized-in-place or set Tombstone as appropriate and marked the record.
                        pendingContext.recordInfo = srcRecordInfo;
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        // status has been set by InPlaceUpdater
                        goto LatchRelease;
                    }

                    if (OperationStatusUtils.BasicOpCode(status) != OperationStatus.SUCCESS)
                        goto LatchRelease;

                    // InPlaceUpdater failed (e.g. insufficient space, another thread set Tombstone, etc). Use this record as the CopyUpdater source.
                    goto CreateNewRecord;
                }
                if (stackCtx.recSrc.LogicalAddress >= hlog.SafeReadOnlyAddress && !stackCtx.recSrc.GetInfo().Tombstone)
                {
                    // Fuzzy Region: Must retry after epoch refresh, due to lost-update anomaly
                    status = OperationStatus.RETRY_LATER;
                    goto LatchRelease;
                }
                if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                {
                    // Safe Read-Only Region: CopyUpdate to create a record in the mutable region.
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                    goto CreateNewRecord;
                }
                if (stackCtx.recSrc.LogicalAddress >= hlog.BeginAddress)
                {
                    if (hlog.IsNullDevice)
                        goto CreateNewRecord;

                    // Disk Region: Need to issue async io requests. Locking will be checked on pending completion.
                    status = OperationStatus.RECORD_ON_DISK;
                    CreatePendingRMWContext(ref key, ref input, output, userContext, ref pendingContext, sessionFunctions, ref stackCtx);
                    goto LatchRelease;
                }

                // No record exists - drop through to create new record.
                Debug.Assert(!sessionFunctions.IsManualLocking || LockTable.IsLockedExclusive(ref stackCtx.hei), "A Lockable-session RMW() of an on-disk or non-existent key requires a LockTable lock");

            CreateNewRecord:
                {
                    Value tempValue = default;
                    ref Value value = ref (stackCtx.recSrc.HasInMemorySrc ? ref stackCtx.recSrc.GetValue() : ref tempValue);

                    // Here, the input* data for 'doingCU' is the same as recSrc.
                    status = CreateNewRecordRMW(ref key, ref input, ref value, ref output, ref pendingContext, sessionFunctions, ref stackCtx, ref srcRecordInfo,
                                                doingCU: stackCtx.recSrc.HasInMemorySrc && !srcRecordInfo.Tombstone);
                    if (!OperationStatusUtils.IsAppend(status))
                    {
                        // OperationStatus.SUCCESS is OK here; it means NeedCopyUpdate or NeedInitialUpdate returned false
                        if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync || status == OperationStatus.RECORD_ON_DISK)
                            CreatePendingRMWContext(ref key, ref input, output, userContext, ref pendingContext, sessionFunctions, ref stackCtx);
                    }
                    goto LatchRelease;
                }
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                TransientXUnlock<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx);
            }

        LatchRelease:
            if (latchOperation != LatchOperation.None)
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

        // No AggressiveInlining; this is a less-common function and it may improve inlining of InternalUpsert if the compiler decides not to inline this.
        private void CreatePendingRMWContext<Input, Output, Context, TSessionFunctionsWrapper>(ref Key key, ref Input input, Output output, Context userContext,
                ref PendingContext<Input, Output, Context> pendingContext, TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<Key, Value> stackCtx)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            pendingContext.type = OperationType.RMW;
            if (pendingContext.key == default)
                pendingContext.key = hlog.GetKeyContainer(ref key);
            if (pendingContext.input == default)
                pendingContext.input = sessionFunctions.GetHeapContainer(ref input);

            pendingContext.output = output;
            sessionFunctions.ConvertOutputToHeap(ref input, ref pendingContext.output);

            pendingContext.userContext = userContext;
            pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
        }

        private bool TryRevivifyInChain<Input, Output, Context, TSessionFunctionsWrapper>(ref Key key, ref Input input, ref Output output, ref PendingContext<Input, Output, Context> pendingContext,
                        TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo, ref RMWInfo rmwInfo, out OperationStatus status, ref Value recordValue)
                    where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            if (IsFrozen<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcRecordInfo))
                goto NeedNewRecord;

            // This record is safe to revivify even if its PreviousAddress points to a valid record, because it is revivified for the same key.
            bool ok = true;
            try
            {
                if (srcRecordInfo.Tombstone)
                {
                    srcRecordInfo.Tombstone = false;

                    if (RevivificationManager.IsFixedLength)
                        rmwInfo.UsedValueLength = rmwInfo.FullValueLength = RevivificationManager<Key, Value>.FixedValueLength;
                    else
                    {
                        var recordLengths = GetRecordLengths(stackCtx.recSrc.PhysicalAddress, ref recordValue, ref srcRecordInfo);
                        rmwInfo.FullValueLength = recordLengths.fullValueLength;

                        // RMW uses GetInitialRecordSize because it has only the initial Input, not a Value
                        var (requiredSize, _, _) = hlog.GetRMWInitialRecordSize(ref key, ref input, sessionFunctions);
                        (ok, rmwInfo.UsedValueLength) = TryReinitializeTombstonedValue<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions,
                                ref srcRecordInfo, ref key, ref recordValue, requiredSize, recordLengths);
                    }

                    if (ok && sessionFunctions.InitialUpdater(ref key, ref input, ref recordValue, ref output, ref rmwInfo, ref srcRecordInfo))
                    {
                        // Success
                        MarkPage(stackCtx.recSrc.LogicalAddress, sessionFunctions.Ctx);
                        pendingContext.recordInfo = srcRecordInfo;
                        pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                        return true;
                    }

                    // Did not revivify; restore the tombstone and leave the deleted record there.
                    srcRecordInfo.Tombstone = true;
                }
            }
            finally
            {
                if (ok)
                    SetExtraValueLength(ref recordValue, ref srcRecordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                else
                    SetTombstoneAndExtraValueLength(ref recordValue, ref srcRecordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);    // Restore tombstone and ensure default value on inability to update in place
            }

        NeedNewRecord:
            // Successful non-revivification; move to CreateNewRecord.
            status = OperationStatus.SUCCESS;
            return false;
        }

        private LatchDestination CheckCPRConsistencyRMW(Phase phase, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, ref LatchOperation latchOperation)
        {
            // The idea of CPR is that if a thread in version V tries to perform an operation and notices a record in V+1, it needs to back off and run CPR_SHIFT_DETECTED.
            // Similarly, a V+1 thread cannot update a V record; it needs to do a read-copy-update (or upsert at tail) instead of an in-place update.
            // For background info: Prior to HashBucket-based locking, we had to lock the bucket in the following way:
            //  1. V threads take shared lock on bucket
            //  2. V+1 threads take exclusive lock on bucket, refreshing until they can
            //  3. If V thread cannot take shared lock, that means the system is in V+1 so we can immediately refresh and go to V+1 (do CPR_SHIFT_DETECTED)
            //  4. If V thread manages to get shared lock, but encounters a V+1 record, it knows the system is in V+1 so it will do CPR_SHIFT_DETECTED
            // Now we no longer need to do the bucket latching, since we already have a latch on the bucket.

            switch (phase)
            {
                case Phase.PREPARE: // Thread is in V
                    if (!IsEntryVersionNew(ref stackCtx.hei.entry))
                        break; // Normal Processing; thread is in V, record is in V

                    status = OperationStatus.CPR_SHIFT_DETECTED;
                    return LatchDestination.Retry;  // Pivot Thread for retry (do not operate on v+1 record when thread is in V)

                case Phase.IN_PROGRESS: // Thread is in v+1
                case Phase.WAIT_INDEX_CHECKPOINT:
                case Phase.WAIT_FLUSH:
                    if (IsRecordVersionNew(stackCtx.recSrc.LogicalAddress))
                        break;      // Normal Processing; V+1 thread encountered a record in V+1

                    if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                        return LatchDestination.CreateNewRecord;    // Record is in memory so force creation of a (V+1) record
                    break;  // Normal Processing; the record is below HeadAddress so the operation will go pending

                default:
                    break;
            }
            return LatchDestination.NormalProcessing;
        }

        /// <summary>
        /// Create a new record for RMW
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="TSessionFunctionsWrapper"></typeparam>
        /// <param name="key">The record Key</param>
        /// <param name="input">Input to the operation</param>
        /// <param name="value">Old value</param>
        /// <param name="output">The result of ISessionFunctions.SingleWriter</param>
        /// <param name="pendingContext">Information about the operation context</param>
        /// <param name="sessionFunctions">The current session</param>
        /// <param name="stackCtx">Contains the <see cref="HashEntryInfo"/> and <see cref="RecordSource{Key, Value}"/> structures for this operation,
        ///     and allows passing back the newLogicalAddress for invalidation in the case of exceptions. If called from pending IO,
        ///     this is populated from the data read from disk.</param>
        /// <param name="srcRecordInfo">If <paramref name="stackCtx"/>.<see cref="RecordSource{Key, Value}.HasInMemorySrc"/>,
        ///     this is the <see cref="RecordInfo"/> for <see cref="RecordSource{Key, Value}.LogicalAddress"/>. Otherwise, if called from pending IO,
        ///     this is the <see cref="RecordInfo"/> read from disk. If neither of these, it is a default <see cref="RecordInfo"/>.</param>
        /// <param name="doingCU">Whether we are doing a CopyUpdate, either from in-memory or pending IO</param>
        /// <returns></returns>
        private OperationStatus CreateNewRecordRMW<Input, Output, Context, TSessionFunctionsWrapper>(ref Key key, ref Input input, ref Value value, ref Output output,
                                                                                          ref PendingContext<Input, Output, Context> pendingContext, TSessionFunctionsWrapper sessionFunctions,
                                                                                          ref OperationStackContext<Key, Value> stackCtx, ref RecordInfo srcRecordInfo, bool doingCU)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            bool forExpiration = false;

        RetryNow:

            RMWInfo rmwInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                SessionID = sessionFunctions.Ctx.sessionID,
                Address = doingCU && !stackCtx.recSrc.HasReadCacheSrc ? stackCtx.recSrc.LogicalAddress : Constants.kInvalidAddress,
                KeyHash = stackCtx.hei.hash,
                IsFromPending = pendingContext.type != OperationType.NONE,
            };

            // Perform Need*
            if (doingCU)
            {
                rmwInfo.SetRecordInfo(ref srcRecordInfo);
                if (!sessionFunctions.NeedCopyUpdate(ref key, ref input, ref value, ref output, ref rmwInfo))
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                        return OperationStatus.CANCELED;
                    else if (rmwInfo.Action == RMWAction.ExpireAndResume)
                    {
                        doingCU = false;
                        forExpiration = true;
                    }
                    else if (rmwInfo.Action == RMWAction.ExpireAndStop)
                        return OperationStatus.NOTFOUND;
                    else
                        return OperationStatus.SUCCESS;
                }
            }

            if (!doingCU)
            {
                rmwInfo.ClearRecordInfo();   // There is no existing record
                if (!sessionFunctions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo))
                    return rmwInfo.Action == RMWAction.CancelOperation ? OperationStatus.CANCELED : OperationStatus.NOTFOUND;
            }

            // Allocate and initialize the new record
            var (actualSize, allocatedSize, keySize) = doingCU ?
                stackCtx.recSrc.Log.GetRMWCopyDestinationRecordSize(ref key, ref input, ref value, ref srcRecordInfo, sessionFunctions) :
                hlog.GetRMWInitialRecordSize(ref key, ref input, sessionFunctions);

            AllocateOptions allocOptions = new()
            {
                Recycle = true,

                // If the source record is elidable we can try to elide from the chain and transfer it to the FreeList if we're doing Revivification
                IgnoreHeiAddress = stackCtx.recSrc.HasMainLogSrc && CanElide<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcRecordInfo)
            };

            if (!TryAllocateRecord(sessionFunctions, ref pendingContext, ref stackCtx, actualSize, ref allocatedSize, keySize, allocOptions,
                    out long newLogicalAddress, out long newPhysicalAddress, out OperationStatus status))
                return status;

            ref RecordInfo newRecordInfo = ref WriteNewRecordInfo(ref key, hlog, newPhysicalAddress, inNewVersion: sessionFunctions.Ctx.InNewVersion, tombstone: false, stackCtx.recSrc.LatestLogicalAddress);
            if (allocOptions.IgnoreHeiAddress)
                newRecordInfo.PreviousAddress = srcRecordInfo.PreviousAddress;
            stackCtx.SetNewRecord(newLogicalAddress);

            rmwInfo.Address = newLogicalAddress;
            rmwInfo.SetRecordInfo(ref newRecordInfo);

            // Populate the new record
            ref Value newRecordValue = ref hlog.GetAndInitializeValue(newPhysicalAddress, newPhysicalAddress + actualSize);
            (rmwInfo.UsedValueLength, rmwInfo.FullValueLength) = GetNewValueLengths(actualSize, allocatedSize, newPhysicalAddress, ref newRecordValue);

            if (!doingCU)
            {
                if (sessionFunctions.InitialUpdater(ref key, ref input, ref newRecordValue, ref output, ref rmwInfo, ref newRecordInfo))
                {
                    SetExtraValueLength(ref newRecordValue, ref newRecordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                    status = forExpiration
                        ? OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord | StatusCode.Expired)
                        : OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.CreatedRecord);
                }
                else
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                        return OperationStatus.CANCELED;
                    return OperationStatus.NOTFOUND | (forExpiration ? OperationStatus.EXPIRED : OperationStatus.NOTFOUND);
                }
            }
            else
            {
                if (sessionFunctions.CopyUpdater(ref key, ref input, ref value, ref newRecordValue, ref output, ref rmwInfo, ref newRecordInfo))
                {
                    SetExtraValueLength(ref newRecordValue, ref newRecordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CopyUpdatedRecord);

                    // Do not elide (restore newRecordInfo.PreviousAddress to its original WriteNewRecordInfo state) if requested to preserve the source record.
                    if (rmwInfo.PreserveCopyUpdaterSourceRecord)
                    {
                        allocOptions.IgnoreHeiAddress = false;
                        newRecordInfo.PreviousAddress = stackCtx.recSrc.LatestLogicalAddress;
                    }
                    goto DoCAS;
                }
                if (rmwInfo.Action == RMWAction.CancelOperation)
                {
                    // Save allocation for revivification (not retry, because this is canceling of the current operation), or abandon it if that fails.
                    if (RevivificationManager.UseFreeRecordPool && RevivificationManager.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize, ref sessionFunctions.Ctx.RevivificationStats))
                        stackCtx.ClearNewRecord();
                    else
                        stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                    return OperationStatus.CANCELED;
                }
                if (rmwInfo.Action == RMWAction.ExpireAndStop)
                {
                    newRecordInfo.Tombstone = true;
                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CreatedRecord | StatusCode.Expired);
                    goto DoCAS;
                }
                else if (rmwInfo.Action == RMWAction.ExpireAndResume)
                {
                    doingCU = false;
                    forExpiration = true;

                    if (!ReinitializeExpiredRecord<Input, Output, Context, TSessionFunctionsWrapper>(ref key, ref input, ref newRecordValue, ref output, ref newRecordInfo,
                                            ref rmwInfo, newLogicalAddress, sessionFunctions, isIpu: false, out status))
                    {
                        // An IPU was not (or could not) be done. Cancel if requested, else invalidate the allocated record and retry.
                        if (status == OperationStatus.CANCELED)
                            return status;

                        // Save allocation for revivification (not retry, because this may have been false because the record was too small), or abandon it if that fails.
                        if (RevivificationManager.UseFreeRecordPool && RevivificationManager.TryAdd(newLogicalAddress, newPhysicalAddress, allocatedSize, ref sessionFunctions.Ctx.RevivificationStats))
                            stackCtx.ClearNewRecord();
                        else
                            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
                        goto RetryNow;
                    }
                    goto DoCAS;
                }
                else
                    return OperationStatus.SUCCESS | (forExpiration ? OperationStatus.EXPIRED : OperationStatus.SUCCESS);
            }

        DoCAS:
            // Insert the new record by CAS'ing either directly into the hash entry or splicing into the readcache/mainlog boundary.
            bool success = CASRecordIntoChain(ref key, ref stackCtx, newLogicalAddress, ref newRecordInfo);
            if (success)
            {
                PostCopyToTail(ref key, ref stackCtx, ref srcRecordInfo);

                // If IU, status will be NOTFOUND; return that.
                if (!doingCU)
                {
                    // If IU, status will be NOTFOUND. ReinitializeExpiredRecord has many paths but is straightforward so no need to assert here.
                    Debug.Assert(forExpiration || OperationStatus.NOTFOUND == OperationStatusUtils.BasicOpCode(status), $"Expected NOTFOUND but was {status}");
                    sessionFunctions.PostInitialUpdater(ref key, ref input, ref hlog.GetValue(newPhysicalAddress), ref output, ref rmwInfo, ref newRecordInfo);
                }
                else
                {
                    // Else it was a CopyUpdater so call PCU
                    if (!sessionFunctions.PostCopyUpdater(ref key, ref input, ref value, ref hlog.GetValue(newPhysicalAddress), ref output, ref rmwInfo, ref newRecordInfo))
                    {
                        if (rmwInfo.Action == RMWAction.ExpireAndStop)
                        {
                            newRecordInfo.Tombstone = true;
                            status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.CopyUpdatedRecord | StatusCode.Expired);
                        }
                        else
                        {
                            Debug.Fail("Can only handle RMWAction.ExpireAndStop on a false return from PostCopyUpdater");
                        }
                    }

                    // IgnoreHeiAddress means we have verified that the old source record is elidable and now that CAS has replaced it in the HashBucketEntry with
                    // the new source record that does not point to the old source record, we have elided it, so try to transfer to freelist.
                    if (allocOptions.IgnoreHeiAddress)
                    {
                        // Success should always Seal the old record. This may be readcache, readonly, or the temporary recordInfo, which is OK and saves the cost of an "if".
                        srcRecordInfo.SealAndInvalidate();    // The record was elided, so Invalidate

                        if (stackCtx.recSrc.LogicalAddress >= GetMinRevivifiableAddress())
                        {
                            // We need to re-get the old record's length because rmwInfo has the new record's info. If freelist-add fails, it remains Sealed/Invalidated.
                            var oldRecordLengths = GetRecordLengths(stackCtx.recSrc.PhysicalAddress, ref hlog.GetValue(stackCtx.recSrc.PhysicalAddress), ref srcRecordInfo);
                            TryTransferToFreeList<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcRecordInfo, oldRecordLengths);
                        }
                    }
                    else
                        srcRecordInfo.Seal();              // The record was not elided, so do not Invalidate
                }

                stackCtx.ClearNewRecord();
                pendingContext.recordInfo = newRecordInfo;
                pendingContext.logicalAddress = newLogicalAddress;
                return status;
            }

            // CAS failed
            stackCtx.SetNewRecordInvalid(ref newRecordInfo);
            ref Value insertedValue = ref hlog.GetValue(newPhysicalAddress);
            ref Key insertedKey = ref hlog.GetKey(newPhysicalAddress);
            if (!doingCU)
                sessionFunctions.DisposeInitialUpdater(ref insertedKey, ref input, ref insertedValue, ref output, ref rmwInfo);
            else
                sessionFunctions.DisposeCopyUpdater(ref insertedKey, ref input, ref value, ref insertedValue, ref output, ref rmwInfo);

            SetExtraValueLength(ref newRecordValue, ref newRecordInfo, rmwInfo.UsedValueLength, rmwInfo.FullValueLength);
            SaveAllocationForRetry(ref pendingContext, newLogicalAddress, newPhysicalAddress, allocatedSize);
            return OperationStatus.RETRY_NOW;   // CAS failure does not require epoch refresh
        }

        internal bool ReinitializeExpiredRecord<Input, Output, Context, TSessionFunctionsWrapper>(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo,
                                                                                       long logicalAddress, TSessionFunctionsWrapper sessionFunctions, bool isIpu, out OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            // This is called for InPlaceUpdater or CopyUpdater only; CopyUpdater however does not copy an expired record, so we return CreatedRecord.
            var advancedStatusCode = isIpu ? StatusCode.InPlaceUpdatedRecord : StatusCode.CreatedRecord;
            advancedStatusCode |= StatusCode.Expired;
            if (!sessionFunctions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo))
            {
                if (rmwInfo.Action == RMWAction.CancelOperation)
                {
                    status = OperationStatus.CANCELED;
                    return false;
                }

                // Expiration with no insertion.
                recordInfo.Tombstone = true;
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                return true;
            }

            // Try to reinitialize in place
            (var currentSize, _, _) = hlog.GetRecordSize(ref key, ref value);
            (var requiredSize, _, _) = hlog.GetRMWInitialRecordSize(ref key, ref input, sessionFunctions);

            if (currentSize >= requiredSize)
            {
                if (sessionFunctions.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo))
                {
                    // If IPU path, we need to complete PostInitialUpdater as well
                    if (isIpu)
                        sessionFunctions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo, ref recordInfo);

                    status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                    return true;
                }
                else
                {
                    if (rmwInfo.Action == RMWAction.CancelOperation)
                    {
                        status = OperationStatus.CANCELED;
                        return false;
                    }
                    else
                    {
                        // Expiration with no insertion.
                        recordInfo.Tombstone = true;
                        status = OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, advancedStatusCode);
                        return true;
                    }
                }
            }

            // Reinitialization in place was not possible. InternalRMW will do the following based on who called this:
            //  IPU: move to the NIU->allocate->IU path
            //  CU: caller invalidates allocation, retries operation as NIU->allocate->IU
            status = OperationStatus.SUCCESS;
            return false;
        }
    }
}