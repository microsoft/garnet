// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMemory(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress, bool stopAtHeadAddress = true)
        {
            if (UseReadCache && FindInReadCache(ref key, ref stackCtx, minAddress: Constants.kInvalidAddress))
                return true;
            if (minAddress < hlog.HeadAddress && stopAtHeadAddress)
                minAddress = hlog.HeadAddress;
            return TryFindRecordInMainLog(ref key, ref stackCtx, minAddress: minAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMemory<Input, Output, Context>(ref Key key, ref OperationStackContext<Key, Value> stackCtx,
                                                                   ref PendingContext<Input, Output, Context> pendingContext)
        {
            // Add 1 to the pendingContext minAddresses because we don't want an inclusive search; we're looking to see if it was added *after*.
            if (UseReadCache)
            {
                var minRC = IsReadCache(pendingContext.InitialEntryAddress) ? pendingContext.InitialEntryAddress + 1 : Constants.kInvalidAddress;
                if (FindInReadCache(ref key, ref stackCtx, minAddress: minRC))
                    return true;
            }
            var minLog = pendingContext.InitialLatestLogicalAddress < hlog.HeadAddress ? hlog.HeadAddress : pendingContext.InitialLatestLogicalAddress + 1;
            return TryFindRecordInMainLog(ref key, ref stackCtx, minAddress: minLog);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryFindRecordInMainLog(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress)
        {
            Debug.Assert(!stackCtx.recSrc.HasInMemorySrc, "Should not have found record before this call");
            if (stackCtx.recSrc.LogicalAddress >= minAddress)
            {
                stackCtx.recSrc.SetPhysicalAddress();
                TraceBackForKeyMatch(ref key, ref stackCtx.recSrc, minAddress);
            }
            return stackCtx.recSrc.HasInMemorySrc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryFindRecordInMainLogForConditionalOperation<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession,
                ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress, out OperationStatus internalStatus, out bool needIO)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            internalStatus = OperationStatus.SUCCESS;
            if (RevivificationManager.UseFreeRecordPool)
            {
                // The TransientSLock here is necessary only for the tag chain to avoid record elision/revivification during traceback.
                if (!FindTagAndTryTransientSLock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx, out internalStatus))
                    return needIO = false;
            }
            else
            {
                if (!FindTag(ref stackCtx.hei))
                {
                    internalStatus = OperationStatus.NOTFOUND;
                    return needIO = false;
                }
                stackCtx.SetRecordSourceToHashEntry(hlog);
            }

            try
            {
                // minAddress is inclusive
                if (!stackCtx.hei.IsReadCache)
                {
                    if (stackCtx.hei.Address < minAddress)
                        return needIO = false;
                    if (stackCtx.hei.Address < hlog.HeadAddress)
                    {
                        needIO = stackCtx.hei.Address >= hlog.BeginAddress;
                        return false;
                    }
                }

                if (UseReadCache)
                    SkipReadCache(ref stackCtx, out _); // Where this is called, we have no dependency on source addresses so we don't care if it Refreshed

                // We don't have a pendingContext here, so pass the minAddress directly.
                needIO = false;
                if (TryFindRecordInMainLogForPendingOperation(ref key, ref stackCtx, minAddress < hlog.HeadAddress ? hlog.HeadAddress : minAddress, out internalStatus))
                {
                    stackCtx.recSrc.UnlockShared(ref stackCtx.recSrc.GetInfo(), hlog.HeadAddress);
                    return true;
                }

                needIO = stackCtx.recSrc.LogicalAddress >= minAddress && stackCtx.recSrc.LogicalAddress < hlog.HeadAddress && stackCtx.recSrc.LogicalAddress >= hlog.BeginAddress;
                return false;
            }
            finally
            {
                TransientSUnlock<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx);
                // No RecordIsolation unlock here; we do not lock it in TryFindRecordForPendingOperation
            }
        }

        // We want to return non-Invalid records or Invalid records that are Sealed, because Sealed is part of our "need RETRY" handling.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsValidTracebackRecord(RecordInfo recordInfo) => !recordInfo.Invalid || recordInfo.IsSealed;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, ref RecordSource<Key, Value> recSrc, long minAddress)
        {
            // PhysicalAddress must already be populated by callers.
            ref var recordInfo = ref recSrc.GetInfo();
            if (IsValidTracebackRecord(recordInfo) && comparer.Equals(ref key, ref recSrc.GetKey()))
            {
                recSrc.SetHasMainLogSrc();
                return true;
            }

            recSrc.LogicalAddress = recordInfo.PreviousAddress;
            if (TraceBackForKeyMatch(ref key, recSrc.LogicalAddress, minAddress, out recSrc.LogicalAddress, out recSrc.PhysicalAddress))
            {
                recSrc.SetHasMainLogSrc();
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(ref Key key, long fromLogicalAddress, long minAddress, out long foundLogicalAddress, out long foundPhysicalAddress)
        {
            // This overload is called when the record at the "current" logical address does not match 'key'; fromLogicalAddress is its .PreviousAddress.
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minAddress)
            {
                foundPhysicalAddress = hlog.GetPhysicalAddress(foundLogicalAddress);

                ref var recordInfo = ref hlog.GetInfo(foundPhysicalAddress);
                if (IsValidTracebackRecord(recordInfo) && comparer.Equals(ref key, ref hlog.GetKey(foundPhysicalAddress)))
                    return true;

                foundLogicalAddress = recordInfo.PreviousAddress;
            }
            foundPhysicalAddress = Constants.kInvalidAddress;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordForUpdate(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress, out OperationStatus internalStatus, bool wantLock)
        {
            // This routine returns true if we should proceed with the InternalXxx operation, else false. If it returns true, caller must set srcRecordInfo.
            internalStatus = OperationStatus.SUCCESS;

            // This callee returns true if we completely process the traceback or have determined that we need a RETRY.
            if (TracebackNeedsRevivCheck(ref stackCtx))
            {
                if (TryMatchFirstRecordWithRecordIsolationAndReviv(ref key, ref stackCtx, isForUpdate: true, out internalStatus))
                {
                    if (!wantLock)
                        stackCtx.recSrc.UnlockExclusive(ref stackCtx.recSrc.GetInfo(), hlog.HeadAddress);
                    return internalStatus == OperationStatus.SUCCESS;
                }
            }

            // Did not completely process the traceback (e.g. had an inelidable first record, are doing bucket locking, etc.) Do normal traceback here.
            if (TryFindRecordInMemory(ref key, ref stackCtx, minAddress))
            {
                ref RecordInfo recordInfo = ref stackCtx.recSrc.GetInfo();
                if (recordInfo.IsClosed
                    || (wantLock && stackCtx.recSrc.HasMainLogSrc && !stackCtx.recSrc.TryLockExclusive(ref recordInfo)))
                {
                    internalStatus = OperationStatus.RETRY_LATER;
                    return false;
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordForRead(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress, out OperationStatus internalStatus)
        {
            // This routine returns true if we should proceed with the InternalXxx operation, else false. If it returns true, caller must set srcRecordInfo.
            internalStatus = OperationStatus.SUCCESS;

            // This callee returns true if we completely process the traceback or have determined that we need a RETRY.
            if (TracebackNeedsRevivCheck(ref stackCtx))
            {
                if (TryMatchFirstRecordWithRecordIsolationAndReviv(ref key, ref stackCtx, isForUpdate: false, out internalStatus))
                    return internalStatus == OperationStatus.SUCCESS;
            }

            // Did not completely process the traceback (e.g. had an inelidable first record, are doing bucket locking, etc.) Do normal traceback here.
            // We are here for read so we have already processed readcache, and are just here for the traceback in the main log.
            if (TryFindRecordInMainLog(ref key, ref stackCtx, minAddress))
            {
                ref RecordInfo recordInfo = ref stackCtx.recSrc.GetInfo();
                if (recordInfo.IsClosed
                    || (DoRecordIsolation && stackCtx.recSrc.HasMainLogSrc && !stackCtx.recSrc.TryLockShared(ref recordInfo)))
                {
                    internalStatus = OperationStatus.RETRY_LATER;
                    return false;
                }
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordForPendingOperation<Input, Output, Context>(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress, out OperationStatus internalStatus,
                                                      ref PendingContext<Input, Output, Context> pendingContext)
        {
            // This routine returns true if we find the key, else false.
            internalStatus = OperationStatus.SUCCESS;

            // This callee returns true if we completely process the traceback or have determined that we need a RETRY.
            if (TracebackNeedsRevivCheck(ref stackCtx))
            {
                if (TryMatchFirstRecordWithRecordIsolationAndReviv(ref key, ref stackCtx, isForUpdate: false, out internalStatus))
                {
                    // We don't want to preserve this lock; pending operations are just here to see if a later record has been added to the log.
                    stackCtx.recSrc.UnlockShared(ref stackCtx.recSrc.GetInfo(), hlog.HeadAddress);
                    return stackCtx.recSrc.HasMainLogSrc && internalStatus == OperationStatus.SUCCESS;
                }
            }

            // Did not completely process the traceback (e.g. had an inelidable first record, are doing bucket locking, etc.) Do normal traceback here.
            if (!TryFindRecordInMemory(ref key, ref stackCtx, ref pendingContext))
                return false;
            if (stackCtx.recSrc.GetInfo().IsClosed)
                internalStatus = OperationStatus.RETRY_LATER;
            // We do not lock here; this is just to see if the key is found
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryFindRecordInMainLogForPendingOperation(ref Key key, ref OperationStackContext<Key, Value> stackCtx, long minAddress, out OperationStatus internalStatus)
        {
            // This overload is called when we do not have a PendingContext to get minAddress from, and we've skipped the readcache if present.

            // This routine returns true if we find the key, else false.
            internalStatus = OperationStatus.SUCCESS;

            // This callee returns true if we completely process the traceback or have determined that we need a RETRY.
            if (TracebackNeedsRevivCheck(ref stackCtx))
            {
                if (TryMatchFirstRecordWithRecordIsolationAndReviv(ref key, ref stackCtx, isForUpdate: false, out internalStatus))
                {
                    // We don't want to preserve this lock; pending operations are just here to see if a later record has been added to the log.
                    stackCtx.recSrc.UnlockShared(ref stackCtx.recSrc.GetInfo(), hlog.HeadAddress);
                    return stackCtx.recSrc.HasMainLogSrc && internalStatus == OperationStatus.SUCCESS;
                }
            }

            // Did not completely process the traceback (e.g. had an inelidable first record, are doing bucket locking, etc.) Do normal traceback here.
            if (!TryFindRecordInMainLog(ref key, ref stackCtx, minAddress))
                return false;
            if (stackCtx.recSrc.GetInfo().IsClosed)
                internalStatus = OperationStatus.RETRY_LATER;
            // We do not lock here; this is just to see if the key is found
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TracebackNeedsRevivCheck(ref OperationStackContext<Key, Value> stackCtx)
        {
            if (!DoRecordIsolation || !RevivificationManager.UseFreeRecordPool || stackCtx.hei.IsReadCache)
                return false;

            // We cannot rely on stackCtx.hei.Address < this.GetMinRevivifiableAddress() being safe; we must use its stored initial value, because it may
            // have risen since; we could be below where it is now, but above where it started so the record could have been elided.
            // Note that we do NOT check for whether .PreviousAddress is valid here; if the record was revivified before we got here, it may have become valid.
            Debug.Assert(stackCtx.recSrc.InitialMinRevivifiableAddress > 0, "InitialMinRevivifiableAddress is not set");
            return stackCtx.hei.Address >= stackCtx.recSrc.InitialMinRevivifiableAddress;   // see comments for this field for details of why it's necessary.
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryMatchFirstRecordWithRecordIsolationAndReviv(ref Key key, ref OperationStackContext<Key, Value> stackCtx, bool isForUpdate, out OperationStatus internalStatus)
        {
            // This routine is necessary for RecordIsolation chain traversal when revivification may elide the tail record of the chain when it is the only record
            // in the chain. Bucket-based locking locks the chain so matters are simpler there.

            // This returns true if we completely process the traceback:
            //   - the first record did not match and there was no valid .PreviousAddress
            //   - the first record did match:
            //      - if the lock failed, return RETRY_LATER
            //      - if the key does not match after the lock, return RETRY_NOW (the record was freelisted and revivified while we were examining it)
            //      - else everything is good; return success
            // If none of these apply, return false and the caller will do the usual Traceback (including RecordIsolation locking if appropriate).

            // Note that we cannot ReadCacheEvict the first record if it's a ReadCache record, because we're epoch-protected.
            Debug.Assert(!stackCtx.recSrc.HasInMemorySrc, "Should not already have HasInMemorySrc");

            // We have not yet set up recSrc; we're doing that here. We don't know yet if the key matches.
            stackCtx.recSrc.SetPhysicalAddress();
            ref var srcRecordInfo = ref stackCtx.recSrc.GetInfo();
            internalStatus = OperationStatus.SUCCESS;

            // Skip the record only if Invalid, not Sealed; Sealed records with the requested key are significant and must be processed.
            // Note: We do not lock for key comparison; the Key's ITsavoriteEqualityComparer implementation must be robust to the key being disposed while the comparison
            // happens. See for example ByteArrayTsavoriteEqualityComparer.
            ref var recordKey = ref stackCtx.recSrc.GetKey();
            if (!srcRecordInfo.Invalid && comparer.Equals(ref key, ref recordKey))
            {
                // Success--key matches. If doing RecordIsolation, lock it, then if necessary make sure it's stable. TryLock fails on a Closed record
                if (!stackCtx.recSrc.TryLock(ref srcRecordInfo, exclusive: isForUpdate))
                {
                    internalStatus = OperationStatus.RETRY_LATER;
                    return true;
                }

                // The record is not Closed, so it is not in the freelist; it is a valid record, either with the requested key or has been revivified with a different
                // key (and if so, likely into another tag chain). The vast majority of RecordIsolation cases, where there are no keyHash collisions and reviv did not
                // pull the record out from underneath us, end up here with HasMainLogSrc becoming true.
                Debug.Assert(!srcRecordInfo.IsClosed, "Should not have have a closed record after successful Lock");
                if (comparer.Equals(ref key, ref recordKey))
                    stackCtx.recSrc.SetHasMainLogSrc();
                else
                {
                    // Key mismatch; the record was revivified. RETRY_NOW because the chain is not awaiting any completing actions (such as a Sealed record replacement).
                    stackCtx.recSrc.ClearHasMainLogSrc();
                    stackCtx.recSrc.Unlock(ref srcRecordInfo, exclusive: isForUpdate);
                    internalStatus = OperationStatus.RETRY_NOW;
                }

                // We still have the record locked if the key matched, so the traversal is complete.
                return true;
            }

            // We have an invalid record or mismatched key. Prepare to move to the previous record for traceback. If that's an invalid address, traceback is complete. 
            stackCtx.recSrc.LogicalAddress = srcRecordInfo.PreviousAddress;
            stackCtx.recSrc.PhysicalAddress = 0;
            if (stackCtx.recSrc.LogicalAddress < hlog.BeginAddress)
                return true;

            // We have a valid previousAddress, so we know the chain will not change from this point. However, the first record may have been swapped out by revivification
            // and thus previousAddress may be in a different tag chain. If stackCtx.hei is still current, then we know we are still in the original tag chain, and can
            // return false to continue the traceback from previousAddress.
            if (stackCtx.hei.IsCurrent)
                return false;

            // If previousAddress is still in memory, we can verify whether its key hashes to the same bucket.
            if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
            {
                stackCtx.recSrc.SetPhysicalAddress();
                long keyHash = comparer.GetHashCode64(ref stackCtx.recSrc.GetKey());
                var bucketIndex = OverflowBucketLockTable<Key, Value>.GetBucketIndex(keyHash, this);
                if (bucketIndex != stackCtx.hei.bucketIndex)
                {
                    // Record has been revivified, but we're not waiting for another operation to complete in the initial tag chain, so RETRY_NOW is sufficient.
                    internalStatus = OperationStatus.RETRY_NOW;
                    return true;
                }
            }

            // Either previousAddress is below HeadAddress or we've verified we're still in the original tag chain. Return false to continue traceback from previousAddress.
            return false;
        }
    }
}