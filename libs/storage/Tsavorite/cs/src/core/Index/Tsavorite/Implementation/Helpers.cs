// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        private enum LatchDestination
        {
            CreateNewRecord,
            NormalProcessing,
            Retry
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static ref RecordInfo WriteNewRecordInfo(ref TKey key, AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> log, long newPhysicalAddress, bool inNewVersion, long previousAddress)
        {
            ref RecordInfo recordInfo = ref log._wrapper.GetInfo(newPhysicalAddress);
            recordInfo.WriteInfo(inNewVersion, previousAddress);
            log._wrapper.SerializeKey(ref key, newPhysicalAddress);
            return ref recordInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MarkPage<TInput, TOutput, TContext>(long logicalAddress, TsavoriteExecutionContext<TInput, TOutput, TContext> sessionCtx)
        {
            if (sessionCtx.phase == Phase.REST)
                hlog.MarkPage(logicalAddress, sessionCtx.version);
            else
                hlog.MarkPageAtomic(logicalAddress, sessionCtx.version);
        }

        /// <summary>
        /// This is a wrapper for checking the record's version instead of just peeking at the latest record at the tail of the bucket.
        /// By calling with the address of the traced record, we can prevent a different key sharing the same bucket from deceiving 
        /// the operation to think that the version of the key has reached v+1 and thus to incorrectly update in place.
        /// </summary>
        /// <param name="logicalAddress">The logical address of the traced record for the key</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsRecordVersionNew(long logicalAddress)
        {
            HashBucketEntry entry = new() { word = logicalAddress };
            return IsEntryVersionNew(ref entry);
        }

        /// <summary>
        /// Check the version of the passed-in entry. 
        /// The semantics of this function are to check the tail of a bucket (indicated by entry), so we name it this way.
        /// </summary>
        /// <param name="entry">the last entry of a bucket</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsEntryVersionNew(ref HashBucketEntry entry)
        {
            // A version shift can only happen in an address after the checkpoint starts, as v_new threads RCU entries to the tail.
            if (entry.Address < _hybridLogCheckpoint.info.startLogicalAddress)
                return false;

            // Read cache entries are not in new version
            if (UseReadCache && entry.ReadCache)
                return false;

            // If the record is in memory, check if it has the new version bit set
            if (entry.Address < hlogBase.HeadAddress)
                return false;
            return hlog.GetInfo(hlog.GetPhysicalAddress(entry.Address)).IsInNewVersion;
        }

        // Can only elide the record if it is the tail of the tag chain (i.e. is the record in the hash bucket entry) and its
        // PreviousAddress does not point to a valid record. Otherwise an earlier record for this key could be reachable again.
        // Also, it cannot be elided if it is frozen due to checkpointing.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref RecordInfo srcRecordInfo)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(!stackCtx.recSrc.HasReadCacheSrc, "Should not call CanElide() for readcache records");
            return stackCtx.hei.Address == stackCtx.recSrc.LogicalAddress && srcRecordInfo.PreviousAddress < hlogBase.BeginAddress
                        && !IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcRecordInfo);
        }

        // If the record is in a checkpoint range, it must not be modified. If it is in the fuzzy region, it can only be modified
        // if it is a new record.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref RecordInfo srcRecordInfo)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(!stackCtx.recSrc.HasReadCacheSrc, "Should not call IsFrozen() for readcache records");
            return sessionFunctions.ExecutionCtx.IsInV1
                        && (stackCtx.recSrc.LogicalAddress <= _hybridLogCheckpoint.info.startLogicalAddress     // In checkpoint range
                            || !srcRecordInfo.IsInNewVersion);                                                  // In fuzzy region and an old version
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (bool elided, bool added) TryElideAndTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref RecordInfo srcRecordInfo, (int usedValueLength, int fullValueLength, int fullRecordLength) recordLengths)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Try to CAS out of the hashtable and if successful, add it to the free list.
            Debug.Assert(srcRecordInfo.IsSealed, "Expected a Sealed record in TryElideAndTransferToFreeList");

            if (!stackCtx.hei.TryElide())
                return (false, false);

            return (true, TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref srcRecordInfo, recordLengths));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx,
                ref RecordInfo srcRecordInfo, (int usedValueLength, int fullValueLength, int fullRecordLength) recordLengths)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // The record has been CAS'd out of the hashtable or elided from the chain, so add it to the free list.
            Debug.Assert(srcRecordInfo.IsSealed, "Expected a Sealed record in TryTransferToFreeList");

            // Dispose any existing key and value. We do this as soon as we have elided so objects are released for GC as early as possible.
            // We don't want the caller to know details of the Filler, so we cleared out any extraValueLength entry to ensure the space beyond
            // usedValueLength is zero'd for log-scan correctness.
            ref TValue recordValue = ref stackCtx.recSrc.GetValue();
            ClearExtraValueSpace(ref srcRecordInfo, ref recordValue, recordLengths.usedValueLength, recordLengths.fullValueLength);
            storeFunctions.DisposeRecord(ref stackCtx.recSrc.GetKey(), ref recordValue, DisposeReason.RevivificationFreeList);

            // Now that we've Disposed the record, see if its address is revivifiable. If not, just leave it orphaned and invalid.
            if (stackCtx.recSrc.LogicalAddress < GetMinRevivifiableAddress())
                return false;

            SetFreeRecordSize(stackCtx.recSrc.PhysicalAddress, ref srcRecordInfo, recordLengths.fullRecordLength);
            return RevivificationManager.TryAdd(stackCtx.recSrc.LogicalAddress, recordLengths.fullRecordLength, ref sessionFunctions.ExecutionCtx.RevivificationStats);
        }

        internal enum LatchOperation : byte
        {
            None,
            Shared,
            Exclusive
        }

        internal void SetRecordInvalid(long logicalAddress)
        {
            // This is called on exception recovery for a newly-inserted record.
            var localLog = IsReadCache(logicalAddress) ? readcache : hlog;
            ref var recordInfo = ref localLog.GetInfo(localLog.GetPhysicalAddress(AbsoluteAddress(logicalAddress)));
            recordInfo.SetInvalid();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CASRecordIntoChain(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, long newLogicalAddress, ref RecordInfo newRecordInfo)
        {
            var result = stackCtx.recSrc.LowestReadCachePhysicalAddress == Constants.kInvalidAddress
                ? stackCtx.hei.TryCAS(newLogicalAddress, partitionId)
                : SpliceIntoHashChainAtReadCacheBoundary(ref key, ref stackCtx, newLogicalAddress);
            if (result)
                newRecordInfo.UnsealAndValidate();
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PostCopyToTail(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref RecordInfo srcRecordInfo)
            => PostCopyToTail(ref key, ref stackCtx, ref srcRecordInfo, stackCtx.hei.Address);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PostCopyToTail(ref TKey key, ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, ref RecordInfo srcRecordInfo, long highestReadCacheAddressChecked)
        {
            // Nothing required here if not using ReadCache
            if (!UseReadCache)
                return;

            if (stackCtx.recSrc.HasReadCacheSrc)
            {
                // If we already have a readcache source, there will not be another inserted, so we can just invalidate the source directly.
                srcRecordInfo.SetInvalidAtomic();
            }
            else
            {
                // We did not have a readcache source, so while we spliced a new record into the readcache/mainlog gap a competing readcache record may have been inserted at the tail.
                // If so, invalidate it. highestReadCacheAddressChecked is hei.Address unless we are from ConditionalCopyToTail, which may have skipped the readcache before this.
                // See "Consistency Notes" in TryCopyToReadCache for a discussion of why there ie no "momentary inconsistency" possible here.
                ReadCacheCheckTailAfterSplice(ref key, ref stackCtx.hei, highestReadCacheAddressChecked);
            }
        }

        // Called after BlockAllocate or anything else that could shift HeadAddress, to adjust addresses or return false for RETRY as needed.
        // This refreshes the HashEntryInfo, so the caller needs to recheck to confirm the BlockAllocated address is still > hei.Address.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool VerifyInMemoryAddresses(ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            // If we have an in-memory source that fell below HeadAddress, return false and the caller will RETRY_LATER.
            if (stackCtx.recSrc.HasInMemorySrc && stackCtx.recSrc.LogicalAddress < stackCtx.recSrc.AllocatorBase.HeadAddress)
                return false;

            // If we're not using readcache or we don't have a splice point or it is still above readcache.HeadAddress, we're good.
            if (!UseReadCache || stackCtx.recSrc.LowestReadCacheLogicalAddress == Constants.kInvalidAddress || stackCtx.recSrc.LowestReadCacheLogicalAddress >= readcacheBase.HeadAddress)
                return true;

            // If the splice point went below readcache.HeadAddress, we would have to wait for the chain to be fixed up by eviction,
            // so just return RETRY_LATER and restart the operation.
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindOrCreateTagAndTryTransientXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Transient must lock the bucket before traceback, to prevent revivification from yanking the record out from underneath us. Manual locking already automatically locks the bucket.
            Kernel.hashTable.FindOrCreateTag(ref stackCtx.hei, hlogBase.BeginAddress);
            if (!TryTransientXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                return false;

            // Between the time we found the tag and the time we locked the bucket the record in hei.entry may have been elided, so make sure we don't have a stale address in hei.entry.
            stackCtx.hei.SetToCurrent();
            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagAndTryTransientXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Transient must lock the bucket before traceback, to prevent revivification from yanking the record out from underneath us. Manual locking already automatically locks the bucket.
            internalStatus = OperationStatus.NOTFOUND;
            if (!Kernel.hashTable.FindTag(ref stackCtx.hei) || !TryTransientXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                return false;

            // Between the time we found the tag and the time we locked the bucket the record in hei.entry may have been elided, so make sure we don't have a stale address in hei.entry.
            stackCtx.hei.SetToCurrent();
            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagAndTryTransientSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref TKey key,
                ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Transient must lock the bucket before traceback, to prevent revivification from yanking the record out from underneath us. Manual locking already automatically locks the bucket.
            internalStatus = OperationStatus.NOTFOUND;
            if (!Kernel.hashTable.FindTag(ref stackCtx.hei) || !TryTransientSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                return false;

            // Between the time we found the tag and the time we locked the bucket the record in hei.entry may have been elided, so make sure we don't have a stale address in hei.entry.
            stackCtx.hei.SetToCurrent();
            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            return true;
        }

        private void SplitBuckets(long hash)
        {
            if (Kernel.hashTable.SplitBuckets(hash, in storeFunctions, hlog, hlogBase, readcache, readcacheBase))
                GlobalStateMachineStep(systemState);
        }
    }
}