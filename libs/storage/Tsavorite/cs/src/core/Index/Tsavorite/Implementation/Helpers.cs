// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using static System.Runtime.InteropServices.JavaScript.JSType;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TValue>
        where TAllocator : IAllocator<TValue, TStoreFunctions>
    {
        private enum LatchDestination
        {
            CreateNewRecord,
            NormalProcessing,
            Retry
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static LogRecord<TValue> WriteNewRecordInfo(SpanByte key, AllocatorBase<TValue, TStoreFunctions, TAllocator> log, long logicalAddress, long physicalAddress, bool inNewVersion, long previousAddress)
        {
            ref var recordInfo = ref LogRecord<TValue>.GetInfoRef(physicalAddress);
            recordInfo.WriteInfo(inNewVersion, previousAddress);
            var logRecord = log._wrapper.CreateLogRecord(logicalAddress, physicalAddress);
            log._wrapper.SerializeKey(key, logicalAddress, ref logRecord);
            return logRecord;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void DisposeRecord(ref LogRecord<TValue> logRecord, DisposeReason disposeReason)
        {
            // Release any overflow allocations for Key and possibly Value spans.
            logRecord.FreeKeyOverflow();

            if (logRecord.ValueObjectId != ObjectIdMap.InvalidObjectId)
            {
                // Clear the IHeapObject, but leave the ObjectId in the record
                ref var heapObj = ref logRecord.ObjectRef;
                if (heapObj is not null)
                {
                    storeFunctions.DisposeValueObject(heapObj, disposeReason);
                    heapObj = default;
                }
            }
            else
                logRecord.FreeValueOverflow();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRecord(ref DiskLogRecord<TValue> logRecord, DisposeReason disposeReason)
        {
            // Clear the IHeapObject if we deserialized it
            if (logRecord.ValueObject is not null)
                storeFunctions.DisposeValueObject(logRecord.ValueObject, disposeReason);
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
            return LogRecord<TValue>.GetInfo(hlog.GetPhysicalAddress(entry.Address)).IsInNewVersion;
        }

        // Can only elide the record if it is the tail of the tag chain (i.e. is the record in the hash bucket entry) and its
        // PreviousAddress does not point to a valid record. Otherwise an earlier record for this key could be reachable again.
        // Also, it cannot be elided if it is frozen due to checkpointing.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CanElide<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, RecordInfo srcRecordInfo)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(!stackCtx.recSrc.HasReadCacheSrc, "Should not call CanElide() for readcache records");
            return stackCtx.hei.Address == stackCtx.recSrc.LogicalAddress && srcRecordInfo.PreviousAddress < hlogBase.BeginAddress
                        && !IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, srcRecordInfo);
        }

        // If the record is in a checkpoint range, it must not be modified. If it is in the fuzzy region, it can only be modified
        // if it is a new record.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsFrozen<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, RecordInfo srcRecordInfo)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(!stackCtx.recSrc.HasReadCacheSrc, "Should not call IsFrozen() for readcache records");
            return sessionFunctions.Ctx.IsInV1
                        && (stackCtx.recSrc.LogicalAddress <= _hybridLogCheckpoint.info.startLogicalAddress     // In checkpoint range
                            || !srcRecordInfo.IsInNewVersion);                                                  // In fuzzy region and an old version
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetMinRevivifiableAddress()
            => RevivificationManager.GetMinRevivifiableAddress(hlogBase.GetTailAddress(), hlogBase.ReadOnlyAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (bool elided, bool added) TryElideAndTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, ref LogRecord<TValue> logRecord)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Try to CAS out of the hashtable and if successful, add it to the free list.
            Debug.Assert(logRecord.Info.IsSealed, "Expected a Sealed record in TryElideAndTransferToFreeList");

            if (!stackCtx.hei.TryElide())
                return (false, false);

            return (true, TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref logRecord));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TryTransferToFreeList<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, ref LogRecord<TValue> logRecord)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // The record has been CAS'd out of the hashtable or elided from the chain, so add it to the free list.
            Debug.Assert(logRecord.Info.IsSealed, "Expected a Sealed record in TryTransferToFreeList");

            // Dispose any existing key and value. We do this as soon as we have elided so objects are released for GC as early as possible.
            DisposeRecord(ref logRecord, DisposeReason.RevivificationFreeList);

            // Now that we've Disposed the record, see if its address is revivifiable. If not, just leave it orphaned and invalid.
            if (stackCtx.recSrc.LogicalAddress < GetMinRevivifiableAddress())
                return false;

            return RevivificationManager.TryAdd(stackCtx.recSrc.LogicalAddress, ref logRecord, ref sessionFunctions.Ctx.RevivificationStats);
        }

        // Do not try to inline this; it causes TryAllocateRecord to bloat and slow
        bool TryTakeFreeRecord<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ref RecordSizeInfo sizeInfo, long minRevivAddress,
                    out long logicalAddress, out long physicalAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Caller checks for UseFreeRecordPool
            if (RevivificationManager.TryTake(ref sizeInfo, minRevivAddress, out logicalAddress, ref sessionFunctions.Ctx.RevivificationStats))
            {
                var logRecord = hlog.CreateLogRecord(logicalAddress);
                Debug.Assert(logRecord.Info.IsSealed, "TryTakeFreeRecord: recordInfo should still have the revivification Seal");

                // Preserve the Sealed bit due to checkpoint/recovery; see RecordInfo.WriteInfo.
                physicalAddress = logRecord.physicalAddress;
                return true;
            }

            // No free record available.
            logicalAddress = physicalAddress = default;
            return false;
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
            LogRecord<TValue>.GetInfoRef(localLog.GetPhysicalAddress(AbsoluteAddress(logicalAddress))).SetInvalid();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CASRecordIntoChain(long newLogicalAddress, ref LogRecord<TValue> newLogRecord, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            var result = stackCtx.recSrc.LowestReadCachePhysicalAddress == Constants.kInvalidAddress
                ? stackCtx.hei.TryCAS(newLogicalAddress)
                : SpliceIntoHashChainAtReadCacheBoundary(newLogRecord.Key, ref stackCtx, newLogicalAddress);
            if (result)
                newLogRecord.InfoRef.UnsealAndValidate();
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PostCopyToTail<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
            where TSourceLogRecord : ISourceLogRecord<TValue>
            => PostCopyToTail(ref srcLogRecord, ref stackCtx, stackCtx.hei.Address);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PostCopyToTail<TSourceLogRecord>(ref TSourceLogRecord srcLogRecord, ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, long highestReadCacheAddressChecked)
            where TSourceLogRecord : ISourceLogRecord<TValue>
        {
            // Nothing required here if not using ReadCache
            if (!UseReadCache)
                return;

            // We're using the read cache, so any insertion must check that a readcache insertion wasn't done
            if (stackCtx.recSrc.HasReadCacheSrc)
            {
                // If we already have a readcache source, there will not be another inserted, so we can just invalidate the source directly.
                srcLogRecord.InfoRef.SetInvalidAtomic();
            }
            else
            {
                // We did not have a readcache source, so while we spliced a new record into the readcache/mainlog gap a competing readcache record may have been inserted at the tail.
                // If so, invalidate it. highestReadCacheAddressChecked is hei.Address unless we are from ConditionalCopyToTail, which may have skipped the readcache before this.
                // See "Consistency Notes" in TryCopyToReadCache for a discussion of why there ie no "momentary inconsistency" possible here.
                ReadCacheCheckTailAfterSplice(srcLogRecord.Key, ref stackCtx.hei, highestReadCacheAddressChecked);
            }
        }

        // Called after BlockAllocate or anything else that could shift HeadAddress, to adjust addresses or return false for RETRY as needed.
        // This refreshes the HashEntryInfo, so the caller needs to recheck to confirm the BlockAllocated address is still > hei.Address.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool VerifyInMemoryAddresses(ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx)
        {
            // If we have an in-memory source that fell below HeadAddress, return false and the caller will RETRY_LATER.
            if (stackCtx.recSrc.HasInMemorySrc && stackCtx.recSrc.LogicalAddress < stackCtx.recSrc.AllocatorBase.HeadAddress)
                return false;

            // If we're not using readcache or we don't have a splice point or it is still above readcache.HeadAddress, we're good.
            if (!UseReadCache || stackCtx.recSrc.LowestReadCacheLogicalAddress == Constants.kInvalidAddress || stackCtx.recSrc.LowestReadCacheLogicalAddress >= readCacheBase.HeadAddress)
                return true;

            // If the splice point went below readcache.HeadAddress, we would have to wait for the chain to be fixed up by eviction,
            // so just return RETRY_LATER and restart the operation.
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindOrCreateTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Ephemeral must lock the bucket before traceback, to prevent revivification from yanking the record out from underneath us.
            // Manual locking already automatically locks the bucket. hei already has the key's hashcode.
            FindOrCreateTag(ref stackCtx.hei, hlogBase.BeginAddress);
            if (!TryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                return false;

            // Between the time we found the tag and the time we locked the bucket the record in hei.entry may have been elided, so make sure we don't have a stale address in hei.entry.
            stackCtx.hei.SetToCurrent();
            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagAndTryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Ephemeral must lock the bucket before traceback, to prevent revivification from yanking the record out from underneath us.
            // Manual locking already automatically locks the bucket. hei already has the key's hashcode.
            internalStatus = OperationStatus.NOTFOUND;
            if (!FindTag(ref stackCtx.hei) || !TryEphemeralXLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                return false;

            // Between the time we found the tag and the time we locked the bucket the record in hei.entry may have been elided, so make sure we don't have a stale address in hei.entry.
            stackCtx.hei.SetToCurrent();
            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool FindTagAndTryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TValue, TStoreFunctions, TAllocator> stackCtx, out OperationStatus internalStatus)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // Ephemeral must lock the bucket before traceback, to prevent revivification from yanking the record out from underneath us.
            // Manual locking already automatically locks the bucket. hei already has the key's hashcode.
            internalStatus = OperationStatus.NOTFOUND;
            if (!FindTag(ref stackCtx.hei) || !TryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out internalStatus))
                return false;

            // Between the time we found the tag and the time we locked the bucket the record in hei.entry may have been elided, so make sure we don't have a stale address in hei.entry.
            stackCtx.hei.SetToCurrent();
            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            return true;
        }
    }
}