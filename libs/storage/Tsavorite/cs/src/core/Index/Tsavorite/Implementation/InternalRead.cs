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
        /// Read operation. Computes the 'output' from 'input' and current value corresponding to 'key'.
        /// When the read operation goes pending, once the record is retrieved from disk, ContinuePendingRead completes the operation.
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="keyHash">Hashcode of <paramref name="key"/></param>
        /// <param name="input">Input required to compute output from value.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
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
        ///     <term>The output has been computed using current value of 'key' and 'input'; and stored in 'output'.</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk and the operation.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Refresh the epoch and retry.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_NOW</term>
        ///     <term>Retry without epoch refresh.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalRead<TKey, TInput, TOutput, TContext, TSessionFunctionsWrapper>(TKey key, long keyHash, ref TInput input, ref TOutput output,
                                    TContext userContext, ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions, bool isFromPending = false)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(keyHash);

            // pendingContext.keyHash and pendingContext.logicalAddress are NOT written here on the in-memory hot path:
            //   - keyHash now lives on the pending slot (op.slot.keyHash) — set lazily in CreatePendingReadContext if we go pending.
            //   - logicalAddress defaults to 0 (== kInvalidAddress) for a fresh pendingContext; the in-memory match paths
            //     below overwrite it on success, and HandleRetryStatus resets it on retry, so the in-memory path needs no write here.

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindTagAndTryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out var status))
                return status;
            ReadInfo readInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                Address = stackCtx.recSrc.LogicalAddress,
                IsFromPending = isFromPending,
            };

            try
            {
                LogRecord srcLogRecord;
                if (stackCtx.hei.IsReadCache)
                {
                    if (FindInReadCache(key, ref stackCtx, minAddress: kInvalidAddress, alwaysFindLatestLA: false))
                    {
                        // Note: When session is in PREPARE phase, a read-cache record cannot be new-version. This is because a new-version record
                        // insertion would have invalidated the read-cache entry, and before the new-version record can go to disk become eligible
                        // to enter the read-cache, the PREPARE phase for that session will be over due to an epoch refresh.
                        readInfo.Address = kInvalidAddress;     // ReadCache addresses are not valid for indexing etc. so pass kInvalidAddress.

                        srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                        return sessionFunctions.Reader(in srcLogRecord, ref input, ref output, ref readInfo)
                            ? OperationStatus.SUCCESS
                            : CheckFalseActionStatus(ref readInfo);
                    }

                    // FindInReadCache updated recSrc so update the readInfo accordingly.
                    readInfo.Address = stackCtx.recSrc.LogicalAddress;
                }

                // recSrc.LogicalAddress is set and is not in the readcache. Try to traceback in main log for key match.
                if (TraceBackForKeyMatch(key, ref stackCtx.recSrc, hlogBase.HeadAddress) && stackCtx.recSrc.GetInfo().IsClosed)
                    return OperationStatus.RETRY_LATER;
                status = OperationStatus.SUCCESS;

                // V threads cannot access V+1 records. Use the latest logical address rather than the traced address (logicalAddress) per comments in AcquireCPRLatchRMW.
                if (sessionFunctions.Ctx.phase == Phase.PREPARE && IsEntryVersionNew(ref stackCtx.hei.entry))
                    return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry

                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;
                if (stackCtx.recSrc.LogicalAddress >= hlogBase.SafeReadOnlyAddress)
                {
                    // Mutable region (even fuzzy region is included here)
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    if (srcLogRecord.Info.IsClosedOrTombstoned(ref status))
                        return status;

                    return sessionFunctions.Reader(in srcLogRecord, ref input, ref output, ref readInfo)
                        ? OperationStatus.SUCCESS
                        : CheckFalseActionStatus(ref readInfo);
                }

                // Pending (and CopyFromImmutable may go pending) must track the latest searched-below addresses. They are the same if there are no readcache records.
                pendingContext.initialEntryAddress = stackCtx.hei.Address;
                pendingContext.initialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;

                if (stackCtx.recSrc.LogicalAddress >= hlogBase.HeadAddress)
                {
                    // Immutable region
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    if (srcLogRecord.Info.IsClosedOrTombstoned(ref status))
                        return status;

                    if (sessionFunctions.Reader(in srcLogRecord, ref input, ref output, ref readInfo))
                    {
                        return pendingContext.readCopyOptions.CopyFrom != ReadCopyFrom.AllImmutable
                            ? OperationStatus.SUCCESS
                            : CopyFromImmutable(ref srcLogRecord, ref input, ref output, userContext, ref pendingContext, sessionFunctions, ref stackCtx, ref status);
                    }
                    return CheckFalseActionStatus(ref readInfo);
                }

                if (stackCtx.recSrc.LogicalAddress >= hlogBase.BeginAddress)
                {
                    // On-Disk Region
                    Debug.Assert(!sessionFunctions.IsTransactionalLocking || LockTable.IsLocked(ref stackCtx.hei), "A Transactional-session Read() of an on-disk key requires a LockTable lock");

                    // Note: we do not lock here; we wait until reading from disk, then lock in the ContinuePendingRead chain.
                    if (hlogBase.IsNullDevice)
                        return OperationStatus.NOTFOUND;
                    CreatePendingReadContext(key, keyHash, ref input, ref output, userContext, ref pendingContext, sessionFunctions, stackCtx.recSrc.LogicalAddress);
                    return OperationStatus.RECORD_ON_DISK;
                }

                // No record found
                Debug.Assert(!sessionFunctions.IsTransactionalLocking || LockTable.IsLocked(ref stackCtx.hei), "A Transactional-session Read() of a non-existent key requires a LockTable lock");
                return OperationStatus.NOTFOUND;
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                EphemeralSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private OperationStatus CopyFromImmutable<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ref LogRecord srcLogRecord, ref TInput input, ref TOutput output, TContext userContext,
                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, ref OperationStatus status)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.MainLog)
            {
                // Plumb source logical address so PostCopyToTail can name per-flush snapshot files.
                pendingContext.originalAddress = stackCtx.recSrc.LogicalAddress;
                status = ConditionalCopyToTail(sessionFunctions, ref pendingContext, stackCtx.hei.hash, in srcLogRecord, ref stackCtx, wantIO: false);
                return status;
            }
            if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.ReadCache && TryCopyToReadCache(in srcLogRecord, sessionFunctions, ref pendingContext, ref stackCtx))
            {
                // Copy to read cache is "best effort"; we don't return an error if it fails.
                return OperationStatus.SUCCESS | OperationStatus.COPIED_RECORD_TO_READ_CACHE;
            }
            return OperationStatus.SUCCESS;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static OperationStatus CheckFalseActionStatus(ref ReadInfo readInfo)
        {
            if (readInfo.Action == ReadAction.CancelOperation)
                return OperationStatus.CANCELED;
            if (readInfo.Action == ReadAction.Expire)
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired);
            if (readInfo.Action == ReadAction.WrongType)
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.WrongType);
            return OperationStatus.NOTFOUND;
        }

        /// <summary>
        /// Read operation without a key. Computes the 'output' from 'input' and current value at 'address'.
        /// When the read operation goes pending, once the record is retrieved from disk, ContinuePendingRead completes the operation.
        /// </summary>
        /// <param name="readAtAddress">The logical address to read from</param>
        /// <param name="key">Key of the record.</param>
        /// <param name="input">Input required to compute output from value.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="readOptions">Contains options controlling the Read operation</param>
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
        ///     <term>The output has been computed using current value of 'key' and 'input'; and stored in 'output'.</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk and the operation.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Refresh the epoch and retry.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_NOW</term>
        ///     <term>Retry without epoch refresh.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalReadAtAddress<TKey, TInput, TOutput, TContext, TSessionFunctionsWrapper>(long readAtAddress, TKey key, ref TInput input, ref TOutput output,
                                    ref ReadOptions readOptions, TContext userContext, ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            if (readAtAddress < hlogBase.BeginAddress)
                return OperationStatus.NOTFOUND;

            pendingContext.SetIsReadAtAddress();

            // We do things in a different order here than in InternalRead, in part to handle NoKey (especially with Revivification).
            if (readAtAddress < hlogBase.HeadAddress)
            {
                // Do not trace back in the pending callback if it is a key mismatch.
                pendingContext.SetIsNoKey();

                // keyHash=0 is intentional: ContinuePendingRead derives the hash from pendingContext.DiskLogRecord
                // for IsReadAtAddress/IsNoKey, and the in-memory re-issue path (which reads pendingContext.keyHash)
                // is gated on !IsReadAtAddress so it cannot fire here.
                CreatePendingReadContext(key, keyHash: 0L, ref input, ref output, userContext, ref pendingContext, sessionFunctions, readAtAddress);
                return OperationStatus.RECORD_ON_DISK;
            }

            // We're in-memory, so it is safe to get the address now.
            var srcLogRecord = hlog.CreateLogRecord(readAtAddress);

            // Get the key hash; we don't write it to pendingContext (a hot-path struct on the caller's stack) — keyHash
            // is a pending-only field that lives on the rented op's slot, and this in-memory branch never goes pending.
            long localKeyHash;
            if (readOptions.KeyHash.HasValue)
                localKeyHash = readOptions.KeyHash.Value;
            else if (!pendingContext.IsNoKey)
                localKeyHash = storeFunctions.GetKeyHashCode64(key);
            else
            {
                // We have NoKey and an in-memory address so we must get the record to get the key to get the hashcode check for index growth,
                // possibly lock the bucket, etc.
                localKeyHash = storeFunctions.GetKeyHashCode64(srcLogRecord);
            }

            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(localKeyHash);
            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindTagAndTryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out var status))
                return status;

            stackCtx.SetRecordSourceToHashEntry(hlogBase);
            stackCtx.recSrc.LogicalAddress = readAtAddress;
            _ = stackCtx.recSrc.SetPhysicalAddress();

            // Note: We read directly from the address either in memory or pending, so do not do any ReadCache operations.

            try
            {
                // We're not doing RETRY_LATER if there is a Closed record; we only return valid records here.
                // Closed records may be reclaimed by revivification, so we do not return them if we are using the revivification free list.
                if (srcLogRecord.Info.IsClosed && RevivificationManager.UseFreeRecordPool)
                    return OperationStatus.NOTFOUND;
                // We do not check for Tombstone here; we return the record to the caller.

                stackCtx.recSrc.SetHasMainLogSrc();
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

                ReadInfo readInfo = new()
                {
                    Version = sessionFunctions.Ctx.version,
                    Address = stackCtx.recSrc.LogicalAddress,
                    IsFromPending = false,
                };

                // Ignore the return value from the ISessionFunctions calls; we're doing nothing else based on it.
                status = OperationStatus.SUCCESS;
                _ = sessionFunctions.Reader(in srcLogRecord, ref input, ref output, ref readInfo);
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                EphemeralSUnlock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx);
            }
            return status;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void CreatePendingReadContext<TKey, TInput, TOutput, TContext, TSessionFunctionsWrapper>(TKey key, long keyHash, ref TInput input, ref TOutput output, TContext userContext,
                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions, long logicalAddress)
             where TKey : IKey
#if NET9_0_OR_GREATER
                , allows ref struct
#endif
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // If a re-pend has already moved a populated slot onto pendingContext.pendingOp, reuse it; otherwise rent a
            // fresh op and populate its slot. HandleOperationStatus will pick up pendingContext.pendingOp on RECORD_ON_DISK
            // to finalize the IO issue.
            var op = pendingContext.pendingOp ?? sessionFunctions.Ctx.RentAsyncIOContext();
            ref var slot = ref op.slot;
            slot.type = OperationType.READ;
            slot.keyHash = keyHash;
            slot.CopyInputsForReadOrRMW(key, ref input, ref output, userContext, sessionFunctions, hlogBase.bufferPool);

            pendingContext.pendingOp = op;
            pendingContext.logicalAddress = logicalAddress;
        }
    }
}