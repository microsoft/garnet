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
        internal OperationStatus InternalRead<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, long keyHash, ref TInput input, ref TOutput output,
                                    TContext userContext, ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;
            pendingContext.logicalAddress = kInvalidAddress;
            pendingContext.eTag = LogRecord.NoETag;

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindTagAndTryEphemeralSLock<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, out var status))
                return status;
            ReadInfo readInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                Address = stackCtx.recSrc.LogicalAddress,
                IsFromPending = pendingContext.type != OperationType.NONE,
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
                        pendingContext.eTag = srcLogRecord.ETag;
                        if (sessionFunctions.Reader(in srcLogRecord, ref input, ref output, ref readInfo))
                            return OperationStatus.SUCCESS;
                        return CheckFalseActionStatus(ref readInfo);
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

                if (stackCtx.recSrc.LogicalAddress >= hlogBase.SafeReadOnlyAddress)
                {
                    // Mutable region (even fuzzy region is included here)
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    pendingContext.eTag = srcLogRecord.ETag;
                    if (srcLogRecord.Info.IsClosedOrTombstoned(ref status))
                        return status;

                    return sessionFunctions.Reader(in srcLogRecord, ref input, ref output, ref readInfo)
                        ? OperationStatus.SUCCESS
                        : CheckFalseActionStatus(ref readInfo);
                }

                // Pending (and CopyFromImmutable may go pending) must track the latest searched-below addresses. They are the same if there are no readcache records.
                pendingContext.initialEntryAddress = stackCtx.hei.Address;
                pendingContext.initialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

                if (stackCtx.recSrc.LogicalAddress >= hlogBase.HeadAddress)
                {
                    // Immutable region
                    srcLogRecord = stackCtx.recSrc.CreateLogRecord();
                    pendingContext.eTag = srcLogRecord.ETag;
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
                    CreatePendingReadContext(key, ref input, ref output, userContext, ref pendingContext, sessionFunctions, stackCtx.recSrc.LogicalAddress);
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
                status = ConditionalCopyToTail(sessionFunctions, ref pendingContext, in srcLogRecord, ref stackCtx, wantIO: false);
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
        internal OperationStatus InternalReadAtAddress<TInput, TOutput, TContext, TSessionFunctionsWrapper>(long readAtAddress, ReadOnlySpan<byte> key, ref TInput input, ref TOutput output,
                                    ref ReadOptions readOptions, TContext userContext, ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions)
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

                CreatePendingReadContext(key, ref input, ref output, userContext, ref pendingContext, sessionFunctions, readAtAddress);
                return OperationStatus.RECORD_ON_DISK;
            }

            // We're in-memory, so it is safe to get the address now.
            var srcLogRecord = hlog.CreateLogRecord(readAtAddress);

            // Get the key hash.
            if (readOptions.KeyHash.HasValue)
                pendingContext.keyHash = readOptions.KeyHash.Value;
            else if (!pendingContext.IsNoKey)
                pendingContext.keyHash = storeFunctions.GetKeyHashCode64(key);
            else
            {
                // We have NoKey and an in-memory address so we must get the record to get the key to get the hashcode check for index growth,
                // possibly lock the bucket, etc.
                pendingContext.keyHash = storeFunctions.GetKeyHashCode64(srcLogRecord.Key);
            }

            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(pendingContext.keyHash);
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
                // Closed records may be reclaimed by revivification, so we do not return them.
                if (srcLogRecord.Info.IsClosed)
                    return OperationStatus.NOTFOUND;
                // We do not check for Tombstone here; we return the record to the caller.

                stackCtx.recSrc.SetHasMainLogSrc();
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

                ReadInfo readInfo = new()
                {
                    Version = sessionFunctions.Ctx.version,
                    Address = stackCtx.recSrc.LogicalAddress,
                    IsFromPending = pendingContext.type != OperationType.NONE,
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
        private void CreatePendingReadContext<TInput, TOutput, TContext, TSessionFunctionsWrapper>(ReadOnlySpan<byte> key, ref TInput input, ref TOutput output, TContext userContext,
                ref PendingContext<TInput, TOutput, TContext> pendingContext, TSessionFunctionsWrapper sessionFunctions, long logicalAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            pendingContext.type = OperationType.READ;
            pendingContext.CopyInputsForReadOrRMW(key, ref input, ref output, userContext, sessionFunctions, hlogBase.bufferPool);
            pendingContext.logicalAddress = logicalAddress;
        }
    }
}