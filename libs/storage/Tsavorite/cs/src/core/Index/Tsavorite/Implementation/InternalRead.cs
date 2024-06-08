﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Tsavorite.core
{
    public unsafe partial class TsavoriteKV<Key, Value> : TsavoriteBase
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
        internal OperationStatus InternalRead<Input, Output, Context, TSessionFunctionsWrapper>(ref Key key, long keyHash, ref Input input, ref Output output,
                                    Context userContext, ref PendingContext<Input, Output, Context> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            OperationStackContext<Key, Value> stackCtx = new(keyHash);
            pendingContext.keyHash = keyHash;

            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindTagAndTryTransientSLock<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx, out OperationStatus status))
                return status;
            stackCtx.SetRecordSourceToHashEntry(hlog);

            // We have to assign a reference on declaration, so assign it here before we know whether LogicalAddress is above or below HeadAddress.
            // It must be at this scope so it can be unlocked in 'finally'.
            RecordInfo dummyRecordInfo = RecordInfo.InitialValid;
            ref RecordInfo srcRecordInfo = ref dummyRecordInfo;

            ReadInfo readInfo = new()
            {
                Version = sessionFunctions.Ctx.version,
                IsFromPending = pendingContext.type != OperationType.NONE,
            };

            try
            {
                if (stackCtx.hei.IsReadCache)
                {
                    if (FindInReadCache(ref key, ref stackCtx, minAddress: Constants.kInvalidAddress, alwaysFindLatestLA: false))
                    {
                        // Note: When session is in PREPARE phase, a read-cache record cannot be new-version. This is because a new-version record
                        // insertion would have invalidated the read-cache entry, and before the new-version record can go to disk become eligible
                        // to enter the read-cache, the PREPARE phase for that session will be over due to an epoch refresh.

                        // This is not called when looking up by address, so we can set pendingContext.recordInfo.
                        srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                        pendingContext.recordInfo = srcRecordInfo;
                        readInfo.SetRecordInfo(ref srcRecordInfo);

                        readInfo.Address = Constants.kInvalidAddress;   // ReadCache addresses are not valid for indexing etc. so pass kInvalidAddress.

                        if (sessionFunctions.SingleReader(ref key, ref input, ref stackCtx.recSrc.GetValue(), ref output, ref readInfo))
                            return OperationStatus.SUCCESS;
                        return readInfo.Action == ReadAction.CancelOperation ? OperationStatus.CANCELED : OperationStatus.NOTFOUND;
                    }
                }

                // recSrc.LogicalAddress is set and is not in the readcache. Traceback for key match.
                if (!TryFindRecordForRead(ref key, ref stackCtx, hlog.HeadAddress, out status))
                    return status;

                // Track the latest searched-below addresses. They are the same if there are no readcache records.
                pendingContext.InitialEntryAddress = stackCtx.hei.Address;
                pendingContext.InitialLatestLogicalAddress = stackCtx.recSrc.LatestLogicalAddress;
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

                readInfo.Address = stackCtx.recSrc.LogicalAddress;

                // V threads cannot access V+1 records. Use the latest logical address rather than the traced address (logicalAddress) per comments in AcquireCPRLatchRMW.
                if (sessionFunctions.Ctx.phase == Phase.PREPARE && IsEntryVersionNew(ref stackCtx.hei.entry))
                    return OperationStatus.CPR_SHIFT_DETECTED; // Pivot thread; retry

                if (stackCtx.recSrc.LogicalAddress >= hlog.SafeReadOnlyAddress)
                {
                    // Mutable region (even fuzzy region is included here)
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                    pendingContext.recordInfo = srcRecordInfo;
                    readInfo.SetRecordInfo(ref srcRecordInfo);

                    if (srcRecordInfo.IsClosedOrTombstoned(ref status))
                        return status;

                    if (sessionFunctions.ConcurrentReader(ref key, ref input, ref stackCtx.recSrc.GetValue(), ref output, ref readInfo, ref srcRecordInfo))
                        return OperationStatus.SUCCESS;
                    return CheckFalseActionStatus(readInfo);
                }

                if (stackCtx.recSrc.LogicalAddress >= hlog.HeadAddress)
                {
                    // Immutable region
                    srcRecordInfo = ref stackCtx.recSrc.GetInfo();
                    pendingContext.recordInfo = srcRecordInfo;
                    readInfo.SetRecordInfo(ref srcRecordInfo);

                    if (srcRecordInfo.IsClosedOrTombstoned(ref status))
                        return status;

                    if (sessionFunctions.SingleReader(ref key, ref input, ref stackCtx.recSrc.GetValue(), ref output, ref readInfo))
                    {
                        if (pendingContext.readCopyOptions.CopyFrom != ReadCopyFrom.AllImmutable)
                            return OperationStatus.SUCCESS;
                        return CopyFromImmutable(ref key, ref input, ref output, userContext, ref pendingContext, sessionFunctions, ref stackCtx, ref status, stackCtx.recSrc.GetValue());
                    }
                    return CheckFalseActionStatus(readInfo);
                }

                if (stackCtx.recSrc.LogicalAddress >= hlog.BeginAddress)
                {
                    // On-Disk Region
                    Debug.Assert(!sessionFunctions.IsManualLocking || LockTable.IsLocked(ref stackCtx.hei), "A Lockable-session Read() of an on-disk key requires a LockTable lock");

                    // Note: we do not lock here; we wait until reading from disk, then lock in the ContinuePendingRead chain.
                    if (hlog.IsNullDevice)
                        return OperationStatus.NOTFOUND;
                    CreatePendingReadContext(ref key, ref input, output, userContext, ref pendingContext, sessionFunctions, stackCtx.recSrc.LogicalAddress);
                    return OperationStatus.RECORD_ON_DISK;
                }

                // No record found
                Debug.Assert(!sessionFunctions.IsManualLocking || LockTable.IsLocked(ref stackCtx.hei), "A Lockable-session Read() of a non-existent key requires a LockTable lock");
                return OperationStatus.NOTFOUND;
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                TransientSUnlock<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx);
            }
        }

        // No AggressiveInlining; this is a less-common function and it may improve inlining of InternalRead to have this be a virtcall.
        private OperationStatus CopyFromImmutable<Input, Output, Context, TSessionFunctionsWrapper>(ref Key key, ref Input input, ref Output output, Context userContext,
                ref PendingContext<Input, Output, Context> pendingContext, TSessionFunctionsWrapper sessionFunctions, ref OperationStackContext<Key, Value> stackCtx, ref OperationStatus status, Value recordValue)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.MainLog)
            {
                status = ConditionalCopyToTail(sessionFunctions, ref pendingContext, ref key, ref input, ref recordValue, ref output, userContext, ref stackCtx,
                                               WriteReason.CopyToTail, wantIO: false);
                if (status == OperationStatus.ALLOCATE_FAILED && pendingContext.IsAsync)    // May happen due to CopyToTailFromReadOnly
                    CreatePendingReadContext(ref key, ref input, output, userContext, ref pendingContext, sessionFunctions, stackCtx.recSrc.LogicalAddress);
                return status;
            }
            if (pendingContext.readCopyOptions.CopyTo == ReadCopyTo.ReadCache
                    && TryCopyToReadCache(sessionFunctions, ref pendingContext, ref key, ref input, ref recordValue, ref stackCtx))
            {
                // Copy to read cache is "best effort"; we don't return an error if it fails.
                return OperationStatus.SUCCESS | OperationStatus.COPIED_RECORD_TO_READ_CACHE;
            }
            return OperationStatus.SUCCESS;
        }

        // No AggressiveInlining; this is a less-common function and it may improve inlining of InternalRead to have this be a virtcall.
        private static OperationStatus CheckFalseActionStatus(ReadInfo readInfo)
        {
            if (readInfo.Action == ReadAction.CancelOperation)
                return OperationStatus.CANCELED;
            if (readInfo.Action == ReadAction.Expire)
                return OperationStatusUtils.AdvancedOpCode(OperationStatus.NOTFOUND, StatusCode.Expired);
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalReadAtAddress<Input, Output, Context, TSessionFunctionsWrapper>(long readAtAddress, ref Key key, ref Input input, ref Output output,
                                    ref ReadOptions readOptions, Context userContext, ref PendingContext<Input, Output, Context> pendingContext, TSessionFunctionsWrapper sessionFunctions)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            if (readAtAddress < hlog.BeginAddress)
                return OperationStatus.NOTFOUND;

            pendingContext.IsReadAtAddress = true;

            // We do things in a different order here than in InternalRead, in part to handle NoKey (especially with Revivification).
            if (readAtAddress < hlog.HeadAddress)
            {
                // Do not trace back in the pending callback if it is a key mismatch.
                pendingContext.NoKey = true;

                CreatePendingReadContext(ref key, ref input, output, userContext, ref pendingContext, sessionFunctions, readAtAddress);
                return OperationStatus.RECORD_ON_DISK;
            }

            // We're in-memory, so it is safe to get the address now.
            var physicalAddress = hlog.GetPhysicalAddress(readAtAddress);

            Key defaultKey = default;
            if (readOptions.KeyHash.HasValue)
                pendingContext.keyHash = readOptions.KeyHash.Value;
            else if (!pendingContext.NoKey)
                pendingContext.keyHash = comparer.GetHashCode64(ref key);
            else
            {
                // We have NoKey and an in-memory address so we must get the record to get the key to get the hashcode check for index growth,
                // possibly lock the bucket, etc.
                pendingContext.keyHash = comparer.GetHashCode64(ref hlog.GetKey(physicalAddress));

#pragma warning disable CS9085 // "This ref-assigns a value that has a narrower escape scope than the target", but we don't return the reference.
                // Note: With bucket-based locking the key is not used for Transient locks (only the key's hashcode is used). A key-based locking system
                // would require this to be the actual key. We do *not* set this to the record key in case that is reclaimed by revivification.
                key = ref defaultKey;
#pragma warning restore CS9085
            }

            OperationStackContext<Key, Value> stackCtx = new(pendingContext.keyHash);
            if (sessionFunctions.Ctx.phase == Phase.IN_PROGRESS_GROW)
                SplitBuckets(stackCtx.hei.hash);

            if (!FindTagAndTryTransientSLock<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx, out OperationStatus status))
                return status;

            stackCtx.SetRecordSourceToHashEntry(hlog);
            stackCtx.recSrc.LogicalAddress = readAtAddress;
            stackCtx.recSrc.SetPhysicalAddress();

            // Note: We read directly from the address either in memory or pending, so do not do any ReadCache operations.

            // srcRecordInfo must be at scope above 'try' so 'finally' can see it for record unlocking.
            ref RecordInfo srcRecordInfo = ref stackCtx.recSrc.GetInfo();

            try
            {
                // We're not doing RETRY_LATER if there is a Closed record; we only return valid records here.
                // Closed records may be reclaimed by revivification, so we do not return them.
                if (srcRecordInfo.IsClosed)
                    return OperationStatus.NOTFOUND;
                // We do not check for Tombstone here; we return the record to the caller.

                stackCtx.recSrc.SetHasMainLogSrc();
                pendingContext.recordInfo = srcRecordInfo;
                pendingContext.logicalAddress = stackCtx.recSrc.LogicalAddress;

                ReadInfo readInfo = new()
                {
                    Version = sessionFunctions.Ctx.version,
                    Address = stackCtx.recSrc.LogicalAddress,
                    IsFromPending = pendingContext.type != OperationType.NONE,
                };
                readInfo.SetRecordInfo(ref srcRecordInfo);

                // Ignore the return value from the ISessionFunctions calls; we're doing nothing else based on it.
                status = OperationStatus.SUCCESS;
                if (stackCtx.recSrc.LogicalAddress >= hlog.SafeReadOnlyAddress)
                {
                    // Mutable region (even fuzzy region is included here).
                    sessionFunctions.ConcurrentReader(ref stackCtx.recSrc.GetKey(), ref input, ref stackCtx.recSrc.GetValue(), ref output, ref readInfo, ref srcRecordInfo);
                }
                else
                {
                    // Immutable region (we tested for < HeadAddress above).
                    sessionFunctions.SingleReader(ref stackCtx.recSrc.GetKey(), ref input, ref stackCtx.recSrc.GetValue(), ref output, ref readInfo);
                }
            }
            finally
            {
                stackCtx.HandleNewRecordOnException(this);
                TransientSUnlock<Input, Output, Context, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx);
            }
            return status;
        }

        // No AggressiveInlining; this is called only for the pending case and may improve inlining of InternalRead in the normal case if the compiler decides not to inline this.
        private void CreatePendingReadContext<Input, Output, Context, TSessionFunctionsWrapper>(ref Key key, ref Input input, Output output, Context userContext,
                ref PendingContext<Input, Output, Context> pendingContext, TSessionFunctionsWrapper sessionFunctions, long logicalAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<Key, Value, Input, Output, Context>
        {
            pendingContext.type = OperationType.READ;
            if (!pendingContext.NoKey && pendingContext.key == default)    // If this is true, we don't have a valid key
                pendingContext.key = hlog.GetKeyContainer(ref key);
            if (pendingContext.input == default)
                pendingContext.input = sessionFunctions.GetHeapContainer(ref input);

            pendingContext.output = output;
            sessionFunctions.ConvertOutputToHeap(ref input, ref pendingContext.output);

            pendingContext.userContext = userContext;
            pendingContext.logicalAddress = logicalAddress;
        }
    }
}