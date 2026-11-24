// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    public abstract partial class AllocatorBase<TStoreFunctions, TAllocator> : IDisposable
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>Clear checkpoint serialization cached by heap objects in the specified in-memory range.</summary>
        internal virtual void ClearSerializedObjectData(long beginAddress, long endAddress) { }

        /// <summary>
        /// Pull-based scan interface for HLOG; user calls GetNext() which advances through the address range.
        /// </summary>
        /// <returns>Pull Scan iterator instance</returns>
        public abstract ITsavoriteScanIterator Scan(TsavoriteKV<TStoreFunctions, TAllocator> store, long beginAddress, long endAddress, DiskScanBufferingMode scanBufferingMode = DiskScanBufferingMode.DoublePageBuffering, bool includeClosedRecords = false);

        /// <summary>
        /// Push-based scan interface for HLOG, called from LogAccessor; scan the log given address range, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal abstract bool Scan<TScanFunctions>(TsavoriteKV<TStoreFunctions, TAllocator> store, long beginAddress, long endAddress, ref TScanFunctions scanFunctions,
                DiskScanBufferingMode scanBufferingMode = DiskScanBufferingMode.DoublePageBuffering)
            where TScanFunctions : IScanIteratorFunctions;

        /// <summary>
        /// Push-based iteration of key versions, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal bool IterateKeyVersions<TKey, TScanFunctions>(TsavoriteKV<TStoreFunctions, TAllocator> store, TKey key, ref TScanFunctions scanFunctions)
            where TKey : IKey, allows ref struct
            where TScanFunctions : IScanIteratorFunctions
        {
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(storeFunctions.GetKeyHashCode64(key));
            if (!store.FindTag(ref stackCtx.hei))
                return false;
            stackCtx.SetRecordSourceToHashEntry(store.hlogBase);
            // Gate on hei.IsReadCache (same cost as UseReadCache): skips the call when this chain has no RC prefix, and
            // lets SkipReadCache's UseReadCache assert flag an RC-record-while-!UseReadCache anomaly.
            if (stackCtx.hei.IsReadCache)
                store.SkipReadCache(ref stackCtx, out _);
            if (stackCtx.recSrc.LogicalAddress < store.hlogBase.BeginAddress)
                return false;
            return IterateKeyVersions(store, key, stackCtx.recSrc.LogicalAddress, ref scanFunctions);
        }

        /// <summary>
        /// Push-based iteration of key versions, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal abstract bool IterateKeyVersions<TKey, TScanFunctions>(TsavoriteKV<TStoreFunctions, TAllocator> store, TKey key, long beginAddress, ref TScanFunctions scanFunctions)
            where TKey : IKey, allows ref struct
            where TScanFunctions : IScanIteratorFunctions;

        /// <summary>
        /// Implementation for push-scanning Tsavorite log
        /// </summary>
        internal bool PushScanImpl<TScanFunctions, TScanIterator>(long beginAddress, long endAddress, ref TScanFunctions scanFunctions, TScanIterator iter)
            where TScanFunctions : IScanIteratorFunctions
            where TScanIterator : ITsavoriteScanIterator, IPushScanIterator
        {
            if (!scanFunctions.OnStart(beginAddress, endAddress))
                return false;

            long numRecords = 1;
            var stop = false;
            for (; !stop && iter.GetNext(); numRecords++)
            {
                try
                {
                    if (iter.Info.IsClosed)    // Iterator checks this but it may have changed since
                        continue;

                    // Pull Iter records are in temp storage so do not need locks.
                    stop = !scanFunctions.Reader(in iter, new RecordMetadata(iter.CurrentAddress), numRecords, out _);
                }
                catch (Exception ex)
                {
                    scanFunctions.OnException(ex, numRecords);
                    throw;
                }
            }

            scanFunctions.OnStop(!stop, numRecords);
            return !stop;
        }

        /// <summary>
        /// Implementation for push-iterating key versions
        /// </summary>
        internal bool IterateHashChain<TKey, TScanFunctions, TScanIterator>(TsavoriteKV<TStoreFunctions, TAllocator> store, TKey key, long beginAddress, ref TScanFunctions scanFunctions, TScanIterator iter)
            where TKey : IKey, allows ref struct
            where TScanFunctions : IScanIteratorFunctions
            where TScanIterator : ITsavoriteScanIterator, IPushScanIterator
        {
            if (!scanFunctions.OnStart(beginAddress, kInvalidAddress))
                return false;
            var readOnlyAddress = ReadOnlyAddress;

            long numRecords = 1;
            bool stop = false, continueOnDisk = false;
            for (; !stop && iter.BeginGetPrevInMemory(key, out var logRecord, out continueOnDisk); numRecords++)
            {
                OperationStackContext<TStoreFunctions, TAllocator> stackCtx = default;
                try
                {
                    // Iter records above readOnlyAddress will be in mutable log memory so the chain must be locked.
                    // We hold the epoch so iter does not need to copy, so do not use iter's ISourceLogRecord implementation; create a local LogRecord around the address.
                    if (iter.CurrentAddress >= readOnlyAddress && !logRecord.Info.IsClosed)
                        store.LockForScan(ref stackCtx, key);
                    stop = !scanFunctions.Reader(in logRecord, new RecordMetadata(iter.CurrentAddress), numRecords, out _);
                }
                catch (Exception ex)
                {
                    scanFunctions.OnException(ex, numRecords);
                    throw;
                }
                finally
                {
                    if (stackCtx.recSrc.HasLock)
                        store.UnlockForScan(ref stackCtx);
                    iter.EndGetPrevInMemory();
                }
            }

            if (continueOnDisk)
            {
                AsyncIOContextCompletionEvent completionEvent = new();
                try
                {
                    var logicalAddress = iter.CurrentAddress;
                    while (!stop && GetFromDiskAndPushToReader(key, ref logicalAddress, ref scanFunctions, numRecords, completionEvent, out stop))
                        ++numRecords;
                }
                catch (Exception ex)
                {
                    scanFunctions.OnException(ex, numRecords);
                    throw;
                }
                finally
                {
                    completionEvent.Dispose();
                }
            }

            scanFunctions.OnStop(!stop, numRecords);
            return !stop;
        }

        internal bool GetFromDiskAndPushToReader<TKey, TScanFunctions>(TKey key, ref long logicalAddress, ref TScanFunctions scanFunctions, long numRecords,
                AsyncIOContextCompletionEvent completionEvent, out bool stop)
            where TKey : IKey, allows ref struct
            where TScanFunctions : IScanIteratorFunctions
        {
            stop = false;
            if (logicalAddress < BeginAddress)
                return false;

            completionEvent.Prepare(key, logicalAddress, bufferPool);
            AsyncGetFromDisk(logicalAddress, IStreamBuffer.DefaultInitialIORecordSize, completionEvent.request);
            completionEvent.Wait();

            ref var request = ref completionEvent.request;
            try
            {
                if (completionEvent.exception is not null)
                {
                    scanFunctions.OnException(completionEvent.exception, numRecords);
                    return false;
                }

                // The chain walk went below the resolvable range without finding another version of the key.
                if (!request.diskLogRecord.IsSet)
                    return false;

                request.diskLogRecord.InfoRef.ClearBitsForDiskImages();
                if (storeFunctions.CallOnDiskRead)
                    storeFunctions.OnDiskRead(ref request.diskLogRecord.logRecord);
                stop = !scanFunctions.Reader(in request.diskLogRecord, new RecordMetadata(request.logicalAddress), numRecords, out _);
                logicalAddress = request.diskLogRecord.Info.PreviousAddress;
            }
            finally
            {
                _wrapper.OnDisposeDiskRecord(ref request.diskLogRecord, DisposeReason.DeserializedFromDisk);
                request.DisposeRecord();
            }
            return !stop;
        }

        /// <summary>
        /// Push-based scan interface for HLOG with cursor, called from LogAccessor; scan the log from <paramref name="cursor"/> (which must be a valid address) and push up to <paramref name="count"/> records
        /// to the caller via <paramref name="scanFunctions"/> for each Key that is not found at a higher address.
        /// </summary>
        /// <returns>True if Scan completed and pushed <paramref name="count"/> records; false if Scan ended early due to finding less than <paramref name="count"/> records
        /// or one of the TScanIterator reader functions returning false</returns>
        /// <remarks>Currently we load an entire page, which while inefficient in performance, allows us to make the cursor safe (by ensuring we align to a valid record) if it is not
        /// the last one returned. We could optimize this to load only the subset of a page that is pointed to by the cursor and use DiskLogRecord.GetSerializedRecordLength as in
        /// AsyncGetFromDiskCallback. However, this would not validate the cursor and would therefore require maintaining a cursor history.
        /// <para>Only streaming checkpoint phase 2 sets <paramref name="skipInvalidRecords"/>, which skips every invalid record, including elided ones.
        /// Other lookups skip only invalid records that are not sealed, and a bounded lookup pushes an elided record only if its key has a record at
        /// or above <paramref name="maxAddress"/>; see <see cref="InvalidRecordScanFilter"/> and <see cref="ConditionalScanPush"/>.</para></remarks>
        internal abstract bool ScanCursor<TScanFunctions>(TsavoriteKV<TStoreFunctions, TAllocator> store, ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions,
                long endAddress, bool validateCursor, long maxAddress, bool resetCursor = true, bool includeTombstones = false, bool skipInvalidRecords = false)
            where TScanFunctions : IScanIteratorFunctions;

        /// <summary>
        /// Disk buffering for <see cref="ScanCursor"/>. A bounded <paramref name="count"/> (e.g. SCAN) creates a new iterator per call and usually
        /// consumes only part of one page, so read-ahead would be wasted IO. An unbounded count iterates the whole log in one call, so reading the
        /// next page while the current one is processed overlaps IO with the per-record liveness checks.
        /// </summary>
        private protected static DiskScanBufferingMode GetScanCursorBufferingMode(long count)
            => count == long.MaxValue ? DiskScanBufferingMode.DoublePageBuffering : DiskScanBufferingMode.SinglePageBuffering;

        private protected bool ScanLookup<TInput, TOutput, TScanFunctions, TScanIterator>(TsavoriteKV<TStoreFunctions, TAllocator> store,
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, TScanIterator iter, bool validateCursor, long maxAddress,
                bool resetCursor = true, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions
            where TScanIterator : ITsavoriteScanIterator, IPushScanIterator
        {
            using var session = store.NewSession<TScanIterator, TInput, TOutput, Empty, NoOpSessionFunctions<TInput, TOutput, Empty>>(new NoOpSessionFunctions<TInput, TOutput, Empty>());
            var bContext = session.BasicContext;

            if (cursor < BeginAddress) // This includes 0, which means to start the Scan
                cursor = BeginAddress;
            else if (validateCursor && !iter.SnapCursorToLogicalAddress(ref cursor))
                goto IterationComplete;

            if (!scanFunctions.OnStart(cursor, iter.EndAddress))
                return false;

            if (cursor >= GetTailAddress())
                goto IterationComplete;

            scanCursorState.Initialize(scanFunctions);

            long numPending = 0;
            var batchComplete = false;
            while (iter.GetNext())
            {
                if (!iter.Info.Tombstone || includeTombstones)
                {
                    var status = bContext.ConditionalScanPush(scanCursorState, in iter, iter.CurrentAddress, iter.NextAddress, maxAddress);
                    if (status.IsPending)
                    {
                        ++numPending;
                        if (numPending == count - scanCursorState.acceptedCount || numPending > 256)
                        {
                            _ = bContext.CompletePending(wait: true);
                            numPending = 0;
                        }
                    }
                }

                // Update the cursor to point to the next record.
                cursor = iter.NextAddress;

                // Now see if we completed the enumeration.
                if (scanCursorState.stop)
                    break;
                if (scanCursorState.acceptedCount >= count || scanCursorState.endBatch)
                {
                    batchComplete = true;
                    break;
                }
            }

            // Pending callbacks can reject earlier records, including after the iterator reaches its end.
            if (numPending > 0)
                _ = bContext.CompletePending(wait: true);

            if (!scanCursorState.stop && (batchComplete || scanCursorState.endBatch))
            {
                cursor = Math.Min(cursor, scanCursorState.retryAddress);
                scanFunctions.OnStop(true, scanCursorState.acceptedCount);
                return true;
            }

        IterationComplete:
            if (resetCursor)
                cursor = 0;
            scanFunctions.OnStop(false, scanCursorState.acceptedCount);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(TSessionFunctionsWrapper sessionFunctions,
                ScanCursorState scanCursorState, in TSourceLogRecord srcLogRecord, long originalAddress, long currentAddress, long minAddress, long maxAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is called only from ScanLookup so the epoch should be protected");
            var operationState = default(TsavoriteKV<TStoreFunctions, TAllocator>.OperationState<TInput, TOutput, TContext>);
            var keyHash = storeFunctions.GetKeyHashCode64(srcLogRecord);

            OperationStatus internalStatus;
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(keyHash);
            bool needIO;
            do
            {
                // If a more recent version of the record exists, do not push this one. Start by searching in-memory.
                if (sessionFunctions.Store.TryFindRecordInMainLogForConditionalOperation<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, srcLogRecord, ref stackCtx,
                      currentAddress, minAddress, maxAddress, out internalStatus, out needIO))
                    return Status.CreateFound();
            }
            while (sessionFunctions.Store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(internalStatus, sessionFunctions));

            // An absent tag means no version of the key is reachable (a delete elided it), so do not push this record.
            if (internalStatus == OperationStatus.NOTFOUND)
                return Status.CreateFound();

            if (needIO)
            {
                // A more recent version of the key was not (yet) found and we need another IO to continue searching.
                internalStatus = PrepareIOForConditionalScan(sessionFunctions, ref operationState, keyHash, in srcLogRecord, ref stackCtx, originalAddress, minAddress, maxAddress, scanCursorState);
            }
            else
            {
                // Only bounded lookups return invalid records, and only sealed ones; streaming checkpoint phase 2 skips them all
                // (see InvalidRecordScanFilter). An invalid sealed record is usually elided: an update replaced the key's only
                // record, or a delete removed it. The search above covers only addresses below maxAddress, and NOTFOUND covers
                // only an empty hash slot; once another key with the same tag holds that slot, a deleted key's elided record looks
                // live. So push the record only if its key has a record at or above maxAddress (a later version or a tombstone),
                // or if finding one needs IO. These callers (slot migration and IterateLookupSnapshot) need keys: reporting a
                // deleted key is harmless, but dropping a live one is not. A key deleted before the bound and re-created after it
                // is pushed; it exists now. A record whose insert never committed is also invalid and sealed, and is handled the
                // same way. An update that elides a record releases its overflow key, so that record cannot be matched to its key,
                // which is then reported only if it has a record below maxAddress. A pending continuation re-enters here with a copy
                // that keeps both bits.
                var recordInfo = srcLogRecord.Info;
                if (recordInfo.Invalid && recordInfo.IsSealed
                        && !HasRecordAtOrAbove<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(sessionFunctions, in srcLogRecord, keyHash, currentAddress, maxAddress))
                    return Status.CreateFound();

                // A more recent version of the key was not found, so push the original record (with its originalAddress).
                RecordMetadata recordMetadata = new(originalAddress);

                epoch.Suspend();
                try
                {
                    var stop = !scanCursorState.functions.Reader(in srcLogRecord, recordMetadata, scanCursorState.acceptedCount, out var cursorRecordResult);
                    if (stop)
                        scanCursorState.stop = true;
                    else
                    {
                        if ((cursorRecordResult & CursorRecordResult.Accept) != 0)
                            _ = Interlocked.Increment(ref scanCursorState.acceptedCount);
                        if ((cursorRecordResult & CursorRecordResult.EndBatch) != 0)
                            scanCursorState.endBatch = true;
                        if ((cursorRecordResult & CursorRecordResult.RetryLastRecord) != 0)
                            scanCursorState.retryAddress = Math.Min(scanCursorState.retryAddress, originalAddress);
                    }
                }
                finally
                {
                    epoch.Resume();
                }
                internalStatus = OperationStatus.SUCCESS;
            }
            return sessionFunctions.Store.HandleOperationStatus(sessionFunctions.Ctx, ref operationState, internalStatus, out _);
        }

        /// <summary>
        /// Whether the key of <paramref name="srcLogRecord"/> has a record at or above <paramref name="maxAddress"/>, or determining
        /// that needs IO. A tombstone counts.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static bool HasRecordAtOrAbove<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(TSessionFunctionsWrapper sessionFunctions,
                in TSourceLogRecord srcLogRecord, long keyHash, long currentAddress, long maxAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(keyHash);
            OperationStatus internalStatus;
            bool needIO;
            do
            {
                if (sessionFunctions.Store.TryFindRecordInMainLogForConditionalOperation<TSourceLogRecord, TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, srcLogRecord, ref stackCtx,
                      currentAddress, minAddress: maxAddress, maxAddress: long.MaxValue, out internalStatus, out needIO))
                    return true;
            }
            while (sessionFunctions.Store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(internalStatus, sessionFunctions));
            return needIO;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static OperationStatus PrepareIOForConditionalScan<TInput, TOutput, TContext, TSourceLogRecord, TSessionFunctionsWrapper>(
                                        TSessionFunctionsWrapper sessionFunctions,
                                        ref TsavoriteKV<TStoreFunctions, TAllocator>.OperationState<TInput, TOutput, TContext> operationState,
                                        long keyHash, in TSourceLogRecord srcLogRecord,
                                        ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long originalAddress, long minAddress, long maxAddress, ScanCursorState scanCursorState)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            var store = sessionFunctions.Store;

            // PrepareIOForConditionalOperation rents the op and populates op.pendingState (type, keyHash, minAddress, maxAddress, requestKey, diskLogRecord).
            // After it returns, the op is on operationState.pendingOp; fill in the scan-only payload (scanCursorState) here.
            var status = store.PrepareIOForConditionalOperation(sessionFunctions, ref operationState, keyHash, in srcLogRecord, ref stackCtx, minAddress, maxAddress, OperationType.CONDITIONAL_SCAN_PUSH);
            operationState.pendingOp.pendingState.scanCursorState = scanCursorState;
            operationState.originalAddress = originalAddress;
            return status;
        }

        /// <summary>
        /// Scan page guaranteed to be in memory
        /// </summary>
        /// <param name="beginAddress">Begin address</param>
        /// <param name="endAddress">End address</param>
        /// <param name="observer">Observer of scan</param>
        internal abstract void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator> observer);
    }
}