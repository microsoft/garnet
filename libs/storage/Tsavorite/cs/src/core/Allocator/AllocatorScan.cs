// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    public abstract partial class AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> : IDisposable
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>
        /// Pull-based scan interface for HLOG; user calls GetNext() which advances through the address range.
        /// </summary>
        /// <returns>Pull Scan iterator instance</returns>
        public abstract ITsavoriteScanIterator<TKey, TValue> Scan(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool includeClosedRecords = false);

        /// <summary>
        /// Push-based scan interface for HLOG, called from LogAccessor; scan the log given address range, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal abstract bool Scan<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, long beginAddress, long endAddress, ref TScanFunctions scanFunctions,
                ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>;

        /// <summary>
        /// Push-based iteration of key versions, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref TKey key, ref TScanFunctions scanFunctions)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
        {
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(_storeFunctions.GetKeyHashCode64(ref key));
            if (!store.FindTag(ref stackCtx.hei))
                return false;
            stackCtx.SetRecordSourceToHashEntry(store.hlogBase);
            if (store.UseReadCache)
                store.SkipReadCache(ref stackCtx, out _);
            if (stackCtx.recSrc.LogicalAddress < store.hlogBase.BeginAddress)
                return false;
            return IterateKeyVersions(store, ref key, stackCtx.recSrc.LogicalAddress, ref scanFunctions);
        }

        /// <summary>
        /// Push-based iteration of key versions, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal abstract bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref TKey key, long beginAddress, ref TScanFunctions scanFunctions)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>;

        /// <summary>
        /// Implementation for push-scanning Tsavorite log
        /// </summary>
        internal bool PushScanImpl<TScanFunctions, TScanIterator>(long beginAddress, long endAddress, ref TScanFunctions scanFunctions, TScanIterator iter)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
            where TScanIterator : ITsavoriteScanIterator<TKey, TValue>, IPushScanIterator<TKey>
        {
            if (!scanFunctions.OnStart(beginAddress, endAddress))
                return false;
            var headAddress = HeadAddress;

            long numRecords = 1;
            var stop = false;
            for (; !stop && iter.GetNext(out var recordInfo); ++numRecords)
            {
                try
                {
                    // Pull Iter records are in temp storage so do not need locks, but we'll call ConcurrentReader because, for example, GenericAllocator
                    // may need to know the object is in that region.
                    if (recordInfo.IsClosed)    // Iterator checks this but it may have changed since
                        continue;
                    if (iter.CurrentAddress >= headAddress)
                        stop = !scanFunctions.ConcurrentReader(ref iter.GetKey(), ref iter.GetValue(), new RecordMetadata(recordInfo, iter.CurrentAddress), numRecords, out _);
                    else
                        stop = !scanFunctions.SingleReader(ref iter.GetKey(), ref iter.GetValue(), new RecordMetadata(recordInfo, iter.CurrentAddress), numRecords, out _);
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
        internal bool IterateKeyVersionsImpl<TScanFunctions, TScanIterator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ref TKey key, long beginAddress, ref TScanFunctions scanFunctions, TScanIterator iter)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
            where TScanIterator : ITsavoriteScanIterator<TKey, TValue>, IPushScanIterator<TKey>
        {
            if (!scanFunctions.OnStart(beginAddress, Constants.kInvalidAddress))
                return false;
            var headAddress = HeadAddress;

            long numRecords = 1;
            bool stop = false, continueOnDisk = false;
            for (; !stop && iter.BeginGetPrevInMemory(ref key, out var recordInfo, out continueOnDisk); ++numRecords)
            {
                OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = default;
                try
                {
                    // Iter records above headAddress will be in log memory and must be locked.
                    if (iter.CurrentAddress >= headAddress && !recordInfo.IsClosed)
                    {
                        store.LockForScan(ref stackCtx, ref key);
                        stop = !scanFunctions.ConcurrentReader(ref key, ref iter.GetValue(), new RecordMetadata(recordInfo, iter.CurrentAddress), numRecords, out _);
                    }
                    else
                        stop = !scanFunctions.SingleReader(ref key, ref iter.GetValue(), new RecordMetadata(recordInfo, iter.CurrentAddress), numRecords, out _);
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
                AsyncIOContextCompletionEvent<TKey, TValue> completionEvent = new();
                try
                {
                    var logicalAddress = iter.CurrentAddress;
                    while (!stop && GetFromDiskAndPushToReader(ref key, ref logicalAddress, ref scanFunctions, numRecords, completionEvent, out stop))
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

        internal unsafe bool GetFromDiskAndPushToReader<TScanFunctions>(ref TKey key, ref long logicalAddress, ref TScanFunctions scanFunctions, long numRecords,
                AsyncIOContextCompletionEvent<TKey, TValue> completionEvent, out bool stop)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
        {
            completionEvent.Prepare(_wrapper.GetKeyContainer(ref key), logicalAddress);

            AsyncGetFromDisk(logicalAddress, _wrapper.GetAverageRecordSize(), completionEvent.request);
            completionEvent.Wait();

            stop = false;
            if (completionEvent.exception is not null)
            {
                scanFunctions.OnException(completionEvent.exception, numRecords);
                return false;
            }
            if (completionEvent.request.logicalAddress < BeginAddress)
                return false;

            RecordInfo recordInfo = _wrapper.GetInfoFromBytePointer(completionEvent.request.record.GetValidPointer());
            recordInfo.ClearBitsForDiskImages();
            stop = !scanFunctions.SingleReader(ref key, ref _wrapper.GetContextRecordValue(ref completionEvent.request), new RecordMetadata(recordInfo, completionEvent.request.logicalAddress), numRecords, out _);
            logicalAddress = recordInfo.PreviousAddress;
            return !stop;
        }

        /// <summary>
        /// Push-based scan interface for HLOG with cursor, called from LogAccessor; scan the log from <paramref name="cursor"/> (which must be a valid address) and push up to <paramref name="count"/> records
        /// to the caller via <paramref name="scanFunctions"/> for each Key that is not found at a higher address.
        /// </summary>
        /// <returns>True if Scan completed and pushed <paramref name="count"/> records; false if Scan ended early due to finding less than <paramref name="count"/> records
        /// or one of the TScanIterator reader functions returning false</returns>
        /// <remarks>Currently we load an entire page, which while inefficient in performance, allows us to make the cursor safe (by ensuring we align to a valid record) if it is not
        /// the last one returned. We could optimize this to load only the subset of a page that is pointed to by the cursor and do GetRequiredRecordSize/RetrievedFullRecord as in
        /// AsyncGetFromDiskCallback. However, this would not validate the cursor and would therefore require maintaining a cursor history.</remarks>
        internal abstract bool ScanCursor<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ScanCursorState<TKey, TValue> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor, long maxAddress, bool resetCursor = true, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>;

        private protected bool ScanLookup<TInput, TOutput, TScanFunctions, TScanIterator>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
                ScanCursorState<TKey, TValue> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, TScanIterator iter, bool validateCursor, long maxAddress, bool resetCursor = true, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
            where TScanIterator : ITsavoriteScanIterator<TKey, TValue>, IPushScanIterator<TKey>
        {
            using var session = store.NewSession<TInput, TOutput, Empty, LogScanCursorFunctions<TInput, TOutput>>(new LogScanCursorFunctions<TInput, TOutput>());
            var bContext = session.BasicContext;

            if (cursor < BeginAddress) // This includes 0, which means to start the Scan
                cursor = BeginAddress;
            else if (validateCursor)
                iter.SnapCursorToLogicalAddress(ref cursor);

            if (!scanFunctions.OnStart(cursor, iter.EndAddress))
                return false;

            if (cursor >= GetTailAddress())
                goto IterationComplete;

            scanCursorState.Initialize(scanFunctions);

            long numPending = 0;
            while (iter.GetNext(out var recordInfo))
            {
                if (!recordInfo.Tombstone || includeTombstones)
                {
                    ref var key = ref iter.GetKey();
                    ref var value = ref iter.GetValue();
                    var status = bContext.ConditionalScanPush(scanCursorState, recordInfo, ref key, ref value, iter.CurrentAddress, iter.NextAddress, maxAddress);
                    if (status.IsPending)
                    {
                        ++numPending;
                        if (numPending == count - scanCursorState.acceptedCount || numPending > 256)
                        {
                            bContext.CompletePending(wait: true);
                            numPending = 0;
                        }
                    }
                }

                // Update the cursor to point to the next record.
                if (scanCursorState.retryLastRecord)
                    cursor = iter.CurrentAddress;
                else
                    cursor = iter.NextAddress;

                // Now see if we completed the enumeration.
                if (scanCursorState.stop)
                    goto IterationComplete;
                if (scanCursorState.acceptedCount >= count || scanCursorState.endBatch)
                {
                    scanFunctions.OnStop(true, scanCursorState.acceptedCount);
                    return true;
                }
            }

            // Drain any pending pushes. We have ended the iteration; we know there are no more matching records, so drop through to end it and return false.
            if (numPending > 0)
                bContext.CompletePending(wait: true);

            IterationComplete:
            if (resetCursor) cursor = 0;
            scanFunctions.OnStop(false, scanCursorState.acceptedCount);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions, ScanCursorState<TKey, TValue> scanCursorState, RecordInfo recordInfo,
                ref TKey key, ref TValue value, long currentAddress, long minAddress, long maxAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is called only from ScanLookup so the epoch should be protected");
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.PendingContext<TInput, TOutput, TContext> pendingContext = new(_storeFunctions.GetKeyHashCode64(ref key));

            OperationStatus internalStatus;
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(pendingContext.keyHash);
            bool needIO;
            do
            {
                // If a more recent version of the record exists, do not push this one. Start by searching in-memory.
                if (sessionFunctions.Store.TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx, currentAddress, minAddress, maxAddress, out internalStatus, out needIO))
                    return Status.CreateFound();
            }
            while (sessionFunctions.Store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(internalStatus, sessionFunctions));

            TInput input = default;
            TOutput output = default;
            if (needIO)
            {
                // A more recent version of the key was not (yet) found and we need another IO to continue searching.
                internalStatus = PrepareIOForConditionalScan(sessionFunctions, ref pendingContext, ref key, ref input, ref value, ref output, default,
                                ref stackCtx, minAddress, maxAddress, scanCursorState);
            }
            else
            {
                // A more recent version of the key was not found. recSrc.LogicalAddress is the correct address, because minAddress was examined
                // and this is the previous record in the tag chain. Push this record to the user.
                epoch.Suspend();
                try
                {
                    RecordMetadata recordMetadata = new(recordInfo, stackCtx.recSrc.LogicalAddress);
                    var stop = (stackCtx.recSrc.LogicalAddress >= HeadAddress)
                        ? !scanCursorState.functions.ConcurrentReader(ref key, ref value, recordMetadata, scanCursorState.acceptedCount, out var cursorRecordResult)
                        : !scanCursorState.functions.SingleReader(ref key, ref value, recordMetadata, scanCursorState.acceptedCount, out cursorRecordResult);
                    if (stop)
                        scanCursorState.stop = true;
                    else
                    {
                        if ((cursorRecordResult & CursorRecordResult.Accept) != 0)
                            Interlocked.Increment(ref scanCursorState.acceptedCount);
                        if ((cursorRecordResult & CursorRecordResult.EndBatch) != 0)
                            scanCursorState.endBatch = true;
                        if ((cursorRecordResult & CursorRecordResult.RetryLastRecord) != 0)
                            scanCursorState.retryLastRecord = true;
                    }
                }
                finally
                {
                    epoch.Resume();
                }
                internalStatus = OperationStatus.SUCCESS;
            }
            return sessionFunctions.Store.HandleOperationStatus(sessionFunctions.Ctx, ref pendingContext, internalStatus, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static OperationStatus PrepareIOForConditionalScan<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                                        ref TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.PendingContext<TInput, TOutput, TContext> pendingContext,
                                        ref TKey key, ref TInput input, ref TValue value, ref TOutput output, TContext userContext,
                                        ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx, long minAddress, long maxAddress, ScanCursorState<TKey, TValue> scanCursorState)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            // WriteReason is not surfaced for this operation, so pick anything.
            var status = sessionFunctions.Store.PrepareIOForConditionalOperation(sessionFunctions, ref pendingContext, ref key, ref input, ref value, ref output,
                    userContext, ref stackCtx, minAddress, maxAddress, WriteReason.Compaction, OperationType.CONDITIONAL_SCAN_PUSH);
            pendingContext.scanCursorState = scanCursorState;
            return status;
        }

        internal struct LogScanCursorFunctions<TInput, TOutput> : ISessionFunctions<TKey, TValue, TInput, TOutput, Empty>
        {
            public bool SingleReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo) => true;
            public bool ConcurrentReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst, ref ReadInfo readInfo, ref RecordInfo recordInfo) => true;
            public void ReadCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

            public bool SingleDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;
            public void PostSingleDeleter(ref TKey key, ref DeleteInfo deleteInfo) { }
            public bool ConcurrentDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

            public bool SingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo) => true;
            public void PostSingleWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, WriteReason reason) { }
            public bool ConcurrentWriter(ref TKey key, ref TInput input, ref TValue src, ref TValue dst, ref TOutput output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo) => true;

            public bool InPlaceUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;

            public bool NeedCopyUpdate(ref TKey key, ref TInput input, ref TValue oldValue, ref TOutput output, ref RMWInfo rmwInfo) => true;
            public bool CopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
            public bool PostCopyUpdater(ref TKey key, ref TInput input, ref TValue oldValue, ref TValue newValue, ref TOutput output, ref RMWInfo rmwInfo) => true;

            public bool NeedInitialUpdate(ref TKey key, ref TInput input, ref TOutput output, ref RMWInfo rmwInfo) => true;
            public bool InitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
            public void PostInitialUpdater(ref TKey key, ref TInput input, ref TValue value, ref TOutput output, ref RMWInfo rmwInfo) { }

            public void RMWCompletionCallback(ref TKey key, ref TInput input, ref TOutput output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

            public int GetRMWModifiedValueLength(ref TValue value, ref TInput input) => 0;
            public int GetRMWInitialValueLength(ref TInput input) => 0;
            public int GetUpsertValueLength(ref TValue value, ref TInput input) => 0;

            public void ConvertOutputToHeap(ref TInput input, ref TOutput output) { }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SnapToFixedLengthLogicalAddressBoundary(ref long logicalAddress, int recordSize)
        {
            // Get the initial offset on the page
            int offset = (int)(logicalAddress & PageSizeMask);
            long pageStart = logicalAddress - offset;

            int recordStartOffset;
            if (logicalAddress < PageSize)
            {
                // We are on the first page so must account for BeginAddress.
                if (offset < BeginAddress)
                    return logicalAddress = BeginAddress;
                recordStartOffset = (int)(((offset - BeginAddress) / recordSize) * recordSize + BeginAddress);
            }
            else
            {
                // Not the first page, so just find the highest recordStartOffset <= offset.
                recordStartOffset = (offset / recordSize) * recordSize;
            }

            // If there is not enough room for a full record, advance logicalAddress to the next page start.
            if (PageSize - recordStartOffset >= recordSize)
                logicalAddress = pageStart + recordStartOffset;
            else
                logicalAddress = pageStart + PageSize;
            return logicalAddress;
        }

        /// <summary>
        /// Scan page guaranteed to be in memory
        /// </summary>
        /// <param name="beginAddress">Begin address</param>
        /// <param name="endAddress">End address</param>
        /// <param name="observer">Observer of scan</param>
        internal abstract void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator<TKey, TValue>> observer);
    }
}