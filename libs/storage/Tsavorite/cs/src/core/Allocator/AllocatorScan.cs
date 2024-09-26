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
        /// Push-based scan interface for HLOG, called from LogAccessor; scan the log given address range, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal abstract bool Scan<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
                UnsafeContext<TKey, TValue, Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>, TStoreFunctions, TAllocator> uContext,
                TScanFunctions scanFunctions, long beginAddress, long endAddress, ScanCursorState<TKey, TValue> scanCursorState,
                ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool includeSealedRecords = false)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>;
 
        internal abstract bool Iterate<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
                UnsafeContext<TKey, TValue, Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>, TStoreFunctions, TAllocator> uContext,
                TScanFunctions scanFunctions, long untilAddress, ScanCursorState<TKey, TValue> scanCursorState)
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
        /// Implementation for push-iterating key versions
        /// </summary>
        /// <returns>true if scan completed, else false if scanFunctions terminated early</returns>
        internal bool IterateKeyVersionsImpl<TScanFunctions, TRecordScanner>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, TScanFunctions scanFunctions, ref TKey key, long beginAddress, TRecordScanner scanner)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
            where TRecordScanner : IRecordScanner<TKey, TValue>
        {
            if (!scanFunctions.OnStart(beginAddress, Constants.kInvalidAddress))
                return false;
            var headAddress = HeadAddress;

            long numRecords = 0;
            bool continueOnDisk = false, stop = false;
            while (scanner.GetPrevInMemory(ref key, scanFunctions, ref numRecords, out continueOnDisk, out stop))
                ;

            if (!stop && continueOnDisk)
            {
                AsyncIOContextCompletionEvent<TKey, TValue> completionEvent = new();
                try
                {
                    var logicalAddress = scanner.CurrentAddress;
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
        internal abstract bool IterateCursor<TScanFunctions>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store, ScanCursorState<TKey, TValue> scanCursorState, TScanFunctions scanFunctions,
                ref long cursor, long count, long endAddress, bool validateCursor)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>;

        private protected bool IterateCursor<TInput, TOutput, TRecordScanner>(TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> store,
                ScanCursorState<TKey, TValue> scanCursorState, ref long cursor, long count, TRecordScanner scanner, bool validateCursor)
            where TRecordScanner : IRecordScanner<TKey, TValue>
        {
            using var session = store.NewSession<Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>>(new LivenessCheckingSessionFunctions<TKey, TValue>());
            var bContext = session.BasicContext;

            if (cursor >= GetTailAddress())
                goto IterationComplete;

            if (cursor < BeginAddress) // This includes 0, which means to start the Scan
                cursor = BeginAddress;
            else if (validateCursor)
                _ = scanner.SnapCursorToLogicalAddress(ref cursor);

            scanCursorState.Initialize();

            var numPending = 0;
            while (scanner.IterateNext(out var status))
            {
                if (status.IsPending)
                {
                    ++numPending;
                    if (numPending == count - scanCursorState.acceptedCount || numPending == 256)
                    {
                        _ = bContext.CompletePending(wait: true);
                        numPending = 0;
                    }
                }

                // Update the cursor to point to the next record.
                cursor = scanner.NextAddress;

                // Now see if we completed the enumeration.
                if (scanCursorState.stop)
                    goto IterationComplete;
                if (scanCursorState.acceptedCount >= count || scanCursorState.endBatch)
                    return true;
            }

            // Drain any pending pushes. We have ended the iteration; there are no more records, so drop through to end it.
            if (numPending > 0)
                _ = bContext.CompletePending(wait: true);

            IterationComplete:
            cursor = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper, TScanFunctions>(TSessionFunctionsWrapper sessionFunctions, ScanCursorState<TKey, TValue> scanCursorState, 
                TScanFunctions scanFunctions, RecordInfo recordInfo, ref TKey key, ref TValue value, long minAddress)
            where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.PendingContext<TInput, TOutput, TContext> pendingContext = new(_storeFunctions.GetKeyHashCode64(ref key));
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(pendingContext.keyHash);
            OperationStatus internalStatus;
            if (RecordLivenessLookupInLog<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref key, minAddress, out var needIO))
                internalStatus = OperationStatus.SUCCESS;
            else if (needIO)
            {
                // A more recent version of the key was not (yet) found and we need another IO to continue searching.
                pendingContext.scanCursorState = scanCursorState;
                internalStatus = sessionFunctions.Store.PrepareIOForConditionalOperation(ref pendingContext, ref key, ref value,
                        ref stackCtx, minAddress, WriteReason.Iteration, OperationType.CONDITIONAL_SCAN_PUSH);
            }
            else
            {
                // A more recent version of the key was not found. recSrc.LogicalAddress is the correct address, because minAddress was examined
                // and this is the previous record in the tag chain. Push this record to the user.
                RecordMetadata recordMetadata = new(recordInfo, stackCtx.recSrc.LogicalAddress);
                var stop = (stackCtx.recSrc.LogicalAddress >= HeadAddress)
                    ? !scanFunctions.ConcurrentReader(ref key, ref value, recordMetadata, scanCursorState.acceptedCount, out var cursorRecordResult)
                    : !scanFunctions.SingleReader(ref key, ref value, recordMetadata, scanCursorState.acceptedCount, out cursorRecordResult);
                if (stop)
                    scanCursorState.stop = true;
                else
                {
                    if ((cursorRecordResult & CursorRecordResult.Accept) != 0)
                        _ = Interlocked.Increment(ref scanCursorState.acceptedCount);
                    if ((cursorRecordResult & CursorRecordResult.EndBatch) != 0)
                        scanCursorState.endBatch = true;
                }
                internalStatus = OperationStatus.SUCCESS;
            }
            return sessionFunctions.Store.HandleOperationStatus(sessionFunctions.Ctx, ref pendingContext, internalStatus, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status IterationRecordLivenessCheck<TInput, TOutput, TContext, TSessionFunctionsWrapper, TScanFunctions>(TSessionFunctionsWrapper sessionFunctions,
                ref TKey key, ref TValue value, long minAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator>.PendingContext<TInput, TOutput, TContext> pendingContext = new(_storeFunctions.GetKeyHashCode64(ref key));
            OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx = new(pendingContext.keyHash);
            OperationStatus internalStatus;
            if (RecordLivenessLookupInLog<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref stackCtx, ref key, minAddress, out var needIO))
                internalStatus = OperationStatus.SUCCESS;
            else if (needIO)
            {
                // A more recent version of the key was not (yet) found and we need another IO to continue searching.
                internalStatus = sessionFunctions.Store.PrepareIOForConditionalOperation(ref pendingContext, ref key, ref value,
                        ref stackCtx, minAddress, WriteReason.Iteration, OperationType.ITERATION_LIVENESS_CHECK);
            }
            else
                internalStatus = OperationStatus.NOTFOUND;
            return sessionFunctions.Store.HandleOperationStatus(sessionFunctions.Ctx, ref pendingContext, internalStatus, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool RecordLivenessLookupInLog<TInput, TOutput, TContext, TSessionFunctionsWrapper>(TSessionFunctionsWrapper sessionFunctions,
                ref OperationStackContext<TKey, TValue, TStoreFunctions, TAllocator> stackCtx,
                ref TKey key, long minAddress, out bool needIO)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TKey, TValue, TInput, TOutput, TContext, TStoreFunctions, TAllocator>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is called only from ScanLookup variants, so the epoch should be protected");
            OperationStatus internalStatus;
            do
            {
                // If a more recent version of the record exists, do not push this one. Start by searching in-memory.
                if (sessionFunctions.Store.TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, ref key, ref stackCtx, minAddress, out internalStatus, out needIO))
                    return true;
            }
            while (sessionFunctions.Store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(internalStatus, sessionFunctions));
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SnapToFixedLengthLogicalAddressBoundary(ref long logicalAddress, int recordSize)
        {
            // Get the initial offset on the page
            var offset = (int)(logicalAddress & PageSizeMask);
            var pageStart = logicalAddress - offset;

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
        internal abstract void MemoryPageScan(long beginAddress, long endAddress, IObserver<IRecordScanner<TKey, TValue>> observer);
    }

    // No-op ISessionFunctions implementation for use in log scanning (they don't do anything because we are only reading for liveness; we have the data in PendingContext).
    internal struct LivenessCheckingSessionFunctions<TKey, TValue> : ISessionFunctions<TKey, TValue, Empty, Empty, Empty>
    {
        public readonly bool SingleReader(ref TKey key, ref Empty input, ref TValue value, ref Empty dst, ref ReadInfo readInfo) => true;
        public readonly bool ConcurrentReader(ref TKey key, ref Empty input, ref TValue value, ref Empty dst, ref ReadInfo readInfo, ref RecordInfo recordInfo) => true;
        public readonly void ReadCompletionCallback(ref TKey key, ref Empty input, ref Empty output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

        public readonly bool SingleDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;
        public readonly void PostSingleDeleter(ref TKey key, ref DeleteInfo deleteInfo) { }
        public readonly bool ConcurrentDeleter(ref TKey key, ref TValue value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

        public readonly bool SingleWriter(ref TKey key, ref Empty input, ref TValue src, ref TValue dst, ref Empty output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo) => true;
        public readonly void PostSingleWriter(ref TKey key, ref Empty input, ref TValue src, ref TValue dst, ref Empty output, ref UpsertInfo upsertInfo, WriteReason reason) { }
        public readonly bool ConcurrentWriter(ref TKey key, ref Empty input, ref TValue src, ref TValue dst, ref Empty output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo) => true;

        public readonly bool InPlaceUpdater(ref TKey key, ref Empty input, ref TValue value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;

        public readonly bool NeedCopyUpdate(ref TKey key, ref Empty input, ref TValue oldValue, ref Empty output, ref RMWInfo rmwInfo) => true;
        public readonly bool CopyUpdater(ref TKey key, ref Empty input, ref TValue oldValue, ref TValue newValue, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
        public readonly bool PostCopyUpdater(ref TKey key, ref Empty input, ref TValue oldValue, ref TValue newValue, ref Empty output, ref RMWInfo rmwInfo) => true;

        public readonly bool NeedInitialUpdate(ref TKey key, ref Empty input, ref Empty output, ref RMWInfo rmwInfo) => true;
        public readonly bool InitialUpdater(ref TKey key, ref Empty input, ref TValue value, ref Empty output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
        public readonly void PostInitialUpdater(ref TKey key, ref Empty input, ref TValue value, ref Empty output, ref RMWInfo rmwInfo) { }

        public readonly void RMWCompletionCallback(ref TKey key, ref Empty input, ref Empty output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

        public readonly int GetRMWModifiedValueLength(ref TValue value, ref Empty input) => 0;
        public readonly int GetRMWInitialValueLength(ref Empty input) => 0;

        public readonly void ConvertOutputToHeap(ref Empty input, ref Empty output) { }
    }

    class DummyScanFunctions<TKey, TValue> : IScanIteratorFunctions<TKey, TValue>
    {
        public bool SingleReader(ref TKey key, ref TValue value, RecordMetadata recordMetadata, long numberOfRecords, out CursorRecordResult cursorRecordResult) 
            => throw new NotImplementedException("Methods on this class should not be called");
    }
}