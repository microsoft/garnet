// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    internal sealed class ScanCursorState<Key, Value>
    {
        internal IScanIteratorFunctions<Key, Value> functions;
        internal long acceptedCount;    // Number of records pushed to and accepted by the caller
        internal bool endBatch;         // End the batch (but return a valid cursor for the next batch, as of "count" records had been returned)
        internal bool stop;             // Stop the operation (as if all records in the db had been returned)

        internal void Initialize(IScanIteratorFunctions<Key, Value> scanIteratorFunctions)
        {
            functions = scanIteratorFunctions;
            acceptedCount = 0;
            endBatch = false;
            stop = false;
        }
    }

    internal abstract partial class AllocatorBase<Key, Value>
    {
        /// <summary>
        /// Pull-based scan interface for HLOG; user calls GetNext() which advances through the address range.
        /// </summary>
        /// <returns>Pull Scan iterator instance</returns>
        public abstract ITsavoriteScanIterator<Key, Value> Scan(TsavoriteKV<Key, Value> store, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering);

        /// <summary>
        /// Push-based scan interface for HLOG, called from LogAccessor; scan the log given address range, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal abstract bool Scan<TScanFunctions>(TsavoriteKV<Key, Value> store, long beginAddress, long endAddress, ref TScanFunctions scanFunctions,
                ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>;

        /// <summary>
        /// Push-based iteration of key versions, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<Key, Value> store, ref Key key, ref TScanFunctions scanFunctions)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
        {
            OperationStackContext<Key, Value> stackCtx = new(store.comparer.GetHashCode64(ref key));
            if (!store.FindTag(ref stackCtx.hei))
                return false;
            stackCtx.SetRecordSourceToHashEntry(store.hlog);
            if (store.UseReadCache)
                store.SkipReadCache(ref stackCtx, out _);
            if (stackCtx.recSrc.LogicalAddress < store.hlog.BeginAddress)
                return false;
            return IterateKeyVersions(store, ref key, stackCtx.recSrc.LogicalAddress, ref scanFunctions);
        }

        /// <summary>
        /// Push-based iteration of key versions, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal abstract bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<Key, Value> store, ref Key key, long beginAddress, ref TScanFunctions scanFunctions)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>;

        /// <summary>
        /// Implementation for push-scanning Tsavorite log
        /// </summary>
        internal bool PushScanImpl<TScanFunctions, TScanIterator>(long beginAddress, long endAddress, ref TScanFunctions scanFunctions, TScanIterator iter)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
            where TScanIterator : ITsavoriteScanIterator<Key, Value>, IPushScanIterator<Key>
        {
            if (!scanFunctions.OnStart(beginAddress, endAddress))
                return false;
            var headAddress = HeadAddress;

            long numRecords = 1;
            bool stop = false;
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
        internal bool IterateKeyVersionsImpl<TScanFunctions, TScanIterator>(TsavoriteKV<Key, Value> store, ref Key key, long beginAddress, ref TScanFunctions scanFunctions, TScanIterator iter)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
            where TScanIterator : ITsavoriteScanIterator<Key, Value>, IPushScanIterator<Key>
        {
            if (!scanFunctions.OnStart(beginAddress, Constants.kInvalidAddress))
                return false;
            var headAddress = HeadAddress;

            long numRecords = 1;
            bool stop = false, continueOnDisk = false;
            for (; !stop && iter.BeginGetPrevInMemory(ref key, out var recordInfo, out continueOnDisk); ++numRecords)
            {
                OperationStackContext<Key, Value> stackCtx = default;
                try
                {
                    // Iter records above headAddress will be in log memory and must be locked.
                    if (iter.CurrentAddress >= headAddress && !recordInfo.IsClosed)
                    {
                        store.LockForScan(ref stackCtx, ref key, ref iter.GetLockableInfo());
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
                        store.UnlockForScan(ref stackCtx, ref iter.GetKey(), ref iter.GetLockableInfo());
                    iter.EndGetPrevInMemory();
                }
            }

            if (continueOnDisk)
            {
                AsyncIOContextCompletionEvent<Key, Value> completionEvent = new();
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

        internal unsafe bool GetFromDiskAndPushToReader<TScanFunctions>(ref Key key, ref long logicalAddress, ref TScanFunctions scanFunctions, long numRecords,
                AsyncIOContextCompletionEvent<Key, Value> completionEvent, out bool stop)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
        {
            completionEvent.Prepare(GetKeyContainer(ref key), logicalAddress);

            AsyncGetFromDisk(logicalAddress, GetAverageRecordSize(), completionEvent.request);
            completionEvent.Wait();

            stop = false;
            if (completionEvent.exception is not null)
            {
                scanFunctions.OnException(completionEvent.exception, numRecords);
                return false;
            }
            if (completionEvent.request.logicalAddress < BeginAddress)
                return false;

            RecordInfo recordInfo = GetInfoFromBytePointer(completionEvent.request.record.GetValidPointer());
            recordInfo.ClearBitsForDiskImages();
            stop = !scanFunctions.SingleReader(ref key, ref GetContextRecordValue(ref completionEvent.request), new RecordMetadata(recordInfo, completionEvent.request.logicalAddress), numRecords, out _);
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
        internal abstract bool ScanCursor<TScanFunctions>(TsavoriteKV<Key, Value> store, ScanCursorState<Key, Value> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, long endAddress, bool validateCursor)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>;

        protected bool ScanLookup<TInput, TOutput, TScanFunctions, TScanIterator>(TsavoriteKV<Key, Value> store, ScanCursorState<Key, Value> scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, TScanIterator iter, bool validateCursor)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
            where TScanIterator : ITsavoriteScanIterator<Key, Value>, IPushScanIterator<Key>
        {
            using var session = store.NewSession<TInput, TOutput, Empty, LogScanCursorFunctions<TInput, TOutput>>(new LogScanCursorFunctions<TInput, TOutput>());

            if (cursor >= GetTailAddress())
                goto IterationComplete;

            if (cursor < BeginAddress) // This includes 0, which means to start the Scan
                cursor = BeginAddress;
            else if (validateCursor)
                iter.SnapCursorToLogicalAddress(ref cursor);

            scanCursorState.Initialize(scanFunctions);

            long numPending = 0;
            while (iter.GetNext(out var recordInfo))
            {
                if (!recordInfo.Tombstone)
                {
                    ref var key = ref iter.GetKey();
                    ref var value = ref iter.GetValue();
                    var status = session.ConditionalScanPush(scanCursorState, recordInfo, ref key, ref value, iter.NextAddress);
                    if (status.IsPending)
                    {
                        ++numPending;
                        if (numPending == count - scanCursorState.acceptedCount || numPending > 256)
                        {
                            session.CompletePending(wait: true);
                            numPending = 0;
                        }
                    }
                }

                // Update the cursor to point to the next record.
                cursor = iter.NextAddress;

                // Now see if we completed the enumeration.
                if (scanCursorState.stop)
                    goto IterationComplete;
                if (scanCursorState.acceptedCount >= count || scanCursorState.endBatch)
                    return true;
            }

            // Drain any pending pushes. We have ended the iteration; there are no more records, so drop through to end it.
            if (numPending > 0)
                session.CompletePending(wait: true);

            IterationComplete:
            cursor = 0;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ConditionalScanPush<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession, ScanCursorState<Key, Value> scanCursorState, RecordInfo recordInfo, ref Key key, ref Value value, long minAddress)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is called only from ScanLookup so the epoch should be protected");
            TsavoriteKV<Key, Value>.PendingContext<Input, Output, Context> pendingContext = new(comparer.GetHashCode64(ref key));

            OperationStatus internalStatus;
            OperationStackContext<Key, Value> stackCtx = new(pendingContext.keyHash);
            bool needIO;
            do
            {
                // If a more recent version of the record exists, do not push this one. Start by searching in-memory.
                if (tsavoriteSession.Store.TryFindRecordInMainLogForConditionalOperation<Input, Output, Context, TsavoriteSession>(tsavoriteSession, ref key, ref stackCtx, minAddress, out internalStatus, out needIO))
                    return Status.CreateFound();
            }
            while (tsavoriteSession.Store.HandleImmediateNonPendingRetryStatus<Input, Output, Context, TsavoriteSession>(internalStatus, tsavoriteSession));

            Input input = default;
            Output output = default;
            if (needIO)
            {
                // A more recent version of the key was not (yet) found and we need another IO to continue searching.
                internalStatus = PrepareIOForConditionalScan(tsavoriteSession, ref pendingContext, ref key, ref input, ref value, ref output, default, 0L,
                                ref stackCtx, minAddress, scanCursorState);
            }
            else
            {
                // A more recent version of the key was not found. recSrc.LogicalAddress is the correct address, because minAddress was examined
                // and this is the previous record in the tag chain. Push this record to the user.
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
                }
                internalStatus = OperationStatus.SUCCESS;
            }
            return tsavoriteSession.Store.HandleOperationStatus(tsavoriteSession.Ctx, ref pendingContext, internalStatus, out _);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus PrepareIOForConditionalScan<Input, Output, Context, TsavoriteSession>(TsavoriteSession tsavoriteSession,
                                        ref TsavoriteKV<Key, Value>.PendingContext<Input, Output, Context> pendingContext,
                                        ref Key key, ref Input input, ref Value value, ref Output output, Context userContext, long lsn,
                                        ref OperationStackContext<Key, Value> stackCtx, long minAddress, ScanCursorState<Key, Value> scanCursorState)
            where TsavoriteSession : ITsavoriteSession<Key, Value, Input, Output, Context>
        {
            // WriteReason is not surfaced for this operation, so pick anything.
            var status = tsavoriteSession.Store.PrepareIOForConditionalOperation(tsavoriteSession, ref pendingContext, ref key, ref input, ref value, ref output,
                    userContext, lsn, ref stackCtx, minAddress, WriteReason.Compaction, OperationType.CONDITIONAL_SCAN_PUSH);
            pendingContext.scanCursorState = scanCursorState;
            return status;
        }

        internal struct LogScanCursorFunctions<Input, Output> : IFunctions<Key, Value, Input, Output, Empty>
        {
            public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo) => true;
            public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref ReadInfo readInfo, ref RecordInfo recordInfo) => true;
            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

            public bool SingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;
            public void PostSingleDeleter(ref Key key, ref DeleteInfo deleteInfo) { }
            public bool ConcurrentDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo, ref RecordInfo recordInfo) => true;

            public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason, ref RecordInfo recordInfo) => true;
            public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }
            public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, ref RecordInfo recordInfo) => true;

            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;

            public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo) => true;
            public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
            public void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }

            public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo) => true;
            public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo, ref RecordInfo recordInfo) => true;
            public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }

            public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata) { }

            public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint) { }

            public int GetRMWModifiedValueLength(ref Value value, ref Input input) => 0;
            public int GetRMWInitialValueLength(ref Input input) => 0;

            public void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref UpsertInfo upsertInfo, WriteReason reason) { }
            public void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RMWInfo rmwInfo) { }
            public void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RMWInfo rmwInfo) { }
            public void DisposeSingleDeleter(ref Key key, ref Value value, ref DeleteInfo deleteInfo) { }
            public void DisposeDeserializedFromDisk(ref Key key, ref Value value) { }
            public void DisposeForRevivification(ref Key key, ref Value value, int keySize) { }
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
        internal abstract void MemoryPageScan(long beginAddress, long endAddress, IObserver<ITsavoriteScanIterator<Key, Value>> observer);
    }
}