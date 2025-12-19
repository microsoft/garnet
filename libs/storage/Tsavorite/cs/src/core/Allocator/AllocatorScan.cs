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
        internal bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TStoreFunctions, TAllocator> store, ReadOnlySpan<byte> key, ref TScanFunctions scanFunctions)
            where TScanFunctions : IScanIteratorFunctions
        {
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(storeFunctions.GetKeyHashCode64(key));
            if (!store.FindTag(ref stackCtx.hei))
                return false;
            stackCtx.SetRecordSourceToHashEntry(store.hlogBase);
            if (store.UseReadCache)
                store.SkipReadCache(ref stackCtx, out _);
            if (stackCtx.recSrc.LogicalAddress < store.hlogBase.BeginAddress)
                return false;
            return IterateKeyVersions(store, key, stackCtx.recSrc.LogicalAddress, ref scanFunctions);
        }

        /// <summary>
        /// Push-based iteration of key versions, calling <paramref name="scanFunctions"/> for each record.
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        internal abstract bool IterateKeyVersions<TScanFunctions>(TsavoriteKV<TStoreFunctions, TAllocator> store, ReadOnlySpan<byte> key, long beginAddress, ref TScanFunctions scanFunctions)
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
                    stop = !scanFunctions.Reader(in iter, new RecordMetadata(iter.CurrentAddress, iter.ETag), numRecords, out _);
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
        internal bool IterateHashChain<TScanFunctions, TScanIterator>(TsavoriteKV<TStoreFunctions, TAllocator> store, ReadOnlySpan<byte> key, long beginAddress, ref TScanFunctions scanFunctions, TScanIterator iter)
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
                    stop = !scanFunctions.Reader(in logRecord, new RecordMetadata(iter.CurrentAddress, iter.ETag), numRecords, out _);
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

        internal unsafe bool GetFromDiskAndPushToReader<TScanFunctions>(ReadOnlySpan<byte> key, ref long logicalAddress, ref TScanFunctions scanFunctions, long numRecords,
                AsyncIOContextCompletionEvent completionEvent, out bool stop)
            where TScanFunctions : IScanIteratorFunctions
        {
            completionEvent.Prepare(PinnedSpanByte.FromPinnedSpan(key), logicalAddress);

            AsyncGetFromDisk(logicalAddress, IStreamBuffer.InitialIOSize, completionEvent.request);
            completionEvent.Wait();

            stop = false;
            if (completionEvent.exception is not null)
            {
                scanFunctions.OnException(completionEvent.exception, numRecords);
                return false;
            }
            if (completionEvent.request.logicalAddress < BeginAddress)
                return false;

            var logRecord = DiskLogRecord.TransferFrom(ref completionEvent.request.record, transientObjectIdMap);
            logRecord.InfoRef.ClearBitsForDiskImages();
            stop = !scanFunctions.Reader(in logRecord, new RecordMetadata(completionEvent.request.logicalAddress, logRecord.ETag), numRecords, out _);
            logicalAddress = logRecord.Info.PreviousAddress;
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
        /// AsyncGetFromDiskCallback. However, this would not validate the cursor and would therefore require maintaining a cursor history.</remarks>
        internal abstract bool ScanCursor<TScanFunctions>(TsavoriteKV<TStoreFunctions, TAllocator> store, ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions,
                long endAddress, bool validateCursor, long maxAddress, bool resetCursor = true, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions;

        private protected bool ScanLookup<TInput, TOutput, TScanFunctions, TScanIterator>(TsavoriteKV<TStoreFunctions, TAllocator> store,
                ScanCursorState scanCursorState, ref long cursor, long count, TScanFunctions scanFunctions, TScanIterator iter, bool validateCursor, long maxAddress,
                bool resetCursor = true, bool includeTombstones = false)
            where TScanFunctions : IScanIteratorFunctions
            where TScanIterator : ITsavoriteScanIterator, IPushScanIterator
        {
            using var session = store.NewSession<TInput, TOutput, Empty, NoOpSessionFunctions<TInput, TOutput, Empty>>(new NoOpSessionFunctions<TInput, TOutput, Empty>());
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
                _ = bContext.CompletePending(wait: true);

            IterationComplete:
            if (resetCursor)
                cursor = 0;
            scanFunctions.OnStop(false, scanCursorState.acceptedCount);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ConditionalScanPush<TInput, TOutput, TContext, TSessionFunctionsWrapper, TSourceLogRecord>(TSessionFunctionsWrapper sessionFunctions,
                ScanCursorState scanCursorState, in TSourceLogRecord srcLogRecord, long currentAddress, long minAddress, long maxAddress)
            where TSessionFunctionsWrapper : ISessionFunctionsWrapper<TInput, TOutput, TContext, TStoreFunctions, TAllocator>
            where TSourceLogRecord : ISourceLogRecord
        {
            Debug.Assert(epoch.ThisInstanceProtected(), "This is called only from ScanLookup so the epoch should be protected");
            var pendingContext = new TsavoriteKV<TStoreFunctions, TAllocator>.PendingContext<TInput, TOutput, TContext>(storeFunctions.GetKeyHashCode64(srcLogRecord.Key));

            OperationStatus internalStatus;
            OperationStackContext<TStoreFunctions, TAllocator> stackCtx = new(pendingContext.keyHash);
            bool needIO;
            do
            {
                // If a more recent version of the record exists, do not push this one. Start by searching in-memory.
                if (sessionFunctions.Store.TryFindRecordInMainLogForConditionalOperation<TInput, TOutput, TContext, TSessionFunctionsWrapper>(sessionFunctions, srcLogRecord.Key, ref stackCtx,
                        currentAddress, minAddress, maxAddress, out internalStatus, out needIO))
                    return Status.CreateFound();
            }
            while (sessionFunctions.Store.HandleImmediateNonPendingRetryStatus<TInput, TOutput, TContext, TSessionFunctionsWrapper>(internalStatus, sessionFunctions));

            if (needIO)
            {
                // A more recent version of the key was not (yet) found and we need another IO to continue searching.
                internalStatus = PrepareIOForConditionalScan(sessionFunctions.Store, ref pendingContext, in srcLogRecord, ref stackCtx, minAddress, maxAddress, scanCursorState);
            }
            else
            {
                // A more recent version of the key was not found. recSrc.LogicalAddress is the correct address, because minAddress was examined
                // and this is the previous record in the tag chain. Push this record to the user.
                epoch.Suspend();
                try
                {
                    RecordMetadata recordMetadata = new(stackCtx.recSrc.LogicalAddress, srcLogRecord.ETag);
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
        internal static OperationStatus PrepareIOForConditionalScan<TInput, TOutput, TContext, TSourceLogRecord>(TsavoriteKV<TStoreFunctions, TAllocator> store,
                                        ref TsavoriteKV<TStoreFunctions, TAllocator>.PendingContext<TInput, TOutput, TContext> pendingContext, in TSourceLogRecord srcLogRecord,
                                        ref OperationStackContext<TStoreFunctions, TAllocator> stackCtx, long minAddress, long maxAddress, ScanCursorState scanCursorState)
            where TSourceLogRecord : ISourceLogRecord
        {
            var status = store.PrepareIOForConditionalOperation(ref pendingContext, in srcLogRecord, ref stackCtx, minAddress, maxAddress, OperationType.CONDITIONAL_SCAN_PUSH);
            pendingContext.scanCursorState = scanCursorState;
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