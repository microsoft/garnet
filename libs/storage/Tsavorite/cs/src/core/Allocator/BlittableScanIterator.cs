// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Scan iterator for hybrid log
    /// </summary>
    internal sealed class BlittableScanIterator<TKey, TValue, TStoreFunctions, TScanFunctions> : ScanIteratorBase, IRecordScanner<TKey, TValue>
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TScanFunctions : IScanIteratorFunctions<TKey, TValue>
    {
        private readonly TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store;
        private readonly UnsafeContext<TKey, TValue, Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> uContext;
        private readonly BlittableAllocatorImpl<TKey, TValue, TStoreFunctions> hlogImpl;
        private readonly ScanCursorState<TKey, TValue> scanCursorState;
        private readonly TScanFunctions scanFunctions;
        private readonly bool assumeIsInMemory;

        private readonly BlittableFrame frame;
        private long framePhysicalAddress;

        /// <summary>
        /// Constructor for use with head-to-tail scan
        /// </summary>
        /// <param name="store"></param>
        /// <param name="uContext"></param>
        /// <param name="scanCursorState"></param>
        /// <param name="scanFunctions"></param>
        /// <param name="hlogImpl">The fully derived log implementation</param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="includeSealedRecords"></param>
        /// <param name="epoch">Epoch to use for protection; may be null if <paramref name="assumeIsInMemory"/> is true.</param>
        /// <param name="assumeIsInMemory">Provided address range is known by caller to be in memory, even if less than HeadAddress</param>
        /// <param name="logger"></param>
        internal BlittableScanIterator(TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store,
                UnsafeContext<TKey, TValue, Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> uContext,
                ScanCursorState<TKey, TValue> scanCursorState, TScanFunctions scanFunctions, BlittableAllocatorImpl<TKey, TValue, TStoreFunctions> hlogImpl,
                long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode, bool includeSealedRecords, LightEpoch epoch, bool assumeIsInMemory = false, ILogger logger = null)
            : base(beginAddress == 0 ? hlogImpl.GetFirstValidLogicalAddress(0) : beginAddress, endAddress, scanBufferingMode, includeSealedRecords, epoch, hlogImpl.LogPageSizeBits, logger: logger)
        {
            this.store = store;
            this.uContext = uContext;
            this.scanCursorState = scanCursorState;
            this.scanFunctions = scanFunctions;
            this.hlogImpl = hlogImpl;
            this.assumeIsInMemory = assumeIsInMemory;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlogImpl.PageSize, hlogImpl.GetDeviceSectorSize());
        }

        /// <summary>
        /// Constructor for use with tail-to-head push iteration of the passed key's record versions
        /// </summary>
        internal BlittableScanIterator(TsavoriteKV<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> store,
                UnsafeContext<TKey, TValue, Empty, Empty, Empty, LivenessCheckingSessionFunctions<TKey, TValue>, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> uContext,
                ScanCursorState<TKey, TValue> scanCursorState, TScanFunctions scanFunctions, BlittableAllocatorImpl<TKey, TValue, TStoreFunctions> hlog, long beginAddress, LightEpoch epoch, ILogger logger = null)
            : this(store, uContext, scanCursorState, scanFunctions, hlog, beginAddress, endAddress:hlog.GetTailAddress(), ScanBufferingMode.SinglePageBuffering, includeSealedRecords: false, epoch, logger: logger)
        {
        }

        /// <inheritdoc/>
        public bool SnapCursorToLogicalAddress(ref long cursor)
        {
            Debug.Assert(currentAddress == -1, "SnapCursorToLogicalAddress must be called before GetNext()");
            beginAddress = nextAddress = hlogImpl.SnapToFixedLengthLogicalAddressBoundary(ref cursor, BlittableAllocatorImpl<TKey, TValue, TStoreFunctions>.RecordSize);
            return true;
        }

        /// <summary>
        /// Get next record in iterator and push it without checking if it is unique.
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        public unsafe bool ScanNext() => ScanNext(scanFunctions);

        /// <summary>
        /// Get next record in iterator and push it without checking if it is unique, using a local type arg for ScanFunctions.
        /// </summary>
        /// <remarks>This is called either to implement the normal ScanNext, or directly by IObserver.OnNext implementations in which case we have created the iterator with <see cref="DummyScanFunctions{TKey, TValue}"/>.</remarks>
        /// <returns>True if record found, false if end of scan</returns>
        public unsafe bool ScanNext<TLocalScanFunctions>(TLocalScanFunctions scanFunctions)
            where TLocalScanFunctions : IScanIteratorFunctions<TKey, TValue>
        {
            if (!MoveToNextRecord(out var recordInfo, out var readOnlyAddress, out var physicalAddress))
                return false;

            OperationStackContext<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> stackCtx = default;
            try
            {
                // Lock if the record is in mutable memory.
                if (currentAddress >= readOnlyAddress)
                    store.LockForScan(ref stackCtx, ref hlogImpl._wrapper.GetKey(physicalAddress));
                ref var key = ref hlogImpl._wrapper.GetKey(physicalAddress);
                ref var value = ref hlogImpl._wrapper.GetValue(physicalAddress);
                RecordMetadata recordMetadata = new(recordInfo, stackCtx.recSrc.LogicalAddress);

                var stop = (stackCtx.recSrc.LogicalAddress >= readOnlyAddress)
                    ? !scanFunctions.ConcurrentReader(ref key, ref value, recordMetadata, scanCursorState.acceptedCount, out var cursorRecordResult)
                    : !scanFunctions.SingleReader(ref key, ref value, recordMetadata, scanCursorState.acceptedCount, out cursorRecordResult);
                if (stop)
                    return false;
                if ((cursorRecordResult & CursorRecordResult.Accept) != 0)
                    _ = Interlocked.Increment(ref scanCursorState.acceptedCount);
            }
            finally
            {
                if (stackCtx.recSrc.HasLock)
                    store.UnlockForScan(ref stackCtx);
                epoch?.Suspend();
            }

            // Success
            return true;
        }

        /// <summary>
        /// Get next record in iterator and push it if it is unique.
        /// </summary>
        /// <returns>True if record found or pending, false if end of scan</returns>
        public unsafe bool IterateNext(out Status status)
        {
            status = Status.CreateFound();
            if (!MoveToNextRecord(out var recordInfo, out var readOnlyAddress, out var physicalAddress))
                return false;

            OperationStackContext<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> stackCtx = default;
            try
            {
                // Lock if the record is in mutable memory.
                if (currentAddress >= readOnlyAddress)
                    store.LockForScan(ref stackCtx, ref hlogImpl._wrapper.GetKey(physicalAddress));
                status = hlogImpl.ConditionalScanPush<Empty, Empty, Empty,
                    SessionFunctionsWrapper<TKey, TValue, Empty, Empty, Empty, 
                                            LivenessCheckingSessionFunctions<TKey, TValue>, 
                                            BasicSessionLocker<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>>, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions
                                            >>, TScanFunctions>
                    (uContext.sessionFunctions, scanCursorState, scanFunctions, recordInfo, ref hlogImpl._wrapper.GetKey(physicalAddress), ref hlogImpl._wrapper.GetValue(physicalAddress), nextAddress);
            }
            finally
            {
                if (stackCtx.recSrc.HasLock)
                    store.UnlockForScan(ref stackCtx);
                epoch?.Suspend();
            }

            // Success
            return true;
        }

        // If this returns true, the epoch is still acquired; otherwise it is not.
        private unsafe bool MoveToNextRecord(out RecordInfo recordInfo, out long readOnlyAddress, out long physicalAddress)
        {
            while (true)
            {
                currentAddress = nextAddress;
                var stopAddress = endAddress < hlogImpl.GetTailAddress() ? endAddress : hlogImpl.GetTailAddress();
                if (currentAddress >= stopAddress)
                {
                    recordInfo = default;
                    readOnlyAddress = physicalAddress = -1L;
                    return false;
                }

                epoch?.Resume();
                readOnlyAddress = hlogImpl.ReadOnlyAddress;
                var headAddress = hlogImpl.HeadAddress;
                if (currentAddress < hlogImpl.BeginAddress && !assumeIsInMemory)
                    currentAddress = hlogImpl.BeginAddress;

                // If currentAddress < headAddress and we're not buffering and not guaranteeing the records are in memory, fail.
                if (frameSize == 0 && currentAddress < headAddress && !assumeIsInMemory)
                {
                    epoch?.Suspend();
                    throw new TsavoriteException("Iterator address is less than log HeadAddress in memory-scan mode");
                }

                var currentPage = currentAddress >> hlogImpl.LogPageSizeBits;
                var offset = currentAddress & hlogImpl.PageSizeMask;

                if (currentAddress < headAddress && !assumeIsInMemory)
                    _ = BufferAndLoad(currentAddress, currentPage, currentPage % frameSize, headAddress, stopAddress);

                physicalAddress = GetPhysicalAddress(currentAddress, headAddress, currentPage, offset);
                var recordSize = hlogImpl._wrapper.GetRecordSize(physicalAddress).allocatedSize;

                // If record does not fit on page, skip to the next page.
                if ((currentAddress & hlogImpl.PageSizeMask) + recordSize > hlogImpl.PageSize)
                {
                    nextAddress = (1 + (currentAddress >> hlogImpl.LogPageSizeBits)) << hlogImpl.LogPageSizeBits;
                    epoch?.Suspend();
                    continue;
                }

                nextAddress = currentAddress + recordSize;

                recordInfo = hlogImpl._wrapper.GetInfo(physicalAddress);
                var skipOnScan = includeSealedRecords ? recordInfo.Invalid : recordInfo.SkipOnScan;
                if (skipOnScan || recordInfo.IsNull())
                {
                    epoch?.Suspend();
                    continue;
                }
                return true;
            }
        }

        /// <summary>
        /// Get previous record and keep the epoch held while we call the user's local scan functions
        /// </summary>
        /// <returns>True if record found, false if end of scan</returns>
        public bool GetPrevInMemory<TLocalScanFunctions>(ref TKey key, TLocalScanFunctions scanFunctions, ref long numRecords, out bool continueOnDisk, out bool stop)
            where TLocalScanFunctions : IScanIteratorFunctions<TKey, TValue>
        {
            continueOnDisk = false;

            // Trace back for key match
            while (true)
            {
                // "nextAddress" is reused as "previous address" for this operation.
                currentAddress = nextAddress;
                if (currentAddress < hlogImpl.HeadAddress)
                {
                    continueOnDisk = currentAddress >= hlogImpl.BeginAddress;
                    return stop = false;
                }

                epoch?.Resume();
                var readOnlyAddress = hlogImpl.ReadOnlyAddress;
                var headAddress = hlogImpl.HeadAddress;

                var currentPage = currentAddress >> hlogImpl.LogPageSizeBits;
                var offset = currentAddress & hlogImpl.PageSizeMask;

                var physicalAddress = GetPhysicalAddress(currentAddress, headAddress, currentPage, offset);

                var recordInfo = hlogImpl._wrapper.GetInfo(physicalAddress);
                nextAddress = recordInfo.PreviousAddress;

                // Do not SkipOnScan here; we Seal previous versions.
                if (recordInfo.IsNull() || !hlogImpl._storeFunctions.KeysEqual(ref hlogImpl._wrapper.GetKey(physicalAddress), ref key))
                {
                    epoch?.Suspend();
                    continue;
                }

                OperationStackContext<TKey, TValue, TStoreFunctions, BlittableAllocator<TKey, TValue, TStoreFunctions>> stackCtx = default;
                try
                {
                    // Lock if the record is in mutable memory.
                    if (currentAddress >= readOnlyAddress)
                        store.LockForScan(ref stackCtx, ref hlogImpl._wrapper.GetKey(physicalAddress));
                    ref var value = ref hlogImpl._wrapper.GetValue(physicalAddress);

                    ++numRecords;
                    stop = currentAddress >= readOnlyAddress
                        ? !scanFunctions.ConcurrentReader(ref key, ref value, new RecordMetadata(recordInfo, CurrentAddress), numRecords, out _)
                        : !scanFunctions.SingleReader(ref key, ref value, new RecordMetadata(recordInfo, CurrentAddress), numRecords, out _);
                    return !stop;
                }
                finally
                {
                    if (stackCtx.recSrc.HasLock)
                        store.UnlockForScan(ref stackCtx);
                    epoch?.Suspend();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long GetPhysicalAddress(long currentAddress, long headAddress, long currentPage, long offset)
        {
            if (currentAddress >= headAddress)
            {
                // physicalAddress is in memory; set framePhysicalAddress to 0 so we'll set currentKey and currentValue from physicalAddress below
                framePhysicalAddress = 0;
                return hlogImpl.GetPhysicalAddress(currentAddress);
            }

            // physicalAddress is not in memory, so we'll GetKey and GetValue will use framePhysicalAddress
            return framePhysicalAddress = frame.GetPhysicalAddress(currentPage % frameSize, offset);
        }

        /// <summary>
        /// Dispose iterator
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
            frame?.Dispose();
        }

        internal override void AsyncReadPagesFromDeviceToFrame<TContext>(long readPageStart, int numPages, long untilAddress, TContext context, out CountdownEvent completed,
                long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            => hlogImpl.AsyncReadPagesFromDeviceToFrame(readPageStart, numPages, untilAddress, AsyncReadPagesCallback, context, frame, out completed, devicePageOffset, device, objectLogDevice);

        private unsafe void AsyncReadPagesCallback(uint errorCode, uint numBytes, object context)
        {
            var result = (PageAsyncReadResult<Empty>)context;

            if (errorCode != 0)
            {
                logger?.LogError($"{nameof(AsyncReadPagesCallback)} error: {{errorCode}}", errorCode);
                result.cts?.Cancel();
            }

            if (result.freeBuffer1 != null)
            {
                BlittableAllocatorImpl<TKey, TValue, TStoreFunctions>.PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
                result.freeBuffer1 = null;
            }

            if (errorCode == 0)
                _ = (result.handle?.Signal());

            Interlocked.MemoryBarrier();
        }
    }
}