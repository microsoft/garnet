// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Scan iterator for TsavoriteLog
    /// </summary>
    public class TsavoriteLogScanIterator : ScanIteratorBase, IDisposable
    {
        protected readonly TsavoriteLog tsavoriteLog;
        private readonly TsavoriteLogAllocatorImpl allocator;
        private readonly BlittableFrame frame;
        private readonly GetMemory getMemory;
        private readonly int headerSize;
        protected readonly bool scanUncommitted;
        protected bool disposed = false;

        /// <summary>
        /// Whether iteration has ended, either because we reached the end address of iteration, or because
        /// we reached the end of a completed log.
        /// </summary>
        public bool Ended => (nextAddress >= endAddress) || (tsavoriteLog.LogCompleted && nextAddress == tsavoriteLog.TailAddress);

        /// <summary>Constructor</summary>
        internal unsafe TsavoriteLogScanIterator(TsavoriteLog tsavoriteLog, TsavoriteLogAllocatorImpl hlog, long beginAddress, long endAddress,
                GetMemory getMemory, DiskScanBufferingMode diskScanBufferingMode, LightEpoch epoch, int headerSize, bool scanUncommitted = false, ILogger logger = null)
            : base(beginAddress == 0 ? hlog.GetFirstValidLogicalAddressOnPage(0) : beginAddress, endAddress,
                diskScanBufferingMode, InMemoryScanBufferingMode.NoBuffering, includeClosedRecords: false, epoch, hlog.LogPageSizeBits, logger: logger)
        {
            this.tsavoriteLog = tsavoriteLog;
            allocator = hlog;
            this.getMemory = getMemory;
            this.headerSize = headerSize;
            this.scanUncommitted = scanUncommitted;
            if (frameSize > 0)
                frame = new BlittableFrame(frameSize, hlog.PageSize, hlog.GetDeviceSectorSize());
        }

        /// <summary>
        /// Async enumerable for iterator
        /// </summary>
        /// <returns>Entry, actual entry length, logical address of entry, logical address of next entry</returns>
        public async IAsyncEnumerable<(byte[] entry, int entryLength, long currentAddress, long nextAddress)> GetAsyncEnumerable([EnumeratorCancellation] CancellationToken token = default)
        {
            while (!disposed)
            {
                byte[] result;
                int length;
                long currentAddress;
                long nextAddress;
                while (!GetNext(out result, out length, out currentAddress, out nextAddress))
                {
                    if (!await WaitAsync(token).ConfigureAwait(false))
                        yield break;
                }
                yield return (result, length, currentAddress, nextAddress);
            }
        }

        /// <summary>
        /// Async enumerable for iterator (memory pool based version)
        /// </summary>
        /// <returns>Entry, actual entry length, logical address of entry, logical address of next entry</returns>
        public async IAsyncEnumerable<(IMemoryOwner<byte> entry, int entryLength, long currentAddress, long nextAddress)> GetAsyncEnumerable(MemoryPool<byte> pool, [EnumeratorCancellation] CancellationToken token = default)
        {
            while (!disposed)
            {
                IMemoryOwner<byte> result;
                int length;
                long currentAddress;
                long nextAddress;
                while (!GetNext(pool, out result, out length, out currentAddress, out nextAddress))
                {
                    if (!await WaitAsync(token).ConfigureAwait(false))
                        yield break;
                }
                yield return (result, length, currentAddress, nextAddress);
            }
        }

        /// <summary>
        /// Asynchronously consume the log with given consumer until end of iteration or cancelled
        /// </summary>
        /// <param name="consumer"> consumer </param>
        /// <param name="token"> cancellation token </param>
        /// <typeparam name="T"> consumer type </typeparam>
        public async Task ConsumeAllAsync<T>(T consumer, CancellationToken token = default) where T : ILogEntryConsumer
        {
            while (!disposed)
            {
                // TryConsumeNext returns false if we have to wait for the next record.
                while (!TryConsumeNext(consumer))
                {
                    if (!await WaitAsync(token).ConfigureAwait(false))
                        return;
                }
            }
        }

        /// <summary>
        /// Asynchronously consume the log with given consumer until end of iteration or cancelled
        /// </summary>
        /// <param name="consumer"> consumer </param>
        /// <param name="throttleMs">throttle the iteration speed</param>
        /// <param name="maxChunkSize">max size of returned chunk</param>
        /// <param name="token"> cancellation token </param>
        /// <typeparam name="T"> consumer type </typeparam>
        public async Task BulkConsumeAllAsync<T>(T consumer, int throttleMs = 0, int maxChunkSize = 0, CancellationToken token = default) where T : IBulkLogEntryConsumer
        {
            while (!disposed)
            {
                // TryConsumeNext returns false if we have to wait for the next record.
                while (!TryBulkConsumeNext(consumer, maxChunkSize))
                {
                    await Task.Delay(throttleMs, token).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Wait for iteration to be ready to continue
        /// </summary>
        /// <returns>true if there's more data available to be read; false if there will never be more data (log has been shutdown / iterator has reached endAddress)</returns>
        public ValueTask<bool> WaitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (!scanUncommitted)
            {
                if (NextAddress >= endAddress)
                    return new ValueTask<bool>(false);
                if (NextAddress < tsavoriteLog.CommittedUntilAddress)
                    return new ValueTask<bool>(true);
                return SlowWaitAsync(this, token);
            }

            if (NextAddress < tsavoriteLog.SafeTailAddress)
                return new ValueTask<bool>(true);
            return SlowWaitUncommittedAsync(token);
        }

        private static async ValueTask<bool> SlowWaitAsync(TsavoriteLogScanIterator @this, CancellationToken token)
        {
            while (true)
            {
                if (@this.disposed || @this.Ended)
                    return false;
                var commitTask = @this.tsavoriteLog.CommitTask;
                if (@this.NextAddress < @this.tsavoriteLog.CommittedUntilAddress)
                    return true;

                // Ignore commit exceptions, except when the token is signaled
                try
                {
                    _ = await commitTask.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (ObjectDisposedException) { return false; }
                catch when (!token.IsCancellationRequested) { }
            }
        }

        protected virtual async ValueTask<bool> SlowWaitUncommittedAsync(CancellationToken token)
        {
            while (true)
            {
                if (disposed)
                    return false;
                if (Ended) return false;

                var tcs = tsavoriteLog.refreshUncommittedTcs;
                if (tcs == null)
                {
                    var newTcs = new TaskCompletionSource<Empty>(TaskCreationOptions.RunContinuationsAsynchronously);
                    tcs = Interlocked.CompareExchange(ref tsavoriteLog.refreshUncommittedTcs, newTcs, null);
                    tcs ??= newTcs; // successful CAS so update the local var
                }

                if (NextAddress < tsavoriteLog.SafeTailAddress)
                    return true;

                // Ignore refresh-uncommitted exceptions, except when the token is signaled
                try
                {
                    _ = await tcs.Task.WithCancellationAsync(token).ConfigureAwait(false);
                }
                catch (ObjectDisposedException) { return false; }
                catch when (!token.IsCancellationRequested) { }
            }
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <param name="currentAddress">Logical address of entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(out byte[] entry, out int entryLength, out long currentAddress)
        {
            return GetNext(out entry, out entryLength, out currentAddress, out _);
        }

        /// <summary>
        /// Get next record in iterator
        /// </summary>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <param name="currentAddress">Logical address of entry</param>
        /// <param name="nextAddress">Logical address of next entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(out byte[] entry, out int entryLength, out long currentAddress, out long nextAddress)
        {
            if (disposed)
            {
                entry = default;
                entryLength = default;
                currentAddress = default;
                nextAddress = default;
                return false;
            }
            epoch.Resume();
            // Continue looping until we find a record that is not a commit record
            while (true)
            {
                long physicalAddress;
                bool isCommitRecord;
                try
                {
                    var hasNext = GetNextInternal(out physicalAddress, out entryLength, out currentAddress,
                        out nextAddress,
                        out isCommitRecord, out _);
                    if (!hasNext)
                    {
                        entry = default;
                        epoch.Suspend();
                        return false;
                    }
                }
                catch (Exception)
                {
                    // Throw upwards, but first, suspend the epoch we are in 
                    epoch.Suspend();
                    throw;
                }

                if (isCommitRecord)
                {
                    TsavoriteLogRecoveryInfo info = new();
                    info.Initialize(new ReadOnlySpan<byte>((byte*)(headerSize + physicalAddress), entryLength));
                    if (info.CommitNum != long.MaxValue) continue;

                    // Otherwise, no more entries
                    entry = default;
                    entryLength = default;
                    epoch.Suspend();
                    return false;
                }

                if (getMemory != null)
                {
                    // Use user delegate to allocate memory
                    entry = getMemory(entryLength);
                    if (entry.Length < entryLength)
                    {
                        epoch.Suspend();
                        throw new TsavoriteException("Byte array provided has invalid length");
                    }
                }
                else
                {
                    // We allocate a byte array from heap
                    entry = new byte[entryLength];
                }

                fixed (byte* bp = entry)
                    Buffer.MemoryCopy((void*)(headerSize + physicalAddress), bp, entryLength, entryLength);

                epoch.Suspend();
                return true;
            }
        }

        /// <summary>
        /// GetNext supporting memory pools
        /// </summary>
        /// <param name="pool">Memory pool</param>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <param name="currentAddress">Logical address of entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(MemoryPool<byte> pool, out IMemoryOwner<byte> entry, out int entryLength, out long currentAddress)
        {
            return GetNext(pool, out entry, out entryLength, out currentAddress, out _);
        }

        /// <summary>
        /// GetNext supporting memory pools
        /// </summary>
        /// <param name="pool">Memory pool</param>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <param name="currentAddress">Logical address of entry</param>
        /// <param name="nextAddress">Logical address of next entry</param>
        /// <returns></returns>
        public unsafe bool GetNext(MemoryPool<byte> pool, out IMemoryOwner<byte> entry, out int entryLength, out long currentAddress, out long nextAddress)
        {
            if (disposed)
            {
                entry = default;
                entryLength = default;
                currentAddress = default;
                nextAddress = default;
                return false;
            }

            epoch.Resume();
            // Continue looping until we find a record that is not a commit record
            while (true)
            {
                long physicalAddress;
                bool isCommitRecord;
                try
                {
                    var hasNext = GetNextInternal(out physicalAddress, out entryLength, out currentAddress,
                        out nextAddress,
                        out isCommitRecord, out _);
                    if (!hasNext)
                    {
                        entry = default;
                        entryLength = default;
                        epoch.Suspend();
                        return false;
                    }

                }
                catch (Exception)
                {
                    // Throw upwards, but first, suspend the epoch we are in 
                    epoch.Suspend();
                    throw;
                }

                if (isCommitRecord)
                {
                    TsavoriteLogRecoveryInfo info = new();
                    info.Initialize(new ReadOnlySpan<byte>((byte*)(headerSize + physicalAddress), entryLength));
                    if (info.CommitNum != long.MaxValue) continue;

                    // Otherwise, no more entries
                    entry = default;
                    entryLength = default;
                    epoch.Suspend();
                    return false;
                }

                entry = pool.Rent(entryLength);

                fixed (byte* bp = &entry.Memory.Span.GetPinnableReference())
                    Buffer.MemoryCopy((void*)(headerSize + physicalAddress), bp, entryLength, entryLength);
                epoch.Suspend();
                return true;
            }
        }


        /// <summary>
        /// Consume the next entry in the log with the given consumer
        /// </summary>
        /// <param name="consumer">consumer</param>
        /// <typeparam name="T">concrete type of consumer</typeparam>
        /// <returns>whether a next entry is present</returns>
        public unsafe bool TryConsumeNext<T>(T consumer) where T : ILogEntryConsumer
        {
            if (disposed)
            {
                currentAddress = default;
                nextAddress = default;
                return false;
            }

            epoch.Resume();
            // Continue looping until we find a record that is not a commit record
            try
            {
                while (true)
                {
                    long physicalAddress;
                    bool isCommitRecord;
                    int entryLength;
                    var onFrame = false;
                    try
                    {
                        var hasNext = GetNextInternal(out physicalAddress, out entryLength, out currentAddress,
                            out nextAddress,
                            out isCommitRecord, out onFrame);
                        if (!hasNext)
                        {
                            return false;
                        }
                    }
                    catch (Exception)
                    {
                        // Throw upwards
                        throw;
                    }

                    if (isCommitRecord)
                    {
                        TsavoriteLogRecoveryInfo info = new();
                        info.Initialize(new ReadOnlySpan<byte>((byte*)(headerSize + physicalAddress), entryLength));
                        if (info.CommitNum != long.MaxValue) continue;

                        // Otherwise, no more entries
                        return false;
                    }

                    // Consume the chunk
                    if (onFrame)
                    {
                        // Record in frame, so we do no need epoch protection to access it
                        epoch.Suspend();
                        try
                        {
                            consumer.Consume((byte*)(physicalAddress + headerSize), entryLength, currentAddress, nextAddress, isProtected: false);
                        }
                        finally
                        {
                            epoch.Resume();
                        }
                    }
                    else
                    {
                        // Consume the chunk (warning: we are under epoch protection here, as we are consuming directly from main memory log buffer)
                        consumer.Consume((byte*)(physicalAddress + headerSize), entryLength, currentAddress, nextAddress, isProtected: true);
                    }
                    return true;
                }
            }
            finally
            {
                epoch.Suspend();
            }
        }

        /// <summary>
        /// Consume the next entry in the log with the given consumer
        /// </summary>
        /// <param name="consumer">consumer</param>
        /// <param name="maxChunkSize"></param>
        /// <typeparam name="T">concrete type of consumer</typeparam>
        /// <returns>whether a next entry is present</returns>
        public unsafe bool TryBulkConsumeNext<T>(T consumer, int maxChunkSize = 0) where T : IBulkLogEntryConsumer
        {
            if (maxChunkSize == 0) maxChunkSize = allocator.PageSize;

            if (disposed)
            {
                currentAddress = default;
                nextAddress = default;
                return false;
            }

            bool retVal;

            epoch.Resume();

            // Find a contiguous set of log entries
            try
            {
                while (true)
                {
                    // If initializing wait for completion
                    while (tsavoriteLog.Initializing)
                    {
                        _ = Thread.Yield();
                        epoch.ProtectAndDrain();
                    }

                    var hasNext = GetNextInternal(out long startPhysicalAddress, out int newEntryLength, out long startLogicalAddress, out long endLogicalAddress, out bool isCommitRecord, out bool onFrame);

                    if (!hasNext)
                    {
                        retVal = false;
                        break;
                    }

                    // GetNextInternal returns only the payload length, so adjust the totalLength
                    var totalLength = headerSize + Align(newEntryLength);

                    // Expand the records in iteration, as long as as they are on the same physical page
                    while (ExpandGetNextInternal(startPhysicalAddress, ref totalLength, out _, out endLogicalAddress, out isCommitRecord))
                    {
                        if (totalLength > maxChunkSize)
                            break;
                    }

                    // Consume the chunk
                    if (onFrame)
                    {
                        // Record in frame, so we do no need epoch protection to access it
                        epoch.Suspend();
                        try
                        {
                            consumer.Consume((byte*)startPhysicalAddress, totalLength, startLogicalAddress, endLogicalAddress, isProtected: false);
                            consumer.Throttle();
                        }
                        finally
                        {
                            epoch.Resume();
                        }
                    }
                    else
                    {
                        // Consume the chunk (warning: we are under epoch protection here, as we are consuming directly from main memory log buffer)
                        consumer.Consume((byte*)startPhysicalAddress, totalLength, startLogicalAddress, endLogicalAddress, isProtected: true);

                        epoch.Suspend();
                        try
                        {
                            // Throttle the iteration if needed (outside epoch protection)
                            consumer.Throttle();
                        }
                        finally
                        {
                            epoch.Resume();
                        }
                    }
                }
            }
            finally
            {
                epoch.Suspend();
            }
            return retVal;
        }

        /// <summary>
        /// WARNING: advanced users only.
        /// Get next record in iterator, accessing unsafe raw bytes and retaining epoch protection.
        /// Make sure to call UnsafeRelease when done processing the raw bytes (without delay).
        /// </summary>
        /// <param name="entry">Copy of entry, if found</param>
        /// <param name="entryLength">Actual length of entry</param>
        /// <param name="currentAddress">Logical address of entry</param>
        /// <param name="nextAddress">Logical address of next entry</param>
        /// <returns></returns>
        public unsafe bool UnsafeGetNext(out byte* entry, out int entryLength, out long currentAddress, out long nextAddress)
        {
            if (disposed)
            {
                entry = default;
                entryLength = default;
                currentAddress = default;
                nextAddress = default;
                return false;
            }
            epoch.Resume();
            // Continue looping until we find a record that is not a commit record
            while (true)
            {
                long physicalAddress;
                bool isCommitRecord;
                try
                {
                    var hasNext = GetNextInternal(out physicalAddress, out entryLength, out currentAddress,
                        out nextAddress,
                        out isCommitRecord, out _);
                    if (!hasNext)
                    {
                        entry = default;
                        epoch.Suspend();
                        return false;
                    }
                }
                catch (Exception)
                {
                    // Throw upwards, but first, suspend the epoch we are in 
                    epoch.Suspend();
                    throw;
                }

                entry = (byte*)(headerSize + physicalAddress);

                if (isCommitRecord)
                {
                    TsavoriteLogRecoveryInfo info = new();
                    info.Initialize(new ReadOnlySpan<byte>(entry, entryLength));
                    if (info.CommitNum != long.MaxValue) continue;

                    // Otherwise, no more entries
                    entry = default;
                    entryLength = default;
                    epoch.Suspend();
                    return false;
                }

                return true;
            }
        }

        /// <summary>
        /// WARNING: advanced users only.
        /// Release a native memory reference obtained via a successful UnsafeGetNext.
        /// </summary>
        public void UnsafeRelease() => epoch.Suspend();

        /// <summary>
        /// Dispose the iterator
        /// </summary>
        public override void Dispose()
        {
            if (!disposed)
            {
                base.Dispose();

                // Dispose/unpin the frame from memory
                frame?.Dispose();

                if (Interlocked.Decrement(ref tsavoriteLog.logRefCount) == 0)
                    tsavoriteLog.TrueDispose();

                disposed = true;
            }
        }

        internal override void AsyncReadPageFromDeviceToFrame<TContext>(CircularDiskReadBuffer _ /*readBuffers*/, long readPage, long untilAddress, TContext context, out CountdownEvent completed,
                long devicePageOffset = 0, IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
            => allocator.AsyncReadPageFromDeviceToFrame(readBuffers: null, readPage, untilAddress, AsyncReadPagesToFrameCallback, context, frame, out completed, devicePageOffset, device, objectLogDevice, cts);

        private unsafe void AsyncReadPagesToFrameCallback(uint errorCode, uint numBytes, object context)
        {
            try
            {
                var result = (PageAsyncReadResult<Empty>)context;

                if (errorCode == 0)
                    _ = result.handle?.Signal();
                else
                {
                    logger?.LogError($"{nameof(AsyncReadPagesToFrameCallback)} error: {{errorCode}}", errorCode);
                    result.cts?.Cancel();
                }
                Interlocked.MemoryBarrier();
            }
            catch when (disposed) { }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Align(int length)
        {
            return (length + 3) & ~3;
        }

        internal unsafe bool ScanForwardForCommit(ref TsavoriteLogRecoveryInfo info, long commitNum = -1)
        {
            epoch.Resume();
            var foundCommit = false;
            try
            {
                // Continue looping until we find a record that is a commit record
                while (GetNextInternal(out long physicalAddress, out var entryLength, out _,
                    out _,
                    out var isCommitRecord, out _))
                {
                    if (!isCommitRecord) continue;

                    foundCommit = true;
                    info.Initialize(new ReadOnlySpan<byte>((void*)(headerSize + physicalAddress), entryLength));

                    Debug.Assert(info.CommitNum != -1);

                    // If we have already found the commit number we are looking for, can stop early
                    if (info.CommitNum == commitNum) break;
                }
            }
            catch (TsavoriteException)
            {
                // If we are here --- simply stop scanning because we ran into an incomplete entry
            }
            finally
            {
                epoch.Suspend();
            }

            if (info.CommitNum == commitNum)
                return true;
            // User wants any commit
            if (commitNum == -1)
                return foundCommit;
            // requested commit not found
            return false;
        }

        /// <summary>
        /// Retrieve physical address of next iterator value (under epoch protection if it is from main page buffer)
        /// </summary>
        private unsafe bool GetNextInternal(out long physicalAddress, out int entryLength, out long currentAddress, out long outNextAddress, out bool commitRecord, out bool onFrame)
        {
            while (true)
            {
                physicalAddress = 0;
                entryLength = 0;
                currentAddress = nextAddress;
                outNextAddress = currentAddress;
                commitRecord = false;
                onFrame = false;

                // Check for boundary conditions
                if (currentAddress < allocator.BeginAddress)
                {
                    Utility.MonotonicUpdate(ref nextAddress, allocator.BeginAddress, out _);
                    currentAddress = nextAddress;
                    outNextAddress = currentAddress;
                }

                var _headAddress = allocator.HeadAddress;

                // Fast forward to memory in case we are flushing to a null device
                if (allocator.IsNullDevice && currentAddress < _headAddress)
                {
                    Utility.MonotonicUpdate(ref nextAddress, _headAddress, out _);
                    currentAddress = nextAddress;
                    outNextAddress = currentAddress;
                }

                var _currentPage = allocator.GetPage(currentAddress);
                var _currentFrame = _currentPage % frameSize;
                var _currentOffset = allocator.GetOffsetOnPage(currentAddress);

                if (disposed)
                    return false;

                if ((currentAddress >= endAddress) || (currentAddress >= (scanUncommitted ? tsavoriteLog.SafeTailAddress : tsavoriteLog.CommittedUntilAddress)))
                    return false;

                if (currentAddress < _headAddress)
                {
                    var _endAddress = endAddress;
                    if (tsavoriteLog.readOnlyMode)
                    {
                        // Support partial page reads of committed data
                        var _flush = tsavoriteLog.CommittedUntilAddress;
                        if (_flush < endAddress)
                            _endAddress = _flush;
                    }

                    if (BufferAndLoad(currentAddress, _currentPage, _currentFrame, _headAddress, _endAddress))
                        continue;
                    physicalAddress = frame.GetPhysicalAddress(_currentFrame, _currentOffset);
                    onFrame = true;
                }
                else
                {
                    physicalAddress = allocator.GetPhysicalAddress(currentAddress);
                }

                // Get and check entry length
                entryLength = tsavoriteLog.GetLength((byte*)physicalAddress);

                // We may encounter zeroed out bits at the end of page in a normal log, therefore, we need to check whether that is the case
                if (entryLength == 0)
                {
                    // Zero-ed out bytes could be padding at the end of page, first jump to the start of next page. 
                    var nextStart = allocator.GetLogicalAddressOfStartOfPage(1 + allocator.GetPage(currentAddress));
                    if (Utility.MonotonicUpdate(ref nextAddress, nextStart, out _))
                    {
                        var pageOffset = allocator.GetOffsetOnPage(currentAddress);

                        // If zeroed out field is at page start, we encountered an uninitialized page and should signal up
                        if (pageOffset == 0)
                            throw new TsavoriteException("Uninitialized page found during scan at page " + allocator.GetPage(currentAddress));
                    }
                    continue;
                }

                // commit records have negative length fields
                if (entryLength < 0)
                {
                    commitRecord = true;
                    entryLength = -entryLength;
                }

                int recordSize = headerSize + Align(entryLength);
                if (_currentOffset + recordSize > allocator.PageSize)
                {
                    currentAddress += headerSize;
                    if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                        throw new TsavoriteException("Invalid length of record found: " + entryLength + " at address " + currentAddress);
                    continue;
                }

                // Verify checksum if needed
                if (currentAddress < _headAddress)
                {
                    if (!tsavoriteLog.VerifyChecksum((byte*)physicalAddress, entryLength))
                    {
                        currentAddress += headerSize;
                        if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out _))
                            throw new TsavoriteException("Invalid checksum found during scan, skipping");
                        continue;
                    }
                }

                if ((allocator.GetOffsetOnPage(currentAddress) + recordSize) == allocator.PageSize)
                    currentAddress = allocator.GetLogicalAddressOfStartOfPage(1 + allocator.GetPage(currentAddress));
                else
                    currentAddress += recordSize;

                if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out long oldCurrentAddress))
                {
                    outNextAddress = currentAddress;
                    currentAddress = oldCurrentAddress;
                    return true;
                }
            }
        }

        private unsafe bool ExpandGetNextInternal(long startPhysicalAddress, ref int totalEntryLength, out long currentAddress, out long outNextAddress, out bool commitRecord)
        {
            while (true)
            {
                long physicalAddress;
                int entryLength;
                currentAddress = nextAddress;
                outNextAddress = currentAddress;
                commitRecord = false;

                // Check for boundary conditions
                if (currentAddress < allocator.BeginAddress)
                {
                    // Cannot expand, return false
                    return false;
                }

                var _headAddress = allocator.HeadAddress;

                // Fast forward to memory in case we are flushing to a null device
                if (allocator.IsNullDevice && currentAddress < _headAddress)
                {
                    // Cannot expand, return false
                    return false;
                }

                var _currentPage = allocator.GetPage(currentAddress);
                var _currentFrame = _currentPage % frameSize;
                var _currentOffset = allocator.GetOffsetOnPage(currentAddress);

                if (disposed)
                    return false;

                if ((currentAddress >= endAddress) || (currentAddress >= (scanUncommitted ? tsavoriteLog.SafeTailAddress : tsavoriteLog.CommittedUntilAddress)))
                    return false;

                if (currentAddress < _headAddress)
                {
                    var _endAddress = endAddress;
                    if (tsavoriteLog.readOnlyMode)
                    {
                        // Support partial page reads of committed data
                        var _flush = tsavoriteLog.CommittedUntilAddress;
                        if (_flush < endAddress)
                            _endAddress = _flush;
                    }

                    if (NeedBufferAndLoad(currentAddress, _currentPage, _currentFrame, _headAddress, _endAddress))
                        return false;

                    physicalAddress = frame.GetPhysicalAddress(_currentFrame, _currentOffset);
                }
                else
                {
                    physicalAddress = allocator.GetPhysicalAddress(currentAddress);
                }

                if (physicalAddress != startPhysicalAddress + totalEntryLength)
                    return false;

                // Get and check entry length
                entryLength = tsavoriteLog.GetLength((byte*)physicalAddress);

                // We may encounter zeroed out bits at the end of page in a normal log, therefore, we need to check whether that is the case
                if (entryLength == 0)
                {
                    return false;
                }

                // commit records have negative length fields
                if (entryLength < 0)
                {
                    commitRecord = true;
                    entryLength = -entryLength;
                }

                int recordSize = headerSize + Align(entryLength);
                if (_currentOffset + recordSize > allocator.PageSize)
                {
                    return false;
                }

                // Verify checksum if needed
                if (currentAddress < _headAddress)
                {
                    if (!tsavoriteLog.VerifyChecksum((byte*)physicalAddress, entryLength))
                    {
                        return false;
                    }
                }

                if ((allocator.GetOffsetOnPage(currentAddress) + recordSize) == allocator.PageSize)
                    currentAddress = allocator.GetLogicalAddressOfStartOfPage(1 + allocator.GetPage(currentAddress));
                else
                    currentAddress += recordSize;

                if (Utility.MonotonicUpdate(ref nextAddress, currentAddress, out long oldCurrentAddress))
                {
                    totalEntryLength += recordSize;
                    outNextAddress = currentAddress;
                    currentAddress = oldCurrentAddress;
                    return true;
                }
            }
        }
    }
}