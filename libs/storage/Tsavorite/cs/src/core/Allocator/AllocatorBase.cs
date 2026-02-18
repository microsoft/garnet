// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using static Tsavorite.core.Utility;

namespace Tsavorite.core
{
#pragma warning disable IDE0065 // Misplaced using directive
    using static LogAddress;

    /// <summary>
    /// Type-free base class for hybrid log memory allocator. Contains utility methods that do not need type args and are not performance-critical
    /// so can be virtual.
    /// </summary>
    public abstract class AllocatorBase
    {
        /// <summary>Create the circular buffers for <see cref="LogRecord"/> flushing to device. Only implemented by ObjectAllocator.</summary>
        internal virtual CircularDiskWriteBuffer CreateCircularFlushBuffers(IDevice objectLogDevice, ILogger logger) => default;
        /// <summary>Create the circular flush buffers for object deserialization from device. Only implemented by ObjectAllocator.</summary>
        internal virtual CircularDiskReadBuffer CreateCircularReadBuffers(IDevice objectLogDevice, ILogger logger) => default;
        /// <summary>Create the circular flush buffers for object deserialization from device. Only implemented by ObjectAllocator.</summary>
        internal virtual CircularDiskReadBuffer CreateCircularReadBuffers() => default;

        /// <summary>Returns the lowest segment in use in the object log; will be zero unless the database has been truncated.</summary>
        internal virtual int LowestObjectLogSegmentInUse => 0;
        /// <summary>Get the ObjectLog tail position, if this is ObjectAllocator.</summary>
        internal virtual ObjectLogFilePositionInfo GetObjectLogTail() => new();  // This marks it as "unset"
        /// <summary>Set the ObjectLog tail position, if this is ObjectAllocator.</summary>
        internal virtual void SetObjectLogTail(ObjectLogFilePositionInfo tail) { }
    }

    /// <summary>
    /// Base class for hybrid log memory allocator. Contains utility methods, some of which are not performance-critical so can be virtual.
    /// </summary>
    public abstract unsafe partial class AllocatorBase<TStoreFunctions, TAllocator> : AllocatorBase, IDisposable
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>The epoch we are operating with</summary>
        internal readonly LightEpoch epoch;
        /// <summary>Whether we own (and thus must dispose) <see cref="epoch"/></summary>
        private readonly bool ownedEpoch;

        /// <summary>The store functions for this instance of TsavoriteKV</summary>
        internal readonly TStoreFunctions storeFunctions;

        /// <summary>The fully-derived allocator struct wrapper (so calls on it are inlined rather than virtual) for this log.</summary>
        internal readonly TAllocator _wrapper;

        /// <summary>The <see cref="ObjectIdMap"/> to hold the objects for transient <see cref="LogRecord"/> instances.</summary>
        internal ObjectIdMap transientObjectIdMap;

        /// <summary>Sometimes it's useful to know this explicitly rather than rely on method overrides etc.</summary>
        internal bool IsObjectAllocator => transientObjectIdMap is not null;

        /// <summary>If true, then this allocator has <see cref="PageHeader"/> as the first bytes on a page, so allocating a logical address
        ///     in <see cref="HandlePageOverflow"/> must skip these bytes.</summary>
        internal int pageHeaderSize;

        #region Protected size definitions
        /// <summary>Buffer size</summary>
        internal readonly int BufferSize;

        /// <summary>Log page size</summary>
        internal readonly int LogPageSizeBits;

        /// <summary>Page size</summary>
        internal readonly int PageSize;

        /// <summary>Page size mask</summary>
        internal readonly int PageSizeMask;

        /// <summary>Buffer size mask</summary>
        protected readonly int BufferSizeMask;

        /// <summary>Aligned (to sector size) page size in bytes</summary>
        protected readonly int AlignedPageSizeBytes;

        /// <summary>Segment size in bits</summary>
        protected readonly int LogSegmentSizeBits;

        /// <summary>Segment size</summary>
        protected readonly long SegmentSize;

        /// <summary>Log mutable fraction</summary>
        internal readonly double logMutableFraction;

        /// <summary>Circular buffer definition</summary>
        /// <remarks>The long is actually a byte*, but storing as 'long' makes going through logicalAddress/physicalAddress translation more easily</remarks>
        protected long* pagePointers;

        /// <summary>
        /// Array of pages kept to ensure the pinned pages are not garbage collected.
        /// </summary>
        protected readonly byte[][] pageArrays;

        #endregion

        #region Public addresses
        /// <summary>The maximum address of the immutable in-memory log region</summary>
        public long ReadOnlyAddress;

        /// <summary>The lowest fuzzy mutable address. This is set by OnPagesMarkedReadOnly as the address to which we are setting the
        /// <see cref="ReadOnlyAddress"/> prior to actually doing the flushes. If it is less than <see cref="ReadOnlyAddress"/> then it
        /// is the low address of the "fuzzy region" (<see cref="ReadOnlyAddress"/> is the high address of the "fuzzy region").</summary>
        public long SafeReadOnlyAddress;

        /// <summary>
        /// The lowest in-memory address in the log. While we hold the epoch this may be changed by other threads as part of ShiftHeadAddress,
        /// but as long as an address was >= HeadAddress while we held the epoch, it cannot be actually evicted until we release the epoch.
        /// </summary>
        public long HeadAddress;

        /// <summary>
        /// The lowest reliable in-memory address. This is set by OnPagesClosed as the highest address of the range it is starting to close;
        /// thus it leads <see cref="ClosedUntilAddress"/>. As long as we hold the epoch, records above this address will not be evicted.
        /// </summary>
        public long SafeHeadAddress;

        /// <summary>Flushed until address</summary>
        public long FlushedUntilAddress;

        /// <summary>
        /// The highest address that has been closed by <see cref="OnPagesClosed"/>. It will catch up to <see cref="SafeHeadAddress"/>
        /// when a region is closed.
        /// </summary>
        public long ClosedUntilAddress;

        /// <summary>The lowest valid address in the log</summary>
        public long BeginAddress;

        /// <summary>
        /// Address until which we are currently closing. Used to coordinate linear closing of pages.
        /// Only one thread will be closing pages at a time.
        /// </summary>
        long OngoingCloseUntilAddress;

        /// <inheritdoc/>
        public override string ToString()
            => $"TA {AddressString(GetTailAddress())}, ROA {AddressString(ReadOnlyAddress)}, SafeROA {AddressString(SafeReadOnlyAddress)}, HA {AddressString(HeadAddress)},"
             + $" SafeHA {AddressString(SafeHeadAddress)}, CUA {AddressString(ClosedUntilAddress)}, FUA {AddressString(FlushedUntilAddress)}, BA {AddressString(BeginAddress)},"
             + $" PgSz {PageSize}, BufSz {BufferSize}, APC {AllocatedPageCount}, MAPC {MaxAllocatedPageCount}";
        #endregion

        #region Protected device info
        /// <summary>Log Device</summary>
        protected readonly IDevice device;

        /// <summary>Sector size</summary>
        protected readonly int sectorSize;
        #endregion

        #region Private page metadata

        // Array that indicates the status of each buffer page
        internal readonly FullPageStatus[] PageStatusIndicator;
        internal readonly PendingFlushList[] PendingFlush;

        /// <summary>Global address of the current tail (next element to be allocated from the circular buffer) </summary>
        internal PageOffset TailPageOffset;

        /// <summary>Whether log is disposed</summary>
        private bool disposed = false;

        /// <summary>Whether device is a null device</summary>
        internal readonly bool IsNullDevice;

        #endregion

        #region Contained classes and related
        /// <summary>Buffer pool</summary>
        internal SectorAlignedBufferPool bufferPool;

        /// <summary>Address type for this hlog's records'</summary>
        /// <summary>Read cache eviction callback</summary>
        protected readonly Action<long, long> EvictCallback = null;

        /// <summary>Flush callback</summary>
        protected readonly Action<CommitInfo> FlushCallback = null;

        /// <summary>Whether to preallocate log on initialization</summary>
        private readonly bool PreallocateLog = false;

        /// <summary>Error handling</summary>
        private readonly ErrorList errorList = new();

        /// <summary>Observer for records entering read-only region</summary>
        internal IObserver<ITsavoriteScanIterator> onReadOnlyObserver;

        /// <summary>Observer for records getting evicted from memory (page closed). May be the same object as <see cref="logSizeTracker"/>.</summary>
        internal IObserver<ITsavoriteScanIterator> onEvictionObserver;

        /// <summary>Log size tracker; called when an operation at the Tsavorite-internal level adds or removes heap memory size 
        /// (e.g. copying to log tail or read cache, which do not call <see cref="ISessionFunctions{TInputOutput, TContext}"/>).
        /// May be the same object as <see cref="onEvictionObserver"/>.</summary>
        internal LogSizeTracker<TStoreFunctions, TAllocator> logSizeTracker;

        /// <summary>The "event" to be waited on for flush completion by the initiator of an operation</summary>
        internal CompletionEvent flushEvent;

        /// <summary>If set, this is a function to call to determine whether the object size tracker reports maximum memory size has been exceeded.</summary>
        public Func<bool> IsSizeBeyondLimit;
        #endregion

        #region Abstract and virtual methods
        /// <summary>Write async to device</summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="startPage"></param>
        /// <param name="flushPage"></param>
        /// <param name="pageSize"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        /// <param name="fuzzyStartLogicalAddress">Start address of fuzzy region, which contains old and new version records (we use this to selectively flush only old-version records during snapshot checkpoint)</param>
        protected abstract void WriteAsyncToDevice<TContext>(long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback,
            PageAsyncFlushResult<TContext> result, IDevice device, IDevice objectLogDevice, long fuzzyStartLogicalAddress);

        /// <summary>Read page from device (async)</summary>
        protected abstract void ReadAsync<TContext>(ulong alignedSourceAddress, IntPtr destinationPtr, uint aligned_read_length,
            DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device);

        /// <summary>Write page to device (async)</summary>
        protected abstract void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult);

        /// <summary>Flush checkpoint Delta to the Device</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal virtual unsafe void AsyncFlushDeltaToDevice(CircularDiskWriteBuffer flushBuffers, long startAddress, long endAddress, long prevEndAddress, long version, DeltaLog deltaLog,
            out SemaphoreSlim completedSemaphore, int throttleCheckpointFlushDelayMs)
        {
            logger?.LogTrace("Starting async delta log flush with throttling {throttlingEnabled}", throttleCheckpointFlushDelayMs >= 0 ? $"enabled ({throttleCheckpointFlushDelayMs}ms)" : "disabled");

            var _completedSemaphore = new SemaphoreSlim(0);
            completedSemaphore = _completedSemaphore;

            // If throttled, convert rest of the method into a truly async task run because issuing IO can take up synchronous time
            if (throttleCheckpointFlushDelayMs >= 0)
                _ = Task.Run(FlushRunner);
            else
                FlushRunner();

            void FlushRunner()
            {
                long startPage = GetPage(startAddress);
                long endPage = GetPage(endAddress);
                if (endAddress > GetLogicalAddressOfStartOfPage(endPage))
                    endPage++;

                long prevEndPage = GetPage(prevEndAddress);
                deltaLog.Allocate(out int entryLength, out long destPhysicalAddress);
                int destOffset = 0;

                // We perform delta capture under epoch protection with page-wise refresh for latency reasons
                bool epochTaken = epoch.ResumeIfNotProtected();

                try
                {
                    for (long p = startPage; p < endPage; p++)
                    {
                        // Check if we have the entire page safely available to process in memory
                        if (HeadAddress >= GetLogicalAddressOfStartOfPage(p) + PageSize)
                            continue;

                        // All RCU pages need to be added to delta
                        // For IPU-only pages, prune based on dirty bit
                        if ((p < prevEndPage || endAddress == prevEndAddress) && PageStatusIndicator[p % BufferSize].Dirty < version)
                            continue;

                        var logicalAddress = GetLogicalAddressOfStartOfPage(p);
                        var endLogicalAddress = logicalAddress + PageSize;
                        logicalAddress += PageHeader.Size;
                        var physicalAddress = GetPhysicalAddress(logicalAddress);

                        if (endAddress < endLogicalAddress) endLogicalAddress = endAddress;
                        Debug.Assert(endLogicalAddress > logicalAddress);
                        var endPhysicalAddress = physicalAddress + (endLogicalAddress - logicalAddress);

                        if (p == startPage)
                        {
                            var offset = (int)GetOffsetOnPage(startAddress);
                            physicalAddress += offset;
                            logicalAddress += offset;
                        }

                        while (physicalAddress < endPhysicalAddress)
                        {
                            var logRecord = _wrapper.CreateLogRecord(logicalAddress);
                            ref var info = ref logRecord.InfoRef;
                            var alignedRecordSize = logRecord.AllocatedSize;
                            if (info.Dirty)
                            {
                                info.ClearDirtyAtomic(); // there may be read locks being taken, hence atomic
                                int size = sizeof(long) + sizeof(int) + alignedRecordSize;
                                if (destOffset + size > entryLength)
                                {
                                    deltaLog.Seal(destOffset);
                                    deltaLog.Allocate(out entryLength, out destPhysicalAddress);
                                    destOffset = 0;
                                    if (destOffset + size > entryLength)
                                    {
                                        deltaLog.Seal(0);
                                        deltaLog.Allocate(out entryLength, out destPhysicalAddress);
                                    }
                                    if (destOffset + size > entryLength)
                                        throw new TsavoriteException("Insufficient page size to write delta");
                                }
                                *(long*)(destPhysicalAddress + destOffset) = logicalAddress;
                                destOffset += sizeof(long);
                                *(int*)(destPhysicalAddress + destOffset) = alignedRecordSize;
                                destOffset += sizeof(int);
                                Buffer.MemoryCopy((void*)physicalAddress, (void*)(destPhysicalAddress + destOffset), alignedRecordSize, alignedRecordSize);
                                destOffset += alignedRecordSize;
                            }
                            physicalAddress += alignedRecordSize;
                            logicalAddress += alignedRecordSize;
                        }
                        epoch.ProtectAndDrain();
                    }
                }
                finally
                {
                    if (epochTaken)
                        epoch.Suspend();
                }

                if (destOffset > 0)
                    deltaLog.Seal(destOffset);
                _completedSemaphore.Release();
            }
        }

        /// <summary>Reset the hybrid log. WARNING: assumes that threads have drained out at this point.</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public virtual void Reset()
        {
            var newBeginAddress = GetTailAddress();

            // Shift read-only addresses to tail without flushing
            _ = MonotonicUpdate(ref ReadOnlyAddress, newBeginAddress, out _);
            _ = MonotonicUpdate(ref SafeReadOnlyAddress, newBeginAddress, out _);

            // Shift head address to tail
            if (MonotonicUpdate(ref HeadAddress, newBeginAddress, out _))
            {
                // Close addresses
                OnPagesClosed(newBeginAddress);

                // Wait for pages to get closed
                while (ClosedUntilAddress < newBeginAddress)
                {
                    _ = Thread.Yield();
                    if (epoch.ThisInstanceProtected())
                        epoch.ProtectAndDrain();
                }
            }

            // Update begin address to tail
            _ = MonotonicUpdate(ref BeginAddress, newBeginAddress, out _);

            flushEvent.Initialize();
            Array.Clear(PageStatusIndicator, 0, BufferSize);
            if (PendingFlush != null)
            {
                for (int i = 0; i < BufferSize; i++)
                    PendingFlush[i]?.list?.Clear();
            }
            device.Reset();
        }

        /// <summary>Asynchronously wraps <see cref="TruncateUntilAddressBlocking(long)"/>.</summary>
        internal void TruncateUntilAddress(long toAddress) => _ = Task.Run(() => TruncateUntilAddressBlocking(toAddress));

        /// <summary>Synchronously (blocking) wraps <see cref="IDevice.TruncateUntilAddress(long)"/>; overridden when an allocator potentially has to interact with multiple devices</summary>
        protected virtual void TruncateUntilAddressBlocking(long toAddress) => device.TruncateUntilAddress(toAddress);

        /// <summary>Remove disk segment</summary>
        protected virtual void RemoveSegment(int segment) => device.RemoveSegment(segment);

        internal virtual bool TryComplete() => device.TryComplete();

        /// <summary>Dispose allocator</summary>
        public virtual void Dispose()
        {
            disposed = true;

            if (pagePointers is not null)
            {
                NativeMemory.AlignedFree((void*)pagePointers);
                pagePointers = null;
            }

            if (ownedEpoch)
                epoch.Dispose();
            bufferPool.Free();

            flushEvent.Dispose();
            notifyFlushedUntilAddressSemaphore?.Dispose();

            onReadOnlyObserver?.OnCompleted();
            onEvictionObserver?.OnCompleted();
            logSizeTracker?.Stop();
        }

        #endregion abstract and virtual methods

        private protected void VerifyCompatibleSectorSize(IDevice device)
        {
            if (sectorSize % device.SectorSize != 0)
                throw new TsavoriteException($"Allocator with sector size {sectorSize} cannot flush to device with sector size {device.SectorSize}");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void ApplyDelta(DeltaLog log, long startPage, long endPage, long recoverTo)
        {
            if (log == null)
                return;

            long pageStartLogicalAddress = GetLogicalAddressOfStartOfPage(startPage);
            long pageEndLogicalAddress = GetLogicalAddressOfStartOfPage(endPage);

            log.Reset();
            while (log.GetNext(out long physicalAddress, out int entryLength, out var type))
            {
                switch (type)
                {
                    case DeltaLogEntryType.DELTA:
                        // Delta records
                        long endAddress = physicalAddress + entryLength;
                        while (physicalAddress < endAddress)
                        {
                            var address = *(long*)physicalAddress;
                            physicalAddress += sizeof(long);
                            var size = *(int*)physicalAddress;
                            physicalAddress += sizeof(int);
                            if (address >= pageStartLogicalAddress && address < pageEndLogicalAddress)
                            {
                                var logRecord = _wrapper.CreateLogRecord(address);
                                var destination = logRecord.physicalAddress;

                                // Clear extra space (if any) in old record
                                var oldSize = logRecord.AllocatedSize;
                                if (oldSize > size)
                                    new Span<byte>((byte*)(destination + size), oldSize - size).Clear();

                                // Update with new record
                                Buffer.MemoryCopy((void*)physicalAddress, (void*)destination, size, size);

                                // Clean up temporary bits when applying the delta log
                                ref var destInfo = ref LogRecord.GetInfoRef(destination);
                                destInfo.ClearBitsForDiskImages();
                            }
                            physicalAddress += size;
                        }
                        break;
                    case DeltaLogEntryType.CHECKPOINT_METADATA:
                        if (recoverTo != -1)
                        {
                            // Only read metadata if we need to stop at a specific version
                            var metadata = new byte[entryLength];
                            unsafe
                            {
                                fixed (byte* m = metadata)
                                    Buffer.MemoryCopy((void*)physicalAddress, m, entryLength, entryLength);
                            }

                            HybridLogRecoveryInfo recoveryInfo = new();
                            using StreamReader s = new(new MemoryStream(metadata));
                            recoveryInfo.Initialize(s);
                            // Finish recovery if only specific versions are requested
                            if (recoveryInfo.version == recoverTo)
                                return;
                        }

                        break;
                    default:
                        throw new TsavoriteException("Unexpected entry type");

                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MarkPage(long logicalAddress, long version)
        {
            var pageIndex = GetPageIndexForAddress(logicalAddress);
            if (PageStatusIndicator[pageIndex].Dirty < version)
                PageStatusIndicator[pageIndex].Dirty = version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MarkPageAtomic(long logicalAddress, long version)
        {
            var pageIndex = GetPageIndexForAddress(logicalAddress);
            MonotonicUpdate(ref PageStatusIndicator[pageIndex].Dirty, version, out _);
        }

        /// <summary>
        /// This writes data from a page (or pages) for allocators that support only inline data.
        /// </summary>
        /// <param name="alignedSourceAddress">The source address, aligned to start of allocator page</param>
        /// <param name="alignedDestinationAddress">The destination address, aligned to start of allocator page</param>
        /// <param name="numBytesToWrite">Number of bytes to be written, based on allocator page range</param>
        /// <param name="callback">The callback for the operation</param>
        /// <param name="asyncResult">The callback state information, including information for the flush operation</param>
        /// <param name="device">The device to write to</param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void WriteInlinePageAsync<TContext>(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult, IDevice device)
        {
            if (asyncResult.partial)
            {
                // Write only required bytes within the page
                int aligned_start = (int)(asyncResult.fromAddress - GetLogicalAddressOfStartOfPage(asyncResult.page));
                aligned_start = (aligned_start / sectorSize) * sectorSize;

                int aligned_end = (int)(asyncResult.untilAddress - GetLogicalAddressOfStartOfPage(asyncResult.page));
                aligned_end = (aligned_end + (sectorSize - 1)) & ~(sectorSize - 1);

                numBytesToWrite = (uint)(aligned_end - aligned_start);
                device.WriteAsync(alignedSourceAddress + aligned_start, alignedDestinationAddress + (ulong)aligned_start, numBytesToWrite, callback, asyncResult);
            }
            else
            {
                // Write the whole page
                device.WriteAsync(alignedSourceAddress, alignedDestinationAddress, numBytesToWrite, callback, asyncResult);
            }
        }

        internal readonly ILogger logger;

        /// <summary>Instantiate base allocator implementation</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private protected AllocatorBase(LogSettings logSettings, TStoreFunctions storeFunctions, Func<object, TAllocator> wrapperCreator, Action<long, long> evictCallback,
                LightEpoch epoch, Action<CommitInfo> flushCallback, ILogger logger = null, ObjectIdMap transientObjectIdMap = null)
        {
            this.storeFunctions = storeFunctions;
            _wrapper = wrapperCreator(this);

            this.transientObjectIdMap = transientObjectIdMap;

            // Validation
            if (logSettings.PageCount == 0 && logSettings.MemorySize == 0)
                throw new TsavoriteException($"{nameof(logSettings.PageCount)} or {nameof(logSettings.MemorySize)} must be specified");
            if (logSettings.PageSizeBits < LogSettings.kMinPageSizeBits || logSettings.PageSizeBits > LogSettings.kMaxPageSizeBits)
                throw new TsavoriteException($"{nameof(logSettings.PageSizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxPageSizeBits}");
            if (logSettings.PageSizeBits < PageHeader.SizeBits)
                throw new TsavoriteException($"{nameof(logSettings.PageSizeBits)} must be >= PageHeader.SizeBits {PageHeader.SizeBits}");
            if (logSettings.PageCount > MemoryUtils.ArrayMaxLength)
                throw new TsavoriteException($"{nameof(logSettings.PageCount)} must be less than or equal to the maximum array length ({MemoryUtils.ArrayMaxLength})");
            if (logSettings.SegmentSizeBits < LogSettings.kMinMainLogSegmentSizeBits || logSettings.SegmentSizeBits > LogSettings.kMaxSegmentSizeBits)
                throw new TsavoriteException($"{nameof(logSettings.SegmentSizeBits)} must be between {LogSettings.kMinMainLogSegmentSizeBits} and {LogSettings.kMaxSegmentSizeBits}");
            if (logSettings.MemorySize != 0 && (logSettings.MemorySize < 1L << LogSettings.kMinMemorySizeBits || logSettings.MemorySize > 1L << LogSettings.kMaxMemorySizeBits))
                throw new TsavoriteException($"{nameof(logSettings.MemorySize)} must be between {1L << LogSettings.kMinMemorySizeBits} and {1L << LogSettings.kMaxMemorySizeBits}, or may be 0 for ReadOnly TsavoriteLog");
            if ((logSettings.MemorySize != 0) && (logSettings.MemorySize < (1L << logSettings.PageSizeBits) * 2))
                throw new TsavoriteException($"{nameof(logSettings.MemorySize)} must be at least twice the page size ({1L << logSettings.PageSizeBits})");
            if (logSettings.MutableFraction < 0.0 || logSettings.MutableFraction > 1.0)
                throw new TsavoriteException($"{nameof(logSettings.MutableFraction)} must be >= 0.0 and <= 1.0");
            if (logSettings.ReadCacheSettings is not null)
            {
                var rcs = logSettings.ReadCacheSettings;
                if (rcs.PageCount == 0 && rcs.MemorySize == 0)
                    throw new TsavoriteException($"{nameof(rcs.PageCount)} or {nameof(rcs.MemorySize)} must be specified");
                if (rcs.PageSizeBits < LogSettings.kMinPageSizeBits || rcs.PageSizeBits > LogSettings.kMaxPageSizeBits)
                    throw new TsavoriteException($"{nameof(rcs.PageSizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxPageSizeBits}");
                if (rcs.PageCount > MemoryUtils.ArrayMaxLength)
                    throw new TsavoriteException($"{nameof(rcs.PageCount)} must be less than or equal to the maximum array length ({MemoryUtils.ArrayMaxLength})");
                if (rcs.MemorySize != 0 && (rcs.MemorySize < 1L << LogSettings.kMinMemorySizeBits || rcs.MemorySize > 1L << LogSettings.kMaxMemorySizeBits))
                    throw new TsavoriteException($"{nameof(rcs.MemorySize)} must be between {1L << LogSettings.kMinMemorySizeBits} and {1L << LogSettings.kMaxMemorySizeBits}");
                if ((rcs.MemorySize != 0) && (rcs.MemorySize < (1L << rcs.PageSizeBits) * 2))
                    throw new TsavoriteException($"{nameof(logSettings.MemorySize)} must be at least twice the page size ({1L << rcs.PageSizeBits})");
                if (rcs.SecondChanceFraction < 0.0 || rcs.SecondChanceFraction > 1.0)
                    throw new TsavoriteException($"{rcs.SecondChanceFraction} must be >= 0.0 and <= 1.0");
            }

            if (logSettings.MaxInlineKeySizeBits < LogSettings.kLowestMaxInlineSizeBits || logSettings.PageSizeBits > LogSettings.kMaxStringSizeBits - 1)
                throw new TsavoriteException($"{nameof(logSettings.MaxInlineKeySizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxStringSizeBits - 1}");
            if (logSettings.MaxInlineValueSizeBits < LogSettings.kLowestMaxInlineSizeBits || logSettings.PageSizeBits > LogSettings.kMaxStringSizeBits - 1)
                throw new TsavoriteException($"{nameof(logSettings.MaxInlineValueSizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxStringSizeBits - 1}");

            this.logger = logger;
            if (logSettings.LogDevice == null)
                throw new TsavoriteException("LogSettings.LogDevice needs to be specified (e.g., use Devices.CreateLogDevice, AzureStorageDevice, or NullDevice)");

            EvictCallback = evictCallback;

            FlushCallback = flushCallback;
            PreallocateLog = logSettings.PreallocateLog;
            flushEvent.Initialize();

            IsNullDevice = logSettings.LogDevice is NullDevice;

            if (epoch == null)
            {
                this.epoch = new LightEpoch();
                ownedEpoch = true;
            }
            else
                this.epoch = epoch;

            logSettings.LogDevice.Initialize(1L << logSettings.SegmentSizeBits, epoch);
            logSettings.ObjectLogDevice?.Initialize(1L << logSettings.ObjectLogSegmentSizeBits, epoch);

            // Page size
            LogPageSizeBits = logSettings.PageSizeBits;
            PageSize = 1 << LogPageSizeBits;
            PageSizeMask = PageSize - 1;

            // Total HLOG size and MaxAllocatedPageCount. There are a couple ways MaxAllocatedPageCount can be set here; once set it
            // will never be exceeded by AllocatedPageCount, even if memory usage falls below logSettings.MemorySize. Memory
            // size tracking will be enforced only if this.logSizeTracker is set; otherwise, we set the MaxAllocatedPageCount here and
            // do not control heap memory usage.
            if (logSettings.MemorySize > 0)
            {
                // If LogSettings.PageCount is specified it becomes MaxAllocatedPageCount; otherwise MaxAllocatedPageCount will be
                // MaxMemorySize divided by page size.
                MaxAllocatedPageCount = logSettings.PageCount > 0 ? logSettings.PageCount : (int)(logSettings.MemorySize / PageSize);
            }
            else
            {
                if (logSettings.PageCount <= 0)
                    throw new TsavoriteException($"Log Memory size or PageCount must be specified");
                MaxAllocatedPageCount = logSettings.PageCount;
            }
            BufferSize = (int)NextPowerOf2(MaxAllocatedPageCount);

            BufferSizeMask = BufferSize - 1;

            logMutableFraction = logSettings.MutableFraction;

            // Segment size
            LogSegmentSizeBits = logSettings.SegmentSizeBits;
            SegmentSize = 1L << LogSegmentSizeBits;
            if (SegmentSize < PageSize)
                throw new TsavoriteException($"Segment ({SegmentSize}) must be at least of page size ({PageSize})");

            PageStatusIndicator = new FullPageStatus[BufferSize];

            if (!IsNullDevice)
            {
                PendingFlush = new PendingFlushList[BufferSize];
                for (int i = 0; i < BufferSize; i++)
                    PendingFlush[i] = new PendingFlushList();
            }
            device = logSettings.LogDevice;
            sectorSize = (int)device.SectorSize;

            if (PageSize < sectorSize)
                throw new TsavoriteException($"Page size must be at least of device sector size ({sectorSize} bytes). Set PageSizeBits accordingly.");

            AlignedPageSizeBytes = RoundUp(PageSize, sectorSize);

            if (BufferSize > 0)
            {
                pageArrays = new byte[BufferSize][];
                var bufferSizeInBytes = (nuint)RoundUp(sizeof(long*) * BufferSize, Constants.kCacheLineBytes);
                pagePointers = (long*)NativeMemory.AlignedAlloc(bufferSizeInBytes, Constants.kCacheLineBytes);
                NativeMemory.Clear(pagePointers, bufferSizeInBytes);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long GetPhysicalAddress(long logicalAddress)
        {
            if (disposed)
                ThrowTsavoriteException("GetPhysicalAddress called when disposed");

            // Index of page within the circular buffer, and offset on the page.
            var pageIndex = GetPageIndexForAddress(logicalAddress);
            var offset = GetOffsetOnPage(logicalAddress);
            return *(pagePointers + pageIndex) + offset;
        }
        internal bool IsAllocated(int pageIndex) => pagePointers[pageIndex] != 0;

        internal void ClearPage(long page, int offset = 0)
            => NativeMemory.Clear((byte*)pagePointers[page % BufferSize] + offset, (nuint)(PageSize - offset));

        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void VerifyRecoveryInfo(HybridLogCheckpointInfo recoveredHLCInfo, bool trimLog = false)
        {
            // Note: trimLog is unused right now. Can be used to trim the log to the minimum
            // segment range necessary for recovery to given checkpoint

            var diskBeginAddress = recoveredHLCInfo.info.beginAddress;
            var diskFlushedUntilAddress =
                recoveredHLCInfo.info.useSnapshotFile == 0 ?
                recoveredHLCInfo.info.finalLogicalAddress :
                recoveredHLCInfo.info.flushedLogicalAddress;

            // Delete disk segments until specified disk begin address

            // First valid disk segment required for recovery
            long firstValidSegment = (int)GetSegment(diskBeginAddress);

            // Last valid disk segment required for recovery
            var lastValidSegment = (int)GetSegment(diskFlushedUntilAddress);
            if (GetOffsetOnSegment(diskFlushedUntilAddress) == 0)
                lastValidSegment--;

            logger?.LogInformation("Recovery requires disk segments in range [{firstSegment}--{tailStartSegment}]", firstValidSegment, lastValidSegment);

            var firstAvailSegment = device.StartSegment;
            var lastAvailSegment = device.EndSegment;

            if (FlushedUntilAddress > GetFirstValidLogicalAddressOnPage(0))
            {
                var flushedUntilAddress = FlushedUntilAddress;
                int currTailSegment = (int)GetSegment(flushedUntilAddress);
                if (GetOffsetOnSegment(flushedUntilAddress) == 0)
                    currTailSegment--;

                if (currTailSegment > lastAvailSegment)
                    lastAvailSegment = currTailSegment;
            }

            logger?.LogInformation("Available segment range on device: [{firstAvailSegment}--{lastAvailSegment}]", firstAvailSegment, lastAvailSegment);

            if (firstValidSegment < firstAvailSegment)
                throw new TsavoriteException($"Unable to set first valid segment to {firstValidSegment}, first available segment on disk is {firstAvailSegment}");

            if (lastAvailSegment >= 0 && lastValidSegment > lastAvailSegment)
                throw new TsavoriteException($"Unable to set last valid segment to {lastValidSegment}, last available segment on disk is {lastAvailSegment}");

            if (trimLog)
            {
                logger?.LogInformation("Trimming disk segments until (not including) {firstSegment}", firstValidSegment);
                TruncateUntilAddressBlocking(GetStartLogicalAddressOfSegment(firstValidSegment));

                for (int s = lastValidSegment + 1; s <= lastAvailSegment; s++)
                {
                    logger?.LogInformation("Trimming tail segment {s} on disk", s);
                    RemoveSegment(s);
                }
            }
        }

        /// <summary>Allocate a pinned byte[] for the page at <paramref name="index"/></summary>
        protected void AllocatePinnedPageArray(int index)
        {
            var adjustedSize = PageSize + 2 * sectorSize;
            byte[] tmp = GC.AllocateArray<byte>(adjustedSize, true);
            long p = (long)Unsafe.AsPointer(ref tmp[0]);
            pagePointers[index] = (p + (sectorSize - 1)) & ~((long)sectorSize - 1);
            pageArrays[index] = tmp;
        }

        /// <summary>Initialize allocator</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        protected internal virtual void Initialize()
        {
            bufferPool ??= new SectorAlignedBufferPool(1, sectorSize);
            var firstValidAddress = FirstValidAddress;

            if (BufferSize > 0)
            {
                long tailPage = GetPage(firstValidAddress);
                int tailPageIndex = GetPageIndexForPage(tailPage);
                if (!IsAllocated(tailPageIndex))
                    _wrapper.AllocatePage(tailPageIndex);

                // Allocate next page as well
                int nextPageIndex = GetPageIndexForPage(tailPage + 1);
                if (!IsAllocated(nextPageIndex))
                    _wrapper.AllocatePage(nextPageIndex);
            }

            if (PreallocateLog)
            {
                for (int pageIndex = 0; pageIndex < BufferSize; pageIndex++)
                {
                    if (!IsAllocated(pageIndex))
                        _wrapper.AllocatePage(pageIndex);
                }
            }

            SafeReadOnlyAddress = firstValidAddress;
            ReadOnlyAddress = firstValidAddress;
            SafeHeadAddress = firstValidAddress;
            HeadAddress = firstValidAddress;
            ClosedUntilAddress = firstValidAddress;
            FlushedUntilAddress = firstValidAddress;
            BeginAddress = firstValidAddress;

            // Initialize TailAddress (the address of the next allocaton); this will always be nonzero.
            TailPageOffset.Page = (int)GetPage(firstValidAddress);
            TailPageOffset.Offset = (int)GetOffsetOnPage(firstValidAddress);
        }

        /// <summary>
        /// Max allocated page count; less than or equal to <see cref="BufferSize"/>. <see cref="AllocatedPageCount"/> will never exceed this,
        /// even if <see cref="logSizeTracker"/> is set and memory usage falls below its TargetSize. This is also used to handle non-powerOf2
        /// inline log sizes..
        /// </summary>
        internal int MaxAllocatedPageCount;

        /// <summary>
        /// The number of memory pages that are currently allocated in the circular buffer. Will never exceed <see cref="MaxAllocatedPageCount"/>.
        /// If there is a <see cref="logSizeTracker"/> then it manages combined inline and heap memory, and <see cref="AllocatedPageCount"/>
        /// may increase or decrease (but again, will never exceed <see cref="MaxAllocatedPageCount"/>).
        /// </summary>
        public int AllocatedPageCount;

        /// <summary>High-water mark of the number of memory pages that were allocated in the circular buffer</summary>
        public int HighWaterAllocatedPageCount;

        /// <summary>Maximum memory size in bytes</summary>
        public long MaxMemorySizeBytes => BufferSize * PageSize;

        /// <summary>Increments AllocatedPageCount. Updates MaxAllocatedPageCount if a higher number of pages have been allocated.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void IncrementAllocatedPageCount()
        {
            var newAllocatedPageCount = Interlocked.Increment(ref AllocatedPageCount);
            var currHighWaterCount = HighWaterAllocatedPageCount;
            while (currHighWaterCount < newAllocatedPageCount)
            {
                if (Interlocked.CompareExchange(ref HighWaterAllocatedPageCount, newAllocatedPageCount, currHighWaterCount) == currHighWaterCount)
                    return;
                currHighWaterCount = HighWaterAllocatedPageCount;
            }
        }

        /// <summary>Main log segment size</summary>
        public long GetMainLogSegmentSize() => SegmentSize;

        /// <summary>Object log segment size</summary>
        public virtual long GetObjectLogSegmentSize() => -1;

        /// <summary>Get tail address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetTailAddress()
        {
            var local = TailPageOffset;

            // Handle corner cases during page overflow.
            // The while loop is guaranteed to terminate because HandlePageOverflow ensures that it fixes the unstable TailPageOffset immediately.
            while (local.Offset >= PageSize)
            {
                if (local.Offset == PageSize)
                {
                    local.Page++;
                    local.Offset = 0;
                    break;
                }
                // Offset is being adjusted by overflow thread, spin-wait
                _ = Thread.Yield();
                local = TailPageOffset;
            }
            return GetLogicalAddressOfStartOfPage(local.Page) | (uint)local.Offset;
        }

        /// <summary>Get tail address without considering whether it is unstable; this is called during HandlePageOverflow as part of determining
        /// whether we must evict, which we may not be able to do; if <see cref="GetTailAddress()"/> is called on the thread that owns the tail-address
        /// stabilization, it will infinite-loop.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long UnstableGetTailAddress()
        {
            var local = TailPageOffset;
            var address = GetLogicalAddressOfStartOfPage(local.Page);
            if (local.Offset < PageSize)
                return address | (uint)local.Offset;

            // It is unstable so stay on the same page, because we will likely restabilize before size tracker evictions commence.
            return address | (uint)(PageSize - Constants.kRecordAlignment);
        }

        /// <summary>Get page index from <paramref name="logicalAddress"/></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPage(long logicalAddress) => GetPageOfAddress(logicalAddress, LogPageSizeBits);

        /// <summary>Get page index for page</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetPageIndexForPage(long page) => (int)(page % BufferSize);

        /// <summary>Get page index for address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetPageIndexForAddress(long logicalAddress) => GetPageIndexForPage(GetPageOfAddress(logicalAddress, LogPageSizeBits));

        /// <summary>Get page size</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPageSize() => PageSize;

        /// <summary>Get logical address of start of page</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetAddressOfStartOfPageOfAddress(long address) => address & ~PageSizeMask;

        /// <summary>Get offset in page</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetOffsetOnPage(long address) => address & PageSizeMask;

        /// <summary>Get start logical address; this is the 0'th byte on the page, i.e. the <see cref="PageHeader"/> start; it is *not* a valid record address
        /// (for that see <see cref="GetFirstValidLogicalAddressOnPage"/>).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetLogicalAddressOfStartOfPage(long page) => page << LogPageSizeBits;

        /// <summary>Get first valid address on a page (which is the start of the page plus sizeof(<see cref="PageHeader"/>)).</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetFirstValidLogicalAddressOnPage(long page) => (page << LogPageSizeBits) + FirstValidAddress;

        /// <summary>Get log segment index from <paramref name="logicalAddress"/></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetSegment(long logicalAddress) => logicalAddress >> LogSegmentSizeBits;

        /// <summary>Get offset in page</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetOffsetOnSegment(long address) => address & (SegmentSize - 1);

        /// <summary>Get start logical address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetStartLogicalAddressOfSegment(long segment) => segment << LogSegmentSizeBits;

        /// <summary>Get sector size for main hlog device</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetDeviceSectorSize() => sectorSize;

        /// <summary>Have the derived allocator allocate the current and next page, if needed.</summary>
        /// <remarks>The derived allocator "owns" the actual memory for the pages in its circular buffer, as it knows the details of their use,
        ///     although for efficiency we keep the <see cref="pagePointers"/> and some utility operations here in
        ///     <see cref="AllocatorBase{TStoreFunctions, TAllocator}"/> (as well as manage <see cref="TailPageOffset"/>).</remarks>
        [MethodImpl(MethodImplOptions.NoInlining)]
        void AllocatePagesWithException(int pageIndex, PageOffset localTailPageOffset, int numSlots)
        {
            try
            {
                // Allocate this page, if needed
                if (!IsAllocated(pageIndex % BufferSize))
                    _wrapper.AllocatePage(pageIndex % BufferSize);

                // Allocate next page in advance, if needed
                if (!IsAllocated((pageIndex + 1) % BufferSize))
                    _wrapper.AllocatePage((pageIndex + 1) % BufferSize);
            }
            catch
            {
                // Reset to previous tail
                localTailPageOffset.PageAndOffset -= numSlots;
                _ = Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);
                throw;
            }
        }

        /// <summary>
        /// Shift log read-only address, with an optional wait
        /// </summary>
        /// <param name="newReadOnlyAddress">Address to shift read-only until</param>
        /// <param name="wait">Wait to ensure shift is complete (may involve page flushing)</param>
        internal void ShiftReadOnlyAddressWithWait(long newReadOnlyAddress, bool wait)
        {
            // If we don't have the epoch, acquire it only long enough to launch the shift.
            if (epoch.ResumeIfNotProtected())
            {
                try
                {
                    _ = ShiftReadOnlyAddress(newReadOnlyAddress);
                }
                finally
                {
                    epoch.Suspend();
                }

                // Wait for flush to complete
                while (wait && FlushedUntilAddress < newReadOnlyAddress)
                    _ = Thread.Yield();
                return;
            }

            // Epoch already protected, so launch the shift and wait for flush to complete
            _ = ShiftReadOnlyAddress(newReadOnlyAddress);
            while (wait && FlushedUntilAddress < newReadOnlyAddress)
                epoch.ProtectAndDrain();
        }

        /// <summary>
        /// Shift log readonly and head addresses, with an optional wait on the head address shift
        /// </summary>
        /// <param name="newReadOnlyAddress">New ReadOnlyAddress</param>
        /// <param name="newHeadAddress">New HeadAddress</param>
        /// <param name="waitForEviction">Wait for operation to complete (may involve page flushing and closing)</param>
        public void ShiftAddressesWithWait(long newReadOnlyAddress, long newHeadAddress, bool waitForEviction)
        {
            // First shift read-only; force wait so that we do not close unflushed page
            ShiftReadOnlyAddressWithWait(newReadOnlyAddress, wait: true);

            // Then shift head address. If we don't have the epoch, acquire it only long enough to launch the shift.
            if (epoch.ResumeIfNotProtected())
            {
                try
                {
                    _ = ShiftHeadAddress(newHeadAddress);
                }
                finally
                {
                    epoch.Suspend();
                }

                while (waitForEviction && SafeHeadAddress < newHeadAddress)
                    _ = Thread.Yield();
                return;
            }

            // Epoch already protected, so launch the shift and wait for eviction to complete
            _ = ShiftHeadAddress(newHeadAddress);
            while (waitForEviction && SafeHeadAddress < newHeadAddress)
                epoch.ProtectAndDrain();
        }

        /// <summary>
        /// Whether we need to shift HeadAddress and ReadOnlyAddress to higher addresses when turning the page.
        /// </summary>
        /// <param name="pageIndex">The page we are turning to; it has just been allocated and TailAddress will be moving to this page</param>
        /// <param name="localTailPageOffset">Local copy of PageOffset (includes the addition of numSlots)</param>
        /// <param name="numSlots">Size of new allocation</param>
        /// <returns></returns>
        bool NeedToShiftAddress(long pageIndex, PageOffset localTailPageOffset, int numSlots)
        {
            var tailAddress = GetLogicalAddressOfStartOfPage(localTailPageOffset.Page) | ((long)(localTailPageOffset.Offset - numSlots));
            var shiftAddress = GetLogicalAddressOfStartOfPage(pageIndex);

            // First check whether we need to shift HeadAddress. If we have a logSizeTracker that's over budget then we have already issued
            // a shift if needed (and allowed by allocated page count); otherwise make sure we stay in the MaxAllocatedPageCount (which may be less than BufferSize).
            var desiredHeadAddress = HeadAddress;
            if (logSizeTracker is null || !logSizeTracker.IsBeyondSizeLimit)
            {
                var headPage = GetPage(desiredHeadAddress);
                if (pageIndex - headPage >= MaxAllocatedPageCount)
                {
                    desiredHeadAddress = GetFirstValidLogicalAddressOnPage(headPage + 1);
                    if (desiredHeadAddress > tailAddress)
                        desiredHeadAddress = tailAddress;
                    return desiredHeadAddress > HeadAddress;
                }
            }

            // Check whether we need to shift ROA based on desiredHeadAddress.
            var desiredReadOnlyAddress = CalculateReadOnlyAddress(shiftAddress, desiredHeadAddress);
            return desiredReadOnlyAddress > ReadOnlyAddress;
        }

        /// <summary>
        /// Shift log addresses when turning the page.
        /// </summary>
        /// <param name="pageIndex">The page we are turning to</param>
        /// <param name="needSHA">If true, we have determined that we must call <see cref="ShiftHeadAddress(long)"/> to Close and evict a
        ///     page before we can allocate a new one. This is done for checks that do not issue a signal to the size tracker, such as a
        ///     Flush or Close via normal wrapping operations.</param>
        void IssueShiftAddress(long pageIndex, bool needSHA)
        {
            // Issue the shift of address
            var shiftAddress = GetLogicalAddressOfStartOfPage(pageIndex);
            var tailAddress = GetTailAddress();

            // First check whether we need to shift HeadAddress. If we are not forcing for flush and have a logSizeTracker that's over budget then we have already issued
            // a shift if needed (and allowed by allocated page count); otherwise make sure we stay in the MaxAllocatedPageCount (which may be less than BufferSize).
            var desiredHeadAddress = HeadAddress;
            if (needSHA || logSizeTracker is null || !logSizeTracker.IsBeyondSizeLimit)
            {
                var headPage = GetPage(desiredHeadAddress);
                if (pageIndex - headPage >= MaxAllocatedPageCount)
                {
                    desiredHeadAddress = GetFirstValidLogicalAddressOnPage(headPage + 1);
                    if (desiredHeadAddress > tailAddress)
                        desiredHeadAddress = tailAddress;
                }
            }

            // Check whether we need to shift ROA based on desiredHeadAddress.
            var desiredReadOnlyAddress = CalculateReadOnlyAddress(shiftAddress, desiredHeadAddress);
            if (desiredReadOnlyAddress > ReadOnlyAddress)
                _ = ShiftReadOnlyAddress(desiredReadOnlyAddress);

            // Now shift HeadAddress if needed
            if (desiredHeadAddress > HeadAddress)
                _ = ShiftHeadAddress(desiredHeadAddress);
        }

        /// <summary>
        /// Throw Tsavorite exception with message. We use a method wrapper so that
        /// the caller method can execute inlined.
        /// </summary>
        /// <param name="message"></param>
        /// <exception cref="TsavoriteException"></exception>
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowTsavoriteException(string message) => throw new TsavoriteException(message);

        /// <summary>
        /// If the page we are trying to allocate is past the last page with an unflushed address region, we have to wait for the flushEvent.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool NeedToWaitForFlush(int page)
            => page >= BufferSize + GetPage(FlushedUntilAddress);   // wraps around the BufferSize

        /// <summary>
        /// If the page we are trying to allocate is past the last page with an unclosed address region, then we can retry immediately
        /// because this is called after NeedToWait, so we know we've completed the wait on flushEvent for the necessary pages to be flushed,
        /// and are waiting for OnPagesClosed to be completed. Similarly, if the log size tracker is over budget, it has already issued
        /// the ShiftHeadAddress that will close pages, so we can retry immediately.
        /// </summary>
        /// <param name="page">The page we are about to move to</param>
        /// <param name="needSHA">Returns whether we need to call <see cref="ShiftHeadAddress(long)"/> to advance HeadAddress so ClosedUntilAddress will advance</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool NeedToWaitForClose(int page, out bool needSHA)
        {
            if (page >= BufferSize + GetPage(ClosedUntilAddress))   // wraps around the BufferSize
            {
                needSHA = HeadAddress == ClosedUntilAddress;    // Need SHA to advance HeadAddress to advance ClosedUntilAddress
                return true;
            }

            needSHA = false;
            if (logSizeTracker is null || !logSizeTracker.IsBeyondSizeLimitAndCanEvict(addingPage: true))
                return false;
            logSizeTracker.Signal();
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        long HandlePageOverflow(ref PageOffset localTailPageOffset, int numSlots)
        {
            var pageIndex = localTailPageOffset.Page + 1;

            // See if this thread is trying to allocate at an offset past where one or more previous threads
            // already overflowed; if so, exit and allow the first overflow thread to proceed. Do not try to
            // remove the update to TailPageOffset that was done by this thread; that will be overwritten when
            // the first overflow thread finally completes and updates TailPageOffset.
            if (localTailPageOffset.Offset - numSlots > PageSize)
            {
                if (NeedToWaitForFlush(pageIndex))
                    return 0; // RETRY_LATER
                return -1; // RETRY_NOW
            }

            // The single thread that "owns" the page-increment proceeds below. This is the thread for which:
            // 1. Old image of offset (pre-Interlocked.Increment) is <= PageSize, and
            // 2. New image of offset (post-Interlocked.Increment) is > PageSize.

            // If we need to wait for the flushEvent, we have to RETRY_LATER
            if (NeedToWaitForFlush(pageIndex))
            {
                // Reset to previous tail so that next attempt can retry
                localTailPageOffset.PageAndOffset -= numSlots;
                _ = Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);

                // Shift only after TailPageOffset is reset to a valid state
                IssueShiftAddress(pageIndex, needSHA: true);
                return 0; // RETRY_LATER
            }

            // We next verify that:
            // 1. The next page (pageIndex) is ready to use (i.e., closed)
            // 2. We have issued any necessary address shifting at the page-turn boundary.
            // If either cannot be verified, we can ask the caller to retry now (immediately), because it is
            // an ephemeral state.
            if (NeedToWaitForClose(pageIndex, out bool needSHA) || NeedToShiftAddress(pageIndex, localTailPageOffset, numSlots))
            {
                // Reset to previous tail so that next attempt can retry
                localTailPageOffset.PageAndOffset -= numSlots;
                _ = Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);

                // Shift only after TailPageOffset is reset to a valid state
                IssueShiftAddress(pageIndex, needSHA);
                return -1; // RETRY_NOW
            }

            // Allocate next page and set new tail
            if (!IsAllocated(pageIndex % BufferSize) || !IsAllocated((pageIndex + 1) % BufferSize))
                AllocatePagesWithException(pageIndex, localTailPageOffset, numSlots);

            // Set up the TailPageOffset to account for the page header and then this allocation.
            localTailPageOffset.Page++;
            localTailPageOffset.Offset = numSlots + pageHeaderSize;
            TailPageOffset = localTailPageOffset;

            // If the logSizeTracker is active then we may have expanded the AllocatedPageCount, and thus may be able to increase ReadOnlyAddress
            // and flush, which will allow the logSizeTracker to evict more records when needed, such as when we add the page's size to the tracker.
            if (logSizeTracker is not null)
            {
                var newReadOnlyAddress = CalculateReadOnlyAddress(GetTailAddress(), HeadAddress);
                if (newReadOnlyAddress > ReadOnlyAddress)
                    _ = ShiftReadOnlyAddress(newReadOnlyAddress);
            }

            // At this point the slot is allocated and we are not allowed to refresh epochs any longer.
            // Return the first logical address after the page header.
            return GetLogicalAddressOfStartOfPage(localTailPageOffset.Page) + pageHeaderSize;   // Same as GetFirstValidLogicalAddressOnPage(localTailPageOffset.Page) but faster
        }

        /// <summary>Try allocate, no thread spinning allowed</summary>
        /// <param name="numSlots">Number of slots to allocate</param>
        /// <returns>The allocated logical address, or 0 in case of inability to allocate</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long TryAllocate(int numSlots = 1)
        {
            if (numSlots > PageSize)
                ThrowTsavoriteException("Entry does not fit on page");

            PageOffset localTailPageOffset = default;
            localTailPageOffset.PageAndOffset = TailPageOffset.PageAndOffset;
            Debug.Assert(localTailPageOffset.Offset >= pageHeaderSize, $"TailPageOffset consistency error: Offset {localTailPageOffset.Offset} should equal be >= pageHeaderSize {pageHeaderSize}");

            // If the TailAddress.Offset is already past PageSize, another thread has incremented it and is working in HandlePageOverflow.
            // Necessary to check because threads keep retrying and we do not want to overflow the offset more than once per thread.
            if (localTailPageOffset.Offset > PageSize)
            {
                if (NeedToWaitForFlush(localTailPageOffset.Page + 1))
                    return 0; // RETRY_LATER
                return -1; // RETRY_NOW
            }

            // Determine insertion index. Note that this forms a kind of "lock"; after the first thread does this, other threads that do
            // it will see that another thread got there first because the subsequent "back up by numSlots" will still be past PageSize,
            // so they will exit and RETRY in HandlePageOverflow; the first thread "owns" the overflow operation and must stabilize it.
            localTailPageOffset.PageAndOffset = Interlocked.Add(ref TailPageOffset.PageAndOffset, numSlots);

            // Slow path when we reach the end of a page.
            if (localTailPageOffset.Offset > PageSize)
            {
                // Note that TailPageOffset is now unstable -- there may be a GetTailAddress call spinning for
                // it to stabilize. Therefore, HandlePageOverflow needs to stabilize TailPageOffset immediately,
                // before performing any epoch bumps or system calls.
                return HandlePageOverflow(ref localTailPageOffset, numSlots);
            }

            return GetLogicalAddressOfStartOfPage(localTailPageOffset.Page) | ((long)(localTailPageOffset.Offset - numSlots));
        }

        /// <summary>Try allocate, spin for RETRY_NOW (logicalAddress is less than 0) case</summary>
        /// <param name="numSlots">Number of slots to allocate</param>
        /// <param name="logicalAddress">Returned address, or RETRY_LATER (if 0) indicator</param>
        /// <returns>True if we were able to allocate, else false</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAllocateRetryNow(int numSlots, out long logicalAddress)
        {
            while ((logicalAddress = TryAllocate(numSlots)) < 0)
            {
                // -1: RETRY_NOW
                _ = TryComplete();
                epoch.ProtectAndDrain();
                _ = Thread.Yield();
            }

            // 0: RETRY_LATER
            return logicalAddress != 0;
        }

        /// <summary>
        /// Calculate the new ReadOnlyAddress from the inputs. This may be called when <see cref="AllocatedPageCount"/> has changed due to memory
        /// size limits, so we cannot use a constant lag approach.
        /// </summary>
        /// <param name="tailAddress">Either the next TailAddress if doing an allocation, or the current TailAddress if trimming memory size.</param>
        /// <param name="headAddress">Either the current HeadAddress if doing an allocation, or the calculated HeadAddress if trimming memory size.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long CalculateReadOnlyAddress(long tailAddress, long headAddress)
        {
            // Snap ReadOnlyAddress to the end of the page that this calculation on logMutableFraction ends up in. This will be at
            // least the end of the HeadAddress page. And the HeadAddress page is always calculated to be below the TailAddress page.
            var readOnlyAddress = RoundUp(headAddress + (long)((1.0 - logMutableFraction) * (tailAddress - headAddress)), PageSize);
            Debug.Assert(readOnlyAddress >= headAddress, $"ReadOnlyAddress {readOnlyAddress} must be at least HeadAddress {headAddress}");
            Debug.Assert(readOnlyAddress <= tailAddress, $"ReadOnlyAddress {readOnlyAddress} must not exceed TailAddress {tailAddress}");
            return readOnlyAddress;
        }

        /// <summary>Used by applications to make the current state of the database immutable quickly</summary>
        public bool ShiftReadOnlyToTail(out long tailAddress, out SemaphoreSlim notifyDone)
        {
            notifyDone = null;
            tailAddress = GetTailAddress();
            var localTailAddress = tailAddress;
            if (MonotonicUpdate(ref ReadOnlyAddress, tailAddress, out _))
            {
                notifyFlushedUntilAddressSemaphore = new SemaphoreSlim(0);
                notifyDone = notifyFlushedUntilAddressSemaphore;
                notifyFlushedUntilAddress = localTailAddress;
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(localTailAddress));
                return true;
            }
            return false;
        }

        /// <summary>Used by applications to move read-only forward</summary>
        public bool ShiftReadOnlyAddress(long newReadOnlyAddress, bool noFlush = false)
        {
            if (MonotonicUpdate(ref ReadOnlyAddress, newReadOnlyAddress, out _))
            {
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(newReadOnlyAddress, noFlush));
                return true;
            }
            return false;
        }

        /// <summary>Shift begin address</summary>
        public void ShiftBeginAddress(long newBeginAddress, bool truncateLog, bool noFlush = false)
        {
            // First update the begin address
            if (!MonotonicUpdate(ref BeginAddress, newBeginAddress, out _))
            {
                if (truncateLog)
                    epoch.BumpCurrentEpoch(() => TruncateUntilAddress(newBeginAddress));
                return;
            }

            // Shift read-only address
            var localFlushEvent = flushEvent;
            _ = ShiftReadOnlyAddress(newBeginAddress, noFlush);

            if (!noFlush)
            {
                // Wait for flush to complete
                var spins = 0;
                while (true)
                {
                    if (FlushedUntilAddress >= newBeginAddress)
                        break;
                    if (++spins < Constants.kFlushSpinCount)
                    {
                        _ = Thread.Yield();
                        continue;
                    }
                    try
                    {
                        epoch.Suspend();
                        localFlushEvent.Wait();
                    }
                    finally
                    {
                        epoch.Resume();
                    }
                    localFlushEvent = flushEvent;
                }
            }

            // Then shift head address
            var h = MonotonicUpdate(ref HeadAddress, newBeginAddress, out _);
            if (h || truncateLog)
            {
                epoch.BumpCurrentEpoch(() =>
                {
                    if (h)
                        OnPagesClosed(newBeginAddress);
                    if (truncateLog)
                        TruncateUntilAddress(newBeginAddress);
                });
            }
        }

        /// <summary>Invokes eviction observer if set and then frees the page.</summary>
        internal virtual void EvictPageForRecovery(long page)
        {
            if (logSizeTracker is not null)
            {
                var start = GetLogicalAddressOfStartOfPage(page);
                var end = GetLogicalAddressOfStartOfPage(page + 1);
                MemoryPageScan(start, end, logSizeTracker);
            }

            // TODO: Currently we don't call DisposeRecord or DisposeValueObject on eviction; we defer to the OnEvictionObserver
            // and do nothing if that is not supplied. Should we add our own observer if they don't supply one?
            _wrapper.FreePage(page);
        }

        /// <summary>
        /// Seal: make sure there are no longer any threads writing to the page
        /// Flush: send page to secondary store
        /// </summary>
        private void OnPagesMarkedReadOnly(long newSafeReadOnlyAddress, bool noFlush = false)
        {
            if (MonotonicUpdate(ref SafeReadOnlyAddress, newSafeReadOnlyAddress, out var oldSafeReadOnlyAddress))
            {
                // Debug.WriteLine("SafeReadOnly shifted from {0:X} to {1:X}", oldSafeReadOnlyAddress, newSafeReadOnlyAddress);
                if (onReadOnlyObserver != null)
                {
                    // This scan does not need a store because it does not lock; it is epoch-protected so by the time it runs no current thread
                    // will have seen a record below the new ReadOnlyAddress as "in mutable region".
                    using var iter = Scan(store: null, oldSafeReadOnlyAddress, newSafeReadOnlyAddress, DiskScanBufferingMode.NoBuffering);
                    onReadOnlyObserver?.OnNext(iter);
                }
                AsyncFlushPagesForReadOnly(oldSafeReadOnlyAddress, newSafeReadOnlyAddress, noFlush);
            }
        }

        /// <summary>Action to be performed for when all threads have agreed that a page range is closed.</summary>
        private void OnPagesClosed(long newSafeHeadAddress)
        {
            Debug.Assert(newSafeHeadAddress > 0);
            if (MonotonicUpdate(ref SafeHeadAddress, newSafeHeadAddress, out _))
            {
                // This thread is responsible for [oldSafeHeadAddress -> newSafeHeadAddress]
                while (true)
                {
                    var _ongoingCloseUntilAddress = OngoingCloseUntilAddress;

                    // If we are closing in the middle of an ongoing OPCWorker loop, exit.
                    if (_ongoingCloseUntilAddress >= newSafeHeadAddress)
                        break;

                    // We'll continue the loop if we fail the CAS here; that means another thread extended the Ongoing range.
                    if (Interlocked.CompareExchange(ref OngoingCloseUntilAddress, newSafeHeadAddress, _ongoingCloseUntilAddress) == _ongoingCloseUntilAddress)
                    {
                        if (_ongoingCloseUntilAddress == 0)
                        {
                            // There was no other thread running the OPCWorker loop, so this thread is responsible for closing [ClosedUntilAddress -> newSafeHeadAddress]
                            OnPagesClosedWorker();
                        }
                        else
                        {
                            // There was another thread runnning the OPCWorker loop, and its ongoing close operation was successfully extended to include the new safe
                            // head address; we have no further work here.
                        }
                        return;
                    }
                    _ = Thread.Yield();
                }
            }
        }

        private void OnPagesClosedWorker()
        {
            while (true)
            {
                var closeStartAddress = ClosedUntilAddress;
                var closeEndAddress = OngoingCloseUntilAddress;

                if (EvictCallback is not null)
                    EvictCallback(closeStartAddress, closeEndAddress);

                // Process a page (possibly fragment) at a time.
                for (var closePageAddress = GetAddressOfStartOfPageOfAddress(closeStartAddress); closePageAddress < closeEndAddress; closePageAddress += PageSize)
                {
                    // Get the range on this page: the start may be 0 or greater, and the end may be end-of-page or less.
                    var start = closeStartAddress > closePageAddress ? closeStartAddress : closePageAddress;
                    var end = closeEndAddress < closePageAddress + PageSize ? closeEndAddress : closePageAddress + PageSize;

                    // This scan does not need a store because it does not lock; it is epoch-protected so by the time it runs no current thread
                    // will have seen a record below the eviction range as "in mutable region".
                    if (onEvictionObserver is not null)
                        MemoryPageScan(start, end, onEvictionObserver);

                    // If we are using a null storage device, we must also shift BeginAddress (leave it in-memory)
                    if (IsNullDevice)
                        _ = MonotonicUpdate(ref BeginAddress, end, out _);

                    // If the end of the closing range is at the end of the page, free the page
                    if (end == closePageAddress + PageSize)
                        _wrapper.FreePage((int)GetPage(closePageAddress));

                    _ = MonotonicUpdate(ref ClosedUntilAddress, end, out _);
                }

                // End if we have exhausted co-operative work
                if (Interlocked.CompareExchange(ref OngoingCloseUntilAddress, 0, closeEndAddress) == closeEndAddress)
                    break;
                _ = Thread.Yield();
            }
        }

        private void DebugPrintAddresses()
        {
            var _begin = BeginAddress;
            var _closedUntil = ClosedUntilAddress;
            var _safehead = SafeHeadAddress;
            var _head = HeadAddress;
            var _flush = FlushedUntilAddress;
            var _safereadonly = SafeReadOnlyAddress;
            var _readonly = ReadOnlyAddress;
            var _tail = GetTailAddress();

            Console.WriteLine("BeginAddress: {0}.{1}", GetPage(_begin), GetOffsetOnPage(_begin));
            Console.WriteLine("ClosedUntilAddress: {0}.{1}", GetPage(_closedUntil), GetOffsetOnPage(_closedUntil));
            Console.WriteLine("SafeHead: {0}.{1}", GetPage(_safehead), GetOffsetOnPage(_safehead));
            Console.WriteLine("Head: {0}.{1}", GetPage(_head), GetOffsetOnPage(_head));
            Console.WriteLine("FlushedUntil: {0}.{1}", GetPage(_flush), GetOffsetOnPage(_flush));
            Console.WriteLine("SafeReadOnly: {0}.{1}", GetPage(_safereadonly), GetOffsetOnPage(_safereadonly));
            Console.WriteLine("ReadOnly: {0}.{1}", GetPage(_readonly), GetOffsetOnPage(_readonly));
            Console.WriteLine("Tail: {0}.{1}", GetPage(_tail), GetOffsetOnPage(_tail));
        }

        /// <summary>
        /// Tries to shift head address to specified value
        /// </summary>
        /// <param name="desiredHeadAddress"></param>
        public long ShiftHeadAddress(long desiredHeadAddress)
        {
            // Obtain local values of variables that can change
            var currentFlushedUntilAddress = FlushedUntilAddress;

            // If the new head address would be higher than the last flushed address, cap it at the start of the last flushed address' page start.
            var newHeadAddress = desiredHeadAddress;
            if (newHeadAddress > currentFlushedUntilAddress)
                newHeadAddress = currentFlushedUntilAddress;

            if (GetOffsetOnPage(newHeadAddress) != 0)
            {
                ; // TODO: HeadAddress advancement at a finer grain than page-level; currently nothing needs to be done
            }
            if (MonotonicUpdate(ref HeadAddress, newHeadAddress, out _))
            {
                // Debug.WriteLine("Allocate: Moving head offset from {0:X} to {1:X}", oldHeadAddress, newHeadAddress);
                epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress));
            }

            return newHeadAddress;
        }

        /// <summary>
        /// Every async flush callback tries to update the flushed until address to the latest value possible
        /// TODO: Is there a better way to do this with enabling fine-grained addresses (not necessarily at page boundaries)?
        /// </summary>
        protected void ShiftFlushedUntilAddress()
        {
            var currentFlushedUntilAddress = FlushedUntilAddress;
            var page = GetPage(currentFlushedUntilAddress);

            var update = false;
            var pageLastFlushedAddress = PageStatusIndicator[page % BufferSize].LastFlushedUntilAddress;
            while (pageLastFlushedAddress >= currentFlushedUntilAddress && currentFlushedUntilAddress >= GetLogicalAddressOfStartOfPage(page))
            {
                currentFlushedUntilAddress = pageLastFlushedAddress;
                update = true;
                page++;
                pageLastFlushedAddress = PageStatusIndicator[page % BufferSize].LastFlushedUntilAddress;
            }

            if (update)
            {
                // Anything here must be valid flushes because error flushes do not set LastFlushedUntilAddress, which
                // prevents future ranges from being marked as flushed
                if (MonotonicUpdate(ref FlushedUntilAddress, currentFlushedUntilAddress, out long oldFlushedUntilAddress))
                {
                    FlushCallback?.Invoke(
                        new CommitInfo
                        {
                            FromAddress = oldFlushedUntilAddress,
                            UntilAddress = currentFlushedUntilAddress,
                            ErrorCode = 0
                        });

                    flushEvent.Set();

                    if ((oldFlushedUntilAddress < notifyFlushedUntilAddress) && (currentFlushedUntilAddress >= notifyFlushedUntilAddress))
                        _ = notifyFlushedUntilAddressSemaphore.Release();
                }
            }

            if (!errorList.Empty)
            {
                var info = errorList.GetEarliestError();
                if (info.FromAddress == FlushedUntilAddress)
                {
                    // All requests before error range has finished successfully -- this is the earliest error and we can invoke callback on it.
                    FlushCallback?.Invoke(info);
                }
                // Otherwise, do nothing and wait for the next invocation.
            }
        }

        /// <summary>Address for notification of flushed-until</summary>
        public long notifyFlushedUntilAddress;

        /// <summary>Semaphore for notification of flushed-until</summary>
        public SemaphoreSlim notifyFlushedUntilAddressSemaphore;

        /// <summary>Reset for recovery</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void RecoveryReset(long tailAddress, long headAddress, long beginAddress, long readonlyAddress)
        {
            long tailPage = GetPage(tailAddress);
            long offsetInPage = GetOffsetOnPage(tailAddress);
            TailPageOffset.Page = (int)tailPage;
            TailPageOffset.Offset = (int)offsetInPage;
            // Sometimes the tailAddress calculation ends on a page boundary and this gets into the RecoveryInfo.
            // Don't change GetTailAddress() as that may affect other calculations; instead, ensure it's set correctly here.
            if (pageHeaderSize > 0 && TailPageOffset.Offset == 0)
                TailPageOffset.Offset = pageHeaderSize;

            // Allocate current page if necessary
            var pageIndex = TailPageOffset.Page % BufferSize;
            if (!IsAllocated(pageIndex))
                _wrapper.AllocatePage(pageIndex);

            // Allocate next page as well - this is an invariant in the allocator!
            var nextPageIndex = (pageIndex + 1) % BufferSize;
            if (!IsAllocated(nextPageIndex))
                _wrapper.AllocatePage(nextPageIndex);

            BeginAddress = beginAddress;
            HeadAddress = headAddress;
            SafeHeadAddress = headAddress;
            ClosedUntilAddress = headAddress;
            FlushedUntilAddress = readonlyAddress;
            ReadOnlyAddress = readonlyAddress;
            SafeReadOnlyAddress = readonlyAddress;

            // for the last page which contains tailoffset, it must be open
            pageIndex = GetPageIndexForAddress(tailAddress);

            // clear the last page starting from tail address
            ClearPage(pageIndex, (int)GetOffsetOnPage(tailAddress));

            // Printing debug info
            logger?.LogInformation("******* Recovered HybridLog Stats *******");
            logger?.LogInformation("Head Address: {HeadAddress}", HeadAddress);
            logger?.LogInformation("Safe Head Address: {SafeHeadAddress}", SafeHeadAddress);
            logger?.LogInformation("ReadOnly Address: {ReadOnlyAddress}", ReadOnlyAddress);
            logger?.LogInformation("Safe ReadOnly Address: {SafeReadOnlyAddress}", SafeReadOnlyAddress);
            logger?.LogInformation("Tail Address: {tailAddress}", tailAddress);
        }

        /// <summary>Read a main log record to <see cref="SectorAlignedMemory"/> - used for RUMD operations.</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void AsyncReadRecordToMemory(long fromLogicalAddress, int numBytes, DeviceIOCompletionCallback callback, ref AsyncIOContext context)
        {
            context.record = GetAndPopulateReadBuffer(fromLogicalAddress, numBytes, out var alignedFileOffset, out var alignedReadLength);
            var asyncResult = new AsyncGetFromDiskResult<AsyncIOContext> { context = context };
            device.ReadAsync(alignedFileOffset, (IntPtr)asyncResult.context.record.aligned_pointer, alignedReadLength, callback, asyncResult);
        }

        /// <summary>Read inline blittable record to <see cref="SectorAlignedMemory"/>> - simple read context version. Used by TsavoriteLog.</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void AsyncReadBlittableRecordToMemory(long fromLogicalAddress, int numBytes, DeviceIOCompletionCallback callback, ref SimpleReadContext context)
        {
            context.record = GetAndPopulateReadBuffer(fromLogicalAddress, numBytes, out var alignedFileOffset, out var alignedReadLength);
            device.ReadAsync(alignedFileOffset, (IntPtr)context.record.aligned_pointer, alignedReadLength, callback, context);
        }

        private SectorAlignedMemory GetAndPopulateReadBuffer(long fromLogicalAddress, int numBytes, out ulong alignedFileOffset, out uint alignedReadLength)
        {
            var fileOffset = (ulong)(AlignedPageSizeBytes * GetPage(fromLogicalAddress) + GetOffsetOnPage(fromLogicalAddress));
            alignedFileOffset = (ulong)RoundDown((long)fileOffset, sectorSize);
            alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);
            alignedReadLength = (uint)RoundUp(alignedReadLength, sectorSize);

            var record = bufferPool.Get((int)alignedReadLength);
            record.valid_offset = (int)(fileOffset - alignedFileOffset);
            record.available_bytes = (int)(alignedReadLength - record.valid_offset);
            record.required_bytes = numBytes;
            return record;
        }

        /// <summary>Read pages from specified device(s) for recovery, with no output of the countdown event (but it is still created in the
        ///     <see cref="PageAsyncReadResult{TContext}"/> and thus must be Dispose()d).</summary>
        public void AsyncReadPagesForRecovery<TContext>(long readPageStart, int numPages, long untilAddress, TContext context,
            long devicePageOffset = 0, IDevice logDevice = null, IDevice objectLogDevice = null)
            => AsyncReadPagesForRecovery(readPageStart, numPages, untilAddress, context, out _, devicePageOffset, logDevice, objectLogDevice);

        /// <summary>Read pages from specified device for recovery, returning the countdown event</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AsyncReadPagesForRecovery<TContext>(long readPageStart, int numPages, long untilAddress, TContext context,
            out CountdownEvent completed, long devicePageOffset = 0, IDevice logDevice = null, IDevice objectLogDevice = null)
        {
            var usedDevice = logDevice ?? this.device;

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                var pageIndex = (int)(readPage % BufferSize);
                if (!IsAllocated(pageIndex))
                    _wrapper.AllocatePage(pageIndex);
                else
                    ClearPage(readPage, offset: 0);

                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    devicePageOffset = devicePageOffset,
                    context = context,
                    handle = completed,
                    maxAddressOffsetOnPage = PageSize,
                    isForRecovery = true
                };

                var offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);
                var readLength = (uint)AlignedPageSizeBytes;
                long adjustedUntilAddress = AlignedPageSizeBytes * GetPage(untilAddress) + GetOffsetOnPage(untilAddress);

                if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
                {
                    readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                    asyncResult.maxAddressOffsetOnPage = readLength;
                    readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
                }

                // If device != null then it is the snapshot file device. In that case we may have an offset into it due to FlushedUntilAddress
                // having advanced; see Recovery.cs:RecoverHybridLog.
                if (logDevice != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                // Create separate readBuffers for each main-log page, as each page launches its own async read and callbacks are on different threads.
                // Do *not* use "using" here as we need it to survive to the ReadAsync AsyncReadPagesForRecoveryCallback.
                asyncResult.readBuffers = CreateCircularReadBuffers(objectLogDevice, logger);

                // Call the overridden ReadAsync for the derived allocator class
                ReadAsync(offsetInFile, (IntPtr)pagePointers[pageIndex], readLength, AsyncReadPagesForRecoveryCallback, asyncResult, usedDevice);
            }
        }

        /// <summary>
        /// Flush page range to disk
        /// Called when all threads have agreed that a page range is sealed.
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        /// <param name="noFlush"></param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void AsyncFlushPagesForReadOnly(long fromAddress, long untilAddress, bool noFlush = false)
        {
            long startPage = GetPage(fromAddress);
            long endPage = GetPage(untilAddress);
            var numPages = (int)(endPage - startPage);

            long offsetInEndPage = GetOffsetOnPage(untilAddress);

            // Extra (partial) page being flushed
            if (offsetInEndPage > 0)
                numPages++;

            // Note: This is called synchronously from OnPagesMarkedReadOnly to kick off a flush sequence of (possibly multiple and/or partial) pages.
            // That call is gated by MonotonicUpdate, so the page indexes will increased monotonically; therefore we don't need to check the PendingFlush
            // queue for previous pages indexes. Also, flush callbacks will attempt to dequeue from PendingFlushes for FlushedUntilAddress, which again
            // increases monotonically.

            // For OA, create the buffers we will use for all ranges of the flush. This calls our callback and disposes itself when the last write of a range completes.
            var flushBuffers = CreateCircularFlushBuffers(objectLogDevice: null, logger);

            // Request asynchronous writes to the device. If waitForPendingFlushComplete is set, then a CountDownEvent is set in the callback handle.
            for (long flushPage = startPage; flushPage < (startPage + numPages); flushPage++)
            {
                // Default to writing the full page.
                long pageStartAddress = GetLogicalAddressOfStartOfPage(flushPage);
                long pageEndAddress = GetLogicalAddressOfStartOfPage(flushPage + 1);

                var asyncResult = new PageAsyncFlushResult<Empty>
                {
                    page = flushPage,
                    count = 1,
                    partial = false,
                    fromAddress = pageStartAddress,
                    untilAddress = pageEndAddress,
                    flushBuffers = flushBuffers
                };

                // If either fromAddress or untilAddress is in the middle of the page, this will be a partial page flush.
                // We'll enqueue into PendingFlush to ensure ordering of the partial-page writes.
                if (((fromAddress > pageStartAddress) && (fromAddress < pageEndAddress)) ||
                    ((untilAddress > pageStartAddress) && (untilAddress < pageEndAddress)))
                {
                    asyncResult.partial = true;

                    if (untilAddress < pageEndAddress)
                        asyncResult.untilAddress = untilAddress;

                    if (fromAddress > pageStartAddress)
                        asyncResult.fromAddress = fromAddress;
                }

                var skip = false;
                if (asyncResult.untilAddress <= BeginAddress)
                {
                    // Short circuit as no flush needed
                    _ = MonotonicUpdate(ref PageStatusIndicator[flushPage % BufferSize].LastFlushedUntilAddress, BeginAddress, out _);
                    ShiftFlushedUntilAddress();
                    skip = true;
                }

                if (IsNullDevice || noFlush)
                {
                    // Short circuit as no flush needed
                    _ = MonotonicUpdate(ref PageStatusIndicator[flushPage % BufferSize].LastFlushedUntilAddress, asyncResult.untilAddress, out _);
                    ShiftFlushedUntilAddress();
                    skip = true;
                }

                if (skip)
                    continue;

                // If there is a partial page starting point, we need to wait until the ongoing adjacent flush is completed to ensure correctness
                var index = GetPageIndexForAddress(asyncResult.fromAddress);
                if (GetOffsetOnPage(asyncResult.fromAddress) > 0)
                {
                    // Try to merge request with existing adjacent (earlier) pending requests (these have not yet begun or they
                    // would not be in the queue).
                    while (PendingFlush[index].RemovePreviousAdjacent(asyncResult.fromAddress, out var existingRequest))
                        asyncResult.fromAddress = existingRequest.fromAddress;

                    // Enqueue the (possibly merged) new work item into the queue.
                    PendingFlush[index].Add(asyncResult);

                    // Perform work from shared queue if possible: When a flush completes it updates FlushedUntilAddress. If there
                    // is an item in the shared queue that starts at FlushedUntilAddress, it can now be flushed. Flush callbacks
                    // will RemoveNextAdjacent(FlushedUntilAddress, ...) to continue the chain of flushes until the queue is empty.
                    // This will issue a write that completes in the background as we move to the next adjacent chunk (or page if
                    // this is the last chunk on the current page).
                    if (PendingFlush[index].RemoveNextAdjacent(FlushedUntilAddress, out PageAsyncFlushResult<Empty> request))
                        WriteAsync(GetPage(request.fromAddress), AsyncFlushPageCallback, request);  // Call the overridden WriteAsync for the derived allocator class
                }
                else
                {
                    // Write the entire page up to asyncResult.untilAddress (there can be no previous items in the queue).
                    Debug.Assert(PendingFlush[index].list.Count == 0, $"Expected PendingFlush count {PendingFlush[index].list.Count} to be 0");

                    // This will issue a write that completes in the background as we move to the next page.
                    WriteAsync(flushPage, AsyncFlushPageCallback, asyncResult);                     // Call the overridden WriteAsync for the derived allocator class
                }
            }
        }

        /// <summary>
        /// Flush pages asynchronously
        /// </summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="flushPageStart"></param>
        /// <param name="numPages"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        public void AsyncFlushPagesForRecovery<TContext>(long flushPageStart, int numPages, DeviceIOCompletionCallback callback, TContext context)
        {
            for (var flushPage = flushPageStart; flushPage < (flushPageStart + numPages); flushPage++)
            {
                var asyncResult = new PageAsyncFlushResult<TContext>()
                {
                    page = flushPage,
                    context = context,
                    count = 1,
                    partial = false,
                    fromAddress = GetLogicalAddressOfStartOfPage(flushPage),
                    untilAddress = GetLogicalAddressOfStartOfPage(flushPage + 1),
                    isForRecovery = true
                };

                // For OA, we do not use FlushBuffers here; we set isForRecovery to reuse the stored lengths rather than re-serializing objects,
                // using the lengths filled in during deserialization in RecoverHybridLog(Async), and when that is complete we fill in objectLogTail.
                WriteAsync(flushPage, callback, asyncResult);
            }
        }

        /// <summary>
        /// Flush pages from startPage (inclusive) to endPage (exclusive)
        /// to specified log device and obj device
        /// </summary>
        /// <param name="startPage"></param>
        /// <param name="endPage"></param>
        /// <param name="endLogicalAddress"></param>
        /// <param name="fuzzyStartLogicalAddress"></param>
        /// <param name="device"></param>
        /// <param name="objectLogDevice"></param>
        /// <param name="completedSemaphore"></param>
        /// <param name="throttleCheckpointFlushDelayMs"></param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void AsyncFlushPagesForSnapshot(CircularDiskWriteBuffer flushBuffers, long startPage, long endPage, long endLogicalAddress, long fuzzyStartLogicalAddress,
            IDevice device, IDevice objectLogDevice, out SemaphoreSlim completedSemaphore, int throttleCheckpointFlushDelayMs)
        {
            logger?.LogTrace("Starting async full log flush with throttling {throttlingEnabled}", throttleCheckpointFlushDelayMs >= 0 ? $"enabled ({throttleCheckpointFlushDelayMs}ms)" : "disabled");

            var _completedSemaphore = new SemaphoreSlim(0);
            completedSemaphore = _completedSemaphore;

            // If throttled, convert rest of the method into a truly async task run because issuing IO can take up synchronous time
            if (throttleCheckpointFlushDelayMs >= 0)
                _ = Task.Run(FlushRunner);
            else
                FlushRunner();

            void FlushRunner()
            {
                var totalNumPages = (int)(endPage - startPage);

                var flushCompletionTracker = new FlushCompletionTracker(_completedSemaphore, throttleCheckpointFlushDelayMs >= 0 ? new SemaphoreSlim(0) : null, totalNumPages);

                // Create the buffers we will use for all ranges of the flush (if we are ObjectAllocator). This calls our callback when the last write of a partial flush completes.
                for (long flushPage = startPage; flushPage < endPage; flushPage++)
                {
                    long flushPageAddress = GetLogicalAddressOfStartOfPage(flushPage);
                    var pageSize = PageSize;
                    if (flushPage == endPage - 1)
                        pageSize = (int)(endLogicalAddress - flushPageAddress);

                    var asyncResult = new PageAsyncFlushResult<Empty>
                    {
                        flushCompletionTracker = flushCompletionTracker,
                        page = flushPage,
                        fromAddress = flushPageAddress,
                        untilAddress = flushPageAddress + pageSize,
                        count = 1,
                        flushBuffers = flushBuffers
                    };

                    // Intended destination is flushPage
                    WriteAsyncToDevice(startPage, flushPage, pageSize, AsyncFlushPageToDeviceCallback, asyncResult, device, objectLogDevice, fuzzyStartLogicalAddress);

                    if (throttleCheckpointFlushDelayMs >= 0)
                    {
                        flushCompletionTracker.WaitOneFlush();
                        Thread.Sleep(throttleCheckpointFlushDelayMs);
                    }
                }
            }
        }

        /// <summary>
        /// Get a single record from the disk.
        /// </summary>
        /// <param name="fromLogicalAddress">Start of the record</param>
        /// <param name="numBytes">Number of bytes to be read (may be less than actual record size)</param>
        /// <param name="context">The <see cref="AsyncIOContext"/> of the operation. This is passed by value, not reference; in the iterator case, it is
        ///     the completionEvent's contained request, and populating it will result in prematurely freeing the record.</param>
        internal void AsyncGetFromDisk(long fromLogicalAddress, int numBytes, AsyncIOContext context)
        {
            // If this is a protected thread, we must wait to issue the Read operation. Spin until the device is not throttled,
            // draining events on each iteration, but do not release the epoch.
            if (epoch.ThisInstanceProtected())
            {
                while (device.Throttle())
                {
                    _ = device.TryComplete();
                    _ = Thread.Yield();
                    epoch.ProtectAndDrain();
                }
            }

            AsyncReadRecordToMemory(fromLogicalAddress, numBytes, AsyncGetFromDiskCallback, ref context);
        }

        /// <summary>
        /// Read pages from specified device
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void AsyncReadPageFromDeviceToFrame<TContext>(CircularDiskReadBuffer readBuffers,
                                        long readPage,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        BlittableFrame frame,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null, IDevice objectLogDevice = null, CancellationTokenSource cts = null)
        {
            var usedDevice = device ?? this.device;

            completed = new CountdownEvent(1);

            int pageIndex = (int)(readPage % frame.frameSize);
            if (frame.frame[pageIndex] == null)
                frame.Allocate(pageIndex);
            else
                frame.Clear(pageIndex);

            var asyncResult = new PageAsyncReadResult<TContext>()
            {
                page = readPage,
                context = context,
                handle = completed,
                cts = cts,
                readBuffers = readBuffers
            };

            ulong offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);
            uint readLength = (uint)AlignedPageSizeBytes;
            long adjustedUntilAddress = AlignedPageSizeBytes * GetPage(untilAddress) + GetOffsetOnPage(untilAddress);

            if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
            {
                readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
            }

            if (device != null)
                offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

            ReadAsync(offsetInFile, (IntPtr)frame.GetPhysicalAddress(pageIndex), readLength, callback, asyncResult, usedDevice);
        }

        /// <summary>
        /// Checks to see if we have a full record, or at least enough to compare the key.
        /// </summary>
        /// <param name="ctx">The context from the IO operation</param>
        /// <param name="prevAddressToRead">If we return false, the address to issue the next IO for</param>
        /// <param name="prevLengthToRead">If we return false, the number of bytes to issue the next IO for</param>
        /// <returns>True if we have the full record and the key was the requested key; if the record is fully inline, then the ctx.diskLogRecord is set and the ctx.record is transferred to it.
        /// Otherwise it is false, and:
        /// <list type="bullet">
        ///     <item>If the key was present, it did not match ctx.requestKey; <paramref name="prevAddressToRead"/> is recordInfo.PreviousAddress, and <paramref name="prevLengthToRead"/>
        ///         is the initial IO size.</item>
        ///     <item>Otherwise, the data we have is not sufficient to determine record length, or we know the length and it is greater than the data we have now.
        ///         <paramref name="prevAddressToRead"/> is the same address we just read, and <paramref name="prevLengthToRead"/>is one of:</item>
        ///         <list type="bullet">
        ///             <item>If we did not have enough data to determine required length, we use the initial IO size. This should seldom happen as we issue the initial
        ///                 IO request with this size, but perhaps this is called with a partial buffer.</item>
        ///             <item>Otherwise, we know the data length needed, and we set <paramref name="prevLengthToRead"/> to that.</item>
        ///         </list>
        /// </list>
        /// </returns>
        /// <remarks>If we have a complete record and the key passes the comparison and we have overflow or objects, then this will be overridden by a derived class (see 
        ///     <see cref="ObjectAllocatorImpl{TStoreFunctions}"/>) which will issue additional reads to retrieve those objects.</remarks>
        private protected virtual bool VerifyRecordFromDiskCallback(ref AsyncIOContext ctx, out long prevAddressToRead, out int prevLengthToRead)
        {
            // TODO: Optimize for non-ReadAtAddress tombstoned records to not have to retrieve the full record or, if we have it, not deserialize objects.

            // Initialize to "key is not present (data too small) or does not match so get previous record" length to read
            prevLengthToRead = IStreamBuffer.InitialIOSize;

            // See if we have a complete record.
            var currentLength = ctx.record.available_bytes;
            if (currentLength >= RecordInfo.Size + RecordDataHeader.MinHeaderBytes)
            {
                var ptr = ctx.record.GetValidPointer();
                var recordInfo = *(RecordInfo*)ptr;
                var dataHeader = new RecordDataHeader(ptr + RecordInfo.Size);
                var (numKeyLengthBytes, numRecordLengthBytes) = dataHeader.DeconstructKVByteLengths(out var headerLength);

                // GetRecordLength is always safe, because it is in the second sizeof(ulong) and we round up to 8-byte alignment.
                var recordLength = dataHeader.GetRecordLength(numRecordLengthBytes);
                if (currentLength <= headerLength)
                {
                    prevLengthToRead = recordLength;
                    goto RereadCurrent;
                }

                // Initialize to "invalid record or key does not match so get previous record" address to read
                prevAddressToRead = recordInfo.PreviousAddress;

                if (recordInfo.Invalid) // includes IsNull
                    return false;

                var offsetToKeyStart = dataHeader.GetOffsetToKeyStart(headerLength);

                // If the length is up to offsetToKeyStart, we can read the full lengths. If not, we'll fall through to reread the current record.
                if (currentLength >= offsetToKeyStart)
                {
                    var keyLength = dataHeader.GetKeyLength(numKeyLengthBytes, numRecordLengthBytes);
                    var keyStartPtr = ptr + offsetToKeyStart;

                    // We have the full key if it is inline, so check for a match if we had a requested key, and return if not.
                    if (!ctx.requestKey.IsEmpty && recordInfo.KeyIsInline && !storeFunctions.KeysEqual(ctx.requestKey, new ReadOnlySpan<byte>(keyStartPtr, keyLength)))
                        return false;

                    // Keys match. If we have the full record, return success; otherwise we'll drop through to read the full record with the length we now know.
                    if (currentLength >= recordLength)
                    {
                        ctx.diskLogRecord = DiskLogRecord.TransferFrom(ref ctx.record, transientObjectIdMap);
                        return true;
                    }
                }
            }

        RereadCurrent:
            // Either we didn't have the full record size, or we didn't have enough bytes to even read the full record size. Either way, prevLengthToRead
            // is set for a re-read of the same record.
            prevAddressToRead = ctx.logicalAddress;
            return false;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AsyncGetFromDiskCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError("AsyncGetFromDiskCallback error: {errorCode}", errorCode);

            var result = (AsyncGetFromDiskResult<AsyncIOContext>)context;
            var ctx = result.context;
            try
            {
                // Note: don't test for (numBytes >= ctx.record.required_bytes) for this initial read, as the file may legitimately end before the
                // InitialIOSize request can be fulfilled.
                ctx.record.available_bytes = (int)numBytes;

                if (!VerifyRecordFromDiskCallback(ref ctx, out var prevAddressToRead, out var prevLengthToRead))
                {
                    Debug.Assert(!(*(RecordInfo*)ctx.record.GetValidPointer()).Invalid, $"Invalid records should not be in the hash chain for pending IO; address {ctx.logicalAddress}");

                    // Either we had an incomplete record and we're re-reading the current record, or the record Key didn't match and we're reading the previous record
                    // in the chain. If the record to read is in the range to resolve then issue the read, else fall through to signal "IO complete".
                    ctx.logicalAddress = prevAddressToRead;
                    if (ctx.logicalAddress >= BeginAddress && ctx.logicalAddress >= ctx.minAddress)
                    {
                        ctx.DisposeRecord();
                        AsyncGetFromDisk(ctx.logicalAddress, prevLengthToRead, ctx);
                        return;
                    }
                }

                // Either we have a full record with a key match or we are below the range to retrieve (which ContinuePending* will detect), so we're done.
                if (ctx.completionEvent is not null)
                    ctx.completionEvent.Set(ref ctx);
                else
                    ctx.callbackQueue.Enqueue(ctx);
            }
            catch (Exception e)
            {
                logger?.LogError(e, "AsyncGetFromDiskCallback error");
                ctx.DisposeRecord();
                if (ctx.completionEvent is not null)
                    ctx.completionEvent.SetException(e);
                else
                    throw;
            }
        }

        /// <summary>
        /// IOCompletion callback for page flush
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="numBytes"></param>
        /// <param name="context"></param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AsyncFlushPageCallback(uint errorCode, uint numBytes, object context)
        {
            try
            {
                if (errorCode != 0)
                    logger?.LogError("AsyncFlushPageCallback error: {errorCode}", errorCode);

                // Set the page status to flushed
                var result = (PageAsyncFlushResult<Empty>)context;

                if (result.Release() == 0)
                {
                    if (errorCode != 0)
                    {
                        // Note down error details and trigger handling only when we are certain this is the earliest error among currently issued flushes
                        errorList.Add(new CommitInfo { FromAddress = result.fromAddress, UntilAddress = result.untilAddress, ErrorCode = errorCode });
                    }
                    else
                    {
                        // There is no failure so update the page's last flushed until address.
                        _ = MonotonicUpdate(ref PageStatusIndicator[result.page % BufferSize].LastFlushedUntilAddress, result.untilAddress, out _);
                    }

                    ShiftFlushedUntilAddress();
                }

                // Continue the chained flushes, popping the next request from the queue if it is adjacent.
                var _flush = FlushedUntilAddress;
                if (GetOffsetOnPage(_flush) > 0 && PendingFlush[GetPage(_flush) % BufferSize].RemoveNextAdjacent(_flush, out PageAsyncFlushResult<Empty> request))
                {
                    request.flushBuffers = result.flushBuffers;  // Reuse the flush buffers from the completed flush to continue the flush chain
                    WriteAsync(GetPage(request.fromAddress), AsyncFlushPageCallback, request);  // Call the overridden WriteAsync for the derived allocator class
                }
            }
            catch when (disposed) { }
        }

        internal void UnsafeSkipError(CommitInfo info)
        {
            try
            {
                errorList.TruncateUntil(info.UntilAddress);
                var page = GetPage(info.FromAddress);
                _ = MonotonicUpdate(ref PageStatusIndicator[page % BufferSize].LastFlushedUntilAddress, info.UntilAddress, out _);
                ShiftFlushedUntilAddress();
                var _flush = FlushedUntilAddress;
                if (GetOffsetOnPage(_flush) > 0 && PendingFlush[GetPage(_flush) % BufferSize].RemoveNextAdjacent(_flush, out PageAsyncFlushResult<Empty> request))
                {
                    // Reuse the flush buffers from the completed flush to continue the flush chain
                    WriteAsync(GetPage(request.fromAddress), AsyncFlushPageCallback, request);  // Call the overridden WriteAsync for the derived allocator class
                }
            }
            catch when (disposed) { }
        }

        /// <summary>
        /// IOCompletion callback for page flush
        /// </summary>
        /// <param name="errorCode"></param>
        /// <param name="numBytes"></param>
        /// <param name="context"></param>
        protected void AsyncFlushPageToDeviceCallback(uint errorCode, uint numBytes, object context)
        {
            try
            {
                if (errorCode != 0)
                    logger?.LogError("AsyncFlushPageToDeviceCallback error: {errorCode}", errorCode);

                var result = (PageAsyncFlushResult<Empty>)context;
                var epochTaken = epoch.ResumeIfNotProtected();

                // Unset dirty bit for flushed pages
                try
                {
                    var startAddress = GetLogicalAddressOfStartOfPage(result.page);
                    var endAddress = startAddress + PageSize;

                    // First make sure we're not trying to process a logical address that's in a page header.
                    startAddress += PageHeader.Size;

                    if (result.fromAddress > startAddress)
                        startAddress = result.fromAddress;
                    if (result.untilAddress < endAddress)
                        endAddress = result.untilAddress;

                    var _readOnlyAddress = SafeReadOnlyAddress;
                    if (_readOnlyAddress > startAddress)
                        startAddress = _readOnlyAddress;
                    if (_readOnlyAddress > endAddress)
                        endAddress = _readOnlyAddress;

                    var flushWidth = (int)(endAddress - startAddress);

                    if (flushWidth > 0)
                    {
                        var physicalAddress = GetPhysicalAddress(startAddress);
                        var endPhysicalAddress = physicalAddress + flushWidth;

                        while (physicalAddress < endPhysicalAddress)
                        {
                            var logRecord = _wrapper.CreateLogRecord(startAddress);
                            ref var info = ref logRecord.InfoRef;
                            var alignedRecordSize = logRecord.AllocatedSize;
                            if (info.Dirty)
                                info.ClearDirtyAtomic(); // there may be read locks being taken, hence atomic
                            physicalAddress += alignedRecordSize;
                        }
                    }
                }
                finally
                {
                    if (epochTaken)
                        epoch.Suspend();
                }

                _ = result.Release();
            }
            catch when (disposed) { }
        }

        internal string PrettyPrintLogicalAddress(long logicalAddress) => $"{logicalAddress}:{GetPage(logicalAddress)}.{GetOffsetOnPage(logicalAddress)}";
    }
}