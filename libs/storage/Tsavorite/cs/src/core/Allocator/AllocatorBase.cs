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
    using static LogAddress;

    /// <summary>
    /// Base class for hybrid log memory allocator. Contains utility methods, some of which are not performance-critical so can be virtual.
    /// </summary>
    public abstract unsafe partial class AllocatorBase<TStoreFunctions, TAllocator> : IDisposable
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        /// <summary>The epoch we are operating with</summary>
        protected readonly LightEpoch epoch;
        /// <summary>Whether we own (and thus must dispose) <see cref="epoch"/></summary>
        private readonly bool ownedEpoch;

        /// <summary>The store functions for this instance of TsavoriteKV</summary>
        internal readonly TStoreFunctions storeFunctions;

        /// <summary>The fully-derived allocator struct wrapper (so calls on it are inlined rather than virtual) for this log.</summary>
        internal readonly TAllocator _wrapper;

        /// <summary>Sometimes it's useful to know this explicitly rather than rely on method overrides etc.</summary>
        internal bool IsObjectAllocator = false;

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

        /// <summary>Total hybrid log size (bits)</summary>
        protected readonly int LogTotalSizeBits;

        /// <summary>Total hybrid log size (bytes)</summary>
        protected readonly long LogTotalSizeBytes;

        /// <summary>Segment size in bits</summary>
        protected readonly int LogSegmentSizeBits;

        /// <summary>Segment size</summary>
        protected readonly long SegmentSize;

        /// <summary>Segment buffer size</summary>
        protected readonly int SegmentBufferSize;

        protected readonly int maxInlineKeySize = LogSettings.kMaxInlineKeySize;

        protected readonly int maxInlineValueSize = int.MaxValue;

        /// <summary>How many pages do we leave empty in the in-memory buffer (between 0 and BufferSize-1)</summary>
        private int emptyPageCount;

        /// <summary>HeadAddress offset from tail (currently page-aligned)</summary>
        internal long HeadAddressLagOffset;

        /// <summary>Log mutable fraction</summary>
        protected readonly double LogMutableFraction;

        /// <summary>ReadOnlyAddress offset from tail (currently page-aligned)</summary>
        protected long ReadOnlyAddressLagOffset;

        /// <summary>Circular buffer definition</summary>
        /// <remarks>The long is actually a byte*, but storing as 'long' makes going through logicalAddress/physicalAddress translation more easily</remarks>
        protected long* pagePointers;

        #endregion

        #region Public addresses
        /// <summary>The maximum address of the immutable in-memory log region</summary>
        public long ReadOnlyAddress;

        /// <summary>Safe read-only address</summary>
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
             + $" SafeHA {AddressString(SafeHeadAddress)}, CUA {AddressString(ClosedUntilAddress)}, FUA {AddressString(FlushedUntilAddress)}, BA {AddressString(BeginAddress)}";
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
        private PageOffset TailPageOffset;

        /// <summary>Whether log is disposed</summary>
        private bool disposed = false;

        /// <summary>Whether device is a null device</summary>
        internal readonly bool IsNullDevice;

        #endregion

        #region Contained classes and related
        /// <summary>Buffer pool</summary>
        internal SectorAlignedBufferPool bufferPool;

        /// <summary>Address type for this hlog's records'</summary>
        private long addressTypeMask;

        /// <summary>Read cache eviction callback</summary>
        protected readonly Action<long, long> EvictCallback = null;

        /// <summary>Flush callback</summary>
        protected readonly Action<CommitInfo> FlushCallback = null;

        /// <summary>Whether to preallocate log on initialization</summary>
        private readonly bool PreallocateLog = false;

        /// <summary>Error handling</summary>
        private readonly ErrorList errorList = new();

        /// <summary>Observer for records entering read-only region</summary>
        internal IObserver<ITsavoriteScanIterator> OnReadOnlyObserver;

        /// <summary>Observer for records getting evicted from memory (page closed)</summary>
        internal IObserver<ITsavoriteScanIterator> OnEvictionObserver;

        /// <summary>Observer for records brought into memory by deserializing pages</summary>
        internal IObserver<ITsavoriteScanIterator> OnDeserializationObserver;

        /// <summary>The "event" to be waited on for flush completion by the initiator of an operation</summary>
        internal CompletionEvent FlushEvent;

        /// <summary>If set, this is a function to call to determine whether the object size tracker reports maximum memory size has been exceeded.</summary>
        public Func<bool> IsSizeBeyondLimit;

        /// <summary>The TsavoriteBase implemention. Currently used for hash table lookup for PatchExpandedAddresses.</summary>
        internal TsavoriteBase storeBase;
        #endregion

        #region Abstract and virtual methods
        /// <summary>Initialize fully derived allocator</summary>
        public abstract void Initialize();

        /// <summary>Write async to device</summary>
        /// <typeparam name="TContext"></typeparam>
        /// <param name="startPage"></param>
        /// <param name="flushPage"></param>
        /// <param name="pageSize"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        /// <param name="device"></param>
        /// <param name="fuzzyStartLogicalAddress">Start address of fuzzy region, which contains old and new version records (we use this to selectively flush only old-version records during snapshot checkpoint)</param>
        protected abstract void WriteAsyncToDevice<TContext>(long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> result, IDevice device, long fuzzyStartLogicalAddress);

        /// <summary>Read page from device (async)</summary>
        protected abstract void ReadAsync<TContext>(ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length, DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device);

        /// <summary>Write page to device (async)</summary>
        protected abstract void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult);

        /// <summary>Flush checkpoint Delta to the Device</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal virtual unsafe void AsyncFlushDeltaToDevice(long startAddress, long endAddress, long prevEndAddress, long version, DeltaLog deltaLog, out SemaphoreSlim completedSemaphore, int throttleCheckpointFlushDelayMs)
        {
            logger?.LogTrace("Starting async delta log flush with throttling {throttlingEnabled}", throttleCheckpointFlushDelayMs >= 0 ? $"enabled ({throttleCheckpointFlushDelayMs}ms)" : "disabled");

            var _completedSemaphore = new SemaphoreSlim(0);
            completedSemaphore = _completedSemaphore;

            if (throttleCheckpointFlushDelayMs >= 0)
                _ = Task.Run(FlushRunner);
            else
                FlushRunner();

            void FlushRunner()
            {
                long startPage = GetPage(startAddress);
                long endPage = GetPage(endAddress);
                if (endAddress > _wrapper.GetStartLogicalAddressOfPage(endPage))
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
                        if (HeadAddress >= GetStartLogicalAddressOfPage(p) + PageSize)
                            continue;

                        // All RCU pages need to be added to delta
                        // For IPU-only pages, prune based on dirty bit
                        if ((p < prevEndPage || endAddress == prevEndAddress) && PageStatusIndicator[p % BufferSize].Dirty < version)
                            continue;

                        var logicalAddress = GetStartLogicalAddressOfPage(p);
                        var physicalAddress = GetPhysicalAddress(logicalAddress);

                        var endLogicalAddress = logicalAddress + PageSize;
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
                            var (_, alignedRecordSize) = logRecord.GetInlineRecordSizes();
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

            FlushEvent.Initialize();
            Array.Clear(PageStatusIndicator, 0, BufferSize);
            if (PendingFlush != null)
            {
                for (int i = 0; i < BufferSize; i++)
                    PendingFlush[i]?.list?.Clear();
            }
            device.Reset();
        }

        /// <summary>Wraps <see cref="IDevice.TruncateUntilAddress(long)"/> when an allocator potentially has to interact with multiple devices</summary>
        protected virtual void TruncateUntilAddress(long toAddress)
        {
            _ = Task.Run(() => device.TruncateUntilAddress(AbsoluteAddress(toAddress)));
        }

        /// <summary>Wraps <see cref="IDevice.TruncateUntilAddress(long)"/> when an allocator potentially has to interact with multiple devices</summary>
        protected virtual void TruncateUntilAddressBlocking(long toAddress) => device.TruncateUntilAddress(AbsoluteAddress(toAddress));

        /// <summary>Remove disk segment</summary>
        protected virtual void RemoveSegment(int segment) => device.RemoveSegment(segment);

        internal virtual bool TryComplete() => device.TryComplete();

        /// <summary>Dispose allocator</summary>
        public virtual void Dispose()
        {
            disposed = true;

            if (pagePointers is not null)
            {
                for (var ii = 0; ii < BufferSize; ii++)
                {
                    if (pagePointers[ii] != 0)
                        NativeMemory.AlignedFree((void*)pagePointers[ii]);
                }
                NativeMemory.AlignedFree((void*)pagePointers);
                pagePointers = null;
            }

            if (ownedEpoch)
                epoch.Dispose();
            bufferPool.Free();

            FlushEvent.Dispose();
            notifyFlushedUntilAddressSemaphore?.Dispose();

            OnReadOnlyObserver?.OnCompleted();
            OnEvictionObserver?.OnCompleted();
        }

        #endregion abstract and virtual methods

        private protected void VerifyCompatibleSectorSize(IDevice device)
        {
            if (sectorSize % device.SectorSize != 0)
                throw new TsavoriteException($"Allocator with sector size {sectorSize} cannot flush to device with sector size {device.SectorSize}");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        internal unsafe void ApplyDelta(DeltaLog log, long startPage, long endPage, long recoverTo)
        {
            if (log == null) return;

            long startLogicalAddress = _wrapper.GetStartLogicalAddressOfPage(startPage);
            long endLogicalAddress = _wrapper.GetStartLogicalAddressOfPage(endPage);

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
                            if (address >= startLogicalAddress && address < endLogicalAddress)
                            {
                                var logRecord = _wrapper.CreateLogRecord(address);
                                var destination = logRecord.physicalAddress;

                                // Clear extra space (if any) in old record
                                var oldSize = logRecord.GetInlineRecordSizes().allocatedSize;
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
                            if (recoveryInfo.version == recoverTo) return;
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
        /// This writes data from a page (or pages) that does not need to be expanded.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal void WriteInlinePageAsync<TContext>(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                IDevice device)
        {
            if (asyncResult.partial)
            {
                // Write only required bytes within the page
                int aligned_start = (int)(asyncResult.fromAddress - GetStartLogicalAddressOfPage(asyncResult.page));
                aligned_start = (aligned_start / sectorSize) * sectorSize;

                int aligned_end = (int)(asyncResult.untilAddress - GetStartLogicalAddressOfPage(asyncResult.page));
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

        internal long SetAddressType(long address) => address | addressTypeMask;

        internal long GetReadOnlyAddressLagOffset() => ReadOnlyAddressLagOffset;

        protected readonly ILogger logger;

        /// <summary>Instantiate base allocator implementation</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private protected AllocatorBase(LogSettings logSettings, TStoreFunctions storeFunctions, Func<object, TAllocator> wrapperCreator, Action<long, long> evictCallback,
                LightEpoch epoch, Action<CommitInfo> flushCallback, ILogger logger, bool isObjectAllocator = false)
        {
            this.storeFunctions = storeFunctions;
            _wrapper = wrapperCreator(this);

            this.IsObjectAllocator = isObjectAllocator;
            if (isObjectAllocator)
            {
                maxInlineKeySize = 1 << logSettings.MaxInlineKeySizeBits;
                maxInlineValueSize = 1 << logSettings.MaxInlineValueSizeBits;
            }

            // Validation
            if (logSettings.PageSizeBits < LogSettings.kMinPageSizeBits || logSettings.PageSizeBits > LogSettings.kMaxPageSizeBits)
                throw new TsavoriteException($"{nameof(logSettings.PageSizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxPageSizeBits}");
            if (logSettings.SegmentSizeBits < LogSettings.kMinSegmentSizeBits || logSettings.SegmentSizeBits > LogSettings.kMaxSegmentSizeBits)
                throw new TsavoriteException($"{nameof(logSettings.SegmentSizeBits)} must be between {LogSettings.kMinSegmentSizeBits} and {LogSettings.kMaxSegmentSizeBits}");
            if (logSettings.MemorySizeBits != 0 && (logSettings.MemorySizeBits < LogSettings.kMinMemorySizeBits || logSettings.MemorySizeBits > LogSettings.kMaxMemorySizeBits))
                throw new TsavoriteException($"{nameof(logSettings.MemorySizeBits)} must be between {LogSettings.kMinMemorySizeBits} and {LogSettings.kMaxMemorySizeBits}, or may be 0 for ReadOnly TsavoriteLog");
            if (logSettings.MutableFraction < 0.0 || logSettings.MutableFraction > 1.0)
                throw new TsavoriteException($"{nameof(logSettings.MutableFraction)} must be >= 0.0 and <= 1.0");
            if (logSettings.ReadCacheSettings is not null)
            {
                var rcs = logSettings.ReadCacheSettings;
                if (rcs.PageSizeBits < LogSettings.kMinPageSizeBits || rcs.PageSizeBits > LogSettings.kMaxPageSizeBits)
                    throw new TsavoriteException($"{nameof(rcs.PageSizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxPageSizeBits}");
                if (rcs.MemorySizeBits < LogSettings.kMinMemorySizeBits || rcs.MemorySizeBits > LogSettings.kMaxMemorySizeBits)
                    throw new TsavoriteException($"{nameof(rcs.MemorySizeBits)} must be between {LogSettings.kMinMemorySizeBits} and {LogSettings.kMaxMemorySizeBits}");
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
            addressTypeMask = GetLogAddressType(EvictCallback is not null);

            FlushCallback = flushCallback;
            PreallocateLog = logSettings.PreallocateLog;
            FlushEvent.Initialize();

            IsNullDevice = logSettings.LogDevice is NullDevice;

            if (epoch == null)
            {
                this.epoch = new LightEpoch();
                ownedEpoch = true;
            }
            else
                this.epoch = epoch;

            logSettings.LogDevice.Initialize(1L << logSettings.SegmentSizeBits, epoch);

            // Page size
            LogPageSizeBits = logSettings.PageSizeBits;
            PageSize = 1 << LogPageSizeBits;
            PageSizeMask = PageSize - 1;

            // Total HLOG size
            LogTotalSizeBits = logSettings.MemorySizeBits;
            LogTotalSizeBytes = 1L << LogTotalSizeBits;
            BufferSize = (int)(LogTotalSizeBytes / PageSize);
            BufferSizeMask = BufferSize - 1;

            LogMutableFraction = logSettings.MutableFraction;

            // Segment size
            LogSegmentSizeBits = logSettings.SegmentSizeBits;
            SegmentSize = 1L << LogSegmentSizeBits;
            SegmentBufferSize = 1 + (LogTotalSizeBytes / SegmentSize < 1 ? 1 : (int)(LogTotalSizeBytes / SegmentSize));

            if (SegmentSize < PageSize)
                throw new TsavoriteException($"Segment ({SegmentSize}) must be at least of page size ({PageSize})");

            if ((LogTotalSizeBits != 0) && (LogTotalSizeBytes < PageSize * 2))
                throw new TsavoriteException($"Memory size ({LogTotalSizeBytes}) must be at least twice the page size ({PageSize})");

            // Readonlymode has MemorySizeBits 0 => skip the check
            if (logSettings.MemorySizeBits > 0 && logSettings.MinEmptyPageCount > MaxEmptyPageCount)
                throw new TsavoriteException($"MinEmptyPageCount ({logSettings.MinEmptyPageCount}) can't be more than MaxEmptyPageCount ({MaxEmptyPageCount})");

            MinEmptyPageCount = logSettings.MinEmptyPageCount;
            EmptyPageCount = logSettings.MinEmptyPageCount;

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
                var bufferSizeInBytes = (nuint)RoundUp(sizeof(long*) * BufferSize, Constants.kCacheLineBytes);
                pagePointers = (long*)NativeMemory.AlignedAlloc(bufferSizeInBytes, Constants.kCacheLineBytes);
                NativeMemory.Clear(pagePointers, bufferSizeInBytes);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long logicalAddress)
        {
            // Index of page within the circular buffer, and offset on the page.
            var pageIndex = GetPageIndexForAddress(logicalAddress);
            var offset = GetOffsetOnPage(logicalAddress);
            return *(pagePointers + pageIndex) + offset;
        }

        internal bool IsAllocated(int pageIndex) => pagePointers[pageIndex] != 0;

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

            if (FlushedUntilAddress > _wrapper.GetFirstValidLogicalAddressOnPage(0))
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

        /// <summary>Initialize allocator</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        protected void Initialize(long firstValidAddress)
        {
            Debug.Assert(AbsoluteAddress(firstValidAddress) <= PageSize, $"AbsoluteAddress(firstValidAddress) {AbsoluteAddress(firstValidAddress)} should be <= PageSize {PageSize}");

            bufferPool ??= new SectorAlignedBufferPool(1, sectorSize);
            firstValidAddress = SetAddressType(AbsoluteAddress(firstValidAddress)); // Initially InLogMemory but may need to be changed to InReadCache

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

            TailPageOffset.Page = (int)GetPage(firstValidAddress);
            TailPageOffset.Offset = (int)GetOffsetOnPage(firstValidAddress);
        }

        /// <summary>Number of pages in circular buffer that are allocated</summary>
        public int AllocatedPageCount;

        /// <summary>Max number of pages that have been allocated at any point in time</summary>
        public int MaxAllocatedPageCount;

        /// <summary>Maximum possible number of empty pages in circular buffer</summary>
        public int MaxEmptyPageCount => BufferSize - 1;

        /// <summary>Minimum number of empty pages in circular buffer to be maintained to account for non-power-of-two size</summary>
        public int MinEmptyPageCount;

        /// <summary>Maximum memory size in bytes</summary>
        public long MaxMemorySizeBytes => (BufferSize - MinEmptyPageCount) * (long)PageSize;

        /// <summary>How many pages do we leave empty in the in-memory buffer (between 0 and BufferSize-1)</summary>
        public int EmptyPageCount
        {
            get => emptyPageCount;

            set
            {
                // HeadOffset lag (from tail).
                var headOffsetLagSize = MaxEmptyPageCount;
                if (value > headOffsetLagSize) return;
                if (value < MinEmptyPageCount) return;

                int oldEPC;
                lock (this) // linearize all setters of EmptyPageCount
                {
                    oldEPC = emptyPageCount;
                    emptyPageCount = value;
                    headOffsetLagSize -= emptyPageCount;

                    // Address lag offsets correspond to the number of pages "behind" TailPageOffset (the tail in the circular buffer).
                    ReadOnlyAddressLagOffset = GetStartAbsoluteLogicalAddressOfPage((long)(LogMutableFraction * headOffsetLagSize));
                    HeadAddressLagOffset = GetStartAbsoluteLogicalAddressOfPage(headOffsetLagSize);
                }

                // Force eviction now if empty page count has increased
                if (value >= oldEPC)
                {
                    var prot = epoch.ThisInstanceProtected();

                    if (!prot) epoch.Resume();
                    try
                    {
                        // These shifts adjust via application of the lag addresses.
                        var _tailAddress = GetTailAddress();
                        PageAlignedShiftReadOnlyAddress(_tailAddress);
                        PageAlignedShiftHeadAddress(_tailAddress);
                    }
                    finally
                    {
                        if (!prot) epoch.Suspend();
                    }
                }
            }
        }

        /// <summary>Increments AllocatedPageCount. Updates MaxAllocatedPageCount if a higher number of pages have been allocated.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void IncrementAllocatedPageCount()
        {
            var newAllocatedPageCount = Interlocked.Increment(ref AllocatedPageCount);
            var currMaxAllocatedPageCount = MaxAllocatedPageCount;
            while (currMaxAllocatedPageCount < newAllocatedPageCount)
            {
                if (Interlocked.CompareExchange(ref MaxAllocatedPageCount, newAllocatedPageCount, currMaxAllocatedPageCount) == currMaxAllocatedPageCount)
                    return;
                currMaxAllocatedPageCount = MaxAllocatedPageCount;
            }
        }

        /// <summary>Segment size</summary>
        public long GetSegmentSize() => SegmentSize;

        /// <summary>Get tail address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetTailAddress()
        {
            var local = TailPageOffset;

            // Handle corner cases during page overflow
            // The while loop is guaranteed to terminate because HandlePageOverflow
            // ensures that it fixes the unstable TailPageOffset immediately.
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
            return GetStartLogicalAddressOfPage(local.Page) | (uint)local.Offset;
        }

        /// <summary>Get page index from <paramref name="logicalAddress"/></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPage(long logicalAddress) => LogAddress.GetPage(logicalAddress, LogPageSizeBits);

        /// <summary>Get page index for page</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetPageIndexForPage(long page) => (int)(page % BufferSize);

        /// <summary>Get page index for address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetPageIndexForAddress(long address) => GetPageIndexForPage(GetPage(address));

        /// <summary>Get capacity (number of pages)</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetCapacityNumPages() => BufferSize;

        /// <summary>Get page size</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPageSize() => PageSize;

        /// <summary>Get logical address of start of page</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetAddressOfStartOfPage(long address) => address & ~PageSizeMask;

        /// <summary>Get offset in page</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetOffsetOnPage(long address) => address & PageSizeMask;

        /// <summary>Get start absolute logical address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetStartAbsoluteLogicalAddressOfPage(long page) => LogAddress.GetStartAbsoluteLogicalAddressOfPage(page, LogPageSizeBits);

        /// <summary>Get start logical address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetStartLogicalAddressOfPage(long page) => SetAddressType(GetStartAbsoluteLogicalAddressOfPage(page));

        /// <summary>Get first valid address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetFirstValidLogicalAddressOnPage(long page) => page == 0 ? FirstValidAddress : GetStartLogicalAddressOfPage(page);

        /// <summary>Get log segment index from <paramref name="logicalAddress"/></summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetSegment(long logicalAddress) => AbsoluteAddress(logicalAddress) >> LogSegmentSizeBits;

        /// <summary>Get offset in page</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetOffsetOnSegment(long address) => address & (SegmentSize - 1);

        /// <summary>Get start absolute logical address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetStartAbsoluteLogicalAddressOfSegment(long segment) => segment << LogSegmentSizeBits;

        /// <summary>Get start logical address</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetStartLogicalAddressOfSegment(long segment) => SetAddressType(GetStartAbsoluteLogicalAddressOfSegment(segment));

        /// <summary>Get sector size for main hlog device</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetDeviceSectorSize() => sectorSize;

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
        /// Throw Tsavorite exception with message. We use a method wrapper so that
        /// the caller method can execute inlined.
        /// </summary>
        /// <param name="message"></param>
        /// <exception cref="TsavoriteException"></exception>
        [MethodImpl(MethodImplOptions.NoInlining)]
        static void ThrowTsavoriteException(string message)
            => throw new TsavoriteException(message);

        /// <summary>
        /// Whether we need to shift addresses when turning the page.
        /// </summary>
        /// <param name="pageIndex">The page we are turning to</param>
        /// <param name="localTailPageOffset">Local copy of PageOffset (includes the addition of numSlots)</param>
        /// <param name="numSlots">Size of new allocation</param>
        /// <returns></returns>
        bool NeedToShiftAddress(long pageIndex, PageOffset localTailPageOffset, int numSlots)
        {
            var tailAddress = GetStartLogicalAddressOfPage(localTailPageOffset.Page) | ((long)(localTailPageOffset.Offset - numSlots));
            var shiftAddress = GetStartLogicalAddressOfPage(pageIndex);

            // Check whether we need to shift ROA
            var desiredReadOnlyAddress = shiftAddress - ReadOnlyAddressLagOffset;
            if (desiredReadOnlyAddress > tailAddress)
                desiredReadOnlyAddress = tailAddress;
            if (desiredReadOnlyAddress > ReadOnlyAddress)
                return true;

            // Check whether we need to shift HA
            var desiredHeadAddress = shiftAddress - HeadAddressLagOffset;
            var currentFlushedUntilAddress = FlushedUntilAddress;
            if (desiredHeadAddress > currentFlushedUntilAddress)
                desiredHeadAddress = currentFlushedUntilAddress;
            if (desiredHeadAddress > tailAddress)
                desiredHeadAddress = tailAddress;
            if (desiredHeadAddress > HeadAddress)
                return true;

            return false;
        }

        /// <summary>
        /// Shift log addresses when turning the page.
        /// </summary>
        /// <param name="pageIndex">The page we are turning to</param>
        void IssueShiftAddress(long pageIndex)
        {
            // Issue the shift of address
            var shiftAddress = GetStartLogicalAddressOfPage(pageIndex);
            var tailAddress = GetTailAddress();

            var desiredReadOnlyAddress = shiftAddress - ReadOnlyAddressLagOffset;
            if (desiredReadOnlyAddress > tailAddress)
                desiredReadOnlyAddress = tailAddress;
            _ = ShiftReadOnlyAddress(desiredReadOnlyAddress);

            var desiredHeadAddress = shiftAddress - HeadAddressLagOffset;
            if (desiredHeadAddress > tailAddress)
                desiredHeadAddress = tailAddress;
            _ = ShiftHeadAddress(desiredHeadAddress);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        long HandlePageOverflow(ref PageOffset localTailPageOffset, int numSlots)
        {
            var pageIndex = localTailPageOffset.Page + 1;

            // This thread is trying to allocate at an offset past where one or more previous threads
            // already overflowed; exit and allow the first overflow thread to proceed. Do not try to remove
            // the update to TailPageOffset that was done by this thread; that will be overwritten when
            // the first overflow thread finally completes and updates TailPageOffset.
            if (localTailPageOffset.Offset - numSlots > PageSize)
            {
                if (NeedToWait(pageIndex))
                    return 0; // RETRY_LATER
                return -1; // RETRY_NOW
            }

            // The single thread that "owns" the page-increment proceeds below. This is the thread for which:
            // 1. Old image of offset (pre-Interlocked.Increment) is <= PageSize, and
            // 2. New image of offset (post-Interlocked.Increment) is > PageSize.
            if (NeedToWait(pageIndex))
            {
                // Reset to previous tail so that next attempt can retry
                localTailPageOffset.PageAndOffset -= numSlots;
                _ = Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);

                // Shift only after TailPageOffset is reset to a valid state
                IssueShiftAddress(pageIndex);

                return 0; // RETRY_LATER
            }

            // We next verify that:
            // 1. The next page (pageIndex) is ready to use (i.e., closed)
            // 2. We have issued any necessary address shifting at the page-turn boundary.
            // If either cannot be verified, we can ask the caller to retry now (immediately), because it is
            // an ephemeral state.
            if (CannotAllocate(pageIndex) || NeedToShiftAddress(pageIndex, localTailPageOffset, numSlots))
            {
                // Reset to previous tail so that next attempt can retry
                localTailPageOffset.PageAndOffset -= numSlots;
                _ = Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);

                // Shift only after TailPageOffset is reset to a valid state
                IssueShiftAddress(pageIndex);

                return -1; // RETRY_NOW
            }

            // Allocate next page and set new tail
            if (!IsAllocated(pageIndex % BufferSize) || !IsAllocated((pageIndex + 1) % BufferSize))
                AllocatePagesWithException(pageIndex, localTailPageOffset, numSlots);

            localTailPageOffset.Page++;
            localTailPageOffset.Offset = numSlots;
            TailPageOffset = localTailPageOffset;

            // At this point the slot is allocated and we are not allowed to refresh epochs any longer.

            // Offset is zero for the first allocation on the new page
            return GetStartLogicalAddressOfPage(localTailPageOffset.Page);
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

            // Necessary to check because threads keep retrying and we do not
            // want to overflow the offset more than once per thread
            if (localTailPageOffset.Offset > PageSize)
            {
                if (NeedToWait(localTailPageOffset.Page + 1))
                    return 0; // RETRY_LATER
                return -1; // RETRY_NOW
            }

            // Determine insertion index. Note that this forms a kind of "lock"; after the first thread does this, other threads that do
            // it will see that another thread got there first because the subsequent "back up by numSlots" will still be past PageSize,
            // so they will exit and RETRY in HandlePageOverflow; the first thread "owns" the overflow operation and must stabilize it.
            localTailPageOffset.PageAndOffset = Interlocked.Add(ref TailPageOffset.PageAndOffset, numSlots);

            // Note: Below here we defer SetAddressType(..) to TryAllocateRetryNow, so 0 and -1 returns are preserved.

            // Slow path when we reach the end of a page.
            if (localTailPageOffset.Offset > PageSize)
            {
                // Note that TailPageOffset is now unstable -- there may be a GetTailAddress call spinning for
                // it to stabilize. Therefore, HandlePageOverflow needs to stabilize TailPageOffset immediately,
                // before performing any epoch bumps or system calls.
                return HandlePageOverflow(ref localTailPageOffset, numSlots);
            }

            return GetStartLogicalAddressOfPage(localTailPageOffset.Page) | ((long)(localTailPageOffset.Offset - numSlots));
        }

        /// <summary>Try allocate, spin for RETRY_NOW (logicalAddress is less than 0) case</summary>
        /// <param name="numSlots">Number of slots to allocate</param>
        /// <param name="logicalAddress">Returned address, or RETRY_LATER (if 0) indicator</param>
        /// <returns>The allocated logical address, or 0 in case of inability to allocate</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAllocateRetryNow(int numSlots, out long logicalAddress)
        {
            long address;
            while ((address = TryAllocate(numSlots)) < 0)
            {
                _ = TryComplete();
                epoch.ProtectAndDrain();
                _ = Thread.Yield();
            }
            logicalAddress = SetAddressType(address);
            return address != 0;
        }

        /// <summary>
        /// If the page we are trying to allocate is past the last page with an unclosed address region, 
        /// then we can retry immediately because this is called after NeedToWait, so we know we've 
        /// completed the wait on flushEvent for the necessary pages to be flushed, and are waiting for
        /// OnPagesClosed to be completed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CannotAllocate(int page) => page >= BufferSize + GetPage(ClosedUntilAddress);

        /// <summary>
        /// If the page we are trying to allocate is past the last page with an unflushed address region, 
        /// we have to wait for the flushEvent.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool NeedToWait(int page) => page >= BufferSize + GetPage(FlushedUntilAddress);

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
            var flushEvent = FlushEvent;
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
                        flushEvent.Wait();
                    }
                    finally
                    {
                        epoch.Resume();
                    }
                    flushEvent = FlushEvent;
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
        internal virtual void EvictPage(long page)
        {
            var start = GetStartLogicalAddressOfPage(page);
            var end = GetStartLogicalAddressOfPage(page + 1);
            if (OnEvictionObserver is not null)
                MemoryPageScan(start, end, OnEvictionObserver);
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
                if (OnReadOnlyObserver != null)
                {
                    // This scan does not need a store because it does not lock; it is epoch-protected so by the time it runs no current thread
                    // will have seen a record below the new ReadOnlyAddress as "in mutable region".
                    using var iter = Scan(store: null, oldSafeReadOnlyAddress, newSafeReadOnlyAddress, DiskScanBufferingMode.NoBuffering);
                    OnReadOnlyObserver?.OnNext(iter);
                }
                AsyncFlushPages(oldSafeReadOnlyAddress, newSafeReadOnlyAddress, noFlush);
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
                long closeStartAddress = ClosedUntilAddress;
                long closeEndAddress = OngoingCloseUntilAddress;

                if (EvictCallback is not null)
                    EvictCallback(closeStartAddress, closeEndAddress);

                // Process a page (possibly fragment) at a time.
                for (long closePageAddress = GetAddressOfStartOfPage(closeStartAddress); closePageAddress < closeEndAddress; closePageAddress += PageSize)
                {
                    long start = closeStartAddress > closePageAddress ? closeStartAddress : closePageAddress;
                    long end = closeEndAddress < closePageAddress + PageSize ? closeEndAddress : closePageAddress + PageSize;

                    // This scan does not need a store because it does not lock; it is epoch-protected so by the time it runs no current thread
                    // will have seen a record below the eviction range as "in mutable region".
                    if (OnEvictionObserver is not null)
                        MemoryPageScan(start, end, OnEvictionObserver);

                    // If we are using a null storage device, we must also shift BeginAddress 
                    if (IsNullDevice)
                        _ = MonotonicUpdate(ref BeginAddress, end, out _);

                    // If this is OA, we need to patch up .PreviousAddress pointers to records in [closeStartAddress, closeEndAddress].
                    PatchExpandedAddresses(closeStartAddress, closeEndAddress, storeBase);

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

        /// <summary>
        /// Overridden by ObjectAllocator
        /// </summary>
        /// <param name="closeStartAddress">First address on page that is closing</param>
        /// <param name="closeEndAddress">Last address on page that is closing</param>
        /// <param name="storeBase">The store base, for hash table lookups</param>
        internal virtual void PatchExpandedAddresses(long closeStartAddress, long closeEndAddress, TsavoriteBase storeBase) { }

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
        /// Called every time a new tail page is allocated. Here the read-only is shifted only to page boundaries 
        /// unlike ShiftReadOnlyToTail where shifting can happen to any fine-grained address.
        /// </summary>
        private void PageAlignedShiftReadOnlyAddress(long currentTailAddress)
        {
            long desiredReadOnlyAddress = GetAddressOfStartOfPage(currentTailAddress) - ReadOnlyAddressLagOffset;
            if (desiredReadOnlyAddress < addressTypeMask)   // ReadOnlyAddressLagOffset is not scaled with addressTypeMask
                return;
            if (MonotonicUpdate(ref ReadOnlyAddress, desiredReadOnlyAddress, out _))
            {
                // Debug.WriteLine("Allocate: Moving read-only offset from {0:X} to {1:X}", oldReadOnlyAddress, desiredReadOnlyAddress);
                epoch.BumpCurrentEpoch(() => OnPagesMarkedReadOnly(desiredReadOnlyAddress));
            }
        }

        /// <summary>
        /// Called whenever a new tail page is allocated or when the user is checking for a failed memory allocation
        /// Tries to shift head address based on the head offset lag size.
        /// </summary>
        /// <param name="currentTailAddress"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PageAlignedShiftHeadAddress(long currentTailAddress)
        {
            var desiredHeadAddress = GetAddressOfStartOfPage(currentTailAddress) - HeadAddressLagOffset;
            if (desiredHeadAddress < addressTypeMask)   // HeadAddressLagOffset is not scaled with addressTypeMask
                return;

            // Obtain local values of variables that can change
            var currentFlushedUntilAddress = FlushedUntilAddress;
            if (desiredHeadAddress > currentFlushedUntilAddress)
                desiredHeadAddress = GetAddressOfStartOfPage(currentFlushedUntilAddress);

            if (MonotonicUpdate(ref HeadAddress, desiredHeadAddress, out _))
            {
                // Debug.WriteLine("Allocate: Moving head offset from {0:X} to {1:X}", oldHeadAddress, newHeadAddress);
                epoch.BumpCurrentEpoch(() => OnPagesClosed(desiredHeadAddress));
            }
        }

        /// <summary>
        /// Tries to shift head address to specified value
        /// </summary>
        /// <param name="desiredHeadAddress"></param>
        public long ShiftHeadAddress(long desiredHeadAddress)
        {
            // Obtain local values of variables that can change
            long currentFlushedUntilAddress = FlushedUntilAddress;

            long newHeadAddress = desiredHeadAddress;
            if (newHeadAddress > currentFlushedUntilAddress)
                newHeadAddress = currentFlushedUntilAddress;

            if (GetOffsetOnPage(newHeadAddress) != 0)
            {

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
        /// Is there a better way to do this with enabling fine-grained addresses (not necessarily at page boundaries)?
        /// </summary>
        protected void ShiftFlushedUntilAddress()
        {
            long currentFlushedUntilAddress = FlushedUntilAddress;
            long page = GetPage(currentFlushedUntilAddress);

            bool update = false;
            long pageLastFlushedAddress = PageStatusIndicator[page % BufferSize].LastFlushedUntilAddress;
            while (pageLastFlushedAddress >= currentFlushedUntilAddress && currentFlushedUntilAddress >= GetStartLogicalAddressOfPage(page))
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

                    FlushEvent.Set();

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
            _wrapper.ClearPage(pageIndex, (int)GetOffsetOnPage(tailAddress));

            // Printing debug info
            logger?.LogInformation("******* Recovered HybridLog Stats *******");
            logger?.LogInformation("Head Address: {HeadAddress}", HeadAddress);
            logger?.LogInformation("Safe Head Address: {SafeHeadAddress}", SafeHeadAddress);
            logger?.LogInformation("ReadOnly Address: {ReadOnlyAddress}", ReadOnlyAddress);
            logger?.LogInformation("Safe ReadOnly Address: {SafeReadOnlyAddress}", SafeReadOnlyAddress);
            logger?.LogInformation("Tail Address: {tailAddress}", tailAddress);
        }

        /// <summary>Invoked by users to obtain a record from disk. It uses sector aligned memory to read the record efficiently into memory.</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal unsafe void AsyncReadRecordToMemory(long fromLogicalAddress, int numBytes, DeviceIOCompletionCallback callback, ref AsyncIOContext context)
        {
            var fileOffset = IsObjectAllocator
                ? (ulong)AbsoluteAddress(fromLogicalAddress)
                : (ulong)(AlignedPageSizeBytes * GetPage(fromLogicalAddress) + GetOffsetOnPage(fromLogicalAddress));
            var alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);          // Round down to sector start

            var alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);  // Round up in length (due to alignedFileOffset round-down to sector start)
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1)); // Round up to sector end

            var record = bufferPool.Get((int)alignedReadLength);
            record.valid_offset = (int)(fileOffset - alignedFileOffset);
            record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));   // alignedReadLength - (valid_offset, which is the round-down to sector start))
            record.required_bytes = numBytes;

            var asyncResult = default(AsyncGetFromDiskResult<AsyncIOContext>);
            asyncResult.context = context;
            asyncResult.context.record = record;
            device.ReadAsync(alignedFileOffset,
                        (IntPtr)asyncResult.context.record.aligned_pointer,
                        alignedReadLength,
                        callback,
                        asyncResult);
        }

        /// <summary>
        /// Read record to memory - simple read context version
        /// </summary>
        /// <param name="fromLogicalAddress"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        internal unsafe void AsyncReadRecordToMemory(long fromLogicalAddress, int numBytes, DeviceIOCompletionCallback callback, ref SimpleReadContext context)
        {
            Debug.Assert(!IsObjectAllocator, $"Cannot use this form of {GetCurrentMethodName()} with {nameof(ObjectAllocator<TStoreFunctions>)}");

            var fileOffset = (ulong)(AlignedPageSizeBytes * GetPage(fromLogicalAddress) + GetOffsetOnPage(fromLogicalAddress));
            var alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);          // Round down to sector start

            var alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);  // Round up in length (due to alignedFileOffset round-down to sector start)
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1)); // Round up to sector end

            context.record = bufferPool.Get((int)alignedReadLength);
            context.record.valid_offset = (int)(fileOffset - alignedFileOffset);
            context.record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));   // alignedReadLength - (valid_offset, which is the round-down to sector start))
            context.record.required_bytes = numBytes;

            device.ReadAsync(alignedFileOffset,
                        (IntPtr)context.record.aligned_pointer,
                        alignedReadLength,
                        callback,
                        context);
        }

        /// <summary>Read pages from specified device</summary>
        public void AsyncReadPagesFromDevice<TContext>(
                                long readPageStart,
                                int numPages,
                                long untilAddress,
                                DeviceIOCompletionCallback callback,
                                TContext context,
                                long devicePageOffset = 0,
                                IDevice logDevice = null)
            => AsyncReadPagesFromDevice(readPageStart, numPages, untilAddress, callback, context, out _, devicePageOffset, logDevice);

        /// <summary>Read pages from specified device</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private void AsyncReadPagesFromDevice<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null)
        {
            var usedDevice = device ?? this.device;

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                var pageIndex = (int)(readPage % BufferSize);
                if (!IsAllocated(pageIndex))
                    _wrapper.AllocatePage(pageIndex);
                else
                    _wrapper.ClearPage(readPage);

                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    offset = devicePageOffset,
                    context = context,
                    handle = completed,
                    maxPtr = PageSize
                };

                var offsetInFile = (ulong)(AlignedPageSizeBytes * readPage);
                var readLength = (uint)AlignedPageSizeBytes;
                long adjustedUntilAddress = AlignedPageSizeBytes * GetPage(untilAddress) + GetOffsetOnPage(untilAddress);

                if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
                {
                    readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                    asyncResult.maxPtr = readLength;
                    readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
                }

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                ReadAsync(offsetInFile, pageIndex, readLength, callback, asyncResult, usedDevice);
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
        public void AsyncFlushPages(long fromAddress, long untilAddress, bool noFlush = false)
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

            // Request asynchronous writes to the device. If waitForPendingFlushComplete is set, then a CountDownEvent is set in the callback handle.
            for (long flushPage = startPage; flushPage < (startPage + numPages); flushPage++)
            {
                // Default to writing the full page.
                long pageStartAddress = GetStartLogicalAddressOfPage(flushPage);
                long pageEndAddress = GetStartLogicalAddressOfPage(flushPage + 1);

                var asyncResult = new PageAsyncFlushResult<Empty>
                {
                    page = flushPage,
                    count = 1,
                    partial = false,
                    fromAddress = pageStartAddress,
                    untilAddress = pageEndAddress
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

                // Partial page starting point, need to wait until the ongoing adjacent flush is completed to ensure correctness
                if (GetOffsetOnPage(asyncResult.fromAddress) > 0)
                {
                    var index = GetPageIndexForAddress(asyncResult.fromAddress);

                    // Try to merge request with existing adjacent (earlier) pending requests (these have not yet begun or they
                    // would not be in the queue).
                    while (PendingFlush[index].RemovePreviousAdjacent(asyncResult.fromAddress, out var existingRequest))
                        asyncResult.fromAddress = existingRequest.fromAddress;

                    // Enqueue the (possibly merged) new work item into the queue.
                    PendingFlush[index].Add(asyncResult);

                    // Perform work from shared queue if possible: When a flush completes it updates FlushedUntilAddress. If there
                    // is an item in the shared queue that starts at FlushedUntilAddress, it can now be flushed. Flush callbacks
                    // will RemoveNextAdjacent(FlushedUntilAddress, ...) to continue the chain of flushes until the queue is empty.
                    if (PendingFlush[index].RemoveNextAdjacent(FlushedUntilAddress, out PageAsyncFlushResult<Empty> request))
                        WriteAsync(GetPage(request.fromAddress), AsyncFlushPageCallback, request);  // Call the overridden WriteAsync for the derived allocator class
                }
                else
                {
                    // Write the entire page up to asyncResult.untilAddress (there can be no previous items in the queue). Flush callbacks
                    // will RemoveNextAdjacent(FlushedUntilAddress, ...) to continue the chain of flushes until the queue is empty.
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
        public void AsyncFlushPages<TContext>(long flushPageStart, int numPages, DeviceIOCompletionCallback callback, TContext context)
        {
            for (long flushPage = flushPageStart; flushPage < (flushPageStart + numPages); flushPage++)
            {
                var asyncResult = new PageAsyncFlushResult<TContext>()
                {
                    page = flushPage,
                    context = context,
                    count = 1,
                    partial = false,
                    untilAddress = GetStartLogicalAddressOfPage(flushPage + 1)
                };

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
        /// <param name="completedSemaphore"></param>
        /// <param name="throttleCheckpointFlushDelayMs"></param>
        [MethodImpl(MethodImplOptions.NoInlining)]
        public void AsyncFlushPagesToDevice(long startPage, long endPage, long endLogicalAddress, long fuzzyStartLogicalAddress, IDevice device, out SemaphoreSlim completedSemaphore, int throttleCheckpointFlushDelayMs)
        {
            logger?.LogTrace("Starting async full log flush with throttling {throttlingEnabled}", throttleCheckpointFlushDelayMs >= 0 ? $"enabled ({throttleCheckpointFlushDelayMs}ms)" : "disabled");

            var _completedSemaphore = new SemaphoreSlim(0);
            completedSemaphore = _completedSemaphore;

            // If throttled, convert rest of the method into a truly async task run
            // because issuing IO can take up synchronous time
            if (throttleCheckpointFlushDelayMs >= 0)
                _ = Task.Run(FlushRunner);
            else
                FlushRunner();

            void FlushRunner()
            {
                var totalNumPages = (int)(endPage - startPage);
                var flushCompletionTracker = new FlushCompletionTracker(_completedSemaphore, throttleCheckpointFlushDelayMs >= 0 ? new SemaphoreSlim(0) : null, totalNumPages);

                for (long flushPage = startPage; flushPage < endPage; flushPage++)
                {
                    long flushPageAddress = GetStartLogicalAddressOfPage(flushPage);
                    var pageSize = PageSize;
                    if (flushPage == endPage - 1)
                        pageSize = (int)(endLogicalAddress - flushPageAddress);

                    var asyncResult = new PageAsyncFlushResult<Empty>
                    {
                        flushCompletionTracker = flushCompletionTracker,
                        page = flushPage,
                        fromAddress = flushPageAddress,
                        untilAddress = flushPageAddress + pageSize,
                        count = 1
                    };

                    // Intended destination is flushPage
                    WriteAsyncToDevice(startPage, flushPage, pageSize, AsyncFlushPageToDeviceCallback, asyncResult, device, fuzzyStartLogicalAddress);

                    if (throttleCheckpointFlushDelayMs >= 0)
                    {
                        flushCompletionTracker.WaitOneFlush();
                        Thread.Sleep(throttleCheckpointFlushDelayMs);
                    }
                }
            }
        }

        internal void AsyncGetFromDisk(long fromLogicalAddress, int numBytes, AsyncIOContext context)
        {
            // If this is a protected thread, spin until the device is not throttled, draining events on each iteration. Do not release the epoch.
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

        private protected void AsyncSimpleReadPageCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncSimpleReadPageCallback)} error: {{errorCode}}", errorCode);

            // Signal the event so the waiter can continue
            var result = (PageAsyncReadResult<Empty>)context;
            _ = result.handle.Signal();
        }

        private protected void AsyncSimpleFlushPageCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncSimpleFlushPageCallback)} error: {{errorCode}}", errorCode);

            // Signal the event so the waiter can continue
            var result = (PageAsyncFlushResult<Empty>)context;
            _ = result.done.Set();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private unsafe void AsyncGetFromDiskCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError("AsyncGetFromDiskCallback error: {0}", errorCode);

            var result = (AsyncGetFromDiskResult<AsyncIOContext>)context;
            var ctx = result.context;
            try
            {
                // TODO: logicalAddress must be physicalAddress here.
                // TODO: move this "if" to an IAllocator function call to do the DiskStreamReadBuffer.Read().
                DiskStreamReadBuffer<TStoreFunctions>.ReadParameters readParams = IsObjectAllocator
                    ? new(bufferPool, PageSize, device.SectorSize, ctx.logicalAddress, storeFunctions)
                    : new(bufferPool, maxInlineKeySize, maxInlineValueSize, device.SectorSize, ctx.logicalAddress, storeFunctions);
                var valueObjectSerializer = IsObjectAllocator ? storeFunctions.CreateValueObjectSerializer() : default;
                var readBuffer = new DiskStreamReadBuffer<TStoreFunctions>(in readParams, device, valueObjectSerializer, AsyncSimpleReadPageCallback);
                if (!readBuffer.Read(ref ctx.record, ctx.request_key, out ctx.diskLogRecord))
                {
                    Debug.Assert(!readBuffer.recordInfo.Invalid, "Invalid records should not be in the hash chain for pending IO");

                    // Keys don't match so request the previous record in the chain if it is in the range to resolve, else fall through to signal "IO complete".
                    ctx.logicalAddress = readBuffer.recordInfo.PreviousAddress;
                    if (ctx.logicalAddress >= BeginAddress && ctx.logicalAddress >= ctx.minAddress)
                    {
                        ctx.Dispose();
                        AsyncGetFromDisk(ctx.logicalAddress, IStreamBuffer.InitialIOSize, ctx);
                        return;
                    }
                }

                // Either the keys match or we are below the range to retrieve (which ContinuePending* will detect), so we're done.
                if (ctx.completionEvent is not null)
                    ctx.completionEvent.Set(ref ctx);
                else
                    ctx.callbackQueue.Enqueue(ctx);
            }
            catch (Exception e)
            {
                logger?.LogError(e, "AsyncGetFromDiskCallback error");

                ctx.Dispose();

                // If someone is waiting on the event, set the exception there; otherwise rethrow
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
                    logger?.LogError("AsyncFlushPageCallback error: {0}", errorCode);

                // Set the page status to flushed
                var result = (PageAsyncFlushResult<Empty>)context;

                if (Interlocked.Decrement(ref result.count) == 0)
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
                    result.Free();
                }

                // Continue the chained flushes, popping the next request from the queue if it is adjacent.
                var _flush = FlushedUntilAddress;
                if (GetOffsetOnPage(_flush) > 0 && PendingFlush[GetPage(_flush) % BufferSize].RemoveNextAdjacent(_flush, out PageAsyncFlushResult<Empty> request))
                    WriteAsync(GetPage(request.fromAddress), AsyncFlushPageCallback, request);  // Call the overridden WriteAsync for the derived allocator class
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
                    WriteAsync(GetPage(request.fromAddress), AsyncFlushPageCallback, request);  // Call the overridden WriteAsync for the derived allocator class
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
                    logger?.LogError("AsyncFlushPageToDeviceCallback error: {0}", errorCode);

                var result = (PageAsyncFlushResult<Empty>)context;

                var epochTaken = false;
                if (!epoch.ThisInstanceProtected())
                {
                    epochTaken = true;
                    epoch.Resume();
                }

                // Unset dirty bit for flushed pages
                try
                {
                    var startAddress = GetStartLogicalAddressOfPage(result.page);
                    var endAddress = startAddress + PageSize;

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
                            var (_, alignedRecordSize) = logRecord.GetInlineRecordSizes();
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

                if (Interlocked.Decrement(ref result.count) == 0)
                    result.Free();
            }
            catch when (disposed) { }
        }

        internal string PrettyPrintLogicalAddress(long logicalAddress) => $"{logicalAddress}:{GetPage(logicalAddress)}.{GetOffsetOnPage(logicalAddress)}";
    }
}