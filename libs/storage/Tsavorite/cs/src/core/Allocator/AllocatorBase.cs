// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Base class for hybrid log memory allocator. Contains utility methods, some of which are not performance-critical so can be virtual.
    /// </summary>
    public abstract partial class AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> : IDisposable
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        /// <summary>The epoch we are operating with</summary>
        protected readonly LightEpoch epoch;
        /// <summary>Whether we own (and thus must dispose) <see cref="epoch"/></summary>
        private readonly bool ownedEpoch;

        /// <summary>The store functions for this instance of TsavoriteKV</summary>
        internal readonly TStoreFunctions _storeFunctions;

        /// <summary>The fully-derived allocator struct wrapper (so calls on it are inlined rather than virtual) for this log.</summary>
        internal readonly TAllocator _wrapper;

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

        /// <summary>Aligned page size in bytes</summary>
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

        /// <summary>How many pages do we leave empty in the in-memory buffer (between 0 and BufferSize-1)</summary>
        private int emptyPageCount;

        /// <summary>Minimum number of empty pages in circular buffer to be maintained to account for non-power-of-two size</summary>
        private int minEmptyPageCount;

        /// <summary>HeadAddress offset from tail (currently page-aligned)</summary>
        internal long HeadAddressLagOffset;

        /// <summary>Log mutable fraction</summary>
        protected readonly double LogMutableFraction;

        /// <summary>ReadOnlyAddress offset from tail (currently page-aligned)</summary>
        protected long ReadOnlyAddressLagOffset;

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

        /// <summary>The lowest valid address on disk - updated when truncating log</summary>
        public long PersistedBeginAddress;

        /// <summary>
        /// Address until which we are currently closing. Used to coordinate linear closing of pages.
        /// Only one thread will be closing pages at a time.
        /// </summary>
        long OngoingCloseUntilAddress;

        /// <inheritdoc/>
        public override string ToString()
            => $"TA {GetTailAddress()}, ROA {ReadOnlyAddress}, SafeROA {SafeReadOnlyAddress}, HA {HeadAddress}, SafeHA {SafeHeadAddress}, CUA {ClosedUntilAddress}, FUA {FlushedUntilAddress}, BA {BeginAddress}";
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

        /// <summary>This hlog is an instance of a Read cache</summary>
        protected readonly bool IsReadCache = false;

        /// <summary>Read cache eviction callback</summary>
        protected readonly Action<long, long> EvictCallback = null;

        /// <summary>Flush callback</summary>
        protected readonly Action<CommitInfo> FlushCallback = null;

        /// <summary>Whether to preallocate log on initialization</summary>
        private readonly bool PreallocateLog = false;

        /// <summary>Error handling</summary>
        private readonly ErrorList errorList = new();

        /// <summary>Observer for records entering read-only region</summary>
        internal IObserver<ITsavoriteScanIterator<TKey, TValue>> OnReadOnlyObserver;

        /// <summary>Observer for records getting evicted from memory (page closed)</summary>
        internal IObserver<ITsavoriteScanIterator<TKey, TValue>> OnEvictionObserver;

        /// <summary>Observer for records brought into memory by deserializing pages</summary>
        internal IObserver<ITsavoriteScanIterator<TKey, TValue>> OnDeserializationObserver;

        /// <summary>The "event" to be waited on for flush completion by the initiator of an operation</summary>
        internal CompletionEvent FlushEvent;

        /// <summary>If set, this is a function to call to determine whether the object size tracker reports maximum memory size has been exceeded.</summary>
        public Func<bool> IsSizeBeyondLimit;
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
        /// <param name="objectLogDevice"></param>
        /// <param name="localSegmentOffsets"></param>
        /// <param name="fuzzyStartLogicalAddress">Start address of fuzzy region, which contains old and new version records (we use this to selectively flush only old-version records during snapshot checkpoint)</param>
        protected abstract void WriteAsyncToDevice<TContext>(long startPage, long flushPage, int pageSize, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> result, IDevice device, IDevice objectLogDevice, long[] localSegmentOffsets, long fuzzyStartLogicalAddress);

        /// <summary>Read objects to memory (async)</summary>
        protected abstract unsafe void AsyncReadRecordObjectsToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, AsyncIOContext<TKey, TValue> context, SectorAlignedMemory result = default);

        /// <summary>Read page from device (async)</summary>
        protected abstract void ReadAsync<TContext>(ulong alignedSourceAddress, int destinationPageIndex, uint aligned_read_length, DeviceIOCompletionCallback callback, PageAsyncReadResult<TContext> asyncResult, IDevice device, IDevice objlogDevice);

        /// <summary>Write page to device (async)</summary>
        protected abstract void WriteAsync<TContext>(long flushPage, DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult);

        /// <summary>Flush checkpoint Delta to the Device</summary>
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
                if (endAddress > _wrapper.GetStartLogicalAddress(endPage))
                    endPage++;

                long prevEndPage = GetPage(prevEndAddress);
                deltaLog.Allocate(out int entryLength, out long destPhysicalAddress);
                int destOffset = 0;

                // We perform delta capture under epoch protection with page-wise refresh for latency reasons
                bool epochTaken = false;
                if (!epoch.ThisInstanceProtected())
                {
                    epochTaken = true;
                    epoch.Resume();
                }

                try
                {
                    for (long p = startPage; p < endPage; p++)
                    {
                        // Check if we have the page safely available to process in memory
                        if (HeadAddress >= (p << LogPageSizeBits) + PageSize)
                            continue;

                        // All RCU pages need to be added to delta
                        // For IPU-only pages, prune based on dirty bit
                        if ((p < prevEndPage || endAddress == prevEndAddress) && PageStatusIndicator[p % BufferSize].Dirty < version)
                            continue;

                        var logicalAddress = p << LogPageSizeBits;
                        var physicalAddress = _wrapper.GetPhysicalAddress(logicalAddress);

                        var endLogicalAddress = logicalAddress + PageSize;
                        if (endAddress < endLogicalAddress) endLogicalAddress = endAddress;
                        Debug.Assert(endLogicalAddress > logicalAddress);
                        var endPhysicalAddress = physicalAddress + (endLogicalAddress - logicalAddress);

                        if (p == startPage)
                        {
                            physicalAddress += (int)(startAddress & PageSizeMask);
                            logicalAddress += (int)(startAddress & PageSizeMask);
                        }

                        while (physicalAddress < endPhysicalAddress)
                        {
                            ref var info = ref _wrapper.GetInfo(physicalAddress);
                            var (_, alignedRecordSize) = _wrapper.GetRecordSize(physicalAddress);
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

        /// <summary>Delete in-memory portion of the log</summary>
        internal abstract void DeleteFromMemory();

        /// <summary>Reset the hybrid log. WARNING: assumes that threads have drained out at this point.</summary>
        public virtual void Reset()
        {
            var newBeginAddress = GetTailAddress();

            // Shift read-only addresses to tail without flushing
            _ = Utility.MonotonicUpdate(ref ReadOnlyAddress, newBeginAddress, out _);
            _ = Utility.MonotonicUpdate(ref SafeReadOnlyAddress, newBeginAddress, out _);

            // Shift head address to tail
            if (Utility.MonotonicUpdate(ref HeadAddress, newBeginAddress, out _))
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
            _ = Utility.MonotonicUpdate(ref BeginAddress, newBeginAddress, out _);

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
            PersistedBeginAddress = toAddress;
            _ = Task.Run(() => device.TruncateUntilAddress(toAddress));
        }

        /// <summary>Wraps <see cref="IDevice.TruncateUntilAddress(long)"/> when an allocator potentially has to interact with multiple devices</summary>
        protected virtual void TruncateUntilAddressBlocking(long toAddress) => device.TruncateUntilAddress(toAddress);

        /// <summary>Remove disk segment</summary>
        protected virtual void RemoveSegment(int segment) => device.RemoveSegment(segment);

        internal virtual bool TryComplete() => device.TryComplete();

        /// <summary>Dispose allocator</summary>
        public virtual void Dispose()
        {
            disposed = true;

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

        internal unsafe void ApplyDelta(DeltaLog log, long startPage, long endPage, long recoverTo)
        {
            if (log == null) return;

            long startLogicalAddress = _wrapper.GetStartLogicalAddress(startPage);
            long endLogicalAddress = _wrapper.GetStartLogicalAddress(endPage);

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
                                var destination = _wrapper.GetPhysicalAddress(address);

                                // Clear extra space (if any) in old record
                                var oldSize = _wrapper.GetRecordSize(destination).Item2;
                                if (oldSize > size)
                                    new Span<byte>((byte*)(destination + size), oldSize - size).Clear();

                                // Update with new record
                                Buffer.MemoryCopy((void*)physicalAddress, (void*)destination, size, size);

                                // Clean up temporary bits when applying the delta log
                                ref var destInfo = ref _wrapper.GetInfo(destination);
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
            var offset = (logicalAddress >> LogPageSizeBits) % BufferSize;
            if (PageStatusIndicator[offset].Dirty < version)
                PageStatusIndicator[offset].Dirty = version;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MarkPageAtomic(long logicalAddress, long version)
        {
            var offset = (logicalAddress >> LogPageSizeBits) % BufferSize;
            Utility.MonotonicUpdate(ref PageStatusIndicator[offset].Dirty, version, out _);
        }

        internal void WriteAsync<TContext>(IntPtr alignedSourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite,
                DeviceIOCompletionCallback callback, PageAsyncFlushResult<TContext> asyncResult,
                IDevice device)
        {
            if (asyncResult.partial)
            {
                // Write only required bytes within the page
                int aligned_start = (int)((asyncResult.fromAddress - (asyncResult.page << LogPageSizeBits)));
                aligned_start = (aligned_start / sectorSize) * sectorSize;

                int aligned_end = (int)(asyncResult.untilAddress - (asyncResult.page << LogPageSizeBits));
                aligned_end = (aligned_end + (sectorSize - 1)) & ~(sectorSize - 1);

                numBytesToWrite = (uint)(aligned_end - aligned_start);
                device.WriteAsync(alignedSourceAddress + aligned_start, alignedDestinationAddress + (ulong)aligned_start, numBytesToWrite, callback, asyncResult);
            }
            else
            {
                device.WriteAsync(alignedSourceAddress, alignedDestinationAddress,
                    numBytesToWrite, callback, asyncResult);
            }
        }

        internal long GetReadOnlyAddressLagOffset() => ReadOnlyAddressLagOffset;

        protected readonly ILogger logger;

        /// <summary>Instantiate base allocator implementation</summary>
        private protected AllocatorBase(LogSettings settings, TStoreFunctions storeFunctions, Func<object, TAllocator> wrapperCreator, Action<long, long> evictCallback, LightEpoch epoch, Action<CommitInfo> flushCallback, ILogger logger = null)
        {
            _storeFunctions = storeFunctions;
            _wrapper = wrapperCreator(this);

            // Validation
            if (settings.PageSizeBits < LogSettings.kMinPageSizeBits || settings.PageSizeBits > LogSettings.kMaxPageSizeBits)
                throw new TsavoriteException($"{nameof(settings.PageSizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxPageSizeBits}");
            if (settings.SegmentSizeBits < LogSettings.kMinSegmentSizeBits || settings.SegmentSizeBits > LogSettings.kMaxSegmentSizeBits)
                throw new TsavoriteException($"{nameof(settings.SegmentSizeBits)} must be between {LogSettings.kMinSegmentSizeBits} and {LogSettings.kMaxSegmentSizeBits}");
            if (settings.MemorySizeBits != 0 && (settings.MemorySizeBits < LogSettings.kMinMemorySizeBits || settings.MemorySizeBits > LogSettings.kMaxMemorySizeBits))
                throw new TsavoriteException($"{nameof(settings.MemorySizeBits)} must be between {LogSettings.kMinMemorySizeBits} and {LogSettings.kMaxMemorySizeBits}, or may be 0 for ReadOnly TsavoriteLog");
            if (settings.MutableFraction < 0.0 || settings.MutableFraction > 1.0)
                throw new TsavoriteException($"{nameof(settings.MutableFraction)} must be >= 0.0 and <= 1.0");
            if (settings.ReadCacheSettings is not null)
            {
                var rcs = settings.ReadCacheSettings;
                if (rcs.PageSizeBits < LogSettings.kMinPageSizeBits || rcs.PageSizeBits > LogSettings.kMaxPageSizeBits)
                    throw new TsavoriteException($"{nameof(rcs.PageSizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxPageSizeBits}");
                if (rcs.MemorySizeBits < LogSettings.kMinMemorySizeBits || rcs.MemorySizeBits > LogSettings.kMaxMemorySizeBits)
                    throw new TsavoriteException($"{nameof(rcs.MemorySizeBits)} must be between {LogSettings.kMinMemorySizeBits} and {LogSettings.kMaxMemorySizeBits}");
                if (rcs.SecondChanceFraction < 0.0 || rcs.SecondChanceFraction > 1.0)
                    throw new TsavoriteException($"{(rcs.SecondChanceFraction)} must be >= 0.0 and <= 1.0");
            }

            this.logger = logger;
            if (settings.LogDevice == null)
                throw new TsavoriteException("LogSettings.LogDevice needs to be specified (e.g., use Devices.CreateLogDevice, AzureStorageDevice, or NullDevice)");

            IsReadCache = evictCallback != null;
            EvictCallback = evictCallback;

            FlushCallback = flushCallback;
            PreallocateLog = settings.PreallocateLog;
            FlushEvent.Initialize();

            IsNullDevice = settings.LogDevice is NullDevice;

            if (epoch == null)
            {
                this.epoch = new LightEpoch();
                ownedEpoch = true;
            }
            else
                this.epoch = epoch;

            settings.LogDevice.Initialize(1L << settings.SegmentSizeBits, epoch);
            settings.ObjectLogDevice?.Initialize(-1, epoch);

            // Page size
            LogPageSizeBits = settings.PageSizeBits;
            PageSize = 1 << LogPageSizeBits;
            PageSizeMask = PageSize - 1;

            // Total HLOG size
            LogTotalSizeBits = settings.MemorySizeBits;
            LogTotalSizeBytes = 1L << LogTotalSizeBits;
            BufferSize = (int)(LogTotalSizeBytes / (1L << LogPageSizeBits));
            BufferSizeMask = BufferSize - 1;

            LogMutableFraction = settings.MutableFraction;

            // Segment size
            LogSegmentSizeBits = settings.SegmentSizeBits;
            SegmentSize = 1L << LogSegmentSizeBits;
            SegmentBufferSize = 1 + (LogTotalSizeBytes / SegmentSize < 1 ? 1 : (int)(LogTotalSizeBytes / SegmentSize));

            if (SegmentSize < PageSize)
                throw new TsavoriteException($"Segment ({SegmentSize}) must be at least of page size ({PageSize})");

            if ((LogTotalSizeBits != 0) && (LogTotalSizeBytes < PageSize * 2))
                throw new TsavoriteException($"Memory size ({LogTotalSizeBytes}) must be at least twice the page size ({PageSize})");

            // Readonlymode has MemorySizeBits 0 => skip the check
            if (settings.MemorySizeBits > 0 && settings.MinEmptyPageCount > MaxEmptyPageCount)
                throw new TsavoriteException($"MinEmptyPageCount ({settings.MinEmptyPageCount}) can't be more than MaxEmptyPageCount ({MaxEmptyPageCount})");

            MinEmptyPageCount = settings.MinEmptyPageCount;
            EmptyPageCount = settings.MinEmptyPageCount;

            PageStatusIndicator = new FullPageStatus[BufferSize];

            if (!IsNullDevice)
            {
                PendingFlush = new PendingFlushList[BufferSize];
                for (int i = 0; i < BufferSize; i++)
                    PendingFlush[i] = new PendingFlushList();
            }
            device = settings.LogDevice;
            sectorSize = (int)device.SectorSize;

            if (PageSize < sectorSize)
                throw new TsavoriteException($"Page size must be at least of device sector size ({sectorSize} bytes). Set PageSizeBits accordingly.");

            AlignedPageSizeBytes = (PageSize + (sectorSize - 1)) & ~(sectorSize - 1);
        }

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
            long firstValidSegment = (int)(diskBeginAddress >> LogSegmentSizeBits);

            // Last valid disk segment required for recovery
            var lastValidSegment = (int)(diskFlushedUntilAddress >> LogSegmentSizeBits);
            if ((diskFlushedUntilAddress & ((1L << LogSegmentSizeBits) - 1)) == 0)
                lastValidSegment--;

            logger?.LogInformation("Recovery requires disk segments in range [{firstSegment}--{tailStartSegment}]", firstValidSegment, lastValidSegment);

            var firstAvailSegment = device.StartSegment;
            var lastAvailSegment = device.EndSegment;

            if (FlushedUntilAddress > _wrapper.GetFirstValidLogicalAddress(0))
            {
                int currTailSegment = (int)(FlushedUntilAddress >> LogSegmentSizeBits);
                if ((FlushedUntilAddress & ((1L << LogSegmentSizeBits) - 1)) == 0)
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
                TruncateUntilAddressBlocking(firstValidSegment << LogSegmentSizeBits);

                for (int s = lastValidSegment + 1; s <= lastAvailSegment; s++)
                {
                    logger?.LogInformation("Trimming tail segment {s} on disk", s);
                    RemoveSegment(s);
                }
            }
        }

        /// <summary>Initialize allocator</summary>
        protected void Initialize(long firstValidAddress)
        {
            Debug.Assert(firstValidAddress <= PageSize, $"firstValidAddress {firstValidAddress} shoulld be <= PageSize {PageSize}");

            bufferPool ??= new SectorAlignedBufferPool(1, sectorSize);

            if (BufferSize > 0)
            {
                long tailPage = firstValidAddress >> LogPageSizeBits;
                int tailPageIndex = (int)(tailPage % BufferSize);
                if (!_wrapper.IsAllocated(tailPageIndex))
                    _wrapper.AllocatePage(tailPageIndex);

                // Allocate next page as well
                int nextPageIndex = (int)(tailPage + 1) % BufferSize;
                if (!_wrapper.IsAllocated(nextPageIndex))
                    _wrapper.AllocatePage(nextPageIndex);
            }

            if (PreallocateLog)
            {
                for (int i = 0; i < BufferSize; i++)
                {
                    if (!_wrapper.IsAllocated(i))
                        _wrapper.AllocatePage(i);
                }
            }

            SafeReadOnlyAddress = firstValidAddress;
            ReadOnlyAddress = firstValidAddress;
            SafeHeadAddress = firstValidAddress;
            HeadAddress = firstValidAddress;
            ClosedUntilAddress = firstValidAddress;
            FlushedUntilAddress = firstValidAddress;
            BeginAddress = firstValidAddress;

            TailPageOffset.Page = (int)(firstValidAddress >> LogPageSizeBits);
            TailPageOffset.Offset = (int)(firstValidAddress & PageSizeMask);
        }

        /// <summary>Number of pages in circular buffer that are allocated</summary>
        public int AllocatedPageCount;

        /// <summary>Max number of pages that have been allocated at any point in time</summary>
        public int MaxAllocatedPageCount;

        /// <summary>Maximum possible number of empty pages in circular buffer</summary>
        public int MaxEmptyPageCount => BufferSize - 1;

        /// <summary>Minimum number of empty pages in circular buffer to be maintained to account for non-power-of-two size</summary>
        public int MinEmptyPageCount
        {
            get => minEmptyPageCount;
            set
            {
                minEmptyPageCount = value;
                if (emptyPageCount != minEmptyPageCount)
                {
                    EmptyPageCount = minEmptyPageCount;
                }
            }
        }

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
                    ReadOnlyAddressLagOffset = (long)(LogMutableFraction * headOffsetLagSize) << LogPageSizeBits;
                    HeadAddressLagOffset = (long)headOffsetLagSize << LogPageSizeBits;
                }

                // Force eviction now if empty page count has increased
                if (value >= oldEPC)
                {
                    bool prot = epoch.ThisInstanceProtected();

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
                Thread.Yield();
                local = TailPageOffset;
            }
            return ((long)local.Page << LogPageSizeBits) | (uint)local.Offset;
        }

        /// <summary>Get page index from <paramref name="logicalAddress"/></summary>
        public long GetPage(long logicalAddress) => logicalAddress >> LogPageSizeBits;

        /// <summary>Get page index for page</summary>
        public int GetPageIndexForPage(long page) => (int)(page % BufferSize);

        /// <summary>Get page index for address</summary>
        public int GetPageIndexForAddress(long address) => (int)((address >> LogPageSizeBits) % BufferSize);

        /// <summary>Get capacity (number of pages)</summary>
        public int GetCapacityNumPages() => BufferSize;

        /// <summary>Get page size</summary>
        public long GetPageSize() => PageSize;

        /// <summary>Get offset in page</summary>
        public long GetOffsetInPage(long address) => address & PageSizeMask;

        /// <summary>Get sector size for main hlog device</summary>
        public int GetDeviceSectorSize() => sectorSize;

        [MethodImpl(MethodImplOptions.NoInlining)]
        void AllocatePagesWithException(int pageIndex, PageOffset localTailPageOffset, int numSlots)
        {
            try
            {
                // Allocate this page, if needed
                if (!_wrapper.IsAllocated(pageIndex % BufferSize))
                    _wrapper.AllocatePage(pageIndex % BufferSize);

                // Allocate next page in advance, if needed
                if (!_wrapper.IsAllocated((pageIndex + 1) % BufferSize))
                    _wrapper.AllocatePage((pageIndex + 1) % BufferSize);
            }
            catch
            {
                // Reset to previous tail
                localTailPageOffset.PageAndOffset -= numSlots;
                Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);
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
            var tailAddress = (((long)localTailPageOffset.Page) << LogPageSizeBits) | ((long)(localTailPageOffset.Offset - numSlots));
            var shiftAddress = pageIndex << LogPageSizeBits;

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
            var shiftAddress = pageIndex << LogPageSizeBits;
            var tailAddress = GetTailAddress();

            long desiredReadOnlyAddress = shiftAddress - ReadOnlyAddressLagOffset;
            if (desiredReadOnlyAddress > tailAddress)
                desiredReadOnlyAddress = tailAddress;
            ShiftReadOnlyAddress(desiredReadOnlyAddress);

            long desiredHeadAddress = shiftAddress - HeadAddressLagOffset;
            if (desiredHeadAddress > tailAddress)
                desiredHeadAddress = tailAddress;
            ShiftHeadAddress(desiredHeadAddress);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        long HandlePageOverflow(ref PageOffset localTailPageOffset, int numSlots)
        {
            int pageIndex = localTailPageOffset.Page + 1;

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
                Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);

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
                Interlocked.Exchange(ref TailPageOffset.PageAndOffset, localTailPageOffset.PageAndOffset);

                // Shift only after TailPageOffset is reset to a valid state
                IssueShiftAddress(pageIndex);

                return -1; // RETRY_NOW
            }

            // Allocate next page and set new tail
            if (!_wrapper.IsAllocated(pageIndex % BufferSize) || !_wrapper.IsAllocated((pageIndex + 1) % BufferSize))
                AllocatePagesWithException(pageIndex, localTailPageOffset, numSlots);

            localTailPageOffset.Page++;
            localTailPageOffset.Offset = numSlots;
            TailPageOffset = localTailPageOffset;

            // At this point, the slot is allocated and we are not allowed to refresh epochs any longer.

            // Offset is zero, for the first allocation on the new page
            return ((long)localTailPageOffset.Page) << LogPageSizeBits;
        }

        /// <summary>Try allocate, no thread spinning allowed</summary>
        /// <param name="numSlots">Number of slots to allocate</param>
        /// <returns>The allocated logical address, or 0 in case of inability to allocate</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        long TryAllocate(int numSlots = 1)
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

            // Slow path when we reach the end of a page.
            if (localTailPageOffset.Offset > PageSize)
            {
                // Note that TailPageOffset is now unstable -- there may be a GetTailAddress call spinning for
                // it to stabilize. Therefore, HandlePageOverflow needs to stabilize TailPageOffset immediately,
                // before performing any epoch bumps or system calls.
                return HandlePageOverflow(ref localTailPageOffset, numSlots);
            }
            return (((long)localTailPageOffset.Page) << LogPageSizeBits) | ((long)(localTailPageOffset.Offset - numSlots));
        }

        /// <summary>Try allocate, spin for RETRY_NOW (logicalAddress is less than 0) case</summary>
        /// <param name="numSlots">Number of slots to allocate</param>
        /// <returns>The allocated logical address, or 0 in case of inability to allocate</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long TryAllocateRetryNow(int numSlots = 1)
        {
            long logicalAddress;
            while ((logicalAddress = TryAllocate(numSlots)) < 0)
            {
                _ = TryComplete();
                epoch.ProtectAndDrain();
                Thread.Yield();
            }
            return logicalAddress;
        }

        /// <summary>
        /// If the page we are trying to allocate is past the last page with an unclosed address region, 
        /// then we can retry immediately because this is called after NeedToWait, so we know we've 
        /// completed the wait on flushEvent for the necessary pages to be flushed, and are waiting for
        /// OnPagesClosed to be completed.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool CannotAllocate(int page) => page >= BufferSize + (ClosedUntilAddress >> LogPageSizeBits);

        /// <summary>
        /// If the page we are trying to allocate is past the last page with an unflushed address region, 
        /// we have to wait for the flushEvent.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool NeedToWait(int page) => page >= BufferSize + (FlushedUntilAddress >> LogPageSizeBits);

        /// <summary>Used by applications to make the current state of the database immutable quickly</summary>
        public bool ShiftReadOnlyToTail(out long tailAddress, out SemaphoreSlim notifyDone)
        {
            notifyDone = null;
            tailAddress = GetTailAddress();
            long localTailAddress = tailAddress;
            if (Utility.MonotonicUpdate(ref ReadOnlyAddress, tailAddress, out _))
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
            if (Utility.MonotonicUpdate(ref ReadOnlyAddress, newReadOnlyAddress, out _))
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
            if (!Utility.MonotonicUpdate(ref BeginAddress, newBeginAddress, out _))
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
            var h = Utility.MonotonicUpdate(ref HeadAddress, newBeginAddress, out _);

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
            var start = page << LogPageSizeBits;
            var end = (page + 1) << LogPageSizeBits;
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
            if (Utility.MonotonicUpdate(ref SafeReadOnlyAddress, newSafeReadOnlyAddress, out long oldSafeReadOnlyAddress))
            {
                // Debug.WriteLine("SafeReadOnly shifted from {0:X} to {1:X}", oldSafeReadOnlyAddress, newSafeReadOnlyAddress);
                if (OnReadOnlyObserver != null)
                {
                    // This scan does not need a store because it does not lock; it is epoch-protected so by the time it runs no current thread
                    // will have seen a record below the new ReadOnlyAddress as "in mutable region".
                    using var iter = Scan(store: null, oldSafeReadOnlyAddress, newSafeReadOnlyAddress, ScanBufferingMode.NoBuffering);
                    OnReadOnlyObserver?.OnNext(iter);
                }
                AsyncFlushPages(oldSafeReadOnlyAddress, newSafeReadOnlyAddress, noFlush);
            }
        }

        /// <summary>Action to be performed for when all threads have agreed that a page range is closed.</summary>
        private void OnPagesClosed(long newSafeHeadAddress)
        {
            Debug.Assert(newSafeHeadAddress > 0);
            if (Utility.MonotonicUpdate(ref SafeHeadAddress, newSafeHeadAddress, out _))
            {
                // This thread is responsible for [oldSafeHeadAddress -> newSafeHeadAddress]
                for (; ; Thread.Yield())
                {
                    long _ongoingCloseUntilAddress = OngoingCloseUntilAddress;

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
                }
            }
        }

        private void OnPagesClosedWorker()
        {
            for (; ; Thread.Yield())
            {
                long closeStartAddress = ClosedUntilAddress;
                long closeEndAddress = OngoingCloseUntilAddress;

                if (IsReadCache)
                    EvictCallback(closeStartAddress, closeEndAddress);

                for (long closePageAddress = closeStartAddress & ~PageSizeMask; closePageAddress < closeEndAddress; closePageAddress += PageSize)
                {
                    long start = closeStartAddress > closePageAddress ? closeStartAddress : closePageAddress;
                    long end = closeEndAddress < closePageAddress + PageSize ? closeEndAddress : closePageAddress + PageSize;

                    // This scan does not need a store because it does not lock; it is epoch-protected so by the time it runs no current thread
                    // will have seen a record below the eviction range as "in mutable region".
                    if (OnEvictionObserver is not null)
                        MemoryPageScan(start, end, OnEvictionObserver);

                    // If we are using a null storage device, we must also shift BeginAddress 
                    if (IsNullDevice)
                        _ = Utility.MonotonicUpdate(ref BeginAddress, end, out _);

                    // If the end of the closing range is at the end of the page, free the page
                    if (end == closePageAddress + PageSize)
                        _wrapper.FreePage((int)(closePageAddress >> LogPageSizeBits));

                    _ = Utility.MonotonicUpdate(ref ClosedUntilAddress, end, out _);
                }

                // End if we have exhausted co-operative work
                if (Interlocked.CompareExchange(ref OngoingCloseUntilAddress, 0, closeEndAddress) == closeEndAddress)
                    break;
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

            Console.WriteLine("BeginAddress: {0}.{1}", GetPage(_begin), GetOffsetInPage(_begin));
            Console.WriteLine("ClosedUntilAddress: {0}.{1}", GetPage(_closedUntil), GetOffsetInPage(_closedUntil));
            Console.WriteLine("SafeHead: {0}.{1}", GetPage(_safehead), GetOffsetInPage(_safehead));
            Console.WriteLine("Head: {0}.{1}", GetPage(_head), GetOffsetInPage(_head));
            Console.WriteLine("FlushedUntil: {0}.{1}", GetPage(_flush), GetOffsetInPage(_flush));
            Console.WriteLine("SafeReadOnly: {0}.{1}", GetPage(_safereadonly), GetOffsetInPage(_safereadonly));
            Console.WriteLine("ReadOnly: {0}.{1}", GetPage(_readonly), GetOffsetInPage(_readonly));
            Console.WriteLine("Tail: {0}.{1}", GetPage(_tail), GetOffsetInPage(_tail));
        }

        /// <summary>
        /// Called every time a new tail page is allocated. Here the read-only is shifted only to page boundaries 
        /// unlike ShiftReadOnlyToTail where shifting can happen to any fine-grained address.
        /// </summary>
        private void PageAlignedShiftReadOnlyAddress(long currentTailAddress)
        {
            long pageAlignedTailAddress = currentTailAddress & ~PageSizeMask;
            long desiredReadOnlyAddress = pageAlignedTailAddress - ReadOnlyAddressLagOffset;
            if (Utility.MonotonicUpdate(ref ReadOnlyAddress, desiredReadOnlyAddress, out _))
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
            var desiredHeadAddress = (currentTailAddress & ~PageSizeMask) - HeadAddressLagOffset;

            // Obtain local values of variables that can change
            var currentFlushedUntilAddress = FlushedUntilAddress;
            if (desiredHeadAddress > currentFlushedUntilAddress)
                desiredHeadAddress = currentFlushedUntilAddress & ~PageSizeMask;

            if (Utility.MonotonicUpdate(ref HeadAddress, desiredHeadAddress, out _))
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

            if (newHeadAddress % (1 << LogPageSizeBits) != 0)
            {

            }
            if (Utility.MonotonicUpdate(ref HeadAddress, newHeadAddress, out _))
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
            while (pageLastFlushedAddress >= currentFlushedUntilAddress && currentFlushedUntilAddress >= (page << LogPageSizeBits))
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
                if (Utility.MonotonicUpdate(ref FlushedUntilAddress, currentFlushedUntilAddress, out long oldFlushedUntilAddress))
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
        public void RecoveryReset(long tailAddress, long headAddress, long beginAddress, long readonlyAddress)
        {
            long tailPage = GetPage(tailAddress);
            long offsetInPage = GetOffsetInPage(tailAddress);
            TailPageOffset.Page = (int)tailPage;
            TailPageOffset.Offset = (int)offsetInPage;

            // Allocate current page if necessary
            var pageIndex = TailPageOffset.Page % BufferSize;
            if (!_wrapper.IsAllocated(pageIndex))
                _wrapper.AllocatePage(pageIndex);

            // Allocate next page as well - this is an invariant in the allocator!
            var nextPageIndex = (pageIndex + 1) % BufferSize;
            if (!_wrapper.IsAllocated(nextPageIndex))
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
            _wrapper.ClearPage(pageIndex, (int)GetOffsetInPage(tailAddress));

            // Printing debug info
            logger?.LogInformation("******* Recovered HybridLog Stats *******");
            logger?.LogInformation("Head Address: {HeadAddress}", HeadAddress);
            logger?.LogInformation("Safe Head Address: {SafeHeadAddress}", SafeHeadAddress);
            logger?.LogInformation("ReadOnly Address: {ReadOnlyAddress}", ReadOnlyAddress);
            logger?.LogInformation("Safe ReadOnly Address: {SafeReadOnlyAddress}", SafeReadOnlyAddress);
            logger?.LogInformation("Tail Address: {tailAddress}", tailAddress);
        }

        /// <summary>Invoked by users to obtain a record from disk. It uses sector aligned memory to read the record efficiently into memory.</summary>
        internal unsafe void AsyncReadRecordToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, ref AsyncIOContext<TKey, TValue> context)
        {
            var fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + (fromLogical & PageSizeMask));
            var alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            var alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            var record = bufferPool.Get((int)alignedReadLength);
            record.valid_offset = (int)(fileOffset - alignedFileOffset);
            record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));
            record.required_bytes = numBytes;

            var asyncResult = default(AsyncGetFromDiskResult<AsyncIOContext<TKey, TValue>>);
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
        /// <param name="fromLogical"></param>
        /// <param name="numBytes"></param>
        /// <param name="callback"></param>
        /// <param name="context"></param>
        internal unsafe void AsyncReadRecordToMemory(long fromLogical, int numBytes, DeviceIOCompletionCallback callback, ref SimpleReadContext context)
        {
            var fileOffset = (ulong)(AlignedPageSizeBytes * (fromLogical >> LogPageSizeBits) + (fromLogical & PageSizeMask));
            var alignedFileOffset = (ulong)(((long)fileOffset / sectorSize) * sectorSize);

            var alignedReadLength = (uint)((long)fileOffset + numBytes - (long)alignedFileOffset);
            alignedReadLength = (uint)((alignedReadLength + (sectorSize - 1)) & ~(sectorSize - 1));

            context.record = bufferPool.Get((int)alignedReadLength);
            context.record.valid_offset = (int)(fileOffset - alignedFileOffset);
            context.record.available_bytes = (int)(alignedReadLength - (fileOffset - alignedFileOffset));
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
                                IDevice logDevice = null, IDevice objectLogDevice = null)
            => AsyncReadPagesFromDevice(readPageStart, numPages, untilAddress, callback, context, out _, devicePageOffset, logDevice, objectLogDevice);

        /// <summary>Read pages from specified device</summary>
        private void AsyncReadPagesFromDevice<TContext>(
                                        long readPageStart,
                                        int numPages,
                                        long untilAddress,
                                        DeviceIOCompletionCallback callback,
                                        TContext context,
                                        out CountdownEvent completed,
                                        long devicePageOffset = 0,
                                        IDevice device = null, IDevice objectLogDevice = null)
        {
            var usedDevice = device ?? this.device;
            IDevice usedObjlogDevice = objectLogDevice;

            completed = new CountdownEvent(numPages);
            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                var pageIndex = (int)(readPage % BufferSize);
                if (!_wrapper.IsAllocated(pageIndex))
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
                long adjustedUntilAddress = AlignedPageSizeBytes * (untilAddress >> LogPageSizeBits) + (untilAddress & PageSizeMask);

                if (adjustedUntilAddress > 0 && ((adjustedUntilAddress - (long)offsetInFile) < PageSize))
                {
                    readLength = (uint)(adjustedUntilAddress - (long)offsetInFile);
                    asyncResult.maxPtr = readLength;
                    readLength = (uint)((readLength + (sectorSize - 1)) & ~(sectorSize - 1));
                }

                if (device != null)
                    offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - devicePageOffset));

                ReadAsync(offsetInFile, pageIndex, readLength, callback, asyncResult, usedDevice, usedObjlogDevice);
            }
        }

        /// <summary>
        /// Flush page range to disk
        /// Called when all threads have agreed that a page range is sealed.
        /// </summary>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        /// <param name="noFlush"></param>
        public void AsyncFlushPages(long fromAddress, long untilAddress, bool noFlush = false)
        {
            long startPage = fromAddress >> LogPageSizeBits;
            long endPage = untilAddress >> LogPageSizeBits;
            var numPages = (int)(endPage - startPage);

            long offsetInEndPage = GetOffsetInPage(untilAddress);

            // Extra (partial) page being flushed
            if (offsetInEndPage > 0)
                numPages++;

            /* Request asynchronous writes to the device. If waitForPendingFlushComplete
             * is set, then a CountDownEvent is set in the callback handle.
             */
            for (long flushPage = startPage; flushPage < (startPage + numPages); flushPage++)
            {
                long pageStartAddress = flushPage << LogPageSizeBits;
                long pageEndAddress = (flushPage + 1) << LogPageSizeBits;

                var asyncResult = new PageAsyncFlushResult<Empty>
                {
                    page = flushPage,
                    count = 1,
                    partial = false,
                    fromAddress = pageStartAddress,
                    untilAddress = pageEndAddress
                };
                if (
                    ((fromAddress > pageStartAddress) && (fromAddress < pageEndAddress)) ||
                    ((untilAddress > pageStartAddress) && (untilAddress < pageEndAddress))
                    )
                {
                    asyncResult.partial = true;

                    if (untilAddress < pageEndAddress)
                        asyncResult.untilAddress = untilAddress;

                    if (fromAddress > pageStartAddress)
                        asyncResult.fromAddress = fromAddress;
                }

                bool skip = false;
                if (asyncResult.untilAddress <= BeginAddress)
                {
                    // Short circuit as no flush needed
                    _ = Utility.MonotonicUpdate(ref PageStatusIndicator[flushPage % BufferSize].LastFlushedUntilAddress, BeginAddress, out _);
                    ShiftFlushedUntilAddress();
                    skip = true;
                }

                if (IsNullDevice || noFlush)
                {
                    // Short circuit as no flush needed
                    _ = Utility.MonotonicUpdate(ref PageStatusIndicator[flushPage % BufferSize].LastFlushedUntilAddress, asyncResult.untilAddress, out _);
                    ShiftFlushedUntilAddress();
                    skip = true;
                }

                if (skip) continue;

                // Partial page starting point, need to wait until the
                // ongoing adjacent flush is completed to ensure correctness
                if (GetOffsetInPage(asyncResult.fromAddress) > 0)
                {
                    var index = GetPageIndexForAddress(asyncResult.fromAddress);

                    // Try to merge request with existing adjacent (earlier) pending requests
                    while (PendingFlush[index].RemovePreviousAdjacent(asyncResult.fromAddress, out var existingRequest))
                        asyncResult.fromAddress = existingRequest.fromAddress;

                    // Enqueue work in shared queue
                    PendingFlush[index].Add(asyncResult);

                    // Perform work from shared queue if possible
                    if (PendingFlush[index].RemoveNextAdjacent(FlushedUntilAddress, out PageAsyncFlushResult<Empty> request))
                        WriteAsync(request.fromAddress >> LogPageSizeBits, AsyncFlushPageCallback, request);
                }
                else
                    WriteAsync(flushPage, AsyncFlushPageCallback, asyncResult);
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
                    untilAddress = (flushPage + 1) << LogPageSizeBits
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
        /// <param name="objectLogDevice"></param>
        /// <param name="completedSemaphore"></param>
        /// <param name="throttleCheckpointFlushDelayMs"></param>
        public void AsyncFlushPagesToDevice(long startPage, long endPage, long endLogicalAddress, long fuzzyStartLogicalAddress, IDevice device, IDevice objectLogDevice, out SemaphoreSlim completedSemaphore, int throttleCheckpointFlushDelayMs)
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
                var localSegmentOffsets = new long[SegmentBufferSize];

                for (long flushPage = startPage; flushPage < endPage; flushPage++)
                {
                    long flushPageAddress = flushPage << LogPageSizeBits;
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
                    WriteAsyncToDevice(startPage, flushPage, pageSize, AsyncFlushPageToDeviceCallback, asyncResult, device, objectLogDevice, localSegmentOffsets, fuzzyStartLogicalAddress);

                    if (throttleCheckpointFlushDelayMs >= 0)
                    {
                        flushCompletionTracker.WaitOneFlush();
                        Thread.Sleep(throttleCheckpointFlushDelayMs);
                    }
                }
            }
        }

        internal void AsyncGetFromDisk(long fromLogical, int numBytes, AsyncIOContext<TKey, TValue> context, SectorAlignedMemory result = default)
        {
            if (epoch.ThisInstanceProtected()) // Do not spin for unprotected IO threads
            {
                while (device.Throttle())
                {
                    _ = device.TryComplete();
                    _ = Thread.Yield();
                    epoch.ProtectAndDrain();
                }
            }

            if (result == null)
                AsyncReadRecordToMemory(fromLogical, numBytes, AsyncGetFromDiskCallback, ref context);
            else
                AsyncReadRecordObjectsToMemory(fromLogical, numBytes, AsyncGetFromDiskCallback, context, result);
        }

        private unsafe void AsyncGetFromDiskCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                logger?.LogError("AsyncGetFromDiskCallback error: {0}", errorCode);

            var result = (AsyncGetFromDiskResult<AsyncIOContext<TKey, TValue>>)context;
            var ctx = result.context;
            try
            {
                var record = ctx.record.GetValidPointer();
                int requiredBytes = _wrapper.GetRequiredRecordSize((long)record, ctx.record.available_bytes);
                if (ctx.record.available_bytes >= requiredBytes)
                {
                    Debug.Assert(!_wrapper.GetInfoFromBytePointer(record).Invalid, "Invalid records should not be in the hash chain for pending IO");

                    // We have all the required bytes. If we don't have the complete record, RetrievedFullRecord calls AsyncGetFromDisk.
                    if (!_wrapper.RetrievedFullRecord(record, ref ctx))
                        return;

                    // If request_key is null we're called from ReadAtAddress, so it is an implicit match.
                    if (ctx.request_key is not null && !_storeFunctions.KeysEqual(ref ctx.request_key.Get(), ref _wrapper.GetContextRecordKey(ref ctx)))
                    {
                        // Keys don't match so request the previous record in the chain if it is in the range to resolve.
                        ctx.logicalAddress = _wrapper.GetInfoFromBytePointer(record).PreviousAddress;
                        if (ctx.logicalAddress >= BeginAddress && ctx.logicalAddress >= ctx.minAddress)
                        {
                            ctx.record.Return();
                            ctx.record = ctx.objBuffer = default;
                            AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, ctx);
                            return;
                        }
                    }

                    // Either the keys match or we are below the range to retrieve (which ContinuePending* will detect), so we're done.
                    if (ctx.completionEvent is not null)
                        ctx.completionEvent.Set(ref ctx);
                    else if (ctx.callbackQueue is not null)
                        ctx.callbackQueue.Enqueue(ctx);
                    else
                        _ = ctx.asyncOperation.TrySetResult(ctx);
                }
                else
                {
                    ctx.record.Return();
                    AsyncGetFromDisk(ctx.logicalAddress, requiredBytes, ctx);
                }
            }
            catch (Exception e)
            {
                logger?.LogError(e, "AsyncGetFromDiskCallback error");
                if (ctx.completionEvent is not null)
                    ctx.completionEvent.SetException(e);
                else if (ctx.asyncOperation is not null)
                    _ = ctx.asyncOperation.TrySetException(e);
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
                        // Note down error details and trigger handling only when we are certain this is the earliest
                        // error among currently issued flushes
                        errorList.Add(new CommitInfo { FromAddress = result.fromAddress, UntilAddress = result.untilAddress, ErrorCode = errorCode });
                    }
                    else
                    {
                        // Update the page's last flushed until address only if there is no failure.
                        _ = Utility.MonotonicUpdate(
                            ref PageStatusIndicator[result.page % BufferSize].LastFlushedUntilAddress,
                            result.untilAddress, out _);
                    }

                    ShiftFlushedUntilAddress();
                    result.Free();
                }

                var _flush = FlushedUntilAddress;
                if (GetOffsetInPage(_flush) > 0 && PendingFlush[GetPage(_flush) % BufferSize].RemoveNextAdjacent(_flush, out PageAsyncFlushResult<Empty> request))
                    WriteAsync(request.fromAddress >> LogPageSizeBits, AsyncFlushPageCallback, request);
            }
            catch when (disposed) { }
        }

        internal void UnsafeSkipError(CommitInfo info)
        {
            try
            {
                errorList.TruncateUntil(info.UntilAddress);
                var page = info.FromAddress >> PageSizeMask;
                _ = Utility.MonotonicUpdate(ref PageStatusIndicator[page % BufferSize].LastFlushedUntilAddress, info.UntilAddress, out _);
                ShiftFlushedUntilAddress();
                var _flush = FlushedUntilAddress;
                if (GetOffsetInPage(_flush) > 0 && PendingFlush[GetPage(_flush) % BufferSize].RemoveNextAdjacent(_flush, out PageAsyncFlushResult<Empty> request))
                    WriteAsync(request.fromAddress >> LogPageSizeBits, AsyncFlushPageCallback, request);
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
                    var startAddress = result.page << LogPageSizeBits;
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
                        var physicalAddress = _wrapper.GetPhysicalAddress(startAddress);
                        var endPhysicalAddress = physicalAddress + flushWidth;

                        while (physicalAddress < endPhysicalAddress)
                        {
                            ref var info = ref _wrapper.GetInfo(physicalAddress);
                            var (_, alignedRecordSize) = _wrapper.GetRecordSize(physicalAddress);
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

        internal string PrettyPrintLogicalAddress(long logicalAddress) => $"{logicalAddress}:{GetPage(logicalAddress)}.{GetOffsetInPage(logicalAddress)}";
    }
}