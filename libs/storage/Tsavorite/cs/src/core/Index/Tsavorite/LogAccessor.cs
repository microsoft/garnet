// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Tsavorite.core
{
    /// <summary>
    /// Wrapper to process log-related commands
    /// </summary>
    public sealed class LogAccessor<TStoreFunctions, TAllocator> : IObservable<ITsavoriteScanIterator>
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        private readonly TsavoriteKV<TStoreFunctions, TAllocator> store;
        private readonly TAllocator allocator;
        private readonly AllocatorBase<TStoreFunctions, TAllocator> allocatorBase;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="store"></param>
        /// <param name="allocator"></param>
        internal LogAccessor(TsavoriteKV<TStoreFunctions, TAllocator> store, TAllocator allocator)
        {
            this.store = store;
            this.allocator = allocator;
            allocatorBase = allocator.GetBase<TAllocator>();
        }

        /// <summary>
        /// Tail address of log
        /// </summary>
        public long TailAddress => allocatorBase.GetTailAddress();

        /// <summary>
        /// Read-only address of log, i.e. boundary between read-only region and mutable region
        /// </summary>
        public long ReadOnlyAddress => allocatorBase.ReadOnlyAddress;

        /// <summary>
        /// Safe read-only address of log, i.e. boundary between read-only region and mutable region
        /// </summary>
        public long SafeReadOnlyAddress => allocatorBase.SafeReadOnlyAddress;

        /// <summary>
        /// Head address of log, i.e. beginning of in-memory regions
        /// </summary>
        public long HeadAddress => allocatorBase.HeadAddress;

        /// <summary>
        /// Beginning address of log
        /// </summary>
        public long BeginAddress => allocatorBase.BeginAddress;

        /// <summary>
        /// Number of pages left empty or unallocated in the in-memory buffer (between 0 and BufferSize-1)
        /// </summary>
        public int EmptyPageCount
        {
            get => allocatorBase.EmptyPageCount;
            set { allocatorBase.EmptyPageCount = value; }
        }

        /// <summary>
        /// Maximum possible number of empty pages in Allocator
        /// </summary>
        public int MaxEmptyPageCount => allocatorBase.MaxEmptyPageCount;

        /// <summary>
        /// Minimum possible number of empty pages in Allocator
        /// </summary>
        public int MinEmptyPageCount
        {
            get => allocatorBase.MinEmptyPageCount;
            set { allocatorBase.MinEmptyPageCount = value; }
        }

        /// <summary>
        /// <see cref="ObjectIdMap"/> for serializing/deserializing <see cref="DiskLogRecord"/>.
        /// </summary>
        public ObjectIdMap TransientObjectIdMap => allocatorBase.transientObjectIdMap;

        /// <summary>
        /// Set empty page count in allocator
        /// </summary>
        /// <param name="pageCount">New empty page count</param>
        /// <param name="wait">Whether to wait for shift addresses to complete</param>
        public void SetEmptyPageCount(int pageCount, bool wait = false)
        {
            allocatorBase.EmptyPageCount = pageCount;
            if (wait)
            {
                long newHeadAddress = allocatorBase.GetAddressOfStartOfPageOfAddress(allocatorBase.GetTailAddress()) - allocatorBase.HeadAddressLagOffset;
                ShiftHeadAddress(newHeadAddress, wait);
            }
        }

        /// <summary>
        /// Total in-memory circular buffer capacity (in number of pages)
        /// </summary>
        public int BufferSize => allocatorBase.BufferSize;

        /// <summary>
        /// Actual memory used by log (not including heap objects) and overflow pages
        /// </summary>
        public long MemorySizeBytes => allocatorBase.GetLogicalAddressOfStartOfPage((long)(allocatorBase.AllocatedPageCount + allocator.OverflowPageCount));

        /// <summary>
        /// Maximum memory size in bytes
        /// </summary>
        public long MaxMemorySizeBytes => allocatorBase.MaxMemorySizeBytes;

        /// <summary>
        /// Number of pages allocated
        /// </summary>
        public int AllocatedPageCount => allocatorBase.AllocatedPageCount;

        /// <summary>
        /// Shift begin address to the provided untilAddress. Make sure address corresponds to record boundary if snapToPageStart is set to 
        /// false. Destructive operation if truncateLog is set to true.
        /// </summary>
        /// <param name="untilAddress">Address to shift begin address until</param>
        /// <param name="snapToPageStart">Whether given address should be snapped to nearest earlier page start address</param>
        /// <param name="truncateLog">If true, we will also truncate the log on disk until the given BeginAddress. Truncate is a destructive operation 
        /// that can result in data loss. If false, log will be truncated after the next checkpoint.</param>
        public void ShiftBeginAddress(long untilAddress, bool snapToPageStart = false, bool truncateLog = false)
        {
            if (snapToPageStart)
                untilAddress = allocatorBase.GetAddressOfStartOfPageOfAddress(untilAddress);

            var epochProtected = store.epoch.ThisInstanceProtected();
            try
            {
                if (!epochProtected)
                    store.epoch.Resume();
                allocatorBase.ShiftBeginAddress(untilAddress, truncateLog);
            }
            finally
            {
                if (!epochProtected)
                    store.epoch.Suspend();
            }
        }

        /// <summary>
        /// Truncate physical log on disk until the current BeginAddress. Use ShiftBeginAddress to shift the begin address.
        /// Truncate is a destructive operation that can result in data loss. For data safety, take a checkpoint instead of 
        /// using this call, as a checkpoint truncates the log to the BeginAddress after persisting the data and metadata.
        /// </summary>
        public void Truncate() => ShiftBeginAddress(BeginAddress, truncateLog: true);

        /// <summary>
        /// Shift log head address to prune memory foorprint of hybrid log
        /// </summary>
        /// <param name="newHeadAddress">Address to shift head until</param>
        /// <param name="wait">Wait for operation to complete (may involve page flushing and closing)</param>
        public void ShiftHeadAddress(long newHeadAddress, bool wait)
        {
            // First shift read-only
            // Force wait so that we do not close unflushed page
            ShiftReadOnlyAddress(newHeadAddress, true);

            // Then shift head address
            if (!store.epoch.ThisInstanceProtected())
            {
                try
                {
                    store.epoch.Resume();
                    allocatorBase.ShiftHeadAddress(newHeadAddress);
                }
                finally
                {
                    store.epoch.Suspend();
                }

                while (wait && allocatorBase.SafeHeadAddress < newHeadAddress)
                    _ = Thread.Yield();
            }
            else
            {
                allocatorBase.ShiftHeadAddress(newHeadAddress);
                while (wait && allocatorBase.SafeHeadAddress < newHeadAddress)
                    store.epoch.ProtectAndDrain();
            }
        }

        public Func<bool> IsSizeBeyondLimit
        {
            get => allocatorBase.IsSizeBeyondLimit;
            set => allocatorBase.IsSizeBeyondLimit = value;
        }

        /// <summary>
        /// Subscribe to records (in batches) as they become read-only in the log
        /// Currently, we support only one subscriber to the log (easy to extend)
        /// Subscriber only receives new log updates from the time of subscription onwards
        /// To scan the historical part of the log, use the Scan(...) method
        /// </summary>
        /// <param name="readOnlyObserver">Observer to which scan iterator is pushed</param>
        public IDisposable Subscribe(IObserver<ITsavoriteScanIterator> readOnlyObserver)
        {
            allocatorBase.OnReadOnlyObserver = readOnlyObserver;
            return new LogSubscribeDisposable(allocatorBase, isReadOnly: true);
        }

        /// <summary>
        /// Subscribe to records (in batches) as they get evicted from main memory.
        /// Currently, we support only one subscriber to the log (easy to extend)
        /// Subscriber only receives eviction updates from the time of subscription onwards
        /// To scan the historical part of the log, use the Scan(...) method
        /// </summary>
        /// <param name="evictionObserver">Observer to which scan iterator is pushed</param>
        public IDisposable SubscribeEvictions(ITsavoriteRecordObserver<ITsavoriteScanIterator> evictionObserver)
        {
            allocatorBase.OnEvictionObserver = evictionObserver;
            return new LogSubscribeDisposable(allocatorBase, isReadOnly: false);
        }

        public IDisposable SubscribeDeserializations(ITsavoriteRecordObserver<ITsavoriteScanIterator> deserializationObserver)
        {
            allocatorBase.OnDeserializationObserver = deserializationObserver;
            return new LogSubscribeDisposable(allocatorBase, isReadOnly: false);
        }

        /// <summary>
        /// Wrapper to help dispose the subscription
        /// </summary>
        class LogSubscribeDisposable : IDisposable
        {
            private readonly AllocatorBase<TStoreFunctions, TAllocator> allocator;
            private readonly bool readOnly;

            public LogSubscribeDisposable(AllocatorBase<TStoreFunctions, TAllocator> allocator, bool isReadOnly)
            {
                this.allocator = allocator;
                readOnly = isReadOnly;
            }

            public void Dispose()
            {
                if (readOnly)
                    allocator.OnReadOnlyObserver = null;
                else
                    allocator.OnEvictionObserver = null;
            }
        }

        /// <summary>
        /// Shift log read-only address
        /// </summary>
        /// <param name="newReadOnlyAddress">Address to shift read-only until</param>
        /// <param name="wait">Wait to ensure shift is complete (may involve page flushing)</param>
        public void ShiftReadOnlyAddress(long newReadOnlyAddress, bool wait)
        {
            if (!store.epoch.ThisInstanceProtected())
            {
                try
                {
                    store.epoch.Resume();
                    _ = allocatorBase.ShiftReadOnlyAddress(newReadOnlyAddress);
                }
                finally
                {
                    store.epoch.Suspend();
                }

                // Wait for flush to complete
                while (wait && allocatorBase.FlushedUntilAddress < newReadOnlyAddress)
                    _ = Thread.Yield();
            }
            else
            {
                _ = allocatorBase.ShiftReadOnlyAddress(newReadOnlyAddress);

                // Wait for flush to complete
                while (wait && allocatorBase.FlushedUntilAddress < newReadOnlyAddress)
                    store.epoch.ProtectAndDrain();
            }
        }

        /// <summary>
        /// Pull-scan the log given address range; returns all records with address less than endAddress
        /// </summary>
        /// <returns>Scan iterator instance</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ITsavoriteScanIterator Scan(long beginAddress, long endAddress, DiskScanBufferingMode scanBufferingMode = DiskScanBufferingMode.DoublePageBuffering, bool includeClosedRecords = false)
            => allocatorBase.Scan(store: null, beginAddress, endAddress, scanBufferingMode, includeClosedRecords);

        /// <summary>
        /// Push-scan the log given address range; returns all records with address less than endAddress
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        public bool Scan<TScanFunctions>(ref TScanFunctions scanFunctions, long beginAddress, long endAddress, DiskScanBufferingMode scanBufferingMode = DiskScanBufferingMode.DoublePageBuffering)
            where TScanFunctions : IScanIteratorFunctions
            => allocatorBase.Scan(store, beginAddress, endAddress, ref scanFunctions, scanBufferingMode);

        /// <summary>
        /// Iterate versions of the specified key, starting with most recent
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        public bool IterateKeyVersions<TScanFunctions>(ref TScanFunctions scanFunctions, ReadOnlySpan<byte> key, ReadOnlySpan<byte> namespaceBytes)
            where TScanFunctions : IScanIteratorFunctions
            => allocatorBase.IterateKeyVersions(store, key, namespaceBytes, ref scanFunctions);

        /// <summary>
        /// Flush log until current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        public void Flush(bool wait) => ShiftReadOnlyAddress(allocatorBase.GetTailAddress(), wait);

        /// <summary>
        /// Flush log and evict all records from memory
        /// </summary>
        /// <param name="wait">Wait for operation to complete</param>
        public void FlushAndEvict(bool wait) => ShiftHeadAddress(allocatorBase.GetTailAddress(), wait);

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<TInput, TOutput, TContext>(long untilAddress, CompactionType compactionType)
            => Compact<TInput, TOutput, TContext, DefaultCompactionFunctions>(default, untilAddress, compactionType);

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="cf">User provided compaction functions (see <see cref="ICompactionFunctions"/>)</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<TInput, TOutput, TContext, TCompactionFunctions>(TCompactionFunctions cf, long untilAddress, CompactionType compactionType)
            where TCompactionFunctions : ICompactionFunctions
            => store.Compact<TInput, TOutput, TContext, TCompactionFunctions>(cf, untilAddress, compactionType);
    }
}