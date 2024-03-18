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
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public sealed class LogAccessor<Key, Value> : IObservable<ITsavoriteScanIterator<Key, Value>>
    {
        private readonly TsavoriteKV<Key, Value> store;
        private readonly AllocatorBase<Key, Value> allocator;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="store"></param>
        /// <param name="allocator"></param>
        internal LogAccessor(TsavoriteKV<Key, Value> store, AllocatorBase<Key, Value> allocator)
        {
            this.store = store;
            this.allocator = allocator;
        }

        /// <summary>
        /// Tail address of log
        /// </summary>
        public long TailAddress => allocator.GetTailAddress();

        /// <summary>
        /// Read-only address of log, i.e. boundary between read-only region and mutable region
        /// </summary>
        public long ReadOnlyAddress => allocator.ReadOnlyAddress;

        /// <summary>
        /// Safe read-only address of log, i.e. boundary between read-only region and mutable region
        /// </summary>
        public long SafeReadOnlyAddress => allocator.SafeReadOnlyAddress;

        /// <summary>
        /// Head address of log, i.e. beginning of in-memory regions
        /// </summary>
        public long HeadAddress => allocator.HeadAddress;

        /// <summary>
        /// Beginning address of log
        /// </summary>
        public long BeginAddress => allocator.BeginAddress;

        /// <summary>
        /// Get the bytes used on the primary log by every record. Does not include
        /// the size of variable-length inline data. Note that class objects occupy
        /// 8 bytes (reference) on the main log (i.e., the heap space occupied by
        /// class objects is not included in the result of this call).
        /// </summary>
        public int FixedRecordSize => allocator.GetFixedRecordSize();

        /// <summary>
        /// Number of pages left empty or unallocated in the in-memory buffer (between 0 and BufferSize-1)
        /// </summary>
        public int EmptyPageCount
        {
            get => allocator.EmptyPageCount;
            set { allocator.EmptyPageCount = value; }
        }

        /// <summary>
        /// Maximum possible number of empty pages in Allocator
        /// </summary>
        public int MaxEmptyPageCount => allocator.MaxEmptyPageCount;

        /// <summary>
        /// Minimum possible number of empty pages in Allocator
        /// </summary>
        public int MinEmptyPageCount => allocator.MinEmptyPageCount;

        /// <summary>
        /// Set empty page count in allocator
        /// </summary>
        /// <param name="pageCount">New empty page count</param>
        /// <param name="wait">Whether to wait for shift addresses to complete</param>
        public void SetEmptyPageCount(int pageCount, bool wait = false)
        {
            allocator.EmptyPageCount = pageCount;
            if (wait)
            {
                long newHeadAddress = (allocator.GetTailAddress() & ~allocator.PageSizeMask) - allocator.HeadOffsetLagAddress;
                ShiftHeadAddress(newHeadAddress, wait);
            }
        }

        /// <summary>
        /// Total in-memory circular buffer capacity (in number of pages)
        /// </summary>
        public int BufferSize => allocator.BufferSize;

        /// <summary>
        /// Actual memory used by log (not including heap objects) and overflow pages
        /// </summary>
        public long MemorySizeBytes => ((long)(allocator.AllocatedPageCount + allocator.OverflowPageCount)) << allocator.LogPageSizeBits;

        /// <summary>
        /// Number of pages allocated
        /// </summary>
        public int AllocatedPageCount => allocator.AllocatedPageCount;

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
                untilAddress &= ~allocator.PageSizeMask;

            bool epochProtected = store.epoch.ThisInstanceProtected();
            try
            {
                if (!epochProtected)
                    store.epoch.Resume();
                allocator.ShiftBeginAddress(untilAddress, truncateLog);
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
                    allocator.ShiftHeadAddress(newHeadAddress);
                }
                finally
                {
                    store.epoch.Suspend();
                }

                while (wait && allocator.SafeHeadAddress < newHeadAddress) Thread.Yield();
            }
            else
            {
                allocator.ShiftHeadAddress(newHeadAddress);
                while (wait && allocator.SafeHeadAddress < newHeadAddress)
                    store.epoch.ProtectAndDrain();
            }
        }

        /// <summary>
        /// Subscribe to records (in batches) as they become read-only in the log
        /// Currently, we support only one subscriber to the log (easy to extend)
        /// Subscriber only receives new log updates from the time of subscription onwards
        /// To scan the historical part of the log, use the Scan(...) method
        /// </summary>
        /// <param name="readOnlyObserver">Observer to which scan iterator is pushed</param>
        public IDisposable Subscribe(IObserver<ITsavoriteScanIterator<Key, Value>> readOnlyObserver)
        {
            allocator.OnReadOnlyObserver = readOnlyObserver;
            return new LogSubscribeDisposable(allocator, true);
        }

        /// <summary>
        /// Subscribe to records (in batches) as they get evicted from main memory.
        /// Currently, we support only one subscriber to the log (easy to extend)
        /// Subscriber only receives eviction updates from the time of subscription onwards
        /// To scan the historical part of the log, use the Scan(...) method
        /// </summary>
        /// <param name="evictionObserver">Observer to which scan iterator is pushed</param>
        public IDisposable SubscribeEvictions(IObserver<ITsavoriteScanIterator<Key, Value>> evictionObserver)
        {
            allocator.OnEvictionObserver = evictionObserver;
            return new LogSubscribeDisposable(allocator, false);
        }

        /// <summary>
        /// Wrapper to help dispose the subscription
        /// </summary>
        class LogSubscribeDisposable : IDisposable
        {
            private readonly AllocatorBase<Key, Value> allocator;
            private readonly bool readOnly;

            public LogSubscribeDisposable(AllocatorBase<Key, Value> allocator, bool readOnly)
            {
                this.allocator = allocator;
                this.readOnly = readOnly;
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
                    allocator.ShiftReadOnlyAddress(newReadOnlyAddress);
                }
                finally
                {
                    store.epoch.Suspend();
                }

                // Wait for flush to complete
                while (wait && allocator.FlushedUntilAddress < newReadOnlyAddress) Thread.Yield();
            }
            else
            {
                allocator.ShiftReadOnlyAddress(newReadOnlyAddress);

                // Wait for flush to complete
                while (wait && allocator.FlushedUntilAddress < newReadOnlyAddress)
                    store.epoch.ProtectAndDrain();
            }
        }

        /// <summary>
        /// Pull-scan the log given address range; returns all records with address less than endAddress
        /// </summary>
        /// <returns>Scan iterator instance</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ITsavoriteScanIterator<Key, Value> Scan(long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
            => allocator.Scan(store: null, beginAddress, endAddress, scanBufferingMode);

        /// <summary>
        /// Push-scan the log given address range; returns all records with address less than endAddress
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        public bool Scan<TScanFunctions>(ref TScanFunctions scanFunctions, long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
            => allocator.Scan(store, beginAddress, endAddress, ref scanFunctions, scanBufferingMode);

        /// <summary>
        /// Iterate versions of the specified key, starting with most recent
        /// </summary>
        /// <returns>True if Scan completed; false if Scan ended early due to one of the TScanIterator reader functions returning false</returns>
        public bool IterateKeyVersions<TScanFunctions>(ref TScanFunctions scanFunctions, ref Key key)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
            => allocator.IterateKeyVersions(store, ref key, ref scanFunctions);

        /// <summary>
        /// Flush log until current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        public void Flush(bool wait)
        {
            ShiftReadOnlyAddress(allocator.GetTailAddress(), wait);
        }

        /// <summary>
        /// Flush log and evict all records from memory
        /// </summary>
        /// <param name="wait">Wait for operation to complete</param>
        public void FlushAndEvict(bool wait)
        {
            ShiftHeadAddress(allocator.GetTailAddress(), wait);
        }

        /// <summary>
        /// Delete log entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        public void DisposeFromMemory()
        {
            // Ensure we have flushed and evicted
            FlushAndEvict(true);

            // Delete from memory
            allocator.DeleteFromMemory();
        }

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during compaction</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<Input, Output, Context, Functions>(Functions functions, long untilAddress, CompactionType compactionType)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            => Compact<Input, Output, Context, Functions, DefaultCompactionFunctions<Key, Value>>(functions, default, untilAddress, compactionType);

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during compaction</param>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<Input, Output, Context, Functions>(Functions functions, ref Input input, ref Output output, long untilAddress, CompactionType compactionType)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            => Compact<Input, Output, Context, Functions, DefaultCompactionFunctions<Key, Value>>(functions, default, ref input, ref output, untilAddress, compactionType);

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during compaction</param>
        /// <param name="cf">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>)</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<Input, Output, Context, Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, long untilAddress, CompactionType compactionType)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            Input input = default;
            Output output = default;
            return Compact<Input, Output, Context, Functions, CompactionFunctions>(functions, cf, ref input, ref output, untilAddress, compactionType);
        }

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during compaction</param>
        /// <param name="cf">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>)</param>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<Input, Output, Context, Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, ref Input input, ref Output output, long untilAddress, CompactionType compactionType)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
            => store.Compact<Input, Output, Context, Functions, CompactionFunctions>(functions, cf, ref input, ref output, untilAddress, compactionType);
    }
}