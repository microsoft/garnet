// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// Memory allocator for objects
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class MallocFixedPageSize<T> : IDisposable
    {
        // We will never get an index of 0 from (Bulk)Allocate
        private const long InvalidAllocationIndex = 0;

        private const int PageSizeBits = 16;
        private const int PageSize = 1 << PageSizeBits;
        private const int PageSizeMask = PageSize - 1;
        private const int LevelSizeBits = 12;
        private const int LevelSize = 1 << LevelSizeBits;

        /// <summary>Smallest legal level count: level 0 is allocated in the constructor and every level-0 allocation
        /// pre-allocates level 1.</summary>
        internal const int MinLevelCount = 2;

        /// <summary>Largest legal level count. <c>count</c> and the allocation index are <see cref="int"/>, so the page
        /// table cannot address more than <see cref="int.MaxValue"/> records.</summary>
        internal const int MaxLevelCount = int.MaxValue / PageSize;

        /// <summary>Number of levels (pages) this instance can address, derived from the configured memory budget.</summary>
        private readonly int levelCount;

        /// <summary>Pages actually allocated, which is what this allocator costs in memory. Pages are committed ahead of
        /// the records handed out: level 0 at construction and every page pre-allocates its successor.</summary>
        private int allocatedPageCount;

        /// <summary>Ceiling an ordinary allocation may take <c>count</c> to. Equal to <see cref="MaxAllocationCount"/>
        /// unless <see cref="Reserve"/> is holding capacity, which only a reserved allocation may take.</summary>
        private long ordinaryAllocationLimit;

        private readonly T[][] values;
        private readonly IntPtr[] pointers;

        /// <summary>Maximum number of records this allocator can hand out, after which <see cref="Allocate"/> and
        /// <see cref="BulkAllocate"/> throw.</summary>
        internal long MaxAllocationCount => (long)levelCount * PageSize;

        /// <summary>Bytes of records the page table can address, which is the memory budget this instance was built for
        /// rounded down to a whole number of pages.</summary>
        internal long MaxMemorySize => MaxAllocationCount * RecordSize;

        /// <summary>Granularity of a memory budget: a budget is rounded down to a whole number of pages of this size.</summary>
        internal static long MemorySizeGranularity => (long)PageSize * RecordSize;

        /// <summary>Bytes this allocator has actually committed. Pages are allocated whole and ahead of the records
        /// handed out, so this exceeds the size implied by the allocation count and is what the process pays.</summary>
        public long AllocatedSizeBytes => (long)Volatile.Read(ref allocatedPageCount) * PageSize * RecordSize;

        /// <summary>Budget used when none is configured: 16 GiB for 64-byte records.</summary>
        internal static long DefaultMaxMemorySize => (long)LevelSize * PageSize * RecordSize;

        /// <summary>Smallest and largest budgets a level count can express, for validation messages.</summary>
        internal static long MinMemorySize => MinLevelCount * MemorySizeGranularity;

        /// <inheritdoc cref="MinMemorySize"/>
        internal static long MaxMemorySizeLimit => MaxLevelCount * MemorySizeGranularity;

        /// <summary>
        /// Convert a memory budget in bytes to the number of page-table levels that addresses, rounding down to a whole
        /// number of pages and clamping to the range the page table can express.
        /// </summary>
        /// <param name="maxMemorySize">Budget in bytes. Zero or negative selects <see cref="DefaultMaxMemorySize"/>.</param>
        internal static int GetLevelCount(long maxMemorySize)
        {
            if (maxMemorySize <= 0)
                maxMemorySize = DefaultMaxMemorySize;
            var levels = maxMemorySize / MemorySizeGranularity;
            if (levels < MinLevelCount)
                return MinLevelCount;
            return levels > MaxLevelCount ? MaxLevelCount : (int)levels;
        }

        /// <summary>
        /// Smallest budget that can hand out <paramref name="recordCount"/> records: their size rounded up to a whole
        /// page, since <see cref="GetLevelCount"/> rounds a budget down. Not clamped, so a caller can tell a requirement
        /// beyond <see cref="MaxMemorySizeLimit"/> from one that fits.
        /// </summary>
        internal static long GetMemorySizeForRecords(long recordCount)
            => (recordCount * RecordSize + MemorySizeGranularity - 1) / MemorySizeGranularity * MemorySizeGranularity;

        private volatile int writeCacheLevel;

        private volatile int count;

        internal static int RecordSize => Unsafe.SizeOf<T>();
        internal static bool IsBlittable => Utility.IsBlittable<T>();

        private int checkpointCallbackCount;
        private IoFailure checkpointError;
        private TaskCompletionSource<bool> checkpointTcs;

        private readonly ConcurrentQueue<long> freeList;

        readonly ILogger logger;

#if DEBUG
        private enum AllocationMode { None, Single, Bulk };
        private AllocationMode allocationMode;
#endif

        // This is used only for Flush and Restore, not random reads, so 512 is fine.
        const int SectorSize = 512;

        private int initialAllocation = 0;

        /// <summary>Number of allocations performed</summary>
        public int NumAllocations => count - initialAllocation; // Ignores the initial allocation

        /// <summary>
        /// Create new instance with the default memory budget of <see cref="DefaultMaxMemorySize"/>.
        /// </summary>
        public MallocFixedPageSize(ILogger logger = null) : this(LevelSize, logger) { }

        /// <summary>
        /// Create new instance sized for a memory budget in bytes, rounded down to a whole number of pages and clamped
        /// to the range the page table can express.
        /// </summary>
        /// <param name="maxMemorySize">Budget in bytes. Zero or negative selects <see cref="DefaultMaxMemorySize"/>.</param>
        /// <param name="logger">Logger</param>
        public MallocFixedPageSize(long maxMemorySize, ILogger logger = null) : this(GetLevelCount(maxMemorySize), logger) { }

        /// <summary>
        /// Create new instance with an explicit level count.
        /// </summary>
        internal unsafe MallocFixedPageSize(int levelCount, ILogger logger = null)
        {
            // Level 0 is allocated below and every level-0 allocation pre-allocates level 1, so two levels are the minimum.
            Debug.Assert(levelCount >= MinLevelCount, "levelCount must be at least MinLevelCount");
            Debug.Assert(levelCount <= MaxLevelCount, "levelCount must be at most MaxLevelCount");

            this.levelCount = levelCount;
            ordinaryAllocationLimit = MaxAllocationCount;
            values = new T[levelCount][];
            pointers = new IntPtr[levelCount];

            this.logger = logger;
            freeList = new ConcurrentQueue<long>();

            values[0] = GC.AllocateArray<T>(PageSize + SectorSize, pinned: IsBlittable);
            _ = Interlocked.Increment(ref allocatedPageCount);
            if (IsBlittable)
            {
                pointers[0] = (IntPtr)(((long)Unsafe.AsPointer(ref values[0][0]) + (SectorSize - 1)) & ~(SectorSize - 1));
            }

            writeCacheLevel = -1;
            Interlocked.MemoryBarrier();

            // Allocate one block so we never return a null pointer; this allocation is never freed.
            // Use BulkAllocate so the caller can still do either BulkAllocate or single Allocate().
            BulkAllocate();
            initialAllocation = AllocateChunkSize;
#if DEBUG
            // Clear this for the next allocation.
            allocationMode = AllocationMode.None;
#endif
        }

        /// <summary>
        /// Get physical address -- for blittable objects only
        /// </summary>
        /// <param name="logicalAddress">The logicalAddress of the allocation. For BulkAllocate, this may be an address within the chunk size, to reference that particular record.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPhysicalAddress(long logicalAddress)
        {
            Debug.Assert(IsBlittable, "GetPhysicalAddress requires the values to be blittable");
            return (long)pointers[logicalAddress >> PageSizeBits] + (logicalAddress & PageSizeMask) * RecordSize;
        }

        /// <summary>
        /// Get object
        /// </summary>
        /// <param name="index">The index of the allocation. For BulkAllocate, this may be a value within the chunk size, to reference that particular record.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe ref T Get(long index)
        {
            Debug.Assert(index != InvalidAllocationIndex, "Invalid allocation index");
            if (IsBlittable)
                return ref Unsafe.AsRef<T>((byte*)(pointers[index >> PageSizeBits]) + (index & PageSizeMask) * RecordSize);
            else
                return ref values[index >> PageSizeBits][index & PageSizeMask];
        }

        /// <summary>
        /// Set object
        /// </summary>
        /// <param name="index"></param>
        /// <param name="value"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Set(long index, ref T value)
        {
            Debug.Assert(index != InvalidAllocationIndex, "Invalid allocation index");
            if (IsBlittable)
                Unsafe.AsRef<T>((byte*)(pointers[index >> PageSizeBits]) + (index & PageSizeMask) * RecordSize) = value;
            else
                values[index >> PageSizeBits][index & PageSizeMask] = value;
        }

        /// <summary>
        /// Free object
        /// </summary>
        /// <param name="pointer"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Free(long pointer)
        {
            freeList.Enqueue(pointer);
        }

        internal int FreeListCount => freeList.Count;   // For test

        /// <summary>
        /// Hold <paramref name="recordCount"/> records at the top of the capacity for allocations that pass
        /// <c>useReservation</c>. Ordinary allocations fail once they would encroach on the reserve, so a caller that
        /// must be able to allocate a known quantity cannot be starved by concurrent ordinary allocations.
        /// </summary>
        internal void Reserve(long recordCount)
        {
            var allocated = count;
            if (recordCount > MaxAllocationCount - allocated)
                ThrowReservationTooLarge(recordCount, allocated);
            _ = Interlocked.Exchange(ref ordinaryAllocationLimit, MaxAllocationCount - recordCount);
        }

        /// <summary>Release a reservation made by <see cref="Reserve"/>, returning its capacity to ordinary allocations.</summary>
        internal void ReleaseReservation() => Interlocked.Exchange(ref ordinaryAllocationLimit, MaxAllocationCount);

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ThrowReservationTooLarge(long recordCount, long allocated)
            => throw new TsavoriteException(
                $"{nameof(MallocFixedPageSize<T>)}<{typeof(T).Name}> cannot reserve {recordCount} records:"
                + $" {MaxAllocationCount - allocated} of its {MaxAllocationCount} remain unallocated.{ExhaustionRemedy}");

        public const int AllocateChunkSize = 16;

        /// <summary>
        /// Allocate a block of size RecordSize * kAllocateChunkSize. 
        /// </summary>
        /// <remarks>Warning: cannot mix 'n' match use of Allocate and BulkAllocate because there is no header indicating record size, so 
        /// the freeList does not distinguish them.</remarks>
        /// <returns>The logicalAddress (index) of the block</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe long BulkAllocate()
        {
#if DEBUG
            Debug.Assert(allocationMode != AllocationMode.Single, "Cannot mix Single and Bulk allocation modes");
            allocationMode = AllocationMode.Bulk;
#endif
            return InternalAllocate(AllocateChunkSize, useReservation: false);
        }

        /// <summary>
        /// Allocate a block of size RecordSize.
        /// </summary>
        /// <param name="useReservation">Draw on capacity held by <see cref="Reserve"/>, for a caller that reserved it.</param>
        /// <returns>The logicalAddress (index) of the block</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe long Allocate(bool useReservation = false)
        {
#if DEBUG
            Debug.Assert(allocationMode != AllocationMode.Bulk, "Cannot mix Single and Bulk allocation modes");
            allocationMode = AllocationMode.Single;
#endif
            return InternalAllocate(1, useReservation);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ThrowAllocatorFull(long limit)
            => throw new TsavoriteException(
                $"{nameof(MallocFixedPageSize<T>)}<{typeof(T).Name}> is full: its page table addresses at most {levelCount} pages of {PageSize} records"
                + $" ({MaxAllocationCount} records, {MaxMemorySize} bytes), and that capacity is exhausted."
                + (limit < MaxAllocationCount ? $" {MaxAllocationCount - limit} records of it are reserved for an index grow in progress." : string.Empty)
                + ExhaustionRemedy);

        /// <summary>Remedy appended to the exhaustion message, naming the settings that size this allocator.</summary>
        private static readonly string ExhaustionRemedy = typeof(T) != typeof(HashBucket)
            ? string.Empty
            : " Too many hash entries have spilled out of the main bucket array. Overflow buckets chain linearly and are"
                + " scanned by reads and upserts, so the index is undersized for the number of distinct keys on this node."
                + " Raise IndexMemorySize, or set IndexMaxMemorySize to let the index grow, so that more entries fit the"
                + " main bucket array. Raising IndexOverflowThreshold buys capacity at the cost of longer chains.";

        private unsafe long InternalAllocate(int blockSize, bool useReservation)
        {
            if (freeList.TryDequeue(out long result))
                return result;

            // An ordinary allocation may not encroach on capacity held by Reserve. useReservation is constant at each
            // call site, so this folds away once inlined.
            var limit = useReservation ? MaxAllocationCount : Volatile.Read(ref ordinaryAllocationLimit);

            // Determine insertion index.
            int index = Interlocked.Add(ref count, blockSize) - blockSize;
            if (index + (long)blockSize > limit)
            {
                // Undo the advance before throwing: repeated rejections would otherwise run count away and overflow int.
                _ = Interlocked.Add(ref count, -blockSize);
                ThrowAllocatorFull(limit);
            }

            int offset = index & PageSizeMask;
            int baseAddr = index >> PageSizeBits;

            // Handle indexes in first batch specially because they do not use write cache.
            if (baseAddr == 0)
            {
                // If index 0, then allocate space for next level.
                if (index == 0)
                {
                    var tmp = GC.AllocateArray<T>(PageSize + SectorSize, pinned: IsBlittable);
                    if (IsBlittable)
                        pointers[1] = (IntPtr)(((long)Unsafe.AsPointer(ref tmp[0]) + (SectorSize - 1)) & ~(SectorSize - 1));

                    values[1] = tmp;
                    _ = Interlocked.Increment(ref allocatedPageCount);
                    Interlocked.MemoryBarrier();
                }

                // Return location.
                return index;
            }

            // See if write cache contains corresponding array.
            var cache = writeCacheLevel;
            T[] array;

            if (cache != -1)
            {
                // Write cache is correct array only if index is within [arrayCapacity, 2*arrayCapacity).
                if (cache == baseAddr)
                {
                    // Return location.
                    return index;
                }
            }

            // Spin-wait until level has an allocated array.
            var spinner = new SpinWait();
            while (true)
            {
                array = values[baseAddr];
                if (array != null)
                {
                    break;
                }
                spinner.SpinOnce();
            }

            // Perform extra actions if inserting at offset 0 of level.
            if (offset == 0)
            {
                // Update write cache to point to current level.
                writeCacheLevel = baseAddr;
                Interlocked.MemoryBarrier();

                // Allocate for next page
                int newBaseAddr = baseAddr + 1;

                // The last level has no successor to pre-allocate; the next allocation past it throws in ThrowAllocatorFull.
                if (newBaseAddr < levelCount)
                {
                    var tmp = GC.AllocateArray<T>(PageSize + SectorSize, pinned: IsBlittable);
                    if (IsBlittable)
                        pointers[newBaseAddr] = (IntPtr)(((long)Unsafe.AsPointer(ref tmp[0]) + (SectorSize - 1)) & ~(SectorSize - 1));

                    values[newBaseAddr] = tmp;
                    _ = Interlocked.Increment(ref allocatedPageCount);
                }

                Interlocked.MemoryBarrier();
            }

            // Return location.
            return index;
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose() => count = 0;


        #region Checkpoint

        /// <summary>
        /// Is checkpoint completed
        /// </summary>
        /// <returns></returns>
        public async ValueTask IsCheckpointCompletedAsync(CancellationToken token = default)
        {
            await checkpointTcs.Task.WaitAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Task that completes when the flush started by the most recent <see cref="BeginCheckpoint(IDevice, ulong, out ulong)"/>
        /// has finished, or <c>null</c> if no checkpoint has been started on this allocator.
        /// </summary>
        public Task GetCheckpointTask() => checkpointTcs?.Task;

        /// <summary>
        /// Public facing persistence API
        /// </summary>
        /// <param name="device"></param>
        /// <param name="offset"></param>
        /// <param name="numBytesWritten"></param>
        public void BeginCheckpoint(IDevice device, ulong offset, out ulong numBytesWritten)
            => BeginCheckpoint(device, offset, out numBytesWritten, false, default, default);

        /// <summary>
        /// Internal persistence API
        /// </summary>
        /// <param name="device"></param>
        /// <param name="offset"></param>
        /// <param name="numBytesWritten"></param>
        /// <param name="useReadCache"></param>
        /// <param name="skipReadCache"></param>
        /// <param name="epoch"></param>
        internal unsafe void BeginCheckpoint(IDevice device, ulong offset, out ulong numBytesWritten, bool useReadCache, SkipReadCache skipReadCache, LightEpoch epoch)
        {
            int localCount = count;
            int recordsCountInLastLevel = localCount & PageSizeMask;
            int numCompleteLevels = localCount >> PageSizeBits;
            int numLevels = numCompleteLevels + (recordsCountInLastLevel > 0 ? 1 : 0);

            // Count an issuance sentinel alongside the levels, retired in the finally below once issuance has ended
            // and the catch has recorded any exception. A device may invoke a completion callback synchronously and
            // then throw out of the same submit; without the sentinel that completion can drive the count to zero and
            // report the checkpoint successful before the failure is recorded.
            checkpointCallbackCount = numLevels + 1;
            checkpointError = null;
            checkpointTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            uint alignedPageSize = PageSize * (uint)RecordSize;
            uint lastLevelSize = (uint)recordsCountInLastLevel * (uint)RecordSize;

            int sectorSize = (int)device.SectorSize;
            numBytesWritten = 0;
            int i = 0;

            // Levels whose retirement is accounted for: the device accepted the write and its completion callback will
            // retire the level, or the submit failed and the catch retired it in place. Levels past this point were
            // counted but never handed to the device.
            var accountedLevels = 0;
            try
            {
                for (; i < numLevels; i++)
                {
                    OverflowPagesFlushAsyncResult result = default;
                    result.levelIndex = i;
                    result.retirementGuard = new(0);

                    uint writeSize = (uint)((i == numCompleteLevels) ? (lastLevelSize + (sectorSize - 1)) & ~(sectorSize - 1) : alignedPageSize);
                    result.numBytesToWrite = writeSize;

                    try
                    {
                        if (!useReadCache)
                        {
                            device.WriteAsync(pointers[i], offset + numBytesWritten, writeSize, AsyncFlushCallback, result);
                        }
                        else
                        {
                            result.mem = new SectorAlignedMemory((int)writeSize, (int)device.SectorSize);
                            bool prot = false;
                            if (!epoch.ThisInstanceProtected())
                            {
                                prot = true;
                                epoch.Resume();
                            }

                            try
                            {
                                Buffer.MemoryCopy((void*)pointers[i], result.mem.aligned_pointer, writeSize, writeSize);
                                int j = 0;
                                if (i == 0) j += AllocateChunkSize * RecordSize;
                                for (; j < writeSize; j += sizeof(HashBucket))
                                {
                                    skipReadCache((HashBucket*)(result.mem.aligned_pointer + j));
                                }
                            }
                            finally
                            {
                                // Staging can throw, and this thread may be a pooled one. Leaving it epoch-protected
                                // would pin the safe-to-reclaim boundary for the life of the process.
                                if (prot) epoch.Suspend();
                            }

                            device.WriteAsync((IntPtr)result.mem.aligned_pointer, offset + numBytesWritten, writeSize, AsyncFlushCallback, result);
                        }
                    }
                    catch (Exception ex)
                    {
                        // A device may invoke the completion callback synchronously and then throw back out of the
                        // submit, so this level may already have been retired and its buffer already released. Claim
                        // exactly once: retiring it twice would complete the checkpoint while earlier writes are still
                        // reading from the buffers they were given. Record the error before retiring, so a retirement
                        // that completes the checkpoint reports the failure.
                        RecordCheckpointError($"level {i} could not be issued", ex);
                        if (result.TryClaimRetirement())
                        {
                            result.mem?.Dispose();
                            RetireCheckpointLevel();
                        }
                        accountedLevels++;
                        throw;
                    }
                    accountedLevels++;
                    numBytesWritten += writeSize;
                }
            }
            catch (Exception ex)
            {
                // The levels already issued will still complete, but the ones counted after the failure have no
                // callback to retire them. Without this the outstanding count never reaches zero and every waiter on
                // the checkpoint task blocks forever. Staging a level can throw before its submit is attempted, so
                // this counts from the levels actually accounted for rather than assuming level i was retired.
                RecordCheckpointError($"level {i} could not be issued", ex);
                for (var unaccounted = accountedLevels; unaccounted < numLevels; unaccounted++)
                    RetireCheckpointLevel();
                throw;
            }
            finally
            {
                // Retire the issuance sentinel, now that issuance has ended and the catch above has recorded any
                // exception it hit. This is what completes the checkpoint, so it completes with that error rather
                // than with a success a synchronous completion reached before the submit threw.
                RetireCheckpointLevel();
            }
        }

        /// <summary>Retire one outstanding level from the checkpoint, completing the checkpoint task when the last one
        /// is retired.</summary>
        private void RetireCheckpointLevel()
        {
            if (Interlocked.Decrement(ref checkpointCallbackCount) == 0)
            {
                var error = checkpointError;
                if (error is not null)
                    checkpointTcs.TrySetException(error.ToException("Overflow-bucket checkpoint flush failed"));
                else
                    checkpointTcs.TrySetResult(true);
            }
        }

        private unsafe void AsyncFlushCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            var result = (OverflowPagesFlushAsyncResult)context;
            if (errorCode != 0)
            {
                if (ioException is null)
                    logger?.LogError($"{nameof(AsyncFlushCallback)} error: {{errorCode}}", errorCode);
                else
                    logger?.LogError($"{nameof(AsyncFlushCallback)} error: {{exception}}", Utility.GetCallbackExceptionDetail(ioException));
                RecordCheckpointError($"level {result.levelIndex} failed with error code {errorCode}");
            }
            else if (numBytes != 0 && numBytes < result.numBytesToWrite)
            {
                // A transferred count below the requested length means the level is not fully on disk. A count of
                // zero means the device does not report one (see DeviceIOCompletionCallback), not a short write.
                logger?.LogError($"{nameof(AsyncFlushCallback)} error: wrote {{numBytes}} of {{numBytesToWrite}} bytes", numBytes, result.numBytesToWrite);
                RecordCheckpointError($"level {result.levelIndex} wrote {numBytes} of {result.numBytesToWrite} bytes");
            }

            if (!result.TryClaimRetirement())
                return;

            result.mem?.Dispose();
            RetireCheckpointLevel();
        }

        /// <summary>Record the first failure seen while flushing the overflow buckets, so the checkpoint fails with
        /// the error that occurred first; subsequent failures are logged but do not overwrite it.</summary>
        private void RecordCheckpointError(string detail, Exception exception = null)
            => _ = Interlocked.CompareExchange(ref checkpointError, new IoFailure(detail, exception), null);

        /// <summary>
        /// Max valid address: the high-water mark of records handed out.
        /// </summary>
        /// <remarks>Clamped to <see cref="MaxAllocationCount"/>. A rejected allocation advances <c>count</c> before
        /// rolling it back, and a reader in that window must not derive a page-table index past the last level.</remarks>
        public int GetMaxValidAddress()
        {
            var current = count;
            return current <= MaxAllocationCount ? current : (int)MaxAllocationCount;
        }

        /// <summary>
        /// Get page size
        /// </summary>
        /// <returns></returns>
        public int GetPageSize() => PageSize;
        #endregion

        #region Recover
        /// <summary>
        /// Recover
        /// </summary>
        /// <param name="device"></param>
        /// <param name="buckets"></param>
        /// <param name="numBytes"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="offset"></param>
        public async ValueTask<ulong> RecoverAsync(IDevice device, ulong offset, int buckets, ulong numBytes, CancellationToken cancellationToken)
        {
            BeginRecovery(device, offset, buckets, numBytes, out ulong numBytesRead, isAsync: true);
            await recoveryCountdown.WaitAsync(cancellationToken).ConfigureAwait(false);
            var error = recoveryError;
            if (error is not null)
                throw error.ToException("Overflow-bucket recovery failed");
            return numBytesRead;
        }

        // Implementation of asynchronous recovery
        private CountdownWrapper recoveryCountdown;
        private IoFailure recoveryError;

        internal unsafe void BeginRecovery(IDevice device,
                                    ulong offset,
                                    int buckets,
                                    ulong numBytesToRead,
                                    out ulong numBytesRead,
                                    bool isAsync = false)
        {
            // Drop any state left by an earlier recovery first, so that a failure in the allocation below cannot
            // leave a caller draining against the previous recovery's countdown.
            recoveryCountdown = null;
            recoveryError = null;

            // Allocate as many records in memory
            while (count < buckets)
            {
                Allocate();
            }

            // Derive the level layout from the byte count the checkpoint wrote rather than from a record count.
            // The checkpoint rounds its final level up to a sector, so the persisted size need not be a multiple of
            // RecordSize; round-tripping through records would drop that padding and issue a read that is both short
            // and unaligned, which the O_DIRECT device paths reject outright.
            uint alignedPageSize = (uint)PageSize * (uint)RecordSize;
            int numCompleteLevels = (int)(numBytesToRead / alignedPageSize);
            uint lastLevelSize = (uint)(numBytesToRead - ((ulong)numCompleteLevels * alignedPageSize));
            int numLevels = numCompleteLevels + (lastLevelSize > 0 ? 1 : 0);

            recoveryCountdown = new CountdownWrapper(numLevels, isAsync);

            numBytesRead = 0;
            int i = 0;
            try
            {
                for (; i < numLevels; i++)
                {
                    // Read exactly what the checkpoint wrote for this level: the final level is shorter than a page and
                    // is the end of the checkpoint region, so requesting a full page would read past the end of the file.
                    uint length = (i == numCompleteLevels) ? lastLevelSize : alignedPageSize;
                    OverflowPagesReadAsyncResult result = default;
                    result.levelIndex = i;
                    result.numBytesToRead = length;
                    result.retirementGuard = new(0);
                    try
                    {
                        device.ReadAsync(offset + numBytesRead, pointers[i], length, AsyncPageReadCallback, result);
                    }
                    catch
                    {
                        // A device may invoke the completion callback synchronously and then throw back out of the
                        // submit, so this level may already have been retired. Claim exactly once: retiring it twice
                        // would let the countdown reach zero while earlier reads are still writing into the pages,
                        // and the caller would close the device out from under them.
                        if (result.TryClaimRetirement())
                            recoveryCountdown.Decrement();
                        throw;
                    }
                    numBytesRead += length;
                }
                Debug.Assert(numBytesRead == numBytesToRead);
            }
            catch
            {
                // The levels already issued will still complete and touch the device, so the countdown must reach
                // zero for the caller to know when it is safe to dispose. Retire the levels never issued; the one
                // that failed to issue was retired above.
                for (i++; i < numLevels; i++)
                    recoveryCountdown.Decrement();
                throw;
            }
        }

        /// <summary>Wait for every overflow-bucket read that was issued to complete, ignoring whether it succeeded.
        /// Used on failure paths before the caller disposes the device these reads are still using.</summary>
        internal ValueTask DrainRecoveryAsync()
            => recoveryCountdown is null ? default : recoveryCountdown.DrainAsync();

        private unsafe void AsyncPageReadCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            var result = (OverflowPagesReadAsyncResult)context;
            if (errorCode != 0)
            {
                if (ioException is null)
                    logger?.LogError($"{nameof(AsyncPageReadCallback)} error: {{errorCode}}", errorCode);
                else
                    logger?.LogError($"{nameof(AsyncPageReadCallback)} error: {{exception}}", Utility.GetCallbackExceptionDetail(ioException));
                RecordRecoveryError($"level {result.levelIndex} failed with error code {errorCode}", ioException);
            }
            else if (numBytes != 0 && numBytes < result.numBytesToRead)
            {
                // A transferred count below the requested length means part of the level still holds its pre-read
                // contents, so fail recovery rather than bring up overflow buckets that are missing entries. A count
                // of zero means the device does not report one (see DeviceIOCompletionCallback), not a short read.
                logger?.LogError($"{nameof(AsyncPageReadCallback)} error: read {{numBytes}} of {{numBytesToRead}} bytes", numBytes, result.numBytesToRead);
                RecordRecoveryError($"level {result.levelIndex} read {numBytes} of {result.numBytesToRead} bytes");
            }
            if (result.TryClaimRetirement())
                recoveryCountdown.Decrement();
        }

        /// <summary>Record the first failure seen while reading the overflow buckets, so recovery fails with the error
        /// that occurred first; subsequent failures are logged but do not overwrite it.</summary>
        private void RecordRecoveryError(string detail, Exception exception = null)
            => _ = Interlocked.CompareExchange(ref recoveryError, new IoFailure(detail, exception), null);
        #endregion
    }
}