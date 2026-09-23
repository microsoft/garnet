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

        private readonly T[][] values = new T[LevelSize][];
        private readonly IntPtr[] pointers = new IntPtr[LevelSize];

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
        /// Create new instance
        /// </summary>
        public unsafe MallocFixedPageSize(ILogger logger = null)
        {
            this.logger = logger;
            freeList = new ConcurrentQueue<long>();

            values[0] = GC.AllocateArray<T>(PageSize + SectorSize, pinned: IsBlittable);
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

        internal const int AllocateChunkSize = 16;     // internal for test

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
            return InternalAllocate(AllocateChunkSize);
        }

        /// <summary>
        /// Allocate a block of size RecordSize.
        /// </summary>
        /// <returns>The logicalAddress (index) of the block</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe long Allocate()
        {
#if DEBUG
            Debug.Assert(allocationMode != AllocationMode.Bulk, "Cannot mix Single and Bulk allocation modes");
            allocationMode = AllocationMode.Single;
#endif
            return InternalAllocate(1);
        }

        private unsafe long InternalAllocate(int blockSize)
        {
            if (freeList.TryDequeue(out long result))
                return result;

            // Determine insertion index.
            int index = Interlocked.Add(ref count, blockSize) - blockSize;
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

                var tmp = GC.AllocateArray<T>(PageSize + SectorSize, pinned: IsBlittable);
                if (IsBlittable)
                    pointers[newBaseAddr] = (IntPtr)(((long)Unsafe.AsPointer(ref tmp[0]) + (SectorSize - 1)) & ~(SectorSize - 1));

                values[newBaseAddr] = tmp;

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
            checkpointCallbackCount = numLevels;
            checkpointError = null;
            checkpointTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            uint alignedPageSize = PageSize * (uint)RecordSize;
            uint lastLevelSize = (uint)recordsCountInLastLevel * (uint)RecordSize;

            int sectorSize = (int)device.SectorSize;
            numBytesWritten = 0;
            int i = 0;
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
                    catch
                    {
                        // A device may invoke the completion callback synchronously and then throw back out of the
                        // submit, so this level may already have been retired and its buffer already released. Claim
                        // exactly once: retiring it twice would complete the checkpoint while earlier writes are still
                        // reading from the buffers they were given.
                        if (result.TryClaimRetirement())
                        {
                            result.mem?.Dispose();
                            RetireCheckpointLevel();
                        }
                        throw;
                    }
                    numBytesWritten += writeSize;
                }
            }
            catch (Exception ex)
            {
                // The levels already issued will still complete, but the ones never issued have no callback to retire
                // them. Without this the outstanding count never reaches zero and every waiter on the checkpoint task
                // blocks forever.
                RecordCheckpointError($"level {i} could not be issued", ex);
                for (i++; i < numLevels; i++)
                    RetireCheckpointLevel();
                throw;
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
        /// Max valid address
        /// </summary>
        /// <returns></returns>
        public int GetMaxValidAddress() => count;

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