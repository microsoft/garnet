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
        private SemaphoreSlim checkpointSemaphore;

        private readonly ConcurrentQueue<long> freeList;

        readonly ILogger logger;

#if DEBUG
        private enum AllocationMode { None, Single, Bulk };
        private AllocationMode allocationMode;
#endif

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
        /// Is checkpoint complete
        /// </summary>
        /// <returns></returns>
        public bool IsCheckpointCompleted() => checkpointCallbackCount == 0;

        /// <summary>
        /// Is checkpoint completed
        /// </summary>
        /// <returns></returns>
        public async ValueTask IsCheckpointCompletedAsync(CancellationToken token = default)
        {
            var s = checkpointSemaphore;
            await s.WaitAsync(token).ConfigureAwait(false);
            s.Release();
        }

        public SemaphoreSlim GetCheckpointSemaphore() => checkpointSemaphore;

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
            checkpointSemaphore = new SemaphoreSlim(0);
            uint alignedPageSize = PageSize * (uint)RecordSize;
            uint lastLevelSize = (uint)recordsCountInLastLevel * (uint)RecordSize;

            int sectorSize = (int)device.SectorSize;
            numBytesWritten = 0;
            for (int i = 0; i < numLevels; i++)
            {
                OverflowPagesFlushAsyncResult result = default;

                uint writeSize = (uint)((i == numCompleteLevels) ? (lastLevelSize + (sectorSize - 1)) & ~(sectorSize - 1) : alignedPageSize);

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

                    Buffer.MemoryCopy((void*)pointers[i], result.mem.aligned_pointer, writeSize, writeSize);
                    int j = 0;
                    if (i == 0) j += AllocateChunkSize * RecordSize;
                    for (; j < writeSize; j += sizeof(HashBucket))
                    {
                        skipReadCache((HashBucket*)(result.mem.aligned_pointer + j));
                    }

                    if (prot) epoch.Suspend();

                    device.WriteAsync((IntPtr)result.mem.aligned_pointer, offset + numBytesWritten, writeSize, AsyncFlushCallback, result);
                }
                numBytesWritten += writeSize;
            }
        }

        private unsafe void AsyncFlushCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                logger?.LogError($"{nameof(AsyncFlushCallback)} error: {{errorCode}}", errorCode);
            }

            var mem = ((OverflowPagesFlushAsyncResult)context).mem;
            mem?.Dispose();

            if (Interlocked.Decrement(ref checkpointCallbackCount) == 0)
            {
                checkpointSemaphore.Release();
            }
        }

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
        /// <param name="offset"></param>
        public void Recover(IDevice device, ulong offset, int buckets, ulong numBytes)
        {
            BeginRecovery(device, offset, buckets, numBytes, out _);
        }

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
            return numBytesRead;
        }

        /// <summary>
        /// Check if recovery complete
        /// </summary>
        /// <param name="waitUntilComplete"></param>
        /// <returns></returns>
        public bool IsRecoveryCompleted(bool waitUntilComplete = false)
        {
            bool completed = recoveryCountdown.IsCompleted;
            if (!completed && waitUntilComplete)
            {
                recoveryCountdown.Wait();
                return true;
            }
            return completed;
        }

        // Implementation of asynchronous recovery
        private CountdownWrapper recoveryCountdown;

        internal unsafe void BeginRecovery(IDevice device,
                                    ulong offset,
                                    int buckets,
                                    ulong numBytesToRead,
                                    out ulong numBytesRead,
                                    bool isAsync = false)
        {
            // Allocate as many records in memory
            while (count < buckets)
            {
                Allocate();
            }

            int numRecords = (int)(numBytesToRead / (ulong)RecordSize);
            int recordsCountInLastLevel = numRecords & PageSizeMask;
            int numCompleteLevels = numRecords >> PageSizeBits;
            int numLevels = numCompleteLevels + (recordsCountInLastLevel > 0 ? 1 : 0);

            recoveryCountdown = new CountdownWrapper(numLevels, isAsync);

            numBytesRead = 0;
            uint alignedPageSize = (uint)PageSize * (uint)RecordSize;
            uint lastLevelSize = (uint)recordsCountInLastLevel * (uint)RecordSize;
            for (int i = 0; i < numLevels; i++)
            {
                //read a full page
                uint length = (uint)PageSize * (uint)RecordSize;
                OverflowPagesReadAsyncResult result = default;
                device.ReadAsync(offset + numBytesRead, pointers[i], length, AsyncPageReadCallback, result);
                numBytesRead += (i == numCompleteLevels) ? lastLevelSize : alignedPageSize;
            }
        }

        private unsafe void AsyncPageReadCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                logger?.LogError($"{nameof(AsyncPageReadCallback)} error: {{errorCode}}", errorCode);
            }
            recoveryCountdown.Decrement();
        }
        #endregion
    }
}