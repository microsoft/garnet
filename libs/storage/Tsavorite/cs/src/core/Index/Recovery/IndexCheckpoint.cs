// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    internal unsafe delegate void SkipReadCache(HashBucket* bucket);

    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        // Derived class facing persistence API
        internal IndexCheckpointInfo _indexCheckpoint;

        internal unsafe void TakeIndexFuzzyCheckpoint()
        {
            var ht_version = Kernel.hashTable.spine.resizeInfo.version;

            BeginMainIndexCheckpoint(ht_version, _indexCheckpoint.main_ht_device, out ulong ht_num_bytes_written, UseReadCache, SkipReadCacheBucket, ThrottleCheckpointFlushDelayMs);

            var sectorSize = _indexCheckpoint.main_ht_device.SectorSize;
            var alignedIndexSize = (ht_num_bytes_written + (sectorSize - 1)) & ~((ulong)sectorSize - 1);
            Kernel.hashTable.overflowBucketsAllocator.BeginCheckpoint(_indexCheckpoint.main_ht_device, alignedIndexSize, out ulong ofb_num_bytes_written, UseReadCache, SkipReadCacheBucket, Kernel.Epoch);
            _indexCheckpoint.info.num_ht_bytes = ht_num_bytes_written;
            _indexCheckpoint.info.num_ofb_bytes = ofb_num_bytes_written;
        }
    }

    public partial class TsavoriteBase
    {
        internal void TakeIndexFuzzyCheckpoint(int ht_version, IDevice device,
                                            out ulong numBytesWritten, IDevice ofbdevice,
                                           out ulong ofbnumBytesWritten, out int num_ofb_buckets)
        {
            BeginMainIndexCheckpoint(ht_version, device, out numBytesWritten);
            var sectorSize = device.SectorSize;
            var alignedIndexSize = (numBytesWritten + (sectorSize - 1)) & ~((ulong)sectorSize - 1);
            Kernel.hashTable.overflowBucketsAllocator.BeginCheckpoint(ofbdevice, alignedIndexSize, out ofbnumBytesWritten);
            num_ofb_buckets = Kernel.hashTable.overflowBucketsAllocator.GetMaxValidAddress();
        }

        internal bool IsIndexFuzzyCheckpointCompleted()
        {
            bool completed1 = IsMainIndexCheckpointCompleted();
            bool completed2 = Kernel.hashTable.overflowBucketsAllocator.IsCheckpointCompleted();
            return completed1 && completed2;
        }

        internal async ValueTask IsIndexFuzzyCheckpointCompletedAsync(CancellationToken token = default)
        {
            // Get tasks first to ensure we have captured the semaphore instances synchronously
            var t1 = IsMainIndexCheckpointCompletedAsync(token);
            var t2 = Kernel.hashTable.overflowBucketsAllocator.IsCheckpointCompletedAsync(token);
            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);
        }


        // Implementation of an asynchronous checkpointing scheme 
        // for main hash index of Tsavorite
        private int mainIndexCheckpointCallbackCount;
        private SemaphoreSlim mainIndexCheckpointSemaphore;
        private SemaphoreSlim throttleIndexCheckpointFlushSemaphore;

        internal unsafe void BeginMainIndexCheckpoint(int version, IDevice device, out ulong numBytesWritten, bool useReadCache = false, SkipReadCache skipReadCache = default, int throttleCheckpointFlushDelayMs = -1)
        {
            long totalSize = Kernel.hashTable.spine.state[version].size * sizeof(HashBucket);
            numBytesWritten = (ulong)totalSize;

            if (throttleCheckpointFlushDelayMs >= 0)
                Task.Run(FlushRunner);
            else
                FlushRunner();

            void FlushRunner()
            {
                int numChunks = 1;
                if (useReadCache && (totalSize > (1L << 25)))
                {
                    numChunks = (int)Math.Ceiling((double)totalSize / (1L << 25));
                    numChunks = (int)Math.Pow(2, Math.Ceiling(Math.Log(numChunks, 2)));
                }
                else if (totalSize > uint.MaxValue)
                {
                    numChunks = (int)Math.Ceiling((double)totalSize / (long)uint.MaxValue);
                    numChunks = (int)Math.Pow(2, Math.Ceiling(Math.Log(numChunks, 2)));
                }

                uint chunkSize = (uint)(totalSize / numChunks);
                mainIndexCheckpointCallbackCount = numChunks;
                mainIndexCheckpointSemaphore = new SemaphoreSlim(0);
                if (throttleCheckpointFlushDelayMs >= 0)
                    throttleIndexCheckpointFlushSemaphore = new SemaphoreSlim(0);
                HashBucket* start = Kernel.hashTable.spine.state[version].tableAligned;

                ulong numBytesWritten = 0;
                for (int index = 0; index < numChunks; index++)
                {
                    IntPtr chunkStartBucket = (IntPtr)((byte*)start + (index * chunkSize));
                    HashIndexPageAsyncFlushResult result = default;
                    result.chunkIndex = index;
                    if (!useReadCache)
                    {
                        device.WriteAsync(chunkStartBucket, numBytesWritten, chunkSize, AsyncPageFlushCallback, result);
                    }
                    else
                    {
                        result.mem = new SectorAlignedMemory((int)chunkSize, (int)device.SectorSize);
                        bool prot = false;
                        if (!Kernel.Epoch.ThisInstanceProtected())
                        {
                            prot = true;
                            Kernel.Epoch.Resume();
                        }
                        Buffer.MemoryCopy((void*)chunkStartBucket, result.mem.aligned_pointer, chunkSize, chunkSize);
                        for (int j = 0; j < chunkSize; j += sizeof(HashBucket))
                        {
                            skipReadCache((HashBucket*)(result.mem.aligned_pointer + j));
                        }
                        if (prot)
                            Kernel.Epoch.Suspend();

                        device.WriteAsync((IntPtr)result.mem.aligned_pointer, numBytesWritten, chunkSize, AsyncPageFlushCallback, result);
                    }
                    if (throttleCheckpointFlushDelayMs >= 0)
                    {
                        throttleIndexCheckpointFlushSemaphore.Wait();
                        Thread.Sleep(throttleCheckpointFlushDelayMs);
                    }
                    numBytesWritten += chunkSize;
                }

                Debug.Assert(numBytesWritten == (ulong)totalSize);
                throttleIndexCheckpointFlushSemaphore = null;
            }
        }

        private bool IsMainIndexCheckpointCompleted()
        {
            return mainIndexCheckpointCallbackCount == 0;
        }

        private async ValueTask IsMainIndexCheckpointCompletedAsync(CancellationToken token = default)
        {
            var s = mainIndexCheckpointSemaphore;
            await s.WaitAsync(token).ConfigureAwait(false);
            s.Release();
        }

        private unsafe void AsyncPageFlushCallback(uint errorCode, uint numBytes, object context)
        {
            // Set the page status to flushed
            var mem = ((HashIndexPageAsyncFlushResult)context).mem;
            mem?.Dispose();

            if (errorCode != 0)
            {
                logger?.LogError($"{nameof(AsyncPageFlushCallback)} error: {{errorCode}}", errorCode);
            }
            if (Interlocked.Decrement(ref mainIndexCheckpointCallbackCount) == 0)
            {
                mainIndexCheckpointSemaphore.Release();
            }
            throttleIndexCheckpointFlushSemaphore?.Release();
        }
    }
}