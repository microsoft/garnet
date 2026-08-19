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

    public partial class TsavoriteKV<TStoreFunctions, TAllocator> : TsavoriteBase
        where TStoreFunctions : IStoreFunctions
        where TAllocator : IAllocator<TStoreFunctions>
    {
        // Derived class facing persistence API
        internal IndexCheckpointInfo _indexCheckpoint;

        internal unsafe void TakeIndexFuzzyCheckpoint()
        {
            var ht_version = resizeInfo.version;

            BeginMainIndexCheckpoint(ht_version, _indexCheckpoint.main_ht_device, out ulong ht_num_bytes_written, UseReadCache, SkipReadCacheBucket, ThrottleCheckpointFlushDelayMs);

            var sectorSize = _indexCheckpoint.main_ht_device.SectorSize;
            var alignedIndexSize = (ht_num_bytes_written + (sectorSize - 1)) & ~((ulong)sectorSize - 1);
            overflowBucketsAllocator.BeginCheckpoint(_indexCheckpoint.main_ht_device, alignedIndexSize, out ulong ofb_num_bytes_written, UseReadCache, SkipReadCacheBucket, epoch);
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
            overflowBucketsAllocator.BeginCheckpoint(ofbdevice, alignedIndexSize, out ofbnumBytesWritten);
            num_ofb_buckets = overflowBucketsAllocator.GetMaxValidAddress();
        }

        internal bool IsIndexFuzzyCheckpointCompleted()
        {
            bool completed1 = IsMainIndexCheckpointCompleted();
            bool completed2 = overflowBucketsAllocator.IsCheckpointCompleted();
            return completed1 && completed2;
        }

        internal void AddIndexCheckpointWaitingList(StateMachineDriver stateMachineDriver)
        {
            stateMachineDriver.AddToWaitingList(mainIndexCheckpointTcs.Task, StateMachineTaskType.IndexCheckpointSMTaskMainIndexCheckpoint);
            stateMachineDriver.AddToWaitingList(overflowBucketsAllocator.GetCheckpointTask(), StateMachineTaskType.IndexCheckpointSMTaskOverflowBucketsCheckpoint);
        }

        internal async ValueTask IsIndexFuzzyCheckpointCompletedAsync(CancellationToken token = default)
        {
            // Get tasks first to ensure we have captured the semaphore instances synchronously
            var t1 = IsMainIndexCheckpointCompletedAsync(token);
            var t2 = overflowBucketsAllocator.IsCheckpointCompletedAsync(token);
            await t1.ConfigureAwait(false);
            await t2.ConfigureAwait(false);
        }

        // Implementation of an asynchronous checkpointing scheme 
        // for main hash index of Tsavorite
        private int mainIndexCheckpointCallbackCount;
        private int mainIndexCheckpointErrorCode;
        private TaskCompletionSource<bool> mainIndexCheckpointTcs;
        private SemaphoreSlim throttleIndexCheckpointFlushSemaphore;

        internal unsafe void BeginMainIndexCheckpoint(int version, IDevice device, out ulong numBytesWritten, bool useReadCache = false, SkipReadCache skipReadCache = default, int throttleCheckpointFlushDelayMs = -1)
        {
            long totalSize = state[version].size * sizeof(HashBucket);
            numBytesWritten = (ulong)totalSize;
            mainIndexCheckpointErrorCode = 0;
            mainIndexCheckpointTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            // Producer sentinel: count index-checkpoint IO as outstanding for the whole issuance so a concurrent
            // grow parks (does not munmap) a superseded native table this checkpoint may still be writing. Released
            // in FlushRunner's finally; each issued chunk write holds its own unit until its completion callback
            // fires. No-op for the managed hash index.
            BeginNativeIndexCheckpointIo();

            if (throttleCheckpointFlushDelayMs >= 0)
                Task.Run(FlushRunner);
            else
                FlushRunner();

            void FlushRunner()
            {
                try
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

                    if (throttleCheckpointFlushDelayMs >= 0)
                        throttleIndexCheckpointFlushSemaphore = new SemaphoreSlim(0);
                    HashBucket* start = state[version].tableAligned;

                    ulong numBytesWritten = 0;
                    for (int index = 0; index < numChunks; index++)
                    {
                        IntPtr chunkStartBucket = (IntPtr)((byte*)start + (index * chunkSize));
                        HashIndexPageAsyncFlushResult result = default;
                        result.chunkIndex = index;
                        result.ioUnitReleaseGuard = new(0);
                        if (!useReadCache)
                        {
                            BeginNativeIndexCheckpointIo();
                            try
                            {
                                device.WriteAsync(chunkStartBucket, numBytesWritten, chunkSize, AsyncPageFlushCallback, result);
                            }
                            catch
                            {
                                // A device may invoke the completion callback synchronously and then throw back out of
                                // the submit (LocalMemoryDevice propagates callback exceptions), so the callback may
                                // already have released this chunk's unit. Claim exactly once to avoid underflowing
                                // the index's outstanding-IO count, which could free a superseded table while issuance still
                                // reads it. If the submit failed before any callback ran, we are the only claimant.
                                if (result.TryClaimIoUnitRelease())
                                    EndNativeIndexCheckpointIo();
                                throw;
                            }
                        }
                        else
                        {
                            result.mem = new SectorAlignedMemory((int)chunkSize, (int)device.SectorSize);
                            bool prot = false;
                            if (!epoch.ThisInstanceProtected())
                            {
                                prot = true;
                                epoch.Resume();
                            }
                            Buffer.MemoryCopy((void*)chunkStartBucket, result.mem.aligned_pointer, chunkSize, chunkSize);
                            for (int j = 0; j < chunkSize; j += sizeof(HashBucket))
                            {
                                skipReadCache((HashBucket*)(result.mem.aligned_pointer + j));
                            }
                            if (prot)
                                epoch.Suspend();

                            BeginNativeIndexCheckpointIo();
                            try
                            {
                                device.WriteAsync((IntPtr)result.mem.aligned_pointer, numBytesWritten, chunkSize, AsyncPageFlushCallback, result);
                            }
                            catch
                            {
                                // A device may invoke the completion callback synchronously and then throw back out of
                                // the submit (LocalMemoryDevice propagates callback exceptions), so the callback may
                                // already have released this chunk's unit. Claim exactly once to avoid underflowing
                                // the index's outstanding-IO count, which could free a superseded table while issuance still
                                // reads it. If the submit failed before any callback ran, we are the only claimant.
                                if (result.TryClaimIoUnitRelease())
                                    EndNativeIndexCheckpointIo();
                                throw;
                            }
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
                catch (Exception ex)
                {
                    logger?.LogError(ex, "{method} failed while flushing index checkpoint", nameof(BeginMainIndexCheckpoint));
                    mainIndexCheckpointTcs.TrySetException(ex);
                }
                finally
                {
                    // Release the issuance sentinel. Any chunk writes still in flight keep the outstanding-IO count
                    // > 0 until their callbacks fire; the last release frees tables superseded during this flush.
                    EndNativeIndexCheckpointIo();
                }
            }
        }

        private bool IsMainIndexCheckpointCompleted()
        {
            return mainIndexCheckpointCallbackCount == 0;
        }

        private async ValueTask IsMainIndexCheckpointCompletedAsync(CancellationToken token = default)
        {
            await mainIndexCheckpointTcs.Task.WaitAsync(token).ConfigureAwait(false);
        }

        private void AsyncPageFlushCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            try
            {
                // Set the page status to flushed
                var mem = ((HashIndexPageAsyncFlushResult)context).mem;
                mem?.Dispose();

                if (errorCode != 0)
                {
                    if (ioException is null)
                        logger?.LogError($"{nameof(AsyncPageFlushCallback)} error: {{errorCode}}", errorCode);
                    else
                        logger?.LogError($"{nameof(AsyncPageFlushCallback)} error: {{exception}}", Utility.GetCallbackExceptionDetail(ioException));
                    _ = Interlocked.CompareExchange(ref mainIndexCheckpointErrorCode, (int)errorCode, 0);
                }
                if (Interlocked.Decrement(ref mainIndexCheckpointCallbackCount) == 0)
                {
                    var err = mainIndexCheckpointErrorCode;
                    if (err != 0)
                        mainIndexCheckpointTcs.TrySetException(new TsavoriteException($"Main index checkpoint flush failed with error code {err}"));
                    else
                        mainIndexCheckpointTcs.TrySetResult(true);
                }
                throttleIndexCheckpointFlushSemaphore?.Release();
            }
            finally
            {
                // Release this chunk write's unit of outstanding index-checkpoint IO. In a finally so it runs on
                // every path (success, error, exception); when the last unit is released, tables superseded by a
                // grow while this write was in flight are munmap'd. No-op for the managed hash index. Claimed
                // exactly once so a synchronous callback here and the issuer's catch cannot both release.
                if (((HashIndexPageAsyncFlushResult)context).TryClaimIoUnitRelease())
                    EndNativeIndexCheckpointIo();
            }
        }
    }
}