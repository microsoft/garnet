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

        internal void AddIndexCheckpointWaitingList(StateMachineDriver stateMachineDriver)
        {
            stateMachineDriver.AddToWaitingList(mainIndexCheckpointTcs.Task, StateMachineTaskType.IndexCheckpointSMTaskMainIndexCheckpoint);
            stateMachineDriver.AddToWaitingList(overflowBucketsAllocator.GetCheckpointTask(), StateMachineTaskType.IndexCheckpointSMTaskOverflowBucketsCheckpoint);
        }

        /// <summary>
        /// Blocks until the fuzzy index checkpoint flush issued at <see cref="Phase.PREPARE"/> has completed, for a
        /// state machine that aborted before <see cref="Phase.WAIT_INDEX_CHECKPOINT"/> put those flushes on the
        /// driver's waiting list.
        /// </summary>
        /// <remarks>
        /// The flush writes to the index checkpoint device and reports through flush state that is shared across
        /// checkpoints (<see cref="mainIndexCheckpointCallbackCount"/>, <see cref="mainIndexCheckpointError"/>,
        /// <see cref="mainIndexCheckpointTcs"/> and their overflow-bucket counterparts). Releasing the device while a
        /// write is still in flight makes that write fail, and its completion then lands on whichever checkpoint owns
        /// the shared state by then - failing the next checkpoint with an error belonging to this one, or completing
        /// its flush before the data is on disk. Faults are observed and discarded here: this checkpoint has already
        /// failed, and the exception that aborted it is the actionable one.
        /// </remarks>
        internal void WaitForIndexCheckpointFlushCompletion()
        {
            WaitForCheckpointFlush(GetMainIndexCheckpointTask());
            WaitForCheckpointFlush(overflowBucketsAllocator.GetCheckpointTask());
        }

        /// <summary>
        /// Task that completes when the flush started by the most recent <see cref="BeginMainIndexCheckpoint"/> has
        /// finished, or <c>null</c> if no main index checkpoint has been started on this instance.
        /// </summary>
        internal Task GetMainIndexCheckpointTask() => mainIndexCheckpointTcs?.Task;

        /// <summary>
        /// Blocks until <paramref name="flushTask"/> completes, discarding its outcome. Does nothing when the flush
        /// was never started.
        /// </summary>
        internal static void WaitForCheckpointFlush(Task flushTask)
        {
            if (flushTask is null)
                return;

            try
            {
                flushTask.GetAwaiter().GetResult();
            }
            catch
            {
                // The flush outcome is deliberately discarded; the caller is already unwinding a failed checkpoint.
                // Retrieving it here also marks the task's exception observed.
            }
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
        private IoFailure mainIndexCheckpointError;
        private TaskCompletionSource<bool> mainIndexCheckpointTcs;
        private SemaphoreSlim throttleIndexCheckpointFlushSemaphore;

        /// <summary>Record the first failure seen while flushing the main index, so the checkpoint fails with the
        /// error that occurred first; subsequent failures are logged but do not overwrite it.</summary>
        private void RecordMainIndexCheckpointError(string detail, Exception exception = null)
            => _ = Interlocked.CompareExchange(ref mainIndexCheckpointError, new IoFailure(detail, exception), null);

        /// <summary>Retire one outstanding chunk from the main index checkpoint flush, completing the checkpoint task
        /// when the last one is retired.</summary>
        private void RetireMainIndexCheckpointChunk()
        {
            if (Interlocked.Decrement(ref mainIndexCheckpointCallbackCount) == 0)
            {
                var error = mainIndexCheckpointError;
                if (error is not null)
                    mainIndexCheckpointTcs.TrySetException(error.ToException("Main index checkpoint flush failed"));
                else
                    mainIndexCheckpointTcs.TrySetResult(true);
            }
        }

        /// <summary>Write the main hash index to <paramref name="device"/> as a sequence of chunks, none larger than
        /// <paramref name="maxIoBytesPerRequest"/>. The default is <see cref="Constants.kMaxIoBytesPerRequest"/>, which
        /// is deliberately below the largest single transfer any supported platform performs; tests lower it to
        /// exercise multi-chunk issuance without allocating a multi-gigabyte table.</summary>
        internal unsafe void BeginMainIndexCheckpoint(int version, IDevice device, out ulong numBytesWritten, bool useReadCache = false, SkipReadCache skipReadCache = default,
                int throttleCheckpointFlushDelayMs = -1, long maxIoBytesPerRequest = Constants.kMaxIoBytesPerRequest)
        {
            long totalSize = state[version].size * sizeof(HashBucket);
            numBytesWritten = (ulong)totalSize;
            mainIndexCheckpointCallbackCount = 0;
            mainIndexCheckpointError = null;
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
                // Number of chunks whose retirement is accounted for: the device accepted the write and its completion
                // callback will retire the chunk, or the submit failed and the catch below retired it in place. Chunks
                // past this point were counted into mainIndexCheckpointCallbackCount but never handed to the device.
                var accountedChunks = 0;
                var countedChunks = 0;
                var sentinelPending = false;
                try
                {
                    // Split the table into chunks small enough that no single device request exceeds the maximum the
                    // OS transfers in one call; a larger request completes short (see Constants.kMaxIoBytesPerRequest)
                    // and would leave the tail of the table out of the checkpoint. When the read cache is enabled each
                    // chunk is additionally staged through a sector-aligned copy, so chunks are kept small.
                    const long kReadCacheChunkSize = 1L << 25;
                    var maxChunkSize = useReadCache ? Math.Min(kReadCacheChunkSize, maxIoBytesPerRequest) : maxIoBytesPerRequest;
                    var numChunks = Utility.GetNumIoChunks(totalSize, maxChunkSize);

                    uint chunkSize = (uint)(totalSize / numChunks);
                    Debug.Assert(chunkSize <= maxChunkSize, "Index checkpoint chunk exceeds the maximum device request size");
                    // Count an issuance sentinel alongside the chunks, retired in the finally below once issuance has
                    // ended and the catch has recorded any exception. A device may invoke a completion callback
                    // synchronously and then throw out of the same submit; without the sentinel that completion can
                    // drive the count to zero and report the checkpoint successful before the failure is recorded.
                    mainIndexCheckpointCallbackCount = numChunks + 1;
                    countedChunks = numChunks;
                    sentinelPending = true;

                    if (throttleCheckpointFlushDelayMs >= 0)
                        throttleIndexCheckpointFlushSemaphore = new SemaphoreSlim(0);
                    HashBucket* start = state[version].tableAligned;

                    ulong numBytesWritten = 0;
                    for (int index = 0; index < numChunks; index++)
                    {
                        IntPtr chunkStartBucket = (IntPtr)((byte*)start + (index * chunkSize));
                        HashIndexPageAsyncFlushResult result = default;
                        result.chunkIndex = index;
                        result.numBytesToWrite = chunkSize;
                        result.ioUnitReleaseGuard = new(0);
                        if (!useReadCache)
                        {
                            BeginNativeIndexCheckpointIo();
                            try
                            {
                                device.WriteAsync(chunkStartBucket, numBytesWritten, chunkSize, AsyncPageFlushCallback, result);
                            }
                            catch (Exception ex)
                            {
                                // A device may invoke the completion callback synchronously and then throw back out of
                                // the submit (LocalMemoryDevice propagates callback exceptions), so the callback may
                                // already have released this chunk's unit and retired the chunk. Claim exactly once to
                                // avoid underflowing the index's outstanding-IO count, which could free a superseded table
                                // while issuance still reads it, and to avoid retiring the chunk twice. Record the error
                                // before retiring, so a retirement that completes the task reports the failure.
                                RecordMainIndexCheckpointError($"chunk {index} could not be issued", ex);
                                if (result.TryClaimIoUnitRelease())
                                {
                                    EndNativeIndexCheckpointIo();
                                    RetireMainIndexCheckpointChunk();
                                }
                                accountedChunks++;
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
                            try
                            {
                                Buffer.MemoryCopy((void*)chunkStartBucket, result.mem.aligned_pointer, chunkSize, chunkSize);
                                for (int j = 0; j < chunkSize; j += sizeof(HashBucket))
                                {
                                    skipReadCache((HashBucket*)(result.mem.aligned_pointer + j));
                                }
                            }
                            finally
                            {
                                // Staging can throw, and with throttling enabled this is a pooled thread. Leaving it
                                // epoch-protected would pin the safe-to-reclaim boundary for the life of the process.
                                if (prot)
                                    epoch.Suspend();
                            }

                            BeginNativeIndexCheckpointIo();
                            try
                            {
                                device.WriteAsync((IntPtr)result.mem.aligned_pointer, numBytesWritten, chunkSize, AsyncPageFlushCallback, result);
                            }
                            catch (Exception ex)
                            {
                                // A device may invoke the completion callback synchronously and then throw back out of
                                // the submit (LocalMemoryDevice propagates callback exceptions), so the callback may
                                // already have released this chunk's unit, its staging buffer, and retired the chunk.
                                // Claim exactly once to avoid underflowing the index's outstanding-IO count, which could
                                // free a superseded table while issuance still reads it, and to avoid returning the buffer
                                // or retiring the chunk twice. Record the error before retiring, so a retirement that
                                // completes the task reports the failure.
                                RecordMainIndexCheckpointError($"chunk {index} could not be issued", ex);
                                if (result.TryClaimIoUnitRelease())
                                {
                                    result.mem.Dispose();
                                    EndNativeIndexCheckpointIo();
                                    RetireMainIndexCheckpointChunk();
                                }
                                accountedChunks++;
                                throw;
                            }
                        }
                        accountedChunks++;
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

                    // Chunks already handed to the device will still complete and retire themselves, but the ones
                    // counted after the failure have no callback to retire them. Retiring them here is what lets the
                    // outstanding count reach zero, so the last real completion - not this thread - completes the task.
                    // Completing the task directly instead would release its waiters while writes are still in flight
                    // against a device the failing checkpoint is about to dispose, and those completions would then be
                    // counted against whichever checkpoint owns the shared flush state by the time they land.
                    RecordMainIndexCheckpointError($"index checkpoint flush failed after {accountedChunks} of {countedChunks} chunks", ex);
                    for (var unaccounted = accountedChunks; unaccounted < countedChunks; unaccounted++)
                        RetireMainIndexCheckpointChunk();

                    // Nothing was counted, so there is no sentinel and no completion will ever run.
                    if (!sentinelPending)
                        mainIndexCheckpointTcs.TrySetException(ex);
                }
                finally
                {
                    // Retire the issuance sentinel, now that issuance has ended and the catch above has recorded any
                    // exception it hit. This is what completes the checkpoint, so it completes with that error rather
                    // than with a success a synchronous completion reached before the submit threw.
                    if (sentinelPending)
                        RetireMainIndexCheckpointChunk();

                    // Release the issuance sentinel. Any chunk writes still in flight keep the outstanding-IO count
                    // > 0 until their callbacks fire; the last release frees tables superseded during this flush.
                    EndNativeIndexCheckpointIo();
                }
            }
        }

        private async ValueTask IsMainIndexCheckpointCompletedAsync(CancellationToken token = default)
        {
            await mainIndexCheckpointTcs.Task.WaitAsync(token).ConfigureAwait(false);
        }

        private void AsyncPageFlushCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            var result = (HashIndexPageAsyncFlushResult)context;
            try
            {
                if (errorCode != 0)
                {
                    if (ioException is null)
                        logger?.LogError($"{nameof(AsyncPageFlushCallback)} error: {{errorCode}}", errorCode);
                    else
                        logger?.LogError($"{nameof(AsyncPageFlushCallback)} error: {{exception}}", Utility.GetCallbackExceptionDetail(ioException));
                    RecordMainIndexCheckpointError($"chunk {result.chunkIndex} failed with error code {errorCode}", ioException);
                }
                else if (numBytes != 0 && numBytes < result.numBytesToWrite)
                {
                    // Linux completes a single request larger than MAX_RW_COUNT successfully but short, so a
                    // transferred count below the requested length means the chunk is not fully on disk. A count of
                    // zero means the device does not report one (see DeviceIOCompletionCallback), not a short write.
                    logger?.LogError($"{nameof(AsyncPageFlushCallback)} error: wrote {{numBytes}} of {{numBytesToWrite}} bytes", numBytes, result.numBytesToWrite);
                    RecordMainIndexCheckpointError($"chunk {result.chunkIndex} wrote {numBytes} of {result.numBytesToWrite} bytes");
                }

                RetireMainIndexCheckpointChunk();
                throttleIndexCheckpointFlushSemaphore?.Release();
            }
            finally
            {
                // Release this chunk write's unit of outstanding index-checkpoint IO, and the staging buffer it wrote
                // from. In a finally so it runs on every path (success, error, exception); when the last unit is
                // released, tables superseded by a grow while this write was in flight are munmap'd. No-op for the
                // managed hash index. Claimed exactly once so a synchronous callback here and the issuer's catch
                // cannot both release the unit or return the buffer twice.
                if (result.TryClaimIoUnitRelease())
                {
                    result.mem?.Dispose();
                    EndNativeIndexCheckpointIo();
                }
            }
        }
    }
}