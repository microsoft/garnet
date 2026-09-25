// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary/>
    public partial class TsavoriteBase
    {
        internal ICheckpointManager checkpointManager;
        internal bool disposeCheckpointManager;

        /// <summary>
        /// CheckpointManager
        /// </summary>
        public ICheckpointManager CheckpointManager => checkpointManager;

        // Derived class exposed API
        /// <summary>Recover the hash index from <paramref name="info"/>.</summary>
        /// <remarks>Cancelling <paramref name="cancellationToken"/> abandons the wait but not the reads it was waiting
        /// for: the reads are writing into the in-memory table and reading from the checkpoint file, so this returns
        /// only once every issued read has called back. A device that never completes a read therefore makes a
        /// cancelled recovery wait indefinitely rather than returning promptly.</remarks>
        internal async ValueTask RecoverFuzzyIndexAsync(IndexCheckpointInfo info, CancellationToken cancellationToken)
        {
            try
            {
                ulong alignedIndexSize = InitializeMainIndexRecovery(ref info, isAsync: true);
                await recoveryCountdown.WaitAsync(cancellationToken).ConfigureAwait(false);
                ThrowIfMainIndexRecoveryFailed();
                await overflowBucketsAllocator.RecoverAsync(info.main_ht_device, alignedIndexSize, info.info.num_buckets, info.info.num_ofb_bytes, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // FinalizeMainIndexRecovery, which closes the index checkpoint file on the success path, is not
                // reached. Reads may still be outstanding against that file: cancelling the waits above does not
                // cancel the reads they were waiting for, and InitializeMainIndexRecovery can throw partway through
                // issuing them. Let every issued read call back before closing the device it is reading from.
                await DrainMainIndexRecoveryAsync().ConfigureAwait(false);
                await overflowBucketsAllocator.DrainRecoveryAsync().ConfigureAwait(false);
                info.main_ht_device?.Dispose();
                throw;
            }
            FinalizeMainIndexRecovery(info);
        }

        private ulong InitializeMainIndexRecovery(ref IndexCheckpointInfo info, bool isAsync)
        {
            var token = info.info.token;
            var ht_version = resizeInfo.version;

            // Drop any state left by an earlier recovery so a failure before BeginMainIndexRecovery cannot drain
            // against a stale countdown, and so a stale error cannot fail this recovery.
            recoveryCountdown = null;
            mainIndexRecoveryError = null;

            // Create devices to read from using Async API
            info.main_ht_device = checkpointManager.GetIndexDevice(token);
            var sectorSize = info.main_ht_device.SectorSize;

            if (state[ht_version].size != info.info.table_size)
            {
                // This will allocate over any existing table for this version and initialize the new table size
                Initialize(info.info.table_size, (int)sectorSize);
            }

            BeginMainIndexRecovery(ht_version, info.main_ht_device, info.info.num_ht_bytes, isAsync);

            var alignedIndexSize = (info.info.num_ht_bytes + (sectorSize - 1)) & ~((ulong)sectorSize - 1);
            return alignedIndexSize;
        }

        private void FinalizeMainIndexRecovery(IndexCheckpointInfo info)
        {
            // close index checkpoint files appropriately
            info.main_ht_device.Dispose();

            // Delete all tentative entries!
            DeleteTentativeEntries();
        }

        // Test-only
        internal async ValueTask RecoverFuzzyIndexAsync(int ht_version, IDevice device, ulong num_ht_bytes, IDevice ofbdevice, int num_buckets, ulong num_ofb_bytes,
                CancellationToken cancellationToken, long maxIoBytesPerRequest = Constants.kMaxIoBytesPerRequest)
        {
            BeginMainIndexRecovery(ht_version, device, num_ht_bytes, isAsync: true, maxIoBytesPerRequest);
            await recoveryCountdown.WaitAsync(cancellationToken).ConfigureAwait(false);
            ThrowIfMainIndexRecoveryFailed();
            var sectorSize = device.SectorSize;
            var alignedIndexSize = (num_ht_bytes + (sectorSize - 1)) & ~((ulong)sectorSize - 1);
            await overflowBucketsAllocator.RecoverAsync(ofbdevice, alignedIndexSize, num_buckets, num_ofb_bytes, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Main Index Recovery Functions
        /// </summary>
        private protected CountdownWrapper recoveryCountdown;
        private IoFailure mainIndexRecoveryError;

        /// <summary>Record the first failure seen while reading the main index, so recovery fails with the error that
        /// occurred first; subsequent failures are logged but do not overwrite it.</summary>
        private void RecordMainIndexRecoveryError(string detail, Exception exception = null)
            => _ = Interlocked.CompareExchange(ref mainIndexRecoveryError, new IoFailure(detail, exception), null);

        /// <summary>Throw if any chunk of the main index failed to read, or read fewer bytes than were requested; the
        /// in-memory table would otherwise be silently missing the buckets that were not read.</summary>
        private void ThrowIfMainIndexRecoveryFailed()
        {
            var error = mainIndexRecoveryError;
            if (error is not null)
                throw error.ToException("Main index recovery failed");
        }

        /// <summary>Read the main hash index from <paramref name="device"/> as a sequence of chunks, none larger than
        /// <paramref name="maxIoBytesPerRequest"/>. The default is <see cref="Constants.kMaxIoBytesPerRequest"/>, which
        /// is deliberately below the largest single transfer any supported platform performs; tests lower it to
        /// exercise multi-chunk issuance without allocating a multi-gigabyte table.</summary>
        private unsafe void BeginMainIndexRecovery(
                                int version,
                                IDevice device,
                                ulong num_bytes,
                                bool isAsync = false,
                                long maxIoBytesPerRequest = Constants.kMaxIoBytesPerRequest)
        {
            long totalSize = state[version].size * sizeof(HashBucket);

            // Split the table into chunks small enough that no single device request exceeds the maximum the OS
            // transfers in one call; a larger request completes short (see Constants.kMaxIoBytesPerRequest) and would
            // leave the tail of the table unrecovered.
            var numChunks = Utility.GetNumIoChunks(totalSize, maxIoBytesPerRequest);

            uint chunkSize = (uint)(totalSize / numChunks);
            Debug.Assert(chunkSize <= maxIoBytesPerRequest, "Index recovery chunk exceeds the maximum device request size");
            mainIndexRecoveryError = null;
            recoveryCountdown = new CountdownWrapper(numChunks, isAsync);
            HashBucket* start = state[version].tableAligned;

            ulong numBytesRead = 0;
            int index = 0;
            try
            {
                for (; index < numChunks; index++)
                {
                    IntPtr chunkStartBucket = (IntPtr)(((byte*)start) + (index * chunkSize));
                    HashIndexPageAsyncReadResult result = default;
                    result.chunkIndex = index;
                    result.numBytesToRead = chunkSize;
                    result.retirementGuard = new(0);
                    try
                    {
                        device.ReadAsync(numBytesRead, chunkStartBucket, chunkSize, AsyncPageReadCallback, result);
                    }
                    catch
                    {
                        // A device may invoke the completion callback synchronously and then throw back out of the
                        // submit, so this chunk may already have been retired. Claim exactly once: retiring it twice
                        // would let the countdown reach zero while earlier reads are still writing into the table,
                        // and the caller would close the device out from under them.
                        if (result.TryClaimRetirement())
                            recoveryCountdown.Decrement();
                        throw;
                    }
                    numBytesRead += chunkSize;
                }
                Debug.Assert(numBytesRead == num_bytes);
            }
            catch
            {
                // The chunks already issued will still complete and touch the device, so the countdown must reach
                // zero for the caller to know when it is safe to dispose. Retire the chunks never issued; the one
                // that failed to issue was retired above.
                for (index++; index < numChunks; index++)
                    recoveryCountdown.Decrement();
                throw;
            }
        }

        /// <summary>Wait for every main-index read that was issued to complete, ignoring whether it succeeded. Used on
        /// failure paths before disposing the device, since cancelling the wait does not cancel the reads.</summary>
        internal ValueTask DrainMainIndexRecoveryAsync()
            => recoveryCountdown is null ? default : recoveryCountdown.DrainAsync();

        private unsafe void AsyncPageReadCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            var result = (HashIndexPageAsyncReadResult)context;
            if (errorCode != 0)
            {
                if (ioException is null)
                    logger?.LogError($"{nameof(AsyncPageReadCallback)} error: {{errorCode}}", errorCode);
                else
                    logger?.LogError($"{nameof(AsyncPageReadCallback)} error: {{exception}}", Utility.GetCallbackExceptionDetail(ioException));
                RecordMainIndexRecoveryError($"chunk {result.chunkIndex} failed with error code {errorCode}", ioException);
            }
            else if (numBytes != 0 && numBytes < result.numBytesToRead)
            {
                // A transferred count below the requested length means part of the chunk still holds its pre-read
                // contents, so fail recovery rather than bring up a hash table that is missing buckets. A count of
                // zero means the device does not report one (see DeviceIOCompletionCallback), not a short read.
                logger?.LogError($"{nameof(AsyncPageReadCallback)} error: read {{numBytes}} of {{numBytesToRead}} bytes", numBytes, result.numBytesToRead);
                RecordMainIndexRecoveryError($"chunk {result.chunkIndex} read {numBytes} of {result.numBytesToRead} bytes");
            }
            if (result.TryClaimRetirement())
                recoveryCountdown.Decrement();
        }

        internal unsafe void DeleteTentativeEntries()
        {
            HashBucketEntry entry = default;

            int version = resizeInfo.version;
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;

            for (long bucket = 0; bucket < table_size_; bucket++)
            {
                HashBucket* b = ptable_ + bucket;
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; bucket_entry++)
                    {
                        entry.word = b->bucket_entries[bucket_entry];
                        if (entry.Tentative)
                            b->bucket_entries[bucket_entry] = 0;
                    }
                    // Reset any ephemeral bucket level locks
                    b->bucket_entries[Constants.kOverflowBucketIndex] &= (long)LogAddress.kAddressBitMask;
                    if (b->bucket_entries[Constants.kOverflowBucketIndex] == 0) break;
                    b = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(b->bucket_entries[Constants.kOverflowBucketIndex]);
                }
            }
        }
    }
}