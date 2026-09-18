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
        internal async ValueTask RecoverFuzzyIndexAsync(IndexCheckpointInfo info, CancellationToken cancellationToken)
        {
            ulong alignedIndexSize = InitializeMainIndexRecovery(ref info, isAsync: true);
            try
            {
                await recoveryCountdown.WaitAsync(cancellationToken).ConfigureAwait(false);
                ThrowIfMainIndexRecoveryFailed();
                await overflowBucketsAllocator.RecoverAsync(info.main_ht_device, alignedIndexSize, info.info.num_buckets, info.info.num_ofb_bytes, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // FinalizeMainIndexRecovery, which closes the index checkpoint file on the success path, is not
                // reached; close it here so a failed recovery does not leak the device.
                info.main_ht_device.Dispose();
                throw;
            }
            FinalizeMainIndexRecovery(info);
        }

        private ulong InitializeMainIndexRecovery(ref IndexCheckpointInfo info, bool isAsync)
        {
            var token = info.info.token;
            var ht_version = resizeInfo.version;

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
        private string mainIndexRecoveryError;

        /// <summary>Record the first failure seen while reading the main index, so recovery fails with the error that
        /// occurred first; subsequent failures are logged but do not overwrite it.</summary>
        private void RecordMainIndexRecoveryError(string detail)
            => _ = Interlocked.CompareExchange(ref mainIndexRecoveryError, detail, null);

        /// <summary>Throw if any chunk of the main index failed to read, or read fewer bytes than were requested; the
        /// in-memory table would otherwise be silently missing the buckets that were not read.</summary>
        private void ThrowIfMainIndexRecoveryFailed()
        {
            var error = mainIndexRecoveryError;
            if (error is not null)
                throw new TsavoriteException($"Main index recovery failed: {error}");
        }

        /// <summary>Read the main hash index from <paramref name="device"/> as a sequence of chunks, none larger than
        /// <paramref name="maxIoBytesPerRequest"/>. The default is the largest transfer the OS performs in a single
        /// request; tests lower it to exercise multi-chunk issuance without allocating a multi-gigabyte table.</summary>
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
            for (int index = 0; index < numChunks; index++)
            {
                IntPtr chunkStartBucket = (IntPtr)(((byte*)start) + (index * chunkSize));
                HashIndexPageAsyncReadResult result = default;
                result.chunkIndex = index;
                result.numBytesToRead = chunkSize;
                device.ReadAsync(numBytesRead, chunkStartBucket, chunkSize, AsyncPageReadCallback, result);
                numBytesRead += chunkSize;
            }
            Debug.Assert(numBytesRead == num_bytes);
        }

        private unsafe void AsyncPageReadCallback(uint errorCode, uint numBytes, object context, Exception ioException)
        {
            var result = (HashIndexPageAsyncReadResult)context;
            if (errorCode != 0)
            {
                if (ioException is null)
                    logger?.LogError($"{nameof(AsyncPageReadCallback)} error: {{errorCode}}", errorCode);
                else
                    logger?.LogError($"{nameof(AsyncPageReadCallback)} error: {{exception}}", Utility.GetCallbackExceptionDetail(ioException));
                RecordMainIndexRecoveryError($"chunk {result.chunkIndex} failed with error code {errorCode}");
            }
            else if (numBytes < result.numBytesToRead)
            {
                // A device may report success for a transfer that moved fewer bytes than requested; Linux does this
                // for any single request above MAX_RW_COUNT. The rest of the chunk still holds its pre-read contents,
                // so fail recovery rather than bring up a hash table that is missing buckets.
                logger?.LogError($"{nameof(AsyncPageReadCallback)} error: read {{numBytes}} of {{numBytesToRead}} bytes", numBytes, result.numBytesToRead);
                RecordMainIndexRecoveryError($"chunk {result.chunkIndex} read {numBytes} of {result.numBytesToRead} bytes");
            }
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