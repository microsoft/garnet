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
        internal void RecoverFuzzyIndex(IndexCheckpointInfo info)
        {
            ulong alignedIndexSize = InitializeMainIndexRecovery(ref info, isAsync: false);
            overflowBucketsAllocator.Recover(info.main_ht_device, alignedIndexSize, info.info.num_buckets, info.info.num_ofb_bytes);

            // Wait until reading is complete
            IsFuzzyIndexRecoveryComplete(true);
            FinalizeMainIndexRecovery(info);
        }

        internal async ValueTask RecoverFuzzyIndexAsync(IndexCheckpointInfo info, CancellationToken cancellationToken)
        {
            ulong alignedIndexSize = InitializeMainIndexRecovery(ref info, isAsync: true);
            await recoveryCountdown.WaitAsync(cancellationToken).ConfigureAwait(false);
            await overflowBucketsAllocator.RecoverAsync(info.main_ht_device, alignedIndexSize, info.info.num_buckets, info.info.num_ofb_bytes, cancellationToken).ConfigureAwait(false);
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
                Free(ht_version);
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
        internal void RecoverFuzzyIndex(int ht_version, IDevice device, ulong num_ht_bytes, IDevice ofbdevice, int num_buckets, ulong num_ofb_bytes)
        {
            BeginMainIndexRecovery(ht_version, device, num_ht_bytes);
            var sectorSize = device.SectorSize;
            var alignedIndexSize = (num_ht_bytes + (sectorSize - 1)) & ~((ulong)sectorSize - 1);
            overflowBucketsAllocator.Recover(ofbdevice, alignedIndexSize, num_buckets, num_ofb_bytes);
        }

        // Test-only
        internal async ValueTask RecoverFuzzyIndexAsync(int ht_version, IDevice device, ulong num_ht_bytes, IDevice ofbdevice, int num_buckets, ulong num_ofb_bytes, CancellationToken cancellationToken)
        {
            BeginMainIndexRecovery(ht_version, device, num_ht_bytes, isAsync: true);
            await recoveryCountdown.WaitAsync(cancellationToken).ConfigureAwait(false);
            var sectorSize = device.SectorSize;
            var alignedIndexSize = (num_ht_bytes + (sectorSize - 1)) & ~((ulong)sectorSize - 1);
            await overflowBucketsAllocator.RecoverAsync(ofbdevice, alignedIndexSize, num_buckets, num_ofb_bytes, cancellationToken).ConfigureAwait(false);
        }

        internal bool IsFuzzyIndexRecoveryComplete(bool waitUntilComplete = false)
        {
            bool completed1 = IsMainIndexRecoveryCompleted(waitUntilComplete);
            bool completed2 = overflowBucketsAllocator.IsRecoveryCompleted(waitUntilComplete);
            return completed1 && completed2;
        }

        /// <summary>
        /// Main Index Recovery Functions
        /// </summary>
        private protected CountdownWrapper recoveryCountdown;

        private unsafe void BeginMainIndexRecovery(
                                int version,
                                IDevice device,
                                ulong num_bytes,
                                bool isAsync = false)
        {
            long totalSize = state[version].size * sizeof(HashBucket);

            int numChunks = 1;
            if (totalSize > uint.MaxValue)
            {
                numChunks = (int)Math.Ceiling((double)totalSize / (long)uint.MaxValue);
                numChunks = (int)Math.Pow(2, Math.Ceiling(Math.Log(numChunks, 2)));
            }

            uint chunkSize = (uint)(totalSize / numChunks);
            recoveryCountdown = new CountdownWrapper(numChunks, isAsync);
            HashBucket* start = state[version].tableAligned;

            ulong numBytesRead = 0;
            for (int index = 0; index < numChunks; index++)
            {
                IntPtr chunkStartBucket = (IntPtr)(((byte*)start) + (index * chunkSize));
                HashIndexPageAsyncReadResult result = default;
                result.chunkIndex = index;
                device.ReadAsync(numBytesRead, chunkStartBucket, chunkSize, AsyncPageReadCallback, result);
                numBytesRead += chunkSize;
            }
            Debug.Assert(numBytesRead == num_bytes);
        }

        private bool IsMainIndexRecoveryCompleted(bool waitUntilComplete = false)
        {
            bool completed = recoveryCountdown.IsCompleted;
            if (!completed && waitUntilComplete)
            {
                recoveryCountdown.Wait();
                return true;
            }
            return completed;
        }

        private unsafe void AsyncPageReadCallback(uint errorCode, uint numBytes, object overlap)
        {
            if (errorCode != 0)
                logger?.LogError($"{nameof(AsyncPageReadCallback)} error: {{errorCode}}", errorCode);
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