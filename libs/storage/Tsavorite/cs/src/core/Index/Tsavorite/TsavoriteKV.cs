// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Tsavorite.core
{
    /// <summary>
    /// The Tsavorite Key/Value store class
    /// </summary>
    public partial class TsavoriteKV<TKey, TValue, TStoreFunctions, TAllocator> : TsavoriteBase, IDisposable
        where TStoreFunctions : IStoreFunctions<TKey, TValue>
        where TAllocator : IAllocator<TKey, TValue, TStoreFunctions>
    {
        internal readonly TAllocator hlog;
        internal readonly AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> hlogBase;
        internal readonly TAllocator readcache;
        internal readonly AllocatorBase<TKey, TValue, TStoreFunctions, TAllocator> readcacheBase;

        internal readonly TStoreFunctions storeFunctions;

        internal readonly bool UseReadCache;
        private readonly ReadCopyOptions ReadCopyOptions;
        internal readonly int sectorSize;  // TODO: remove in favor of what's in Kernel

        /// <summary>
        /// Number of active entries in hash index (does not correspond to total records, due to hash collisions)
        /// </summary>
        public long EntryCount => GetEntryCount();

        /// <summary>
        /// Maximum number of memory pages ever allocated
        /// </summary>
        public long MaxAllocatedPageCount => hlogBase.MaxAllocatedPageCount;

        /// <summary>
        /// Size of index in #cache lines (64 bytes each)
        /// </summary>
        public long IndexSize => Kernel.hashTable.spine.state[Kernel.hashTable.spine.resizeInfo.version].size;

        /// <summary>Number of allocations performed</summary>
        public long OverflowBucketAllocations => Kernel.hashTable.overflowBucketsAllocator.NumAllocations;

        /// <summary>
        /// Number of overflow buckets in use (64 bytes each)
        /// </summary>
        public long OverflowBucketCount => Kernel.hashTable.overflowBucketsAllocator.GetMaxValidAddress();

        /// <summary>
        /// Hybrid log used by this Tsavorite instance
        /// </summary>
        public LogAccessor<TKey, TValue, TStoreFunctions, TAllocator> Log { get; }

        /// <summary>
        /// Read cache used by this Tsavorite instance
        /// </summary>
        public LogAccessor<TKey, TValue, TStoreFunctions, TAllocator> ReadCache { get; }

        int maxSessionID;

        internal readonly bool CheckpointVersionSwitchBarrier;  // version switch barrier
        internal HashBucketLockTable LockTable => Kernel.lockTable;

        internal void IncrementNumTxnSessions()
        {
            _hybridLogCheckpoint.info.transactionActive = true;
            Interlocked.Increment(ref hlogBase.NumActiveTxnSessions);
        }
        internal void DecrementNumTxnSessions() => Interlocked.Decrement(ref hlogBase.NumActiveTxnSessions);

        internal readonly int ThrottleCheckpointFlushDelayMs = -1;

        internal RevivificationManager<TKey, TValue, TStoreFunctions, TAllocator> RevivificationManager;

        internal Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorFactory;

        /// <summary>
        /// Create TsavoriteKV instance using non-shared kernel in a non-partitioned implementation. TODO This is temporary and used by tests only; it should eventually be removed along with kvSettings.IndexSize.
        /// </summary>
        /// <param name="kvSettings">Config settings</param>
        /// <param name="storeFunctions">Store-level user function implementations</param>
        /// <param name="allocatorFactory">Func to call to create the allocator(s, if doing readcache)</param>
        public TsavoriteKV(KVSettings<TKey, TValue> kvSettings, TStoreFunctions storeFunctions, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorFactory)
            : this(new TsavoriteKernel(kvSettings.GetIndexSizeCacheLines(), Environment.SystemPageSize, kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger("TsavoriteKernel")),
                  partitionId: 0, kvSettings, storeFunctions, allocatorFactory)
        {
        }

        /// <summary>
        /// Create TsavoriteKV instance
        /// </summary>
        /// <param name="kernel">The <see cref="TsavoriteKernel"/></param>
        /// <param name="kvSettings">Config settings</param>
        /// <param name="storeFunctions">Store-level user function implementations</param>
        /// <param name="allocatorFactory">Func to call to create the allocator(s, if doing readcache)</param>
        public TsavoriteKV(TsavoriteKernel kernel, ushort partitionId, KVSettings<TKey, TValue> kvSettings, TStoreFunctions storeFunctions, Func<AllocatorSettings, TStoreFunctions, TAllocator> allocatorFactory)
            : base(kernel, partitionId, kvSettings.logger, kvSettings.loggerFactory)
        {
            this.allocatorFactory = allocatorFactory;

            this.storeFunctions = storeFunctions;

            var checkpointSettings = kvSettings.GetCheckpointSettings() ?? new CheckpointSettings();

            CheckpointVersionSwitchBarrier = checkpointSettings.CheckpointVersionSwitchBarrier;
            ThrottleCheckpointFlushDelayMs = checkpointSettings.ThrottleCheckpointFlushDelayMs;

            if (checkpointSettings.CheckpointDir != null && checkpointSettings.CheckpointManager != null)
                logger?.LogInformation("CheckpointManager and CheckpointDir specified, ignoring CheckpointDir");

            checkpointManager = checkpointSettings.CheckpointManager ??
                new DeviceLogCommitCheckpointManager
                (new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(
                        new DirectoryInfo(checkpointSettings.CheckpointDir ?? ".").FullName), removeOutdated: checkpointSettings.RemoveOutdated);

            if (checkpointSettings.CheckpointManager is null)
                disposeCheckpointManager = true;

            var logSettings = kvSettings.GetLogSettings();
            sectorSize = (int)logSettings.LogDevice.SectorSize;

            UseReadCache = kvSettings.ReadCacheEnabled;

            ReadCopyOptions = logSettings.ReadCopyOptions;
            if (ReadCopyOptions.CopyTo == ReadCopyTo.Inherit)
                ReadCopyOptions.CopyTo = UseReadCache ? ReadCopyTo.ReadCache : ReadCopyTo.None;
            else if (ReadCopyOptions.CopyTo == ReadCopyTo.ReadCache && !UseReadCache)
                ReadCopyOptions.CopyTo = ReadCopyTo.None;

            if (ReadCopyOptions.CopyFrom == ReadCopyFrom.Inherit)
                ReadCopyOptions.CopyFrom = ReadCopyFrom.Device;

            bool isFixedLenReviv = hlog.IsFixedLength;

            // Create the allocator
            var allocatorSettings = new AllocatorSettings(logSettings, kernel.Epoch, kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger(typeof(TAllocator).Name));
            hlog = allocatorFactory(allocatorSettings, storeFunctions);
            hlogBase = hlog.GetBase<TAllocator>();
            hlogBase.partitionId = partitionId;
            hlogBase.Initialize();
            Log = new(this, hlog);

            if (UseReadCache)
            {
                allocatorSettings.LogSettings = new()
                {
                    LogDevice = new NullDevice(),
                    ObjectLogDevice = hlog.HasObjectLog ? new NullDevice() : null,
                    PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                    MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                    SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                    MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                };
                allocatorSettings.logger = kvSettings.logger ?? kvSettings.loggerFactory?.CreateLogger($"{typeof(TAllocator).Name} ReadCache");
                allocatorSettings.evictCallback = ReadCacheEvict;
                readcache = allocatorFactory(allocatorSettings, storeFunctions);
                readcacheBase = readcache.GetBase<TAllocator>();
                readcacheBase.Initialize();
                ReadCache = new(this, readcache);
            }

            RevivificationManager = new(this, isFixedLenReviv, kvSettings.RevivificationSettings, logSettings);

            systemState = SystemState.Make(Phase.REST, 1);

            if (kvSettings.TryRecoverLatest)
            {
                try
                {
                    Recover();
                }
                catch { }
            }
        }

        /// <summary>Get the hashcode for a key.</summary>
        public long GetKeyHash(TKey key) => storeFunctions.GetKeyHashCode64(ref key);

        /// <summary>Get the hashcode for a key.</summary>
        public long GetKeyHash(ref TKey key) => storeFunctions.GetKeyHashCode64(ref key);

        /// <summary>
        /// Grow the hash index by a factor of two. Caller should take a full checkpoint after growth, for persistence.
        /// </summary>
        /// <returns>Whether the grow completed</returns>
        public bool GrowIndex()
        {
            if (Kernel.Epoch.ThisInstanceProtected())
                throw new TsavoriteException("Cannot use GrowIndex when using non-async sessions");

            if (!StartStateMachine(new IndexResizeStateMachine<TKey, TValue, TStoreFunctions, TAllocator>()))
                return false;

            Kernel.Epoch.Resume();

            try
            {
                while (true)
                {
                    var _systemState = SystemState.Copy(ref systemState);
                    if (_systemState.Phase == Phase.PREPARE_GROW)
                        ThreadStateMachineStep<Empty, Empty, Empty>(null, default);
                    else if (_systemState.Phase == Phase.IN_PROGRESS_GROW)
                        SplitBuckets(0);
                    else if (_systemState.Phase == Phase.REST)
                        break;
                    Kernel.Epoch.ProtectAndDrain();
                    _ = Thread.Yield();
                }
            }
            finally
            {
                Kernel.Epoch.Suspend();
            }
            return true;
        }

        /// <summary>
        /// Dispose Tsavorite instance
        /// </summary>
        public void Dispose()
        {
            Free();
            hlogBase.Dispose();
            readcacheBase?.Dispose();
            LockTable.Dispose();
            _lastSnapshotCheckpoint.Dispose();
            if (disposeCheckpointManager)
                checkpointManager?.Dispose();
            RevivificationManager.Dispose();
        }

        /// <summary>
        /// Total number of valid entries in hash table
        /// </summary>
        /// <returns></returns>
        private unsafe long GetEntryCount()
        {
            var version = Kernel.hashTable.spine.resizeInfo.version;
            var table_size_ = Kernel.hashTable.spine.state[version].size;
            var ptable_ = Kernel.hashTable.spine.state[version].tableAligned;
            long total_entry_count = 0;
            long beginAddress = hlogBase.BeginAddress;

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                        if (b.bucket_entries[bucket_entry] >= beginAddress)
                            ++total_entry_count;
                    if ((b.bucket_entries[Constants.kOverflowBucketIndex] & HashBucketEntry.kAddressMask) == 0) break;
                    b = *(HashBucket*)Kernel.hashTable.overflowBucketsAllocator.GetPhysicalAddress(b.bucket_entries[Constants.kOverflowBucketIndex] & HashBucketEntry.kAddressMask);
                }
            }
            return total_entry_count;
        }

        private unsafe string DumpDistributionInternal(int version)
        {
            var table_size_ = Kernel.hashTable.spine.state[version].size;
            var ptable_ = Kernel.hashTable.spine.state[version].tableAligned;
            long total_record_count = 0;
            long beginAddress = hlogBase.BeginAddress;
            Dictionary<int, long> histogram = new();

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                List<int> tags = new();
                int cnt = 0;
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                    {
                        var x = default(HashBucketEntry);
                        x.word = b.bucket_entries[bucket_entry];
                        if (((!x.ReadCache) && (x.Address >= beginAddress)) || (x.ReadCache && (x.AbsoluteAddress >= readcacheBase.HeadAddress)))
                        {
                            if (tags.Contains(x.Tag) && !x.Tentative)
                                throw new TsavoriteException("Duplicate tag found in index");
                            tags.Add(x.Tag);
                            ++cnt;
                            ++total_record_count;
                        }
                    }
                    if ((b.bucket_entries[Constants.kOverflowBucketIndex] & HashBucketEntry.kAddressMask) == 0) break;
                    b = *(HashBucket*)Kernel.hashTable.overflowBucketsAllocator.GetPhysicalAddress(b.bucket_entries[Constants.kOverflowBucketIndex] & HashBucketEntry.kAddressMask);
                }

                if (!histogram.ContainsKey(cnt)) histogram[cnt] = 0;
                histogram[cnt]++;
            }

            var distribution =
                $"Number of hash buckets: {table_size_}\n" +
                $"Number of overflow buckets: {OverflowBucketCount}\n" +
                $"Size of each bucket: {Constants.kEntriesPerBucket * sizeof(HashBucketEntry)} bytes\n" +
                $"Total distinct hash-table entry count: {{{total_record_count}}}\n" +
                $"Average #entries per hash bucket: {{{total_record_count / (double)table_size_:0.00}}}\n" +
                $"Histogram of #entries per bucket:\n";

            foreach (var kvp in histogram.OrderBy(e => e.Key))
            {
                distribution += $"  {kvp.Key} : {kvp.Value}\n";
            }

            return distribution;
        }

        /// <summary>
        /// Dumps the distribution of each non-empty bucket in the hash table.
        /// </summary>
        public string DumpDistribution()
        {
            return DumpDistributionInternal(Kernel.hashTable.spine.resizeInfo.version);
        }
    }
}