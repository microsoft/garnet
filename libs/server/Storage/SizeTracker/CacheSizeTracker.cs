// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    using ObjectStoreAllocator = GenericAllocator<byte[], IGarnetObject, StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>>;
    using ObjectStoreFunctions = StoreFunctions<byte[], IGarnetObject, ByteArrayKeyComparer, DefaultRecordDisposer<byte[], IGarnetObject>>;

    /// <summary>
    /// Tracks the size of the main log and read cache. 
    /// Based on the current size and the target size, it uses the corresponding LogSizeTracker objects to increase
    /// or decrease memory utilization.
    /// </summary>
    public class CacheSizeTracker
    {
        internal readonly LogSizeTracker<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator> mainLogTracker;
        internal readonly LogSizeTracker<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator> readCacheTracker;
        internal long targetSize;

        private const int deltaFraction = 10; // 10% of target size
        private TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> store;

        internal long IndexSizeBytes => store.IndexSize * 64 + store.OverflowBucketCount * 64;

        internal bool Stopped => mainLogTracker.Stopped && (readCacheTracker == null || readCacheTracker.Stopped);

        /// <summary>Helps calculate size of a record including heap memory in Object store.</summary>
        internal struct LogSizeCalculator : ILogSizeCalculator<byte[], IGarnetObject>
        {
            /// <summary>Calculate the size of a record in the cache</summary>
            /// <param name="recordInfo">Information about the record</param>
            /// <param name="key">The record's key</param>
            /// <param name="value">The record's value</param>
            /// <returns>The size of the record</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly long CalculateRecordSize(RecordInfo recordInfo, byte[] key, IGarnetObject value)
            {
                long size = Utility.RoundUp(key.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead;

                if (!recordInfo.Tombstone) // ignore deleted values being evicted (they are accounted for by ConcurrentDeleter)
                    size += value.Size;

                return size;
            }
        }

        /// <summary>Class to track and update cache size</summary>
        /// <param name="store">Tsavorite store instance</param>
        /// <param name="logSettings">Hybrid log settings</param>
        /// <param name="targetSize">Total memory size target</param>
        /// <param name="loggerFactory"></param>
        public CacheSizeTracker(TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> store, KVSettings<byte[], IGarnetObject> logSettings,
                long targetSize, ILoggerFactory loggerFactory = null)
        {
            Debug.Assert(store != null);
            Debug.Assert(logSettings != null);
            Debug.Assert(targetSize > 0);

            this.store = store;
            var logSizeCalculator = new LogSizeCalculator();

            var (mainLogTargetSizeBytes, readCacheTargetSizeBytes) = CalculateLogTargetSizeBytes(targetSize);

            this.mainLogTracker = new LogSizeTracker<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(store.Log, logSizeCalculator,
                mainLogTargetSizeBytes, mainLogTargetSizeBytes / deltaFraction, loggerFactory?.CreateLogger("ObjSizeTracker"));
            store.Log.SubscribeEvictions(mainLogTracker);
            store.Log.SubscribeDeserializations(new LogOperationObserver<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(mainLogTracker, LogOperationType.Deserialize));
            store.Log.IsSizeBeyondLimit = () => mainLogTracker.IsSizeBeyondLimit;

            if (store.ReadCache != null)
            {
                this.readCacheTracker = new LogSizeTracker<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(store.ReadCache, logSizeCalculator,
                    readCacheTargetSizeBytes, readCacheTargetSizeBytes / deltaFraction, loggerFactory?.CreateLogger("ObjReadCacheSizeTracker"));
                store.ReadCache.SubscribeEvictions(readCacheTracker);
            }
        }

        public void Start(CancellationToken token)
        {
            mainLogTracker.Start(token);
            readCacheTracker?.Start(token);
        }

        /// <summary>
        /// Calculates target size for both the main cache and read cache.
        /// Target size will be checked against the total size of the log pages and heap memory size. Index is now separated out to Kernel so is not included.
        /// For now, main cache and read cache are allocated equal size. If needed, this could be driven by a configurable setting.
        /// </summary>
        /// <param name="newTargetSize">Target size</param>
        public (long mainLogSizeBytes, long readCacheSizeBytes) CalculateLogTargetSizeBytes(long newTargetSize)
        {
            long residual = newTargetSize;

            var mainLogSizeBytes = this.store.ReadCache == null ? residual : residual / 2;
            var readCacheSizeBytes = this.store.ReadCache == null ? 0 : residual / 2;

            return (mainLogSizeBytes, readCacheSizeBytes);
        }

        /// <summary>Add to the tracked size of the cache.</summary>
        /// <param name="size">Size to be added</param>
        public void AddTrackedSize(long size)
        {
            if (size == 0) return;

            this.mainLogTracker.IncrementSize(size);
        }

        /// <summary>Add to the tracked size of read cache.</summary>
        /// <param name="size">Size to be added</param>
        public void AddReadCacheTrackedSize(long size)
        {
            if (size == 0) return;

            this.readCacheTracker.IncrementSize(size);
        }
    }
}