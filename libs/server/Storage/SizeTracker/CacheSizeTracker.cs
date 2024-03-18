// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    /// <summary>
    /// Tracks the size of the main log and read cache. 
    /// Based on the current size and the target size, it uses the corresponding LogSizeTracker objects to increase
    /// or decrease memory utilization.
    /// </summary>
    public class CacheSizeTracker
    {
        internal readonly LogSizeTracker<byte[], IGarnetObject, LogSizeCalculator> mainLogTracker;
        internal readonly LogSizeTracker<byte[], IGarnetObject, LogSizeCalculator> readCacheTracker;
        internal long targetSize;

        private const int deltaFraction = 10; // 10% of target size
        private TsavoriteKV<byte[], IGarnetObject> store;

        internal long IndexSizeBytes => store.IndexSize * 64 + store.OverflowBucketCount * 64;

        /// <summary>Helps calculate size of a record including heap memory in Object store.</summary>
        internal struct LogSizeCalculator : ILogSizeCalculator<byte[], IGarnetObject>
        {
            /// <summary>Calculate the size of a record in the cache</summary>
            /// <param name="recordInfo">Information about the record</param>
            /// <param name="key">The record's key</param>
            /// <param name="value">The record's value</param>
            /// <returns>The size of the record</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public long CalculateRecordSize(RecordInfo recordInfo, byte[] key, IGarnetObject value)
            {
                long size = Utility.RoundUp(key.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead;

                if (!recordInfo.Tombstone) // ignore deleted values being evicted (they are accounted for by ConcurrentDeleter)
                    size += value.Size;

                return size;
            }
        }

        /// <summary>Class to track and update cache size</summary>
        /// <param name="store">FASTER store instance</param>
        /// <param name="logSettings">Hybrid log settings</param>
        /// <param name="targetSize">Total memory size target</param>
        /// <param name="loggerFactory"></param>
        public CacheSizeTracker(TsavoriteKV<byte[], IGarnetObject> store, LogSettings logSettings, long targetSize, ILoggerFactory loggerFactory = null)
        {
            Debug.Assert(store != null);
            Debug.Assert(logSettings != null);
            Debug.Assert(targetSize > 0);

            this.store = store;
            var logSizeCalculator = new LogSizeCalculator();

            var (mainLogTargetSizeBytes, readCacheTargetSizeBytes) = CalculateLogTargetSizeBytes(targetSize);

            this.mainLogTracker = new LogSizeTracker<byte[], IGarnetObject, LogSizeCalculator>(store.Log, logSizeCalculator,
                mainLogTargetSizeBytes, mainLogTargetSizeBytes / deltaFraction, loggerFactory?.CreateLogger("ObjSizeTracker"));
            store.Log.SubscribeEvictions(mainLogTracker);

            if (store.ReadCache != null)
            {
                this.readCacheTracker = new LogSizeTracker<byte[], IGarnetObject, LogSizeCalculator>(store.ReadCache, logSizeCalculator,
                    readCacheTargetSizeBytes, readCacheTargetSizeBytes / deltaFraction, loggerFactory?.CreateLogger("ObjReadCacheSizeTracker"));
                store.ReadCache.SubscribeEvictions(readCacheTracker);
            }
        }

        /// <summary>
        /// Calculates target size for both the main cache and read cache.
        /// Target size will be checked against the total size of the index, log pages and heap memory size.
        /// For now, main cache and read cache are allocated equal size. If needed, this could be driven by a configurable setting.
        /// </summary>
        /// <param name="newTargetSize">Target size</param>
        public (long mainLogSizeBytes, long readCacheSizeBytes) CalculateLogTargetSizeBytes(long newTargetSize)
        {
            long residual = newTargetSize - IndexSizeBytes;

            if (residual <= 0)
                throw new TsavoriteException($"Target size {newTargetSize} must be larger than index size {IndexSizeBytes}");

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