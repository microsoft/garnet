// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Threading;
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
        internal readonly LogSizeTracker<StoreFunctions, StoreAllocator> mainLogTracker;
        internal readonly LogSizeTracker<StoreFunctions, StoreAllocator> readCacheTracker;
        private long targetSize;
        private long readCacheTargetSize;

        int isStarted = 0;
        private const int HighTargetSizeDeltaFraction = 10; // When memory usage grows, trigger trimming at 10% above target size (for both main log and readcache)
        private const int LowTargetSizeDeltaFraction = HighTargetSizeDeltaFraction * 5;  // When trimming memory, trim down to 1/5 of HighTargetSizeDeltaFraction below target size (for both main log and readcache)

        internal bool Stopped => (mainLogTracker == null || mainLogTracker.Stopped) && (readCacheTracker == null || readCacheTracker.Stopped);

        /// <summary>Total memory size target for main log</summary>
        public long TargetSize
        {
            get => targetSize;
            set
            {
                Debug.Assert(value >= 0);
                targetSize = value;
                mainLogTracker?.UpdateTargetSize(targetSize, targetSize / HighTargetSizeDeltaFraction, targetSize / LowTargetSizeDeltaFraction);
            }
        }

        /// <summary>Total memory size target for readcache</summary>
        public long ReadCacheTargetSize
        {
            get => readCacheTargetSize;
            set
            {
                Debug.Assert(value >= 0);
                readCacheTargetSize = value;
                readCacheTracker?.UpdateTargetSize(readCacheTargetSize, readCacheTargetSize / HighTargetSizeDeltaFraction, readCacheTargetSize / LowTargetSizeDeltaFraction);
            }
        }

        /// <summary>Class to track and update cache size</summary>
        /// <param name="store">Tsavorite store instance</param>
        /// <param name="targetSize">Total memory size target</param>
        /// <param name="readCacheTargetSize">Target memory size for read cache</param>
        /// <param name="loggerFactory"></param>
        public CacheSizeTracker(TsavoriteKV<StoreFunctions, StoreAllocator> store, long targetSize, long readCacheTargetSize, ILoggerFactory loggerFactory = null)
        {
            Debug.Assert(store != null);
            Debug.Assert(targetSize > 0 || readCacheTargetSize > 0);

            // Subscribe to the eviction notifications. We don't hang onto the LogSubscribeDisposable because the CacheSizeTracker is never disposed once created.
            if (targetSize > 0)
            {
                mainLogTracker = new LogSizeTracker<StoreFunctions, StoreAllocator>(store.Log, targetSize, 
                        targetSize / HighTargetSizeDeltaFraction, targetSize / LowTargetSizeDeltaFraction, loggerFactory?.CreateLogger("MainLogSizeTracker"));
                store.Log.SetLogSizeTracker(mainLogTracker);
            }

            if (store.ReadCache != null && readCacheTargetSize > 0)
            {
                readCacheTracker = new LogSizeTracker<StoreFunctions, StoreAllocator>(store.ReadCache, readCacheTargetSize, 
                        readCacheTargetSize / HighTargetSizeDeltaFraction, readCacheTargetSize / LowTargetSizeDeltaFraction, loggerFactory?.CreateLogger("ReadCacheSizeTracker"));
                store.ReadCache.SetLogSizeTracker(readCacheTracker);
            }

            TargetSize = targetSize;
            ReadCacheTargetSize = readCacheTargetSize;
        }

        /// <summary>Start the trackers, ensuring that only one thread does so</summary>
        public void Start(CancellationToken token)
        {
            // Prevent multiple calls to Start
            var prevIsStarted = Interlocked.CompareExchange(ref isStarted, 1, 0);
            if (prevIsStarted == 0)
            {
                mainLogTracker?.Start(token);
                readCacheTracker?.Start(token);
            }
        }

        /// <summary>Add to the tracked size of the cache.</summary>
        /// <param name="size">Size to be added</param>
        public void AddTrackedSize(long size)
        {
            // mainLogTracker could be null if heap size limit is set just for the read cache
            if (size != 0)
                mainLogTracker?.IncrementSize(size);
        }

        /// <summary>Add to the tracked size of read cache.</summary>
        /// <param name="size">Size to be added</param>
        public void AddReadCacheTrackedSize(long size)
        {
            // readCacheTracker could be null if read cache is not enabled or heap size limit is set only for the main log
            if (size != 0)
                readCacheTracker?.IncrementSize(size);
        }

        /// <summary>
        /// If tracker has not started, prevent it from starting
        /// </summary>
        /// <returns>True if tracker hasn't previously started</returns>
        public bool TryPreventStart()
        {
            var prevStarted = Interlocked.CompareExchange(ref isStarted, 1, 0);
            return prevStarted == 0;
        }
    }
}