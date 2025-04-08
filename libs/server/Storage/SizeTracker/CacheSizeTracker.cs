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
    using ObjectStoreAllocator = ObjectAllocator<StoreFunctions<SpanByteComparer, DefaultRecordDisposer>>;
    using ObjectStoreFunctions = StoreFunctions<SpanByteComparer, DefaultRecordDisposer>;

    /// <summary>
    /// Tracks the size of the main log and read cache. 
    /// Based on the current size and the target size, it uses the corresponding LogSizeTracker objects to increase
    /// or decrease memory utilization.
    /// </summary>
    public class CacheSizeTracker
    {
        internal readonly LogSizeTracker<ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator> mainLogTracker;
        internal readonly LogSizeTracker<ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator> readCacheTracker;
        public long TargetSize;
        public long ReadCacheTargetSize;

        private const int deltaFraction = 10; // 10% of target size
        private TsavoriteKV<ObjectStoreFunctions, ObjectStoreAllocator> store;

        internal bool Stopped => (mainLogTracker == null || mainLogTracker.Stopped) && (readCacheTracker == null || readCacheTracker.Stopped);

        /// <summary>Helps calculate size of a record including heap memory in Object store.</summary>
        internal struct LogSizeCalculator : ILogSizeCalculator
        {
            /// <summary>Calculate the size of a record in the cache</summary>
            /// <param name="logRecord">Information about the record</param>
            /// <returns>The size of the record</returns>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public readonly long CalculateRecordSize<TSourceLogRecord>(ref TSourceLogRecord logRecord)
                where TSourceLogRecord : ISourceLogRecord
            {
                long size = Utility.RoundUp(logRecord.Key.Length, IntPtr.Size) + MemoryUtils.ByteArrayOverhead;

                if (!logRecord.Info.Tombstone)
                { 
                    var value = logRecord.ValueObject;
                    if (value != null) // ignore deleted values being evicted (they are accounted for by ConcurrentDeleter)
                        size += value.Size;
                }
                return size;
            }
        }

        /// <summary>Class to track and update cache size</summary>
        /// <param name="store">Tsavorite store instance</param>
        /// <param name="logSettings">Hybrid log settings</param>
        /// <param name="targetSize">Total memory size target</param>
        /// <param name="readCacheTargetSize">Target memory size for read cache</param>
        /// <param name="loggerFactory"></param>
        public CacheSizeTracker(TsavoriteKV<ObjectStoreFunctions, ObjectStoreAllocator> store, KVSettings logSettings,
                long targetSize, long readCacheTargetSize, ILoggerFactory loggerFactory = null)
        {
            Debug.Assert(store != null);
            Debug.Assert(logSettings != null);
            Debug.Assert(targetSize > 0 || readCacheTargetSize > 0);

            this.store = store;
            this.TargetSize = targetSize;
            this.ReadCacheTargetSize = readCacheTargetSize;
            var logSizeCalculator = new LogSizeCalculator();

            if (targetSize > 0)
            {
                this.mainLogTracker = new LogSizeTracker<ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(store.Log, logSizeCalculator,
                    targetSize, targetSize / deltaFraction, loggerFactory?.CreateLogger("ObjSizeTracker"));
                store.Log.SubscribeEvictions(mainLogTracker);
                store.Log.SubscribeDeserializations(new LogOperationObserver<ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(mainLogTracker, LogOperationType.Deserialize));
                store.Log.IsSizeBeyondLimit = () => mainLogTracker.IsSizeBeyondLimit;
            }

            if (store.ReadCache != null && readCacheTargetSize > 0)
            {
                this.readCacheTracker = new LogSizeTracker<ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(store.ReadCache, logSizeCalculator,
                    readCacheTargetSize, readCacheTargetSize / deltaFraction, loggerFactory?.CreateLogger("ObjReadCacheSizeTracker"));
                store.ReadCache.SubscribeEvictions(readCacheTracker);
                store.ReadCache.SubscribeDeserializations(new LogOperationObserver<ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(readCacheTracker, LogOperationType.Deserialize));
                store.ReadCache.IsSizeBeyondLimit = () => readCacheTracker.IsSizeBeyondLimit;
            }
        }

        public void Start(CancellationToken token)
        {
            mainLogTracker?.Start(token);
            readCacheTracker?.Start(token);
        }

        /// <summary>Add to the tracked size of the cache.</summary>
        /// <param name="size">Size to be added</param>
        public void AddTrackedSize(long size)
        {
            if (size == 0) return;

            // mainLogTracker could be null if heap size limit is set just for the read cache
            this.mainLogTracker?.IncrementSize(size);
        }

        /// <summary>Add to the tracked size of read cache.</summary>
        /// <param name="size">Size to be added</param>
        public void AddReadCacheTrackedSize(long size)
        {
            if (size == 0) return;

            // readCacheTracker could be null if read cache is not enabled or heap size limit is set
            // just for the main log
            this.readCacheTracker?.IncrementSize(size);
        }
    }
}