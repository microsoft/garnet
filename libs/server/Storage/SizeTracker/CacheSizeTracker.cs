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
        private long targetSize;
        public long ReadCacheTargetSize;

        int isStarted = 0;
        private const int deltaFraction = 10; // 10% of target size

        internal bool Stopped => (mainLogTracker == null || mainLogTracker.Stopped) && (readCacheTracker == null || readCacheTracker.Stopped);

        /// <summary>
        /// Total memory size target
        /// </summary>
        public long TargetSize
        {
            get => targetSize;
            set
            {
                Debug.Assert(value >= 0);
                targetSize = value;
                mainLogTracker?.UpdateTargetSize(targetSize, targetSize / deltaFraction);
            }
        }

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

                if (!recordInfo.Tombstone && value != null) // ignore deleted values being evicted (they are accounted for by ConcurrentDeleter)
                    size += value.Size;

                return size;
            }
        }

        /// <summary>Class to track and update cache size</summary>
        /// <param name="store">Tsavorite store instance</param>
        /// <param name="logSettings">Hybrid log settings</param>
        /// <param name="targetSize">Total memory size target</param>
        /// <param name="readCacheTargetSize">Target memory size for read cache</param>
        /// <param name="loggerFactory"></param>
        public CacheSizeTracker(TsavoriteKV<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator> store, KVSettings<byte[], IGarnetObject> logSettings,
                long targetSize, long readCacheTargetSize, ILoggerFactory loggerFactory = null)
        {
            Debug.Assert(store != null);
            Debug.Assert(logSettings != null);
            Debug.Assert(targetSize > 0 || readCacheTargetSize > 0);

            this.TargetSize = targetSize;
            this.ReadCacheTargetSize = readCacheTargetSize;
            var logSizeCalculator = new LogSizeCalculator();

            if (targetSize > 0)
            {
                this.mainLogTracker = new LogSizeTracker<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(store.Log, logSizeCalculator,
                    targetSize, targetSize / deltaFraction, loggerFactory?.CreateLogger("ObjSizeTracker"));
                store.Log.SubscribeEvictions(mainLogTracker);
                store.Log.SubscribeDeserializations(new LogOperationObserver<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(mainLogTracker, LogOperationType.Deserialize));
                store.Log.IsSizeBeyondLimit = () => mainLogTracker.IsSizeBeyondLimit;
            }

            if (store.ReadCache != null && readCacheTargetSize > 0)
            {
                this.readCacheTracker = new LogSizeTracker<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(store.ReadCache, logSizeCalculator,
                    readCacheTargetSize, readCacheTargetSize / deltaFraction, loggerFactory?.CreateLogger("ObjReadCacheSizeTracker"));
                store.ReadCache.SubscribeEvictions(readCacheTracker);
                store.ReadCache.SubscribeDeserializations(new LogOperationObserver<byte[], IGarnetObject, ObjectStoreFunctions, ObjectStoreAllocator, LogSizeCalculator>(readCacheTracker, LogOperationType.Deserialize));
                store.ReadCache.IsSizeBeyondLimit = () => readCacheTracker.IsSizeBeyondLimit;
            }
        }

        public void Start(CancellationToken token)
        {
            // Prevent multiple calls to Start
            var prevIsStarted = Interlocked.CompareExchange(ref isStarted, 1, 0);
            if (prevIsStarted == 1) return;

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