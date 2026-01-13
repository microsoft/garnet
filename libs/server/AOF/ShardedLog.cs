// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Linq;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public class ShardedLog(int physicalSublogCount, TsavoriteLogSettings[] logSettings, ILogger logger = null)
    {
        /// <summary>
        /// Number of physical sublogs
        /// </summary>
        public int Length { get; private set; } = physicalSublogCount;
        readonly TsavoriteLogSettings[] logSettings = logSettings;
        
        /// <summary>
        /// Physical sublog instances
        /// </summary>
        public readonly TsavoriteLog[] sublog = [.. logSettings.Select(settings => new TsavoriteLog(settings, logger))];

        /// <summary>
        /// Distinct locks per sublog instance
        /// </summary>
        public readonly SingleWriterMultiReaderLock[] logLocks = [.. Enumerable.Range(0, physicalSublogCount).Select(_ => new SingleWriterMultiReaderLock())];

        ulong lockMap = 0;

        public void LockSublogs(ulong logAccessBitmap)
        {
            while (true)
            {
                Thread.Yield();
                var currentLockMap = lockMap;
                var newLockMap = currentLockMap | logAccessBitmap;
                if (Interlocked.CompareExchange(ref lockMap, newLockMap, currentLockMap) == currentLockMap)
                    break;
            }
        }

        public void UnlockSublogs(ulong logAccessBitmap)
        {
            Debug.Assert((lockMap & logAccessBitmap) > 0);
            logAccessBitmap = ~logAccessBitmap;
            while (true)
            {
                Thread.Yield();
                var currentLockMap = lockMap;
                var newLockMap = currentLockMap & logAccessBitmap;
                if (Interlocked.CompareExchange(ref lockMap, newLockMap, currentLockMap) == currentLockMap)
                    break;
            }
        }

        public AofAddress BeginAddress
        {
            get
            {
                var result = AofAddress.Create(Length, 0);
                for (var i = 0; i < sublog.Length; i++)
                    result[i] = sublog[i].BeginAddress;
                return result;
            }
        }

        public AofAddress TailAddress
        {
            get
            {
                var result = AofAddress.Create(Length, 0);
                for (var i = 0; i < sublog.Length; i++)
                    result[i] = sublog[i].TailAddress;
                return result;
            }
        }

        public AofAddress CommittedUntilAddress
        {
            get
            {
                var result = AofAddress.Create(Length, 0);
                for (var i = 0; i < sublog.Length; i++)
                    result[i] = sublog[i].CommittedUntilAddress;
                return result;
            }
        }

        public AofAddress CommittedBeginAddress
        {
            get
            {
                var result = AofAddress.Create(Length, 0);
                for (var i = 0; i < sublog.Length; i++)
                    result[i] = sublog[i].CommittedBeginAddress;
                return result;
            }
        }

        public AofAddress FlushedUntilAddress
        {
            get
            {
                var result = AofAddress.Create(Length, 0);
                for (var i = 0; i < sublog.Length; i++)
                    result[i] = sublog[i].FlushedUntilAddress;
                return result;
            }
        }

        public long HeaderSize => sublog[0].HeaderSize;

        public AofAddress MaxMemorySizeBytes
        {
            get
            {
                var result = AofAddress.Create(Length, 0);
                for (var i = 0; i < sublog.Length; i++)
                    result[i] = sublog[i].MaxMemorySizeBytes;
                return result;
            }
        }

        public AofAddress MemorySizeBytes
        {
            get
            {
                var result = AofAddress.Create(Length, 0);
                for (var i = 0; i < sublog.Length; i++)
                    result[i] = sublog[i].MemorySizeBytes;
                return result;
            }
        }

        public void Recover()
        {
            foreach (var log in sublog)
                log.Recover();
        }

        public void Reset()
        {
            foreach (var log in sublog)
                log.Reset();
        }

        public void Dispose()
        {
            for (var i = 0; i < sublog.Length; i++)
            {
                logSettings[i].LogDevice.Dispose();
                sublog[i].Dispose();
            }
        }

        public void Initialize(in AofAddress beginAddress, in AofAddress committedUntilAddress, long lastCommitNum = 0)
        {
            for (var i = 0; i < sublog.Length; i++)
                sublog[i].Initialize(beginAddress[i], committedUntilAddress[i], lastCommitNum);
        }
    }
}