// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System.Linq;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public class ShardedLog(int sublogCount, TsavoriteLogSettings[] logSettings, ILogger logger = null)
    {
        public int Length { get; private set; } = sublogCount;
        readonly TsavoriteLogSettings[] logSettings = logSettings;
        public readonly TsavoriteLog[] sublog = [.. logSettings.Select(settings => new TsavoriteLog(settings, logger))];

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