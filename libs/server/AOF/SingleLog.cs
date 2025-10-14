// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using Tsavorite.core;


namespace Garnet.server
{
    public class SingleLog(TsavoriteLogSettings logSettings, ILogger logger = null)
    {
        readonly TsavoriteLogSettings logSettings = logSettings;
        public readonly TsavoriteLog log = new(logSettings, logger);

        public long HeaderSize => log.HeaderSize;        

        public AofAddress BeginAddress => AofAddress.Create(1, value: log.BeginAddress);

        public AofAddress TailAddress => AofAddress.Create(1, value: log.TailAddress);

        public AofAddress CommittedUntilAddress => AofAddress.Create(1, value: log.CommittedUntilAddress);

        public AofAddress CommittedBeginAddress => AofAddress.Create(1, value: log.CommittedBeginAddress);

        public AofAddress FlushedUntilAddress => AofAddress.Create(1, value: log.FlushedUntilAddress);

        public AofAddress MaxMemorySizeBytes => AofAddress.Create(1, value: log.MaxMemorySizeBytes);

        public AofAddress MemorySizeBytes => AofAddress.Create(1, value: log.MemorySizeBytes);

        public void Recover() => log.Recover();
        public void Reset() => log.Reset();

        public void Dispose()
        {
            logSettings.LogDevice.Dispose();
            log.Dispose();
        }
    }
}