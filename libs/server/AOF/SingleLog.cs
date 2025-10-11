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

        public ref AofAddress BeginAddress
        {
            get
            {
                // Optimized for single log - no loop needed
                beginAddress[0] = log.BeginAddress;
                return ref beginAddress;
            }
        }
        AofAddress beginAddress = AofAddress.SetValue(length: 1, value: 0);

        public ref AofAddress TailAddress
        {
            get
            {
                // Optimized for single log - no loop needed
                tailAddress[0] = log.TailAddress;
                return ref tailAddress;
            }
        }
        AofAddress tailAddress = AofAddress.SetValue(length: 1, value: 0);

        public ref AofAddress CommittedUntilAddress
        {
            get
            {
                // Optimized for single log - no loop needed
                committedUntilAddress[0] = log.CommittedUntilAddress;
                return ref committedUntilAddress;
            }
        }
        AofAddress committedUntilAddress = AofAddress.SetValue(length: 1, value: 0);

        public ref AofAddress CommittedBeginAddress
        {
            get
            {
                // Optimized for single log - no loop needed
                commitedBeginnAddress[0] = log.CommittedBeginAddress;
                return ref commitedBeginnAddress;
            }
        }
        AofAddress commitedBeginnAddress = AofAddress.SetValue(length: 1, value: 0);

        public ref AofAddress FlushedUntilAddress
        {
            get
            {
                // Optimized for single log - no loop needed
                flushedUntilAddress[0] = log.FlushedUntilAddress;
                return ref flushedUntilAddress;
            }
        }
        AofAddress flushedUntilAddress = AofAddress.SetValue(length: 1, value: 0);

        public long HeaderSize => log.HeaderSize;

        public ref AofAddress MaxMemorySizeBytes
        {
            get
            {
                // Optimized for single log - no loop needed
                maxMemorySizeBytes[0] = log.MaxMemorySizeBytes;
                return ref maxMemorySizeBytes;
            }
        }
        AofAddress maxMemorySizeBytes = AofAddress.SetValue(length: 1, value: 0);

        public ref AofAddress MemorySizeBytes
        {
            get
            {
                // Optimized for single log - no loop needed
                memorySizeBytes[0] = log.MemorySizeBytes;
                return ref memorySizeBytes;
            }
        }
        AofAddress memorySizeBytes = AofAddress.SetValue(length: 1, value: 0);

        public void Recover() => log.Recover();
        public void Reset() => log.Reset();

        public void Dispose()
        {
            logSettings.LogDevice.Dispose();
            log.Dispose();
        }
    }
}