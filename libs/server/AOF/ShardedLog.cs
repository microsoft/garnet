using Tsavorite.core;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Garnet.server
{
    public class ShardedLog(int sublogCount, TsavoriteLogSettings[] logSettings, ILogger logger = null)
    {
        public int Length { get; private set; } = sublogCount;
        readonly TsavoriteLogSettings[] logSettings = logSettings;
        public readonly TsavoriteLog[] sublog = [.. logSettings.Select(settings => new TsavoriteLog(settings, logger))];

        public ref AofAddress BeginAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    beginAddress[i] = sublog[i].BeginAddress;
                return ref beginAddress;
            }
        }
        AofAddress beginAddress = AofAddress.SetValue(length: sublogCount, value: 0);

        public ref AofAddress TailAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    tailAddress[i] = sublog[i].TailAddress;
                return ref tailAddress;
            }
        }
        AofAddress tailAddress = AofAddress.SetValue(length: sublogCount, value: 0);

        public ref AofAddress CommittedUntilAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    committedUntilAddress[i] = sublog[i].CommittedUntilAddress;
                return ref committedUntilAddress;
            }
        }
        AofAddress committedUntilAddress = AofAddress.SetValue(length: sublogCount, value: 0);

        public ref AofAddress CommittedBeginAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    commitedBeginnAddress[i] = sublog[i].CommittedBeginAddress;
                return ref commitedBeginnAddress;
            }
        }
        AofAddress commitedBeginnAddress = AofAddress.SetValue(length: sublogCount, value: 0);

        public ref AofAddress FlushedUntilAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    flushedUntilAddress[i] = sublog[i].FlushedUntilAddress;
                return ref flushedUntilAddress;
            }
        }
        AofAddress flushedUntilAddress = AofAddress.SetValue(length: sublogCount, value: 0);

        public long HeaderSize => sublog[0].HeaderSize;

        public ref AofAddress MaxMemorySizeBytes
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    maxMemorySizeBytes[i] = sublog[i].MaxMemorySizeBytes;
                return ref maxMemorySizeBytes;
            }
        }
        AofAddress maxMemorySizeBytes = AofAddress.SetValue(length: sublogCount, value: 0);

        public ref AofAddress MemorySizeBytes
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    memorySizeBytes[i] = sublog[i].MemorySizeBytes;
                return ref memorySizeBytes;
            }
        }
        AofAddress memorySizeBytes = AofAddress.SetValue(length: sublogCount, value: 0);

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