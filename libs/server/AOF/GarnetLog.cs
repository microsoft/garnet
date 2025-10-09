// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using Tsavorite.core;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Threading;
using Garnet.common;

namespace Garnet.server
{
    public class GarnetLog(GarnetServerOptions serverOptions, TsavoriteLogSettings[] logSettings, ILogger logger = null)
    {
        const long kFirstValidAofAddress = 64;

        readonly SingleLog singleLog = serverOptions.AofSublogCount == 1 ? new SingleLog(logSettings[0], logger) : null;
        readonly ShardedLog shardedLog = serverOptions.AofSublogCount > 1 ? new ShardedLog(serverOptions.AofSublogCount, logSettings, logger) : null;

        public long HeaderSize => singleLog != null ? singleLog.HeaderSize : shardedLog.HeaderSize;

        public static long Hash(ref SpanByte key)
            => (long)HashSlotUtils.Hash(key.AsSpan());

        public ref AofAddress BeginAddress
        {
            get
            {
                if (singleLog != null)
                    return ref singleLog.BeginAddress;
                return ref shardedLog.BeginAddress;
            }
        }

        public ref AofAddress TailAddress
        {
            get
            {
                if (singleLog != null)
                    return ref singleLog.TailAddress;
                return ref shardedLog.TailAddress;
            }
        }

        public ref AofAddress CommittedUntilAddress
        {
            get
            {
                if (singleLog != null)
                    return ref singleLog.CommittedUntilAddress;
                return ref shardedLog.CommittedUntilAddress;
            }
        }

        public ref AofAddress CommittedBeginAddress
        {
            get
            {
                if (singleLog != null)
                    return ref singleLog.CommittedBeginAddress;
                return ref shardedLog.CommittedBeginAddress;
            }
        }

        public ref AofAddress FlushedUntilAddress
        {
            get
            {
                if (singleLog != null)
                    return ref singleLog.FlushedUntilAddress;
                return ref shardedLog.FlushedUntilAddress;
            }
        }

        public ref AofAddress MaxMemorySizeBytes
        {
            get
            {
                if (singleLog != null)
                    return ref singleLog.MaxMemorySizeBytes;
                return ref shardedLog.MaxMemorySizeBytes;
            }
        }

        public ref AofAddress MemorySizeBytes
        {
            get
            {
                if (singleLog != null)
                    return ref singleLog.MemorySizeBytes;
                return ref shardedLog.MemorySizeBytes;
            }
        }

        public void Recover()
        {
            if (singleLog != null)
                singleLog.Recover();
            else
                shardedLog.Recover();
        }

        public void Reset()
        {
            if (singleLog != null)
                singleLog.Reset();
            else
                shardedLog.Reset();
        }

        public void Dispose()
        {
            if (singleLog != null)
                singleLog.Dispose();
            else
                shardedLog.Dispose();
        }

        public TsavoriteLog GetSubLog(int sublogIdx)
        {
            if (singleLog != null)
            {
                Debug.Assert(sublogIdx == 0);
                return singleLog.log;
            }
            else
            {
                Debug.Assert(sublogIdx < shardedLog.Length);
                return shardedLog.sublog[sublogIdx];
            }
        }

        public TsavoriteLog GetSubLog(ref SpanByte key)
        {
            if (singleLog != null)
                return singleLog.log;

            var hash = Hash(ref key);
            return shardedLog.sublog[hash % shardedLog.Length];
        }

        public unsafe int UnsafeGetLength(byte* headerPtr)
            => GetSubLog(0).UnsafeGetLength(headerPtr);

        public int UnsafeGetLogPageSizeBits()
            => GetSubLog(0).UnsafeGetLogPageSizeBits();

        // FIXME: Is this safe given that ReadOnlyAddressLagOffset takes a lock
        // but also in the old code it was only initialized onece
        public long UnsafeGetReadOnlyAddressLagOffset()
            => GetSubLog(0).UnsafeGetReadOnlyAddressLagOffset();

        public void InitializeIf(ref AofAddress recoveredSafeAofAddress)
        {
            if (singleLog != null)
            {
                if (TailAddress[0] < recoveredSafeAofAddress[0])
                    singleLog.log.Initialize(TailAddress[0], recoveredSafeAofAddress[0]);
            }
            else
            {
                for (var i = 0; i < shardedLog.Length; i++)
                    if (TailAddress[i] < recoveredSafeAofAddress[i])
                        shardedLog.sublog[i].Initialize(TailAddress[i], recoveredSafeAofAddress[i]);
            }
        }

        public void Initialize(in AofAddress beginAddress, in AofAddress committedUntilAddress, long lastCommitNum = 0)
        {
            if (singleLog != null)
            {
                Debug.Assert(beginAddress.Length == 1);
                singleLog.log.Initialize(beginAddress[0], committedUntilAddress[0], lastCommitNum);
            }
            else
            {
                Debug.Assert(beginAddress.Length == shardedLog.Length);
                for (var i = 0; i < shardedLog.Length; i++)
                    shardedLog.sublog[i].Initialize(beginAddress[i], committedUntilAddress[i], lastCommitNum);
            }
        }

        // FIXME: should we pass AofAddress here?
        public void WaitForCommit(long untilAddress = 0, long commitNum = -1)
        {
            if (singleLog != null)
            {
                singleLog.log.WaitForCommit(untilAddress, commitNum);
            }
            else
            {
                for (var i = 0; i < shardedLog.Length; i++)
                    shardedLog.sublog[i].WaitForCommit(untilAddress, commitNum);
            }
        }

        public void Commit(bool spinWait = false, byte[] cookie = null)
        {
            if (singleLog != null)
            {
                singleLog.log.Commit(spinWait, cookie);
            }
            else
            {
                for (var i = 0; i < shardedLog.Length; i++)
                    shardedLog.sublog[i].Commit(spinWait, cookie);
            }
        }

        public async ValueTask CommitAsync(byte[] cookie = null, CancellationToken token = default)
        {
            if (singleLog != null)
            {
                // Optimization for single log case
                await singleLog.log.CommitAsync(cookie, token);
                return;
            }

            // Create tasks for all sublogs
            var tasks = new ValueTask[shardedLog.Length];
            for (var i = 0; i < shardedLog.Length; i++)
                tasks[i] = shardedLog.sublog[i].CommitAsync(cookie, token);

            // Wait for all commits to complete
            for (var i = 0; i < tasks.Length; i++)
                await tasks[i];
        }

        public async ValueTask WaitForCommitAsync(long untilAddress = 0, long commitNum = -1, CancellationToken token = default)
        {
            if (singleLog != null)
            {
                // Optimization for single log case
                await singleLog.log.WaitForCommitAsync(untilAddress, commitNum, token);
                return;
            }

            // Create tasks for all sublogs
            var tasks = new ValueTask[shardedLog.Length];
            for (var i = 0; i < shardedLog.Length; i++)
                tasks[i] = shardedLog.sublog[i].WaitForCommitAsync(untilAddress, commitNum, token);

            // Wait for all commits to complete
            for (var i = 0; i < tasks.Length; i++)
                await tasks[i];
        }

        public void UnsafeShiftBeginAddress(AofAddress untilAddress, bool snapToPageStart = false, bool truncateLog = false)
        {
            if (singleLog != null)
            {
                singleLog.log.UnsafeShiftBeginAddress(untilAddress[0], snapToPageStart, truncateLog);
            }
            else
            {
                for (var i = 0; i < shardedLog.Length; i++)
                    shardedLog.sublog[i].UnsafeShiftBeginAddress(untilAddress[i], snapToPageStart, truncateLog);
            }
        }

        public void TruncateUntil(AofAddress untilAddress)
        {
            if (singleLog != null)
            {
                singleLog.log.TruncateUntil(untilAddress[0]);
            }
            else
            {
                for (var i = 0; i < shardedLog.Length; i++)
                    shardedLog.sublog[i].TruncateUntil(untilAddress[i]);
            }
        }
    }
}