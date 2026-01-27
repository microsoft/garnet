// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed class GarnetLog
    {
        readonly GarnetServerOptions serverOptions;
        readonly int replayTaskCount;
        readonly SingleLog singleLog;
        readonly ShardedLog shardedLog;

        /// <summary>
        /// Initializes a new GarnetLog instance with the specified configuration.
        /// </summary>
        /// <param name="serverOptions">Server configuration determining log mode and sharding.</param>
        /// <param name="logSettings">Settings for the underlying log(s).</param>
        /// <param name="logger">Optional logger for recording events.</param>
        public GarnetLog(GarnetServerOptions serverOptions, TsavoriteLogSettings[] logSettings, ILogger logger = null)
        {
            Debug.Assert(serverOptions.EnableFastCommit || serverOptions.AofPhysicalSublogCount == 1, "Cannot use sharded-log without FastCommit!");
            this.serverOptions = serverOptions;
            this.replayTaskCount = serverOptions.AofReplayTaskCount;
            this.singleLog = serverOptions.AofVirtualSublogCount == 1 ? new SingleLog(logSettings[0], logger) : null;
            this.shardedLog = serverOptions.MultiLogEnabled ? new ShardedLog(serverOptions.AofPhysicalSublogCount, logSettings, logger) : null;
        }

        public TsavoriteLog SigleLog => singleLog.log;
        public long HeaderSize => singleLog != null ? singleLog.HeaderSize : shardedLog.HeaderSize;
        public int Size => singleLog != null ? 1 : shardedLog.Length;
        public int ReplayTaskCount => replayTaskCount;

        /// <summary>
        /// Hash function used for sharded-log
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long HASH(ReadOnlySpan<byte> key)
            => (long)HashUtils.MurmurHash2x64A(key) & long.MaxValue;

        public AofAddress BeginAddress
        {
            get
            {
                if (singleLog != null)
                    return singleLog.BeginAddress;
                return shardedLog.BeginAddress;
            }
        }

        public AofAddress TailAddress
        {
            get
            {
                if (singleLog != null)
                    return singleLog.TailAddress;
                return shardedLog.TailAddress;
            }
        }

        public AofAddress CommittedUntilAddress
        {
            get
            {
                if (singleLog != null)
                    return singleLog.CommittedUntilAddress;
                return shardedLog.CommittedUntilAddress;
            }
        }

        public AofAddress CommittedBeginAddress
        {
            get
            {
                if (singleLog != null)
                    return singleLog.CommittedBeginAddress;
                return shardedLog.CommittedBeginAddress;
            }
        }

        public AofAddress FlushedUntilAddress
        {
            get
            {
                if (singleLog != null)
                    return singleLog.FlushedUntilAddress;
                return shardedLog.FlushedUntilAddress;
            }
        }

        public AofAddress MaxMemorySizeBytes
        {
            get
            {
                if (singleLog != null)
                    return singleLog.MaxMemorySizeBytes;
                return shardedLog.MaxMemorySizeBytes;
            }
        }

        public AofAddress MemorySizeBytes
        {
            get
            {
                if (singleLog != null)
                    return singleLog.MemorySizeBytes;
                return shardedLog.MemorySizeBytes;
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

        /// <summary>
        /// Get a bitmap having all bits set according to the total number of sublogs being used
        /// </summary>
        /// <returns></returns>
        public ulong AllLogsBitmask() => (ulong)((1L << Size) - 1);

        /// <summary>
        /// Lock sublogs for enqueue operation (bits indicate sublogIdx)
        /// NOTE: Slow; should be used sparingly 
        /// </summary>
        /// <param name="logAccessBitmap"></param>
        public void LockSublogs(ulong logAccessBitmap)
        {
            Debug.Assert(serverOptions != null);
            Debug.Assert(BitOperations.PopCount(logAccessBitmap) <= shardedLog.Length);
            shardedLog.LockSublogs(logAccessBitmap);
        }

        /// <summary>
        /// Unlock sublogs using the provided logAccessBitmap (bits indicate sublogIdx)
        /// </summary>
        /// <param name="logAccessBitmap"></param>
        public void UnlockSublogs(ulong logAccessBitmap)
        {
            Debug.Assert(shardedLog != null);
            Debug.Assert(BitOperations.PopCount(logAccessBitmap) <= shardedLog.Length);
            shardedLog.UnlockSublogs(logAccessBitmap);
        }

        /// <summary>
        /// Get sublog instance indicated by the provided index
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Get log page size bits
        /// TODO: Is this initialized only once? Is it same across sublogs?
        /// </summary>
        /// <returns></returns>
        public int UnsafeGetLogPageSizeBits()
            => GetSubLog(0).UnsafeGetLogPageSizeBits();

        /// <summary>
        /// Get read only address lag offset
        /// TODO: Is this initialized only once? Is it same across sublogs?
        /// </summary>
        /// <returns></returns>
        public long UnsafeGetReadOnlyAddressLagOffset()
            => GetSubLog(0).UnsafeGetReadOnlyAddressLagOffset();

        /// <summary>
        /// Conditional initialize
        /// </summary>
        /// <param name="recoveredSafeAofAddress"></param>
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

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="committedUntilAddress"></param>
        /// <param name="lastCommitNum"></param>
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

        /// <summary>
        /// TODO: should we pass an AofAddress vector?
        /// </summary>
        /// <param name="untilAddress"></param>
        /// <param name="commitNum"></param>
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

        /// <summary>
        /// Commit all physical sublogs
        /// </summary>
        /// <param name="spinWait"></param>
        /// <param name="cookie"></param>
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

        /// <summary>
        /// Commit async all physical sublogs
        /// </summary>
        /// <param name="cookie"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask CommitAsync(byte[] cookie = null, CancellationToken token = default)
        {
            if (singleLog != null)
            {
                // Optimization for single log case
                await singleLog.log.CommitAsync(cookie, token);
            }
            else
            {
                // Create tasks for all sublogs
                var tasks = new Task[shardedLog.Length];
                for (var i = 0; i < shardedLog.Length; i++)
                    tasks[i] = shardedLog.sublog[i].CommitAsync(cookie, token).AsTask();

                await Task.WhenAll(tasks);
            }
        }

        /// <summary>
        /// Wait for commit asycn of all sublogs
        /// </summary>
        /// <param name="untilAddress"></param>
        /// <param name="commitNum"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(long untilAddress = 0, long commitNum = -1, CancellationToken token = default)
        {
            if (singleLog != null)
            {
                // Optimization for single log case
                await singleLog.log.WaitForCommitAsync(untilAddress, commitNum, token);
            }
            else
            {
                // Create tasks for all sublogs
                var tasks = new Task[shardedLog.Length];
                for (var i = 0; i < shardedLog.Length; i++)
                    tasks[i] = shardedLog.sublog[i].WaitForCommitAsync(untilAddress, commitNum, token).AsTask();

                await Task.WhenAll(tasks);
            }
        }

        /// <summary>
        /// Shift begin address of all physical sublogs
        /// </summary>
        /// <param name="untilAddress"></param>
        /// <param name="snapToPageStart"></param>
        /// <param name="truncateLog"></param>
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

        /// <summary>
        /// Truncate until address for all physical sublogs
        /// </summary>
        /// <param name="untilAddress"></param>
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

        internal void Enqueue<TInput>(AofShardedHeader shardedHeader, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input, out long logicalAddress)
            where TInput : IStoreInput
        {
            var physicalSublogIdx = 0L;
            if (serverOptions.AofPhysicalSublogCount > 1)
            {
                var hash = HASH(key);
                physicalSublogIdx = hash % serverOptions.AofPhysicalSublogCount;
            }
            shardedLog.sublog[physicalSublogIdx].Enqueue(
                shardedHeader,
                key,
                value,
                ref input,
                out logicalAddress);
        }

        internal void Enqueue<TInput>(AofShardedHeader shardedHeader, ReadOnlySpan<byte> key, ref TInput input, out long logicalAddress)
            where TInput : IStoreInput
        {
            var physicalSublogIdx = 0L;
            if (serverOptions.AofPhysicalSublogCount > 1)
            {
                var hash = HASH(key);
                physicalSublogIdx = hash % serverOptions.AofPhysicalSublogCount;
            }
            shardedLog.sublog[physicalSublogIdx].Enqueue(
                shardedHeader,
                key,
                ref input,
                out logicalAddress);
        }

        internal void Enqueue(AofShardedHeader shardedHeader, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, out long logicalAddress)
        {
            var physicalSublogIdx = 0L;
            if (serverOptions.AofPhysicalSublogCount > 1)
            {
                var hash = HASH(key);
                physicalSublogIdx = hash % serverOptions.AofPhysicalSublogCount;
            }
            shardedLog.sublog[physicalSublogIdx].Enqueue(
                shardedHeader,
                key,
                value,
                out logicalAddress);
        }
    }
}