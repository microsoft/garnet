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
    public class GarnetLog(GarnetServerOptions serverOptions, TsavoriteLogSettings[] logSettings, ILogger logger = null)
    {
        readonly int replayTaskCount = serverOptions.AofReplaySubtaskCount;
        readonly int aofSublogCount = serverOptions.AofPhysicalSublogCount;
        readonly SingleLog singleLog = serverOptions.AofPhysicalSublogCount == 1 ? new SingleLog(logSettings[0], logger) : null;
        readonly ShardedLog shardedLog = serverOptions.AofPhysicalSublogCount > 1 ? new ShardedLog(serverOptions.AofPhysicalSublogCount, logSettings, logger) : null;

        public TsavoriteLog SigleLog => singleLog.log;
        public readonly BitVector[] AllParticipatingTasks;

        public long HeaderSize => singleLog != null ? singleLog.HeaderSize : shardedLog.HeaderSize;

        public int Size => singleLog != null ? 1 : shardedLog.Length;

        public int ReplayTaskCount => replayTaskCount;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long HASH(ReadOnlySpan<byte> key)
            => Utility.HashBytes(key) & long.MaxValue;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (int, int) HashKey(ReadOnlySpan<byte> key)
        {
            var hash = HASH(key);
            return ((int)(((ulong)hash) % (ulong)aofSublogCount), (int)(((ulong)hash) % 0xFF));
        }

        public AofAddress BeginAddress
        {
            get
            {
                if (singleLog != null)
                    return singleLog.BeginAddress;
                return shardedLog.BeginAddress;
            }
        }

        public long GetBeginAddress(int sublogIdx) => singleLog != null ? singleLog.log.BeginAddress : shardedLog.sublog[sublogIdx].BeginAddress;

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
            Debug.Assert(shardedLog != null);
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

        public unsafe int UnsafeGetLength(int sublogIdx, byte* headerPtr)
            => GetSubLog(sublogIdx).UnsafeGetLength(headerPtr);

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

        internal void Enqueue<TInput>(AofShardedHeader shardedHeader, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input, out long logicalAddress)
            where TInput : IStoreInput
        {
            var (sublogIdx, keyDigest) = HashKey(key);
            shardedHeader.keyDigest = (byte)keyDigest;
            shardedLog.sublog[sublogIdx].Enqueue(
                shardedHeader,
                key,
                value,
                ref input,
                out logicalAddress);
        }

        internal void Enqueue<TInput>(AofShardedHeader shardedHeader, ReadOnlySpan<byte> key, ref TInput input, out long logicalAddress)
            where TInput : IStoreInput
        {
            var (sublogIdx, keyDigest) = HashKey(key);
            shardedHeader.keyDigest = (byte)keyDigest;
            shardedLog.sublog[sublogIdx].Enqueue(
                shardedHeader,
                key,
                ref input,
                out logicalAddress);
        }

        internal void Enqueue(AofShardedHeader shardedHeader, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, out long logicalAddress)
        {
            var (sublogIdx, keyDigest) = HashKey(key);
            shardedHeader.keyDigest = (byte)keyDigest;
            shardedLog.sublog[sublogIdx].Enqueue(
                shardedHeader,
                key,
                value,
                out logicalAddress);
        }
    }
}