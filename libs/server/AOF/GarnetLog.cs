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
    /// <summary>
    /// Provides a unified interface for managing append-only logs in both single-log and sharded-log modes, supporting
    /// operations such as recovery, commit, truncation, and scanning across all sublogs as configured by the server
    /// options.
    /// </summary>
    public sealed class GarnetLog
    {
        readonly GarnetServerOptions serverOptions;
        readonly SingleLog singleLog;
        readonly ShardedLog shardedLog;
        readonly Func<byte[]> cookieGeneratorCallback;

        public static unsafe long GetSequenceNumberFromCookie(byte[] cookie)
        {
            fixed (byte* ptr = cookie)
            {
                return *(long*)ptr;
            }
        }

        /// <summary>
        /// Initializes a new GarnetLog instance with the specified configuration.
        /// </summary>
        /// <param name="appendOnlyFile">Append only file instance.</param>
        /// <param name="serverOptions">Server configuration determining log mode and sharding.</param>
        /// <param name="logSettings">Settings for the underlying log(s).</param>
        /// <param name="logger">Optional logger for recording events.</param>
        public GarnetLog(GarnetAppendOnlyFile appendOnlyFile, GarnetServerOptions serverOptions, TsavoriteLogSettings[] logSettings, ILogger logger = null)
        {
            Debug.Assert(serverOptions.EnableFastCommit || serverOptions.AofPhysicalSublogCount == 1, "Cannot use sharded-log without FastCommit!");
            this.cookieGeneratorCallback = () =>
            {
                unsafe
                {
                    var cookie = stackalloc byte[8];
                    *(long*)cookie = appendOnlyFile.seqNumGen.GetSequenceNumber();
                    return new Span<byte>(cookie, 8).ToArray();
                }
            };

            this.serverOptions = serverOptions;

            if (serverOptions.AofPhysicalSublogCount == 1)
                this.singleLog = new SingleLog(logSettings[0], logger);
            else
                this.shardedLog = new ShardedLog(serverOptions.AofPhysicalSublogCount, logSettings, logger: logger);
        }

        public TsavoriteLog SingleLog => singleLog.log;
        public long HeaderSize => singleLog != null ? singleLog.HeaderSize : shardedLog.HeaderSize;
        public int Size => singleLog != null ? 1 : shardedLog.Length;
        public int ReplayTaskCount => serverOptions.AofReplayTaskCount;

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

        public unsafe bool RecoverLatestSequenceNumber(out long recoverUntilSequenceNumber)
        {
            recoverUntilSequenceNumber = -1;
            if (serverOptions.AofPhysicalSublogCount == 1)
                return true;
            var sublogCount = shardedLog.sublog.Length;
            for (var physicalSublogIdx = 0; physicalSublogIdx < sublogCount; physicalSublogIdx++)
            {
                var physicalSublog = shardedLog.sublog[physicalSublogIdx];
                var cookie = physicalSublog.RecoveredCookie;
                if (cookie == null)
                    return false;
                var latestSequenceNumber = GetSequenceNumberFromCookie(cookie);
                recoverUntilSequenceNumber = recoverUntilSequenceNumber == -1 ? latestSequenceNumber : Math.Min(recoverUntilSequenceNumber, latestSequenceNumber);
            }
            return true;
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
        /// Set log shift tail callbacks
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="SafeTailShiftCallback"></param>
        public void SetLogShiftTailCallback(int sublogIdx, Action<long, long> SafeTailShiftCallback)
        {
            if (singleLog != null)
            {
                singleLog.log.SafeTailShiftCallback = SafeTailShiftCallback;
            }
            else
            {
                shardedLog.sublog[sublogIdx].SafeTailShiftCallback = SafeTailShiftCallback;
            }
        }

        /// <summary>
        /// Scan sublog with the specified parameters
        /// </summary>
        /// <param name="sublogIdx">Sublog index</param>
        /// <param name="beginAddress">Begin address for scan</param>
        /// <param name="endAddress">End address for scan</param>
        /// <param name="recover">Whether to recover named iterator from latest commit</param>
        /// <param name="scanBufferingMode">Use single or double buffering</param>
        /// <param name="scanUncommitted">Whether we scan uncommitted data</param>
        /// <param name="logger">Optional logger</param>
        /// <returns>TsavoriteLogScanIterator instance</returns>
        public TsavoriteLogScanIterator Scan(int sublogIdx, long beginAddress, long endAddress, bool recover = true, DiskScanBufferingMode scanBufferingMode = DiskScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
        {
            if (singleLog != null)
            {
                Debug.Assert(sublogIdx == 0);
                return singleLog.log.Scan(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);
            }
            else
            {
                Debug.Assert(sublogIdx < shardedLog.Length);
                return shardedLog.sublog[sublogIdx].Scan(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);
            }
        }

        /// <summary>
        /// Scan single sublog with the specified parameters
        /// </summary>
        /// <param name="sublogIdx">Sublog index</param>
        /// <param name="beginAddress">Begin address for scan</param>
        /// <param name="endAddress">End address for scan</param>
        /// <param name="recover">Whether to recover named iterator from latest commit</param>
        /// <param name="scanBufferingMode">Use single or double buffering</param>
        /// <param name="scanUncommitted">Whether we scan uncommitted data</param>
        /// <param name="logger">Optional logger</param>
        /// <returns>TsavoriteLogScanSingleIterator instance</returns>
        public TsavoriteLogScanSingleIterator ScanSingle(int sublogIdx, long beginAddress, long endAddress, bool recover = true, DiskScanBufferingMode scanBufferingMode = DiskScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
        {
            if (singleLog != null)
            {
                Debug.Assert(sublogIdx == 0);
                return singleLog.log.ScanSingle(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);
            }
            else
            {
                Debug.Assert(sublogIdx < shardedLog.Length);
                return shardedLog.sublog[sublogIdx].ScanSingle(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);
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
        /// Shifts the begin address of the specified sublog or single log to the given address.
        /// </summary>
        /// <param name="physicalSublogIdx">Index of the physical sublog to operate on when using a sharded log.</param>
        /// <param name="untilAddress">The address to which the begin address should be shifted.</param>
        /// <param name="snapToPageStart">If true, snaps the begin address to the start of the page containing the specified address.</param>
        /// <param name="truncateLog">If true, truncates the log up to the new begin address.</param>
        public void UnsafeShiftBeginAddress(int physicalSublogIdx, long untilAddress, bool snapToPageStart = false, bool truncateLog = false)
        {
            if (singleLog != null)
                singleLog.log.UnsafeShiftBeginAddress(untilAddress, snapToPageStart: snapToPageStart, truncateLog: truncateLog);
            else
                shardedLog.sublog[physicalSublogIdx].UnsafeShiftBeginAddress(untilAddress, snapToPageStart: snapToPageStart, truncateLog: truncateLog);
        }

        /// <summary>
        /// Truncates the log up to the specified address, either on a single log or a sharded sublog.
        /// </summary>
        /// <param name="physicalSublogIdx">The index of the physical sublog to truncate when using sharded logs.</param>
        /// <param name="untilAddress">The address up to which the log should be truncated.</param>
        public void TruncateUntil(int physicalSublogIdx, long untilAddress)
        {
            if (singleLog != null)
                singleLog.log.TruncateUntil(untilAddress);
            else
                shardedLog.sublog[physicalSublogIdx].TruncateUntil(untilAddress);
        }

        /// <summary>
        /// Safe initialize when FastAofTruncate is enabled
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="beginAddress"></param>
        /// <param name="committedUntilAddress"></param>
        /// <param name="lastCommitNum"></param>
        public void SafeInitialize(int sublogIdx, long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
        {
            if (singleLog != null)
                singleLog.log.SafeInitialize(beginAddress, committedUntilAddress, lastCommitNum);
            else
                shardedLog.sublog[sublogIdx].SafeInitialize(beginAddress, committedUntilAddress, lastCommitNum);
        }

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
                var _cookie = cookieGeneratorCallback();
                for (var i = 0; i < shardedLog.Length; i++)
                    shardedLog.sublog[i].Commit(spinWait, cookie: _cookie);
            }
        }

        /// <summary>
        /// Blocks the calling thread until the log is committed up to the specified address or commit number.
        /// </summary>
        /// <param name="untilAddress">The address up to which to wait for the commit. Defaults to 0.</param>
        /// <param name="commitNum">The commit number up to which to wait. Defaults to -1.</param>
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
                await singleLog.log.CommitAsync(cookie: cookie, token);
            }
            else
            {
                var _cookie = cookieGeneratorCallback();
                // Create tasks for all sublogs
                var tasks = new Task[shardedLog.Length];
                for (var i = 0; i < shardedLog.Length; i++)
                    tasks[i] = shardedLog.sublog[i].CommitAsync(token: token, cookie: _cookie).AsTask();

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

        internal void Enqueue<TInput, TEpochAccessor>(AofShardedHeader shardedHeader, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, ref TInput input, TEpochAccessor epochAccessor, out long logicalAddress)
            where TInput : IStoreInput
            where TEpochAccessor : IEpochAccessor
        {
            if (serverOptions.AofPhysicalSublogCount == 1)
            {
                // Single log with multi-replay enabled needs to add sharderHeader to implement read protocol.
                // Header modifier callback not needed because single physical sublog commit marker is read consistent when flushed.
                singleLog.log.Enqueue(shardedHeader,
                    key,
                    value,
                    ref input,
                    epochAccessor,
                    out logicalAddress);
            }
            else
            {
                var hash = HASH(key);
                var physicalSublogIdx = hash % serverOptions.AofPhysicalSublogCount;
                shardedLog.sublog[physicalSublogIdx].Enqueue(
                    shardedHeader,
                    key,
                    value,
                    ref input,
                    epochAccessor,
                    out logicalAddress);

                if (serverOptions.AofAutoCommit)
                    Commit();
            }
        }

        internal void Enqueue<TInput, TEpochAccessor>(AofShardedHeader shardedHeader, ReadOnlySpan<byte> key, ref TInput input, TEpochAccessor epochAccessor, out long logicalAddress)
            where TInput : IStoreInput
            where TEpochAccessor : IEpochAccessor
        {
            if (serverOptions.AofPhysicalSublogCount == 1)
            {
                // Single log with multi-replay enabled needs to add sharderHeader to implement read protocol.
                // Header modifier callback not needed because single physical sublog commit marker is read consistent when flushed.
                singleLog.log.Enqueue(
                    shardedHeader,
                    key,
                    ref input,
                    epochAccessor,
                    out logicalAddress);
            }
            else
            {
                var hash = HASH(key);
                var physicalSublogIdx = hash % serverOptions.AofPhysicalSublogCount;
                shardedLog.sublog[physicalSublogIdx].Enqueue(
                    shardedHeader,
                    key,
                    ref input,
                    epochAccessor,
                    out logicalAddress);

                if (serverOptions.AofAutoCommit)
                    Commit();
            }
        }

        internal void Enqueue<TEpochAccessor>(AofShardedHeader shardedHeader, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, TEpochAccessor epochAccessor, out long logicalAddress)
            where TEpochAccessor : IEpochAccessor
        {
            if (serverOptions.AofPhysicalSublogCount == 1)
            {
                // Single log with multi-replay enabled needs to add sharderHeader to implement read protocol.
                // Header modifier callback not needed because single physical sublog commit marker is read consistent when flushed.
                singleLog.log.Enqueue(
                    shardedHeader,
                    key,
                    value,
                    epochAccessor,
                    out logicalAddress);
            }
            else
            {
                var hash = HASH(key);
                var physicalSublogIdx = hash % serverOptions.AofPhysicalSublogCount;
                shardedLog.sublog[physicalSublogIdx].Enqueue(
                    shardedHeader,
                    key,
                    value,
                    epochAccessor,
                    out logicalAddress);

                if (serverOptions.AofAutoCommit)
                    Commit();
            }
        }

        internal unsafe void Enqueue(AofTransactionHeader header, ref CustomProcedureInput procInput, CustomTransactionProcedure proc)
        {
            if (serverOptions.AofPhysicalSublogCount == 1)
            {
                // Update corresponding sublog participating vector before enqueue to related physical sublog
                proc.replayTaskAccessVector[0].CopyTo(
                    new Span<byte>(header.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorBytes));
                // Single log with multi-replay enabled needs to add sharderHeader to implement read protocol.
                // Header modifier callback not needed because single physical sublog commit marker is read consistent when flushed.
                singleLog.log.Enqueue(header, ref procInput, out _);
            }
            else
            {
                try
                {
                    if (serverOptions.AofPhysicalSublogCount > 1)
                        LockSublogs(proc.physicalSublogAccessVector);
                    var _physicalSublogAccessVector = proc.physicalSublogAccessVector;
                    while (_physicalSublogAccessVector > 0)
                    {
                        var physicalSublogIdx = _physicalSublogAccessVector.GetNextOffset();
                        // Update corresponding sublog participating vector before enqueue to related physical sublog
                        proc.replayTaskAccessVector[physicalSublogIdx].CopyTo(
                            new Span<byte>(header.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorBytes));
                        shardedLog.sublog[physicalSublogIdx].Enqueue(header, ref procInput, out _);
                    }
                }
                finally
                {
                    if (serverOptions.AofPhysicalSublogCount > 1)
                        UnlockSublogs(proc.physicalSublogAccessVector);
                }

                if (serverOptions.AofAutoCommit)
                    Commit();
            }
        }

        internal unsafe void Enqueue(AofTransactionHeader header, ulong physicalSublogAccessVector, BitVector[] virtualSublogAccessVector, int participantCount)
        {
            if (serverOptions.AofPhysicalSublogCount == 1)
            {
                virtualSublogAccessVector[0].CopyTo(new Span<byte>(header.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorBytes));
                // Single log with multi-replay enabled needs to add sharderHeader to implement read protocol.
                // Header modifier callback not needed because single physical sublog commit marker is read consistent when flushed.
                singleLog.log.Enqueue(header, out _);
            }
            else
            {
                try
                {
                    if (serverOptions.AofPhysicalSublogCount > 1)
                        LockSublogs(physicalSublogAccessVector);
                    var _physicalSublogAccessVector = physicalSublogAccessVector;
                    while (_physicalSublogAccessVector > 0)
                    {
                        var physicalSublogIdx = _physicalSublogAccessVector.GetNextOffset();
                        // Update corresponding sublog participating vector before enqueue to related physical sublog
                        virtualSublogAccessVector[physicalSublogIdx].CopyTo(new Span<byte>(header.replayTaskAccessVector, AofTransactionHeader.ReplayTaskAccessVectorBytes));
                        shardedLog.sublog[physicalSublogIdx].Enqueue(header, out _);
                    }
                }
                finally
                {
                    if (serverOptions.AofPhysicalSublogCount > 1)
                        UnlockSublogs(physicalSublogAccessVector);
                }

                if (serverOptions.AofAutoCommit)
                    Commit();
            }
        }

        internal void Enqueue(AofTransactionHeader header, ulong physicalSublogAccessVector)
        {
            if (serverOptions.AofPhysicalSublogCount == 1)
            {
                // Single log with multi-replay enabled needs to add sharderHeader to implement read protocol.
                // Header modifier callback not needed because single physical sublog commit marker is read consistent when flushed.
                singleLog.log.Enqueue(header, out _);
            }
            else
            {
                try
                {
                    if (serverOptions.AofPhysicalSublogCount > 1)
                        LockSublogs(physicalSublogAccessVector);
                    var _physicalSublogAccessVector = physicalSublogAccessVector;

                    while (_physicalSublogAccessVector > 0)
                    {
                        var physicalSublogIdx = _physicalSublogAccessVector.GetNextOffset();
                        shardedLog.sublog[physicalSublogIdx].Enqueue(header, out _);
                    }
                }
                finally
                {
                    if (serverOptions.AofPhysicalSublogCount > 1)
                        UnlockSublogs(physicalSublogAccessVector);
                }

                if (serverOptions.AofAutoCommit)
                    Commit();
            }
        }
    }
}