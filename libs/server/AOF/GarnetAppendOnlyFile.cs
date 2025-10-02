// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public class Log(GarnetServerOptions serverOptions, TsavoriteLogSettings[] logSettings, ILogger logger = null)
    {
        readonly TsavoriteLogSettings[] logSettings = logSettings;
        public readonly TsavoriteLog[] sublog = CreateLogInstance(logSettings, logger);
        static TsavoriteLog[] CreateLogInstance(TsavoriteLogSettings[] logSettings, ILogger logger = null)
            => [.. logSettings.Select(settings => new TsavoriteLog(settings, logger))];

        public AofAddress InvalidAofAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: -1);

        public AofAddress MaxAofAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: long.MaxValue);

        public ref AofAddress BeginAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    beginAddress[i] = sublog[i].BeginAddress;
                return ref beginAddress;
            }
        }
        AofAddress beginAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

        public ref AofAddress TailAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    tailAddress[i] = sublog[i].TailAddress;
                return ref tailAddress;
            }
        }
        AofAddress tailAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

        public ref AofAddress CommittedUntilAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    committedUntilAddress[i] = sublog[i].CommittedUntilAddress;
                return ref committedUntilAddress;
            }
        }
        AofAddress committedUntilAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

        public ref AofAddress CommittedBeginAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    commitedBeginnAddress[i] = sublog[i].CommittedBeginAddress;
                return ref commitedBeginnAddress;
            }
        }
        AofAddress commitedBeginnAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

        public ref AofAddress FlushedUntilAddress
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    flushedUntilAddress[i] = sublog[i].FlushedUntilAddress;
                return ref flushedUntilAddress;
            }
        }
        AofAddress flushedUntilAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

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
        AofAddress maxMemorySizeBytes = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

        public ref AofAddress MemorySizeBytes
        {
            get
            {
                for (var i = 0; i < sublog.Length; i++)
                    memorySizeBytes[i] = sublog[i].MemorySizeBytes;
                return ref memorySizeBytes;
            }
        }
        AofAddress memorySizeBytes = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

        public unsafe int UnsafeGetLength(byte* headerPtr) => sublog[0].UnsafeGetLength(headerPtr);

        public int UnsafeGetLogPageSizeBits() => sublog[0].UnsafeGetLogPageSizeBits();

        public long UnsafeGetReadOnlyAddressLagOffset()
            //FIXME:
            => sublog[0].UnsafeGetReadOnlyAddressLagOffset();

        public void InitializeIf(ref AofAddress recoveredSafeAofAddress)
        {
            for (var i = 0; i < sublog.Length; i++)
                if (TailAddress[i] < recoveredSafeAofAddress[i])
                    sublog[i].Initialize(TailAddress[i], recoveredSafeAofAddress[i]);
        }

        public void Initialize(in AofAddress beginAddress, in AofAddress committedUntilAddress, long lastCommitNum = 0)
        {
            for (var i = 0; i < sublog.Length; i++)
                sublog[i].Initialize(beginAddress[i], committedUntilAddress[i], lastCommitNum);
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

        public TsavoriteLog GetSubLog(int slot, ref SpanByte key)
        {
            if (slot == -1)
            {
                var keySlice = ArgSlice.FromPinnedSpan(key.AsReadOnlySpan());
                slot = ArgSliceUtils.HashSlot(ref keySlice);
            }
            return sublog[slot % sublog.Length];
        }

        public TsavoriteLog GetSubLog(int sublogIdx)
        {
            Debug.Assert(sublogIdx < sublog.Length);
            return sublog[sublogIdx];
        }

        public void WaitForCommit(long untilAddress = 0, long commitNum = -1)
        {
            foreach (var log in sublog)
                log.WaitForCommit();
        }

        public void Commit(bool spinWait = false, byte[] cookie = null)
        {
            foreach (var log in sublog)
                log.Commit(spinWait, cookie);
        }

        public async ValueTask CommitAsync(byte[] cookie = null, CancellationToken token = default)
        {
            if (sublog.Length == 1)
            {
                // Optimization for single log case
                await sublog[0].CommitAsync(cookie, token);
                return;
            }

            // Create tasks for all sublogs
            var tasks = new ValueTask[sublog.Length];
            for (var i = 0; i < sublog.Length; i++)
                tasks[i] = sublog[i].CommitAsync(cookie, token);

            // Wait for all commits to complete
            for (var i = 0; i < tasks.Length; i++)
                await tasks[i];
        }

        public async ValueTask WaitForCommitAsync(long untilAddress = 0, long commitNum = -1, CancellationToken token = default)
        {
            if (sublog.Length == 1)
            {
                // Optimization for single log case
                await sublog[0].WaitForCommitAsync(untilAddress, commitNum, token);
                return;
            }

            // Create tasks for all sublogs
            var tasks = new ValueTask[sublog.Length];
            for (var i = 0; i < sublog.Length; i++)
                tasks[i] = sublog[i].WaitForCommitAsync(untilAddress, commitNum, token);

            // Wait for all commits to complete
            for (var i = 0; i < tasks.Length; i++)
                await tasks[i];
        }

        public void UnsafeShiftBeginAddress(AofAddress untilAddress, bool snapToPageStart = false, bool truncateLog = false)
        {
            for (var i = 0; i < sublog.Length; i++)
                sublog[i].UnsafeShiftBeginAddress(untilAddress[i], snapToPageStart, truncateLog);
        }

        public void TruncateUntil(AofAddress untilAddress)
        {
            for (var i = 0; i < sublog.Length; i++)
                sublog[i].TruncateUntil(untilAddress[i]);
        }
    }

    public sealed class GarnetAppendOnlyFile(GarnetServerOptions serverOptions, TsavoriteLogSettings[] logSettings, ILogger logger = null)
    {
        const long kFirstValidAofAddress = 64;

        public Log Log => log;
        readonly Log log = new(serverOptions, logSettings, logger);
        readonly GarnetServerOptions serverOptions = serverOptions;

        public long HeaderSize => log.HeaderSize;

        public ref AofAddress InvalidAofAddress => ref log.InvalidAofAddress;

        public AofAddress MaxAofAddress => log.MaxAofAddress;

        public ref AofAddress BeginAddress => ref log.BeginAddress;

        public ref AofAddress TailAddress => ref log.TailAddress;

        public ref AofAddress CommittedUntilAddress => ref log.CommittedUntilAddress;

        public ref AofAddress CommittedBeginAddress => ref log.CommittedBeginAddress;

        public ref AofAddress FlushedUntilAddress => ref log.FlushedUntilAddress;

        public ref AofAddress MaxMemorySizeBytes => ref log.MaxMemorySizeBytes;

        public ref AofAddress MemorySizeBytes => ref log.MemorySizeBytes;

        public void Recover() => log.Recover();
        public void Reset() => log.Reset();
        public void Dispose() => log.Dispose();

        public void SetLogShiftTailCallback(Action<long, long> SafeTailShiftCallback, int sublogIdx = 0)
            => log.GetSubLog(sublogIdx).SafeTailShiftCallback = SafeTailShiftCallback;

        public TsavoriteLogScanIterator Scan(int sublogIdx, ref AofAddress beginAddress, ref AofAddress endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => log.GetSubLog(sublogIdx).Scan(beginAddress[sublogIdx], endAddress[sublogIdx], recover, scanBufferingMode, scanUncommitted, logger);

        public TsavoriteLogScanSingleIterator ScanSingle(int sublogIdx, long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => log.GetSubLog(sublogIdx).ScanSingle(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        public void Initialize(in AofAddress beginAddress, in AofAddress committedUntilAddress, long lastCommitNum = 0)
            => log.Initialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void SafeInitialize(int sublogIdx, long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
            => log.GetSubLog(sublogIdx).SafeInitialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void Enqueue<THeader>(THeader userHeader, out long logicalAddress)
            where THeader : unmanaged
            // FIXME:
            => log.sublog[0].Enqueue(userHeader, out logicalAddress);

        public void EnqueueCustomProc<THeader, TInput>(THeader userHeader, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            // FIXME:
            => log.sublog[0].Enqueue(userHeader, ref input, out logicalAddress);

        public void Enqueue<THeader>(int slot, THeader userHeader, ref SpanByte item1, ref SpanByte item2, out long logicalAddress)
            where THeader : unmanaged
            => log.GetSubLog(slot, ref item1).Enqueue(userHeader, ref item1, ref item2, out logicalAddress);

        public void Enqueue<THeader, TInput>(int slot, THeader userHeader, ref SpanByte item1, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => log.GetSubLog(slot, ref item1).Enqueue(userHeader, ref item1, ref input, out logicalAddress);

        public void Enqueue<THeader, TInput>(int slot, THeader userHeader, ref SpanByte item1, ref SpanByte item2, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => log.GetSubLog(slot, ref item1).Enqueue(userHeader, ref item1, ref item2, ref input, out logicalAddress);

        public long UnsafeEnqueueRaw(int sublogIdx, ReadOnlySpan<byte> entryBytes, bool noCommit = false)
            => log.GetSubLog(sublogIdx).UnsafeEnqueueRaw(entryBytes, noCommit);

        public void UnsafeCommitMetadataOnly(int sublogIdx, TsavoriteLogRecoveryInfo info, bool isProtected)
            => log.GetSubLog(sublogIdx).UnsafeCommitMetadataOnly(info, isProtected);

        public void UnsafeShiftBeginAddress(int sublogIdx, long untilAddress, bool snapToPageStart = false, bool truncateLog = false)
            => log.GetSubLog(sublogIdx).UnsafeShiftBeginAddress(untilAddress, snapToPageStart, truncateLog);

        public void TruncateUntil(int sublogIdx, long untilAddress) => log.GetSubLog(sublogIdx).TruncateUntil(untilAddress);

        public bool ReplicaBeginAofSyncAddress(
            bool recoverFromRemote,
            bool sameMainStoreCheckpointHistory,
            bool sameObjectStoreCheckpointHistory,
            bool sameHistory2,
            in AofAddress replicationOffset2,
            in AofAddress replicaAofBeginAddress,
            in AofAddress replicaAofTailAddress,
            ref AofAddress beginAddress,
            ref AofAddress checkpointAofBeginAddress)
        {
            var replayAOF = false;
            if (!recoverFromRemote)
            {
                if (replicaAofBeginAddress[0] > kFirstValidAofAddress && replicaAofBeginAddress[0] > checkpointAofBeginAddress[0])
                {
                    logger?.LogInformation(
                        "ReplicaSyncSession: replicaAofBeginAddress {replicaAofBeginAddress} > PrimaryCheckpointRecoveredReplicationOffset {RecoveredReplicationOffset}, cannot use remote AOF",
                        replicaAofBeginAddress[0], checkpointAofBeginAddress[0]);
                }
                else
                {
                    // Tail address cannot be behind the recovered address since above we checked replicaAofBeginAddress and it appears after RecoveredReplicationOffset
                    // unless we are performing MainMemoryReplication
                    // TODO: shouldn't we use the remote cEntry's tail address here since replica will recover to that?
                    if (replicaAofTailAddress[0] < checkpointAofBeginAddress[0] && !serverOptions.FastAofTruncate)
                    {
                        logger?.LogCritical("ReplicaSyncSession replicaAofTail {replicaAofTailAddress} < canServeFromAofAddress {RecoveredReplicationOffset}", replicaAofTailAddress, checkpointAofBeginAddress);
                        throw new Exception($"ReplicaSyncSession replicaAofTail {replicaAofTailAddress} < canServeFromAofAddress {checkpointAofBeginAddress}");
                    }

                    // If we are behind this primary we need to decide until where to replay
                    var replayUntilAddress = replicaAofTailAddress;
                    // Replica tail is further ahead than committed address of primary
                    if (CommittedUntilAddress[0] < replayUntilAddress[0])
                        replayUntilAddress[0] = CommittedUntilAddress[0];

                    // Replay only if records not included in checkpoint
                    if (replayUntilAddress[0] > checkpointAofBeginAddress[0])
                    {
                        logger?.LogInformation("ReplicaSyncSession: have to replay remote AOF from {beginAddress} until {untilAddress}", checkpointAofBeginAddress[0], replayUntilAddress);
                        replayAOF = true;
                        // Bound replayUntilAddress to ReplicationOffset2 to avoid replaying divergent history only if connecting replica was attached to old primary
                        if (sameHistory2 && replayUntilAddress[0] > replicationOffset2[0])
                            replayUntilAddress[0] = replicationOffset2[0];
                        checkpointAofBeginAddress = replayUntilAddress;
                    }

                    if (!sameMainStoreCheckpointHistory || !sameObjectStoreCheckpointHistory)
                    {
                        // If we are not in the same checkpoint history, we need to stream the AOF from the primary's beginning address
                        checkpointAofBeginAddress[0] = beginAddress[0];
                        replayAOF = false;
                        logger?.LogInformation("ReplicaSyncSession: not in same checkpoint history, will replay from beginning address {checkpointAofBeginAddress}", checkpointAofBeginAddress);
                    }
                }
            }
            return replayAOF;
        }

        public bool DataLossCheck(bool possibleAofDataLoss, AofAddress syncFromAofAddress, ILogger logger = null)
        {
            var softDataLoss = false;
            if (!possibleAofDataLoss)
            {
                if (syncFromAofAddress[0] < BeginAddress[0])
                {
                    logger?.LogError("syncFromAofAddress: {syncFromAofAddress} < beginAofAddress: {storeWrapper.appendOnlyFile.BeginAddress}", syncFromAofAddress[0], BeginAddress[0]);
                    throw new Exception("Failed syncing because replica requested truncated AOF address");
                }
            }
            else // possible AOF data loss
            {
                if (syncFromAofAddress[0] < BeginAddress[0])
                {
                    logger?.LogWarning("AOF truncated, unsafe attach: syncFromAofAddress: {syncFromAofAddress} < beginAofAddress: {storeWrapper.appendOnlyFile.BeginAddress}", syncFromAofAddress, BeginAddress[0]);
                    softDataLoss = true;
                }
            }
            return softDataLoss;
        }
    }
}