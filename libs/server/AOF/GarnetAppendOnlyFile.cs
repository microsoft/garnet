// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;
using Tsavorite.core;
using Garnet.common;

namespace Garnet.server
{
    public sealed class GarnetAppendOnlyFile(GarnetServerOptions serverOptions, TsavoriteLogSettings[] logSettings, ILogger logger = null)
    {
        const long kFirstValidAofAddress = 64;

        public long TotalSize() => Log.TailAddress.AggregateDiff(Log.BeginAddress);

        public readonly ReplayTimestampTracker replayTimestampTracker = new((int)serverOptions.AofSublogCount);
        public GarnetLog Log { get; private set; } = new(serverOptions, logSettings, logger);
        readonly GarnetServerOptions serverOptions = serverOptions;

        public long HeaderSize => Log.HeaderSize;

        public readonly AofAddress InvalidAofAddress = AofAddress.Create(length: serverOptions.AofSublogCount, value: -1);

        public readonly AofAddress MaxAofAddress = AofAddress.Create(length: serverOptions.AofSublogCount, value: long.MaxValue);

        public void Dispose() => Log.Dispose();

        public void SetLogShiftTailCallback(int sublogIdx, Action<long, long> SafeTailShiftCallback)
            => Log.GetSubLog(sublogIdx).SafeTailShiftCallback = SafeTailShiftCallback;

        public TsavoriteLogScanIterator Scan(int sublogIdx, long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => Log.GetSubLog(sublogIdx).Scan(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        public TsavoriteLogScanSingleIterator ScanSingle(int sublogIdx, long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => Log.GetSubLog(sublogIdx).ScanSingle(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        public void SafeInitialize(int sublogIdx, long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
            => Log.GetSubLog(sublogIdx).SafeInitialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void Initialize(in AofAddress beginAddress, in AofAddress committedUntilAddress, long lastCommitNum = 0)
            => Log.Initialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void Enqueue<THeader>(ulong logAccessBitmap, THeader userHeader)
            where THeader : unmanaged
        {
            if (serverOptions.AofSublogCount == 1)
            {
                Log.GetSubLog(0).Enqueue(userHeader, out _);
            }
            else
            {
                try
                {
                    Log.LockSublogs(logAccessBitmap);
                    var _logAccessBitmap = logAccessBitmap;
                    while (_logAccessBitmap > 0)
                    {
                        var offset = _logAccessBitmap.GetNextOffset();
                        Log.GetSubLog(offset).Enqueue(userHeader, out _);
                    }
                }
                finally
                {
                    Log.UnlockSublogs(logAccessBitmap);
                }
            }
        }

        public void EnqueueRefreshSublogTail(int sublogIdx, long timestamp)
        {
            var refreshSublogTailHeader = new AofHeader { opType = AofEntryType.RefreshSublogTail, timestamp = timestamp };
            Log.GetSubLog(sublogIdx).Enqueue(refreshSublogTailHeader, out _);
        }

        public void EnqueueCustomProc<THeader, TInput>(THeader userHeader, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            // FIXME: Handle custom proce enqueue in sharded environment
            => Log.GetSubLog(0).Enqueue(userHeader, ref input, out logicalAddress);

        public void Enqueue<THeader>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, out long logicalAddress)
            where THeader : unmanaged
            => Log.GetSubLog(ref item1).Enqueue(userHeader, ref item1, ref item2, out logicalAddress);

        public void Enqueue<THeader, TInput>(THeader userHeader, ref SpanByte item1, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => Log.GetSubLog(ref item1).Enqueue(userHeader, ref item1, ref input, out logicalAddress);

        public void Enqueue<THeader, TInput>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => Log.GetSubLog(ref item1).Enqueue(userHeader, ref item1, ref item2, ref input, out logicalAddress);

        public long UnsafeEnqueueRaw(int sublogIdx, ReadOnlySpan<byte> entryBytes, bool noCommit = false)
            => Log.GetSubLog(sublogIdx).UnsafeEnqueueRaw(entryBytes, noCommit);

        public void UnsafeCommitMetadataOnly(int sublogIdx, TsavoriteLogRecoveryInfo info, bool isProtected)
            => Log.GetSubLog(sublogIdx).UnsafeCommitMetadataOnly(info, isProtected);

        public void UnsafeShiftBeginAddress(int sublogIdx, long untilAddress, bool snapToPageStart = false, bool truncateLog = false)
            => Log.GetSubLog(sublogIdx).UnsafeShiftBeginAddress(untilAddress, snapToPageStart, truncateLog);

        public void TruncateUntil(int sublogIdx, long untilAddress)
            => Log.GetSubLog(sublogIdx).TruncateUntil(untilAddress);

        public ulong ComputeAofSyncReplayAddress(
            bool recoverFromRemote,
            bool sameMainStoreCheckpointHistory,
            bool sameObjectStoreCheckpointHistory,
            bool sameHistory2,
            in AofAddress replicationOffset2,
            in AofAddress replicaAofBeginAddress,
            in AofAddress replicaAofTailAddress,
            in AofAddress beginAddress,
            ref AofAddress checkpointAofBeginAddress)
        {
            var replayAOFMap = 0UL;
            for (var sublogIdx = 0; sublogIdx < serverOptions.AofSublogCount; sublogIdx++)
                ComputeAofSubloSyncReplayAddress(sublogIdx, ref replayAOFMap, recoverFromRemote, sameMainStoreCheckpointHistory, sameObjectStoreCheckpointHistory, sameHistory2, replicationOffset2, replicaAofBeginAddress, replicaAofTailAddress, beginAddress, ref checkpointAofBeginAddress);

            return replayAOFMap;

            void ComputeAofSubloSyncReplayAddress(
                int sublogIdx,
                ref ulong replayAOFMap,
                bool recoverFromRemote,
                bool sameMainStoreCheckpointHistory,
                bool sameObjectStoreCheckpointHistory,
                bool sameHistory2,
                in AofAddress replicationOffset2,
                in AofAddress replicaAofBeginAddress,
                in AofAddress replicaAofTailAddress,
                in AofAddress beginAddress,
                ref AofAddress checkpointAofBeginAddress)
            {
                if (!recoverFromRemote)
                {
                    if (replicaAofBeginAddress[sublogIdx] > kFirstValidAofAddress && replicaAofBeginAddress[sublogIdx] > checkpointAofBeginAddress[sublogIdx])
                    {
                        logger?.LogInformation(
                            "ReplicaSyncSession: replicaAofBeginAddress {replicaAofBeginAddress} > PrimaryCheckpointRecoveredReplicationOffset {RecoveredReplicationOffset}, cannot use remote AOF",
                            replicaAofBeginAddress[sublogIdx], checkpointAofBeginAddress[sublogIdx]);
                    }
                    else
                    {
                        // Tail address cannot be behind the recovered address since above we checked replicaAofBeginAddress and it appears after RecoveredReplicationOffset
                        // unless we are performing MainMemoryReplication
                        // TODO: shouldn't we use the remote cEntry's tail address here since replica will recover to that?
                        if (replicaAofTailAddress[sublogIdx] < checkpointAofBeginAddress[sublogIdx] && !serverOptions.FastAofTruncate)
                        {
                            logger?.LogCritical("ReplicaSyncSession replicaAofTail {replicaAofTailAddress} < canServeFromAofAddress {RecoveredReplicationOffset}", replicaAofTailAddress, checkpointAofBeginAddress);
                            throw new Exception($"ReplicaSyncSession replicaAofTail {replicaAofTailAddress} < canServeFromAofAddress {checkpointAofBeginAddress}");
                        }

                        // If we are behind this primary we need to decide until where to replay
                        var replayUntilAddress = replicaAofTailAddress;
                        // Replica tail is further ahead than committed address of primary
                        if (Log.CommittedUntilAddress[sublogIdx] < replayUntilAddress[sublogIdx])
                            replayUntilAddress[sublogIdx] = Log.CommittedUntilAddress[sublogIdx];

                        // Replay only if records not included in checkpoint
                        if (replayUntilAddress[sublogIdx] > checkpointAofBeginAddress[sublogIdx])
                        {
                            logger?.LogInformation("ReplicaSyncSession: have to replay remote AOF from {beginAddress} until {untilAddress}", checkpointAofBeginAddress[sublogIdx], replayUntilAddress);
                            replayAOFMap |= 1UL << sublogIdx;
                            // Bound replayUntilAddress to ReplicationOffset2 to avoid replaying divergent history only if connecting replica was attached to old primary
                            if (sameHistory2 && replayUntilAddress[sublogIdx] > replicationOffset2[sublogIdx])
                                replayUntilAddress[sublogIdx] = replicationOffset2[sublogIdx];
                            checkpointAofBeginAddress = replayUntilAddress;
                        }

                        if (!sameMainStoreCheckpointHistory || !sameObjectStoreCheckpointHistory)
                        {
                            // If we are not in the same checkpoint history, we need to stream the AOF from the primary's beginning address
                            checkpointAofBeginAddress[sublogIdx] = beginAddress[sublogIdx];
                            replayAOFMap &= ~(1UL << sublogIdx);
                            logger?.LogInformation("ReplicaSyncSession: not in same checkpoint history, will replay from beginning address {checkpointAofBeginAddress}", checkpointAofBeginAddress);
                        }
                    }
                }
            }
        }

        public void DataLossCheck(bool possibleAofDataLoss, AofAddress syncFromAofAddress, ILogger logger = null)
        {
            var beginAddress = Log.BeginAddress;
            var anyLesser = syncFromAofAddress.AnyLesser(beginAddress);

            if (anyLesser)
            {
                if (!possibleAofDataLoss)
                {
                    logger?.LogError("syncFromAofAddress: {syncFromAofAddress} < beginAofAddress: {storeWrapper.appendOnlyFile.BeginAddress}", syncFromAofAddress, beginAddress);
                    throw new Exception("Failed syncing because replica requested truncated AOF address");
                }
                else
                {
                    logger?.LogWarning("AOF truncated, unsafe attach: syncFromAofAddress: {syncFromAofAddress} < beginAofAddress: {storeWrapper.appendOnlyFile.BeginAddress}", syncFromAofAddress, beginAddress);
                }
            }
        }
    }
}
