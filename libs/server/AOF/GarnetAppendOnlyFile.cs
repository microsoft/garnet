// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed class GarnetAppendOnlyFile(GarnetServerOptions serverOptions, TsavoriteLogSettings[] logSettings, ILogger logger = null)
    {
        const long kFirstValidAofAddress = 64;

        public readonly ReplayTimestampTracker replayTimestampTracker = new((int)serverOptions.AofSublogCount);
        public GarnetLog Log { get; private set; } = new(serverOptions, logSettings, logger);
        readonly GarnetServerOptions serverOptions = serverOptions;

        public long HeaderSize => Log.HeaderSize;

        public AofAddress InvalidAofAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: -1);

        public AofAddress MaxAofAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: long.MaxValue);

        public void Dispose() => Log.Dispose();

        public void SetLogShiftTailCallback(int sublogIdx, Action<long, long> SafeTailShiftCallback)
            => Log.GetSubLog(sublogIdx).SafeTailShiftCallback = SafeTailShiftCallback;

        public TsavoriteLogScanIterator Scan(int sublogIdx, ref AofAddress beginAddress, ref AofAddress endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => Log.GetSubLog(sublogIdx).Scan(beginAddress[sublogIdx], endAddress[sublogIdx], recover, scanBufferingMode, scanUncommitted, logger);

        public TsavoriteLogScanSingleIterator ScanSingle(int sublogIdx, long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => Log.GetSubLog(sublogIdx).ScanSingle(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        public void SafeInitialize(int sublogIdx, long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
            => Log.GetSubLog(sublogIdx).SafeInitialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void Initialize(in AofAddress beginAddress, in AofAddress committedUntilAddress, long lastCommitNum = 0)
            => Log.Initialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void Enqueue<THeader>(THeader userHeader, out long logicalAddress)
            where THeader : unmanaged
            // FIXME: Add marker for every sublog involved in the TXN
            => Log.GetSubLog(0).Enqueue(userHeader, out logicalAddress);

        public void EnqueueRefreshSublogTail(int sublogIdx, long timestamp)
        {
            var refreshSublogTailHeader = new AofHeader { opType = AofEntryType.RefreshSublogTail, timestamp = timestamp };
            Log.GetSubLog(sublogIdx).Enqueue(refreshSublogTailHeader, out _);
        }

        public void EnqueueCustomProc<THeader, TInput>(THeader userHeader, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            // FIXME: Handle custom proce enqueu in sharded environment
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
                    if (Log.CommittedUntilAddress[0] < replayUntilAddress[0])
                        replayUntilAddress[0] = Log.CommittedUntilAddress[0];

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
                if (syncFromAofAddress[0] < Log.BeginAddress[0])
                {
                    logger?.LogError("syncFromAofAddress: {syncFromAofAddress} < beginAofAddress: {storeWrapper.appendOnlyFile.BeginAddress}", syncFromAofAddress[0], Log.BeginAddress[0]);
                    throw new Exception("Failed syncing because replica requested truncated AOF address");
                }
            }
            else // possible AOF data loss
            {
                if (syncFromAofAddress[0] < Log.BeginAddress[0])
                {
                    logger?.LogWarning("AOF truncated, unsafe attach: syncFromAofAddress: {syncFromAofAddress} < beginAofAddress: {storeWrapper.appendOnlyFile.BeginAddress}", syncFromAofAddress, Log.BeginAddress[0]);
                    softDataLoss = true;
                }
            }
            return softDataLoss;
        }
    }
}