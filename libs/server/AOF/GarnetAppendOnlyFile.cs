// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed class GarnetAppendOnlyFile(GarnetServerOptions serverOptions, TsavoriteLogSettings logSettings, ILogger logger = null)
    {
        const long kFirstValidAofAddress = 64;
        readonly GarnetServerOptions serverOptions = serverOptions;
        readonly TsavoriteLog log = new(logSettings, logger);
        
        public AofAddress InvalidAofAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: -1);

        public AofAddress MaxAofAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: long.MaxValue);

        public ref AofAddress BeginAddress
        {
            get
            {
                aofBeginAddress[0] = log.BeginAddress;
                return ref aofBeginAddress;
            }
        }
        AofAddress aofBeginAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

        public ref AofAddress TailAddress
        {
            get
            {
                aofTailAddress[0] = log.TailAddress;
                return ref aofTailAddress;
            }
        }
        AofAddress aofTailAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

        public ref AofAddress CommittedUntilAddress
        {
            get
            {
                aofCommittedUntilAddress[0] = log.CommittedUntilAddress;
                return ref aofCommittedUntilAddress;
            }
        }
        AofAddress aofCommittedUntilAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);
        
        public ref AofAddress CommittedBeginAddress
        {
            get
            {
                commitedBeginnAddress[0] = log.CommittedBeginAddress;
                return ref commitedBeginnAddress;
            }
        }
        AofAddress commitedBeginnAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);
        public ref AofAddress FlushedUntilAddress
        {
            get
            {
                flushedUntilAddress[0] = log.FlushedUntilAddress;
                return ref flushedUntilAddress;
            }
        }
        AofAddress flushedUntilAddress = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);
        public long HeaderSize => log.HeaderSize;
        public ref AofAddress MaxMemorySizeBytes
        {
            get
            {
                maxMemorySizeBytes[0] = log.MaxMemorySizeBytes;
                return ref maxMemorySizeBytes;
            }
        }
        AofAddress maxMemorySizeBytes = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);
        public ref AofAddress MemorySizeBytes
        {
            get
            {
                memorySizeBytes[0] = log.MemorySizeBytes;
                return ref memorySizeBytes;
            }
        }
        AofAddress memorySizeBytes = AofAddress.SetValue(length: serverOptions.AofSublogCount, value: 0);

        public void Recover() => log.Recover();
        public void Reset() => log.Reset();
        public void Dispose() => log.Dispose();

        public void SetLogShiftTailCallback(Action<long, long> SafeTailShiftCallback, int idx = 0)
            => log.SafeTailShiftCallback = SafeTailShiftCallback;

        public TsavoriteLogScanIterator Scan(long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => log.Scan(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        public TsavoriteLogScanSingleIterator ScanSingle(long beginAddress, long endAddress, bool recover = true, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => log.ScanSingle(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        public void InitializeIf(ref AofAddress recoveredSafeAofAddress)
        {
            if (TailAddress[0] < recoveredSafeAofAddress[0])
                Initialize(TailAddress[0], recoveredSafeAofAddress[0]);
        }

        public void Initialize(in AofAddress beginAddress, in AofAddress committedUntilAddress, long lastCommitNum = 0)
            => Initialize(beginAddress[0], committedUntilAddress[0], lastCommitNum);

        private void Initialize(long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
            => log.Initialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void SafeInitialize(long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
            => log.SafeInitialize(beginAddress, committedUntilAddress, lastCommitNum);

        public void Enqueue<THeader>(THeader userHeader, out long logicalAddress)
            where THeader : unmanaged
            => log.Enqueue(userHeader, out logicalAddress);

        public void Enqueue<THeader, TInput>(THeader userHeader, ref SpanByte item1, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => log.Enqueue(userHeader, ref item1, ref input, out logicalAddress);

        public void Enqueue<THeader>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, out long logicalAddress)
            where THeader : unmanaged
            => log.Enqueue(userHeader, ref item1, ref item2, out logicalAddress);

        public void Enqueue<THeader, TInput>(THeader userHeader, ref SpanByte item1, ref SpanByte item2, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => log.Enqueue(userHeader, ref item1, ref item2, ref input, out logicalAddress);

        public void Enqueue<THeader, TInput>(THeader userHeader, ref TInput input, out long logicalAddress)
            where THeader : unmanaged where TInput : IStoreInput
            => log.Enqueue(userHeader, ref input, out logicalAddress);

        public unsafe int UnsafeGetLength(byte* headerPtr)
            => log.UnsafeGetLength(headerPtr);

        public void UnsafeCommitMetadataOnly(TsavoriteLogRecoveryInfo info, bool isProtected)
            => log.UnsafeCommitMetadataOnly(info, isProtected);

        public long UnsafeEnqueueRaw(ReadOnlySpan<byte> entryBytes, bool noCommit = false)
            => log.UnsafeEnqueueRaw(entryBytes, noCommit);

        public int UnsafeGetLogPageSizeBits()
            => log.UnsafeGetLogPageSizeBits();

        public long UnsafeGetReadOnlyAddressLagOffset()
            => log.UnsafeGetReadOnlyAddressLagOffset();

        public void UnsafeShiftBeginAddress(AofAddress untilAddress, bool snapToPageStart = false, bool truncateLog = false)
            => log.UnsafeShiftBeginAddress(untilAddress[0], snapToPageStart, truncateLog);

        public void UnsafeShiftBeginAddress(long untilAddress, long subIdx, bool snapToPageStart = false, bool truncateLog = false)
            => log.UnsafeShiftBeginAddress(untilAddress, snapToPageStart, truncateLog);

        public void TruncateUntil(AofAddress untilAddress) => log.TruncateUntil(untilAddress[0]);
        public void TruncateUntil(long untilAddress, long subIdx) => log.TruncateUntil(untilAddress);

        public ValueTask CommitAsync(byte[] cookie = null, CancellationToken token = default)
            => log.CommitAsync(cookie, token);

        public void Commit(bool spinWait = false, byte[] cookie = null)
            => log.Commit(spinWait, cookie);

        public ValueTask WaitForCommitAsync(long untilAddress = 0, long commitNum = -1, CancellationToken token = default)
            => log.WaitForCommitAsync(untilAddress, commitNum, token);

        public void WaitForInitializeCommit(long untilAddress = 0, long commitNum = -1)
            => log.WaitForCommit(untilAddress, commitNum);

        public void WaitForCommit(long untilAddress = 0, long commitNum = -1)
            => log.WaitForCommit();

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
