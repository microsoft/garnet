// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using System.Threading;
using Garnet.common;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.server
{
    public sealed class GarnetAppendOnlyFile
    {
        const long kFirstValidAofAddress = 64;

        public long TotalSize() => Log.TailAddress.AggregateDiff(Log.BeginAddress);

        public ReplicaReadConsistencyStateManager replicaReadConsistencyStateManager = null;

        public SequenceNumberGenerator seqNumGen = null;

        public GarnetLog Log { get; private set; }

        public readonly GarnetServerOptions serverOptions;

        public long HeaderSize => Log.HeaderSize;

        public readonly AofAddress InvalidAofAddress;

        public readonly AofAddress MaxAofAddress;

        public readonly ILogger logger;

        /// <summary>
        /// Calculate virtual sublog index provided physical sublog index and replay task index
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="replayIdx"></param>
        /// <returns></returns>
        public int GetVirtualSublogIdx(int sublogIdx, int replayIdx)
            => (sublogIdx * serverOptions.AofReplayTaskCount) + replayIdx;


        /// <summary>
        /// Garnet append only file constructor
        /// </summary>
        /// <param name="serverOptions"></param>
        /// <param name="logSettings"></param>
        /// <param name="logger"></param>
        public GarnetAppendOnlyFile(GarnetServerOptions serverOptions, TsavoriteLogSettings[] logSettings, ILogger logger = null)
        {
            Log = new(serverOptions, logSettings, logger);
            this.serverOptions = serverOptions;
            InvalidAofAddress = AofAddress.Create(length: serverOptions.AofPhysicalSublogCount, value: -1);
            MaxAofAddress = AofAddress.Create(length: serverOptions.AofPhysicalSublogCount, value: long.MaxValue);
            CreateOrUpdateKeySequenceManager();
            if (serverOptions.MultiLogEnabled)
                seqNumGen = new SequenceNumberGenerator(0);
            this.logger = logger;
        }

        /// <summary>
        /// Dispose append only file
        /// </summary>
        public void Dispose() => Log.Dispose();

        /// <summary>
        /// Create or update existing timestamp manager
        /// NOTE: We need to create a new version for consistency manager in order for running sessions to update their context on the next read
        /// </summary>
        public void CreateOrUpdateKeySequenceManager()
        {
            // Create manager only if sharded log is enabled
            if (!serverOptions.MultiLogEnabled) return;
            var currentVersion = replicaReadConsistencyStateManager?.CurrentVersion ?? 0L;
            var _replayTimestampManager = new ReplicaReadConsistencyStateManager(currentVersion + 1, this, serverOptions, logger);
            _ = Interlocked.CompareExchange(ref replicaReadConsistencyStateManager, _replayTimestampManager, replicaReadConsistencyStateManager);
        }

        /// <summary>
        /// Reset sequence number generator
        /// </summary>
        public void ResetSequenceNumberGenerator()
        {
            var start = replicaReadConsistencyStateManager.MaxSequenceNumber;
            var newSeqNumGen = new SequenceNumberGenerator(start);
            _ = Interlocked.CompareExchange(ref seqNumGen, newSeqNumGen, seqNumGen);
        }

        /// <summary>
        /// Invoke the prepare phase of the consistent read protocol
        /// </summary>
        /// <param name="key"></param>
        /// <param name="replicaReadSessionContext"></param>
        /// <param name="readSessionWaiter"></param>
        public void ConsistentReadKeyPrepare(ReadOnlySpan<byte> key, ref ReplicaReadSessionContext replicaReadSessionContext, ReadSessionWaiter readSessionWaiter)
            => replicaReadConsistencyStateManager.ConsistentReadKeyPrepare(key, ref replicaReadSessionContext, readSessionWaiter);

        /// <summary>
        /// Invoke the update phase of the consistent read protocol
        /// </summary>
        /// <param name="replicaReadSessionContext"></param>
        public void ConsistentReadSequenceNumberUpdate(ref ReplicaReadSessionContext replicaReadSessionContext)
            => replicaReadConsistencyStateManager.ConsistentReadSequenceNumberUpdate(ref replicaReadSessionContext);

        /// <summary>
        /// Set log shift tail callbacks
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="SafeTailShiftCallback"></param>
        public void SetLogShiftTailCallback(int sublogIdx, Action<long, long> SafeTailShiftCallback)
            => Log.GetSubLog(sublogIdx).SafeTailShiftCallback = SafeTailShiftCallback;

        /// <summary>
        /// TODO: Is this necessary for recover? Can we use ScanSingle?
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="recover"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="scanUncommitted"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public TsavoriteLogScanIterator Scan(int sublogIdx, long beginAddress, long endAddress, bool recover = true, DiskScanBufferingMode scanBufferingMode = DiskScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => Log.GetSubLog(sublogIdx).Scan(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        /// <summary>
        /// TODO: same question as above but for replay? Why do we have 2 different methods?
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="recover"></param>
        /// <param name="scanBufferingMode"></param>
        /// <param name="scanUncommitted"></param>
        /// <param name="logger"></param>
        /// <returns></returns>
        public TsavoriteLogScanSingleIterator ScanSingle(int sublogIdx, long beginAddress, long endAddress, bool recover = true, DiskScanBufferingMode scanBufferingMode = DiskScanBufferingMode.DoublePageBuffering, bool scanUncommitted = false, ILogger logger = null)
            => Log.GetSubLog(sublogIdx).ScanSingle(beginAddress, endAddress, recover, scanBufferingMode, scanUncommitted, logger);

        /// <summary>
        /// Safe initialize when FastAofTruncate is enabled
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="beginAddress"></param>
        /// <param name="committedUntilAddress"></param>
        /// <param name="lastCommitNum"></param>
        public void SafeInitialize(int sublogIdx, long beginAddress, long committedUntilAddress, long lastCommitNum = 0)
            => Log.GetSubLog(sublogIdx).SafeInitialize(beginAddress, committedUntilAddress, lastCommitNum);

        /// <summary>
        /// Initialize sublog before attach
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="committedUntilAddress"></param>
        /// <param name="lastCommitNum"></param>
        public void Initialize(in AofAddress beginAddress, in AofAddress committedUntilAddress, long lastCommitNum = 0)
            => Log.Initialize(beginAddress, committedUntilAddress, lastCommitNum);

        /// <summary>
        /// Enqueue a signal to refresh sublog tail
        /// </summary>
        /// <param name="sublogIdx"></param>
        /// <param name="sequenceNumber"></param>
        public void EnqueueRefreshSublogTail(int sublogIdx, long sequenceNumber)
        {
            var refreshSublogTailHeader = new AofShardedHeader
            {
                basicHeader = new AofHeader { opType = AofEntryType.RefreshSublogTail },
                sequenceNumber = sequenceNumber
            };
            Log.GetSubLog(sublogIdx).Enqueue(refreshSublogTailHeader, out _);
        }

        /// <summary>
        /// Compute AOF sync replay address at recovery
        /// </summary>
        /// <param name="recoverFromRemote"></param>
        /// <param name="sameMainStoreCheckpointHistory"></param>
        /// <param name="sameHistory2"></param>
        /// <param name="replicationOffset2"></param>
        /// <param name="replicaAofBeginAddress"></param>
        /// <param name="replicaAofTailAddress"></param>
        /// <param name="beginAddress"></param>
        /// <param name="checkpointAofBeginAddress"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public ulong ComputeAofSyncReplayAddress(
            bool recoverFromRemote,
            bool sameMainStoreCheckpointHistory,
            bool sameHistory2,
            in AofAddress replicationOffset2,
            in AofAddress replicaAofBeginAddress,
            in AofAddress replicaAofTailAddress,
            in AofAddress beginAddress,
            ref AofAddress checkpointAofBeginAddress)
        {
            var replayAOFMap = 0UL;
            for (var sublogIdx = 0; sublogIdx < serverOptions.AofPhysicalSublogCount; sublogIdx++)
                ComputeAofSubloSyncReplayAddress(sublogIdx, ref replayAOFMap, recoverFromRemote, sameMainStoreCheckpointHistory, sameHistory2, replicationOffset2, replicaAofBeginAddress, replicaAofTailAddress, beginAddress, ref checkpointAofBeginAddress);

            return replayAOFMap;

            void ComputeAofSubloSyncReplayAddress(
                int sublogIdx,
                ref ulong replayAOFMap,
                bool recoverFromRemote,
                bool sameMainStoreCheckpointHistory,
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

                        if (!sameMainStoreCheckpointHistory)
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

        /// <summary>
        /// Perform a data loss check at recovery
        /// </summary>
        /// <param name="possibleAofDataLoss"></param>
        /// <param name="syncFromAofAddress"></param>
        /// <param name="logger"></param>
        /// <exception cref="Exception"></exception>
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