// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using System;
using Garnet.common;
using Garnet.server;
using Microsoft.Extensions.Logging;
using Tsavorite.core;

namespace Garnet.cluster
{
    internal sealed unsafe partial class ClusterSession : IClusterSession
    {
        ReplicaReplayDriverStore replicaReplayDriverStore = null;
        TsavoriteLog replaySessionSublog = null;

        /// <summary>
        /// Apply primary AOF records.
        /// </summary>
        /// <param name="physicalSublogIdx"></param>
        /// <param name="record"></param>
        /// <param name="recordLength"></param>
        /// <param name="previousAddress"></param>
        /// <param name="currentAddress"></param>
        /// <param name="nextAddress"></param>
        public unsafe void ProcessPrimaryStream(int physicalSublogIdx, byte* record, int recordLength, long previousAddress, long currentAddress, long nextAddress)
        {
            // logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, storeWrapper.appendOnlyFile.TailAddress);
            var currentConfig = clusterProvider.clusterManager.CurrentConfig;
            var syncReplay = clusterProvider.serverOptions.ReplicationOffsetMaxLag == 0;

            // Need to ensure that this replay task is allowed to complete before the replicaReplayGroup is disposed
            // NOTE: this should not be expensive because every replay task has its own lock copy
            // Cache invalidation happens only on dispose which is rare operation
            var failReplay = syncReplay && !replicaReplayDriverStore.GetReplayDriver(physicalSublogIdx).ResumeReplay();
            try
            {
                if (failReplay)
                    throw new GarnetException($"[{physicalSublogIdx}] Failed to acquire activeReplay lock!", LogLevel.Warning, clientResponse: false);

                if (clusterProvider.replicationManager.CannotStreamAOF)
                {
                    logger?.LogError("Replica is recovering cannot sync AOF");
                    throw new GarnetException("Replica is recovering cannot sync AOF", LogLevel.Warning, clientResponse: false);
                }

                if (currentConfig.LocalNodeRole != NodeRole.REPLICA)
                {
                    logger?.LogWarning("This node {nodeId} is not a replica", currentConfig.LocalNodeId);
                    throw new GarnetException($"This node {currentConfig.LocalNodeId} is not a replica", LogLevel.Warning, clientResponse: false);
                }

                if (clusterProvider.serverOptions.FastAofTruncate)
                {
                    // If the incoming AOF chunk fits in the space between previousAddress and currentAddress (ReplicationOffset),
                    // an enqueue will result in an offset mismatch. So, we have to first reset the AOF to point to currentAddress.
                    if (currentAddress > previousAddress)
                    {
                        if (
                            (currentAddress % (1 << clusterProvider.replicationManager.PageSizeBits) != 0) || // the skip was to a non-page-boundary
                            (currentAddress >= previousAddress + recordLength) // the skip will not be auto-handled by the AOF enqueue
                            )
                        {
                            logger?.LogWarning("MainMemoryReplication: Skipping from {ReplicaReplicationOffset} to {currentAddress}", clusterProvider.replicationManager.GetSublogReplicationOffset(physicalSublogIdx), currentAddress);
                            clusterProvider.storeWrapper.appendOnlyFile.SafeInitialize(physicalSublogIdx, currentAddress, currentAddress);
                            clusterProvider.replicationManager.SetSublogReplicationOffset(physicalSublogIdx, currentAddress);
                        }
                    }
                }

                // Injection for a "something went wrong with THIS Replica's AOF file"
                ExceptionInjectionHelper.TriggerException(ExceptionInjectionType.Divergent_AOF_Stream);

                var tail = clusterProvider.storeWrapper.appendOnlyFile.Log.TailAddress;
                var nextPageBeginAddress = ((tail[physicalSublogIdx] >> clusterProvider.replicationManager.PageSizeBits) + 1) << clusterProvider.replicationManager.PageSizeBits;
                // Check to ensure:
                // 1. if record fits in current page tailAddress of this local node (replica) should be equal to the incoming currentAddress (address of chunk send from primary node)
                // 2. if record does not fit in current page start address of the next page matches incoming currentAddress (address of chunk send from primary node)
                // otherwise fail and break the connection
                if ((tail[physicalSublogIdx] + recordLength <= nextPageBeginAddress && tail[physicalSublogIdx] != currentAddress) ||
                    (tail[physicalSublogIdx] + recordLength > nextPageBeginAddress && nextPageBeginAddress != currentAddress))
                {
                    logger?.LogError("Divergent AOF Stream recordLength:{recordLength}; previousAddress:{previousAddress}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; tailAddress:{tail}", recordLength, previousAddress, currentAddress, nextAddress, tail);
                    throw new GarnetException($"Divergent AOF Stream recordLength:{recordLength}; previousAddress:{previousAddress}; currentAddress:{currentAddress}; nextAddress:{nextAddress}; tailAddress:{tail}", LogLevel.Warning, clientResponse: false);
                }

                // Address check only if synchronous replication is enabled
                if (clusterProvider.storeWrapper.serverOptions.ReplicationOffsetMaxLag == 0 && clusterProvider.replicationManager.GetSublogReplicationOffset(physicalSublogIdx) != tail[physicalSublogIdx])
                {
                    logger?.LogInformation("Processing {recordLength} bytes; previousAddress {previousAddress}, currentAddress {currentAddress}, nextAddress {nextAddress}, current AOF tail {tail}", recordLength, previousAddress, currentAddress, nextAddress, tail);
                    logger?.LogError("Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {ReplicaReplicationOffset}, aof.TailAddress {tailAddress}", clusterProvider.replicationManager.ReplicationOffset, tail);
                    throw new GarnetException($"Before ProcessPrimaryStream: Replication offset mismatch: ReplicaReplicationOffset {clusterProvider.replicationManager.ReplicationOffset}, aof.TailAddress {tail}", LogLevel.Warning, clientResponse: false);
                }

                // Initialize sublog ref if first time
                replaySessionSublog ??= clusterProvider.storeWrapper.appendOnlyFile.Log.GetSubLog(physicalSublogIdx);

                // Enqueue to AOF
                _ = replaySessionSublog.UnsafeEnqueueRaw(new Span<byte>(record, recordLength), noCommit: clusterProvider.serverOptions.EnableFastCommit);

                if (clusterProvider.storeWrapper.serverOptions.ReplicationOffsetMaxLag == 0)
                {
                    // Synchronous replay
                    replicaReplayDriverStore.GetReplayDriver(physicalSublogIdx).Consume(record, recordLength, currentAddress, nextAddress, isProtected: false);
                }
                else
                {
                    // Initialize iterator and run background task once
                    replicaReplayDriverStore.GetReplayDriver(physicalSublogIdx).InitialiazeBackgroundReplayTask(previousAddress);

                    // Throttle to give the opportunity to the background replay task to catch up
                    replicaReplayDriverStore.GetReplayDriver(physicalSublogIdx).ThrottlePrimary();
                }
            }
            catch (Exception ex)
            {
                logger?.LogWarning(ex, "An exception occurred at ReplicationManager.ProcessPrimaryStream");
                throw new GarnetException(ex.Message, ex, LogLevel.Warning, clientResponse: false);
            }
            finally
            {
                if (syncReplay && !failReplay)
                    replicaReplayDriverStore.GetReplayDriver(physicalSublogIdx).SuspendReplay();
            }
        }
    }
}